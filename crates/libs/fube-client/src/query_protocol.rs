//! Declarative external query framing with shared list/item reply profiles.

use std::marker::PhantomData;

use fubst::serde::{
    to_vec_with_limits, EmptyObjectArray, FixedTag, ObjectArray, Pointer, RawTagged,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    query_arguments::QueryArguments, DomainFailure, PagedQuery, Paging, QueryDescription, QueryPage,
};

/// Native payload type IDs from ServiceModel/QueryResultHelpersEnum.h.
pub(crate) mod result_tags {
    pub(crate) const NODE: i32 = 1;
    pub(crate) const SERVICE: i32 = 2;
    pub(crate) const APPLICATION: i32 = 4;
    pub(crate) const PARTITION: i32 = 5;
    pub(crate) const REPLICA: i32 = 6;
    pub(crate) const DEPLOYED_APPLICATION: i32 = 10;
    pub(crate) const DEPLOYED_SERVICE_PACKAGE: i32 = 11;
    pub(crate) const DEPLOYED_REPLICA: i32 = 12;
    pub(crate) const DEPLOYED_CODE_PACKAGE: i32 = 14;
    pub(crate) const NODE_HEALTH: i32 = 16;
}

pub(crate) trait Query: Serialize {
    type Output: Send + 'static;
    type Reply: Reply<Output = Self::Output>;
    const NAME: &'static str;
    const PAGING_FIRST: bool = false;
    const SUPPORTS_MAX_RESULTS: bool = true;

    fn validate_parameters(&self) -> fubst::Result<()> {
        Ok(())
    }
    fn validate_record(
        record: <Self::Reply as Reply>::Record,
    ) -> std::result::Result<<Self::Reply as Reply>::Record, &'static str> {
        Ok(record)
    }
}

pub(crate) trait QueryRequest: Send {
    type Parameters: Query;

    fn parameters(&self) -> &Self::Parameters;
    fn paging(&self) -> Option<&Paging>;
}

impl<Q: Query + Send> QueryRequest for PagedQuery<Q> {
    type Parameters = Q;

    fn parameters(&self) -> &Q {
        &self.parameters
    }

    fn paging(&self) -> Option<&Paging> {
        Some(&self.paging)
    }
}

impl<Q: Query + Send> QueryRequest for QueryDescription<Q> {
    type Parameters = Q;

    fn parameters(&self) -> &Q {
        &self.parameters
    }

    fn paging(&self) -> Option<&Paging> {
        None
    }
}

pub(crate) fn encode_query_body<Q: Query>(
    query: &Q,
    paging: Option<&Paging>,
    limits: fubst::Limits,
) -> fubst::Result<Vec<u8>> {
    let arguments = arguments(query, paging, QueryArguments::native())?;
    encode_body(Q::NAME, &arguments.values, limits)
}

fn arguments<Q: Query>(
    query: &Q,
    paging: Option<&Paging>,
    mut arguments: QueryArguments,
) -> fubst::Result<QueryArguments> {
    if Q::PAGING_FIRST {
        append_paging::<Q>(&mut arguments, paging)?;
    }
    query.validate_parameters()?;
    arguments.append(query)?;
    if !Q::PAGING_FIRST {
        append_paging::<Q>(&mut arguments, paging)?;
    }
    Ok(arguments)
}

fn append_paging<Q: Query>(
    arguments: &mut QueryArguments,
    paging: Option<&Paging>,
) -> fubst::Result<()> {
    let Some(paging) = paging else {
        return Ok(());
    };
    if paging.max_results.is_some() && !Q::SUPPORTS_MAX_RESULTS {
        return Err(fubst::Error::InvalidInput(
            "query does not support MaxResults",
        ));
    }
    if paging
        .max_results
        .is_some_and(|max| max.get() > i64::MAX as u64)
    {
        return Err(fubst::Error::InvalidInput(
            "MaxResults exceeds native signed range",
        ));
    }
    arguments.append(paging)
}

fn encode_body(
    name: &str,
    args: &[(String, String)],
    limits: fubst::Limits,
) -> fubst::Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Key<'a> {
        value: &'a str,
    }
    #[derive(Serialize)]
    struct Pair<'a>(Key<'a>, &'a str);
    #[derive(Serialize)]
    struct Arguments<T> {
        values: ObjectArray<T>,
    }
    #[derive(Serialize)]
    struct Request<'a, T>(Arguments<T>, &'a str);
    let pairs: Vec<_> = args
        .iter()
        .map(|(key, value)| Pair(Key { value: key }, value))
        .collect();
    to_vec_with_limits(
        &Request(
            Arguments {
                values: ObjectArray(pairs),
            },
            name,
        ),
        limits,
    )
}

pub(crate) enum ReplyError {
    Protocol(&'static str),
    Domain(DomainFailure),
}

pub(crate) type ReplyResult<T> = std::result::Result<T, ReplyError>;

impl From<&'static str> for ReplyError {
    fn from(reason: &'static str) -> Self {
        Self::Protocol(reason)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ReplyEnvelope {
    List(i64),
    Item(i64),
    Empty(i64),
}

impl ReplyEnvelope {
    pub(crate) fn check(
        self,
        actor: Option<i64>,
        action: Option<impl Iterator<Item = u16>>,
    ) -> std::result::Result<(), &'static str> {
        match self {
            Self::List(expected) => {
                if actor.is_some_and(|actor| actor != expected) {
                    return Err(if expected == 5 {
                        "list reply is not from NamingGateway"
                    } else {
                        "unexpected unpaged list reply actor"
                    });
                }
                if action.is_some_and(|action| !action.eq("QueryReply".encode_utf16())) {
                    return Err("unexpected list reply action");
                }
            }
            Self::Item(expected) => {
                if actor != Some(expected)
                    || !action
                        .is_some_and(|action| action.eq("OperationSuccessAction".encode_utf16()))
                {
                    return Err("unexpected item success envelope");
                }
            }
            Self::Empty(expected) => {
                if actor.is_some_and(|actor| actor != expected)
                    || action.is_some_and(|action| !action.eq("QueryReply".encode_utf16()))
                {
                    return Err("movement query requires RS actor 13 and QueryReply");
                }
            }
        }
        Ok(())
    }
}

pub(crate) trait Reply {
    type Record: DeserializeOwned;
    type Body: DeserializeOwned + Send;
    type Output: Send + 'static;
    const ENVELOPE: ReplyEnvelope;

    fn decode_body(
        body: Self::Body,
        validate: fn(Self::Record) -> std::result::Result<Self::Record, &'static str>,
    ) -> ReplyResult<Self::Output>;
}

/// NamingGateway list aggregate with optional, checked actor/action headers.
pub(crate) struct ListReply<T, const TAG: i32, const STRICT_PAGING: bool = true>(PhantomData<T>);

impl<T: DeserializeOwned + Send + 'static, const TAG: i32, const STRICT_PAGING: bool> Reply
    for ListReply<T, TAG, STRICT_PAGING>
{
    type Record = T;
    type Body = QueryResultEnvelope<FixedTag<ListPayload<T>, TAG>>;
    type Output = QueryPage<T>;
    const ENVELOPE: ReplyEnvelope = ReplyEnvelope::List(5);

    fn decode_body(
        body: Self::Body,
        validate: fn(T) -> std::result::Result<T, &'static str>,
    ) -> ReplyResult<Self::Output> {
        decode_list::<T, TAG, STRICT_PAGING>(body, validate, true)
    }
}

/// Native node-local lists that have no paging request or continuation contract.
pub(crate) struct UnpagedListReply<T, const TAG: i32, const ACTOR_ID: i64 = 5>(PhantomData<T>);

impl<T: DeserializeOwned + Send + 'static, const TAG: i32, const ACTOR_ID: i64> Reply
    for UnpagedListReply<T, TAG, ACTOR_ID>
{
    type Record = T;
    type Body = QueryResultEnvelope<FixedTag<ListPayload<T>, TAG>>;
    type Output = Vec<T>;
    const ENVELOPE: ReplyEnvelope = ReplyEnvelope::List(ACTOR_ID);

    fn decode_body(
        body: Self::Body,
        validate: fn(T) -> std::result::Result<T, &'static str>,
    ) -> ReplyResult<Vec<T>> {
        decode_list::<T, TAG, true>(body, validate, false).map(|page| page.items)
    }
}

fn decode_list<T: DeserializeOwned, const TAG: i32, const STRICT_PAGING: bool>(
    body: QueryResultEnvelope<FixedTag<ListPayload<T>, TAG>>,
    validate: fn(T) -> std::result::Result<T, &'static str>,
    allow_paging: bool,
) -> ReplyResult<QueryPage<T>> {
    let result = decode::<FixedTag<ListPayload<T>, TAG>, _>(
        body,
        STRICT_PAGING,
        allow_paging,
        |FixedTag(payload)| {
            if payload.item.0.is_some() {
                return Err("list has a single-item payload".into());
            }
            payload
                .items
                .0
                .into_iter()
                .map(validate)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        },
    )?;
    if result.kind != 2 {
        return Err("query result is not a List".into());
    }
    Ok(QueryPage {
        items: result.payload.ok_or("list has no typed wrapper")?,
        continuation_token: result.paging.filter(|s| !s.is_empty()),
    })
}

/// Unpaged item with a fixed payload tag and required actor/OperationSuccessAction.
pub(crate) struct ItemReply<T, const TAG: i32, const ACTOR_ID: i64>(PhantomData<T>);

impl<T: DeserializeOwned + Send + 'static, const TAG: i32, const ACTOR_ID: i64> Reply
    for ItemReply<T, TAG, ACTOR_ID>
{
    type Record = T;
    type Body = QueryResultEnvelope<FixedTag<ItemPayload<T>, TAG>>;
    type Output = T;
    const ENVELOPE: ReplyEnvelope = ReplyEnvelope::Item(ACTOR_ID);

    fn decode_body(
        body: Self::Body,
        validate: fn(T) -> std::result::Result<T, &'static str>,
    ) -> ReplyResult<T> {
        let result =
            decode::<FixedTag<ItemPayload<T>, TAG>, _>(body, false, true, |FixedTag(payload)| {
                validate(payload.item.0.ok_or("null query item")?).map_err(Into::into)
            })?;
        if result.kind != 1 || result.paging.is_some() {
            return Err("query result is not an unpaged Item".into());
        }
        result
            .payload
            .ok_or_else(|| "item has no typed wrapper".into())
    }
}

#[derive(Deserialize)]
pub(crate) struct Empty {}

#[derive(Deserialize)]
pub(crate) struct ListPayload<T> {
    pub items: ObjectArray<Vec<T>>,
    pub item: Pointer<Empty>,
}

#[derive(Deserialize)]
pub(crate) struct ItemPayload<T> {
    _list: EmptyObjectArray,
    item: Pointer<T>,
}

#[derive(Deserialize)]
pub(crate) struct PagingStatus {
    pub token: String,
}

#[derive(Deserialize)]
pub(crate) struct QueryResultEnvelope<T> {
    pub kind: i64,
    pub payload: Pointer<T>,
    pub error: i64,
    pub paging: Pointer<RawTagged<PagingStatus>>,
}

struct QueryResult<T> {
    kind: i64,
    payload: Option<T>,
    paging: Option<String>,
}

fn decode<T, U>(
    envelope: QueryResultEnvelope<T>,
    strict_paging: bool,
    allow_paging: bool,
    validate: impl FnOnce(T) -> ReplyResult<U>,
) -> ReplyResult<QueryResult<U>> {
    // A native query error must not hide a malformed payload and allow reuse
    // of a connection that previously failed protocol validation.
    let payload = envelope.payload.0.map(validate).transpose()?;
    if !allow_paging && envelope.paging.0.is_some() {
        return Err("unpaged list has paging status".into());
    }
    let paging = envelope
        .paging
        .0
        .map(|status| {
            if strict_paging && !status.type_info.is_empty() {
                return Err(ReplyError::Protocol("unexpected paging status type"));
            }
            Ok(status.value.token)
        })
        .transpose()?;
    if envelope.error != 0 {
        return Err(ReplyError::Domain(DomainFailure {
            code: envelope.error,
            message: String::new(),
        }));
    }
    Ok(QueryResult {
        kind: envelope.kind,
        payload,
        paging,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use std::num::NonZeroU64;

    #[test]
    fn declarations_define_native_named_json_and_round_trips() {
        fn check<Q>(query: Q, expected: serde_json::Value)
        where
            Q: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
        {
            assert_eq!(serde_json::to_value(&query).unwrap(), expected);
            assert_eq!(serde_json::from_value::<Q>(expected).unwrap(), query);
        }
        check(
            NodeQueryDescription {
                parameters: NodeQueryParameters {
                    node_name: Some("Node".into()),
                    status_filter: NodeStatusFilter::UP,
                },
                paging: Paging {
                    continuation_token: Some("next".into()),
                    max_results: NonZeroU64::new(3),
                },
            },
            serde_json::json!({
                "parameters": { "NodeName": "Node", "NodeStatusFilter": 1 },
                "paging": { "ContinuationToken": "next", "MaxResults": 3 },
            }),
        );
        check(
            ApplicationQueryDescription::new(ApplicationQueryParameters {
                filter: ApplicationListFilter::DefinitionKind(
                    ApplicationDefinitionKindFilter::COMPOSE,
                ),
                exclude_application_parameters: false,
            }),
            serde_json::json!({
                "parameters": { "ApplicationDefinitionKindFilter": 2 },
                "paging": {},
            }),
        );
        check(
            ServiceQueryDescription::new(ServiceQueryParameters {
                application_name: "fabric:/App".parse().unwrap(),
                filter: ServiceListFilter::TypeName("Type".into()),
            }),
            serde_json::json!({
                "parameters": {
                    "ApplicationName": "fabric:/App", "ServiceTypeName": "Type",
                },
                "paging": {},
            }),
        );
        check(
            NodeHealthQueryDescription::new(NodeHealthQueryParameters {
                node_name: "Node".into(),
                events_filter: Some(HealthEventsFilter::ERROR.into()),
                health_policy: Some(NodeHealthPolicy {
                    consider_warning_as_error: false,
                }),
            }),
            serde_json::json!({
                "parameters": {
                    "NodeName": "Node", "EventsFilter": { "HealthStateFilter": 8 },
                    "ClusterHealthPolicy": { "ConsiderWarningAsError": false },
                },
            }),
        );
    }
}
