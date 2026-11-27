//! Declarative external query framing with shared list/item reply profiles.

use std::marker::PhantomData;

use fubst::serde::{
    to_vec_with_limits, EmptyObjectArray, FixedTag, ObjectArray, Pointer, RawTagged,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    query_arguments::QueryArguments, DomainFailure, PagedQuery, Paging, QueryDescription, QueryPage,
};

crate::native::lossless_enum!(
    /// FABRIC_QUERY_RESULT_KIND from internal FabricTypes_.idl at native pin 3988b451.
    /// Invalid is also the native empty/error-envelope default, not a List or Item.
    pub(crate) enum NativeQueryResultKind {
        Invalid = 0,
        Item = 1,
        List = 2,
    }
);

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
    if result.kind != NativeQueryResultKind::List {
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
        if result.kind != NativeQueryResultKind::Item || result.paging.is_some() {
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
    pub kind: NativeQueryResultKind,
    pub payload: Pointer<T>,
    pub error: i64,
    pub paging: Pointer<RawTagged<PagingStatus>>,
}

struct QueryResult<T> {
    kind: NativeQueryResultKind,
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
    fn query_kinds_preserve_numeric_json_and_unknown_values() {
        for code in [i64::MIN, -1, 0, 1, 2, 3, i64::MAX] {
            let json = code.to_string();
            let kind: NativeQueryResultKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind.as_raw(), code);
            assert_eq!(serde_json::to_string(&kind).unwrap(), json);
        }
        for json in ["null", "\"List\"", "1.5", "18446744073709551615"] {
            assert!(serde_json::from_str::<NativeQueryResultKind>(json).is_err());
        }
    }

    // Model-only reply validation: no native byte fixtures or model peers.
    type ModelList = ListReply<u32, 0>;
    type ModelItem = ItemReply<u32, 0, 0>;

    fn model_record(value: u32) -> std::result::Result<u32, &'static str> {
        if value == 0 {
            Err("invalid model record")
        } else {
            Ok(value)
        }
    }

    fn list_model(kind: NativeQueryResultKind, error: i64) -> <ModelList as Reply>::Body {
        QueryResultEnvelope {
            kind,
            payload: Pointer(Some(FixedTag(ListPayload {
                items: ObjectArray(vec![42]),
                item: Pointer(None),
            }))),
            error,
            paging: Pointer(None),
        }
    }

    fn item_model(kind: NativeQueryResultKind, error: i64) -> <ModelItem as Reply>::Body {
        QueryResultEnvelope {
            kind,
            payload: Pointer(Some(FixedTag(ItemPayload {
                _list: EmptyObjectArray,
                item: Pointer(Some(42)),
            }))),
            error,
            paging: Pointer(None),
        }
    }

    fn paging_model(type_info: Vec<u8>) -> Pointer<RawTagged<PagingStatus>> {
        Pointer(Some(RawTagged {
            type_info,
            value: PagingStatus {
                token: "next".into(),
            },
        }))
    }

    #[test]
    fn query_reply_models_require_the_expected_kind_but_preserve_domain_error_priority() {
        for code in [i64::MIN, -1, 0, 1, 2, 3, i64::MAX] {
            let kind = NativeQueryResultKind::from_raw(code);
            let mut list = list_model(kind, 0);
            list.paging = paging_model(vec![]);
            let result = ModelList::decode_body(list, model_record);
            if kind == NativeQueryResultKind::List {
                let page = result.unwrap_or_else(|_| panic!("valid List model rejected"));
                assert_eq!(page.items, [42]);
                assert_eq!(page.continuation_token.as_deref(), Some("next"));
            } else {
                assert!(matches!(
                    result,
                    Err(ReplyError::Protocol("query result is not a List"))
                ));
            }
            let result = ModelItem::decode_body(item_model(kind, 0), model_record);
            if kind == NativeQueryResultKind::Item {
                assert!(matches!(result, Ok(42)));
            } else {
                assert!(matches!(
                    result,
                    Err(ReplyError::Protocol("query result is not an unpaged Item"))
                ));
            }
            let mut list = list_model(kind, -77);
            list.payload = Pointer(None);
            assert!(matches!(
                ModelList::decode_body(list, model_record),
                Err(ReplyError::Domain(DomainFailure { code: -77, .. }))
            ));
            let mut item = item_model(kind, -77);
            item.payload = Pointer(None);
            item.paging = paging_model(vec![1]);
            assert!(matches!(
                ModelItem::decode_body(item, model_record),
                Err(ReplyError::Domain(DomainFailure { code: -77, .. }))
            ));
        }
    }

    #[test]
    fn malformed_reply_models_are_not_hidden_by_query_kind_or_domain_errors() {
        let mut list = list_model(NativeQueryResultKind::Unrecognized(-9), -77);
        list.payload.0.as_mut().unwrap().0.items.0[0] = 0;
        assert!(matches!(
            ModelList::decode_body(list, model_record),
            Err(ReplyError::Protocol("invalid model record"))
        ));
        let mut list = list_model(NativeQueryResultKind::List, -77);
        list.payload.0.as_mut().unwrap().0.item = Pointer(Some(Empty {}));
        assert!(matches!(
            ModelList::decode_body(list, model_record),
            Err(ReplyError::Protocol("list has a single-item payload"))
        ));
        let mut list = list_model(NativeQueryResultKind::Unrecognized(-9), -77);
        list.paging = paging_model(vec![1]);
        assert!(matches!(
            ModelList::decode_body(list, model_record),
            Err(ReplyError::Protocol("unexpected paging status type"))
        ));
        let mut list = list_model(NativeQueryResultKind::List, -77);
        list.paging = paging_model(vec![]);
        assert!(matches!(
            UnpagedListReply::<u32, 0>::decode_body(list, model_record),
            Err(ReplyError::Protocol("unpaged list has paging status"))
        ));
        let mut item = item_model(NativeQueryResultKind::Unrecognized(-9), -77);
        item.payload.0.as_mut().unwrap().0.item = Pointer(None);
        assert!(matches!(
            ModelItem::decode_body(item, model_record),
            Err(ReplyError::Protocol("null query item"))
        ));
        let mut item = item_model(NativeQueryResultKind::Item, 0);
        item.paging = paging_model(vec![]);
        assert!(matches!(
            ModelItem::decode_body(item, model_record),
            Err(ReplyError::Protocol("query result is not an unpaged Item"))
        ));
    }

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
