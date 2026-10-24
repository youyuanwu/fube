//! Declarative external query framing with shared list/item reply profiles.

use std::marker::PhantomData;

use mssf_transport::{
    message::{Message, ACTION, ACTOR},
    serialization::{
        serde::{from_slice, to_vec, EmptyObjectArray, FixedTag, ObjectArray, Pointer, RawTagged},
        MAX_COLLECTION_COUNT,
    },
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{query_arguments::QueryArguments, Error, Paging, QueryPage, Result};

pub(crate) trait Query: Serialize {
    type Output: Send + 'static;
    type Reply: Reply<Output = Self::Output>;
    const NAME: &'static str;
    const PAGING_FIRST: bool = false;
    const SUPPORTS_MAX_RESULTS: bool = true;

    fn validate_parameters(&self) -> Result<()> {
        Ok(())
    }
    fn validate_record(
        record: <Self::Reply as Reply>::Record,
    ) -> Result<<Self::Reply as Reply>::Record> {
        Ok(record)
    }
}

pub(crate) fn encode_query<Q: Query>(query: &Q, paging: Option<&Paging>) -> Result<Vec<u8>> {
    let mut arguments = QueryArguments::default();
    if Q::PAGING_FIRST {
        append_paging::<Q>(&mut arguments, paging)?;
    }
    query.validate_parameters()?;
    arguments.append(query)?;
    if !Q::PAGING_FIRST {
        append_paging::<Q>(&mut arguments, paging)?;
    }
    encode(Q::NAME, &arguments.values)
}

pub(crate) fn decode_reply<Q: Query>(message: &Message) -> Result<Q::Output> {
    Q::Reply::decode(message, Q::validate_record)
}

fn append_paging<Q: Query>(arguments: &mut QueryArguments, paging: Option<&Paging>) -> Result<()> {
    let Some(paging) = paging else {
        return Ok(());
    };
    if paging.max_results.is_some() && !Q::SUPPORTS_MAX_RESULTS {
        return Err(Error::InvalidInput("query does not support MaxResults"));
    }
    if paging
        .max_results
        .is_some_and(|max| max.get() > i64::MAX as u64)
    {
        return Err(Error::InvalidInput(
            "MaxResults exceeds native signed range",
        ));
    }
    arguments.append(paging)
}

fn encode(name: &str, args: &[(String, String)]) -> Result<Vec<u8>> {
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
    if args.len() > MAX_COLLECTION_COUNT {
        return Err(Error::InvalidInput("object-array count limit exceeded"));
    }
    let pairs: Vec<_> = args
        .iter()
        .map(|(key, value)| Pair(Key { value: key }, value))
        .collect();
    to_vec(&Request(
        Arguments {
            values: ObjectArray(pairs),
        },
        name,
    ))
}

pub(crate) trait Reply {
    type Record: DeserializeOwned;
    type Output: Send + 'static;

    fn decode(
        message: &Message,
        validate: fn(Self::Record) -> Result<Self::Record>,
    ) -> Result<Self::Output>;
}

/// NamingGateway list aggregate with optional, checked actor/action headers.
pub(crate) struct ListReply<T, const TAG: i32, const STRICT_PAGING: bool = true>(PhantomData<T>);

impl<T: DeserializeOwned + Send + 'static, const TAG: i32, const STRICT_PAGING: bool> Reply
    for ListReply<T, TAG, STRICT_PAGING>
{
    type Record = T;
    type Output = QueryPage<T>;

    fn decode(message: &Message, validate: fn(T) -> Result<T>) -> Result<Self::Output> {
        crate::protocol::check_failure(message)?;
        let properties = message.properties()?;
        // Native list aggregates can omit actor/action; validate them if present.
        if message.headers.iter().any(|h| h.id == ACTOR) && properties.actor != 5 {
            return Err(Error::Protocol("list reply is not from NamingGateway"));
        }
        if message.headers.iter().any(|h| h.id == ACTION) && properties.action != "QueryReply" {
            return Err(Error::Protocol("unexpected list reply action"));
        }
        let result = decode::<FixedTag<ListPayload<T>, TAG>, _>(
            &message.body,
            STRICT_PAGING,
            |FixedTag(payload)| {
                if payload.item.0.is_some() {
                    return Err(Error::Protocol("list has a single-item payload"));
                }
                payload
                    .items
                    .0
                    .into_iter()
                    .map(validate)
                    .collect::<Result<Vec<_>>>()
            },
        )?;
        if result.kind != 2 {
            return Err(Error::Protocol("query result is not a List"));
        }
        Ok(QueryPage {
            items: result
                .payload
                .ok_or(Error::Protocol("list has no typed wrapper"))?,
            continuation_token: result.paging.filter(|s| !s.is_empty()),
        })
    }
}

/// Unpaged item with a fixed payload tag and required actor/OperationSuccessAction.
pub(crate) struct ItemReply<T, const TAG: i32, const ACTOR_ID: i64>(PhantomData<T>);

impl<T: DeserializeOwned + Send + 'static, const TAG: i32, const ACTOR_ID: i64> Reply
    for ItemReply<T, TAG, ACTOR_ID>
{
    type Record = T;
    type Output = T;

    fn decode(message: &Message, validate: fn(T) -> Result<T>) -> Result<T> {
        crate::protocol::check_failure(message)?;
        let properties = message.properties()?;
        if !message.headers.iter().any(|header| header.id == ACTOR)
            || properties.actor != ACTOR_ID
            || properties.action != "OperationSuccessAction"
        {
            return Err(Error::Protocol("unexpected item success envelope"));
        }
        let result = decode::<FixedTag<ItemPayload<T>, TAG>, _>(
            &message.body,
            false,
            |FixedTag(payload)| validate(payload.item.0.ok_or(Error::Protocol("null query item"))?),
        )?;
        if result.kind != 1 || result.paging.is_some() {
            return Err(Error::Protocol("query result is not an unpaged Item"));
        }
        result
            .payload
            .ok_or(Error::Protocol("item has no typed wrapper"))
    }
}

#[derive(Deserialize)]
struct Empty {}

#[derive(Deserialize)]
struct ListPayload<T> {
    items: ObjectArray<Vec<T>>,
    item: Pointer<Empty>,
}

#[derive(Deserialize)]
struct ItemPayload<T> {
    _list: EmptyObjectArray,
    item: Pointer<T>,
}

#[derive(Deserialize)]
struct PagingStatus {
    token: String,
}

#[derive(Deserialize)]
struct QueryResultEnvelope<T> {
    kind: i64,
    payload: Pointer<T>,
    error: i64,
    paging: Pointer<RawTagged<PagingStatus>>,
}

struct QueryResult<T> {
    kind: i64,
    payload: Option<T>,
    paging: Option<String>,
}

fn decode<T: DeserializeOwned, U>(
    body: &[u8],
    strict_paging: bool,
    validate: impl FnOnce(T) -> Result<U>,
) -> Result<QueryResult<U>> {
    let envelope: QueryResultEnvelope<T> = from_slice(body)?;
    // A native query error must not hide a malformed payload and allow reuse
    // of a connection that previously failed protocol validation.
    let payload = envelope.payload.0.map(validate).transpose()?;
    let paging = envelope
        .paging
        .0
        .map(|status| {
            if strict_paging && !status.type_info.is_empty() {
                return Err(Error::Protocol("unexpected paging status type"));
            }
            Ok(status.value.token)
        })
        .transpose()?;
    if envelope.error != 0 {
        return Err(Error::Remote {
            code: envelope.error,
            message: String::new(),
        });
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
    use mssf_transport::{message::Header, serialization::Encoder};
    use std::num::NonZeroU64;

    #[derive(Debug, PartialEq, Deserialize)]
    struct ReplyRecord {
        value: String,
    }

    #[derive(Serialize)]
    struct DeclaredListQuery {}

    impl Query for DeclaredListQuery {
        type Output = QueryPage<ReplyRecord>;
        type Reply = ListReply<ReplyRecord, 23>;
        const NAME: &'static str = "DeclaredList";

        fn validate_record(record: ReplyRecord) -> Result<ReplyRecord> {
            if record.value == "invalid" {
                return Err(Error::Protocol("invalid fixture record"));
            }
            Ok(record)
        }
    }

    #[derive(Serialize)]
    struct DeclaredItemQuery {}

    impl Query for DeclaredItemQuery {
        type Output = ReplyRecord;
        type Reply = ItemReply<ReplyRecord, 37, 99>;
        const NAME: &'static str = "DeclaredItem";
    }

    #[test]
    fn shared_encoder_validates_before_serializing_only_the_parameter_struct() {
        use serde::ser::SerializeStruct;
        use std::cell::Cell;

        struct Parameters {
            serializations: Cell<usize>,
            invalid: bool,
        }
        impl Serialize for Parameters {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                self.serializations.set(self.serializations.get() + 1);
                let mut fields = serializer.serialize_struct("Parameters", 1)?;
                fields.serialize_field("Argument", "value")?;
                fields.end()
            }
        }

        impl Query for Parameters {
            type Output = QueryPage<ReplyRecord>;
            type Reply = ListReply<ReplyRecord, 23>;
            const NAME: &'static str = "CheckedQuery";

            fn validate_parameters(&self) -> Result<()> {
                if self.invalid {
                    return Err(Error::InvalidInput("invalid fixture parameters"));
                }
                Ok(())
            }
        }

        let mut query = QueryDescription::new(Parameters {
            serializations: Cell::new(0),
            invalid: true,
        });
        assert!(matches!(
            encode_query(&query.parameters, None),
            Err(Error::InvalidInput("invalid fixture parameters"))
        ));
        assert_eq!(query.parameters.serializations.get(), 0);
        query.parameters.invalid = false;
        assert_native_bytes(
            &query.parameters,
            None,
            "CheckedQuery",
            &[("Argument", "value")],
        );
        assert_eq!(query.parameters.serializations.get(), 1);
    }

    fn reply_fixture(
        kind: i64,
        tag: Option<i32>,
        values: &[&str],
        item: Option<&str>,
        error: i64,
        paging: Option<(&[u8], &str)>,
    ) -> Message {
        let mut encoder = Encoder::new();
        encoder
            .object(|e| {
                e.i64(kind);
                let tag = tag.map(i32::to_le_bytes);
                e.pointer(tag.as_ref().map(|tag| tag.as_slice()), |e| {
                    e.object_array(values, |e, value| e.string(value))?;
                    e.pointer(item.map(|_| &[][..]), |e| e.string(item.unwrap()))
                })?;
                e.i64(error);
                e.pointer(paging.map(|(tag, _)| tag), |e| e.string(paging.unwrap().1))
            })
            .unwrap();
        Message {
            headers: Vec::new(),
            body: encoder.finish(),
        }
    }

    fn item_envelope(mut message: Message) -> Message {
        message.headers = vec![
            Header::actor(99).unwrap(),
            Header::action("OperationSuccessAction").unwrap(),
        ];
        message
    }

    #[test]
    fn new_reply_schemas_need_only_derived_records_and_profile_declarations() {
        assert_native_bytes(&DeclaredListQuery {}, None, "DeclaredList", &[]);
        assert_native_bytes(&DeclaredItemQuery {}, None, "DeclaredItem", &[]);
        let page = decode_reply::<DeclaredListQuery>(&reply_fixture(
            2,
            Some(23),
            &["first", "second"],
            None,
            0,
            Some((&[], "opaque-next")),
        ))
        .unwrap();
        assert_eq!(
            page.items,
            ["first", "second"].map(|value| ReplyRecord {
                value: value.into()
            })
        );
        assert_eq!(page.continuation_token.as_deref(), Some("opaque-next"));
        let item = decode_reply::<DeclaredItemQuery>(&item_envelope(reply_fixture(
            1,
            Some(37),
            &[],
            Some("only"),
            0,
            None,
        )))
        .unwrap();
        assert_eq!(item.value, "only");
    }

    #[test]
    fn reply_profiles_preserve_optional_list_and_required_item_headers() {
        for headers in [
            vec![],
            vec![Header::actor(5).unwrap()],
            vec![Header::action("QueryReply").unwrap()],
            vec![
                Header::actor(5).unwrap(),
                Header::action("QueryReply").unwrap(),
            ],
        ] {
            let mut message = reply_fixture(2, Some(23), &[], None, 0, None);
            message.headers = headers;
            decode_reply::<DeclaredListQuery>(&message).unwrap();
        }
        for header in [Header::actor(0).unwrap(), Header::action("wrong").unwrap()] {
            let mut message = reply_fixture(2, Some(23), &[], None, 0, None);
            message.headers.push(header);
            assert!(matches!(
                decode_reply::<DeclaredListQuery>(&message),
                Err(Error::Protocol(_))
            ));
        }
        for headers in [
            vec![],
            vec![Header::actor(99).unwrap()],
            vec![Header::action("OperationSuccessAction").unwrap()],
            vec![
                Header::actor(37).unwrap(),
                Header::action("OperationSuccessAction").unwrap(),
            ],
            vec![Header::actor(99).unwrap(), Header::action("wrong").unwrap()],
        ] {
            let mut message = reply_fixture(1, Some(37), &[], Some("only"), 0, None);
            message.headers = headers;
            assert!(matches!(
                decode_reply::<DeclaredItemQuery>(&message),
                Err(Error::Protocol(_))
            ));
        }
        let mut message = reply_fixture(1, Some(37), &[], Some("only"), 0, None);
        message.headers = vec![Header::action("OperationSuccessAction").unwrap()];
        assert!(matches!(
            ItemReply::<ReplyRecord, 37, 0>::decode(&message, Ok),
            Err(Error::Protocol(_))
        ));
        message.headers.push(Header::actor(0).unwrap());
        ItemReply::<ReplyRecord, 37, 0>::decode(&message, Ok).unwrap();
    }

    #[test]
    fn reply_profiles_reject_wrong_kinds_tags_slots_and_item_paging() {
        for message in [
            reply_fixture(1, Some(23), &[], None, 0, None),
            reply_fixture(2, None, &[], None, 0, None),
            reply_fixture(2, Some(24), &[], None, 0, None),
            reply_fixture(2, Some(23), &[], Some("unexpected"), 0, None),
        ] {
            assert!(matches!(
                decode_reply::<DeclaredListQuery>(&message),
                Err(Error::Protocol(_))
            ));
        }
        for message in [
            reply_fixture(2, Some(37), &[], Some("only"), 0, None),
            reply_fixture(1, None, &[], None, 0, None),
            reply_fixture(1, Some(38), &[], Some("only"), 0, None),
            reply_fixture(1, Some(37), &["unexpected"], Some("only"), 0, None),
            reply_fixture(1, Some(37), &[], None, 0, None),
            reply_fixture(1, Some(37), &[], Some("only"), 0, Some((&[], ""))),
            reply_fixture(1, Some(37), &[], Some("only"), 0, Some((b"typed", ""))),
        ] {
            assert!(matches!(
                decode_reply::<DeclaredItemQuery>(&item_envelope(message)),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn shared_reply_validation_precedes_embedded_errors_without_rejecting_null_error_payloads() {
        const REMOTE: i64 = -123;
        for message in [
            reply_fixture(2, Some(23), &["invalid"], None, REMOTE, None),
            reply_fixture(2, Some(23), &[], Some("unexpected"), REMOTE, None),
            reply_fixture(2, Some(23), &[], None, REMOTE, Some((b"typed", ""))),
        ] {
            assert!(matches!(
                decode_reply::<DeclaredListQuery>(&message),
                Err(Error::Protocol(_))
            ));
        }
        for message in [
            reply_fixture(1, Some(37), &[], Some("invalid"), REMOTE, None),
            reply_fixture(1, Some(37), &[], None, REMOTE, None),
            reply_fixture(1, Some(37), &["unexpected"], Some("only"), REMOTE, None),
        ] {
            assert!(matches!(
                ItemReply::<ReplyRecord, 37, 99>::decode(
                    &item_envelope(message),
                    DeclaredListQuery::validate_record,
                ),
                Err(Error::Protocol(_))
            ));
        }
        for tag in [None, Some(23)] {
            let message = reply_fixture(0, tag, &[], None, REMOTE, None);
            assert!(matches!(
                decode_reply::<DeclaredListQuery>(&message),
                Err(Error::Remote { code: REMOTE, .. })
            ));
        }
        for tag in [None, Some(37)] {
            let message = item_envelope(reply_fixture(
                0,
                tag,
                &[],
                Some("only"),
                REMOTE,
                Some((b"typed", "")),
            ));
            assert!(matches!(
                decode_reply::<DeclaredItemQuery>(&message),
                Err(Error::Remote { code: REMOTE, .. })
            ));
        }
    }

    #[test]
    fn shared_reply_profiles_reject_truncation_and_trailing_bytes() {
        let mut list = reply_fixture(2, Some(23), &["first"], None, 0, Some((&[], "next")));
        let mut item = item_envelope(reply_fixture(1, Some(37), &[], Some("only"), 0, None));
        for end in 0..list.body.len() {
            let mut truncated = list.clone();
            truncated.body.truncate(end);
            assert!(matches!(
                decode_reply::<DeclaredListQuery>(&truncated),
                Err(Error::Protocol(_))
            ));
        }
        for end in 0..item.body.len() {
            let mut truncated = item.clone();
            truncated.body.truncate(end);
            assert!(matches!(
                decode_reply::<DeclaredItemQuery>(&truncated),
                Err(Error::Protocol(_))
            ));
        }
        list.body.push(0);
        item.body.push(0);
        assert!(decode_reply::<DeclaredListQuery>(&list).is_err());
        assert!(decode_reply::<DeclaredItemQuery>(&item).is_err());
    }

    fn assert_native_bytes(
        query: &impl Query,
        paging: Option<&Paging>,
        name: &str,
        arguments: &[(&str, &str)],
    ) {
        let mut old = Encoder::new();
        old.object(|e| {
            e.object(|e| {
                e.object_array(arguments, |e, (key, value)| {
                    e.object(|e| e.string(key))?;
                    e.string(value)
                })
            })?;
            e.string(name)
        })
        .unwrap();
        assert_eq!(encode_query(query, paging).unwrap(), old.finish());
    }

    fn paging() -> Paging {
        Paging {
            continuation_token: Some(" opaque+/\u{1f980} ".into()),
            max_results: NonZeroU64::new(i64::MAX as u64),
        }
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

    #[test]
    fn node_parameters_and_shared_paging_match_the_previous_wire_layout() {
        assert_native_bytes(&NodeQueryParameters::default(), None, "GetNodeList", &[]);
        assert_native_bytes(
            &NodeQueryParameters {
                node_name: Some(String::new()),
                ..Default::default()
            },
            None,
            "GetNodeList",
            &[],
        );
        let query = NodeQueryDescription {
            parameters: NodeQueryParameters {
                node_name: Some("Exact_Node".into()),
                status_filter: NodeStatusFilter::from_bits_retain(u32::MAX),
            },
            paging: paging(),
        };
        assert_native_bytes(
            &query.parameters,
            Some(&query.paging),
            "GetNodeList",
            &[
                ("ContinuationToken", " opaque+/\u{1f980} "),
                ("MaxResults", "9223372036854775807"),
                ("NodeName", "Exact_Node"),
                ("NodeStatusFilter", "4294967295"),
            ],
        );
    }

    #[test]
    fn every_application_selector_preserves_order_numeric_flags_and_omission() {
        assert_native_bytes(
            &ApplicationQueryParameters::default(),
            None,
            "GetApplicationList",
            &[],
        );
        for (filter, argument) in [
            (ApplicationListFilter::All, None),
            (
                ApplicationListFilter::Name("fabric:/App".parse().unwrap()),
                Some(("ApplicationName", "fabric:/App")),
            ),
            (
                ApplicationListFilter::TypeName("ExactType".into()),
                Some(("ApplicationTypeName", "ExactType")),
            ),
            (
                ApplicationListFilter::DefinitionKind(ApplicationDefinitionKindFilter::ALL),
                Some(("ApplicationDefinitionKindFilter", "65535")),
            ),
            (
                ApplicationListFilter::DefinitionKind(
                    ApplicationDefinitionKindFilter::COMPOSE
                        | ApplicationDefinitionKindFilter::MESH_APPLICATION_DESCRIPTION,
                ),
                Some(("ApplicationDefinitionKindFilter", "6")),
            ),
            (
                ApplicationListFilter::DefinitionKind(ApplicationDefinitionKindFilter::DEFAULT),
                None,
            ),
        ] {
            let query = ApplicationQueryDescription {
                parameters: ApplicationQueryParameters {
                    filter,
                    exclude_application_parameters: true,
                },
                paging: paging(),
            };
            let mut expected = Vec::from_iter(argument);
            expected.extend([
                ("ExcludeApplicationParameters", "true"),
                ("ContinuationToken", " opaque+/\u{1f980} "),
                ("MaxResults", "9223372036854775807"),
            ]);
            assert_native_bytes(
                &query.parameters,
                Some(&query.paging),
                "GetApplicationList",
                &expected,
            );
        }
    }

    #[test]
    fn every_service_selector_preserves_application_scope_and_paging_order() {
        for (filter, argument) in [
            (ServiceListFilter::All, None),
            (
                ServiceListFilter::Name("fabric:/App/Service".parse().unwrap()),
                Some(("ServiceName", "fabric:/App/Service")),
            ),
            (
                ServiceListFilter::TypeName("ExactType".into()),
                Some(("ServiceTypeName", "ExactType")),
            ),
        ] {
            let query = ServiceQueryDescription {
                parameters: ServiceQueryParameters {
                    application_name: "fabric:/App".parse().unwrap(),
                    filter,
                },
                paging: paging(),
            };
            let mut expected = vec![("ApplicationName", "fabric:/App")];
            expected.extend(argument);
            expected.extend([
                ("ContinuationToken", " opaque+/\u{1f980} "),
                ("MaxResults", "9223372036854775807"),
            ]);
            assert_native_bytes(
                &query.parameters,
                Some(&query.paging),
                "GetApplicationServiceList",
                &expected,
            );
        }
    }

    #[test]
    fn health_json_stays_embedded_numeric_and_distinguishes_absent_from_false() {
        assert_native_bytes(
            &NodeHealthQueryParameters::new("Node"),
            None,
            "GetNodeHealth",
            &[("NodeName", "Node")],
        );
        for (policy, filter, json) in [
            (
                false,
                HealthEventsFilter::DEFAULT,
                r#"{"HealthStateFilter":0}"#,
            ),
            (true, HealthEventsFilter::NONE, r#"{"HealthStateFilter":1}"#),
            (
                false,
                HealthEventsFilter::ALL,
                r#"{"HealthStateFilter":65535}"#,
            ),
            (
                true,
                HealthEventsFilter::from_bits_retain(u32::MAX),
                r#"{"HealthStateFilter":4294967295}"#,
            ),
        ] {
            let query = NodeHealthQueryParameters {
                node_name: "Node".into(),
                health_policy: Some(NodeHealthPolicy {
                    consider_warning_as_error: policy,
                }),
                events_filter: Some(filter.into()),
            };
            assert_native_bytes(
                &query,
                None,
                "GetNodeHealth",
                &[
                    (
                        "ClusterHealthPolicy",
                        if policy {
                            r#"{"ConsiderWarningAsError":true}"#
                        } else {
                            r#"{"ConsiderWarningAsError":false}"#
                        },
                    ),
                    ("EventsFilter", json),
                    ("NodeName", "Node"),
                ],
            );
        }
        assert_native_bytes(
            &NodeHealthQueryParameters {
                node_name: "Node".into(),
                health_policy: Some(NodeHealthPolicy::default()),
                events_filter: None,
            },
            None,
            "GetNodeHealth",
            &[
                ("ClusterHealthPolicy", r#"{"ConsiderWarningAsError":false}"#),
                ("NodeName", "Node"),
            ],
        );
        assert_native_bytes(
            &NodeHealthQueryParameters {
                node_name: "Node".into(),
                health_policy: None,
                events_filter: Some(HealthEventsFilter::DEFAULT.into()),
            },
            None,
            "GetNodeHealth",
            &[
                ("EventsFilter", r#"{"HealthStateFilter":0}"#),
                ("NodeName", "Node"),
            ],
        );
    }

    #[test]
    fn all_list_operations_share_signed_limits_and_bounded_opaque_tokens() {
        fn check<P: Query>(parameters: P) {
            let mut query = PagedQuery::new(parameters);
            let baseline = encode_query(&query.parameters, Some(&query.paging)).unwrap();
            query.paging.continuation_token = Some(String::new());
            assert_eq!(
                encode_query(&query.parameters, Some(&query.paging)).unwrap(),
                baseline
            );
            for limit in [i64::MAX as u64 + 1, u64::MAX] {
                query.paging.max_results = NonZeroU64::new(limit);
                assert!(matches!(
                    encode_query(&query.parameters, Some(&query.paging)),
                    Err(Error::InvalidInput(_))
                ));
            }
            query.paging.max_results = NonZeroU64::new(i64::MAX as u64);
            let token = "\u{1f980}".repeat(16_384);
            query.paging.continuation_token = Some(token.clone());
            assert!(encode_query(&query.parameters, Some(&query.paging)).is_ok());
            query.paging.continuation_token = Some(token + "x");
            assert!(matches!(
                encode_query(&query.parameters, Some(&query.paging)),
                Err(Error::InvalidInput(_))
            ));
        }
        check(NodeQueryParameters::default());
        check(ApplicationQueryParameters::default());
        check(ServiceQueryParameters::new("fabric:/App".parse().unwrap()));
    }

    #[test]
    fn shared_page_decoder_keeps_operation_specific_paging_tag_policy() {
        fn reply(wrapper: i32, tag: &[u8], token: &str) -> Message {
            let mut encoder = Encoder::new();
            encoder
                .object(|e| {
                    e.i64(2);
                    e.pointer(Some(&wrapper.to_le_bytes()), |e| {
                        e.object_array::<String>(&[], |e, value| e.string(value))?;
                        e.pointer(None, |_| Ok(()))
                    })?;
                    e.i64(0);
                    e.pointer(Some(tag), |e| e.string(token))
                })
                .unwrap();
            Message {
                headers: Vec::new(),
                body: encoder.finish(),
            }
        }
        for token in ["opaque-next", ""] {
            let expected = (!token.is_empty()).then(|| token.to_owned());
            for tag in [b"".as_slice(), b"typed".as_slice()] {
                let nodes = decode_reply::<NodeQueryParameters>(&reply(1, tag, token)).unwrap();
                assert!(nodes.items.is_empty());
                assert_eq!(nodes.continuation_token, expected);
                let apps = decode_reply::<ApplicationQueryParameters>(&reply(4, tag, token));
                let services = decode_reply::<ServiceQueryParameters>(&reply(2, tag, token));
                if tag.is_empty() {
                    assert_eq!(apps.unwrap().continuation_token, expected);
                    assert_eq!(services.unwrap().continuation_token, expected);
                } else {
                    assert!(matches!(apps, Err(Error::Protocol(_))));
                    assert!(matches!(services, Err(Error::Protocol(_))));
                }
            }
        }
    }

    #[test]
    fn json_deserialization_cannot_bypass_application_filter_validation() {
        for bits in [8, 9, 65534, 65536, u32::MAX] {
            let query = ApplicationQueryDescription::new(ApplicationQueryParameters {
                filter: ApplicationListFilter::DefinitionKind(
                    ApplicationDefinitionKindFilter::from_bits_retain(bits),
                ),
                ..Default::default()
            });
            let query: ApplicationQueryDescription =
                serde_json::from_str(&serde_json::to_string(&query).unwrap()).unwrap();
            assert!(matches!(
                encode_query(&query.parameters, Some(&query.paging)),
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[test]
    fn derived_query_bytes_match_old_order_and_nesting_including_empty_arguments() {
        for args in [
            Vec::<(String, String)>::new(),
            vec![
                ("z".into(), "".into()),
                ("a".into(), "🦀é".into()),
                ("z".into(), "duplicate".into()),
            ],
        ] {
            let mut old = Encoder::new();
            old.object(|e| {
                e.object(|e| {
                    e.object_array(&args, |e, (key, value)| {
                        e.object(|e| e.string(key))?;
                        e.string(value)
                    })
                })?;
                e.string("Get🦀")
            })
            .unwrap();
            assert_eq!(encode("Get🦀", &args).unwrap(), old.finish());
        }
        let too_many = vec![("key".into(), "value".into()); MAX_COLLECTION_COUNT + 1];
        assert!(matches!(
            encode("Query", &too_many),
            Err(Error::InvalidInput(_))
        ));
    }
}
