//! Bounded external query framing; payload and success policy stay operation-specific.

use mssf_transport::{
    message::{Message, ACTION, ACTOR},
    serialization::{
        serde::{to_vec, ObjectArray},
        Decoder, MAX_COLLECTION_COUNT,
    },
};
use serde::Serialize;

use crate::{query_arguments::QueryArguments, Error, Paging, QueryPage, Result};

pub(crate) trait Query {
    type Output: Send + 'static;
    const NAME: &'static str;
    const PAGING_FIRST: bool = false;

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()>;
    fn decode_reply(message: &Message) -> Result<Self::Output>;
    fn paging(&self) -> Option<&Paging> {
        None
    }
}

pub(crate) fn encode_query<Q: Query>(query: &Q) -> Result<Vec<u8>> {
    let mut arguments = QueryArguments::default();
    if Q::PAGING_FIRST {
        append_paging(&mut arguments, query.paging())?;
    }
    query.encode_parameters(&mut arguments)?;
    if !Q::PAGING_FIRST {
        append_paging(&mut arguments, query.paging())?;
    }
    encode(Q::NAME, &arguments.values)
}

fn append_paging(arguments: &mut QueryArguments, paging: Option<&Paging>) -> Result<()> {
    #[derive(Serialize)]
    #[serde(rename_all = "PascalCase")]
    struct Fields<'a> {
        continuation_token: Option<&'a str>,
        max_results: Option<u64>,
    }
    let Some(paging) = paging else {
        return Ok(());
    };
    if paging
        .max_results
        .is_some_and(|max| max.get() > i64::MAX as u64)
    {
        return Err(Error::InvalidInput(
            "MaxResults exceeds native signed range",
        ));
    }
    arguments.append(&Fields {
        continuation_token: paging
            .continuation_token
            .as_deref()
            .filter(|s| !s.is_empty()),
        max_results: paging.max_results.map(|max| max.get()),
    })
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

pub(crate) fn decode_list<T>(
    message: &Message,
    wrapper: i32,
    strict_paging: bool,
    record: fn(&mut Decoder<'_>) -> Result<T>,
) -> Result<QueryPage<T>> {
    crate::protocol::check_failure(message)?;
    let properties = message.properties()?;
    // Native list aggregates can omit actor/action; validate them if present.
    if message.headers.iter().any(|h| h.id == ACTOR) && properties.actor != 5 {
        return Err(Error::Protocol("list reply is not from NamingGateway"));
    }
    if message.headers.iter().any(|h| h.id == ACTION) && properties.action != "QueryReply" {
        return Err(Error::Protocol("unexpected list reply action"));
    }
    let result = decode_with_paging_policy(
        &message.body,
        |decoder, tag| {
            if tag != wrapper.to_le_bytes() {
                return Err(Error::Protocol("unexpected native list wrapper type"));
            }
            let items = decoder.object_array(record)?;
            if decoder.pointer(|_, _| Ok(()))?.is_some() {
                return Err(Error::Protocol("list has a single-item payload"));
            }
            Ok(items)
        },
        strict_paging,
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

pub(crate) struct QueryResult<T> {
    pub kind: i64,
    pub payload: Option<T>,
    pub paging: Option<String>,
}

pub(crate) fn decode<T>(
    body: &[u8],
    payload: impl FnOnce(&mut Decoder<'_>, &[u8]) -> Result<T>,
) -> Result<QueryResult<T>> {
    decode_with_paging_policy(body, payload, false)
}

pub(crate) fn decode_with_paging_policy<T>(
    body: &[u8],
    payload: impl FnOnce(&mut Decoder<'_>, &[u8]) -> Result<T>,
    strict_paging: bool,
) -> Result<QueryResult<T>> {
    let mut decoder = Decoder::new(body);
    let (kind, payload, error, paging) = decoder.object(|decoder| {
        Ok((
            decoder.i64()?,
            decoder.pointer(payload)?,
            decoder.i64()?,
            decoder.pointer(|decoder, tag| {
                if strict_paging && !tag.is_empty() {
                    return Err(Error::Protocol("unexpected paging status type"));
                }

                decoder.string()
            })?,
        ))
    })?;
    decoder.finish()?;
    if error != 0 {
        return Err(Error::Remote {
            code: error,
            message: String::new(),
        });
    }
    Ok(QueryResult {
        kind,
        payload,
        paging,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use mssf_transport::serialization::Encoder;
    use std::num::NonZeroU64;

    fn assert_native_bytes(query: &impl Query, name: &str, arguments: &[(&str, &str)]) {
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
        assert_eq!(encode_query(query).unwrap(), old.finish());
    }

    fn paging() -> Paging {
        Paging {
            continuation_token: Some(" opaque+/\u{1f980} ".into()),
            max_results: NonZeroU64::new(i64::MAX as u64),
        }
    }

    #[test]
    fn node_parameters_and_shared_paging_match_the_previous_wire_layout() {
        assert_native_bytes(&NodeQueryDescription::default(), "GetNodeList", &[]);
        let query = NodeQueryDescription {
            parameters: NodeQueryParameters {
                node_name: Some("Exact_Node".into()),
                status_filter: NodeStatusFilter::from_bits_retain(u32::MAX),
            },
            paging: paging(),
        };
        assert_native_bytes(
            &query,
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
            &ApplicationQueryDescription::default(),
            "GetApplicationList",
            &[],
        );
        for (filter, argument) in [
            (ApplicationListFilter::All, None),
            (
                ApplicationListFilter::Name("fabric:/App".into()),
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
            assert_native_bytes(&query, "GetApplicationList", &expected);
        }
    }

    #[test]
    fn every_service_selector_preserves_application_scope_and_paging_order() {
        for (filter, argument) in [
            (ServiceListFilter::All, None),
            (
                ServiceListFilter::Name("fabric:/App/Service".into()),
                Some(("ServiceName", "fabric:/App/Service")),
            ),
            (
                ServiceListFilter::TypeName("ExactType".into()),
                Some(("ServiceTypeName", "ExactType")),
            ),
        ] {
            let query = ServiceQueryDescription {
                parameters: ServiceQueryParameters {
                    application_name: "fabric:/App".into(),
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
            assert_native_bytes(&query, "GetApplicationServiceList", &expected);
        }
    }

    #[test]
    fn health_json_stays_embedded_numeric_and_distinguishes_absent_from_false() {
        assert_native_bytes(
            &NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("Node")),
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
            let query = NodeHealthQueryDescription::new(NodeHealthQueryParameters {
                node_name: "Node".into(),
                health_policy: Some(NodeHealthPolicy {
                    consider_warning_as_error: policy,
                }),
                events_filter: Some(filter),
            });
            assert_native_bytes(
                &query,
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
    }

    #[test]
    fn all_list_operations_share_signed_limits_and_bounded_opaque_tokens() {
        fn check<P>(parameters: P)
        where
            PagedQuery<P>: Query,
        {
            let mut query = PagedQuery::new(parameters);
            let baseline = encode_query(&query).unwrap();
            query.paging.continuation_token = Some(String::new());
            assert_eq!(encode_query(&query).unwrap(), baseline);
            for limit in [i64::MAX as u64 + 1, u64::MAX] {
                query.paging.max_results = NonZeroU64::new(limit);
                assert!(matches!(encode_query(&query), Err(Error::InvalidInput(_))));
            }
            query.paging.max_results = NonZeroU64::new(i64::MAX as u64);
            let token = "\u{1f980}".repeat(16_384);
            query.paging.continuation_token = Some(token.clone());
            assert!(encode_query(&query).is_ok());
            query.paging.continuation_token = Some(token + "x");
            assert!(matches!(encode_query(&query), Err(Error::InvalidInput(_))));
        }
        check(NodeQueryParameters::default());
        check(ApplicationQueryParameters::default());
        check(ServiceQueryParameters::new("fabric:/App"));
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
                let nodes = NodeQueryDescription::decode_reply(&reply(1, tag, token)).unwrap();
                assert!(nodes.items.is_empty());
                assert_eq!(nodes.continuation_token, expected);
                let apps = ApplicationQueryDescription::decode_reply(&reply(4, tag, token));
                let services = ServiceQueryDescription::decode_reply(&reply(2, tag, token));
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
            assert!(matches!(encode_query(&query), Err(Error::InvalidInput(_))));
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
