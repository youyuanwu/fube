use super::{
    methods::{envelope, query_request},
    *,
};
use crate::{
    query_protocol::{ItemReply, ListReply, UnpagedListReply},
    NodeStatusFilter, PagedQuery, Paging, QueryDescription, QueryPage,
};
use std::num::NonZeroU64;

#[derive(Debug, Deserialize)]
struct Record {
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct ListQuery {
    filter: String,
    #[serde(with = "crate::query_arguments::flags")]
    mask: NodeStatusFilter,
    #[serde(with = "crate::query_arguments::json")]
    options: Vec<String>,
}

impl Query for ListQuery {
    type Output = QueryPage<Record>;
    type Reply = ListReply<Record, 23>;
    const NAME: &'static str = "DeclaredList";

    fn validate_record(record: Record) -> std::result::Result<Record, &'static str> {
        if record.value == "invalid" {
            return Err("invalid query record");
        }
        Ok(record)
    }
}

fn list_query() -> PagedQuery<ListQuery> {
    PagedQuery {
        parameters: ListQuery {
            filter: "exact".into(),
            mask: NodeStatusFilter::from_bits_retain(u32::MAX),
            options: vec!["quote\"".into()],
        },
        paging: Paging {
            continuation_token: Some("opaque".into()),
            max_results: NonZeroU64::new(2),
        },
    }
}

#[derive(Serialize)]
struct ItemQuery {}

impl Query for ItemQuery {
    type Output = Record;
    type Reply = ItemReply<Record, 37, 16>;
    const NAME: &'static str = "DeclaredItem";
}

#[derive(Serialize)]
struct UnpagedQuery {}

impl Query for UnpagedQuery {
    type Output = Vec<Record>;
    type Reply = UnpagedListReply<Record, 23, 13>;
    const NAME: &'static str = "DeclaredUnpagedList";
    const SUPPORTS_MAX_RESULTS: bool = false;
}

fn reply(
    kind: i64,
    tag: Option<i32>,
    values: &[&str],
    item: Option<&str>,
    error: i64,
    paging: Option<(&[u8], &str)>,
) -> Body {
    let mut encoder = fubst::Encoder::new();
    encoder
        .object(|encoder| {
            encoder.i64(kind)?;
            let tag = tag.map(i32::to_le_bytes);
            encoder.pointer(tag.as_ref().map(|tag| tag.as_slice()), |encoder| {
                encoder.object_array(values, |encoder, value| encoder.string(value))?;
                encoder.pointer(item.map(|_| &[][..]), |encoder| {
                    encoder.string(item.unwrap())
                })
            })?;
            encoder.i64(error)?;
            encoder.pointer(paging.map(|(tag, _)| tag), |encoder| {
                encoder.string(paging.unwrap().1)
            })
        })
        .unwrap();
    Body::from_bytes(encoder.finish()).unwrap()
}

async fn call<Q: QueryRequest>(
    query: Q,
    response: Response<Body>,
) -> Result<Response<<Q::Parameters as Query>::Output>> {
    let mut response = Some(response);
    let mut client = FurpcClient::new(service(move |_| {
        ready(Ok::<_, furpc::Error>(response.take().unwrap()))
    }));
    client.query(Request::new(query), deadline()).await
}

#[tokio::test]
async fn generic_queries_share_framing_but_select_typed_reply_profiles() {
    let mut client = FurpcClient::new(service(|request: Request<Body>| {
        assert!(request
            .headers()
            .effective()
            .action()
            .iter()
            .copied()
            .eq("QueryRequest".encode_utf16()));
        let (name, arguments) = query_request(request.body());
        let response = match name.as_str() {
            "DeclaredList" => {
                assert_eq!(
                    arguments,
                    [
                        ("Filter", "exact"),
                        ("Mask", "4294967295"),
                        ("Options", "[\"quote\\\"\"]"),
                        ("ContinuationToken", "opaque"),
                        ("MaxResults", "2"),
                    ]
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                );
                envelope(
                    None,
                    None,
                    reply(
                        2,
                        Some(23),
                        &["first", "second"],
                        None,
                        0,
                        Some((&[], "next")),
                    ),
                )
            }
            "DeclaredItem" => {
                assert!(arguments.is_empty());
                envelope(
                    Some(16),
                    Some("OperationSuccessAction"),
                    reply(1, Some(37), &[], Some("only"), 0, None),
                )
            }
            "DeclaredUnpagedList" => {
                assert!(arguments.is_empty());
                envelope(
                    Some(13),
                    Some("QueryReply"),
                    reply(2, Some(23), &["local"], None, 0, None),
                )
            }
            "MovePrimary" => {
                assert_eq!(arguments[0], ("NodeName".into(), "N0020".into()));
                envelope(
                    Some(13),
                    Some("QueryReply"),
                    reply(0, None, &[], None, 0, None),
                )
            }
            other => panic!("unexpected query {other}"),
        };
        ready(Ok::<_, furpc::Error>(response))
    }));
    let page = client
        .query(Request::new(list_query()), deadline())
        .await
        .unwrap()
        .into_body();
    assert_eq!(
        page.items
            .iter()
            .map(|record| record.value.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(page.continuation_token.as_deref(), Some("next"));

    let item = client
        .query(
            Request::new(QueryDescription::new(ItemQuery {})),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(item.body().value, "only");
    assert_eq!(item.headers().effective().actor.get(), 16);
    let records = client
        .query(
            Request::new(QueryDescription::new(UnpagedQuery {})),
            deadline(),
        )
        .await
        .unwrap()
        .into_body();
    assert_eq!(records[0].value, "local");
    client
        .query(
            Request::new(QueryDescription::new(crate::movement::MovePrimary {
                target: "N0020".into(),
                partition: uuid::Uuid::from_u128(1),
            })),
            deadline(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn query_envelopes_are_profile_specific_and_precede_body_decoding() {
    for (actor, action) in [
        (None, None),
        (Some(16), None),
        (None, Some("OperationSuccessAction")),
        (Some(5), Some("OperationSuccessAction")),
        (Some(16), Some("QueryReply")),
    ] {
        let error = call(
            QueryDescription::new(ItemQuery {}),
            envelope(actor, action, Body::from_bytes(vec![0xff]).unwrap()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, FurpcClientError::Protocol(_)));
    }
    let error = call(
        QueryDescription::new(UnpagedQuery {}),
        envelope(
            Some(5),
            Some("QueryReply"),
            Body::from_bytes(vec![0xff]).unwrap(),
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, FurpcClientError::Protocol(_)));

    #[derive(Serialize)]
    struct Failure(i64, String);
    let error = call(
        QueryDescription::new(ItemQuery {}),
        envelope(
            Some(5),
            Some("ClientOperationFailure"),
            Body::from_bytes(
                fubst::serde::to_vec(&Failure(-123, "native diagnostic".into())).unwrap(),
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        FurpcClientError::Domain(DomainFailure { code: -123, .. })
    ));
}

#[tokio::test]
async fn query_profiles_preserve_paging_and_malformed_payload_error_ordering() {
    for body in [
        reply(2, Some(23), &[], None, -123, Some((b"typed", ""))),
        reply(2, Some(23), &["invalid"], None, -123, None),
    ] {
        assert!(matches!(
            call(list_query(), Response::new(body)).await,
            Err(FurpcClientError::Protocol(_))
        ));
    }
    assert!(matches!(
        call(
            list_query(),
            Response::new(reply(0, None, &[], None, -123, None))
        )
        .await,
        Err(FurpcClientError::Domain(DomainFailure { code: -123, .. }))
    ));
    assert!(matches!(
        call(
            QueryDescription::new(UnpagedQuery {}),
            Response::new(reply(2, Some(23), &[], None, -123, Some((&[], "")))),
        )
        .await,
        Err(FurpcClientError::Protocol(_))
    ));
    for body in [
        reply(1, Some(37), &[], None, -123, None),
        reply(1, Some(37), &[], Some("only"), 0, Some((&[], ""))),
        reply(2, Some(37), &[], Some("only"), 0, None),
    ] {
        assert!(matches!(
            call(
                QueryDescription::new(ItemQuery {}),
                envelope(Some(16), Some("OperationSuccessAction"), body),
            )
            .await,
            Err(FurpcClientError::Protocol(_))
        ));
    }
    let page = call(
        NodeQueryDescription::default(),
        Response::new(reply(2, Some(1), &[], None, 0, Some((b"typed", "next")))),
    )
    .await
    .unwrap()
    .into_body();
    assert_eq!(page.continuation_token.as_deref(), Some("next"));
}

#[tokio::test]
async fn generic_query_validation_rejects_bad_parameters_before_submission() {
    let calls = AtomicUsize::new(0);
    let mut client = FurpcClient::new(service(|_: Request<Body>| {
        calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok::<_, furpc::Error>(Response::new(Body::empty())))
    }));
    let error = client
        .query(
            Request::new(QueryDescription::new(
                crate::NodeHealthQueryParameters::new(""),
            )),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FurpcClientError::Native(furpc::Error::Codec {
            phase: "encode",
            ..
        })
    ));
    let error = client
        .query(
            Request::new(PagedQuery {
                parameters: crate::PartitionQueryParameters {
                    service_name: "fabric:/App/Service".parse().unwrap(),
                    partition_id: None,
                },
                paging: Paging {
                    max_results: NonZeroU64::new(1),
                    ..Default::default()
                },
            }),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FurpcClientError::Native(furpc::Error::Codec {
            phase: "encode",
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
