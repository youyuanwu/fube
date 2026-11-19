use super::*;
use crate::{NodeStatusFilter, Paging};
use std::{collections::BTreeMap, num::NonZeroU64};

pub(super) fn envelope(actor: Option<i64>, action: Option<&str>, body: Body) -> Response<Body> {
    let mut response = Response::new(body);
    if let Some(actor) = actor {
        response
            .headers_mut()
            .append(header::actor(ActorId::new(actor).unwrap()).unwrap())
            .unwrap();
    }
    if let Some(action) = action {
        response
            .headers_mut()
            .append(header::action(action).unwrap())
            .unwrap();
    }
    response
}

fn bytes(body: &Body) -> Vec<u8> {
    body.segments().flatten().copied().collect()
}

fn query_arguments(body: &Body) -> BTreeMap<String, String> {
    let (name, arguments) = query_request(body);
    assert_eq!(name, "GetNodeList");
    arguments.into_iter().collect()
}

pub(super) fn query_request(body: &Body) -> (String, Vec<(String, String)>) {
    #[derive(Deserialize)]
    struct Key {
        value: String,
    }
    #[derive(Deserialize)]
    struct Pair(Key, String);
    #[derive(Deserialize)]
    struct Arguments {
        values: fubst::serde::ObjectArray<Vec<Pair>>,
    }
    #[derive(Deserialize)]
    struct Query(Arguments, String);
    let Query(arguments, name) = fubst::serde::from_slice(&bytes(body)).unwrap();
    let arguments = arguments
        .values
        .0
        .into_iter()
        .map(|Pair(key, value)| (key.value, value))
        .collect();
    (name, arguments)
}

fn encode_node(encoder: &mut fubst::Encoder, ids_match: bool) -> fubst::Result<()> {
    let mut id = Vec::from(1u64.to_le_bytes());
    id.extend_from_slice(&2u64.to_le_bytes());
    encoder.string("N0010")?;
    encoder.byte_array(&id)?;
    encoder.string("127.0.0.1")?;
    encoder.string("NodeType0")?;
    encoder.string("11.8.121.4")?;
    encoder.string("1")?;
    encoder.i64(1)?;
    encoder.i64(10)?;
    encoder.i64(1)?;
    encoder.boolean(true)?;
    encoder.string("UD0")?;
    encoder.string("fd:/0")?;
    if !ids_match {
        id[0] ^= 1;
    }
    encoder.byte_array(&id)?;
    encoder.u64(3)?;
    encoder.u32(19002)?;
    encoder.object(|encoder| {
        encoder.i64(0)?;
        encoder.i64(0)?;
        encoder.object_array::<()>(&[], |_, _| Ok(()))?;
        encoder.object_array::<()>(&[], |_, _| Ok(()))
    })?;
    encoder.boolean(false)?;
    encoder.i64(0)?;
    encoder.i64(133_700_000_000_000_000)?;
    encoder.i64(0)?;
    encoder.u16(19080)
}

fn list_body(
    kind: i64,
    tag: Option<i32>,
    ids_match: &[bool],
    single_item: bool,
    error: i64,
    token: Option<&str>,
) -> Body {
    let mut encoder = fubst::Encoder::new();
    encoder
        .object(|encoder| {
            encoder.i64(kind)?;
            let tag = tag.map(i32::to_le_bytes);
            encoder.pointer(tag.as_ref().map(|tag| tag.as_slice()), |encoder| {
                encoder.object_array(ids_match, |encoder, valid| encode_node(encoder, *valid))?;
                encoder.pointer(single_item.then_some(&[][..]), |_| Ok(()))
            })?;
            encoder.i64(error)?;
            encoder.pointer(token.map(|_| &[][..]), |encoder| {
                encoder.string(token.unwrap())
            })
        })
        .unwrap();
    Body::from_bytes(encoder.finish()).unwrap()
}

#[tokio::test]
async fn mixed_methods_use_native_bodies_and_per_call_reply_validation() {
    let name: NamingUri = "fabric:/App/Child".parse().unwrap();
    let expected_name = name.clone();
    let mut client = FurpcClient::new(service(move |request: Request<Body>| {
        let action: String = String::from_utf16(request.headers().effective().action()).unwrap();
        let response = match action.as_str() {
            "PingRequest" => super::response(Body::from_bytes(body("gateway")).unwrap()),
            "NameExistsRequest" => {
                assert_eq!(
                    fubst::serde::from_slice::<NamingUri>(&bytes(request.body())).unwrap(),
                    expected_name
                );
                #[derive(Serialize)]
                struct Exists(bool, i64);
                envelope(
                    Some(5),
                    Some("NameOperationReply"),
                    Body::from_bytes(fubst::serde::to_vec(&Exists(true, -7)).unwrap()).unwrap(),
                )
            }
            "QueryRequest" => {
                assert_eq!(
                    query_arguments(request.body()),
                    BTreeMap::from([
                        ("ContinuationToken".into(), " opaque+/ ".into()),
                        ("MaxResults".into(), "2".into()),
                        ("NodeName".into(), "N0010".into()),
                        ("NodeStatusFilter".into(), u32::MAX.to_string()),
                    ])
                );
                let body = list_body(2, Some(1), &[true], false, 0, Some("next"));
                envelope(
                    None,
                    None,
                    Body::from_segments(vec![
                        body.slice(0..7).unwrap(),
                        body.slice(7..body.len()).unwrap(),
                    ])
                    .unwrap(),
                )
            }
            other => panic!("unexpected native action {other}"),
        };
        ready(Ok::<_, furpc::Error>(response))
    }));
    assert!(client
        .name_exists(Request::new(name), deadline())
        .await
        .unwrap()
        .into_body());
    let query = NodeQueryDescription {
        parameters: crate::NodeQueryParameters {
            node_name: Some("N0010".into()),
            status_filter: NodeStatusFilter::from_bits_retain(u32::MAX),
        },
        paging: Paging {
            continuation_token: Some(" opaque+/ ".into()),
            max_results: NonZeroU64::new(2),
        },
    };
    let response = client
        .get_node_list(Request::new(query), deadline())
        .await
        .unwrap();
    assert!(response.headers().raw_records().next().is_none());
    assert!(response.source().is_none());
    assert_eq!(response.body().items[0].name, "N0010");
    assert_eq!(
        response.body().items[0].id,
        crate::NodeId { high: 1, low: 2 }
    );
    assert_eq!(response.body().continuation_token.as_deref(), Some("next"));
    assert_eq!(
        client
            .ping(Request::new(()), deadline())
            .await
            .unwrap()
            .body()
            .address,
        "gateway"
    );
}

#[tokio::test]
async fn method_failure_envelopes_are_checked_before_success_codecs() {
    #[derive(Serialize)]
    struct Failure(i64, String);
    let mut client = FurpcClient::new(service(|_: Request<Body>| {
        ready(Ok::<_, furpc::Error>(envelope(
            Some(5),
            Some("ClientOperationFailure"),
            Body::from_bytes(
                fubst::serde::to_vec(&Failure(-123, "native diagnostic".into())).unwrap(),
            )
            .unwrap(),
        )))
    }));
    let name = Request::new("fabric:/App".parse().unwrap());
    for error in [
        client.name_exists(name, deadline()).await.unwrap_err(),
        client
            .get_node_list(Request::new(NodeQueryDescription::default()), deadline())
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(
            error,
            FurpcClientError::Domain(DomainFailure { code: -123, message })
                if message == "native diagnostic"
        ));
    }
    let mut client = FurpcClient::new(service(|_: Request<Body>| {
        ready(Ok::<_, furpc::Error>(envelope(
            Some(6),
            Some("QueryReply"),
            Body::from_bytes(vec![0xff]).unwrap(),
        )))
    }));
    assert!(matches!(
        client
            .get_node_list(Request::new(NodeQueryDescription::default()), deadline())
            .await,
        Err(FurpcClientError::Protocol(_))
    ));
}

#[tokio::test]
async fn query_errors_do_not_mask_malformed_payloads_or_native_node_ids() {
    let replies = [
        list_body(2, Some(1), &[false], false, -123, None),
        list_body(2, Some(1), &[], true, -123, None),
        list_body(2, Some(99), &[], false, -123, None),
        list_body(2, None, &[], false, -123, None),
        list_body(2, Some(1), &[true], false, -123, None),
        list_body(1, Some(1), &[], false, 0, None),
        list_body(2, None, &[], false, 0, None),
        list_body(2, Some(1), &[true], false, 0, Some("")),
    ];
    let mut replies = replies.into_iter();
    let mut client = FurpcClient::new(service(move |_| {
        ready(Ok::<_, furpc::Error>(Response::new(
            replies.next().unwrap(),
        )))
    }));
    for case in 0..8 {
        let result = client
            .get_node_list(Request::new(NodeQueryDescription::default()), deadline())
            .await;
        match (case, result) {
            (0 | 1 | 5 | 6, Err(FurpcClientError::Protocol(_))) => {}
            (2, Err(FurpcClientError::Decode(_))) => {}
            (3 | 4, Err(FurpcClientError::Domain(DomainFailure { code: -123, .. }))) => {}
            (7, Ok(response)) => {
                assert_eq!(response.body().items.len(), 1);
                assert!(response.body().continuation_token.is_none());
            }
            (case, result) => panic!("case {case}: {result:?}"),
        }
    }
}

#[tokio::test]
async fn query_encoding_preserves_omissions_and_native_widths_without_legacy_caps() {
    let calls = AtomicUsize::new(0);
    let mut client = FurpcClient::new(service(|request: Request<Body>| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        let arguments = query_arguments(request.body());
        if call == 0 {
            assert!(arguments.is_empty());
        } else {
            assert_eq!(arguments["NodeName"].len(), 32_769);
            assert_eq!(arguments["ContinuationToken"].len(), 32_769);
        }
        ready(Ok::<_, furpc::Error>(Response::new(list_body(
            2,
            Some(1),
            &[],
            false,
            0,
            None,
        ))))
    }));
    let mut query = NodeQueryDescription::default();
    query.parameters.node_name = Some(String::new());
    query.paging.continuation_token = Some(String::new());
    client
        .get_node_list(Request::new(query), deadline())
        .await
        .unwrap();
    let mut query = NodeQueryDescription::default();
    query.parameters.node_name = Some("n".repeat(32_769));
    query.paging.continuation_token = Some("t".repeat(32_769));
    client
        .get_node_list(Request::new(query), deadline())
        .await
        .unwrap();
    let mut query = NodeQueryDescription::default();
    query.paging.max_results = NonZeroU64::new(u64::MAX);
    assert!(matches!(
        client.get_node_list(Request::new(query), deadline()).await,
        Err(FurpcClientError::Native(furpc::Error::Codec {
            phase: "encode",
            ..
        }))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
