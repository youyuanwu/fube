//! Source-shaped health models are safety tests, not native interoperability evidence.
use super::*;
use mssf_client::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};

#[derive(Clone, Copy, Debug, Default)]
enum Shape {
    #[default]
    Complete,
    Empty,
    UnknownScalars,
    ListKind,
    NullWrapper,
    WrongWrapper,
    ListPayload,
    NullItem,
    TypedItem,
    Paging,
    NullEvaluation,
    UnknownEvaluation,
    MissingTag,
    ShortTag,
    MismatchedKind,
    WrongEventField,
    MissingEventField,
    WrongEvaluationField,
}

fn event(e: &mut Encoder, shape: Shape) -> mssf_transport::Result<()> {
    e.string("health-model-owner")?;
    e.string("fixture-property")?;
    e.i64(i64::MAX);
    if matches!(shape, Shape::WrongEventField) {
        e.u32(2);
    } else {
        e.i64(if matches!(shape, Shape::UnknownScalars) {
            -99
        } else {
            2
        });
    }
    e.string("owned event Å😀")?;
    e.i64(i64::MIN);
    e.i64(133000000000000001);
    e.i64(133000000000000002);
    e.boolean(false);
    e.boolean(true);
    e.i64(0);
    e.i64(-1);
    if !matches!(shape, Shape::MissingEventField) {
        e.i64(i64::MAX);
    }
    Ok(())
}

fn health(e: &mut Encoder, shape: Shape) -> mssf_transport::Result<()> {
    e.string("Node_Å😀")?;
    let entries = if matches!(shape, Shape::Empty) {
        vec![]
    } else {
        vec![()]
    };
    e.object_array(&entries, |e, _| event(e, shape))?;
    e.i64(if matches!(shape, Shape::UnknownScalars) {
        987654321
    } else {
        2
    });
    e.object_array(&entries, |e, _| {
        let tag = match shape {
            Shape::UnknownEvaluation => 99i32.to_le_bytes().to_vec(),
            Shape::MissingTag => vec![],
            Shape::ShortTag => vec![1],
            _ => 1i32.to_le_bytes().to_vec(),
        };
        e.pointer(
            (!matches!(shape, Shape::NullEvaluation)).then_some(tag.as_slice()),
            |e| {
                e.i64(if matches!(shape, Shape::MismatchedKind) {
                    2
                } else {
                    1
                });
                e.string("representative event evaluation")?;
                e.object(|e| event(e, shape))?;
                if matches!(shape, Shape::WrongEvaluationField) {
                    e.i64(1);
                } else {
                    e.boolean(true);
                }
                e.i64(if matches!(shape, Shape::UnknownScalars) {
                    -77
                } else {
                    3
                });
                Ok(())
            },
        )
    })
}

fn result(shape: Shape, error: i64) -> Vec<u8> {
    body(|e| {
        e.i64(if error != 0 {
            0
        } else if matches!(shape, Shape::ListKind) {
            2
        } else {
            1
        });
        let tag = if matches!(shape, Shape::WrongWrapper) {
            1i32
        } else {
            16i32
        }
        .to_le_bytes();
        e.pointer(
            (error == 0 && !matches!(shape, Shape::NullWrapper)).then_some(tag.as_slice()),
            |e| {
                let list = if matches!(shape, Shape::ListPayload) {
                    vec![()]
                } else {
                    vec![]
                };
                e.object_array(&list, |e, _| health(e, Shape::Empty))?;
                let tag = if matches!(shape, Shape::TypedItem) {
                    vec![1]
                } else {
                    vec![]
                };
                e.pointer(
                    (!matches!(shape, Shape::NullItem)).then_some(tag.as_slice()),
                    |e| health(e, shape),
                )
            },
        )?;
        e.i64(error);
        e.pointer(matches!(shape, Shape::Paging).then_some(&[][..]), |e| {
            e.string("")
        })
    })
}

fn health_reply(request: &Message, bytes: Vec<u8>) -> Message {
    let mut message = reply(request, "OperationSuccessAction", bytes);
    message.headers[0] = Header::actor(16).unwrap();
    message
}

fn ping_response(request: &Message) -> Message {
    let mut response = reply(request, "PingRequest", ping_body());
    response.headers.push(Header::version(1, 1).unwrap());
    response
}

fn arguments(request: &Message) -> Vec<(String, String)> {
    assert_eq!(request.properties().unwrap().actor, 5);
    assert_eq!(request.properties().unwrap().action, "QueryRequest");
    let mut d = Decoder::new(&request.body);
    let args = d
        .object(|d| {
            let args =
                d.object(|d| d.object_array(|d| Ok((d.object(|d| d.string())?, d.string()?))))?;
            assert_eq!(d.string()?, "GetNodeHealth");
            Ok(args)
        })
        .unwrap();
    d.finish().unwrap();
    args
}

#[tokio::test]
async fn health_options_encode_sorted_json_and_preserve_omission_false_and_raw_bits() {
    let cases = [
        (None, None),
        (Some(false), Some(HealthEventsFilter::DEFAULT)),
        (Some(true), Some(HealthEventsFilter::NONE)),
        (Some(false), Some(HealthEventsFilter::ALL)),
        (
            None,
            Some(HealthEventsFilter::ERROR | HealthEventsFilter::NONE),
        ),
        (None, Some(HealthEventsFilter::from_bits_retain(u32::MAX))),
    ];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for_stream(listener, cases, |request, (policy, filter)| {
            let mut expected = vec![];
            if let Some(value) = policy {
                expected.push((
                    "ClusterHealthPolicy".to_string(),
                    format!("{{\"ConsiderWarningAsError\":{value}}}"),
                ));
            }
            if let Some(value) = filter {
                expected.push((
                    "EventsFilter".to_string(),
                    format!("{{\"HealthStateFilter\":{}}}", value.bits()),
                ));
            }
            expected.push(("NodeName".to_string(), "Node_Å😀".to_string()));
            assert_eq!(arguments(request), expected);
            health_reply(request, result(Shape::Empty, 0))
        })
        .await;
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    for (policy, filter) in cases {
        let mut query = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("Node_Å😀"));
        query.parameters.health_policy = policy.map(|value| NodeHealthPolicy {
            consider_warning_as_error: value,
        });
        query.parameters.events_filter = filter.map(Into::into);
        let value = client.get_node_health(&query).await.unwrap();
        assert_eq!(value.node_name, query.parameters.node_name);
        assert!(value.health_events.is_empty());
        assert!(value.unhealthy_evaluations.is_empty());
    }
    client.close().await.unwrap();
    server.await.unwrap();
}

async fn for_stream<T>(
    listener: TcpListener,
    cases: impl IntoIterator<Item = T>,
    mut response: impl FnMut(&Message, T) -> Message,
) {
    let mut stream = establish(listener).await;
    for case in cases {
        let request = read_message(&mut stream).await.unwrap();
        write_message(&mut stream, &response(&request, case))
            .await
            .unwrap();
    }
    assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
}

#[tokio::test]
async fn health_complete_owned_data_and_unknown_scalars_retain_native_values() {
    for shape in [Shape::Complete, Shape::UnknownScalars] {
        let (client, server) =
            single_reply(move |request| health_reply(request, result(shape, 0))).await;
        let value = client
            .get_node_health(&NodeHealthQueryDescription::new(
                NodeHealthQueryParameters::new("Node_Å😀"),
            ))
            .await
            .unwrap();
        server.await.unwrap();
        let e = &value.health_events[0];
        assert_eq!(value.node_name, "Node_Å😀");
        assert_eq!(e.source_id, "health-model-owner");
        assert_eq!(e.property, "fixture-property");
        assert_eq!(e.description, "owned event Å😀");
        assert_eq!(e.time_to_live_ticks, i64::MAX);
        assert_eq!(e.sequence_number, i64::MIN);
        assert_eq!(e.source_utc_timestamp.0, 133000000000000001);
        assert_eq!(e.last_modified_utc_timestamp.0, 133000000000000002);
        assert!(!e.is_expired);
        assert!(e.remove_when_expired);
        assert_eq!(e.last_ok_transition_at.0, 0);
        assert_eq!(e.last_warning_transition_at.0, -1);
        assert_eq!(e.last_error_transition_at.0, i64::MAX);
        let HealthEvaluation::Event(evaluation) = &value.unhealthy_evaluations[0] else {
            panic!("not Event")
        };
        assert_eq!(evaluation.unhealthy_event, *e);
        assert_eq!(evaluation.description, "representative event evaluation");
        assert!(evaluation.consider_warning_as_error);
        if matches!(shape, Shape::UnknownScalars) {
            assert_eq!(
                value.aggregated_health_state,
                HealthState::Unrecognized(987654321)
            );
            assert_eq!(e.health_state, HealthState::Unrecognized(-99));
            assert_eq!(
                evaluation.aggregated_health_state,
                HealthState::Unrecognized(-77)
            );
        } else {
            assert_eq!(value.aggregated_health_state, HealthState::Warning);
            assert_eq!(e.health_state, HealthState::Warning);
            assert_eq!(evaluation.aggregated_health_state, HealthState::Error);
        }
    }
}

async fn malformed(bytes: Vec<u8>) {
    let (client, server) = single_reply(move |request| health_reply(request, bytes)).await;
    let clone = client.clone();
    assert!(matches!(
        client
            .get_node_health(&NodeHealthQueryDescription::new(
                NodeHealthQueryParameters::new("n")
            ))
            .await,
        Err(Error::Protocol(_))
    ));
    assert!(client.is_closed());
    assert!(matches!(clone.ping().await, Err(Error::Protocol(_))));
    server.await.unwrap();
}

#[tokio::test]
async fn health_malformed_structure_tags_pointers_fields_and_extents_retire() {
    for shape in [
        Shape::ListKind,
        Shape::NullWrapper,
        Shape::WrongWrapper,
        Shape::ListPayload,
        Shape::NullItem,
        Shape::TypedItem,
        Shape::Paging,
        Shape::NullEvaluation,
        Shape::UnknownEvaluation,
        Shape::MissingTag,
        Shape::ShortTag,
        Shape::MismatchedKind,
        Shape::WrongEventField,
        Shape::MissingEventField,
        Shape::WrongEvaluationField,
    ] {
        malformed(result(shape, 0)).await;
    }
    let bytes = result(Shape::Complete, 0);
    for end in (0..bytes.len())
        .step_by(23)
        .chain([bytes.len() - 1, bytes.len() - 2])
    {
        malformed(bytes[..end].to_vec()).await;
    }
    let mut oversized = bytes.clone();
    oversized[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
    malformed(oversized).await;
    let mut flags = bytes.clone();
    flags[5] = 2;
    malformed(flags).await;
    let mut trailing = bytes;
    trailing.push(0);
    malformed(trailing).await;
}

#[tokio::test]
async fn health_success_requires_hm_envelope() {
    for headers in [
        vec![],
        vec![
            Header::actor(5).unwrap(),
            Header::action("QueryReply").unwrap(),
        ],
        vec![Header::actor(16).unwrap()],
        vec![Header::action("OperationSuccessAction").unwrap()],
    ] {
        let (client, server) = single_reply(move |request| Message {
            headers: [
                headers,
                vec![Header::relates_to(request.properties().unwrap().id).unwrap()],
            ]
            .concat(),
            body: result(Shape::Empty, 0),
        })
        .await;
        assert!(matches!(
            client
                .get_node_health(&NodeHealthQueryDescription::new(
                    NodeHealthQueryParameters::new("n")
                ))
                .await,
            Err(Error::Protocol(_))
        ));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn health_native_failures_are_local_and_healthy_queries_reuse_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for_stream(listener, 0..6, |request, index| match index {
            0 => health_reply(request, result(Shape::Empty, REMOTE_CODE)),
            2 => reply(
                request,
                "ClientOperationFailure",
                body(|e| {
                    e.i64(REMOTE_CODE);
                    e.string("missing node")
                }),
            ),
            4 => {
                let mut response = health_reply(request, vec![]);
                response.headers.push(
                    Header::object(FAULT, |e| {
                        e.i64(REMOTE_CODE);
                        e.boolean(false);
                        Ok(())
                    })
                    .unwrap(),
                );
                response
            }
            _ => health_reply(request, result(Shape::Empty, 0)),
        })
        .await;
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let query = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("n"));
    for _ in 0..3 {
        assert!(matches!(
            client.get_node_health(&query).await,
            Err(Error::Remote {
                code: REMOTE_CODE,
                ..
            })
        ));
        assert!(!client.is_closed());
        assert!(client
            .get_node_health(&query)
            .await
            .unwrap()
            .health_events
            .is_empty());
    }
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn health_local_invalid_names_and_timeouts_send_nothing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for_stream(listener, [()], |request, _| {
            assert_eq!(request.properties().unwrap().action, "PingRequest");
            ping_response(request)
        })
        .await;
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    for name in [
        "".into(),
        "a\0b".into(),
        "x".repeat(4097),
        "😀".repeat(2049),
        "x".repeat(100000),
    ] {
        assert!(matches!(
            client
                .get_node_health(&NodeHealthQueryDescription::new(
                    NodeHealthQueryParameters::new(name)
                ))
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    for timeout in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
        assert!(matches!(
            client
                .get_node_health_with_timeout(
                    &NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("n")),
                    timeout
                )
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
    client.ping().await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn health_malformed_success_retires_before_repoll_and_fails_pending_neighbor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let first = read_message(&mut stream).await.unwrap();
        let second = read_message(&mut stream).await.unwrap();
        let query = if first.properties().unwrap().action == "QueryRequest" {
            first
        } else {
            second
        };
        write_message(
            &mut stream,
            &health_reply(&query, result(Shape::UnknownEvaluation, 0)),
        )
        .await
        .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let query = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("n"));
    let health = client.get_node_health(&query);
    let neighbor = client.name_exists("fabric:/neighbor");
    tokio::pin!(health, neighbor);
    poll_fn(|cx| {
        assert!(health.as_mut().poll(cx).is_pending());
        assert!(neighbor.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::timeout(TIMEOUT_DURATION, async {
        while !client.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(health.await, Err(Error::Protocol(_))));
    assert!(matches!(neighbor.await, Err(Error::Protocol(_))));
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn health_reordered_mixed_operations_and_late_cancelled_malformed_replies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let (received, ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let cancelled = read_message(&mut stream).await.unwrap();
        received.send(()).unwrap();
        released.await.unwrap();
        write_message(&mut stream, &health_reply(&cancelled, vec![0xff]))
            .await
            .unwrap();
        let timed = read_message(&mut stream).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        write_message(&mut stream, &health_reply(&timed, vec![0xff]))
            .await
            .unwrap();
        let first = read_message(&mut stream).await.unwrap();
        let second = read_message(&mut stream).await.unwrap();
        for request in [second, first] {
            let response = if request.properties().unwrap().action == "PingRequest" {
                ping_response(&request)
            } else {
                health_reply(&request, result(Shape::Complete, 0))
            };
            write_message(&mut stream, &response).await.unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let clone = client.clone();
    let pending = tokio::spawn(async move {
        clone
            .get_node_health(&NodeHealthQueryDescription::new(
                NodeHealthQueryParameters::new("cancelled"),
            ))
            .await
    });
    ready.await.unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    assert!(matches!(
        client
            .get_node_health_with_timeout(
                &NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("timed")),
                Duration::from_millis(30)
            )
            .await,
        Err(Error::Timeout)
    ));
    let query = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("n"));
    let (health, ping) = tokio::join!(client.get_node_health(&query), client.ping());
    assert_eq!(health.unwrap().health_events.len(), 1);
    assert_eq!(ping.unwrap().node_id, NODE_ID);
    assert!(!client.is_closed());
    client.close().await.unwrap();
    server.await.unwrap();
}
