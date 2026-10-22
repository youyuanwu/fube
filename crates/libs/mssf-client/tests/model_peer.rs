//! Offline model peers exercise the native wire protocol, not a Service Fabric runtime.

use std::{net::SocketAddr, time::Duration};

use mssf_client::{Error, FabricClient, GatewayDescription};
use mssf_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId, ACTIVITY, FAULT, TIMEOUT},
    serialization::{Decoder, Encoder},
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const TIMEOUT_DURATION: Duration = Duration::from_secs(3);
const NODE_ID: u128 = 0x0123456789abcdef_fedcba9876543210;
const REMOTE_CODE: i64 = -2147017794;

#[path = "support/health_model.rs"]
mod health_queries;

#[path = "support/name_model.rs"]
mod name_writes;

#[path = "support/list_model.rs"]
mod workload_lists;

fn body(fields: impl FnOnce(&mut Encoder) -> mssf_transport::Result<()>) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.object(fields).unwrap();
    encoder.finish()
}

fn naming_body(exists: bool, state: i64) -> Vec<u8> {
    body(|encoder| {
        encoder.boolean(exists);
        encoder.i64(state);
        Ok(())
    })
}

fn ping_body() -> Vec<u8> {
    body(|encoder| {
        encoder.object(|encoder| {
            encoder.string("10.0.0.2:19000")?;
            encoder.object(|encoder| {
                let mut id = Vec::from(((NODE_ID >> 64) as u64).to_le_bytes());
                id.extend_from_slice(&(NODE_ID as u64).to_le_bytes());
                encoder.byte_array(&id)?;
                encoder.u64(0xfedcba9876543210);
                Ok(())
            })?;
            encoder.string("N0010")
        })
    })
}

fn reply(request: &Message, action: &str, body: Vec<u8>) -> Message {
    Message {
        headers: vec![
            Header::actor(5).unwrap(),
            Header::action(action).unwrap(),
            Header::relates_to(request.properties().unwrap().id).unwrap(),
        ],
        body,
    }
}

async fn establish(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let client_control = read_message(&mut stream).await.unwrap();
    assert_eq!(client_control.properties().unwrap().actor, 1);
    write_message(
        &mut stream,
        &Message::listen_instance(
            MessageId {
                guid: [7; 16],
                index: 1,
            },
            &ListenInstance {
                address: "127.0.0.1:19000".into(),
                instance: 42,
                nonce: [8; 16],
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    stream
}

async fn single_reply(
    make_reply: impl FnOnce(&Message) -> Message + Send + 'static,
) -> (FabricClient, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        write_message(&mut stream, &make_reply(&request))
            .await
            .unwrap();
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    (client, server)
}

#[tokio::test]
async fn sequential_ping_and_naming_use_native_headers_and_keep_bool_independent_of_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let mut ids = Vec::new();
        let mut activities = Vec::new();
        for (index, (exists, state)) in [
            (true, 0),
            (true, 1),
            (false, 4),
            (true, 987654321),
            (false, -7),
        ]
        .into_iter()
        .enumerate()
        {
            let request = read_message(&mut stream).await.unwrap();
            let properties = request.properties().unwrap();
            assert_eq!(properties.actor, 5);
            assert!(properties.expects_reply);
            assert_ne!(properties.id, MessageId::default());
            assert!(!ids.contains(&properties.id));
            ids.push(properties.id);
            assert_eq!(request.protocol_version().unwrap(), Some((1, 1)));
            let activity = request
                .headers
                .iter()
                .find(|header| header.id == ACTIVITY)
                .unwrap()
                .read(|decoder| {
                    decoder.object(|decoder| {
                        let guid = decoder.guid()?;
                        assert_ne!(guid, [0; 16]);
                        assert_eq!(decoder.u64()?, 0);
                        Ok(guid)
                    })
                })
                .unwrap();
            assert!(!activities.contains(&activity));
            activities.push(activity);
            let ticks = request
                .headers
                .iter()
                .find(|header| header.id == TIMEOUT)
                .unwrap()
                .read(|decoder| decoder.i64())
                .unwrap();
            assert!(ticks > 0 && ticks <= TIMEOUT_DURATION.as_nanos() as i64 / 100);
            let response = if index == 0 {
                assert_eq!(properties.action, "PingRequest");
                assert!(request.body.is_empty());
                let mut response = reply(&request, "PingRequest", ping_body());
                response.headers.push(Header::version(1, 2).unwrap());
                response
            } else {
                assert_eq!(properties.action, "NameExistsRequest");
                let mut decoder = Decoder::new(&request.body);
                decoder
                    .object(|decoder| {
                        assert_eq!(decoder.i64()?, 1);
                        assert_eq!(decoder.string()?, "fabric");
                        assert_eq!(decoder.string()?, "");
                        assert_eq!(decoder.i64()?, 0);
                        assert_eq!(decoder.string()?, "");
                        assert_eq!(decoder.i32()?, -1);
                        assert_eq!(decoder.string()?, "/App_1/Service-2");
                        assert_eq!(decoder.string()?, "");
                        assert_eq!(decoder.string()?, "");
                        assert_eq!(decoder.u32()?, 2);
                        assert_eq!(decoder.string()?, "App_1");
                        assert_eq!(decoder.string()?, "Service-2");
                        Ok(())
                    })
                    .unwrap();
                decoder.finish().unwrap();
                reply(&request, "NameOperationReply", naming_body(exists, state))
            };
            write_message(&mut stream, &response).await.unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    assert_eq!(
        client.ping().await.unwrap(),
        GatewayDescription {
            address: "10.0.0.2:19000".into(),
            node_id: NODE_ID,
            node_instance: 0xfedcba9876543210,
            node_name: "N0010".into(),
        }
    );
    for expected in [true, false, true, false] {
        assert_eq!(
            client.name_exists("fabric:/App_1/Service-2").await.unwrap(),
            expected
        );
        assert!(!client.is_closed());
    }
    client.close().await.unwrap();
    assert!(client.is_closed());
    client.close().await.unwrap();
    assert!(matches!(client.ping().await, Err(Error::Closed)));
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_naming_success_never_returns_absent_and_closes() {
    let incomplete = [
        Vec::new(),
        body(|_| Ok(())),
        body(|encoder| {
            encoder.boolean(false);
            Ok(())
        }),
        body(|encoder| {
            encoder.i64(0);
            encoder.i64(4);
            Ok(())
        }),
    ];
    for bytes in incomplete {
        let (client, server) =
            single_reply(move |request| reply(request, "NameOperationReply", bytes)).await;
        assert!(matches!(
            client.name_exists("fabric:/a").await,
            Err(Error::Protocol(_))
        ));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_ping_success_requires_every_identity_field() {
    let incomplete = [
        Vec::new(),
        body(|_| Ok(())),
        body(|encoder| encoder.object(|encoder| encoder.string("address"))),
        body(|encoder| {
            encoder.object(|encoder| {
                encoder.string("address")?;
                encoder.object(|encoder| {
                    encoder.byte_array(&[1; 15])?;
                    encoder.u64(1);
                    Ok(())
                })?;
                encoder.string("node")
            })
        }),
        body(|encoder| {
            encoder.object(|encoder| {
                encoder.string("address")?;
                encoder.object(|encoder| encoder.byte_array(&[1; 16]))?;
                encoder.string("node")
            })
        }),
        body(|encoder| {
            encoder.object(|encoder| {
                encoder.string("address")?;
                encoder.object(|encoder| {
                    encoder.byte_array(&[1; 16])?;
                    encoder.u64(1);
                    Ok(())
                })
            })
        }),
    ];
    for bytes in incomplete {
        let (client, server) = single_reply(move |request| {
            let mut response = reply(request, "PingRequest", bytes);
            response.headers.push(Header::version(1, 1).unwrap());
            response
        })
        .await;
        assert!(matches!(client.ping().await, Err(Error::Protocol(_))));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn empty_gateway_identity_is_not_a_success() {
    for (address, node_name) in [("", "N0010"), ("10.0.0.2:19000", "")] {
        let (client, server) = single_reply(move |request| {
            let bytes = body(|encoder| {
                encoder.object(|encoder| {
                    encoder.string(address)?;
                    encoder.object(|encoder| {
                        encoder.byte_array(&[1; 16])?;
                        encoder.u64(1);
                        Ok(())
                    })?;
                    encoder.string(node_name)
                })
            });
            let mut response = reply(request, "PingRequest", bytes);
            response.headers.push(Header::version(1, 1).unwrap());
            response
        })
        .await;
        assert!(matches!(client.ping().await, Err(Error::Protocol(_))));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn ping_requires_compatible_version_action_and_actor() {
    for version in [
        None,
        Some((1, 0)),
        Some((2, 1)),
        Some((0, 2)),
        Some((1, -1)),
    ] {
        let (client, server) = single_reply(move |request| {
            let mut response = reply(request, "PingRequest", ping_body());
            if let Some((major, minor)) = version {
                response
                    .headers
                    .push(Header::version(major, minor).unwrap());
            }
            response
        })
        .await;
        assert!(matches!(client.ping().await, Err(Error::Protocol(_))));
        assert!(client.is_closed());
        server.await.unwrap();
    }
    for (actor, action) in [
        (5, "NameOperationReply"),
        (1, "PingRequest"),
        (0, "PingRequest"),
    ] {
        let (client, server) = single_reply(move |request| {
            let mut response = reply(request, action, ping_body());
            response.headers.push(Header::actor(actor).unwrap());
            response.headers.push(Header::version(1, 1).unwrap());
            response
        })
        .await;
        assert!(matches!(client.ping().await, Err(Error::Protocol(_))));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn naming_rejects_wrong_success_action_or_actor() {
    for (actor, action) in [(5, "NameExistsReply"), (2, "NameOperationReply")] {
        let (client, server) = single_reply(move |request| {
            let mut response = reply(request, action, naming_body(false, 0));
            response.headers.push(Header::actor(actor).unwrap());
            response
        })
        .await;
        assert!(matches!(
            client.name_exists("fabric:/a").await,
            Err(Error::Protocol(_))
        ));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn transport_fault_preserves_error_with_optional_malformed_details() {
    let details = [
        (false, Vec::new(), REMOTE_CODE, ""),
        (true, Vec::new(), REMOTE_CODE, "malformed"),
        (
            true,
            body(|encoder| {
                encoder.object(|encoder| {
                    encoder.i64(REMOTE_CODE - 1);
                    encoder.string("diagnostic")
                })
            }),
            REMOTE_CODE - 1,
            "diagnostic",
        ),
        (
            true,
            body(|encoder| {
                encoder.object(|encoder| {
                    encoder.i64(0);
                    encoder.string("diagnostic")
                })
            }),
            REMOTE_CODE,
            "diagnostic",
        ),
        (
            true,
            body(|encoder| {
                encoder.object(|encoder| {
                    encoder.i64(REMOTE_CODE - 1);
                    encoder.string("")
                })
            }),
            REMOTE_CODE,
            "",
        ),
        (
            true,
            body(|encoder| {
                encoder.i64(REMOTE_CODE);
                encoder.string("wrong un-nested schema")
            }),
            REMOTE_CODE,
            "malformed",
        ),
    ];
    for (has_body, bytes, expected_code, diagnostic) in details {
        let (client, server) = single_reply(move |request| {
            let mut response = reply(request, "", bytes);
            response.headers.push(
                Header::object(FAULT, |encoder| {
                    encoder.i64(REMOTE_CODE);
                    encoder.boolean(has_body);
                    Ok(())
                })
                .unwrap(),
            );
            response
        })
        .await;
        match client.name_exists("fabric:/a").await {
            Err(Error::Remote { code, message }) => {
                assert_eq!(code, expected_code);
                assert!(message.contains(diagnostic), "{message}");
            }
            result => panic!("fault became {result:?}"),
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn operation_failure_is_not_reject_fault_and_retains_readable_code() {
    for (bytes, expected_diagnostic) in [
        (
            body(|encoder| {
                encoder.i64(REMOTE_CODE);
                encoder.string("operation failed")
            }),
            "operation failed",
        ),
        (
            body(|encoder| {
                encoder.i64(REMOTE_CODE);
                Ok(())
            }),
            "diagnostic",
        ),
        (
            body(|encoder| {
                encoder.i64(REMOTE_CODE);
                encoder.boolean(true);
                Ok(())
            }),
            "diagnostic",
        ),
    ] {
        let (client, server) =
            single_reply(move |request| reply(request, "ClientOperationFailure", bytes)).await;
        match client.ping().await {
            Err(Error::Remote { code, message }) => {
                assert_eq!(code, REMOTE_CODE);
                assert!(message.contains(expected_diagnostic));
            }
            result => panic!("operation failure became {result:?}"),
        }
        server.await.unwrap();
    }
    for bytes in [
        Vec::new(),
        body(|_| Ok(())),
        body(|encoder| {
            encoder.i64(0);
            encoder.string("not a native failure")
        }),
    ] {
        let (client, server) =
            single_reply(move |request| reply(request, "ClientOperationFailure", bytes)).await;
        assert!(matches!(
            client.name_exists("fabric:/a").await,
            Err(Error::Protocol(_))
        ));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn input_error_does_not_send_and_native_error_does_not_poison_complete_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for index in 0..2 {
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(request.properties().unwrap().action, "NameExistsRequest");
            let response = if index == 0 {
                reply(
                    &request,
                    "ClientOperationFailure",
                    body(|encoder| {
                        encoder.i64(REMOTE_CODE);
                        encoder.string("native input error")
                    }),
                )
            } else {
                reply(&request, "NameOperationReply", naming_body(false, 0))
            };
            write_message(&mut stream, &response).await.unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    assert!(matches!(
        client.name_exists("fabric:/invalid?").await,
        Err(Error::InvalidInput(_))
    ));
    assert!(!client.is_closed());
    assert!(matches!(
        client.name_exists("fabric:/a").await,
        Err(Error::Remote {
            code: REMOTE_CODE,
            ..
        })
    ));
    assert!(!client.is_closed());
    assert!(!client.name_exists("fabric:/a").await.unwrap());
    client.close().await.unwrap();
    assert!(client.is_closed());
    server.await.unwrap();
}

#[tokio::test]
async fn client_validates_timeout_before_connecting() {
    let endpoint: SocketAddr = "127.0.0.1:1".parse().unwrap();
    for timeout in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
        assert!(matches!(
            FabricClient::connect_clear(endpoint, timeout).await,
            Err(Error::InvalidInput(_))
        ));
    }
}

mod node_queries {
    use super::*;
    use mssf_client::{
        HealthState, Node, NodeDeactivation, NodeDeactivationIntent, NodeDeactivationStatus,
        NodeDeactivationTask, NodeDeactivationTaskType, NodeId, NodeListPage, NodeQueryDescription,
        NodeStatus, NodeStatusFilter, NodeTimestamp, SafetyCheck, SafetyCheckKind,
    };
    use std::{
        future::{poll_fn, Future},
        num::NonZeroU64,
        task::Poll,
    };
    use tokio::sync::oneshot;
    use uuid::Uuid;

    // Source-shaped models exercise safety, not runtime compatibility. The
    // positional schemas are QueryResult, NodeQueryResult::FABRIC_FIELDS_21,
    // NodeDeactivationQueryResult, and SafetyCheck's base/derived scopes.
    #[derive(Clone, Copy, Debug)]
    enum NodeShape {
        Complete,
        UnrecognizedScalars,
        ShortId,
        ShortRepeatedId,
        DisagreeingIdCopies,
        WrongStatusType,
        MissingPort,
        WrongPortType,
        InlineTaskId,
        MissingTaskIntent,
        NullSafetyCheck,
        MissingSafetyTag,
        ShortSafetyTag,
        DisagreeingSafetyKind,
        UnknownSafetyKind,
        OutOfRangeSafetyKind,
        MissingDerivedScope,
        MissingPartitionId,
        WrongPartitionIdType,
    }

    fn partition_id(kind: i64) -> Uuid {
        Uuid::from_u128(0x11223344_5566_7788_99aa_bbccddeeff00 + kind as u128)
    }

    fn encode_safety(
        encoder: &mut Encoder,
        kind: i64,
        shape: NodeShape,
    ) -> mssf_transport::Result<()> {
        let mut tag = (kind as i32).to_le_bytes().to_vec();
        let mut serialized_kind = kind;
        if kind == 2 {
            match shape {
                NodeShape::NullSafetyCheck => return encoder.pointer(None, |_| Ok(())),
                NodeShape::MissingSafetyTag => tag.clear(),
                NodeShape::ShortSafetyTag => tag.truncate(1),
                NodeShape::DisagreeingSafetyKind => serialized_kind = 3,
                NodeShape::UnknownSafetyKind => {
                    tag = 91i32.to_le_bytes().to_vec();
                    serialized_kind = 91;
                }
                NodeShape::OutOfRangeSafetyKind => serialized_kind = i64::MAX,
                _ => {}
            }
        }
        encoder.pointer(Some(&tag), |encoder| {
            encoder.i64(serialized_kind);
            if (2..=7).contains(&kind) {
                if kind != 2 || !matches!(shape, NodeShape::MissingDerivedScope) {
                    encoder.next_scope()?;
                }
                if kind == 2 && matches!(shape, NodeShape::WrongPartitionIdType) {
                    encoder.byte_array(&partition_id(kind).to_bytes_le())?;
                } else if kind != 2 || !matches!(shape, NodeShape::MissingPartitionId) {
                    encoder.guid(&partition_id(kind).to_bytes_le());
                }
            }
            Ok(())
        })
    }

    fn encode_node(encoder: &mut Encoder, shape: &NodeShape) -> mssf_transport::Result<()> {
        let mut id = Vec::from(0x0123456789abcdefu64.to_le_bytes());
        id.extend_from_slice(&0xfedcba9876543210u64.to_le_bytes());
        let unknown = matches!(shape, NodeShape::UnrecognizedScalars);
        encoder.string("Node_Å-02")?;
        encoder.byte_array(if matches!(shape, NodeShape::ShortId) {
            &id[..15]
        } else {
            &id
        })?;
        encoder.string("node-02.example.test")?;
        encoder.string("WorkerType")?;
        encoder.string("11.8.121.4")?;
        encoder.string("cfg:7")?;
        if matches!(shape, NodeShape::WrongStatusType) {
            encoder.i32(4);
        } else {
            encoder.i64(if unknown { i64::MIN } else { 4 });
        }
        encoder.i64(123_456_789);
        encoder.i64(if unknown { i64::MAX } else { 2 });
        encoder.boolean(true);
        encoder.string("UD-02")?;
        encoder.string("fd:/rack/02")?;
        if matches!(shape, NodeShape::DisagreeingIdCopies) {
            id[15] ^= 1;
        }
        encoder.byte_array(if matches!(shape, NodeShape::ShortRepeatedId) {
            &id[..15]
        } else {
            &id
        })?;
        encoder.u64(0xfedcba9876543210);
        encoder.u32(19002);
        encoder.object(|encoder| {
            encoder.i64(if unknown { -7001 } else { 4 });
            encoder.i64(if unknown { 7_654_321 } else { 1 });
            encoder.object_array(
                &[("infra:01", 1, 1), ("repair:02", 2, 2), ("client:03", 3, 3)],
                |encoder, (id, task_type, intent)| {
                    let write_id = |encoder: &mut Encoder| {
                        encoder.string(id)?;
                        encoder.i64(if unknown { -5003 } else { *task_type });
                        Ok(())
                    };
                    if matches!(shape, NodeShape::InlineTaskId) {
                        write_id(encoder)?;
                    } else {
                        encoder.object(write_id)?;
                    }
                    if !matches!(shape, NodeShape::MissingTaskIntent) {
                        encoder.i64(if unknown { i64::MAX - 1 } else { *intent });
                    }
                    Ok(())
                },
            )?;
            encoder.object_array(&[0, 1, 2, 3, 4, 5, 6, 7], |encoder, kind| {
                encode_safety(encoder, *kind, *shape)
            })
        })?;
        encoder.boolean(true);
        encoder.i64(17);
        encoder.i64(133_700_000_000_000_001);
        encoder.i64(133_699_999_830_000_001);
        match shape {
            NodeShape::MissingPort => {}
            NodeShape::WrongPortType => encoder.u32(65535),
            _ => encoder.u16(65535),
        }
        Ok(())
    }

    fn encode_wrapper(encoder: &mut Encoder, nodes: &[NodeShape]) -> mssf_transport::Result<()> {
        encoder.object_array(nodes, encode_node)?;
        encoder.pointer(None, |_| Ok(()))
    }

    fn query_result(
        kind: i64,
        wrapper_tag: Option<&[u8]>,
        error: i64,
        token: Option<&str>,
        wrapper: impl FnOnce(&mut Encoder) -> mssf_transport::Result<()>,
    ) -> Vec<u8> {
        body(|encoder| {
            encoder.i64(kind);
            encoder.pointer(wrapper_tag, wrapper)?;
            encoder.i64(error);
            if let Some(token) = token {
                encoder.pointer(Some(&[]), |encoder| encoder.string(token))?;
            } else {
                encoder.pointer(None, |_| Ok(()))?;
            }
            Ok(())
        })
    }

    fn node_body(nodes: &[NodeShape], token: Option<&str>) -> Vec<u8> {
        query_result(2, Some(&1i32.to_le_bytes()), 0, token, |encoder| {
            encode_wrapper(encoder, nodes)
        })
    }

    fn node_reply(request: &Message, body: Vec<u8>) -> Message {
        // The native aggregate can be bare; do not require the generic factory's
        // QueryReply action/actor to make a model success acceptable.
        Message {
            headers: vec![Header::relates_to(request.properties().unwrap().id).unwrap()],
            body,
        }
    }

    fn ping_reply(request: &Message) -> Message {
        let mut response = reply(request, "PingRequest", ping_body());
        response.headers.push(Header::version(1, 1).unwrap());
        response
    }

    fn query_arguments(request: &Message) -> Vec<(String, String)> {
        let properties = request.properties().unwrap();
        assert_eq!(properties.action, "QueryRequest");
        assert_eq!(properties.actor, 5);
        assert!(properties.expects_reply);
        assert_ne!(properties.id, MessageId::default());
        assert_eq!(request.protocol_version().unwrap(), Some((1, 1)));
        let mut decoder = Decoder::new(&request.body);
        let arguments = decoder
            .typed_object(|decoder, tag| {
                assert!(tag.is_empty());
                let arguments = decoder.typed_object(|decoder, tag| {
                    assert!(tag.is_empty());
                    decoder.object_array(|decoder| {
                        let key = decoder.typed_object(|decoder, tag| {
                            assert!(tag.is_empty());
                            decoder.string()
                        })?;
                        Ok((key, decoder.string()?))
                    })
                })?;
                assert_eq!(decoder.string()?, "GetNodeList");
                Ok(arguments)
            })
            .unwrap();
        decoder.finish().unwrap();
        arguments
    }

    fn expected_node() -> Node {
        Node {
            name: "Node_Å-02".into(),
            id: NodeId {
                high: 0x0123456789abcdef,
                low: 0xfedcba9876543210,
            },
            ip_address_or_fqdn: "node-02.example.test".into(),
            node_type: "WorkerType".into(),
            code_version: "11.8.121.4".into(),
            config_version: "cfg:7".into(),
            status: NodeStatus::Disabling,
            up_time_seconds: 123_456_789,
            health_state: HealthState::Warning,
            is_seed_node: true,
            upgrade_domain: "UD-02".into(),
            fault_domain: "fd:/rack/02".into(),
            instance_id: 0xfedcba9876543210,
            cluster_connection_port: 19002,
            deactivation: NodeDeactivation {
                effective_intent: NodeDeactivationIntent::RemoveNode,
                status: NodeDeactivationStatus::SafetyCheckInProgress,
                tasks: vec![
                    NodeDeactivationTask {
                        id: "infra:01".into(),
                        task_type: NodeDeactivationTaskType::Infrastructure,
                        intent: NodeDeactivationIntent::Pause,
                    },
                    NodeDeactivationTask {
                        id: "repair:02".into(),
                        task_type: NodeDeactivationTaskType::Repair,
                        intent: NodeDeactivationIntent::Restart,
                    },
                    NodeDeactivationTask {
                        id: "client:03".into(),
                        task_type: NodeDeactivationTaskType::Client,
                        intent: NodeDeactivationIntent::RemoveData,
                    },
                ],
                pending_safety_checks: vec![
                    SafetyCheck::Invalid,
                    SafetyCheck::EnsureSeedNodeQuorum,
                    SafetyCheck::Partition {
                        kind: SafetyCheckKind::EnsurePartitionQuorum,
                        partition_id: partition_id(2),
                    },
                    SafetyCheck::Partition {
                        kind: SafetyCheckKind::WaitForPrimaryPlacement,
                        partition_id: partition_id(3),
                    },
                    SafetyCheck::Partition {
                        kind: SafetyCheckKind::WaitForPrimarySwap,
                        partition_id: partition_id(4),
                    },
                    SafetyCheck::Partition {
                        kind: SafetyCheckKind::WaitForReconfiguration,
                        partition_id: partition_id(5),
                    },
                    SafetyCheck::Partition {
                        kind: SafetyCheckKind::WaitForInBuildReplica,
                        partition_id: partition_id(6),
                    },
                    SafetyCheck::Partition {
                        kind: SafetyCheckKind::EnsureAvailability,
                        partition_id: partition_id(7),
                    },
                ],
            },
            is_stopped: true,
            down_time_seconds: 17,
            up_at: NodeTimestamp(133_700_000_000_000_001),
            down_at: NodeTimestamp(133_699_999_830_000_001),
            http_gateway_port: 65535,
        }
    }

    #[tokio::test]
    async fn arguments_use_nested_sorted_map_exact_name_and_native_omission_rules() {
        let cases = [
            (NodeQueryDescription::default(), vec![]),
            (
                NodeQueryDescription {
                    node_name: Some(String::new()),
                    continuation_token: Some(String::new()),
                    ..Default::default()
                },
                vec![],
            ),
            (
                NodeQueryDescription {
                    node_name: Some("MiXeD_Node-02".into()),
                    status_filter: NodeStatusFilter::UP | NodeStatusFilter::DISABLING,
                    continuation_token: Some(" opaque+/= token ".into()),
                    max_results: NonZeroU64::new(2),
                },
                vec![
                    ("ContinuationToken", " opaque+/= token "),
                    ("MaxResults", "2"),
                    ("NodeName", "MiXeD_Node-02"),
                    ("NodeStatusFilter", "9"),
                ],
            ),
            (
                NodeQueryDescription {
                    node_name: Some("ExactNode".into()),
                    ..Default::default()
                },
                vec![("NodeName", "ExactNode")],
            ),
            (
                NodeQueryDescription {
                    max_results: NonZeroU64::new(i64::MAX as u64),
                    status_filter: NodeStatusFilter::ALL,
                    ..Default::default()
                },
                vec![
                    ("MaxResults", "9223372036854775807"),
                    ("NodeStatusFilter", "65535"),
                ],
            ),
            (
                NodeQueryDescription {
                    status_filter: NodeStatusFilter::from_bits(u32::MAX),
                    ..Default::default()
                },
                vec![("NodeStatusFilter", "4294967295")],
            ),
            (
                NodeQueryDescription {
                    status_filter: NodeStatusFilter::DOWN,
                    ..Default::default()
                },
                vec![("NodeStatusFilter", "2")],
            ),
        ];
        let expected: Vec<Vec<_>> = cases
            .iter()
            .map(|(_, arguments)| {
                arguments
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect()
            })
            .collect();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut stream = establish(listener).await;
            for arguments in expected {
                let request = read_message(&mut stream).await.unwrap();
                assert_eq!(query_arguments(&request), arguments);
                write_message(&mut stream, &node_reply(&request, node_body(&[], None)))
                    .await
                    .unwrap();
            }
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
            .await
            .unwrap();
        for (query, _) in cases {
            assert_eq!(
                client.get_node_list(&query).await.unwrap(),
                NodeListPage {
                    nodes: vec![],
                    continuation_token: None,
                }
            );
            assert!(!client.is_closed());
        }
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn all_21_fields_tasks_and_every_safety_subtype_are_owned_and_decoded() {
        let (client, server) = single_reply(|request| {
            node_reply(
                request,
                node_body(&[NodeShape::Complete], Some("opaque-next")),
            )
        })
        .await;
        assert_eq!(
            client
                .get_node_list(&NodeQueryDescription::default())
                .await
                .unwrap(),
            NodeListPage {
                nodes: vec![expected_node()],
                continuation_token: Some("opaque-next".into()),
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unrecognized_scalar_enums_preserve_signed_values_not_named_unknown() {
        let (client, server) = single_reply(|request| {
            node_reply(request, node_body(&[NodeShape::UnrecognizedScalars], None))
        })
        .await;
        let mut expected = expected_node();
        expected.status = NodeStatus::Unrecognized(i64::MIN);
        expected.health_state = HealthState::Unrecognized(i64::MAX);
        expected.deactivation.effective_intent = NodeDeactivationIntent::Unrecognized(-7001);
        expected.deactivation.status = NodeDeactivationStatus::Unrecognized(7_654_321);
        for task in &mut expected.deactivation.tasks {
            task.task_type = NodeDeactivationTaskType::Unrecognized(-5003);
            task.intent = NodeDeactivationIntent::Unrecognized(i64::MAX - 1);
        }
        let page = client
            .get_node_list(&NodeQueryDescription::default())
            .await
            .unwrap();
        assert_eq!(page.nodes, vec![expected]);
        assert_eq!(page.nodes[0].status.as_raw(), i64::MIN);
        assert_eq!(page.nodes[0].health_state.as_raw(), i64::MAX);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn paging_is_explicit_and_a_full_terminal_page_does_not_invent_a_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut stream = establish(listener).await;
            for token in [Some("server-token"), None, Some("")] {
                let request = read_message(&mut stream).await.unwrap();
                let expected = if token.is_none() {
                    vec![
                        ("ContinuationToken".into(), "server-token".into()),
                        ("MaxResults".into(), "1".into()),
                    ]
                } else {
                    vec![("MaxResults".into(), "1".into())]
                };
                assert_eq!(query_arguments(&request), expected);
                write_message(
                    &mut stream,
                    &node_reply(&request, node_body(&[NodeShape::Complete], token)),
                )
                .await
                .unwrap();
            }
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
            .await
            .unwrap();
        let mut query = NodeQueryDescription {
            max_results: NonZeroU64::new(1),
            ..Default::default()
        };
        let first = client.get_node_list(&query).await.unwrap();
        assert_eq!(first.nodes, vec![expected_node()]);
        assert_eq!(first.continuation_token.as_deref(), Some("server-token"));
        query.continuation_token = first.continuation_token;
        let terminal = client.get_node_list(&query).await.unwrap();
        assert_eq!(terminal.nodes.len(), 1);
        assert_eq!(terminal.continuation_token, None);
        query.continuation_token = None;
        assert_eq!(
            client
                .get_node_list(&query)
                .await
                .unwrap()
                .continuation_token,
            None
        );
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_page_limits_and_timeouts_are_local_and_send_no_query() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut stream = establish(listener).await;
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(query_arguments(&request), vec![]);
            write_message(&mut stream, &node_reply(&request, node_body(&[], None)))
                .await
                .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
            .await
            .unwrap();
        for limit in [i64::MAX as u64 + 1, u64::MAX] {
            let query = NodeQueryDescription {
                max_results: NonZeroU64::new(limit),
                ..Default::default()
            };
            assert!(matches!(
                client.get_node_list(&query).await,
                Err(Error::InvalidInput(_))
            ));
            assert!(!client.is_closed());
        }
        for query in [
            NodeQueryDescription {
                node_name: Some("x".repeat(100_000)),
                ..Default::default()
            },
            NodeQueryDescription {
                continuation_token: Some("x".repeat(100_000)),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                client.get_node_list(&query).await,
                Err(Error::InvalidInput(_))
            ));
            assert!(!client.is_closed());
        }
        for timeout in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
            assert!(matches!(
                client
                    .get_node_list_with_timeout(&NodeQueryDescription::default(), timeout)
                    .await,
                Err(Error::InvalidInput(_))
            ));
            assert!(!client.is_closed());
        }
        assert!(client
            .get_node_list(&NodeQueryDescription::default())
            .await
            .unwrap()
            .nodes
            .is_empty());
        client.close().await.unwrap();
        server.await.unwrap();
    }

    async fn assert_malformed_success(bytes: Vec<u8>, label: &str) {
        let (client, server) = single_reply(move |request| node_reply(request, bytes)).await;
        let clone = client.clone();
        let result = client.get_node_list(&NodeQueryDescription::default()).await;
        assert!(
            matches!(result, Err(Error::Protocol(_))),
            "{label}: {result:?}"
        );
        assert!(client.is_closed(), "{label}");
        assert!(clone.is_closed(), "{label}");
        assert!(
            matches!(clone.ping().await, Err(Error::Protocol(_))),
            "{label}"
        );
        assert!(
            matches!(client.terminal_error(), Some(Error::Protocol(_))),
            "{label}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wrong_result_kinds_tags_pointers_and_scalar_metadata_retire_stream() {
        let tag = 1i32.to_le_bytes();
        for kind in [0, 1, -1, 99] {
            assert_malformed_success(
                query_result(kind, Some(&tag), 0, None, |encoder| {
                    encode_wrapper(encoder, &[])
                }),
                &format!("result kind {kind}"),
            )
            .await;
        }
        for wrong_tag in [
            vec![],
            vec![1],
            2i32.to_le_bytes().to_vec(),
            1i64.to_le_bytes().to_vec(),
        ] {
            assert_malformed_success(
                query_result(2, Some(&wrong_tag), 0, None, |encoder| {
                    encode_wrapper(encoder, &[])
                }),
                &format!("wrapper tag {wrong_tag:?}"),
            )
            .await;
        }
        let malformed = [
            (
                "null successful wrapper",
                query_result(2, None, 0, None, |_| Ok(())),
            ),
            (
                "single-item payload in list wrapper",
                query_result(2, Some(&tag), 0, None, |encoder| {
                    encoder.object_array::<()>(&[], |_, _| Ok(()))?;
                    encoder.pointer(Some(&[]), |encoder| {
                        encode_node(encoder, &NodeShape::Complete)
                    })
                }),
            ),
            (
                "scalar count is not an object array",
                query_result(2, Some(&tag), 0, None, |encoder| {
                    encoder.u32(0);
                    encoder.pointer(None, |_| Ok(()))
                }),
            ),
            (
                "wrong result-kind metadata",
                body(|encoder| {
                    encoder.i32(2);
                    encoder.pointer(Some(&tag), |encoder| encode_wrapper(encoder, &[]))?;
                    encoder.i64(0);
                    encoder.pointer(None, |_| Ok(()))
                }),
            ),
            (
                "nested error object instead of inline enum",
                body(|encoder| {
                    encoder.i64(2);
                    encoder.pointer(Some(&tag), |encoder| encode_wrapper(encoder, &[]))?;
                    encoder.object(|encoder| {
                        encoder.i64(0);
                        Ok(())
                    })?;
                    encoder.pointer(None, |_| Ok(()))
                }),
            ),
            (
                "paging pointer with wrong payload type",
                body(|encoder| {
                    encoder.i64(2);
                    encoder.pointer(Some(&tag), |encoder| encode_wrapper(encoder, &[]))?;
                    encoder.i64(0);
                    encoder.pointer(Some(&[]), |encoder| {
                        encoder.boolean(false);
                        Ok(())
                    })
                }),
            ),
            (
                "inline token instead of paging pointer",
                body(|encoder| {
                    encoder.i64(2);
                    encoder.pointer(Some(&tag), |encoder| encode_wrapper(encoder, &[]))?;
                    encoder.i64(0);
                    encoder.string("token")
                }),
            ),
        ];
        for (label, bytes) in malformed {
            assert_malformed_success(bytes, label).await;
        }
    }

    #[tokio::test]
    async fn malformed_nodes_tasks_safety_tags_and_derived_scopes_retire_stream() {
        for shape in [
            NodeShape::ShortId,
            NodeShape::ShortRepeatedId,
            NodeShape::DisagreeingIdCopies,
            NodeShape::WrongStatusType,
            NodeShape::MissingPort,
            NodeShape::WrongPortType,
            NodeShape::InlineTaskId,
            NodeShape::MissingTaskIntent,
            NodeShape::NullSafetyCheck,
            NodeShape::MissingSafetyTag,
            NodeShape::ShortSafetyTag,
            NodeShape::DisagreeingSafetyKind,
            NodeShape::UnknownSafetyKind,
            NodeShape::OutOfRangeSafetyKind,
            NodeShape::MissingDerivedScope,
            NodeShape::MissingPartitionId,
            NodeShape::WrongPartitionIdType,
        ] {
            assert_malformed_success(node_body(&[shape], None), &format!("{shape:?}")).await;
        }
    }

    #[tokio::test]
    async fn truncated_extents_and_trailing_bytes_are_not_empty_successes() {
        let empty = node_body(&[], None);
        for end in 0..empty.len() {
            assert_malformed_success(empty[..end].to_vec(), &format!("empty prefix {end}")).await;
        }
        let populated = node_body(&[NodeShape::Complete], Some("next"));
        for end in (0..populated.len()).step_by(64).chain([
            populated.len() - 3,
            populated.len() - 2,
            populated.len() - 1,
        ]) {
            assert_malformed_success(
                populated[..end].to_vec(),
                &format!("populated prefix {end}"),
            )
            .await;
        }
        let mut invalid_extent = populated.clone();
        invalid_extent[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_malformed_success(invalid_extent, "oversized object extent").await;
        let mut invalid_flags = populated.clone();
        invalid_flags[5] = 2;
        assert_malformed_success(invalid_flags, "unsupported object extension").await;
        let mut trailing = populated;
        trailing.extend(body(|_| Ok(())));
        assert_malformed_success(trailing, "trailing serialized object").await;
    }

    #[tokio::test]
    async fn bare_or_recognized_success_headers_are_valid_but_wrong_headers_retire() {
        for headers in [
            vec![],
            vec![Header::actor(5).unwrap()],
            vec![Header::action("QueryReply").unwrap()],
            vec![
                Header::actor(5).unwrap(),
                Header::action("QueryReply").unwrap(),
            ],
        ] {
            let (client, server) = single_reply(move |request| {
                let mut response = node_reply(request, node_body(&[], None));
                response.headers.extend(headers);
                response
            })
            .await;
            assert!(client
                .get_node_list(&NodeQueryDescription::default())
                .await
                .unwrap()
                .nodes
                .is_empty());
            server.await.unwrap();
        }
        for header in [
            Header::actor(2).unwrap(),
            Header::action("NameOperationReply").unwrap(),
        ] {
            let (client, server) = single_reply(move |request| {
                let mut response = node_reply(request, node_body(&[], None));
                response.headers.push(header);
                response
            })
            .await;
            assert!(matches!(
                client.get_node_list(&NodeQueryDescription::default()).await,
                Err(Error::Protocol(_))
            ));
            assert!(client.is_closed());
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn embedded_query_errors_and_native_faults_are_operation_local() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut stream = establish(listener).await;
            for index in 0..4 {
                let request = read_message(&mut stream).await.unwrap();
                query_arguments(&request);
                let response = match index {
                    0 => node_reply(
                        &request,
                        query_result(0, None, REMOTE_CODE, None, |_| Ok(())),
                    ),
                    1 => node_reply(
                        &request,
                        query_result(
                            2,
                            Some(&1i32.to_le_bytes()),
                            REMOTE_CODE - 1,
                            None,
                            |encoder| encode_wrapper(encoder, &[]),
                        ),
                    ),
                    2 => {
                        let mut response = node_reply(&request, vec![]);
                        response.headers.push(
                            Header::object(FAULT, |encoder| {
                                encoder.i64(REMOTE_CODE - 2);
                                encoder.boolean(true);
                                Ok(())
                            })
                            .unwrap(),
                        );
                        response
                    }
                    _ => reply(
                        &request,
                        "ClientOperationFailure",
                        body(|encoder| {
                            encoder.i64(REMOTE_CODE - 3);
                            encoder.string("invalid continuation")
                        }),
                    ),
                };
                write_message(&mut stream, &response).await.unwrap();
                let neighbor = read_message(&mut stream).await.unwrap();
                assert_eq!(neighbor.properties().unwrap().action, "PingRequest");
                write_message(&mut stream, &ping_reply(&neighbor))
                    .await
                    .unwrap();
            }
            let request = read_message(&mut stream).await.unwrap();
            query_arguments(&request);
            write_message(&mut stream, &node_reply(&request, node_body(&[], None)))
                .await
                .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
            .await
            .unwrap();
        for (index, diagnostic) in [
            "",
            "",
            "malformed optional fault details",
            "invalid continuation",
        ]
        .into_iter()
        .enumerate()
        {
            match client.get_node_list(&NodeQueryDescription::default()).await {
                Err(Error::Remote { code, message }) => {
                    assert_eq!(code, REMOTE_CODE - index as i64);
                    assert_eq!(message, diagnostic);
                }
                result => panic!("fault {index} became {result:?}"),
            }
            assert!(!client.is_closed());
            assert_eq!(client.ping().await.unwrap().node_id, NODE_ID);
            assert!(!client.is_closed());
        }
        assert!(client
            .get_node_list(&NodeQueryDescription::default())
            .await
            .unwrap()
            .nodes
            .is_empty());
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_node_success_retires_before_waiter_repoll_and_fails_pending_clone() {
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
                &node_reply(&query, node_body(&[NodeShape::DisagreeingIdCopies], None)),
            )
            .await
            .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
            .await
            .unwrap();
        let clone = client.clone();
        let query = NodeQueryDescription::default();
        let nodes = client.get_node_list(&query);
        let neighbor = clone.name_exists("fabric:/neighbor");
        tokio::pin!(nodes, neighbor);
        poll_fn(|cx| {
            assert!(nodes.as_mut().poll(cx).is_pending());
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
        assert!(matches!(nodes.await, Err(Error::Protocol(_))));
        assert!(matches!(neighbor.await, Err(Error::Protocol(_))));
        assert!(clone.is_closed());
        assert!(matches!(clone.ping().await, Err(Error::Protocol(_))));
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_ping_name_and_distinct_node_pages_complete_in_reverse_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut stream = establish(listener).await;
            let mut requests = Vec::new();
            let mut ids = Vec::new();
            for _ in 0..4 {
                let request = read_message(&mut stream).await.unwrap();
                let id = request.properties().unwrap().id;
                assert!(!ids.contains(&id));
                ids.push(id);
                requests.push(request);
            }
            for request in requests.into_iter().rev() {
                let response = match request.properties().unwrap().action.as_str() {
                    "PingRequest" => ping_reply(&request),
                    "NameExistsRequest" => {
                        reply(&request, "NameOperationReply", naming_body(true, 4))
                    }
                    "QueryRequest" => {
                        let args = query_arguments(&request);
                        if args == vec![("NodeName".into(), "A".into())] {
                            node_reply(&request, node_body(&[NodeShape::Complete], Some("A-next")))
                        } else {
                            assert_eq!(args, vec![("NodeName".into(), "B".into())]);
                            node_reply(&request, node_body(&[], None))
                        }
                    }
                    action => panic!("unexpected action {action}"),
                };
                write_message(&mut stream, &response).await.unwrap();
            }
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(request.properties().unwrap().action, "PingRequest");
            write_message(&mut stream, &ping_reply(&request))
                .await
                .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
            .await
            .unwrap();
        let (a_client, b_client, name_client, ping_client) = (
            client.clone(),
            client.clone(),
            client.clone(),
            client.clone(),
        );
        let a = NodeQueryDescription {
            node_name: Some("A".into()),
            ..Default::default()
        };
        let b = NodeQueryDescription {
            node_name: Some("B".into()),
            ..Default::default()
        };
        let (a_page, b_page, exists, gateway) = tokio::join!(
            a_client.get_node_list(&a),
            b_client.get_node_list(&b),
            name_client.name_exists("fabric:/App"),
            ping_client.ping(),
        );
        assert_eq!(
            a_page.unwrap(),
            NodeListPage {
                nodes: vec![expected_node()],
                continuation_token: Some("A-next".into()),
            }
        );
        assert_eq!(
            b_page.unwrap(),
            NodeListPage {
                nodes: vec![],
                continuation_token: None,
            }
        );
        assert!(exists.unwrap());
        assert_eq!(gateway.unwrap().node_id, NODE_ID);
        assert!(!client.is_closed());
        assert_eq!(client.ping().await.unwrap().node_id, NODE_ID);
        client.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_and_timed_out_nodes_ignore_late_malformed_replies_and_keep_neighbors() {
        for cancel in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap();
            let (observed_tx, observed_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            let server = tokio::spawn(async move {
                let mut stream = establish(listener).await;
                let first = read_message(&mut stream).await.unwrap();
                let second = read_message(&mut stream).await.unwrap();
                let (query, neighbor) = if first.properties().unwrap().action == "QueryRequest" {
                    (first, second)
                } else {
                    (second, first)
                };
                query_arguments(&query);
                assert_eq!(neighbor.properties().unwrap().action, "NameExistsRequest");
                observed_tx.send(()).unwrap();
                release_rx.await.unwrap();
                write_message(&mut stream, &node_reply(&query, vec![]))
                    .await
                    .unwrap();
                write_message(
                    &mut stream,
                    &reply(&neighbor, "NameOperationReply", naming_body(true, 0)),
                )
                .await
                .unwrap();
                let followup = read_message(&mut stream).await.unwrap();
                query_arguments(&followup);
                write_message(
                    &mut stream,
                    &node_reply(&followup, node_body(&[NodeShape::Complete], None)),
                )
                .await
                .unwrap();
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            });
            let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
                .await
                .unwrap();
            let query_client = client.clone();
            let nodes = tokio::spawn(async move {
                query_client
                    .get_node_list_with_timeout(
                        &NodeQueryDescription::default(),
                        if cancel {
                            TIMEOUT_DURATION
                        } else {
                            Duration::from_millis(250)
                        },
                    )
                    .await
            });
            let name_client = client.clone();
            let neighbor =
                tokio::spawn(async move { name_client.name_exists("fabric:/neighbor").await });
            tokio::time::timeout(TIMEOUT_DURATION, observed_rx)
                .await
                .unwrap()
                .unwrap();
            if cancel {
                nodes.abort();
                assert!(nodes.await.unwrap_err().is_cancelled());
            } else {
                assert!(matches!(nodes.await.unwrap(), Err(Error::Timeout)));
            }
            assert!(!client.is_closed());
            release_tx.send(()).unwrap();
            assert!(neighbor.await.unwrap().unwrap());
            assert!(!client.is_closed());
            assert_eq!(
                client
                    .get_node_list(&NodeQueryDescription::default())
                    .await
                    .unwrap(),
                NodeListPage {
                    nodes: vec![expected_node()],
                    continuation_token: None,
                }
            );
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}
