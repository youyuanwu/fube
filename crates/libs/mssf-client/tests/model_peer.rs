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
