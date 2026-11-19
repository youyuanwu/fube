use super::*;
use super::{legacy_reusable_reply as reusable_reply, write_probe};
use std::{
    future::{poll_fn, Future},
    task::Poll,
};

const DELETE_NAME: u16 = 0x8033;
const DUPLICATE: i64 = -2147017792;
const MISSING: i64 = -2147017791;

fn check_request(request: &Message, delete: bool) {
    let properties = request.properties().unwrap();
    assert_eq!(properties.actor, 5);
    assert_eq!(
        properties.action,
        if delete {
            "DeleteNameRequest"
        } else {
            "CreateNameRequest"
        }
    );
    assert!(properties.expects_reply);
    assert_ne!(properties.id, MessageId::default());
    assert_eq!(request.protocol_version().unwrap(), Some((1, 1)));
    let ticks = request
        .headers
        .iter()
        .find(|header| header.id == TIMEOUT)
        .unwrap()
        .read(|decoder| decoder.i64())
        .unwrap();
    assert!(ticks > 0 && ticks <= 30_000_000);
    let activity = request
        .headers
        .iter()
        .find(|header| header.id == ACTIVITY)
        .unwrap()
        .read(|decoder| decoder.object(|decoder| Ok((decoder.guid()?, decoder.u64()?))))
        .unwrap();
    assert_ne!(activity.0, [0; 16]);
    assert_eq!(activity.1, 0);
    let explicit: Vec<_> = request
        .headers
        .iter()
        .filter(|header| header.id == DELETE_NAME)
        .collect();
    assert_eq!(explicit.len(), usize::from(delete));
    if delete {
        assert!(explicit[0].read(|decoder| decoder.boolean()).unwrap());
    }
    let mut decoder = Decoder::new(&request.body);
    decoder
        .object(|decoder| {
            assert_eq!(decoder.i64()?, 1);
            assert_eq!(decoder.string()?, "fabric");
            assert_eq!(decoder.string()?, "");
            assert_eq!(decoder.i64()?, 0);
            assert_eq!(decoder.string()?, "");
            assert_eq!(decoder.i32()?, -1);
            assert_eq!(decoder.string()?, "/App_1/Name-2");
            assert_eq!(decoder.string()?, "");
            assert_eq!(decoder.string()?, "");
            assert_eq!(decoder.u32()?, 2);
            assert_eq!(decoder.string()?, "App_1");
            assert_eq!(decoder.string()?, "Name-2");
            Ok(())
        })
        .unwrap();
    decoder.finish().unwrap();
}

async fn write(
    client: &FabricClient,
    delete: bool,
    timeout: Duration,
) -> fube_client::ClientResult<()> {
    if delete {
        client
            .delete_name("fabric:/App_1/Name-2", Some(timeout))
            .await
    } else {
        client
            .create_name("fabric:/App_1/Name-2", Some(timeout))
            .await
    }
}

#[tokio::test]
async fn exact_native_write_requests_all_entry_points_and_empty_replies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let mut ids = Vec::new();
        for delete in [false, false, true, true] {
            let request = read_message(&mut stream).await.unwrap();
            check_request(&request, delete);
            let id = request.properties().unwrap().id;
            assert!(!ids.contains(&id));
            ids.push(id);
            write_message(&mut stream, &reply(&request, "NameOperationReply", vec![]))
                .await
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    let name: fube_client::NamingUri = "fabric:/App_1/Name-2".parse().unwrap();
    client.create_name(&name, None).await.unwrap();
    write(&client, false, TIMEOUT_DURATION).await.unwrap();
    client.delete_name(&name, None).await.unwrap();
    write(&client, true, TIMEOUT_DURATION).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn native_duplicate_and_missing_are_explicit_and_operation_local() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for (delete, code) in [(false, DUPLICATE), (true, MISSING)] {
            let request = read_message(&mut stream).await.unwrap();
            check_request(&request, delete);
            write_message(
                &mut stream,
                &reply(
                    &request,
                    "ClientOperationFailure",
                    body(|encoder| {
                        encoder.i64(code).unwrap();
                        encoder.string("native name diagnostic")
                    }),
                ),
            )
            .await
            .unwrap();
            let followup = read_message(&mut stream).await.unwrap();
            check_request(&followup, delete);
            write_message(&mut stream, &reply(&followup, "NameOperationReply", vec![]))
                .await
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    for (delete, code) in [(false, DUPLICATE), (true, MISSING)] {
        assert!(matches!(
            write(&client, delete, TIMEOUT_DURATION).await,
            Err(ClientError::Domain(DomainFailure { code: actual, message })) if actual == code && message == "native name diagnostic"
        ));
        assert!(!client.is_closed());
        write(&client, delete, TIMEOUT_DURATION).await.unwrap();
    }
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn invalid_uri_and_budget_never_send_writes_and_keep_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.properties().unwrap().action, "CreateNameRequest");
        write_message(&mut stream, &reply(&request, "NameOperationReply", vec![]))
            .await
            .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    for name in [
        "".into(),
        "fabric:/".into(),
        "fabric:/a//b".into(),
        "fabric:/é".into(),
        format!("fabric:/{}", "a".repeat(4089)),
    ] {
        assert!(matches!(
            client.create_name(&name, None).await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
        assert!(matches!(
            client.delete_name(&name, None).await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
    }
    for delete in [false, true] {
        for budget in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
            assert!(matches!(
                write(&client, delete, budget).await,
                Err(ClientError::Local(Error::InvalidInput(_)))
            ));
        }
    }
    client
        .create_name("fabric:/App_1/Name-2", None)
        .await
        .unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_write_body_and_domain_envelope_are_call_local() {
    for delete in [false, true] {
        for shape in 0..5 {
            let make_reply = move |request: &Message| {
                let mut response = reply(request, "NameOperationReply", vec![]);
                match shape {
                    0 => response.body = body(|_| Ok(())),
                    1 => response.body = naming_body(true, 0),
                    2 => response.body = vec![0xff],
                    3 => response.headers[0] = Header::actor(6).unwrap(),
                    _ => response.headers[1] = Header::action("CreateNameReply").unwrap(),
                }
                response
            };
            let (client, server) = reusable_reply(make_reply).await;
            let result = write(&client, delete, TIMEOUT_DURATION).await;
            assert!(matches!(result, Err(ClientError::Protocol(_))));
            assert!(!client.is_closed());
            assert!(client.terminal_error().is_none());
            client.create_name("fabric:/Probe", None).await.unwrap();
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn malformed_write_is_call_local_and_pending_neighbor_succeeds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let a = read_message(&mut stream).await.unwrap();
        let b = read_message(&mut stream).await.unwrap();
        let (request, neighbor) = if a.properties().unwrap().action == "DeleteNameRequest" {
            (a, b)
        } else {
            (b, a)
        };
        write_message(&mut stream, &reply(&request, "NameOperationReply", vec![0]))
            .await
            .unwrap();
        write_message(&mut stream, &reply(&neighbor, "NameOperationReply", vec![]))
            .await
            .unwrap();
        let reuse = read_message(&mut stream).await.unwrap();
        write_message(&mut stream, &write_probe(&reuse))
            .await
            .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    let mutation = client.delete_name("fabric:/App_1/Name-2", None);
    let neighbor = client.create_name("fabric:/App_1/Name-2", None);
    tokio::pin!(mutation, neighbor);
    poll_fn(|cx| {
        assert!(mutation.as_mut().poll(cx).is_pending());
        assert!(neighbor.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(mutation.await, Err(ClientError::Protocol(_))));
    neighbor.await.unwrap();
    assert!(!client.is_closed());
    assert!(client.terminal_error().is_none());
    client.create_name("fabric:/Probe", None).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn reordered_concurrent_writes_share_one_driver() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let mut requests = Vec::new();
        for _ in 0..3 {
            requests.push(read_message(&mut stream).await.unwrap());
        }
        for request in requests.into_iter().rev() {
            check_request(
                &request,
                request.properties().unwrap().action == "DeleteNameRequest",
            );
            let body = vec![];
            write_message(&mut stream, &reply(&request, "NameOperationReply", body))
                .await
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    let (create, delete, exists) = tokio::join!(
        client.create_name("fabric:/App_1/Name-2", None),
        client.delete_name("fabric:/App_1/Name-2", None),
        client.create_name("fabric:/App_1/Name-2", None),
    );
    create.unwrap();
    delete.unwrap();
    exists.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn peer_observed_cancel_and_timeout_never_replay_writes_or_decode_late_replies() {
    for delete in [false, true] {
        for cancel in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap();
            let (seen, received) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let mut stream = establish(listener).await;
                let request = read_message(&mut stream).await.unwrap();
                check_request(&request, delete);
                seen.send(()).unwrap();
                released.await.unwrap();
                write_message(
                    &mut stream,
                    &reply(&request, "NameOperationReply", vec![0xff]),
                )
                .await
                .unwrap();
                // The next frame must be the survivor, never a replay.
                let survivor = read_message(&mut stream).await.unwrap();
                assert_eq!(survivor.properties().unwrap().action, "CreateNameRequest");
                write_message(&mut stream, &reply(&survivor, "NameOperationReply", vec![]))
                    .await
                    .unwrap();
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            });
            let channel = FabricClient::endpoint_clear(endpoint)
                .unwrap()
                .connect_timeout(TIMEOUT_DURATION)
                .connect()
                .await
                .unwrap();
            let client = FabricClient::new(
                channel,
                fube_client::ClientOptions {
                    default_timeout: TIMEOUT_DURATION,
                    ..Default::default()
                },
            )
            .unwrap();
            let clone = client.clone();
            let pending = tokio::spawn(async move {
                write(
                    &clone,
                    delete,
                    if cancel {
                        TIMEOUT_DURATION
                    } else {
                        Duration::from_millis(250)
                    },
                )
                .await
            });
            tokio::time::timeout(TIMEOUT_DURATION, received)
                .await
                .unwrap()
                .unwrap();
            if cancel {
                pending.abort();
                assert!(pending.await.unwrap_err().is_cancelled());
            } else {
                assert!(matches!(
                    pending.await.unwrap(),
                    Err(ClientError::Transport(Error::Timeout))
                ));
            }
            release.send(()).unwrap();
            client
                .create_name("fabric:/App_1/Name-2", None)
                .await
                .unwrap();
            assert!(!client.is_closed());
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn naming_deadline_precedes_and_includes_caller_conversion() {
    struct InvalidConversion;
    impl AsRef<str> for InvalidConversion {
        fn as_ref(&self) -> &str {
            panic!("invalid timeout must precede conversion")
        }
    }
    struct SlowConversion;
    impl AsRef<str> for SlowConversion {
        fn as_ref(&self) -> &str {
            std::thread::sleep(Duration::from_millis(20));
            "fabric:/SlowConversion"
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.properties().unwrap().action, "CreateNameRequest");
        write_message(&mut stream, &write_probe(&request))
            .await
            .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        client
            .create_name(InvalidConversion, Some(Duration::ZERO))
            .await,
        Err(ClientError::Local(Error::InvalidInput(_)))
    ));
    assert!(matches!(
        client
            .delete_name(InvalidConversion, Some(Duration::ZERO))
            .await,
        Err(ClientError::Local(Error::InvalidInput(_)))
    ));
    let tiny = Some(Duration::from_millis(5));
    assert!(matches!(
        client.create_name(SlowConversion, tiny).await,
        Err(ClientError::Local(Error::Timeout))
    ));
    assert!(matches!(
        client.delete_name(SlowConversion, tiny).await,
        Err(ClientError::Local(Error::Timeout))
    ));
    client.create_name("fabric:/Probe", None).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn naming_conversion_input_is_not_retained_across_rpc() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    struct Input(Arc<AtomicBool>);
    impl AsRef<str> for Input {
        fn as_ref(&self) -> &str {
            "fabric:/OwnedSnapshot"
        }
    }
    impl Drop for Input {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let released = Arc::new(AtomicBool::new(false));
    let observed = released.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.properties().unwrap().action, "CreateNameRequest");
        assert!(observed.load(Ordering::SeqCst));
        write_message(&mut stream, &reply(&request, "NameOperationReply", vec![]))
            .await
            .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(client.create_name(Input(released), None).await.is_ok());
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn all_operations_select_optional_timeout_and_reject_invalid_overrides_without_sending() {
    use fube_client::{
        DeployedApplicationQueryDescription, DeployedApplicationQueryParameters,
        DeployedCodePackageQueryDescription, DeployedCodePackageQueryParameters,
        DeployedReplicaQueryDescription, DeployedReplicaQueryParameters,
        DeployedServicePackageQueryDescription, DeployedServicePackageQueryParameters,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for budget in [
            TIMEOUT_DURATION,
            Duration::from_secs(1),
            Duration::from_secs(6),
        ] {
            for _ in 0..6 {
                let request = read_message(&mut stream).await.unwrap();
                let ticks = request
                    .headers
                    .iter()
                    .find(|header| header.id == TIMEOUT)
                    .unwrap()
                    .read(|decoder| decoder.i64())
                    .unwrap();
                let lower = (budget - Duration::from_secs(1)).as_nanos() as i64 / 100;
                let upper = budget.as_nanos() as i64 / 100;
                assert!(
                    ticks > lower && ticks <= upper,
                    "{ticks}: budget {budget:?}"
                );
                let mut response = reply(&request, "ClientOperationFailure", vec![]);
                response.headers.push(
                    Header::object(FAULT, |encoder| {
                        encoder.i64(REMOTE_CODE).unwrap();
                        encoder.boolean(false).unwrap();
                        Ok(())
                    })
                    .unwrap(),
                );
                write_message(&mut stream, &response).await.unwrap();
            }
        }
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.properties().unwrap().action, "CreateNameRequest");
        let mut response = write_probe(&request);
        response.headers.push(Header::version(1, 1).unwrap());
        write_message(&mut stream, &response).await.unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = FabricClient::endpoint_clear(endpoint)
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION)
        .connect()
        .await
        .unwrap();
    let client = FabricClient::new(
        channel,
        fube_client::ClientOptions {
            default_timeout: TIMEOUT_DURATION,
            ..Default::default()
        },
    )
    .unwrap();
    for (timeout, valid) in [
        (None, true),
        (Some(Duration::from_secs(1)), true),
        (Some(Duration::from_secs(6)), true),
        (Some(Duration::ZERO), false),
        (Some(Duration::from_nanos(99)), false),
        (Some(Duration::MAX), false),
    ] {
        macro_rules! check {
            ($method:ident $(, $argument:expr)?) => {
                let result = client.$method($($argument,)? timeout).await;
                if valid {
                    assert!(matches!(
                        result,
                        Err(ClientError::Transport(Error::Remote { code: REMOTE_CODE, .. }))
                    ), "{}: {result:?}", stringify!($method));
                } else {
                    assert!(matches!(result, Err(ClientError::Local(Error::InvalidInput(_)))),
                        "{}: {result:?}", stringify!($method));
                }
                assert!(!client.is_closed());
            };
        }

        check!(create_name, "fabric:/Model");
        check!(delete_name, "fabric:/Model");

        check!(
            get_deployed_application_list,
            &DeployedApplicationQueryDescription::new(DeployedApplicationQueryParameters::new(
                "N0010"
            ))
        );
        check!(
            get_deployed_service_package_list,
            &DeployedServicePackageQueryDescription::new(
                DeployedServicePackageQueryParameters::new(
                    "N0010",
                    "fabric:/Model".parse().unwrap()
                )
            )
        );
        check!(
            get_deployed_code_package_list,
            &DeployedCodePackageQueryDescription::new(DeployedCodePackageQueryParameters::new(
                "N0010",
                "fabric:/Model".parse().unwrap()
            ))
        );
        check!(
            get_deployed_replica_list,
            &DeployedReplicaQueryDescription::new(DeployedReplicaQueryParameters::new(
                "N0010",
                "fabric:/Model".parse().unwrap()
            ))
        );
    }
    client.create_name("fabric:/Probe", None).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn client_validates_timeout_before_connecting() {
    let endpoint: SocketAddr = "127.0.0.1:1".parse().unwrap();
    for timeout in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
        assert!(matches!(
            FabricClient::endpoint_clear(endpoint)
                .unwrap()
                .connect_timeout(timeout)
                .connect()
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
}
