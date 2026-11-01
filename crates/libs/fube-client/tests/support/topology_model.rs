//! Shared transport behavior for the additional native list query families.
use super::*;
use fube_client::{
    DeployedApplicationQueryDescription, DeployedApplicationQueryParameters,
    DeployedCodePackageQueryDescription, DeployedCodePackageQueryParameters,
    DeployedReplicaQueryDescription, DeployedReplicaQueryParameters,
    DeployedServicePackageQueryDescription, DeployedServicePackageQueryParameters,
    NodeHealthQueryDescription, NodeHealthQueryParameters, PartitionQueryDescription,
    PartitionQueryParameters, ReplicaQueryDescription, ReplicaQueryParameters,
};
use fube_transport::message::{ACTION, ACTOR, IPC, RELATES_TO};
use fube_transport::serialization::serde::{EmptyObjectArray, FixedTag, Pointer};
use serde::Serialize;

#[tokio::test]
async fn forwarded_body_only_aggregate_lists_use_their_typed_profile() {
    for deployed in [false, true] {
        for records in [2, 3] {
            let (client, server) = reusable_reply(move |request| {
                let outer = request.properties().unwrap().id;
                let mut headers = vec![
                    Header::relates_to(MessageId {
                        guid: [91; 16],
                        index: 900,
                    })
                    .unwrap(),
                    Header::relates_to(outer).unwrap(),
                    // Native forwarded aggregates repeat this opaque flag.
                    // It never grants the Rust client retry/replay authority.
                    Header::boolean(0x8008, true).unwrap(),
                    Header::boolean(0x8008, true).unwrap(),
                ];
                if records == 3 {
                    headers.push(Header::relates_to(outer).unwrap());
                }
                let tag: i32 = if deployed { 10 } else { 4 };
                Message {
                    headers,
                    body: body(|e| {
                        e.i64(2);
                        e.pointer(Some(&tag.to_le_bytes()), |e| {
                            e.object_array::<()>(&[], |_, _| Ok(()))?;
                            e.pointer(None, |_| Ok(()))
                        })?;
                        e.i64(0);
                        e.pointer(None, |_| Ok(()))
                    }),
                }
            })
            .await;
            if deployed {
                client
                    .get_deployed_application_list(
                        &DeployedApplicationQueryDescription::new(
                            DeployedApplicationQueryParameters::new("N0010"),
                        ),
                        None,
                    )
                    .await
                    .unwrap();
            } else {
                client
                    .get_application_list(
                        &fube_client::ApplicationQueryDescription::default(),
                        None,
                    )
                    .await
                    .unwrap();
            }
            client.ping(None).await.unwrap();
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum QueryKind {
    Partitions,
    Replicas,
    Deployments,
    ServicePackages,
    CodePackages,
    DeployedReplicas,
}

impl QueryKind {
    const ALL: [Self; 6] = [
        Self::Partitions,
        Self::Replicas,
        Self::Deployments,
        Self::ServicePackages,
        Self::CodePackages,
        Self::DeployedReplicas,
    ];

    fn tag(self) -> i32 {
        match self {
            Self::Partitions => 5,
            Self::Replicas => 6,
            Self::Deployments => 10,
            Self::ServicePackages => 11,
            Self::CodePackages => 14,
            Self::DeployedReplicas => 12,
        }
    }

    async fn call(
        self,
        client: &FabricClient,
        timeout: Option<Duration>,
    ) -> fube_client::ClientResult<()> {
        match self {
            Self::Partitions => {
                let query = PartitionQueryDescription::new(PartitionQueryParameters::new(
                    "fabric:/Model/Service".parse().unwrap(),
                ));
                client.get_partition_list(&query, timeout).await.map(|_| ())
            }
            Self::Replicas => {
                let query = ReplicaQueryDescription::new(ReplicaQueryParameters::new(
                    uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                ));
                client.get_replica_list(&query, timeout).await.map(|_| ())
            }
            Self::Deployments => {
                let query = DeployedApplicationQueryDescription::new(
                    DeployedApplicationQueryParameters::new("N0010"),
                );
                client
                    .get_deployed_application_list(&query, timeout)
                    .await
                    .map(|_| ())
            }
            Self::ServicePackages => {
                let query = DeployedServicePackageQueryDescription::new(
                    DeployedServicePackageQueryParameters::new(
                        "N0010",
                        "fabric:/Model".parse().unwrap(),
                    ),
                );
                client
                    .get_deployed_service_package_list(&query, timeout)
                    .await
                    .map(|_| ())
            }
            Self::CodePackages => {
                let query = DeployedCodePackageQueryDescription::new(
                    DeployedCodePackageQueryParameters::new(
                        "N0010",
                        "fabric:/Model".parse().unwrap(),
                    ),
                );
                client
                    .get_deployed_code_package_list(&query, timeout)
                    .await
                    .map(|_| ())
            }
            Self::DeployedReplicas => {
                let query = DeployedReplicaQueryDescription::new(
                    DeployedReplicaQueryParameters::new("N0010", "fabric:/Model".parse().unwrap()),
                );
                client
                    .get_deployed_replica_list(&query, timeout)
                    .await
                    .map(|_| ())
            }
        }
    }
}

#[tokio::test]
async fn empty_list_successes_support_default_and_explicit_timeouts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for kind in QueryKind::ALL {
            for _ in 0..2 {
                let request = read_message(&mut stream).await.unwrap();
                assert_eq!(request.properties().unwrap().action, "QueryRequest");
                let bytes = body(|e| {
                    e.i64(2);
                    e.pointer(Some(&kind.tag().to_le_bytes()), |e| {
                        e.object_array::<()>(&[], |_, _| Ok(()))?;
                        e.pointer(None, |_| Ok(()))
                    })?;
                    e.i64(0);
                    e.pointer(None, |_| Ok(()))
                });
                write_message(&mut stream, &query_reply(kind, &request, bytes))
                    .await
                    .unwrap();
            }
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    for kind in QueryKind::ALL {
        kind.call(&client, None).await.unwrap();
        kind.call(&client, Some(TIMEOUT_DURATION)).await.unwrap();
    }
    client.close().await.unwrap();
    server.await.unwrap();
}

fn query_reply(kind: QueryKind, request: &Message, bytes: Vec<u8>) -> Message {
    let mut response = reply(request, "QueryReply", bytes);
    match kind {
        QueryKind::DeployedReplicas => response.headers[0] = Header::actor(13).unwrap(),
        QueryKind::ServicePackages | QueryKind::CodePackages => {
            response.headers = vec![Header::relates_to(request.properties().unwrap().id).unwrap()];
        }
        _ => {}
    }
    response
}

fn native_error(kind: QueryKind, request: &Message) -> Message {
    #[derive(Serialize)]
    struct QueryError {
        kind: i64,
        payload: Pointer<FixedTag<EmptyObjectArray, 0>>,
        error: i64,
        paging: Pointer<EmptyObjectArray>,
    }
    query_reply(
        kind,
        request,
        fube_transport::serialization::serde::to_vec(&QueryError {
            kind: 0,
            payload: Pointer(None),
            error: REMOTE_CODE,
            paging: Pointer(None),
        })
        .unwrap(),
    )
}

#[tokio::test]
async fn unpaged_deployment_continuations_are_call_local_even_with_native_error() {
    for kind in [
        QueryKind::ServicePackages,
        QueryKind::CodePackages,
        QueryKind::DeployedReplicas,
    ] {
        for token in ["", "unexpected-next"] {
            for error in [0, REMOTE_CODE] {
                let (client, server) = reusable_reply(move |request| {
                    let bytes = body(|e| {
                        e.i64(2);
                        e.pointer(Some(&kind.tag().to_le_bytes()), |e| {
                            e.object_array::<()>(&[], |_, _| Ok(()))?;
                            e.pointer(None, |_| Ok(()))
                        })?;
                        e.i64(error);
                        e.pointer(Some(&[]), |e| e.string(token))
                    });
                    query_reply(kind, request, bytes)
                })
                .await;
                assert!(matches!(
                    kind.call(&client, None).await,
                    Err(ClientError::Protocol(_))
                ));
                assert!(!client.is_closed());
                assert!(client.terminal_error().is_none());
                client.ping(None).await.unwrap();
                client.close().await.unwrap();
                server.await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn native_errors_from_each_new_query_leave_shared_connection_usable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for kind in QueryKind::ALL {
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(request.properties().unwrap().action, "QueryRequest");
            write_message(&mut stream, &native_error(kind, &request))
                .await
                .unwrap();
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(request.properties().unwrap().action, "PingRequest");
            write_message(&mut stream, &ping_reply(&request))
                .await
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    for kind in QueryKind::ALL {
        assert!(matches!(
            kind.call(&client, None).await,
            Err(ClientError::Domain(DomainFailure {
                code: REMOTE_CODE,
                ..
            }))
        ));
        assert!(!client.is_closed());
        client.ping(None).await.unwrap();
    }
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_new_query_successes_are_call_local() {
    for kind in QueryKind::ALL {
        let (client, server) =
            reusable_reply(|request| reply(request, "QueryReply", Vec::new())).await;
        assert!(matches!(
            kind.call(&client, Some(TIMEOUT_DURATION)).await,
            Err(ClientError::Protocol(_))
        ));
        assert!(!client.is_closed());
        assert!(client.terminal_error().is_none());
        client.ping(None).await.unwrap();
        client.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn partition_field_adapters_reject_bad_tags_and_guid_metadata_before_native_errors() {
    for malformed in 0..3 {
        for error in [0, REMOTE_CODE] {
            let (client, server) = reusable_reply(move |request| {
                let bytes = body(|e| {
                    e.i64(2);
                    e.pointer(Some(&5i32.to_le_bytes()), |e| {
                        e.object_array(&[()], |e, _| {
                            e.i64(1);
                            e.typed_object(if malformed == 0 { &[1] } else { &[] }, |e| {
                                e.i64(1);
                                match malformed {
                                    1 => e.byte_array(&[0; 16])?,
                                    2 => e.string("00112233-4455-6677-8899-aabbccddeeff")?,
                                    _ => e.guid(&[0; 16]),
                                }
                                e.string("")?;
                                e.i64(0);
                                e.i64(0);
                                Ok(())
                            })?;
                            e.i32(1);
                            e.u32(0);
                            e.u32(0);
                            e.i64(1);
                            e.i64(1);
                            Ok(())
                        })?;
                        e.pointer(None, |_| Ok(()))
                    })?;
                    e.i64(error);
                    e.pointer(None, |_| Ok(()))
                });
                reply(request, "QueryReply", bytes)
            })
            .await;
            assert!(matches!(
                QueryKind::Partitions.call(&client, None).await,
                Err(ClientError::Protocol(_))
            ));
            assert!(!client.is_closed());
            assert!(client.terminal_error().is_none());
            client.ping(None).await.unwrap();
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn invalid_new_query_options_send_nothing_and_preserve_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
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
    for kind in QueryKind::ALL {
        for timeout in [Duration::ZERO, Duration::MAX] {
            assert!(matches!(
                kind.call(&client, Some(timeout)).await,
                Err(ClientError::Local(Error::InvalidInput(_)))
            ));
            assert!(!client.is_closed());
        }
    }
    for limit in [1, i64::MAX as u64, i64::MAX as u64 + 1] {
        let mut partitions = PartitionQueryDescription::new(PartitionQueryParameters::new(
            "fabric:/Model/Service".parse().unwrap(),
        ));
        partitions.paging.max_results = std::num::NonZeroU64::new(limit);
        let mut replicas =
            ReplicaQueryDescription::new(ReplicaQueryParameters::new(uuid::Uuid::nil()));
        replicas.paging = partitions.paging.clone();
        assert!(matches!(
            client.get_partition_list(&partitions, None).await,
            Err(ClientError::Local(Error::InvalidInput(
                "query does not support MaxResults"
            )))
        ));
        assert!(matches!(
            client.get_replica_list(&replicas, None).await,
            Err(ClientError::Local(Error::InvalidInput(
                "query does not support MaxResults"
            )))
        ));
        assert!(!client.is_closed());
    }
    for node in ["", "N00\0hidden"] {
        let query =
            DeployedApplicationQueryDescription::new(DeployedApplicationQueryParameters::new(node));
        assert!(matches!(
            client.get_deployed_application_list(&query, None).await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
        assert!(!client.is_closed());
        let application = "fabric:/Model".parse().unwrap();
        assert!(matches!(
            client
                .get_deployed_service_package_list(
                    &DeployedServicePackageQueryDescription::new(
                        DeployedServicePackageQueryParameters::new(node, application),
                    ),
                    None
                )
                .await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
        assert!(matches!(
            client
                .get_deployed_code_package_list(
                    &DeployedCodePackageQueryDescription::new(
                        DeployedCodePackageQueryParameters::new(
                            node,
                            "fabric:/Model".parse().unwrap()
                        ),
                    ),
                    None
                )
                .await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
        assert!(matches!(
            client
                .get_deployed_replica_list(
                    &DeployedReplicaQueryDescription::new(DeployedReplicaQueryParameters::new(
                        node,
                        "fabric:/Model".parse().unwrap()
                    ),),
                    None
                )
                .await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
        assert!(!client.is_closed());
    }
    client.ping(None).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[derive(Clone, Copy, Debug)]
enum NativeCompletion {
    Replicas,
    Hosting,
    Health,
    Movement,
}

impl NativeCompletion {
    const ALL: [Self; 4] = [Self::Replicas, Self::Hosting, Self::Health, Self::Movement];

    fn actor_action(self) -> (i64, &'static str) {
        match self {
            Self::Replicas | Self::Movement => (13, "QueryReply"),
            Self::Hosting => (5, "QueryReply"),
            Self::Health => (16, "OperationSuccessAction"),
        }
    }

    async fn call(self, client: &FabricClient) -> fube_client::ClientResult<()> {
        match self {
            Self::Replicas => QueryKind::DeployedReplicas.call(client, None).await,
            Self::Hosting => QueryKind::ServicePackages.call(client, None).await,
            Self::Health => client
                .get_node_health(
                    &NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("N0010")),
                    None,
                )
                .await
                .map(|health| {
                    assert_eq!(health.node_name, "N0010");
                    assert!(health.health_events.is_empty());
                }),
            Self::Movement => {
                client
                    .move_primary(
                        "00112233-4455-6677-8899-aabbccddeeff".parse().unwrap(),
                        "N0010",
                        None,
                    )
                    .await
            }
        }
    }

    fn reply(self, request: &Message, error: i64) -> Message {
        assert_eq!(request.properties().unwrap().action, "QueryRequest");
        let bytes = body(|e| {
            e.i64(match self {
                Self::Health => 1,
                Self::Movement => 0,
                _ => 2,
            });
            let tag = match self {
                Self::Replicas => Some(12i32.to_le_bytes()),
                Self::Hosting => Some(11i32.to_le_bytes()),
                Self::Health => Some(16i32.to_le_bytes()),
                Self::Movement => None,
            };
            e.pointer(tag.as_ref().map(|tag| tag.as_slice()), |e| {
                e.object_array::<()>(&[], |_, _| Ok(()))?;
                if matches!(self, Self::Health) {
                    e.pointer(Some(&[]), |e| {
                        e.string("N0010")?;
                        e.object_array::<()>(&[], |_, _| Ok(()))?;
                        e.i64(1);
                        e.object_array::<()>(&[], |_, _| Ok(()))
                    })
                } else {
                    e.pointer(None, |_| Ok(()))
                }
            })?;
            e.i64(error);
            e.pointer(None, |_| Ok(()))
        });
        let (actor, action) = self.actor_action();
        let mut response = reply(request, action, bytes);
        response.headers[0] = Header::actor(actor).unwrap();
        response
    }

    fn forwarded_reply(self, request: &Message, records: usize, error: i64) -> Message {
        let mut response = self.reply(request, error);
        if matches!(self, Self::Hosting) {
            response.headers.retain(|h| !matches!(h.id, ACTOR | ACTION));
        }
        let outer = response
            .headers
            .iter()
            .position(|h| h.id == RELATES_TO)
            .unwrap();
        response.headers.insert(
            outer,
            Header::relates_to(MessageId {
                guid: [91; 16],
                index: 17,
            })
            .unwrap(),
        );
        if records == 3 {
            response.headers.push(response.headers[outer + 1].clone());
        }
        assert_eq!(
            response
                .headers
                .iter()
                .filter(|h| h.id == RELATES_TO)
                .count(),
            records
        );
        response
    }
}

#[tokio::test]
async fn native_query_presence_matrix_accepts_rs_hosting_and_move_primary() {
    for profile in [
        NativeCompletion::Replicas,
        NativeCompletion::Hosting,
        NativeCompletion::Movement,
    ] {
        for presence in 0..4 {
            let (client, server) = reusable_reply(move |request| {
                let mut response = profile.reply(request, 0);
                response.headers.retain(|h| match h.id {
                    ACTOR => presence & 1 != 0,
                    ACTION => presence & 2 != 0,
                    _ => true,
                });
                response
            })
            .await;
            profile
                .call(&client)
                .await
                .unwrap_or_else(|error| panic!("{profile:?}, presence {presence}: {error}"));
            assert!(client.terminal_error().is_none());
            client.ping(None).await.unwrap();
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn native_query_presence_matrix_rejects_wrong_values_ipc_and_duplicate_headers() {
    for profile in [
        NativeCompletion::Replicas,
        NativeCompletion::Hosting,
        NativeCompletion::Movement,
    ] {
        for case in 0..10 {
            let terminal = case >= 4;
            let make_reply = move |request: &Message| {
                let mut response = profile.reply(request, 0);
                match case {
                    0 => {
                        let other_actor = if matches!(profile, NativeCompletion::Hosting) {
                            13
                        } else {
                            5
                        };
                        response.headers[0] = Header::actor(other_actor).unwrap();
                    }
                    1 => response.headers[0] = Header::actor(0).unwrap(),
                    2 => response.headers[0] = Header::actor(12).unwrap(),
                    3 => response.headers[1] = Header::action("WrongAction").unwrap(),
                    4 | 5 => response
                        .headers
                        .push(Header::boolean(IPC, case == 5).unwrap()),
                    6 => response.headers.push(response.headers[0].clone()),
                    7 => response.headers.push(response.headers[1].clone()),
                    8 => response.headers.extend([
                        Header::message_id(MessageId {
                            guid: [81; 16],
                            index: 1,
                        })
                        .unwrap(),
                        Header::message_id(MessageId {
                            guid: [81; 16],
                            index: 1,
                        })
                        .unwrap(),
                    ]),
                    _ => response.headers.push(
                        Header::boolean(fube_transport::message::UNCORRELATED_REPLY, true).unwrap(),
                    ),
                }
                response
            };
            let (client, server) = if terminal {
                single_reply(make_reply).await
            } else {
                reusable_reply(make_reply).await
            };
            let result = profile.call(&client).await;
            if terminal {
                assert!(
                    matches!(result, Err(ClientError::Transport(Error::Protocol(_)))),
                    "{profile:?}, case {case}: {result:?}"
                );
                assert!(client.is_closed());
            } else {
                assert!(
                    matches!(result, Err(ClientError::Protocol(_))),
                    "{profile:?}, case {case}: {result:?}"
                );
                assert!(client.terminal_error().is_none());
                client.ping(None).await.unwrap();
            }
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn native_query_identical_two_record_correlation_and_expects_reply_baseline_is_unchanged() {
    for profile in NativeCompletion::ALL {
        let (client, server) = reusable_reply(move |request| {
            let mut response = profile.reply(request, 0);
            response.headers.push(response.headers[2].clone());
            response.headers.extend([
                Header::boolean(fube_transport::message::EXPECTS_REPLY, false).unwrap(),
                Header::boolean(fube_transport::message::EXPECTS_REPLY, false).unwrap(),
            ]);
            response
        })
        .await;
        profile.call(&client).await.unwrap();
        assert!(client.terminal_error().is_none());
        client.ping(None).await.unwrap();
        client.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn native_query_domain_header_duplicates_remain_call_local_even_when_forwarded() {
    for profile in NativeCompletion::ALL {
        for forwarded in [false, true] {
            for header in [
                Header::activity([3; 16]).unwrap(),
                Header::timeout(123).unwrap(),
            ] {
                let (client, server) = reusable_reply(move |request| {
                    let mut response = if forwarded {
                        profile.forwarded_reply(request, 3, 0)
                    } else {
                        profile.reply(request, 0)
                    };
                    response.headers.extend([header.clone(), header]);
                    response
                })
                .await;
                let result = profile.call(&client).await;
                assert!(
                    matches!(result, Err(ClientError::Protocol(_))),
                    "{profile:?}, forwarded {forwarded}: {result:?}"
                );
                assert!(client.terminal_error().is_none());
                client.ping(None).await.unwrap();
                client.close().await.unwrap();
                server.await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn native_forwarded_query_success_and_domain_errors_accept_two_or_three_records() {
    for profile in NativeCompletion::ALL {
        for records in [2, 3] {
            for error in [0, REMOTE_CODE] {
                let (client, server) =
                    reusable_reply(move |request| profile.forwarded_reply(request, records, error))
                        .await;
                let result = profile.call(&client).await;
                if error == 0 {
                    result.unwrap_or_else(|error| panic!("{profile:?}, {records}: {error}"));
                } else {
                    assert!(
                        matches!(
                            result,
                            Err(ClientError::Domain(DomainFailure {
                                code: REMOTE_CODE,
                                ..
                            }))
                        ),
                        "{profile:?}, {records}: {result:?}"
                    );
                }
                assert!(client.terminal_error().is_none());
                client.ping(None).await.unwrap();
                client.close().await.unwrap();
                server.await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn native_forwarded_queries_still_decode_the_selected_operations_profile() {
    for profile in NativeCompletion::ALL {
        for records in [2, 3] {
            for wrong_body in [false, true] {
                let (client, server) = reusable_reply(move |request| {
                    let mut response = profile.forwarded_reply(request, records, 0);
                    let other = match profile {
                        NativeCompletion::Replicas | NativeCompletion::Movement => {
                            NativeCompletion::Health
                        }

                        _ => NativeCompletion::Replicas,
                    };
                    if wrong_body {
                        response.body = other.reply(request, 0).body;
                    } else {
                        response.headers.retain(|h| !matches!(h.id, ACTOR | ACTION));
                        let (actor, action) = other.actor_action();
                        response.headers.insert(0, Header::actor(actor).unwrap());
                        response.headers.insert(1, Header::action(action).unwrap());
                    }
                    response
                })
                .await;
                let result = profile.call(&client).await;
                assert!(
                    matches!(result, Err(ClientError::Protocol(_))),
                    "{profile:?}, {records}, wrong body {wrong_body}: {result:?}"
                );
                assert!(client.terminal_error().is_none());
                client.ping(None).await.unwrap();
                client.close().await.unwrap();
                server.await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn native_forwarded_queries_reject_ipc_extra_or_nonrepeating_correlation() {
    for profile in NativeCompletion::ALL {
        for case in 0..4 {
            let (client, server) = single_reply(move |request| {
                let mut response = profile.forwarded_reply(request, 3, 0);
                match case {
                    0 | 1 => response
                        .headers
                        .push(Header::boolean(IPC, case == 1).unwrap()),
                    2 => response
                        .headers
                        .push(response.headers.last().unwrap().clone()),
                    _ => {
                        *response.headers.last_mut().unwrap() = Header::relates_to(MessageId {
                            guid: [92; 16],
                            index: 18,
                        })
                        .unwrap();
                    }
                }
                response
            })
            .await;
            let result = profile.call(&client).await;
            assert!(
                matches!(result, Err(ClientError::Transport(Error::Protocol(_)))),
                "{profile:?}, case {case}: {result:?}"
            );
            assert!(client.is_closed());
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}
