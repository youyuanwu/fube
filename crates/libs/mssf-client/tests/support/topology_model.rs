//! Shared transport behavior for the additional native list query families.
use super::*;
use mssf_client::{
    DeployedApplicationQueryDescription, DeployedApplicationQueryParameters,
    DeployedCodePackageQueryDescription, DeployedCodePackageQueryParameters,
    DeployedReplicaQueryDescription, DeployedReplicaQueryParameters,
    DeployedServicePackageQueryDescription, DeployedServicePackageQueryParameters,
    PartitionQueryDescription, PartitionQueryParameters, ReplicaQueryDescription,
    ReplicaQueryParameters,
};
use mssf_transport::serialization::serde::{EmptyObjectArray, FixedTag, Pointer};
use serde::Serialize;

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
    ) -> mssf_client::Result<()> {
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
        mssf_transport::serialization::serde::to_vec(&QueryError {
            kind: 0,
            payload: Pointer(None),
            error: REMOTE_CODE,
            paging: Pointer(None),
        })
        .unwrap(),
    )
}

#[tokio::test]
async fn unpaged_deployment_continuations_retire_connection_even_with_native_error() {
    for kind in [
        QueryKind::ServicePackages,
        QueryKind::CodePackages,
        QueryKind::DeployedReplicas,
    ] {
        for token in ["", "unexpected-next"] {
            for error in [0, REMOTE_CODE] {
                let (client, server) = single_reply(move |request| {
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
                    Err(Error::Protocol(_))
                ));
                assert!(client.is_closed());
                assert!(client.ping(None).await.is_err());
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
            Err(Error::Remote {
                code: REMOTE_CODE,
                ..
            })
        ));
        assert!(!client.is_closed());
        client.ping(None).await.unwrap();
    }
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_new_query_successes_retire_the_shared_connection() {
    for kind in QueryKind::ALL {
        let (client, server) =
            single_reply(|request| reply(request, "QueryReply", Vec::new())).await;
        assert!(matches!(
            kind.call(&client, Some(TIMEOUT_DURATION)).await,
            Err(Error::Protocol(_))
        ));
        assert!(client.is_closed());
        assert!(client.ping(None).await.is_err());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn partition_field_adapters_reject_bad_tags_and_guid_metadata_before_native_errors() {
    for malformed in 0..3 {
        for error in [0, REMOTE_CODE] {
            let (client, server) = single_reply(move |request| {
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
                Err(Error::Protocol(_))
            ));
            assert!(client.is_closed());
            assert!(client.ping(None).await.is_err());
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
                Err(Error::InvalidInput(_))
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
            Err(Error::InvalidInput("query does not support MaxResults"))
        ));
        assert!(matches!(
            client.get_replica_list(&replicas, None).await,
            Err(Error::InvalidInput("query does not support MaxResults"))
        ));
        assert!(!client.is_closed());
    }
    for node in ["", "N00\0hidden"] {
        let query =
            DeployedApplicationQueryDescription::new(DeployedApplicationQueryParameters::new(node));
        assert!(matches!(
            client.get_deployed_application_list(&query, None).await,
            Err(Error::InvalidInput(_))
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
            Err(Error::InvalidInput(_))
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
            Err(Error::InvalidInput(_))
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
            Err(Error::InvalidInput(_))
        ));
        assert!(!client.is_closed());
    }
    client.ping(None).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}
