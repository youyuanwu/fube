//! Shared transport behavior for the additional native list query families.
use super::*;
use mssf_client::{
    DeployedApplicationQueryDescription, DeployedApplicationQueryParameters,
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
}

impl QueryKind {
    const ALL: [Self; 3] = [Self::Partitions, Self::Replicas, Self::Deployments];

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
                match timeout {
                    Some(timeout) => client
                        .get_partition_list_with_timeout(&query, timeout)
                        .await
                        .map(|_| ()),
                    None => client.get_partition_list(&query).await.map(|_| ()),
                }
            }
            Self::Replicas => {
                let query = ReplicaQueryDescription::new(ReplicaQueryParameters::new(
                    uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                ));
                match timeout {
                    Some(timeout) => client
                        .get_replica_list_with_timeout(&query, timeout)
                        .await
                        .map(|_| ()),
                    None => client.get_replica_list(&query).await.map(|_| ()),
                }
            }
            Self::Deployments => {
                let query = DeployedApplicationQueryDescription::new(
                    DeployedApplicationQueryParameters::new("N0010"),
                );
                match timeout {
                    Some(timeout) => client
                        .get_deployed_application_list_with_timeout(&query, timeout)
                        .await
                        .map(|_| ()),
                    None => client
                        .get_deployed_application_list(&query)
                        .await
                        .map(|_| ()),
                }
            }
        }
    }
}

fn native_error(request: &Message) -> Message {
    #[derive(Serialize)]
    struct QueryError {
        kind: i64,
        payload: Pointer<FixedTag<EmptyObjectArray, 0>>,
        error: i64,
        paging: Pointer<EmptyObjectArray>,
    }
    reply(
        request,
        "QueryReply",
        mssf_transport::serialization::serde::to_vec(&QueryError {
            kind: 0,
            payload: Pointer(None),
            error: REMOTE_CODE,
            paging: Pointer(None),
        })
        .unwrap(),
    )
}

fn ping_reply(request: &Message) -> Message {
    let mut response = reply(request, "PingRequest", ping_body());
    response.headers.push(Header::version(1, 1).unwrap());
    response
}

#[tokio::test]
async fn native_errors_from_each_new_query_leave_shared_connection_usable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        for _ in QueryKind::ALL {
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(request.properties().unwrap().action, "QueryRequest");
            write_message(&mut stream, &native_error(&request))
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
        client.ping().await.unwrap();
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
        assert!(client.ping().await.is_err());
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
            assert!(client.ping().await.is_err());
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
            client.get_partition_list(&partitions).await,
            Err(Error::InvalidInput("query does not support MaxResults"))
        ));
        assert!(matches!(
            client.get_replica_list(&replicas).await,
            Err(Error::InvalidInput("query does not support MaxResults"))
        ));
        assert!(!client.is_closed());
    }
    for node in ["", "N00\0hidden"] {
        let query =
            DeployedApplicationQueryDescription::new(DeployedApplicationQueryParameters::new(node));
        assert!(matches!(
            client.get_deployed_application_list(&query).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(!client.is_closed());
    }
    client.ping().await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}
