//! Synthetic positional schema tests, not native runtime qualification.
//! Field order is checked separately from production DTOs using the shared Rust codec.
use fube_client::*;
use fube_transport::{
    connection::{read_message, write_message},
    serialization::Decoder,
};
use std::{cmp::Ordering, time::Duration};
use tokio::net::TcpListener;
use uuid::Uuid;
#[path = "support/gateway_peer.rs"]
mod peer;
use peer::body;

const WAIT: Duration = Duration::from_secs(3);
const ID: [u8; 16] = [6; 16];

fn result(kind: i64, low: i64, high: i64, name: &str, version: i64, group: bool) -> Vec<u8> {
    result_for_service(kind, low, high, name, version, group, "fabric:/App/Service")
}

fn result_for_service(
    kind: i64,
    low: i64,
    high: i64,
    name: &str,
    version: i64,
    group: bool,
    service: &str,
) -> Vec<u8> {
    body(|e| {
        e.boolean(group);
        e.object(|e| {
            e.object(|e| {
                e.guid(&ID);
                Ok(())
            })?;
            e.string(service)?;
            e.object(|e| {
                e.boolean(true);
                e.boolean(true);
                e.string("{opaque primary}")?;
                e.u32(1);
                e.string("secondary")?;
                e.i64(version);
                Ok(())
            })?;
            e.boolean(true);
            Ok(())
        })?;
        e.object(|e| {
            e.i64(kind);
            e.i64(low);
            e.i64(high);
            e.string(name)
        })?;
        e.i64(11);
        e.object(|e| {
            e.i64(7);
            e.byte_array(&[0; 16])
        })?;
        e.pointer(None, |_| unreachable!())?;
        Ok(())
    })
}

fn request(body: &[u8], kind: i64, integer: i64, named: &str, previous: bool) {
    let mut d = Decoder::new(body);
    d.object(|d| {
        d.object(|d| {
            assert_eq!(d.i64()?, 1);
            assert_eq!(d.string()?, "fabric");
            assert_eq!(d.string()?, "");
            assert_eq!(d.i64()?, 0);
            assert_eq!(d.string()?, "");
            assert_eq!(d.i32()?, -1);
            assert_eq!(d.string()?, "/App/Service");
            assert_eq!(d.string()?, "");
            assert_eq!(d.string()?, "");
            assert_eq!(d.u32()?, 2);
            assert_eq!(d.string()?, "App");
            assert_eq!(d.string()?, "Service");
            Ok(())
        })?;
        d.object(|d| {
            d.object(|d| {
                assert_eq!(d.i64()?, kind);
                assert_eq!(d.i64()?, integer);
                assert_eq!(d.string()?, named);
                assert!(!d.boolean()?);
                Ok(())
            })?;
            d.object(|d| {
                assert_eq!(d.i64()?, if previous { 21 } else { -1 });
                d.object(|d| {
                    assert_eq!(d.i64()?, if previous { 7 } else { 0 });
                    assert_eq!(d.byte_array()?, vec![0; 16]);
                    Ok(())
                })?;
                assert_eq!(d.i64()?, if previous { 11 } else { -1 });
                Ok(())
            })?;
            d.object(|d| {
                assert_eq!(d.guid()?, if previous { ID } else { [0; 16] });
                Ok(())
            })?;
            assert!(!d.boolean()?);
            Ok(())
        })?;
        assert!(!d.boolean()?);
        Ok(())
    })
    .unwrap();
    d.finish().unwrap();
}

#[tokio::test]
async fn keys_refresh_binding_and_versions() {
    for (key, kind, integer, named, partition_kind, low, high) in [
        (PartitionKey::Singleton, 1, 0, "", 1, 0, 0),
        (
            PartitionKey::Int64(i64::MIN),
            2,
            i64::MIN,
            "",
            2,
            i64::MIN,
            i64::MAX,
        ),
        (
            PartitionKey::Named("🦀 partition / not a URI".into()),
            3,
            0,
            "🦀 partition / not a URI",
            3,
            0,
            0,
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut s = peer::accept(listener).await;
            for previous in [false, true, true] {
                let req = read_message(&mut s).await.unwrap();
                assert_eq!(req.properties().unwrap().action, "ResolveServiceRequest");
                request(&req.body, kind, integer, named, previous);
                write_message(
                    &mut s,
                    &peer::reply(
                        &req,
                        "ResolveServiceRequest",
                        result(partition_kind, low, high, named, 21, false),
                    ),
                )
                .await
                .unwrap();
            }
            // Wait for close so the caller may perform local rejections.
            assert!(read_message(&mut s).await.is_err());
        });
        let client = FabricClient::connect_clear(addr, WAIT).await.unwrap();
        let name = NamingUri::parse("fabric:/App/Service").unwrap();
        let first = client
            .resolve_service_partition(&name, &key, None, None)
            .await
            .unwrap();
        assert_eq!(first.partition_information.id, Uuid::from_bytes_le(ID));
        assert_eq!(
            first.endpoints[0].role,
            ServiceEndpointRole::StatefulPrimary
        );
        assert_eq!(first.endpoints[0].address, "{opaque primary}");
        assert_eq!(
            first.endpoints[1].role,
            ServiceEndpointRole::StatefulSecondary
        );
        let refreshed = client
            .resolve_service_partition(&name, &key, Some(&first), Some(WAIT))
            .await
            .unwrap();
        assert_eq!(
            first.version.compare(&refreshed.version).unwrap(),
            Ordering::Equal
        );
        let again = client
            .resolve_service_partition(&name, &key, Some(&first), None)
            .await
            .unwrap();
        assert_eq!(again.endpoints, first.endpoints);
        let mut altered = first.clone();
        altered.partition_information.id = Uuid::new_v4();
        assert!(matches!(
            client
                .resolve_service_partition(&name, &key, Some(&altered), None)
                .await,
            Err(ClientError::Local(Error::InvalidInput(_)))
        ));
        let other = NamingUri::parse("fabric:/Other").unwrap();
        assert!(client
            .resolve_service_partition(&other, &key, Some(&first), None)
            .await
            .is_err());
        for invalid in ["", "bad\0name"] {
            assert!(matches!(
                client
                    .resolve_service_partition(
                        &name,
                        &PartitionKey::Named(invalid.into()),
                        None,
                        None
                    )
                    .await,
                Err(ClientError::Local(Error::InvalidInput(_)))
            ));
        }
        client.close().await.unwrap();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn groups_malformed_and_mismatched_replies_are_call_local() {
    for bytes in [
        result(1, 0, 0, "", 1, true),
        vec![0, 1, 2],
        result(2, 0, 10, "", 1, false),
        result(3, 0, 0, "other-partition", 1, false),
        result_for_service(1, 0, 0, "", 1, false, "fabric:/Other"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut s = peer::accept(listener).await;
            let req = read_message(&mut s).await.unwrap();
            let neighbor = read_message(&mut s).await.unwrap();
            assert_eq!(req.properties().unwrap().action, "ResolveServiceRequest");
            assert_eq!(neighbor.properties().unwrap().action, "NameExistsRequest");
            write_message(
                &mut s,
                &peer::reply(
                    &neighbor,
                    "NameOperationReply",
                    body(|e| {
                        e.boolean(true);
                        e.i64(0);
                        Ok(())
                    }),
                ),
            )
            .await
            .unwrap();
            write_message(&mut s, &peer::reply(&req, "ResolveServiceRequest", bytes))
                .await
                .unwrap();
            let reuse = read_message(&mut s).await.unwrap();
            write_message(
                &mut s,
                &peer::reply(
                    &reuse,
                    "ResolveServiceRequest",
                    result(1, 0, 0, "", 1, false),
                ),
            )
            .await
            .unwrap();
            assert!(read_message(&mut s).await.is_err());
        });
        let client = FabricClient::connect_clear(addr, WAIT).await.unwrap();
        let name = "fabric:/App/Service".parse().unwrap();
        let (outcome, neighbor) = tokio::join!(
            client.resolve_service_partition(&name, &PartitionKey::Singleton, None, None),
            client.name_exists("fabric:/neighbor", None)
        );
        assert!(
            matches!(outcome, Err(ClientError::Protocol(_))),
            "{outcome:?}"
        );
        assert!(neighbor.unwrap());
        assert!(!client.is_closed());
        assert!(client.terminal_error().is_none());
        client
            .resolve_service_partition(&name, &PartitionKey::Singleton, None, None)
            .await
            .unwrap();
        client.close().await.unwrap();
        task.await.unwrap();
    }
}
