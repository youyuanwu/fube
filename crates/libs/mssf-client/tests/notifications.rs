//! Synthetic schema and lifecycle tests, not a Service Fabric runtime oracle.
//! Positional field checks bypass production DTOs but share the production Rust codec.
use mssf_client::*;
use mssf_transport::{
    connection::{read_message, write_message},
    message::{Header, Message, MessageId, EXPECTS_REPLY, UNCORRELATED_REPLY},
    serialization::Decoder,
};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
#[path = "support/notification_ingress_tests.rs"]
mod ingress;
#[path = "support/gateway_peer.rs"]
mod peer;
use peer::body;

const WAIT: Duration = Duration::from_secs(3);
async fn connect_peer(listener: TcpListener) -> (TcpStream, String) {
    let mut stream = peer::accept(listener).await;
    let request = read_message(&mut stream).await.unwrap();
    let p = request.properties().unwrap();
    assert_eq!(p.action, "NotificationClientConnectionRequest");
    assert!(p.high_priority);
    let mut d = Decoder::new(&request.body);
    let id = d
        .object(|d| {
            let id = d.string()?;
            assert!(uuid::Uuid::parse_str(&id).is_ok());
            d.object(|d| {
                assert_eq!(d.i64()?, 0);
                assert_eq!(d.byte_array()?, vec![0; 16]);
                Ok(())
            })?;
            d.pointer(|d, tag| {
                assert!(tag.is_empty());
                assert!(d.object_array(|_| Ok(())).unwrap().is_empty());
                Ok(())
            })?
            .unwrap();
            assert_eq!(d.u32()?, 0);
            Ok(id)
        })
        .unwrap();
    d.finish().unwrap();
    let reply = body(|e| {
        e.object(|e| {
            e.i64(1);
            e.byte_array(&[0; 16])
        })?;
        e.i64(0);
        e.object(|e| {
            e.string("private.example:19000")?;
            e.object(|e| {
                e.byte_array(&[0; 16])?;
                e.u64(42);
                Ok(())
            })?;
            e.string("native-peer")
        })
    });
    write_message(
        &mut stream,
        &peer::reply(&request, "NotificationClientConnectionReply", reply),
    )
    .await
    .unwrap();
    (stream, id)
}
fn register(request: &Message, expected: &str) -> u64 {
    assert_eq!(
        request.properties().unwrap().action,
        "RegisterServiceNotificationFilterRequest"
    );
    let mut d = Decoder::new(&request.body);
    let id = d
        .object(|d| {
            assert_eq!(d.string()?, expected);
            d.pointer(|d, _| {
                let id = d.u64()?;
                // NamingUri structure is independently covered by resolution peers.
                d.object(|_| Ok(()))?;
                d.object(|d| {
                    assert!(d.u32()? <= 3);
                    Ok(())
                })?;
                Ok(id)
            })
            .map(|id| id.unwrap())
        })
        .unwrap();
    d.finish().unwrap();
    id
}
fn unregister(request: &Message, expected: &str) -> u64 {
    assert_eq!(
        request.properties().unwrap().action,
        "UnregisterServiceNotificationFilterRequest"
    );
    let mut d = Decoder::new(&request.body);
    let id = d
        .object(|d| {
            assert_eq!(d.string()?, expected);
            d.u64()
        })
        .unwrap();
    d.finish().unwrap();
    id
}
fn page(index: u32, version: i64, count: usize) -> Message {
    page_with_endpoints(index, version, count, &[])
}
fn page_with_endpoints(index: u32, version: i64, count: usize, endpoints: &[String]) -> Message {
    Message {
        headers: vec![
            Header::actor(5).unwrap(),
            Header::action("ServiceNotificationRequest").unwrap(),
            Header::message_id(MessageId {
                guid: [4; 16],
                index,
            })
            .unwrap(),
            Header::boolean(UNCORRELATED_REPLY, true).unwrap(),
            Header::boolean(EXPECTS_REPLY, true).unwrap(),
        ],
        body: body(|e| {
            e.pointer(Some(&[]), |e| {
                e.object(|e| {
                    e.object(|e| {
                        e.guid(&[1; 16]);
                        e.u64(1);
                        Ok(())
                    })?;
                    e.u64(0);
                    Ok(())
                })?;
                e.object(|e| {
                    e.i64(1);
                    e.byte_array(&[0; 16])
                })?;
                e.pointer(Some(&[]), |e| {
                    e.object_array(&[(version, version + count as i64)], |e, (start, end)| {
                        e.i64(*start);
                        e.i64(*end);
                        Ok(())
                    })
                })?;
                e.u32(count as u32);
                for id in 0..count {
                    e.pointer(Some(&[]), |e| {
                        e.pointer(Some(&[]), |e| {
                            e.object(|e| {
                                e.guid(&uuid::Uuid::from_u128(id as u128 + 1).to_bytes_le());
                                Ok(())
                            })?;
                            e.string("fabric:/App/Service")?;
                            e.object(|e| {
                                e.boolean(false);
                                e.boolean(false);
                                e.string("")?;
                                e.u32(endpoints.len() as u32);
                                for endpoint in endpoints {
                                    e.string(endpoint)?;
                                }
                                e.i64(version + id as i64);
                                Ok(())
                            })?;
                            e.boolean(true);
                            Ok(())
                        })?;
                        e.boolean(false);
                        Ok(())
                    })?;
                }
                Ok(())
            })
            .map(|_| ())
        }),
    }
}
async fn ack(stream: &mut TcpStream, index: u32, success: bool) {
    let reply = read_message(stream).await.unwrap();
    assert_eq!(
        reply.properties().unwrap().relates_to,
        MessageId {
            guid: [4; 16],
            index
        }
    );
    assert_eq!(
        reply.properties().unwrap().action,
        "ServiceNotificationReply"
    );
    let mut d = Decoder::new(&reply.body);
    d.object(|d| {
        assert_eq!(d.i64()? == 0, success);
        Ok(())
    })
    .unwrap();
    d.finish().unwrap();
}
fn filter() -> ServiceNotificationFilter {
    ServiceNotificationFilter {
        name: "fabric:/App/Service".parse().unwrap(),
        name_prefix: false,
        primary_only: false,
    }
}

#[tokio::test]
async fn thirty_two_filter_slots_include_pending_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (started, cleanup_started) = tokio::sync::oneshot::channel();
    let (allow, allowed) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        for _ in 0..32 {
            let req = read_message(&mut s).await.unwrap();
            register(&req, &id);
            write_message(
                &mut s,
                &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
            )
            .await
            .unwrap();
        }
        let remove = read_message(&mut s).await.unwrap();
        unregister(&remove, &id);
        started.send(()).unwrap();
        allowed.await.unwrap();
        write_message(
            &mut s,
            &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let req = read_message(&mut s).await.unwrap();
        register(&req, &id);
        write_message(
            &mut s,
            &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        for _ in 0..32 {
            let remove = read_message(&mut s).await.unwrap();
            unregister(&remove, &id);
            write_message(
                &mut s,
                &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
            )
            .await
            .unwrap();
        }
        assert!(read_message(&mut s).await.is_err());
    });
    let client = ServiceNotificationClient::connect_clear(addr, WAIT)
        .await
        .unwrap();
    let mut guards = Vec::new();
    for _ in 0..32 {
        guards.push(client.register_filter(&filter(), None).await.unwrap());
    }
    assert!(matches!(
        client.register_filter(&filter(), None).await,
        Err(NotificationError::ResourceLimit(_))
    ));
    drop(guards.pop());
    cleanup_started.await.unwrap();
    assert!(matches!(
        client.register_filter(&filter(), None).await,
        Err(NotificationError::ResourceLimit(_))
    ));
    allow.send(()).unwrap();
    let guard = tokio::time::timeout(WAIT, async {
        loop {
            match client.register_filter(&filter(), None).await {
                Err(NotificationError::ResourceLimit(_)) => tokio::task::yield_now().await,
                result => break result.unwrap(),
            }
        }
    })
    .await
    .unwrap();
    guards.push(guard);
    client.close(None).await.unwrap();
    drop(guards);
    task.await.unwrap();
}

#[tokio::test]
async fn failed_or_cancelled_construction_closes_candidate_socket() {
    for cancel in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen, wait) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut s = peer::accept(listener).await;
            let req = read_message(&mut s).await.unwrap();
            assert_eq!(
                req.properties().unwrap().action,
                "NotificationClientConnectionRequest"
            );
            let _ = seen.send(());
            assert!(read_message(&mut s).await.is_err());
        });
        if cancel {
            tokio::select! {
                biased;
                _=wait=>{},
                result=ServiceNotificationClient::connect_clear(addr,WAIT)=>panic!("unexpected construction result: {}",result.is_ok()),
            }
        } else {
            assert!(matches!(
                ServiceNotificationClient::connect_clear(addr, Duration::from_millis(100)).await,
                Err(NotificationError::Common(Error::Timeout))
            ));
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn independent_public_clients_survive_other_client_close_and_connection_faults() {
    for ordinary_fails in [false, true] {
        let ordinary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ordinary_address = ordinary_listener.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let normal_peer = tokio::spawn(async move {
            let mut s = peer::accept(ordinary_listener).await;
            if ordinary_fails {
                return;
            }
            while let Ok(req) = read_message(&mut s).await {
                let data = body(|e| {
                    e.boolean(true);
                    e.i64(0);
                    Ok(())
                });
                write_message(&mut s, &peer::reply(&req, "NameOperationReply", data))
                    .await
                    .unwrap();
            }
        });
        let notification_peer = tokio::spawn(async move {
            let (mut s, id) = connect_peer(listener).await;
            if !ordinary_fails {
                return;
            }
            while let Ok(req) = read_message(&mut s).await {
                let action = if req.properties().unwrap().action
                    == "RegisterServiceNotificationFilterRequest"
                {
                    register(&req, &id);
                    "RegisterServiceNotificationFilterReply"
                } else {
                    unregister(&req, &id);
                    "UnregisterServiceNotificationFilterReply"
                };
                write_message(&mut s, &peer::reply(&req, action, vec![]))
                    .await
                    .unwrap();
            }
        });
        let ordinary = FabricClient::connect_clear(ordinary_address, WAIT)
            .await
            .unwrap();
        let mut client = ServiceNotificationClient::connect_clear(addr, WAIT)
            .await
            .unwrap();
        if ordinary_fails {
            normal_peer.await.unwrap();
            ordinary.close().await.unwrap();
            let guard = client.register_filter(&filter(), None).await.unwrap();
            guard.unregister(None).await.unwrap();
            client.close(None).await.unwrap();
            notification_peer.await.unwrap();
        } else {
            notification_peer.await.unwrap();
            assert!(matches!(client.recv().await, Some(Err(_))));
            drop(client);
            assert!(ordinary.name_exists("fabric:/App", None).await.unwrap());
            ordinary.close().await.unwrap();
            normal_peer.await.unwrap();
        }
    }
}

#[tokio::test]
async fn early_update_retry_explicit_and_drop_cleanup_and_surviving_guard_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (retried, retry_done) = tokio::sync::oneshot::channel();
    let (cleaned, cleanup_done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        let first = read_message(&mut s).await.unwrap();
        let first_id = register(&first, &id);
        write_message(&mut s, &page(10, 1, 1)).await.unwrap();
        ack(&mut s, 10, true).await;
        write_message(
            &mut s,
            &peer::reply(&first, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        write_message(&mut s, &page(11, 1, 1)).await.unwrap();
        ack(&mut s, 11, true).await;
        retried.send(()).unwrap();
        let second = read_message(&mut s).await.unwrap();
        let second_id = register(&second, &id);
        assert_ne!(first_id, second_id);
        write_message(
            &mut s,
            &peer::reply(&second, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let remove = read_message(&mut s).await.unwrap();
        assert_eq!(unregister(&remove, &id), first_id);
        write_message(
            &mut s,
            &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let remove = read_message(&mut s).await.unwrap();
        assert_eq!(unregister(&remove, &id), second_id);
        write_message(
            &mut s,
            &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        cleaned.send(()).unwrap();
        let third = read_message(&mut s).await.unwrap();
        let third_id = register(&third, &id);
        write_message(
            &mut s,
            &peer::reply(&third, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let remove = read_message(&mut s).await.unwrap();
        assert_eq!(unregister(&remove, &id), third_id);
        write_message(
            &mut s,
            &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        assert!(read_message(&mut s).await.is_err());
    });
    let mut client = ServiceNotificationClient::connect_clear(addr, WAIT)
        .await
        .unwrap();
    assert_eq!(client.gateway().address, "private.example:19000");
    let one = client.register_filter(&filter(), None).await.unwrap();
    assert!(client.recv().await.unwrap().unwrap().endpoints.is_empty());
    retry_done.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), client.recv())
            .await
            .is_err()
    );
    let two = client.register_filter(&filter(), None).await.unwrap();
    one.unregister(None).await.unwrap();
    drop(two);
    cleanup_done.await.unwrap();
    let three = client.register_filter(&filter(), None).await.unwrap();
    client.close(None).await.unwrap();
    assert!(matches!(
        three.unregister(None).await,
        Err(NotificationError::Common(Error::Closed))
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn full_delivery_reports_terminal_once_and_does_not_ack_lost_page_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (registered, registration_done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        let req = read_message(&mut s).await.unwrap();
        register(&req, &id);
        write_message(
            &mut s,
            &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        // The injected failure must follow local registration completion.
        registration_done.await.unwrap();
        write_message(&mut s, &page(1, 1, 256)).await.unwrap();
        ack(&mut s, 1, true).await;
        write_message(&mut s, &page(2, 1000, 1)).await.unwrap();
        ack(&mut s, 2, false).await;
        assert!(read_message(&mut s).await.is_err());
    });
    let mut client = ServiceNotificationClient::connect_clear(addr, WAIT)
        .await
        .unwrap();
    let _guard = client.register_filter(&filter(), None).await.unwrap();
    registered.send(()).unwrap();
    task.await.unwrap();
    assert!(matches!(
        client.recv().await,
        Some(Err(NotificationError::Lagged))
    ));
    assert!(client.recv().await.is_none());
}

#[tokio::test]
async fn registration_cancellation_after_send_retires_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (seen, wait) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        let req = read_message(&mut s).await.unwrap();
        register(&req, &id);
        let _ = seen.send(());
        assert!(read_message(&mut s).await.is_err());
    });
    let mut client = ServiceNotificationClient::connect_clear(addr, WAIT)
        .await
        .unwrap();
    let f = filter();
    tokio::select! {
        biased;
        _=wait=>{},
        result=client.register_filter(&f,None)=>panic!("unexpected result: {}",result.is_ok()),
    }
    assert!(matches!(client.recv().await, Some(Err(_))));
    assert!(client.recv().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn cleanup_timeout_is_terminal_and_owner_drop_closes_with_guards_alive() {
    for explicit in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut s, id) = connect_peer(listener).await;
            let req = read_message(&mut s).await.unwrap();
            register(&req, &id);
            write_message(
                &mut s,
                &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
            )
            .await
            .unwrap();
            if explicit {
                let req = read_message(&mut s).await.unwrap();
                unregister(&req, &id);
            }
            assert!(read_message(&mut s).await.is_err());
        });
        let mut client = ServiceNotificationClient::connect_clear(addr, Duration::from_millis(100))
            .await
            .unwrap();
        let guard = client.register_filter(&filter(), Some(WAIT)).await.unwrap();
        if explicit {
            assert!(guard
                .unregister(Some(Duration::from_millis(10)))
                .await
                .is_err());
            assert!(matches!(
                client.recv().await,
                Some(Err(NotificationError::Common(Error::Timeout)))
            ));
            assert!(client.recv().await.is_none());
        } else {
            drop(client);
            drop(guard);
        }
        task.await.unwrap();
    }
}
