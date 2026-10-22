use super::*;
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

async fn write(client: &FabricClient, delete: bool, timeout: Duration) -> mssf_client::Result<()> {
    if delete {
        client
            .delete_name_with_timeout("fabric:/App_1/Name-2", timeout)
            .await
    } else {
        client
            .create_name_with_timeout("fabric:/App_1/Name-2", timeout)
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
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    client.create_name("fabric:/App_1/Name-2").await.unwrap();
    write(&client, false, TIMEOUT_DURATION).await.unwrap();
    client.delete_name("fabric:/App_1/Name-2").await.unwrap();
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
                        encoder.i64(code);
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
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    for (delete, code) in [(false, DUPLICATE), (true, MISSING)] {
        assert!(matches!(
            write(&client, delete, TIMEOUT_DURATION).await,
            Err(Error::Remote { code: actual, message }) if actual == code && message == "native name diagnostic"
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
        assert_eq!(request.properties().unwrap().action, "NameExistsRequest");
        write_message(
            &mut stream,
            &reply(&request, "NameOperationReply", naming_body(false, 0)),
        )
        .await
        .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    for name in [
        "".into(),
        "fabric:/".into(),
        "fabric:/a//b".into(),
        "fabric:/é".into(),
        format!("fabric:/{}", "a".repeat(4089)),
    ] {
        assert!(matches!(
            client.create_name(&name).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            client.delete_name(&name).await,
            Err(Error::InvalidInput(_))
        ));
    }
    for delete in [false, true] {
        for budget in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
            assert!(matches!(
                write(&client, delete, budget).await,
                Err(Error::InvalidInput(_))
            ));
        }
    }
    assert!(!client.name_exists("fabric:/neighbor").await.unwrap());
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_write_success_retires_connection() {
    for delete in [false, true] {
        for shape in 0..5 {
            let (client, server) = single_reply(move |request| {
                let mut response = reply(request, "NameOperationReply", vec![]);
                match shape {
                    0 => response.body = body(|_| Ok(())),
                    1 => response.body = naming_body(true, 0),
                    2 => response.body = vec![0xff],
                    3 => response.headers[0] = Header::actor(6).unwrap(),
                    _ => response.headers[1] = Header::action("CreateNameReply").unwrap(),
                }
                response
            })
            .await;
            assert!(matches!(
                write(&client, delete, TIMEOUT_DURATION).await,
                Err(Error::Protocol(_))
            ));
            assert!(client.is_closed());
            assert!(matches!(client.terminal_error(), Some(Error::Protocol(_))));
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn malformed_write_retires_without_waiter_repoll_and_fails_neighbor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let a = read_message(&mut stream).await.unwrap();
        let b = read_message(&mut stream).await.unwrap();
        let request = if a.properties().unwrap().action == "DeleteNameRequest" {
            a
        } else {
            b
        };
        write_message(&mut stream, &reply(&request, "NameOperationReply", vec![0]))
            .await
            .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let mutation = client.delete_name("fabric:/App_1/Name-2");
    let neighbor = client.name_exists("fabric:/neighbor");
    tokio::pin!(mutation, neighbor);
    poll_fn(|cx| {
        assert!(mutation.as_mut().poll(cx).is_pending());
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
    assert!(matches!(mutation.await, Err(Error::Protocol(_))));
    assert!(matches!(neighbor.await, Err(Error::Protocol(_))));
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn reordered_concurrent_writes_and_read_share_one_driver() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut stream = establish(listener).await;
        let mut requests = Vec::new();
        for _ in 0..3 {
            requests.push(read_message(&mut stream).await.unwrap());
        }
        for request in requests.into_iter().rev() {
            let body = if request.properties().unwrap().action == "NameExistsRequest" {
                naming_body(true, 0)
            } else {
                check_request(
                    &request,
                    request.properties().unwrap().action == "DeleteNameRequest",
                );
                vec![]
            };
            write_message(&mut stream, &reply(&request, "NameOperationReply", body))
                .await
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
        .await
        .unwrap();
    let (create, delete, exists) = tokio::join!(
        client.create_name("fabric:/App_1/Name-2"),
        client.delete_name("fabric:/App_1/Name-2"),
        client.name_exists("fabric:/neighbor"),
    );
    create.unwrap();
    delete.unwrap();
    assert!(exists.unwrap());
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
                assert_eq!(survivor.properties().unwrap().action, "NameExistsRequest");
                write_message(
                    &mut stream,
                    &reply(&survivor, "NameOperationReply", naming_body(true, 0)),
                )
                .await
                .unwrap();
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            });
            let client = FabricClient::connect_clear(endpoint, TIMEOUT_DURATION)
                .await
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
                assert!(matches!(pending.await.unwrap(), Err(Error::Timeout)));
            }
            release.send(()).unwrap();
            assert!(client.name_exists("fabric:/neighbor").await.unwrap());
            assert!(!client.is_closed());
            client.close().await.unwrap();
            server.await.unwrap();
        }
    }
}
