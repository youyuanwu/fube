use super::*;
use crate::message::{HIGH_PRIORITY, UNCORRELATED_REPLY};
use tokio::io::{duplex, AsyncWriteExt};

fn notification(index: u32) -> Message {
    Message {
        headers: vec![
            Header::actor(5).unwrap(),
            Header::action("ServiceNotificationRequest").unwrap(),
            Header::message_id(MessageId {
                guid: [3; 16],
                index,
            })
            .unwrap(),
            Header::boolean(UNCORRELATED_REPLY, true).unwrap(),
            Header::boolean(EXPECTS_REPLY, true).unwrap(),
        ],
        body: vec![],
    }
}

#[tokio::test]
async fn fragmented_reverse_and_correlated_reply_are_independent() {
    let (stream, mut peer) = duplex(4096);
    let (tx, mut rx) = mpsc::channel(32);
    let connection = Connection::start_role(
        stream,
        ListenInstance::default(),
        [9; 16],
        Some(Arc::new(move |request| {
            tx.try_send(request)
                .map_err(|_| Error::ResourceLimit("test ingress"))
        })),
    );
    let pending = connection.clone();
    let task = tokio::spawn(async move {
        pending
            .request(
                "PingRequest",
                5,
                vec![],
                vec![],
                deadline_after(Duration::from_secs(3)).unwrap(),
            )
            .await
    });
    let request = read_message(&mut peer).await.unwrap();
    let pushed = notification(17);
    let bytes = pushed.to_frame().unwrap().encode().unwrap();
    for byte in bytes {
        peer.write_all(&[byte]).await.unwrap();
    }
    let reverse = rx.recv().await.unwrap();
    assert_eq!(
        reverse.message.properties().unwrap().id,
        pushed.properties().unwrap().id
    );
    let ack = tokio::spawn(
        reverse
            .context
            .reply(vec![42], deadline_after(Duration::from_secs(3)).unwrap()),
    );
    let reply = read_message(&mut peer).await.unwrap();
    assert_eq!(
        reply.properties().unwrap().relates_to,
        pushed.properties().unwrap().id
    );
    assert_eq!(
        reply.properties().unwrap().action,
        "ServiceNotificationReply"
    );
    assert!(reply.properties().unwrap().high_priority);
    assert!(reply.headers.iter().any(|h| h.id == HIGH_PRIORITY));
    assert_eq!(reply.body, vec![42]);
    ack.await.unwrap().unwrap();
    write_message(
        &mut peer,
        &Message {
            headers: vec![
                Header::actor(5).unwrap(),
                Header::action("PingRequest").unwrap(),
                Header::relates_to(request.properties().unwrap().id).unwrap(),
            ],
            body: vec![],
        },
    )
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    // No local request needs to be outstanding for another ACK.
    write_message(&mut peer, &notification(18)).await.unwrap();
    let old = rx.recv().await.unwrap().context;
    connection.close().await.unwrap();
    assert!(old
        .reply(vec![], deadline_after(Duration::from_secs(1)).unwrap())
        .await
        .is_err());
}

#[tokio::test]
async fn full_reverse_handoff_and_unsupported_actions_retire_only_duplex_socket() {
    for full in [false, true] {
        let (stream, mut peer) = duplex(4096);
        let (tx, _rx) = mpsc::channel(1);
        let connection = Connection::start_role(
            stream,
            ListenInstance::default(),
            [8; 16],
            Some(Arc::new(move |r| {
                tx.try_send(r)
                    .map_err(|_| Error::ResourceLimit("reverse ingress"))
            })),
        );
        let mut first = notification(1);
        if !full {
            first.headers.push(Header::action("Unsupported").unwrap());
        }
        write_message(&mut peer, &first).await.unwrap();
        if full {
            write_message(&mut peer, &notification(2)).await.unwrap();
        }
        let error = connection.closed().await;
        assert!(if full {
            matches!(error, Error::ResourceLimit(_))
        } else {
            matches!(error, Error::Protocol(_))
        });
    }
}

#[tokio::test]
async fn reply_control_capacity_is_bounded_without_request_admission() {
    let (stream, _peer) = duplex(64);
    let connection = Connection::start_role(stream, ListenInstance::default(), [8; 16], None);
    let mut tasks = Vec::new();
    for index in 0..34 {
        let context = ReplyContext {
            shared: Arc::downgrade(&connection.handle.shared),
            id: MessageId {
                guid: [3; 16],
                index,
            },
        };
        tasks.push(tokio::spawn(context.reply(
            vec![0; 4096],
            deadline_after(Duration::from_secs(3)).unwrap(),
        )));
        tokio::task::yield_now().await;
    }
    assert_eq!(connection.handle.shared.controls.capacity(), 0);
    assert_eq!(
        connection.handle.shared.admission.available_permits(),
        MAX_PENDING_REQUESTS
    );
    assert!(matches!(
        tasks.pop().unwrap().await.unwrap(),
        Err(Error::ResourceLimit(_))
    ));
    connection.close().await.unwrap();
    for task in tasks {
        assert!(task.await.unwrap().is_err());
    }
}
