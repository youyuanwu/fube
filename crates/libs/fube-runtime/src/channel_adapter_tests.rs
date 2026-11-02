use super::*;
use fube_transport::{
    connection::{deadline_after, read_message, write_message},
    message::{self, ListenInstance, MessageId},
};
use std::{future::Future, task::Poll};
use tokio::net::{TcpListener, TcpStream};

fn deadline() -> Instant {
    deadline_after(Duration::from_secs(2)).unwrap()
}
async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
async fn pair() -> (IpcChannel, TcpStream, mpsc::Receiver<IpcMessage>) {
    let (send, receive) = mpsc::channel(32);
    let handoff = Arc::new(move |message| {
        send.try_send(message)
            .map_err(|_| Error::ResourceLimit("fixture handoff"))
    });
    let (channel, stream) = pair_with(handoff).await;
    (channel, stream, receive)
}
async fn pair_with(handoff: Handoff) -> (IpcChannel, TcpStream) {
    pair_bound(handoff, None).await
}
async fn pair_bound(handoff: Handoff, serving: Option<WritePredicate>) -> (IpcChannel, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (channel, stream) = tokio::join!(
        IpcChannel::connect(address, deadline(), "host", handoff, serving),
        async {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_message(&mut stream).await.unwrap();
            write_message(
                &mut stream,
                &Message::listen_instance(
                    MessageId {
                        guid: [8; 16],
                        index: 1,
                    },
                    &ListenInstance::default(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            stream
        }
    );
    (channel.unwrap(), stream)
}
fn reply(id: MessageId, body: Vec<u8>) -> Message {
    Message {
        headers: vec![Header::relates_to(id).unwrap()],
        body,
    }
}
fn decode(message: Message) -> Result<u8> {
    if message.body.len() != 1 {
        return Err(Error::Protocol("malformed health fixture"));
    }
    Ok(message.body[0])
}

#[tokio::test]
async fn retired_serving_authority_fences_registration_and_lease_but_not_cleanup() {
    let (channel, mut peer) = pair_bound(Arc::new(|_| Ok(())), Some(Arc::new(|| false))).await;
    for (actor, action) in [
        (15, ""),
        (9, "RegisterServiceTypeRequest"),
        (7, "StartRegisterApplicationHostRequest"),
    ] {
        assert!(matches!(
            channel
                .request(action, actor, vec![], vec![], deadline())
                .await,
            Err(Error::Closed)
        ));
    }
    let mut cleanup = Box::pin(channel.request(
        "UnregisterFabricRuntimeRequest",
        9,
        vec![],
        vec![],
        deadline(),
    ));
    pending(cleanup.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    assert_eq!(
        sent.properties().unwrap().action,
        "UnregisterFabricRuntimeRequest"
    );
    write_message(&mut peer, &reply(sent.properties().unwrap().id, vec![]))
        .await
        .unwrap();
    cleanup.await.unwrap();
    assert!(!channel.is_closed());
    channel.close().await.unwrap();
}

#[tokio::test]
async fn malformed_health_retires_before_later_completion_without_observer_repoll() {
    let (channel, mut peer, _) = pair().await;
    let mut health = Box::pin(channel.request_validated(
        "ReportHealth",
        7,
        vec![],
        vec![],
        deadline(),
        decode,
        Arc::new(|| true),
    ));
    let mut lease = Box::pin(channel.request("", 15, vec![], vec![], deadline()));
    pending(health.as_mut()).await;
    pending(lease.as_mut()).await;
    let a = read_message(&mut peer).await.unwrap();
    let b = read_message(&mut peer).await.unwrap();
    let (health_id, lease_id) = if a.properties().unwrap().actor == 7 {
        (a.properties().unwrap().id, b.properties().unwrap().id)
    } else {
        (b.properties().unwrap().id, a.properties().unwrap().id)
    };
    let mut burst = reply(health_id, vec![])
        .to_frame()
        .unwrap()
        .encode()
        .unwrap();
    burst.extend(
        reply(lease_id, vec![1])
            .to_frame()
            .unwrap()
            .encode()
            .unwrap(),
    );
    use tokio::io::AsyncWriteExt;
    peer.write_all(&burst).await.unwrap();
    let cause = tokio::time::timeout_at(deadline(), channel.closed())
        .await
        .unwrap();
    assert!(matches!(cause, Error::Protocol("malformed health fixture")));
    assert!(matches!(
        health.await,
        Err(Error::Protocol("malformed health fixture"))
    ));
    assert!(matches!(
        lease.await,
        Err(Error::Protocol("malformed health fixture"))
    ));
    channel.close().await.unwrap();
}

#[tokio::test]
async fn unobserved_health_results_retain_budget_without_starving_lease() {
    let (channel, mut peer, _) = pair().await;
    let mut results = Vec::new();
    for _ in 0..16 {
        let mut future = Box::pin(channel.request_validated(
            "ReportHealth",
            7,
            vec![],
            vec![],
            deadline(),
            decode,
            Arc::new(|| true),
        ));
        pending(future.as_mut()).await;
        let request = read_message(&mut peer).await.unwrap();
        write_message(
            &mut peer,
            &reply(request.properties().unwrap().id, vec![42]),
        )
        .await
        .unwrap();
        results.push(future);
    }
    assert_eq!(channel.0.slots.available_permits(), 16);
    let mut lease = Box::pin(channel.request("", 15, vec![], vec![], deadline()));
    pending(lease.as_mut()).await;
    let request = read_message(&mut peer).await.unwrap();
    write_message(&mut peer, &reply(request.properties().unwrap().id, vec![1]))
        .await
        .unwrap();
    lease.await.unwrap();
    for result in results {
        assert_eq!(result.await.unwrap(), 42);
    }
    assert_eq!(channel.0.slots.available_permits(), OPERATIONS);
    channel.close().await.unwrap();
    assert!(channel.0.driver.lock().await.is_none());
}

#[tokio::test]
async fn runtime_owner_drop_aborts_even_if_transport_observer_is_retained() {
    let (channel, mut peer, _) = pair().await;
    let mut stopped = channel.0.stopped.clone();
    let transport = channel.0.channel.clone();
    let clone = channel.clone();
    drop(channel);
    assert!(transport.terminal_error().is_none());
    drop(clone);
    tokio::time::timeout_at(deadline(), async {
        while !*stopped.borrow_and_update() {
            stopped.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    use tokio::io::AsyncReadExt;
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(matches!(transport.terminal_error(), Some(Error::Closed)));
}

#[tokio::test]
async fn runtime_policy_is_above_transport_and_semantic_responses_are_not_grants() {
    let (channel, mut peer, mut incoming) = pair().await;
    assert!(matches!(
        channel
            .request("Foreign", 5, vec![], vec![], deadline())
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(
        matches!(
            channel
                .send_one_way(12, "RAReportFault", vec![], deadline(), Arc::new(|| true))
                .await,
            Err(Error::InvalidInput(_))
        ),
        "reports cannot use reserved reply capacity"
    );
    let request = Message {
        headers: vec![
            Header::actor(8).unwrap(),
            Header::action("InvalidateLeaseRequest").unwrap(),
            Header::message_id(MessageId {
                guid: [3; 16],
                index: 1,
            })
            .unwrap(),
            Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
        ],
        body: vec![],
    };
    let before = std::time::Instant::now();
    write_message(&mut peer, &request).await.unwrap();
    let request = incoming.recv().await.unwrap();
    assert!(request.received_at >= before);
    assert!(request.received_at <= std::time::Instant::now());
    // The adapter does not tie this original grant to serving lease authority.
    request
        .reply
        .unwrap()
        .reply(8, None, vec![], deadline())
        .await
        .unwrap();
    let response = read_message(&mut peer).await.unwrap();
    assert_eq!(response.properties().unwrap().relates_to.index, 1);
    assert!(!response.headers.iter().any(|h| h.id == message::IPC));
    channel
        .send_one_way(
            12,
            "ReplicaOpenReply",
            vec![],
            deadline(),
            Arc::new(|| true),
        )
        .await
        .unwrap();
    let response = read_message(&mut peer).await.unwrap();
    assert_eq!(
        response.properties().unwrap().relates_to,
        MessageId::default()
    );
    assert!(response.headers.iter().any(|h| h.id == message::IPC));
    channel.close().await.unwrap();
}

#[tokio::test]
async fn early_context_update_and_serving_invalidation_precede_following_lease_completion() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::AsyncWriteExt;
    for action in ["UpdateCodePackageContextRequest", "InvalidateLeaseRequest"] {
        let ready = Arc::new(AtomicBool::new(action == "InvalidateLeaseRequest"));
        let retired = Arc::new(AtomicBool::new(false));
        let ingress_ready = ready.clone();
        let ingress_retired = retired.clone();
        // Same bounded readiness/retirement decision as Runtime::attach_inner.
        let handoff = Arc::new(move |request: IpcMessage| {
            let p = request.message.properties()?;
            if p.action != "UpdateCodePackageContextRequest"
                || !ingress_ready.load(Ordering::Acquire)
            {
                ingress_retired.store(true, Ordering::Release);
            }
            Ok(())
        });
        let (channel, mut peer) = pair_with(handoff).await;
        let mut lease = Box::pin(channel.request("", 15, vec![], vec![], deadline()));
        pending(lease.as_mut()).await;
        let id = read_message(&mut peer)
            .await
            .unwrap()
            .properties()
            .unwrap()
            .id;
        let control = Message {
            headers: vec![
                Header::actor(8).unwrap(),
                Header::action(action).unwrap(),
                Header::message_id(MessageId {
                    guid: [3; 16],
                    index: 1,
                })
                .unwrap(),
                Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
            ],
            body: vec![],
        };
        let mut burst = control.to_frame().unwrap().encode().unwrap();
        burst.extend(reply(id, vec![1]).to_frame().unwrap().encode().unwrap());
        peer.write_all(&burst).await.unwrap();
        lease.await.unwrap();
        ready.store(true, Ordering::Release);
        assert!(
            retired.load(Ordering::Acquire),
            "earlier {action} must precede completion"
        );
        channel.close().await.unwrap();
    }
}
