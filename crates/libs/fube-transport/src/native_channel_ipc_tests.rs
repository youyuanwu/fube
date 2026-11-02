use super::*;
use crate::native_demux::RequestPolicy;
use std::sync::atomic::{AtomicBool, Ordering};

fn allow(_: &Message, _: &Properties) -> Result<()> {
    Ok(())
}
fn outbound(_: &Message, _: &Properties, _: OutboundKind) -> Result<()> {
    Ok(())
}
fn identity() -> Header {
    Header::object(message::IPC, |e| {
        e.string("host")?;
        e.u32(42);
        Ok(())
    })
    .unwrap()
}
fn composition() -> ChannelComposition {
    ChannelComposition {
        outbound,
        ordinary_reply: allow,
        actors: ActorDemux::new(vec![ActorRegistration {
            actor: 8,
            authorize: allow,
        }])
        .unwrap(),
        duplex: None,
        requests: Some(RequestPolicy {
            incoming: allow,
            reply: allow,
        }),
    }
}
fn ipc_pair(capacity: usize) -> (GatewayChannel, DuplexStream) {
    let (stream, peer) = duplex(capacity);
    (
        GatewayChannel::start_with_identity(
            stream,
            "127.0.0.1:19000".parse().unwrap(),
            composition(),
            limits(),
            [100; 16],
            ListenInstance::default(),
            Some(identity()),
        ),
        peer,
    )
}
fn command(index: u32, expects: bool) -> Message {
    let mut message = independent_message(8, "Control");
    message.headers.extend([
        Header::message_id(MessageId {
            guid: [3; 16],
            index,
        })
        .unwrap(),
        Header::boolean(message::EXPECTS_REPLY, expects).unwrap(),
    ]);
    message
}

#[tokio::test]
async fn ipc_identity_is_automatic_but_original_context_reply_has_no_identity() {
    let (channel, mut peer) = ipc_pair(8192);
    let target = target(&channel);
    let mut request = channel.request(&target, msg("Register", vec![]), deadline());
    pending(request.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    assert_eq!(
        sent.headers.iter().find(|h| h.id == message::IPC),
        Some(&identity())
    );
    write_message(&mut peer, &reply(sent.properties().unwrap().id, vec![1]))
        .await
        .unwrap();
    request.await.unwrap();
    let one_way = command(1, false);
    write_message(&mut peer, &one_way).await.unwrap();
    let receiver = channel.receiver().unwrap();
    assert!(receiver.receive(deadline()).await.unwrap().reply.is_none());
    let original = command(2, true);
    write_message(&mut peer, &original).await.unwrap();
    let mut incoming = receiver.receive(deadline()).await.unwrap();
    let (foreign, _) = ipc_pair(8192);
    incoming.source = super::target(&foreign);
    incoming.message = command(999, true);
    incoming
        .reply
        .unwrap()
        .reply(msg("ControlReply", vec![]), deadline())
        .await
        .unwrap();
    let sent = read_message(&mut peer).await.unwrap();
    assert_eq!(
        sent.properties().unwrap().relates_to,
        original.properties().unwrap().id
    );
    assert!(!has(&sent, message::IPC));
    assert!(!has(&sent, message::MESSAGE_ID));
    channel
        .send_one_way(&target, msg("SemanticReply", vec![]), deadline())
        .await
        .unwrap();
    let sent = read_message(&mut peer).await.unwrap();
    let props = sent.properties().unwrap();
    assert!(has(&sent, message::IPC));
    assert_ne!(props.id, MessageId::default());
    assert_eq!(props.relates_to, MessageId::default());
    assert!(!props.expects_reply);
    channel.close(deadline()).await.unwrap();
    foreign.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn normal_request_policy_never_grants_special_or_duplicate_authority() {
    for mode in 0..5 {
        let (channel, mut peer) = ipc_pair(8192);
        let receiver = channel.receiver().unwrap();
        let mut message = command(1, true);
        match mode {
            0 => message
                .headers
                .push(Header::boolean(message::UNCORRELATED_REPLY, true).unwrap()),
            1 => message.headers.retain(|h| h.id != message::MESSAGE_ID),
            2 => message.headers.push(
                Header::object(message::FAULT, |e| {
                    e.i64(0);
                    e.boolean(false);
                    Ok(())
                })
                .unwrap(),
            ),
            3 => message.headers.push(identity()),
            _ => {
                write_message(&mut peer, &message).await.unwrap();
                let context = receiver.receive(deadline()).await.unwrap().reply.unwrap();
                drop(context);
            }
        }
        write_message(&mut peer, &message).await.unwrap();
        until(|| channel.terminal_error().is_some()).await;
        assert!(matches!(channel.terminal_error(), Some(Error::Protocol(_))));
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test]
async fn authority_fences_are_resampled_after_zero_progress_for_every_send_kind() {
    for kind in 0..3 {
        let (channel, mut peer) = ipc_pair(1024);
        let target = target(&channel);
        let context = if kind == 2 {
            write_message(&mut peer, &command(1, true)).await.unwrap();
            Some(
                channel
                    .receiver()
                    .unwrap()
                    .receive(deadline())
                    .await
                    .unwrap()
                    .reply
                    .unwrap(),
            )
        } else {
            None
        };
        // Fill exactly the remaining duplex capacity with an already-started
        // large frame, then drain it before the fenced operation is selected.
        let mut fill = channel.send_one_way(&target, msg("Fill", vec![1; 700]), deadline());
        pending(fill.as_mut()).await;
        let filled = read_message(&mut peer).await.unwrap();
        fill.await.unwrap();
        // Use the encoded frame size as an exact fresh transport capacity.
        channel.close(deadline()).await.unwrap();
        drop(context);
        let capacity = filled.to_frame().unwrap().encode().unwrap().len();
        let (channel, mut peer) = ipc_pair(capacity);
        let target = super::target(&channel);
        let context = if kind == 2 {
            write_message(&mut peer, &command(1, true)).await.unwrap();
            Some(
                channel
                    .receiver()
                    .unwrap()
                    .receive(deadline())
                    .await
                    .unwrap()
                    .reply
                    .unwrap(),
            )
        } else {
            None
        };
        channel
            .send_one_way(&target, msg("Fill", vec![1; 700]), deadline())
            .await
            .unwrap();
        let valid = Arc::new(AtomicBool::new(true));
        let signal = valid.clone();
        let diagnostic = Arc::downgrade(&channel.owner.shared);
        let predicate: WritePredicate = Arc::new(move || {
            // Would deadlock if predicates ran under the shared-state lock.
            if let Some(shared) = diagnostic.upgrade() {
                let _ = shared.error();
            }
            signal.load(Ordering::Acquire)
        });
        let mut operation: ChannelFuture<'_, ()> = match kind {
            0 => channel.send_one_way_fenced(&target, msg("Raw", vec![]), deadline(), predicate),
            1 => {
                let (request, _) = channel.request_tracked_fenced(
                    &target,
                    msg("Rpc", vec![]),
                    deadline(),
                    Some(predicate),
                );
                Box::pin(async { request.await.map(|_| ()) })
            }
            _ => context
                .unwrap()
                .reply_fenced(msg("Reply", vec![]), deadline(), predicate),
        };
        pending(operation.as_mut()).await;
        until(|| {
            channel
                .owner
                .shared
                .inner
                .lock()
                .unwrap()
                .entries
                .values()
                .any(|entry| entry.bytes.is_none() && entry.result.is_none())
        })
        .await;
        valid.store(false, Ordering::Release);
        read_message(&mut peer).await.unwrap();
        assert!(matches!(operation.await, Err(Error::Closed)));
        channel.close(deadline()).await.unwrap();
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn held_completion_preserves_reader_order_reservation_and_terminal_cause() {
    let (channel, mut peer) = ipc_pair(8192);
    let target = target(&channel);
    let mut first = channel.request_held(&target, msg("Validate", vec![]), deadline(), None);
    let mut second = channel.request(&target, msg("Neighbor", vec![]), deadline());
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;
    let a = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let b = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let mut burst = reply(a, vec![0]).to_frame().unwrap().encode().unwrap();
    burst.extend(reply(b, vec![1]).to_frame().unwrap().encode().unwrap());
    peer.write_all(&burst).await.unwrap();
    let (_, mut guard) = first.await.unwrap();
    pending(second.as_mut()).await;
    assert_eq!(count(&channel, ORDINARY), 2);
    guard.acknowledge();
    assert_eq!(second.await.unwrap().body, [1]);
    assert_eq!(count(&channel, ORDINARY), 1);
    drop(guard);
    assert_eq!(count(&channel, ORDINARY), 0);
    channel.abort_with_error(Error::Protocol("validation"));
    channel.close(deadline()).await.unwrap();
    assert!(matches!(
        channel.terminal_error(),
        Some(Error::Protocol("validation"))
    ));
}

#[tokio::test]
async fn dropping_held_observer_releases_ordered_handoff_and_request_budget() {
    let (channel, mut peer) = ipc_pair(8192);
    let target = target(&channel);
    let mut request = channel.request_held(&target, msg("Abandon", vec![]), deadline(), None);
    pending(request.as_mut()).await;
    let id = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    write_message(&mut peer, &reply(id, vec![])).await.unwrap();
    until(|| {
        channel
            .owner
            .shared
            .inner
            .lock()
            .unwrap()
            .entries
            .values()
            .any(|e| e.result.is_some())
    })
    .await;
    drop(request);
    until(|| count(&channel, ORDINARY) == 0).await;
    channel
        .send_one_way(&target, msg("StillOpen", vec![]), deadline())
        .await
        .unwrap();
    read_message(&mut peer).await.unwrap();
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn authority_loss_after_positive_write_keeps_complete_frame_for_every_kind() {
    for kind in 0..3 {
        let (channel, mut peer) = ipc_pair(128);
        let target = target(&channel);
        let context = if kind == 2 {
            // A small pipe also tests fragmented inbound request assembly.
            let receiver = channel.receiver().unwrap();
            let request = command(1, true);
            let (sent, received) = tokio::join!(
                write_message(&mut peer, &request),
                receiver.receive(deadline())
            );
            sent.unwrap();
            Some(received.unwrap().reply.unwrap())
        } else {
            None
        };
        let valid = Arc::new(AtomicBool::new(true));
        let signal = valid.clone();
        let predicate: WritePredicate = Arc::new(move || signal.load(Ordering::Acquire));
        let mut operation: ChannelFuture<'_, ()> = match kind {
            0 => channel.send_one_way_fenced(
                &target,
                msg("Raw", vec![7; 2048]),
                deadline(),
                predicate,
            ),
            1 => {
                let (request, _) = channel.request_tracked_fenced(
                    &target,
                    msg("Rpc", vec![7; 2048]),
                    deadline(),
                    Some(predicate),
                );
                Box::pin(async { request.await.map(|_| ()) })
            }
            _ => context
                .unwrap()
                .reply_fenced(msg("Reply", vec![7; 2048]), deadline(), predicate),
        };
        pending(operation.as_mut()).await;
        let mut prefix = [0; FIXED_HEADER_SIZE];
        peer.read_exact(&mut prefix).await.unwrap();
        valid.store(false, Ordering::Release);
        drop(operation);
        let mut bytes = vec![0; validate_fixed(&prefix).unwrap().0];
        bytes[..FIXED_HEADER_SIZE].copy_from_slice(&prefix);
        peer.read_exact(&mut bytes[FIXED_HEADER_SIZE..])
            .await
            .unwrap();
        let frame = Message::from_frame(Frame::decode(&bytes).unwrap()).unwrap();
        assert_eq!(frame.body, vec![7; 2048]);
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test]
async fn ipc_connect_uses_listen_exchange_then_actual_host_pid_without_extra_hello() {
    use tokio::net::TcpListener;
    for host in ["", &"x".repeat(4097)] {
        assert!(matches!(
            GatewayChannel::connect_ipc_clear(
                "127.0.0.1:19000".parse().unwrap(),
                composition(),
                limits(),
                deadline(),
                host
            )
            .await,
            Err(Error::InvalidInput(_))
        ));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(
            read_message(&mut stream)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .actor,
            1
        );
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: [7; 16],
                    index: 1,
                },
                &ListenInstance::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.properties().unwrap().action, "Register");
        let expected = Header::object(message::IPC, |e| {
            e.string("host-guid")?;
            e.u32(std::process::id());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            request.headers.iter().find(|h| h.id == message::IPC),
            Some(&expected)
        );
        write_message(
            &mut stream,
            &reply(request.properties().unwrap().id, vec![]),
        )
        .await
        .unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    let channel = GatewayChannel::connect_ipc_clear(
        endpoint,
        composition(),
        limits(),
        deadline(),
        "host-guid",
    )
    .await
    .unwrap();
    let target = channel.resolve_target(&endpoint.to_string()).unwrap();
    channel
        .request(&target, msg("Register", vec![]), deadline())
        .await
        .unwrap();
    channel.close(deadline()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn ipc_raw_empty_supplied_id_rejects_before_visibility_without_poisoning_channel() {
    let (channel, mut peer) = ipc_pair(8192);
    let target = target(&channel);
    let mut invalid = msg("Empty", vec![]);
    invalid
        .headers
        .push(Header::message_id(MessageId::default()).unwrap());
    assert!(matches!(
        channel.send_one_way(&target, invalid, deadline()).await,
        Err(Error::InvalidInput(_))
    ));
    channel
        .send_one_way(&target, msg("Good", vec![]), deadline())
        .await
        .unwrap();
    assert_eq!(
        read_message(&mut peer)
            .await
            .unwrap()
            .properties()
            .unwrap()
            .action,
        "Good"
    );
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn repeated_valid_zero_progress_polls_do_not_commit_any_send_kind() {
    use std::sync::atomic::AtomicUsize;
    for kind in 0..3 {
        // Fill with one exactly sized encoded IPC frame.
        let mut fill = msg("Fill", vec![1; 700]);
        fill.headers.extend([
            identity(),
            Header::message_id(MessageId {
                guid: [100; 16],
                index: 2,
            })
            .unwrap(),
        ]);
        let capacity = fill.to_frame().unwrap().encode().unwrap().len();
        let (channel, mut peer) = ipc_pair(capacity);
        let target = target(&channel);
        let context = if kind == 2 {
            write_message(&mut peer, &command(1, true)).await.unwrap();
            Some(
                channel
                    .receiver()
                    .unwrap()
                    .receive(deadline())
                    .await
                    .unwrap()
                    .reply
                    .unwrap(),
            )
        } else {
            None
        };
        channel
            .send_one_way(&target, msg("Fill", vec![1; 700]), deadline())
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let predicate: WritePredicate = Arc::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            true
        });
        let mut cancellation = None;
        let mut operation: ChannelFuture<'_, ()> = match kind {
            0 => channel.send_one_way_fenced(&target, msg("Raw", vec![]), deadline(), predicate),
            1 => {
                let (request, cancel) = channel.request_tracked_fenced(
                    &target,
                    msg("Rpc", vec![]),
                    deadline(),
                    Some(predicate),
                );
                cancellation = Some(cancel);
                Box::pin(async { request.await.map(|_| ()) })
            }
            _ => context
                .unwrap()
                .reply_fenced(msg("Reply", vec![]), deadline(), predicate),
        };
        pending(operation.as_mut()).await;
        for expected in 1..=4 {
            channel.owner.shared.changed.notify_waiters();
            until(|| calls.load(Ordering::SeqCst) >= expected).await;
        }
        if let Some(cancel) = cancellation {
            assert!(cancel.cancel_unsent());
        }
        drop(operation);
        read_message(&mut peer).await.unwrap();
        channel.close(deadline()).await.unwrap();
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    }
}
