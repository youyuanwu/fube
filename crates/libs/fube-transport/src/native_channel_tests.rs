use super::*;
use crate::connection::{read_message, write_message};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream};

#[tokio::test]
async fn unchanged_notification_profile_accepts_its_empty_relation_without_forwarding_authority() {
    for count in [1, 2] {
        let (channel, mut peer) = pair(8192, true, limits());
        let receiver = channel.receiver().unwrap();
        let mut message = notification(42);
        for _ in 0..count {
            message
                .headers
                .push(Header::relates_to(MessageId::default()).unwrap());
        }
        write_message(&mut peer, &message).await.unwrap();
        let incoming = receiver.receive(deadline()).await.unwrap();
        assert_eq!(incoming.message, message);
        channel.close(deadline()).await.unwrap();
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn limits() -> ChannelLimits {
    ChannelLimits {
        max_frame_bytes: 8192,
        max_retained_bytes: 16 * 1024 * 1024,
        ..ChannelLimits::default()
    }
}

fn pair(
    capacity: usize,
    notifications: bool,
    limits: ChannelLimits,
) -> (GatewayChannel, DuplexStream) {
    static INDEX: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(1);
    let (stream, peer) = duplex(capacity);
    let channel = GatewayChannel::start(
        stream,
        "127.0.0.1:19000".parse().unwrap(),
        notifications,
        limits,
        {
            // Distinct from the constant-byte synthetic notification/peer IDs,
            // even when an expanded test inventory reaches their byte values.
            let mut id = std::array::from_fn(|i| (i + 1) as u8);
            id[0] = INDEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            id
        },
        ListenInstance::default(),
    );
    (channel, peer)
}

fn target(channel: &GatewayChannel) -> Arc<GatewayTarget> {
    channel.resolve_target("127.0.0.1:19000").unwrap()
}

fn msg(action: &str, body: Vec<u8>) -> Message {
    Message {
        headers: vec![Header::actor(5).unwrap(), Header::action(action).unwrap()],
        body,
    }
}

fn reply(id: MessageId, body: Vec<u8>) -> Message {
    let mut message = msg("Reply", body);
    message.headers.push(Header::relates_to(id).unwrap());
    message
}

fn notification(index: u32) -> Message {
    let mut message = msg("ServiceNotificationRequest", vec![3, 1, 4]);
    message.headers.extend([
        Header::message_id(MessageId {
            guid: [77; 16],
            index,
        })
        .unwrap(),
        Header::boolean(message::UNCORRELATED_REPLY, true).unwrap(),
        Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
    ]);
    message
}

async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

async fn until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded driver progress");
}

fn count(channel: &GatewayChannel, class: usize) -> usize {
    channel.owner.shared.budget.usage.lock().unwrap().counts[class]
}

#[tokio::test]
async fn lazy_expired_raw_rpc_receive_reply_and_close_do_nothing_before_poll() {
    let (channel, mut peer) = pair(4096, true, limits());
    let target = target(&channel);
    let receiver = channel.receiver().unwrap();
    drop(channel.send_one_way(&target, msg("Raw", vec![]), deadline()));
    drop(channel.request(&target, msg("Rpc", vec![]), deadline()));
    drop(receiver.receive(deadline()));
    drop(channel.close(deadline()));
    assert!(channel.terminal_error().is_none());
    assert_eq!(count(&channel, ORDINARY), 0);
    let past = Instant::now();
    assert!(matches!(
        channel
            .send_one_way(&target, msg("Raw", vec![]), past)
            .await,
        Err(Error::Timeout)
    ));
    assert!(matches!(
        channel.request(&target, msg("Rpc", vec![]), past).await,
        Err(Error::Timeout)
    ));
    write_message(&mut peer, &notification(1)).await.unwrap();
    until(|| count(&channel, INGRESS) == 1).await;
    assert!(matches!(receiver.receive(past).await, Err(Error::Timeout)));
    assert_eq!(count(&channel, INGRESS), 1);
    let incoming = receiver.receive(deadline()).await.unwrap();
    drop(
        incoming
            .reply
            .unwrap()
            .reply(msg("ServiceNotificationReply", vec![]), deadline()),
    );
    write_message(&mut peer, &notification(2)).await.unwrap();
    let incoming = receiver.receive(deadline()).await.unwrap();
    assert!(matches!(
        incoming
            .reply
            .unwrap()
            .reply(msg("ServiceNotificationReply", vec![]), past)
            .await,
        Err(Error::Timeout)
    ));
    channel.close(deadline()).await.unwrap();
    assert_eq!(
        peer.read(&mut [0; 1]).await.unwrap(),
        0,
        "no application frame was emitted"
    );
}

#[tokio::test]
async fn raw_preserves_all_domain_headers_and_success_requires_full_local_write() {
    let (channel, mut peer) = pair(16, false, limits());
    let target = target(&channel);
    let mut original = msg("Raw", vec![8; 2048]);
    original.headers.extend([
        Header::timeout(123).unwrap(),
        Header::activity([3; 16]).unwrap(),
        Header::object(0x8999, |e| {
            e.u32(42);
            Ok(())
        })
        .unwrap(),
    ]);
    let mut send = channel.send_one_way(&target, original.clone(), deadline());
    pending(send.as_mut()).await;
    let mut prefix = [0; 12];
    peer.read_exact(&mut prefix).await.unwrap();
    pending(send.as_mut()).await;
    let size = validate_fixed(&prefix).unwrap().0;
    let mut bytes = vec![0; size];
    bytes[..12].copy_from_slice(&prefix);
    peer.read_exact(&mut bytes[12..]).await.unwrap();
    send.await.unwrap();
    assert_eq!(
        Message::from_frame(Frame::decode(&bytes).unwrap()).unwrap(),
        original
    );
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn rpc_preserves_envelope_adds_only_owned_correlation_and_returns_opaque_reply() {
    let (channel, mut peer) = pair(4096, false, limits());
    let target = target(&channel);
    let original = msg("PingRequest", vec![0xff, 0, 0xc3]);
    let mut request = channel.request(&target, original.clone(), deadline());
    pending(request.as_mut()).await;
    assert_eq!(channel.owner.shared.inner.lock().unwrap().pending.len(), 1);
    let sent = read_message(&mut peer).await.unwrap();
    assert_eq!(
        &sent.headers[..original.headers.len()],
        original.headers.as_slice()
    );
    assert_eq!(sent.headers.len(), original.headers.len() + 2);
    assert!(!has(&sent, message::TIMEOUT));
    assert!(sent.properties().unwrap().expects_reply);
    assert_eq!(sent.body, original.body);
    let response = reply(sent.properties().unwrap().id, vec![0xff; 31]);
    write_message(&mut peer, &response).await.unwrap();
    assert_eq!(request.await.unwrap(), response);
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn pre_start_drop_retracts_but_started_raw_and_rpc_frames_finish_after_abandonment() {
    for rpc in [false, true] {
        let (channel, mut peer) = pair(16, false, limits());
        let target = target(&channel);
        let mut operation: ChannelFuture<'_, ()> = if rpc {
            Box::pin(async {
                channel
                    .request(&target, msg("Rpc", vec![4; 2048]), deadline())
                    .await
                    .map(|_| ())
            })
        } else {
            channel.send_one_way(&target, msg("Raw", vec![4; 2048]), deadline())
        };
        pending(operation.as_mut()).await;
        // Reader proves the frame actually started, not just queued admission.
        let mut prefix = [0; 12];
        peer.read_exact(&mut prefix).await.unwrap();
        let mut queued = channel.send_one_way(&target, msg("Never", vec![9]), deadline());
        pending(queued.as_mut()).await;
        drop(queued);
        drop(operation);
        assert_eq!(
            count(&channel, ORDINARY),
            1,
            "started writer retains its reservation"
        );
        let size = validate_fixed(&prefix).unwrap().0;
        let mut bytes = vec![0; size];
        bytes[..12].copy_from_slice(&prefix);
        peer.read_exact(&mut bytes[12..]).await.unwrap();
        assert_eq!(
            Message::from_frame(Frame::decode(&bytes).unwrap())
                .unwrap()
                .body,
            vec![4; 2048]
        );
        until(|| count(&channel, ORDINARY) == 0).await;
        channel.close(deadline()).await.unwrap();
        assert_eq!(
            peer.read(&mut [0; 1]).await.unwrap(),
            0,
            "cancelled queued frame never started"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn one_absolute_deadline_and_timeout_does_not_truncate_started_write() {
    let (channel, mut peer) = pair(16, false, limits());
    let target = target(&channel);
    let end = Instant::now() + Duration::from_secs(1);
    let mut request = channel.request(&target, msg("Blocked", vec![7; 2048]), end);
    pending(request.as_mut()).await;
    let mut prefix = [0; 12];
    peer.read_exact(&mut prefix).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(request.await, Err(Error::Timeout)));
    assert_eq!(count(&channel, ORDINARY), 1);
    let size = validate_fixed(&prefix).unwrap().0;
    let mut suffix = vec![0; size - 12];
    peer.read_exact(&mut suffix).await.unwrap();
    channel.close(deadline()).await.unwrap();
    assert_eq!(count(&channel, ORDINARY), 0);
}

#[tokio::test]
async fn invalid_role_target_and_correlation_fail_before_visibility() {
    let (channel, mut peer) = pair(4096, false, limits());
    let (foreign, _foreign_peer) = pair(4096, false, limits());
    let foreign_target = target(&foreign);
    let target = target(&channel);
    assert!(matches!(
        channel
            .request(&foreign_target, msg("Rpc", vec![]), deadline())
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(channel.resolve_target("localhost:19000").is_err());
    assert!(channel.resolve_target("127.0.0.1:19001").is_err());
    assert!(channel.receiver().is_err());
    for actor in [0, 1, 7, 21, 26, 29, 35, 36, 65536] {
        let message = Message {
            headers: vec![
                Header::actor(actor).unwrap(),
                Header::action("Forbidden").unwrap(),
            ],
            body: vec![],
        };
        assert!(matches!(
            channel
                .send_one_way(&target, message.clone(), deadline())
                .await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            channel.request(&target, message, deadline()).await,
            Err(Error::InvalidInput(_))
        ));
    }
    for header in [
        Header::actor(5).unwrap(),
        Header::boolean(message::EXPECTS_REPLY, false).unwrap(),
        Header::boolean(message::UNCORRELATED_REPLY, true).unwrap(),
        Header::relates_to(MessageId {
            guid: [1; 16],
            index: 1,
        })
        .unwrap(),
        Header::message_id(MessageId::default()).unwrap(),
    ] {
        let mut message = msg("Rpc", vec![]);
        message.headers.push(header);
        assert!(matches!(
            channel.request(&target, message, deadline()).await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert_eq!(count(&channel, ORDINARY), 0);
    channel.close(deadline()).await.unwrap();
    foreign.close(deadline()).await.unwrap();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn faults_preserve_native_code_zero_is_opaque_and_malformed_is_protocol() {
    for code in [None, Some(0), Some(-2147467259)] {
        let (channel, mut peer) = pair(4096, false, limits());
        let target = target(&channel);
        let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
        pending(request.as_mut()).await;
        let sent = read_message(&mut peer).await.unwrap();
        let mut response = reply(sent.properties().unwrap().id, vec![0xff, 2]);
        if let Some(code) = code {
            response.headers.push(
                Header::object(message::FAULT, |e| {
                    e.i64(code);
                    e.boolean(true);
                    Ok(())
                })
                .unwrap(),
            );
        }
        write_message(&mut peer, &response).await.unwrap();
        match (code, request.await) {
            (Some(code), Err(Error::Remote { code: actual, .. })) if code != 0 => {
                assert_eq!(actual, code)
            }
            (None | Some(0), Ok(actual)) => assert_eq!(actual, response),
            (_, result) => panic!("unexpected Fault outcome: {result:?}"),
        }
        channel.close(deadline()).await.unwrap();
    }
    let (channel, mut peer) = pair(4096, false, limits());
    let target = target(&channel);
    let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
    pending(request.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    let mut response = reply(sent.properties().unwrap().id, vec![]);
    response.headers.push(
        Header::object(message::FAULT, |e| {
            e.u32(8);
            Ok(())
        })
        .unwrap(),
    );
    write_message(&mut peer, &response).await.unwrap();
    until(|| channel.terminal_error().is_some()).await;
    assert!(matches!(channel.terminal_error(), Some(Error::Protocol(_))));
    assert!(matches!(request.await, Err(Error::Protocol(_))));
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn supplied_ids_preserved_duplicates_late_cross_socket_replies_never_complete_other_work() {
    let (channel, mut peer) = pair(4096, false, limits());
    let (other, mut other_peer) = pair(4096, false, limits());
    let other_target = target(&other);
    let target = target(&channel);
    let id = MessageId {
        guid: [123; 16],
        index: 71,
    };
    let mut original = msg("First", vec![]);
    original.headers.extend([
        Header::message_id(id).unwrap(),
        Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
    ]);
    let mut first = channel.request(&target, original.clone(), deadline());
    pending(first.as_mut()).await;
    assert_eq!(read_message(&mut peer).await.unwrap(), original);
    assert!(matches!(
        channel.request(&target, original.clone(), deadline()).await,
        Err(Error::InvalidInput(_))
    ));
    drop(first);
    write_message(&mut peer, &reply(id, vec![99]))
        .await
        .unwrap();
    let mut next = channel.request(&target, msg("Next", vec![]), deadline());
    pending(next.as_mut()).await;
    let next_id = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let mut second = other.request(&other_target, msg("Other", vec![]), deadline());
    pending(second.as_mut()).await;
    let second_id = read_message(&mut other_peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    write_message(&mut other_peer, &reply(next_id, vec![88]))
        .await
        .unwrap();
    write_message(&mut peer, &reply(id, vec![77]))
        .await
        .unwrap();
    until(|| {
        channel.owner.shared.inner.lock().unwrap().unmatched == 2
            && other.owner.shared.inner.lock().unwrap().unmatched == 1
    })
    .await;
    pending(next.as_mut()).await;
    pending(second.as_mut()).await;
    assert!(matches!(
        channel.request(&target, original, deadline()).await,
        Err(Error::InvalidInput(_))
    ));
    write_message(&mut peer, &reply(next_id, vec![1]))
        .await
        .unwrap();
    write_message(&mut other_peer, &reply(second_id, vec![2]))
        .await
        .unwrap();
    assert_eq!(next.await.unwrap().body, [1]);
    assert_eq!(second.await.unwrap().body, [2]);
    write_message(&mut peer, &reply(next_id, vec![3]))
        .await
        .unwrap();
    until(|| channel.owner.shared.inner.lock().unwrap().unmatched == 1).await;
    channel.close(deadline()).await.unwrap();
    other.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn completed_undelivered_results_retain_count_and_bytes_and_close_discards_payloads() {
    let mut profile = limits();
    profile.max_operations = 1;
    let (channel, mut peer) = pair(4096, false, profile);
    let target = target(&channel);
    let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
    pending(request.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    write_message(
        &mut peer,
        &reply(sent.properties().unwrap().id, vec![9; 1024]),
    )
    .await
    .unwrap();
    until(|| {
        channel
            .owner
            .shared
            .inner
            .lock()
            .unwrap()
            .entries
            .values()
            .any(|entry| entry.result.is_some())
    })
    .await;
    assert_eq!(count(&channel, ORDINARY), 1);
    assert_eq!(
        channel.owner.shared.budget.usage.lock().unwrap().bytes,
        profile.slot_bytes().unwrap()
    );
    assert!(matches!(
        channel
            .send_one_way(&target, msg("Overload", vec![]), deadline())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    channel.close(deadline()).await.unwrap();
    assert_eq!(count(&channel, ORDINARY), 0);
    assert!(channel
        .owner
        .shared
        .inner
        .lock()
        .unwrap()
        .entries
        .is_empty());
    assert!(matches!(request.await, Err(Error::Closed)));
}

#[tokio::test]
async fn allocation_byte_overload_and_history_exhaustion_are_finite() {
    let mut profile = limits();
    profile.max_retained_bytes = profile.slot_bytes().unwrap();
    profile.max_history = 2;
    let (channel, mut peer) = pair(4096, false, profile);
    let target = target(&channel);
    let mut inflated = msg("Inflated", vec![]);
    inflated.body.reserve(1024 * 1024);
    assert!(matches!(
        channel.send_one_way(&target, inflated, deadline()).await,
        Err(Error::ResourceLimit(_))
    ));
    let mut first = channel.request(&target, msg("Rpc", vec![]), deadline());
    pending(first.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    assert!(matches!(
        channel
            .send_one_way(&target, msg("NoBytes", vec![]), deadline())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    write_message(&mut peer, &reply(sent.properties().unwrap().id, vec![]))
        .await
        .unwrap();
    first.await.unwrap();
    assert!(matches!(
        channel
            .request(&target, msg("History", vec![]), deadline())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    assert!(matches!(
        channel.terminal_error(),
        Some(Error::ResourceLimit(_))
    ));
    channel.close(deadline()).await.unwrap();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn full_ingress_does_not_block_controls_rpc_or_reserved_reply_capacity() {
    let mut profile = limits();
    profile.max_ingress = 1;
    profile.max_operations = 1;
    let (channel, mut peer) = pair(4096, true, profile);
    let target = target(&channel);
    let receiver = channel.receiver().unwrap();
    write_message(&mut peer, &notification(1)).await.unwrap();
    let delivered = receiver.receive(deadline()).await.unwrap();
    write_message(&mut peer, &notification(2)).await.unwrap();
    until(|| count(&channel, INGRESS) == 1).await;
    let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
    pending(request.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    let control = Message::listen_instance(
        MessageId {
            guid: [9; 16],
            index: 99,
        },
        &ListenInstance {
            instance: 42,
            ..Default::default()
        },
    )
    .unwrap();
    write_message(&mut peer, &control).await.unwrap();
    write_message(&mut peer, &reply(sent.properties().unwrap().id, vec![5]))
        .await
        .unwrap();
    until(|| {
        channel.remote_instance().instance == 42
            && channel
                .owner
                .shared
                .inner
                .lock()
                .unwrap()
                .pending
                .is_empty()
    })
    .await;
    assert_eq!(count(&channel, INGRESS), 1);
    let mut sending = delivered
        .reply
        .unwrap()
        .reply(msg("ServiceNotificationReply", vec![0]), deadline());
    pending(sending.as_mut()).await;
    let response = read_message(&mut peer).await.unwrap();
    assert_eq!(
        response.properties().unwrap().relates_to,
        notification(1).properties().unwrap().id
    );
    sending.await.unwrap();
    assert_eq!(request.await.unwrap().body, [5]);
    write_message(&mut peer, &notification(3)).await.unwrap();
    until(|| channel.terminal_error().is_some()).await;
    assert!(matches!(
        channel.terminal_error(),
        Some(Error::ResourceLimit(_))
    ));
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn receive_cancellation_expiry_and_concurrency_preserve_distinct_envelopes() {
    let (channel, mut peer) = pair(4096, true, limits());
    let receiver = channel.receiver().unwrap();
    let mut receiving = receiver.receive(deadline());
    pending(receiving.as_mut()).await;
    drop(receiving);
    write_message(&mut peer, &notification(1)).await.unwrap();
    write_message(&mut peer, &notification(2)).await.unwrap();
    until(|| count(&channel, INGRESS) == 2).await;
    assert!(matches!(
        receiver.receive(Instant::now()).await,
        Err(Error::Timeout)
    ));
    let (first, second) = tokio::join!(receiver.receive(deadline()), receiver.receive(deadline()));
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(
        first.message.properties().unwrap().id,
        second.message.properties().unwrap().id
    );
    assert!(Arc::ptr_eq(&first.source, &second.source));
    assert_eq!(count(&channel, INGRESS), 0);
    drop(first);
    drop(second);
    channel.close(deadline()).await.unwrap();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn consuming_reply_validates_captured_authority_and_preserves_response() {
    let (channel, mut peer) = pair(4096, true, limits());
    let receiver = channel.receiver().unwrap();
    let mut index = 0;
    for extra in [
        Some(Header::relates_to(MessageId::default()).unwrap()),
        Some(Header::boolean(message::EXPECTS_REPLY, true).unwrap()),
        Some(Header::boolean(message::UNCORRELATED_REPLY, true).unwrap()),
        Some(Header::actor(5).unwrap()),
        None,
    ] {
        index += 1;
        write_message(&mut peer, &notification(index))
            .await
            .unwrap();
        let incoming = receiver.receive(deadline()).await.unwrap();
        let mut response = msg(
            if extra.is_none() {
                "WrongAction"
            } else {
                "ServiceNotificationReply"
            },
            vec![],
        );
        if let Some(extra) = extra {
            response.headers.push(extra);
        }
        assert!(matches!(
            incoming.reply.unwrap().reply(response, deadline()).await,
            Err(Error::InvalidInput(_))
        ));
    }
    index += 1;
    let original = notification(index);
    write_message(&mut peer, &original).await.unwrap();
    let incoming = receiver.receive(deadline()).await.unwrap();
    assert_eq!(incoming.message, original);
    let mut response = msg("ServiceNotificationReply", vec![0xfe, 1]);
    response.headers.push(Header::timeout(91).unwrap());
    let mut sending = incoming.reply.unwrap().reply(response.clone(), deadline());
    pending(sending.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    response
        .headers
        .push(Header::relates_to(original.properties().unwrap().id).unwrap());
    assert_eq!(sent, response);
    sending.await.unwrap();
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn duplicate_notification_mints_no_second_capability_and_disabled_mode_rejects() {
    for enabled in [false, true] {
        let (channel, mut peer) = pair(4096, enabled, limits());
        write_message(&mut peer, &notification(1)).await.unwrap();
        if enabled {
            let receiver = channel.receiver().unwrap();
            let retained = receiver.receive(deadline()).await.unwrap();
            write_message(&mut peer, &notification(1)).await.unwrap();
            until(|| channel.terminal_error().is_some()).await;
            assert!(matches!(
                retained
                    .reply
                    .unwrap()
                    .reply(msg("ServiceNotificationReply", vec![]), deadline())
                    .await,
                Err(Error::Closed)
            ));
        } else {
            until(|| channel.terminal_error().is_some()).await;
        }
        assert!(matches!(channel.terminal_error(), Some(Error::Protocol(_))));
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn close_timeout_retains_terminal_state_and_started_frame_then_repeated_close_joins() {
    let (channel, mut peer) = pair(16, true, limits());
    let target = target(&channel);
    let receiver = channel.receiver().unwrap();
    write_message(&mut peer, &notification(1)).await.unwrap();
    let retained = receiver.receive(deadline()).await.unwrap();
    let mut sending = channel.send_one_way(&target, msg("Raw", vec![5; 2048]), deadline());
    pending(sending.as_mut()).await;
    let mut prefix = [0; 12];
    peer.read_exact(&mut prefix).await.unwrap();
    assert!(matches!(
        channel.close(Instant::now()).await,
        Err(Error::Timeout)
    ));
    assert!(matches!(sending.await, Err(Error::Closed)));
    assert!(
        !*channel.owner.shared.stopped.borrow(),
        "started writer still owns work"
    );
    assert!(matches!(
        receiver.receive(deadline()).await,
        Err(Error::Closed)
    ));
    let size = validate_fixed(&prefix).unwrap().0;
    peer.read_exact(&mut vec![0; size - 12]).await.unwrap();
    channel.close(deadline()).await.unwrap();
    channel.close(deadline()).await.unwrap();
    assert!(*channel.owner.shared.stopped.borrow());
    assert!(matches!(
        retained
            .reply
            .unwrap()
            .reply(msg("ServiceNotificationReply", vec![]), deadline())
            .await,
        Err(Error::Closed)
    ));
    assert!(channel.resolve_target("127.0.0.1:19000").is_err());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn inert_target_receiver_envelope_context_do_not_keep_last_owner_workers_alive() {
    let (channel, mut peer) = pair(4096, true, limits());
    let target = target(&channel);
    let receiver = channel.receiver().unwrap();
    write_message(&mut peer, &notification(1)).await.unwrap();
    let retained = receiver.receive(deadline()).await.unwrap();
    let weak = Arc::downgrade(&channel.owner.shared);
    let mut stopped = channel.owner.shared.stopped.subscribe();
    drop(channel);
    stopped.wait_for(|value| *value).await.unwrap();
    until(|| weak.upgrade().is_none()).await;
    assert_eq!(target.address.port(), 19000);
    assert!(matches!(
        retained
            .reply
            .unwrap()
            .reply(msg("ServiceNotificationReply", vec![]), deadline())
            .await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        receiver.receive(deadline()).await,
        Err(Error::Closed)
    ));
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn disconnect_security_controls_and_malformed_duplicate_replies_retire_without_recovery() {
    for mode in 0..3 {
        let (channel, mut peer) = pair(4096, false, limits());
        let target = target(&channel);
        let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
        pending(request.as_mut()).await;
        let sent = read_message(&mut peer).await.unwrap();
        match mode {
            0 => drop(peer),
            1 => {
                let message = Message {
                    headers: vec![
                        Header::actor(29).unwrap(),
                        Header::action("Reconnect").unwrap(),
                    ],
                    body: vec![],
                };
                write_message(&mut peer, &message).await.unwrap();
            }
            _ => {
                let mut response = reply(sent.properties().unwrap().id, vec![]);
                response.headers.push(
                    Header::relates_to(MessageId {
                        guid: [42; 16],
                        index: 42,
                    })
                    .unwrap(),
                );
                write_message(&mut peer, &response).await.unwrap();
            }
        }
        until(|| channel.terminal_error().is_some()).await;
        assert!(matches!(
            request.await,
            Err(Error::Protocol(_) | Error::Io(_))
        ));
        channel.close(deadline()).await.unwrap();
    }
}

#[test]
fn native_duplicate_allowance_is_narrow_and_finite() {
    let id = MessageId {
        guid: [3; 16],
        index: 19,
    };
    for headers in [
        vec![Header::actor(5).unwrap()],
        vec![Header::action("Other").unwrap()],
        vec![
            Header::message_id(id).unwrap(),
            Header::message_id(id).unwrap(),
        ],
        vec![
            Header::relates_to(id).unwrap(),
            Header::relates_to(id).unwrap(),
            Header::relates_to(id).unwrap(),
        ],
        vec![
            Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
            Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
            Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
        ],
        vec![
            Header::relates_to(id).unwrap(),
            Header::relates_to(MessageId::default()).unwrap(),
        ],
    ] {
        let mut message = msg("Reply", vec![]);
        message.headers.extend(headers);
        assert!(matches!(envelope(&message, true), Err(Error::Protocol(_))));
    }
}

#[test]
fn limits_reject_zero_overflow_and_oversized_profiles() {
    for profile in [
        ChannelLimits {
            max_operations: 0,
            ..limits()
        },
        ChannelLimits {
            max_history: 0,
            ..limits()
        },
        ChannelLimits {
            max_operations: usize::MAX,
            ..limits()
        },
        ChannelLimits {
            max_frame_bytes: MAX_FRAME_SIZE + 1,
            ..limits()
        },
    ] {
        assert!(profile.validate().is_err());
    }
    limits().validate().unwrap();
}

#[tokio::test]
async fn reply_queue_count_and_started_abandonment_keep_full_frame_ownership() {
    let mut profile = limits();
    profile.max_replies = 1;
    let (channel, mut peer) = pair(16, true, profile);
    let receiver = channel.receiver().unwrap();
    write_message(&mut peer, &notification(1)).await.unwrap();
    let first = receiver.receive(deadline()).await.unwrap();
    write_message(&mut peer, &notification(2)).await.unwrap();
    let second = receiver.receive(deadline()).await.unwrap();
    let id = first.message.properties().unwrap().id;
    let mut sending = first
        .reply
        .unwrap()
        .reply(msg("ServiceNotificationReply", vec![6; 2048]), deadline());
    pending(sending.as_mut()).await;
    let mut prefix = [0; 12];
    peer.read_exact(&mut prefix).await.unwrap();
    assert!(matches!(
        second
            .reply
            .unwrap()
            .reply(msg("ServiceNotificationReply", vec![]), deadline())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    drop(sending);
    assert_eq!(count(&channel, REPLY), 1);
    let size = validate_fixed(&prefix).unwrap().0;
    let mut bytes = vec![0; size];
    bytes[..12].copy_from_slice(&prefix);
    peer.read_exact(&mut bytes[12..]).await.unwrap();
    let sent = Message::from_frame(Frame::decode(&bytes).unwrap()).unwrap();
    assert_eq!(sent.properties().unwrap().relates_to, id);
    assert_eq!(sent.body, vec![6; 2048]);
    until(|| count(&channel, REPLY) == 0).await;
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn receive_waiters_are_bounded_and_matching_reply_headers_are_preserved() {
    let mut profile = limits();
    profile.max_operations = 1;
    let (channel, mut peer) = pair(4096, true, profile);
    let receiver = channel.receiver().unwrap();
    let mut receiving = receiver.receive(deadline());
    pending(receiving.as_mut()).await;
    assert!(matches!(
        receiver.receive(deadline()).await,
        Err(Error::ResourceLimit(_))
    ));
    write_message(&mut peer, &notification(1)).await.unwrap();
    let incoming = receiving.await.unwrap();
    let mut response = msg("ServiceNotificationReply", vec![]);
    response.headers.extend([
        Header::relates_to(incoming.message.properties().unwrap().id).unwrap(),
        Header::boolean(message::EXPECTS_REPLY, false).unwrap(),
    ]);
    let mut sending = incoming.reply.unwrap().reply(response.clone(), deadline());
    pending(sending.as_mut()).await;
    assert_eq!(read_message(&mut peer).await.unwrap(), response);
    sending.await.unwrap();
    channel.close(deadline()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn complete_write_after_deadline_is_timeout_and_late_reply_never_becomes_success() {
    for rpc in [false, true] {
        let (channel, mut peer) = pair(16, false, limits());
        let target = target(&channel);
        let end = Instant::now() + Duration::from_secs(1);
        let mut operation: ChannelFuture<'_, ()> = if rpc {
            Box::pin(async {
                channel
                    .request(&target, msg("Late", vec![1; 2048]), end)
                    .await
                    .map(|_| ())
            })
        } else {
            channel.send_one_way(&target, msg("Late", vec![1; 2048]), end)
        };
        pending(operation.as_mut()).await;
        let mut prefix = [0; 12];
        peer.read_exact(&mut prefix).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        let size = validate_fixed(&prefix).unwrap().0;
        let mut bytes = vec![0; size];
        bytes[..12].copy_from_slice(&prefix);
        peer.read_exact(&mut bytes[12..]).await.unwrap();
        if rpc {
            let sent = Message::from_frame(Frame::decode(&bytes).unwrap()).unwrap();
            write_message(&mut peer, &reply(sent.properties().unwrap().id, vec![2]))
                .await
                .unwrap();
        }
        assert!(matches!(operation.await, Err(Error::Timeout)));
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test]
async fn connection_establishment_enforces_profile_and_rejects_oversized_frame_before_tail() {
    use tokio::net::TcpListener;
    assert!(matches!(
        GatewayChannel::connect_unsecured_loopback(
            "192.0.2.1:19000".parse().unwrap(),
            false,
            limits(),
            deadline()
        )
        .await,
        Err(Error::InvalidInput(_))
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let local = read_message(&mut stream).await.unwrap();
        assert_eq!(local.properties().unwrap().actor, 1);
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: [4; 16],
                    index: 1,
                },
                &ListenInstance {
                    instance: 33,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        // Send just an oversized fixed header: the backend must not await/allocate its tail.
        let bytes = msg("TooLarge", vec![0; 9000])
            .to_frame()
            .unwrap()
            .encode()
            .unwrap();
        stream.write_all(&bytes[..FIXED_HEADER_SIZE]).await.unwrap();
        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.unwrap(), 0);
    });
    let channel = GatewayChannel::connect_unsecured_loopback(endpoint, false, limits(), deadline())
        .await
        .unwrap();
    assert_eq!(channel.remote_instance().instance, 33);
    until(|| channel.terminal_error().is_some()).await;
    assert!(matches!(
        channel.terminal_error(),
        Some(Error::ResourceLimit(_))
    ));
    channel.close(deadline()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn selected_zero_byte_writes_retract_on_drop_or_expiry_for_raw_rpc_and_reply() {
    for kind in 0..3 {
        for expire in [false, true] {
            let first = msg("Fill", vec![1; 128]);
            let capacity = first.to_frame().unwrap().encode().unwrap().len();
            let (channel, mut peer) = pair(capacity, kind == 2, limits());
            let target = target(&channel);
            let context = if kind == 2 {
                write_message(&mut peer, &notification(1)).await.unwrap();
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
                .send_one_way(&target, first.clone(), deadline())
                .await
                .unwrap();
            // No byte of a second frame fits until the first full frame is drained.
            let end = Instant::now() + Duration::from_secs(1);
            let mut operation: ChannelFuture<'_, ()> = match kind {
                0 => channel.send_one_way(&target, msg("CancelledRaw", vec![2]), end),
                1 => Box::pin(async {
                    channel
                        .request(&target, msg("CancelledRpc", vec![2]), end)
                        .await
                        .map(|_| ())
                }),
                _ => context
                    .unwrap()
                    .reply(msg("ServiceNotificationReply", vec![2]), end),
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
            if expire {
                tokio::time::advance(Duration::from_secs(1)).await;
                assert!(matches!(operation.await, Err(Error::Timeout)));
            } else {
                drop(operation);
            }
            assert_eq!(read_message(&mut peer).await.unwrap(), first);
            channel.close(deadline()).await.unwrap();
            assert_eq!(
                peer.read(&mut [0; 1]).await.unwrap(),
                0,
                "no first byte after retraction"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn on_time_settlement_survives_late_observation_for_rpc_fault_raw_and_reply() {
    for kind in 0..4 {
        let (channel, mut peer) = pair(4096, kind == 3, limits());
        let target = target(&channel);
        let context = if kind == 3 {
            write_message(&mut peer, &notification(1)).await.unwrap();
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
        let end = Instant::now() + Duration::from_secs(1);
        let mut operation: ChannelFuture<'_, ()> = match kind {
            0 => channel.send_one_way(&target, msg("Raw", vec![42]), end),
            1 | 2 => Box::pin(async {
                let response = channel.request(&target, msg("Rpc", vec![]), end).await?;
                assert_eq!(response.body, [42]);
                Ok(())
            }),
            _ => context
                .unwrap()
                .reply(msg("ServiceNotificationReply", vec![42]), end),
        };
        pending(operation.as_mut()).await;
        let sent = read_message(&mut peer).await.unwrap();
        if matches!(kind, 1 | 2) {
            let mut response = reply(sent.properties().unwrap().id, vec![42]);
            if kind == 2 {
                response.headers.push(
                    Header::object(message::FAULT, |e| {
                        e.i64(-7);
                        e.boolean(true);
                        Ok(())
                    })
                    .unwrap(),
                );
            }

            write_message(&mut peer, &response).await.unwrap();
        }
        until(|| {
            channel
                .owner
                .shared
                .inner
                .lock()
                .unwrap()
                .entries
                .values()
                .any(|entry| entry.result.is_some())
        })
        .await;
        assert!(Instant::now() < end);
        let class = if kind == 3 { REPLY } else { ORDINARY };
        assert_eq!(count(&channel, class), 1);
        tokio::time::advance(Duration::from_secs(2)).await;
        match operation.await {
            Err(Error::Remote { code: -7, .. }) if kind == 2 => {}
            Ok(()) if kind != 2 => {}
            result => panic!("settled result changed: {result:?}"),
        }
        assert_eq!(count(&channel, class), 0);
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test]
async fn native_equal_duplicate_correlation_is_preserved_but_caller_and_contradictory_duplicates_reject(
) {
    let (channel, mut peer) = pair(4096, true, limits());
    let target = target(&channel);
    let receiver = channel.receiver().unwrap();
    let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
    pending(request.as_mut()).await;
    let sent = read_message(&mut peer).await.unwrap();
    let mut response = reply(sent.properties().unwrap().id, vec![42]);
    response
        .headers
        .push(Header::relates_to(sent.properties().unwrap().id).unwrap());
    write_message(&mut peer, &response).await.unwrap();
    assert_eq!(
        request.await.unwrap(),
        response,
        "both native wire headers preserved"
    );
    let mut pushed = notification(1);
    pushed
        .headers
        .push(Header::boolean(message::EXPECTS_REPLY, true).unwrap());
    write_message(&mut peer, &pushed).await.unwrap();
    let incoming = receiver.receive(deadline()).await.unwrap();
    assert_eq!(incoming.message, pushed);
    let mut caller = msg("ServiceNotificationReply", vec![]);
    caller.headers.extend([
        Header::relates_to(incoming.message.properties().unwrap().id).unwrap(),
        Header::relates_to(incoming.message.properties().unwrap().id).unwrap(),
    ]);
    assert!(matches!(
        incoming.reply.unwrap().reply(caller, deadline()).await,
        Err(Error::InvalidInput(_))
    ));
    let mut caller = msg("Rpc", vec![]);
    caller.headers.extend([
        Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
        Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
    ]);
    assert!(matches!(
        channel.request(&target, caller, deadline()).await,
        Err(Error::InvalidInput(_))
    ));
    let mut contradictory = pushed;
    contradictory.headers.last_mut().unwrap().value =
        Header::boolean(message::EXPECTS_REPLY, false)
            .unwrap()
            .value;
    write_message(&mut peer, &contradictory).await.unwrap();
    until(|| channel.terminal_error().is_some()).await;
    assert!(matches!(channel.terminal_error(), Some(Error::Protocol(_))));
    channel.close(deadline()).await.unwrap();
}

fn forwarded_reply(
    ids: &[MessageId],
    actor: Option<i64>,
    action: Option<&str>,
    body: Vec<u8>,
) -> Message {
    let mut headers = Vec::new();
    if let Some(actor) = actor {
        headers.push(Header::actor(actor).unwrap());
    }
    if let Some(action) = action {
        headers.push(Header::action(action).unwrap());
    }
    headers.extend(ids.iter().map(|id| Header::relates_to(*id).unwrap()));
    Message { headers, body }
}

#[tokio::test]
async fn forwarded_correlation_completes_only_final_neighbor_and_preserves_wire_bytes() {
    for (actor, action) in [
        (Some(16), Some("OperationSuccessAction")),
        (Some(13), Some("QueryReply")),
        (None, None),
    ] {
        for records in [2, 3] {
            let (channel, mut peer) = pair(4096, false, limits());
            let target = target(&channel);
            let mut first = channel.request(&target, msg("QueryRequest", vec![1]), deadline());
            pending(first.as_mut()).await;
            let a = read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .id;
            let mut second = channel.request(&target, msg("QueryRequest", vec![2]), deadline());
            pending(second.as_mut()).await;
            let b = read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .id;
            let ids = if records == 2 {
                vec![a, b]
            } else {
                vec![a, b, b]
            };
            let mut response = forwarded_reply(&ids, actor, action, vec![0xff, 0, 0xc3, 42]);
            response.headers.push(Header::activity([3; 16]).unwrap());
            response.headers.push(
                Header::object(0x8999, |e| {
                    e.u32(42);
                    Ok(())
                })
                .unwrap(),
            );
            let wire = response.to_frame().unwrap().encode().unwrap();
            peer.write_all(&wire).await.unwrap();
            until(|| {
                let inner = channel.owner.shared.inner.lock().unwrap();
                inner.pending.contains_key(&a)
                    && !inner.pending.contains_key(&b)
                    && inner.entries.values().any(|entry| entry.result.is_some())
            })
            .await;
            pending(first.as_mut()).await;
            let received = second.await.unwrap();
            assert_eq!(received, response);
            assert_eq!(received.to_frame().unwrap().encode().unwrap(), wire);
            assert_eq!(received.properties().unwrap().relates_to, b);
            assert_eq!(channel.owner.shared.inner.lock().unwrap().pending.len(), 1);
            assert!(channel.terminal_error().is_none());
            write_message(&mut peer, &reply(a, vec![1])).await.unwrap();
            assert_eq!(first.await.unwrap().body, [1]);
            channel.close(deadline()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn forwarded_correlation_unknown_final_id_never_falls_back_to_pending_earlier_id() {
    for (actor, action) in [
        (Some(16), Some("OperationSuccessAction")),
        (Some(13), Some("QueryReply")),
        (None, None),
    ] {
        for records in [2, 3] {
            let (channel, mut peer) = pair(4096, false, limits());
            let target = target(&channel);
            let mut request = channel.request(&target, msg("QueryRequest", vec![]), deadline());
            pending(request.as_mut()).await;
            let a = read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .id;
            let unknown = MessageId {
                guid: [73; 16],
                index: 91,
            };
            let ids = if records == 2 {
                vec![a, unknown]
            } else {
                vec![a, unknown, unknown]
            };
            write_message(&mut peer, &forwarded_reply(&ids, actor, action, vec![99]))
                .await
                .unwrap();
            until(|| channel.owner.shared.inner.lock().unwrap().unmatched == 1).await;
            pending(request.as_mut()).await;
            assert!(channel
                .owner
                .shared
                .inner
                .lock()
                .unwrap()
                .pending
                .contains_key(&a));
            assert!(channel.terminal_error().is_none());
            write_message(&mut peer, &reply(a, vec![1])).await.unwrap();
            assert_eq!(request.await.unwrap().body, [1]);
            channel.close(deadline()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn forwarded_correlation_reversing_wire_order_selects_a_not_b() {
    let (channel, mut peer) = pair(4096, false, limits());
    let target = target(&channel);
    let mut first = channel.request(&target, msg("QueryRequest", vec![1]), deadline());
    pending(first.as_mut()).await;
    let a = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let mut second = channel.request(&target, msg("QueryRequest", vec![2]), deadline());
    pending(second.as_mut()).await;
    let b = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let response = forwarded_reply(&[b, a], Some(13), Some("QueryReply"), vec![3]);
    write_message(&mut peer, &response).await.unwrap();
    until(|| {
        let inner = channel.owner.shared.inner.lock().unwrap();
        !inner.pending.contains_key(&a) && inner.pending.contains_key(&b)
    })
    .await;
    assert_eq!(first.await.unwrap(), response);
    pending(second.as_mut()).await;
    assert_eq!(channel.owner.shared.inner.lock().unwrap().pending.len(), 1);
    write_message(&mut peer, &reply(b, vec![2])).await.unwrap();
    assert_eq!(second.await.unwrap().body, [2]);
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn forwarded_correlation_accepts_nonempty_ids_with_one_zero_component() {
    for outer in [
        MessageId {
            guid: [0; 16],
            index: 19,
        },
        MessageId {
            guid: [71; 16],
            index: 0,
        },
    ] {
        let (channel, mut peer) = pair(4096, false, limits());
        let target = target(&channel);
        let mut original = msg("QueryRequest", vec![]);
        original.headers.push(Header::message_id(outer).unwrap());
        let mut request = channel.request(&target, original, deadline());
        pending(request.as_mut()).await;
        assert_eq!(
            read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .id,
            outer
        );
        let inner = MessageId {
            guid: [0; 16],
            index: 20,
        };
        let response = forwarded_reply(&[inner, outer, outer], None, None, vec![42]);
        write_message(&mut peer, &response).await.unwrap();
        assert_eq!(request.await.unwrap(), response);
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test]
async fn forwarded_correlation_rejects_malformed_extra_nonrepeating_or_unproven_envelopes() {
    for case in 0..24 {
        let (channel, mut peer) = pair(4096, false, limits());
        let target = target(&channel);
        let mut request = channel.request(&target, msg("QueryRequest", vec![]), deadline());
        pending(request.as_mut()).await;
        let outer = read_message(&mut peer)
            .await
            .unwrap()
            .properties()
            .unwrap()
            .id;
        let inner = MessageId {
            guid: [74; 16],
            index: 92,
        };
        let mut response = forwarded_reply(&[inner, outer], Some(13), Some("QueryReply"), vec![99]);
        match case {
            0 => {
                response.headers[2].value = vec![1];
            }
            1 => response.headers[2] = Header::relates_to(MessageId::default()).unwrap(),
            2 => response.headers[3] = Header::relates_to(MessageId::default()).unwrap(),
            3 => response.headers.extend([
                Header::relates_to(outer).unwrap(),
                Header::relates_to(outer).unwrap(),
            ]),
            4 => response.headers.push(Header::relates_to(inner).unwrap()),
            5 => response.headers[0] = Header::actor(5).unwrap(),
            6 => response.headers[1] = Header::action("WrongAction").unwrap(),
            7 => {
                response.headers.remove(1);
            }
            8 => {
                response.headers[0] = Header::actor(16).unwrap();
                response.headers.remove(1);
            }
            9 => response.headers[0] = Header::actor(16).unwrap(),
            10 => {
                response.headers.remove(0);
            }
            11 => {
                response.headers[0] = Header::actor(0).unwrap();
                response.headers.remove(1);
            }
            12 => {
                response.headers.remove(0);
                response.headers[0] = Header::action("").unwrap();
            }
            13 | 14 => response.headers.push(
                Header::object(message::FAULT, |e| {
                    e.i64(if case == 13 { 0 } else { -7 });
                    e.boolean(false);
                    Ok(())
                })
                .unwrap(),
            ),
            15 => response
                .headers
                .push(Header::boolean(message::UNCORRELATED_REPLY, true).unwrap()),
            16 => response
                .headers
                .push(Header::boolean(message::EXPECTS_REPLY, true).unwrap()),
            17 | 18 => response
                .headers
                .push(Header::boolean(message::IPC, case == 18).unwrap()),
            19 => response.headers.push(response.headers[0].clone()),
            20 => response.headers.push(response.headers[1].clone()),
            21 => {
                response =
                    forwarded_reply(&[MessageId::default(), outer, outer], None, None, vec![99]);
            }
            22 => {
                response =
                    forwarded_reply(&[inner, outer, MessageId::default()], None, None, vec![99]);
            }
            _ => {
                let mut third = Header::relates_to(outer).unwrap();
                third.value[6] ^= 1;
                assert_eq!(third.read(MessageId::decode).unwrap(), outer);
                assert_ne!(third.value, response.headers[3].value);
                response.headers.push(third);
            }
        };
        write_message(&mut peer, &response).await.unwrap();
        until(|| channel.terminal_error().is_some()).await;
        let result = request.await;
        assert!(
            matches!(result, Err(Error::Protocol(_))),
            "case {case}: {result:?}"
        );
        assert_eq!(channel.owner.shared.inner.lock().unwrap().pending.len(), 0);
        channel.close(deadline()).await.unwrap();
    }
}

#[tokio::test]
async fn forwarded_correlation_cannot_authorize_a_ping_pending_entry_or_complete_earlier_query() {
    for (actor, action) in [
        (Some(16), Some("OperationSuccessAction")),
        (Some(13), Some("QueryReply")),
        (None, None),
    ] {
        for records in [2, 3] {
            let (channel, mut peer) = pair(4096, false, limits());
            let target = target(&channel);
            let mut query = channel.request(&target, msg("QueryRequest", vec![1]), deadline());
            pending(query.as_mut()).await;
            let a = read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .id;
            let mut ping = channel.request(&target, msg("PingRequest", vec![2]), deadline());
            pending(ping.as_mut()).await;
            let b = read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .id;
            let ids = if records == 2 {
                vec![a, b]
            } else {
                vec![a, b, b]
            };
            write_message(&mut peer, &forwarded_reply(&ids, actor, action, vec![99]))
                .await
                .unwrap();
            until(|| channel.terminal_error().is_some()).await;
            assert!(matches!(ping.await, Err(Error::Protocol(_))));
            assert!(matches!(query.await, Err(Error::Protocol(_))));
            channel.close(deadline()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn forwarded_inbound_allowance_never_relaxes_caller_duplicates() {
    let (channel, mut peer) = pair(4096, false, limits());
    let target = target(&channel);
    let a = MessageId {
        guid: [75; 16],
        index: 1,
    };
    let b = MessageId {
        guid: [75; 16],
        index: 2,
    };
    for headers in [
        vec![
            Header::relates_to(a).unwrap(),
            Header::relates_to(a).unwrap(),
        ],
        vec![
            Header::relates_to(a).unwrap(),
            Header::relates_to(b).unwrap(),
        ],
        vec![
            Header::relates_to(a).unwrap(),
            Header::relates_to(b).unwrap(),
            Header::relates_to(b).unwrap(),
        ],
        vec![
            Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
            Header::boolean(message::EXPECTS_REPLY, true).unwrap(),
        ],
        vec![Header::actor(5).unwrap()],
        vec![Header::action("QueryRequest").unwrap()],
        vec![
            Header::message_id(a).unwrap(),
            Header::message_id(a).unwrap(),
        ],
    ] {
        let mut original = msg("QueryRequest", vec![]);
        original.headers.extend(headers);
        assert!(matches!(
            channel.request(&target, original.clone(), deadline()).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            channel.send_one_way(&target, original, deadline()).await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert_eq!(count(&channel, ORDINARY), 0);
    assert!(channel
        .owner
        .shared
        .inner
        .lock()
        .unwrap()
        .pending
        .is_empty());
    channel.close(deadline()).await.unwrap();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}
