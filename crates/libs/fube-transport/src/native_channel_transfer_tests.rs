//! Focused coverage transferred before retiring the legacy object driver.
use super::*;

#[tokio::test(start_paused = true)]
async fn idle_controls_and_eof_have_no_inactivity_timeout() {
    let (channel, mut peer) = pair(8192, false, limits());
    tokio::time::advance(MAX_FRAME_TIME * 3).await;
    assert!(channel.terminal_error().is_none());
    write_message(
        &mut peer,
        &Message::listen_instance(
            MessageId {
                guid: [5; 16],
                index: 1,
            },
            &ListenInstance {
                instance: 123,
                ..Default::default()
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    until(|| channel.remote_instance().instance == 123).await;
    drop(peer);
    channel.closed(deadline()).await.unwrap();
    assert!(matches!(channel.terminal_error(), Some(Error::Io(_))));
}

#[tokio::test]
async fn blocked_writer_does_not_stop_out_of_order_full_id_replies() {
    let (channel, mut peer) = pair(64, false, limits());
    let target = target(&channel);
    let mut a = msg("A", vec![]);
    let mut b = msg("B", vec![]);
    a.headers.push(
        Header::message_id(MessageId {
            guid: [2; 16],
            index: 5,
        })
        .unwrap(),
    );
    b.headers.push(
        Header::message_id(MessageId {
            guid: [3; 16],
            index: 5,
        })
        .unwrap(),
    );
    let mut a = channel.request(&target, a, deadline());
    pending(a.as_mut()).await;
    let a_id = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let mut b = channel.request(&target, b, deadline());
    pending(b.as_mut()).await;
    let b_id = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let mut blocked = channel.send_one_way(&target, msg("Blocked", vec![7; 2048]), deadline());
    pending(blocked.as_mut()).await;
    let mut prefix = [0; FIXED_HEADER_SIZE];
    peer.read_exact(&mut prefix).await.unwrap();
    write_message(&mut peer, &reply(b_id, vec![2]))
        .await
        .unwrap();
    write_message(&mut peer, &reply(a_id, vec![1]))
        .await
        .unwrap();
    assert_eq!(b.await.unwrap().body, [2]);
    assert_eq!(a.await.unwrap().body, [1]);
    drop(blocked);
    let mut bytes = vec![0; validate_fixed(&prefix).unwrap().0];
    bytes[..FIXED_HEADER_SIZE].copy_from_slice(&prefix);
    peer.read_exact(&mut bytes[FIXED_HEADER_SIZE..])
        .await
        .unwrap();
    assert_eq!(
        Message::from_frame(Frame::decode(&bytes).unwrap())
            .unwrap()
            .body,
        vec![7; 2048]
    );
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn partial_reader_survives_outbound_activity() {
    let (channel, mut peer) = pair(8192, false, limits());
    let target = target(&channel);
    let mut request = channel.request(&target, msg("Rpc", vec![]), deadline());
    pending(request.as_mut()).await;
    let id = read_message(&mut peer)
        .await
        .unwrap()
        .properties()
        .unwrap()
        .id;
    let bytes = reply(id, vec![1, 2, 3])
        .to_frame()
        .unwrap()
        .encode()
        .unwrap();
    let split = bytes.len() / 2;
    peer.write_all(&bytes[..split]).await.unwrap();
    channel
        .send_one_way(&target, msg("Neighbor", vec![]), deadline())
        .await
        .unwrap();
    assert_eq!(
        read_message(&mut peer)
            .await
            .unwrap()
            .properties()
            .unwrap()
            .action,
        "Neighbor"
    );
    peer.write_all(&bytes[split..]).await.unwrap();
    assert_eq!(request.await.unwrap().body, [1, 2, 3]);
    channel.close(deadline()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn started_write_and_partial_read_watchdogs_are_finite() {
    for writing in [false, true] {
        let (channel, mut peer) = pair(64, false, limits());
        let target = target(&channel);
        if writing {
            let mut operation =
                channel.send_one_way(&target, msg("Blocked", vec![1; 2048]), deadline());
            pending(operation.as_mut()).await;
            peer.read_exact(&mut [0]).await.unwrap();
            drop(operation);
        } else {
            peer.write_all(&[1]).await.unwrap();
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
        }
        channel
            .closed(Instant::now() + MAX_FRAME_TIME * 2)
            .await
            .unwrap();
        assert!(matches!(channel.terminal_error(), Some(Error::Timeout)));
    }
}

#[tokio::test]
async fn unmatched_budget_and_id_exhaustion_remain_terminal() {
    let (channel, mut peer) = pair(8192, false, limits());
    for _ in 0..1025 {
        write_message(
            &mut peer,
            &reply(
                MessageId {
                    guid: [6; 16],
                    index: 1,
                },
                vec![],
            ),
        )
        .await
        .unwrap();
    }
    channel.closed(deadline()).await.unwrap();
    assert!(matches!(
        channel.terminal_error(),
        Some(Error::Protocol("too many unmatched replies"))
    ));
    let (channel, _peer) = pair(8192, false, limits());
    let target = target(&channel);
    channel.owner.shared.inner.lock().unwrap().next_index = u32::MAX;
    assert!(matches!(
        channel
            .request(&target, msg("Exhausted", vec![]), deadline())
            .await,
        Err(Error::ResourceLimit("channel generated ID exhaustion"))
    ));
    channel.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn silent_establishment_is_deadline_bound() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_message(&mut stream).await.unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    });
    assert!(matches!(
        GatewayChannel::connect_unsecured_loopback(
            address,
            fixture_composition(GatewayProfile::Ordinary),
            limits(),
            Instant::now() + Duration::from_millis(30)
        )
        .await,
        Err(Error::Timeout)
    ));
    peer.await.unwrap();
}
