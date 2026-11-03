//! Public composition regressions using the existing framed peer, not a native oracle.
use super::*;
use fube_transport::channel::MessageChannel;
use fube_transport::message::{ListenInstance, TIMEOUT};
use tokio::time::Instant;

async fn established(endpoint: Endpoint, listener: TcpListener) -> (Channel, TcpStream) {
    let (channel, stream) = tokio::join!(endpoint.connect(), peer::accept(listener));
    (channel.unwrap(), stream)
}

fn options(startup_timeout: Duration, default_timeout: Duration) -> NotificationClientOptions {
    NotificationClientOptions {
        startup_timeout,
        default_timeout,
    }
}

fn ticks(request: &Message) -> i64 {
    request
        .headers
        .iter()
        .find(|h| h.id == TIMEOUT)
        .unwrap()
        .read(|d| d.i64())
        .unwrap()
}

pub(super) async fn open(address: std::net::SocketAddr) -> (ServiceNotificationClient, Channel) {
    let raw = ServiceNotificationClient::endpoint_clear(address)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = ServiceNotificationClient::start(raw.clone(), options(WAIT, WAIT))
        .await
        .unwrap();
    (client, raw)
}

#[tokio::test]
async fn configuration_invalid_options_and_unpolled_start_do_not_begin_protocol_or_abort_clones() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = ServiceNotificationClient::endpoint_clear(listener.local_addr().unwrap())
        .unwrap()
        .connect_timeout(WAIT);
    let _copy = endpoint.clone();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let (raw, mut stream) = established(endpoint, listener).await;
    for (startup, default) in [
        (Duration::ZERO, WAIT),
        (WAIT, Duration::ZERO),
        (Duration::from_nanos(1), WAIT),
        (WAIT, Duration::MAX),
    ] {
        assert!(matches!(
            ServiceNotificationClient::start(raw.clone(), options(startup, default)).await,
            Err(NotificationError::Common(Error::InvalidInput(_)))
        ));
        assert!(raw.target().is_ok());
    }
    drop(ServiceNotificationClient::start(
        raw.clone(),
        options(WAIT, WAIT),
    ));
    assert!(raw.target().is_ok());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), read_message(&mut stream))
            .await
            .is_err()
    );
    raw.close(Instant::now() + WAIT).await.unwrap();
    assert!(read_message(&mut stream).await.is_err());
}

#[tokio::test]
async fn composed_start_timeout_and_visible_cancellation_retire_retained_raw_clone() {
    for cancel in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            ServiceNotificationClient::endpoint_clear(listener.local_addr().unwrap()).unwrap();
        let (raw, mut stream) = established(endpoint, listener).await;
        let (seen, visible) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let request = read_message(&mut stream).await.unwrap();
            assert_eq!(
                request.properties().unwrap().action,
                "NotificationClientConnectionRequest"
            );
            seen.send(()).unwrap();
            assert!(read_message(&mut stream).await.is_err());
        });
        let mut opening = Box::pin(ServiceNotificationClient::start(
            raw.clone(),
            options(Duration::from_millis(100), WAIT),
        ));
        if cancel {
            tokio::select! {
                biased;
                _ = visible => {},
                result = &mut opening => panic!("unexpected startup: {}", result.is_ok()),
            }
            drop(opening);
        } else {
            assert!(matches!(
                opening.await,
                Err(NotificationError::Common(Error::Timeout))
            ));
        }
        raw.closed(Instant::now() + WAIT).await.unwrap();
        assert!(raw.target().is_err());
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn start_preserves_ordinary_and_receiver_enabled_nonnotification_denials() {
    for upload in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = if upload {
            ImageStoreClient::endpoint_clear(address).unwrap()
        } else {
            FabricClient::endpoint_clear(address).unwrap()
        };
        let (raw, mut stream) = established(endpoint, listener).await;
        assert_eq!(raw.receiver().is_ok(), upload);
        let peer = tokio::spawn(async move {
            if upload {
                let request = read_message(&mut stream).await.unwrap();
                assert_eq!(
                    request.properties().unwrap().action,
                    "NotificationClientConnectionRequest"
                );
                write_message(&mut stream, &page(100, 1, 0)).await.unwrap();
            }
            // Ordinary startup cannot send Connect; upload cannot admit notification ingress.
            assert!(read_message(&mut stream).await.is_err());
        });
        let result = ServiceNotificationClient::start(raw.clone(), options(WAIT, WAIT)).await;
        if upload {
            assert!(matches!(
                result,
                Err(NotificationError::Common(Error::Protocol(
                    "native duplex ingress is not enabled"
                )))
            ));
        } else {
            assert!(matches!(
                result,
                Err(NotificationError::Common(Error::InvalidInput(_)))
            ));
        }
        raw.closed(Instant::now() + WAIT).await.unwrap();
        assert!(raw.target().is_err());
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn preexisting_terminal_start_preserves_recorded_common_cause() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint =
        ServiceNotificationClient::endpoint_clear(listener.local_addr().unwrap()).unwrap();
    let (raw, mut stream) = established(endpoint, listener).await;
    // Notification-shaped ingress with the wrong actor is rejected by installed policy.
    let mut invalid = page(100, 1, 0);
    invalid.headers[0] = Header::actor(6).unwrap();
    write_message(&mut stream, &invalid).await.unwrap();
    raw.closed(Instant::now() + WAIT).await.unwrap();
    assert!(matches!(
        ServiceNotificationClient::start(raw.clone(), options(WAIT, WAIT)).await,
        Err(NotificationError::Common(Error::Protocol(
            "unauthorized native notification envelope"
        )))
    ));
    assert!(read_message(&mut stream).await.is_err());
}

#[tokio::test]
async fn independent_start_and_operation_budgets_reach_native_headers() {
    for composed in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut stream = peer::accept(listener).await;
            let request = read_message(&mut stream).await.unwrap();
            assert!(ticks(&request) > 10_000_000);
            assert!(ticks(&request) <= 30_000_000);
            write_message(
                &mut stream,
                &peer::reply(
                    &request,
                    "NotificationClientConnectionReply",
                    connected_body(),
                ),
            )
            .await
            .unwrap();
            for action in [
                "RegisterServiceNotificationFilterReply",
                "UnregisterServiceNotificationFilterReply",
            ] {
                let request = read_message(&mut stream).await.unwrap();
                let budget = ticks(&request);
                if composed {
                    assert!(budget > 0 && budget <= 1_000_000);
                } else {
                    assert!(budget > 10_000_000 && budget <= 30_000_000);
                }
                write_message(&mut stream, &peer::reply(&request, action, vec![]))
                    .await
                    .unwrap();
            }
            assert!(read_message(&mut stream).await.is_err());
        });
        let channel = ServiceNotificationClient::endpoint_clear(address)
            .unwrap()
            .connect_timeout(WAIT)
            .connect()
            .await
            .unwrap();
        let operation_timeout = if composed {
            Duration::from_millis(100)
        } else {
            WAIT
        };
        let client = ServiceNotificationClient::start(channel, options(WAIT, operation_timeout))
            .await
            .unwrap();
        let guard = client.register_filter(&filter(), None).await.unwrap();
        guard.unregister(None).await.unwrap();
        client.close(None).await.unwrap();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn caller_owned_dial_and_native_start_share_one_absolute_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_message(&mut stream).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: [9; 16],
                    index: 1,
                },
                &ListenInstance::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let request = read_message(&mut stream).await.unwrap();
        assert_eq!(
            request.properties().unwrap().action,
            "NotificationClientConnectionRequest"
        );
        assert!(ticks(&request) <= 3_000_000);
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _ = write_message(
            &mut stream,
            &peer::reply(
                &request,
                "NotificationClientConnectionReply",
                connected_body(),
            ),
        )
        .await;
        assert!(read_message(&mut stream).await.is_err());
    });
    let deadline = Instant::now() + Duration::from_millis(700);
    let opening = tokio::time::timeout_at(deadline, async {
        let channel = ServiceNotificationClient::endpoint_clear(address)?
            .connect_until(deadline)
            .await?;
        ServiceNotificationClient::start(
            channel,
            options(deadline.saturating_duration_since(Instant::now()), WAIT),
        )
        .await
    });
    assert!(matches!(
        opening.await,
        Err(_) | Ok(Err(NotificationError::Common(Error::Timeout)))
    ));
    task.await.unwrap();
}
