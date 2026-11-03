use super::*;
use fube_client::{ChannelLimits, ClientLimits, ClientOptions, Endpoint, CLIENT_SLOT_BYTES};
use fube_transport::{
    channel::MessageChannel,
    message::Properties,
    native_demux::{ActorDemux, ChannelComposition, OutboundKind},
};
use std::{future::Future, pin::Pin, task::Poll};
use tokio::time::{sleep, timeout, Instant};

async fn pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await
}

fn options(calls: usize) -> ClientOptions {
    ClientOptions {
        default_timeout: TIMEOUT_DURATION,
        limits: ClientLimits {
            max_calls: calls,
            max_retained_bytes: calls * CLIENT_SLOT_BYTES,
        },
    }
}

#[tokio::test]
async fn stub_failure_is_pre_application_and_drops_only_its_consumed_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = FabricClient::endpoint_clear(listener.local_addr().unwrap()).unwrap();
    let (channel, mut peer) = tokio::join!(endpoint.connect(), establish(listener));
    let channel = channel.unwrap();
    for invalid in [
        ClientOptions {
            default_timeout: Duration::ZERO,
            ..options(1)
        },
        ClientOptions {
            default_timeout: Duration::MAX,
            ..options(1)
        },
        options(0),
        ClientOptions {
            limits: ClientLimits {
                max_calls: 1,
                max_retained_bytes: CLIENT_SLOT_BYTES - 1,
            },
            ..options(1)
        },
        ClientOptions {
            limits: ClientLimits {
                max_calls: usize::MAX,
                max_retained_bytes: usize::MAX,
            },
            ..options(1)
        },
    ] {
        assert!(matches!(
            FabricClient::new(channel.clone(), invalid),
            Err(Error::InvalidInput(_))
        ));
        assert!(channel.target().is_ok());
    }
    let client = FabricClient::new(channel.clone(), options(1)).unwrap();
    assert!(timeout(Duration::from_millis(20), read_message(&mut peer))
        .await
        .is_err());
    let mut ping = Box::pin(client.ping(None));
    pending(ping.as_mut()).await;
    let request = read_message(&mut peer).await.unwrap();
    assert_eq!(request.properties().unwrap().action, "PingRequest");
    write_message(&mut peer, &ping_reply(&request))
        .await
        .unwrap();
    ping.await.unwrap();
    drop(client);
    // Dropping the stub did not close the retained raw owner.
    let client = FabricClient::new(channel.clone(), options(1)).unwrap();
    client.close().await.unwrap();
    assert!(matches!(
        FabricClient::new(channel, options(1)),
        Err(Error::Closed)
    ));
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = FabricClient::endpoint_clear(listener.local_addr().unwrap()).unwrap();
    let (channel, mut peer) = tokio::join!(endpoint.connect(), establish(listener));
    assert!(FabricClient::new(channel.unwrap(), options(0)).is_err());
    // Normal last-owner drop still shuts down; no exceptional explicit close.
    assert_eq!(
        timeout(TIMEOUT_DURATION, peer.read(&mut [0; 1]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn stub_binding_never_grants_installed_policy_or_sends_an_extra_handshake() {
    fn deny(_: &Message, _: &Properties, _: OutboundKind) -> fube_transport::Result<()> {
        Err(Error::InvalidInput("test denies all submissions"))
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Endpoint::unsecured_loopback(
        listener.local_addr().unwrap(),
        ChannelComposition {
            outbound: deny,
            ordinary_reply: |_, _| Ok(()),
            actors: ActorDemux::new(vec![]).unwrap(),
            duplex: None,
            requests: None,
            ordered_ingress: false,
        },
    )
    .unwrap();
    let (channel, mut peer) = tokio::join!(endpoint.connect(), establish(listener));
    let client = FabricClient::new(channel.unwrap(), options(1)).unwrap();
    assert!(matches!(
        client.ping(None).await,
        Err(ClientError::Transport(Error::InvalidInput(
            "test denies all submissions"
        )))
    ));
    assert!(timeout(Duration::from_millis(20), read_message(&mut peer))
        .await
        .is_err());
    client.close().await.unwrap();
}

#[tokio::test]
async fn clone_and_distinct_stubs_keep_local_budgets_but_share_transport_admission_and_release() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = FabricClient::endpoint_clear(listener.local_addr().unwrap())
        .unwrap()
        .channel_limits(ChannelLimits {
            max_operations: 2,
            ..ChannelLimits::default()
        });
    let (channel, mut peer) = tokio::join!(endpoint.connect(), establish(listener));
    let channel = channel.unwrap();
    let first = FabricClient::new(channel.clone(), options(1)).unwrap();
    let cloned = first.clone();
    let second = FabricClient::new(channel.clone(), options(1)).unwrap();
    let third = FabricClient::new(channel.clone(), options(1)).unwrap();
    let mut a = Box::pin(first.ping(None));
    pending(a.as_mut()).await;
    let request_a = read_message(&mut peer).await.unwrap();
    assert!(matches!(
        cloned.ping(None).await,
        Err(ClientError::Local(Error::ResourceLimit(_)))
    ));
    let mut b = Box::pin(second.ping(None));
    pending(b.as_mut()).await;
    let request_b = read_message(&mut peer).await.unwrap();
    assert_ne!(
        request_a.properties().unwrap().id,
        request_b.properties().unwrap().id
    );
    assert!(matches!(
        third.ping(None).await,
        Err(ClientError::Transport(Error::ResourceLimit(_)))
    ));
    write_message(&mut peer, &ping_reply(&request_a))
        .await
        .unwrap();
    a.await.unwrap();
    // A transport rejection released third's local slot; a's completion released
    // both layers, while second remains pending on the shared connection.
    let mut c = Box::pin(third.ping(None));
    pending(c.as_mut()).await;
    let request_c = read_message(&mut peer).await.unwrap();
    write_message(&mut peer, &ping_reply(&request_c))
        .await
        .unwrap();
    c.await.unwrap();
    write_message(&mut peer, &ping_reply(&request_b))
        .await
        .unwrap();
    b.await.unwrap();
    cloned.close().await.unwrap();
    assert!(second.is_closed());
    assert!(matches!(
        third.ping(None).await,
        Err(ClientError::Transport(Error::Closed))
    ));
    channel
        .close(Instant::now() + TIMEOUT_DURATION)
        .await
        .unwrap();
}

#[tokio::test]
async fn connect_default_and_override_budgets_are_independent_and_start_per_operation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = FabricClient::endpoint_clear(listener.local_addr().unwrap())
        .unwrap()
        .connect_timeout(TIMEOUT_DURATION);
    let server = tokio::spawn(async move {
        let mut peer = establish(listener).await;
        // Each operation observes its own budget rather than the connection
        // budget or a timer begun at stub/configuration construction.
        for (maximum, minimum, respond) in [
            (Duration::from_millis(100), Duration::ZERO, false),
            (Duration::from_secs(2), Duration::from_secs(1), true),
            (Duration::from_secs(1), Duration::from_millis(500), true),
        ] {
            let request = read_message(&mut peer).await.unwrap();
            let ticks = request
                .headers
                .iter()
                .find(|h| h.id == TIMEOUT)
                .unwrap()
                .read(|d| d.i64())
                .unwrap();
            assert!(ticks > minimum.as_nanos() as i64 / 100);
            assert!(ticks <= maximum.as_nanos() as i64 / 100);
            if respond {
                write_message(&mut peer, &ping_reply(&request))
                    .await
                    .unwrap();
            }
        }
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    });
    let channel = endpoint.connect().await.unwrap();
    let client = FabricClient::new(
        channel.clone(),
        ClientOptions {
            default_timeout: Duration::from_millis(100),
            ..options(1)
        },
    )
    .unwrap();
    sleep(Duration::from_millis(150)).await;
    assert!(matches!(
        client.ping(None).await,
        Err(ClientError::Transport(Error::Timeout))
    ));
    client.ping(Some(Duration::from_secs(2))).await.unwrap();
    let distinct = FabricClient::new(
        channel,
        ClientOptions {
            default_timeout: Duration::from_secs(1),
            ..options(1)
        },
    )
    .unwrap();
    distinct.ping(None).await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}
