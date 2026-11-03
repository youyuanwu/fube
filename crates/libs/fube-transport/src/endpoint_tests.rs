use super::*;
use crate::Endpoint;
use tokio::net::TcpListener;

async fn connect(endpoint: &Endpoint, listener: &TcpListener) -> (crate::Channel, TcpStream) {
    let (channel, stream) = tokio::join!(endpoint.connect(), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let hello = read_message(&mut stream).await.unwrap();
        assert_eq!(hello.properties().unwrap().actor, 1);
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: [4; 16],
                    index: 1,
                },
                &ListenInstance {
                    address: "127.0.0.1:1".into(),
                    instance: 33,
                    nonce: [4; 16],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        stream
    });
    (channel.unwrap(), stream)
}

#[tokio::test]
async fn reusable_configuration_has_no_io_and_each_connect_keeps_actual_target_policy_and_history()
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint =
        Endpoint::unsecured_loopback(address, fixture_composition(GatewayProfile::Notifications))
            .unwrap()
            .channel_limits(ChannelLimits {
                max_history: 3,
                ..limits()
            });
    let copied = endpoint.clone();
    assert!(timeout(Duration::from_millis(20), listener.accept())
        .await
        .is_err());
    let (first, mut first_peer) = connect(&endpoint, &listener).await;
    let (second, mut second_peer) = connect(&copied, &listener).await;
    let target = first.target().unwrap();
    assert_eq!(target.address, address);
    assert_eq!(first.remote_instance().address, "127.0.0.1:1");
    assert!(Arc::ptr_eq(&target, &first.clone().target().unwrap()));
    assert!(Arc::ptr_eq(
        &target,
        &first.resolve_target(&address.to_string()).unwrap()
    ));
    assert!(!Arc::ptr_eq(&target, &second.target().unwrap()));
    assert!(first.resolve_target("127.0.0.1:1").is_err());
    assert!(matches!(
        second
            .request(&target, msg("Rpc", vec![]), deadline())
            .await,
        Err(Error::InvalidInput(_))
    ));
    for (channel, peer) in [(&first, &mut first_peer), (&second, &mut second_peer)] {
        let target = channel.target().unwrap();
        assert!(matches!(
            channel
                .send_one_way(&target, upload_message(26), deadline())
                .await,
            Err(Error::InvalidInput(_))
        ));
        // The identical caller ID is valid once in EACH independent history.
        let mut message = msg("Rpc", vec![]);
        message.headers.push(
            Header::message_id(MessageId {
                guid: [9; 16],
                index: 2,
            })
            .unwrap(),
        );
        let mut request = channel.request(&target, message.clone(), deadline());
        pending(request.as_mut()).await;
        let sent = read_message(peer).await.unwrap();
        write_message(peer, &reply(sent.properties().unwrap().id, vec![]))
            .await
            .unwrap();
        request.await.unwrap();
        assert!(channel.request(&target, message, deadline()).await.is_err());
        // Cloned registration/duplex policy still grants only the original context.
        write_message(peer, &notification(7)).await.unwrap();
        let incoming = channel
            .receiver()
            .unwrap()
            .receive(deadline())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&incoming.source, &target));
        assert!(incoming.reply.is_some());
        drop(incoming);
        channel.close(deadline()).await.unwrap();
        assert!(matches!(channel.target(), Err(Error::Closed)));
    }
}

#[tokio::test]
async fn invalid_endpoint_settings_never_dial_and_connect_budget_includes_hello() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint =
        Endpoint::unsecured_loopback(address, fixture_composition(GatewayProfile::Ordinary))
            .unwrap();
    for invalid in [
        endpoint.clone().connect_timeout(Duration::ZERO),
        endpoint.clone().channel_limits(ChannelLimits {
            max_operations: 0,
            ..limits()
        }),
    ] {
        assert!(matches!(
            invalid.connect().await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(timeout(Duration::from_millis(20), listener.accept())
        .await
        .is_err());
    let endpoint = endpoint.connect_timeout(Duration::from_millis(50));
    let (result, mut peer) = tokio::join!(endpoint.connect(), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_message(&mut stream).await.unwrap();
        stream
    });
    assert!(matches!(result, Err(Error::Timeout)));
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn publication_expiry_after_successful_hello_drops_unpublished_owner_and_joins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Endpoint::unsecured_loopback(
        listener.local_addr().unwrap(),
        fixture_composition(GatewayProfile::Ordinary),
    )
    .unwrap();
    let (channel, mut peer) = connect(&endpoint, &listener).await;
    let target = channel.target().unwrap();
    let weak = Arc::downgrade(&channel.owner.shared);
    let mut stopped = channel.owner.shared.stopped.subscribe();
    tokio::time::pause();
    let deadline = Instant::now() + Duration::from_secs(1);
    // Reproduce preemption after successful hello/owner construction, before
    // the final publication guard. No real-time race or production hook.
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        GatewayChannel::publish_before_deadline(channel, deadline),
        Err(Error::Timeout)
    ));
    stopped.wait_for(|value| *value).await.unwrap();
    until(|| weak.upgrade().is_none()).await;
    assert_eq!(target.address, listener.local_addr().unwrap());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}
