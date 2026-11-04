use super::*;
use crate::{accept_unsecured_loopback, native_demux::RequestPolicy, Endpoint};
use tokio::net::TcpListener;

fn server_composition() -> ChannelComposition {
    ChannelComposition {
        outbound: fixture_outbound,
        ordinary_reply: independent_reply,
        actors: ActorDemux::new(vec![ActorRegistration {
            actor: 70002,
            authorize: fixture_reply,
        }])
        .unwrap(),
        duplex: None,
        requests: Some(RequestPolicy {
            incoming: fixture_reply,
            reply: independent_response,
        }),
        ordered_ingress: false,
    }
}

async fn streams() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (accepted, peer) = tokio::join!(
        listener.accept(),
        TcpStream::connect(listener.local_addr().unwrap())
    );
    (accepted.unwrap().0, peer.unwrap())
}

fn initiating_hello(instance: &ListenInstance) -> Message {
    Message::listen_instance(
        MessageId {
            guid: [3; 16],
            index: 1,
        },
        instance,
    )
    .unwrap()
}

#[tokio::test]
async fn accepted_native_endpoint_pair_preserves_roles_and_explicit_send_reply_authority() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = Endpoint::unsecured_loopback(
        address,
        ChannelComposition {
            outbound: independent_outbound,
            ordinary_reply: independent_response,
            actors: ActorDemux::new(vec![ActorRegistration {
                actor: 5,
                authorize: fixture_reply,
            }])
            .unwrap(),
            duplex: None,
            requests: None,
            ordered_ingress: false,
        },
    )
    .unwrap()
    .channel_limits(limits());
    let (client, server) = tokio::join!(endpoint.connect(), async {
        let (stream, peer) = listener.accept().await.unwrap();
        (
            accept_unsecured_loopback(stream, server_composition(), limits(), deadline())
                .await
                .unwrap(),
            peer,
        )
    });
    let client = client.unwrap();
    let (server, peer) = server;
    let client_target = client.target().unwrap();
    let server_target = server.target().unwrap();
    assert_eq!(client_target.address, address);
    assert_eq!(server_target.address, peer);
    assert_eq!(server.remote_instance(), ListenInstance::default());
    assert_eq!(client.remote_instance().address, address.to_string());
    assert_eq!(client.remote_instance().instance, 1);
    assert_ne!(client.remote_instance().nonce, [0; 16]);
    assert!(matches!(
        server
            .send_one_way(
                &server_target,
                independent_message(70002, "RegisteredButNotAuthorized"),
                deadline(),
            )
            .await,
        Err(Error::InvalidInput("fixture outbound"))
    ));
    assert!(matches!(
        server
            .send_one_way(&client_target, msg("Foreign", vec![]), deadline())
            .await,
        Err(Error::InvalidInput("foreign channel target"))
    ));
    let id = MessageId {
        guid: [9; 16],
        index: 42,
    };
    let mut request = independent_message(70002, "Request");
    request.headers.push(Header::message_id(id).unwrap());
    let response = independent_message(70004, "Accepted");
    let receiver = server.receiver().unwrap();
    let (completed, ()) =
        tokio::join!(client.request(&client_target, request, deadline()), async {
            let incoming = receiver.receive(deadline()).await.unwrap();
            assert!(Arc::ptr_eq(&incoming.source, &server_target));
            let props = incoming.message.properties().unwrap();
            assert_eq!(props.id, id);
            assert!(props.expects_reply);
            incoming
                .reply
                .unwrap()
                .reply(response.clone(), deadline())
                .await
                .unwrap();
        });
    let mut expected = response;
    expected.headers.push(Header::relates_to(id).unwrap());
    assert_eq!(completed.unwrap(), expected);
    let receiver = client.receiver().unwrap();
    for reserved in [false, true] {
        let message = msg(if reserved { "SemanticReply" } else { "OneWay" }, vec![8]);
        let sent = if reserved {
            server.send_reserved(&server_target, message.clone(), deadline(), None)
        } else {
            server.send_one_way(&server_target, message.clone(), deadline())
        };
        sent.await.unwrap();
        let incoming = receiver.receive(deadline()).await.unwrap();
        // Native sends preserve the envelope: accepted IPC would add an ID.
        assert_eq!(incoming.message, message);
        assert!(incoming.reply.is_none());
    }
    server.close(deadline()).await.unwrap();
    client.close(deadline()).await.unwrap();
}

#[tokio::test]
async fn accepted_native_rejects_wrong_role_empty_identity_ipc_and_nonhello_without_publication() {
    let wrong_role = ListenInstance {
        nonce: [4; 16],
        ..ListenInstance::default()
    };
    let empty_id =
        Message::listen_instance(MessageId::default(), &ListenInstance::default()).unwrap();
    let mut ipc = initiating_hello(&ListenInstance::default());
    ipc.headers.push(
        Header::object(message::IPC, |e| {
            e.string("host")?;
            e.u32(42);
            Ok(())
        })
        .unwrap(),
    );
    for (hello, reason) in [
        (
            initiating_hello(&wrong_role),
            "invalid initiating native ListenInstance",
        ),
        (empty_id, "invalid initiating native ListenInstance"),
        (ipc, "IPC envelope on native channel"),
        (
            independent_message(70002, "NotAHello"),
            "expected native ListenInstance control",
        ),
    ] {
        let (stream, mut peer) = streams().await;
        let (result, ()) = tokio::join!(
            accept_unsecured_loopback(stream, server_composition(), limits(), deadline()),
            async {
                read_message(&mut peer).await.unwrap();
                write_message(&mut peer, &hello).await.unwrap();
                assert_eq!(
                    timeout_at(deadline(), peer.read(&mut [0]))
                        .await
                        .unwrap()
                        .unwrap(),
                    0
                );
            }
        );
        assert!(matches!(result, Err(Error::Protocol(error)) if error == reason));
    }
}

#[tokio::test]
async fn accepted_native_checks_limits_and_expired_deadline_before_any_hello() {
    for case in 0..3 {
        let (stream, mut peer) = streams().await;
        let mut settings = limits();
        if case == 0 {
            settings.max_operations = 0;
        } else if case == 1 {
            settings.max_frame_bytes = FIXED_HEADER_SIZE;
        }
        let result = accept_unsecured_loopback(
            stream,
            server_composition(),
            settings,
            if case == 2 {
                Instant::now()
            } else {
                deadline()
            },
        )
        .await;
        match case {
            0 => assert!(matches!(result, Err(Error::InvalidInput(_)))),
            1 => assert!(matches!(
                result,
                Err(Error::ResourceLimit("native ListenInstance frame limit"))
            )),
            _ => assert!(matches!(result, Err(Error::Timeout))),
        }
        assert_eq!(
            timeout_at(deadline(), peer.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn accepted_native_deadline_and_cancellation_dispose_stalled_unpublished_hello() {
    let (stream, mut peer) = streams().await;
    let budget = Duration::from_secs(60);
    let mut accepting = Box::pin(accept_unsecured_loopback(
        stream,
        server_composition(),
        limits(),
        Instant::now() + budget,
    ));
    tokio::select! {
        _ = &mut accepting => panic!("published before initiating hello"),
        hello = read_message(&mut peer) => { hello.unwrap(); }
    }
    tokio::time::pause();
    tokio::time::advance(budget).await;
    assert!(matches!(accepting.await, Err(Error::Timeout)));
    tokio::time::resume();
    assert_eq!(
        timeout_at(deadline(), peer.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );

    let (stream, mut peer) = streams().await;
    let mut accepting = Box::pin(accept_unsecured_loopback(
        stream,
        server_composition(),
        limits(),
        deadline(),
    ));
    tokio::select! {
        _ = &mut accepting => panic!("published before initiating hello"),
        hello = read_message(&mut peer) => {
            assert_ne!(ListenInstance::decode(&hello.unwrap().body).unwrap().nonce, [0; 16]);
        }
    }
    drop(accepting);
    assert_eq!(
        timeout_at(deadline(), peer.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn accepted_native_hello_frame_limit_rejects_fixed_header_without_waiting_for_body() {
    let (stream, mut peer) = streams().await;
    let settings = ChannelLimits {
        max_frame_bytes: 256,
        ..limits()
    };
    let bytes = Frame {
        headers: vec![],
        body: vec![0; 257 - FIXED_HEADER_SIZE],
    }
    .encode()
    .unwrap();
    let (result, ()) = tokio::join!(
        accept_unsecured_loopback(stream, server_composition(), settings, deadline()),
        async {
            read_message(&mut peer).await.unwrap();
            peer.write_all(&bytes[..FIXED_HEADER_SIZE]).await.unwrap();
            assert_eq!(
                timeout_at(deadline(), peer.read(&mut [0]))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        }
    );
    assert!(matches!(
        result,
        Err(Error::ResourceLimit("raw inbound frame limit"))
    ));
}

#[tokio::test]
async fn accepted_native_uses_actual_peer_identity_and_retains_native_envelope_after_hello() {
    let (stream, mut peer) = streams().await;
    let address = peer.local_addr().unwrap();
    let advertised = ListenInstance {
        address: "127.0.0.1:1".into(),
        instance: 99,
        nonce: [0; 16],
    };
    let (channel, ()) = tokio::join!(
        accept_unsecured_loopback(stream, server_composition(), limits(), deadline()),
        async {
            read_message(&mut peer).await.unwrap();
            write_message(&mut peer, &initiating_hello(&advertised))
                .await
                .unwrap();
        }
    );
    let channel = channel.unwrap();
    let target = channel.target().unwrap();
    assert_eq!(target.address, address);
    assert_eq!(channel.remote_instance(), advertised);
    assert!(Arc::ptr_eq(
        &target,
        &channel.resolve_target(&address.to_string()).unwrap()
    ));
    assert!(channel.resolve_target(&advertised.address).is_err());
    let mut incoming = independent_message(70002, "IpcIsNotNative");
    incoming.headers.push(
        Header::object(message::IPC, |e| {
            e.string("host")?;
            e.u32(42);
            Ok(())
        })
        .unwrap(),
    );
    write_message(&mut peer, &incoming).await.unwrap();
    assert!(matches!(
        channel.wait_terminal(deadline()).await.unwrap(),
        Error::Protocol("IPC envelope on native channel")
    ));
    channel.closed(deadline()).await.unwrap();
    assert_eq!(
        timeout_at(deadline(), peer.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}
