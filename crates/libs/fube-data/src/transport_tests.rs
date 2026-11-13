// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Real loopback Fabric TCP tests. The independent socket fixture below speaks
// native message/ListenInstance schemas, but is NOT a native SF executable and
// does not qualify native interoperability.

use super::*;
use crate::{wire::WireOperation, Epoch};
use std::{io::Read, time::Instant};
use tokio::{io::AsyncWriteExt, time::sleep};

fn actor(replica: i64) -> Actor {
    Actor {
        partition: Uuid::from_u128(17),
        replica,
        incarnation: Uuid::from_u128(replica as u128 + 100),
    }
}

fn endpoint(address: &str, replica: i64) -> Endpoint {
    Endpoint {
        address: address.into(),
        actor: actor(replica),
        transport_id: None,
    }
}

fn request(address: &str, from: i64, to: i64) -> Packet {
    Packet {
        from: endpoint(address, from),
        to: actor(to),
        body: Body::RequestAck,
    }
}

#[test]
fn dual_stack_peer_resolution_prefers_listener_family_without_dropping_single_family_destinations()
{
    let v4: SocketAddr = "127.0.0.1:31000".parse().unwrap();
    let v6: SocketAddr = "[::1]:31000".parse().unwrap();
    assert_eq!(peer_socket([v6, v4], v4).unwrap(), v4);
    assert_eq!(peer_socket([v4, v6], v6).unwrap(), v6);
    assert_eq!(peer_socket([v6], v4).unwrap(), v6);
    assert_eq!(peer_socket([v4], v6).unwrap(), v4);
    assert!(peer_socket([], v4).is_err());
}

#[tokio::test]
async fn wildcard_listener_advertises_concrete_identity_for_hello_and_bidirectional_packets() {
    let ip = "127.0.0.2".parse().unwrap();
    let (a, mut incoming_a) =
        PeerTransport::bind_with_advertised_ip("0.0.0.0:0", Some(ip), Settings::default()).unwrap();
    let (b, mut incoming_b) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    assert_eq!(a.address().parse::<SocketAddr>().unwrap().ip(), ip);
    let a_endpoint = endpoint(a.address(), 1);
    let b_endpoint = endpoint(b.address(), 2);
    a.register_peer(&b_endpoint).unwrap();
    b.register_peer(&a_endpoint).unwrap();
    a.send(request(a.address(), 1, 2)).unwrap();
    let packet = receive(incoming_b.recv_control()).await;
    assert_eq!(packet.from, a_endpoint);
    b.send(request(b.address(), 2, 1)).unwrap();
    assert_eq!(receive(incoming_a.recv_control()).await.from, b_endpoint);
    let mut socket = native_socket(a.address(), b.address()).await;
    socket.shutdown().await.unwrap();
    a.shutdown_checked().unwrap();
    b.shutdown_checked().unwrap();
}

#[test]
fn advertised_replication_ip_rejects_wildcard_multicast_and_mismatched_listeners() {
    assert!(PeerTransport::bind("0.0.0.0:0", Settings::default()).is_err());
    for (listen, advertised) in [
        ("0.0.0.0:0", "0.0.0.0"),
        ("0.0.0.0:0", "224.0.0.1"),
        ("0.0.0.0:0", "::1"),
        ("127.0.0.1:0", "127.0.0.2"),
    ] {
        assert!(matches!(
            PeerTransport::bind_with_advertised_ip(
                listen,
                Some(advertised.parse().unwrap()),
                Settings::default()
            ),
            Err(Error::InvalidInput(_))
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_one_shared_address_actor_keeps_the_other_actor_socket_usable() {
    let (a, _incoming_a) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let (b, mut incoming_b) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let alias = format!(
        "localhost:{}",
        b.address.parse::<SocketAddr>().unwrap().port()
    );
    a.register_peer(&endpoint(&alias, 2)).unwrap();
    a.register_peer(&endpoint(&b.address, 3)).unwrap();
    let first = a
        .send_observed(request(&a.address, 1, 2))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(receive(incoming_b.recv_control()).await.to, actor(2));
    a.unregister_peer(&actor(2)).unwrap();
    let second = a
        .send_observed(request(&a.address, 1, 3))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(receive(incoming_b.recv_control()).await.to, actor(3));
    assert_eq!(first.session, second.session);
    drop(first);
    drop(second);
    a.unregister_peer(&actor(3)).unwrap();
    for _ in 0..100 {
        if a.handle.network.live_addresses.lock().unwrap().is_empty() {
            break;
        }
        sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(a.handle.network.live_addresses.lock().unwrap().is_empty());
    assert!(a
        .handle
        .network
        .retired_addresses
        .lock()
        .unwrap()
        .is_empty());
    a.shutdown();
    b.shutdown();
}

fn operation(address: &str, from: i64, to: i64, bytes: usize) -> Packet {
    Packet {
        from: endpoint(address, from),
        to: actor(to),
        body: Body::Replication {
            primary_epoch: Epoch {
                data_loss_number: 1,
                configuration_number: 2,
            },
            operation_epoch: Epoch {
                data_loss_number: 1,
                configuration_number: 2,
            },
            operations: vec![WireOperation {
                sequence: 1,
                kind: 1,
                atomic_group: -1,
                data: vec![vec![31; bytes], vec![], vec![7]],
            }],
            batch_end: 1,
            completed: 0,
        },
    }
}

async fn receive(receiver: impl std::future::Future<Output = Option<Packet>>) -> Packet {
    timeout(std::time::Duration::from_secs(3), receiver)
        .await
        .expect("packet deadline")
        .expect("live incoming channel")
}

pub(crate) async fn native_socket(address: &str, published: &str) -> TcpStream {
    let mut socket = TcpStream::connect(address).await.unwrap();
    let hello = read_message(&mut socket).await.unwrap();
    assert_eq!(hello.properties().unwrap().actor, 1);
    let local = ListenInstance::decode(&hello.body).unwrap();
    assert_eq!(local.address, address);
    assert!(local.instance > 0);
    assert_ne!(local.nonce, [0; 16]);
    let hello = Message::listen_instance(
        MessageId {
            guid: [9; 16],
            index: 1,
        },
        &ListenInstance {
            address: published.into(),
            // Native replication transports default to instance zero, and an
            // initiating TcpConnection has no listen-side nonce yet.
            instance: 0,
            nonce: [0; 16],
        },
    )
    .unwrap();
    let bytes = hello.to_frame().unwrap().encode().unwrap();
    for chunk in bytes.chunks(3) {
        socket.write_all(chunk).await.unwrap();
        tokio::task::yield_now().await;
    }
    socket
}

async fn expect_closed(socket: &mut TcpStream) {
    let mut byte = [0];
    let result = timeout(std::time::Duration::from_secs(2), socket.read(&mut byte))
        .await
        .expect("session must close");
    assert!(matches!(result, Ok(0) | Err(_)));
}

#[tokio::test]
async fn ingress_grant_survives_registration_but_never_session_retirement_or_relearning() {
    let (transport, mut incoming) =
        PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let published = "127.0.0.1:32113";
    let mut selected = native_socket(transport.address(), published).await;
    write_message(
        &mut selected,
        &request(published, 3, 1).encode(true).unwrap(),
    )
    .await
    .unwrap();
    incoming.recv_bound().await.unwrap();
    let mut source = native_socket(transport.address(), published).await;
    write_message(&mut source, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    // Obtain authority through genuine earlier ingress from this exact session.
    let (marker, old_grant) = incoming.recv_bound().await.unwrap();
    assert!(old_grant.matches(&marker.from));
    write_message(&mut source, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    // Leave the old packet queued until after its original session retires.
    timeout(std::time::Duration::from_secs(2), async {
        while incoming.control.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(source);
    timeout(std::time::Duration::from_secs(2), async {
        while old_grant.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Retirement without any replacement cannot become an outbound dial.
    transport.register_peer(&endpoint(published, 2)).unwrap();
    assert!(matches!(
        transport.send_bound(request(transport.address(), 1, 2), &old_grant),
        Err(Error::Closed)
    ));
    let mut replacement = native_socket(transport.address(), published).await;
    write_message(
        &mut replacement,
        &request(published, 2, 1).encode(true).unwrap(),
    )
    .await
    .unwrap();
    let (old_packet, queued_old) = incoming.recv_bound().await.unwrap();
    assert_eq!(old_packet, marker);
    assert!(queued_old.matches(&old_packet.from));
    assert!(Weak::ptr_eq(&queued_old.state, &old_grant.state));
    assert!(!queued_old.is_live());
    assert!(transport
        .send_bound(request(transport.address(), 1, 2), &queued_old)
        .is_err());
    let (_, current) = incoming.recv_bound().await.unwrap();
    assert!(!Weak::ptr_eq(&current.state, &queued_old.state));
    transport.register_peer(&endpoint(published, 2)).unwrap();
    transport
        .send_bound(request(transport.address(), 1, 2), &current)
        .unwrap();
    assert_eq!(
        Packet::decode(&read_message(&mut replacement).await.unwrap())
            .unwrap()
            .body,
        Body::RequestAck
    );
    assert!(timeout(
        std::time::Duration::from_millis(30),
        read_message(&mut selected)
    )
    .await
    .is_err());
    transport.shutdown();
}

#[test]
fn bind_without_runtime_and_last_owner_drop_joins_and_releases_listener() {
    let (transport, mut incoming) =
        PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let address = transport.address().to_owned();
    let clone = transport.clone();
    drop(transport);
    let mut socket = std::net::TcpStream::connect(&address).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    let mut byte = [0];
    assert_eq!(socket.read(&mut byte).unwrap(), 1);
    let start = Instant::now();
    drop(clone);
    assert!(start.elapsed() < std::time::Duration::from_millis(250));
    assert!(std::net::TcpStream::connect(address).is_err());
    assert!(matches!(
        incoming.try_recv_control(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn real_fabric_tcp_is_duplex_and_reuses_an_accepted_socket() {
    let (a, mut a_in) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let (b, mut b_in) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    a.register_peer(&endpoint(b.address(), 2)).unwrap();
    let sent = operation(a.address(), 1, 2, 1024);
    a.send(sent.clone()).unwrap();
    assert_eq!(receive(b_in.recv_data()).await, sent);
    // No registration on b: its accepted reader learned the sender actor and
    // its writer must continuously progress on the same duplex connection.
    let ack = Packet {
        from: endpoint(b.address(), 2),
        to: actor(1),
        body: Body::Ack {
            received: 1,
            applied: 0,
            copy_received: -1,
            copy_applied: -1,
            error: 0,
        },
    };
    b.send(ack.clone()).unwrap();
    assert_eq!(receive(a_in.recv_control()).await, ack);
    a.send(request(a.address(), 1, 2)).unwrap();
    assert_eq!(receive(b_in.recv_control()).await.body, Body::RequestAck);
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn observed_invalid_frame_completion_keeps_exact_bytes_tuple_and_bounded_charge() {
    let settings = Settings {
        control_messages: 1,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings.clone()).unwrap();
    let published = "127.0.0.1:32112";
    let mut socket = native_socket(transport.address(), published).await;
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    let mut invalid = request(transport.address(), 1, 2);
    invalid.from.actor.incarnation = Uuid::new_v4();
    let completion = transport.send_observed(invalid.clone()).unwrap();
    let mut fixed = [0; FIXED_HEADER_SIZE];
    socket.read_exact(&mut fixed).await.unwrap();
    let (size, _) = validate_fixed(&fixed).unwrap();
    let mut actual = fixed.to_vec();
    actual.resize(size, 0);
    socket
        .read_exact(&mut actual[FIXED_HEADER_SIZE..])
        .await
        .unwrap();
    let receipt = timeout(std::time::Duration::from_secs(2), completion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.frame, actual);
    let message = Message::from_frame(Frame::decode(&actual).unwrap()).unwrap();
    assert_eq!(Packet::decode(&message).unwrap(), invalid);
    assert_eq!(receipt.message_id, message.properties().unwrap().id);
    assert_ne!(receipt.message_id, MessageId::default());
    assert_eq!(receipt.local, socket.peer_addr().unwrap());
    assert_eq!(receipt.remote, socket.local_addr().unwrap());
    assert_eq!(receipt.remote_listener, published);
    assert!(receipt.session > 0);
    assert!(matches!(
        transport.send_observed(invalid.clone()),
        Err(Error::ResourceLimit("outgoing peer queue"))
    ));
    assert!(transport.handle.network.control.bytes.available_permits() < settings.control_bytes);
    drop(receipt);
    assert_eq!(
        transport.handle.network.control.bytes.available_permits(),
        settings.control_bytes
    );
    let completion = transport.send_observed(invalid).unwrap();
    read_message(&mut socket).await.unwrap();
    let next = completion.await.unwrap();
    assert_ne!(next.message_id, message.properties().unwrap().id);
    drop(next);
    transport.shutdown();
}

#[tokio::test]
async fn observed_admission_is_an_error_when_no_frame_can_reach_a_socket() {
    let settings = Settings {
        callback_timeout: std::time::Duration::from_millis(100),
        connections: 1,
        ..Settings::default()
    };
    for failure in ["refused", "handshake", "session_limit", "shutdown"] {
        let (transport, _incoming) = PeerTransport::bind("127.0.0.1:0", settings.clone()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        transport
            .register_peer(&endpoint(&listener.local_addr().unwrap().to_string(), 2))
            .unwrap();
        let occupied = if failure == "session_limit" {
            Some(native_socket(transport.address(), "127.0.0.1:32113").await)
        } else {
            None
        };
        if failure == "refused" {
            drop(listener);
            let completion = transport
                .send_observed(request(transport.address(), 1, 2))
                .unwrap();
            assert!(timeout(std::time::Duration::from_secs(1), completion)
                .await
                .unwrap()
                .is_err());
        } else {
            let completion = transport
                .send_observed(request(transport.address(), 1, 2))
                .unwrap();
            if failure == "handshake" {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_message(&mut socket).await.unwrap();
                // Never send the peer ListenInstance.
                assert!(timeout(std::time::Duration::from_secs(1), completion)
                    .await
                    .unwrap()
                    .is_err());
            } else {
                if failure == "shutdown" {
                    transport.shutdown();
                }
                assert!(timeout(std::time::Duration::from_secs(1), completion)
                    .await
                    .unwrap()
                    .is_err());
            }
        }
        drop(occupied);
        transport.shutdown();
        assert!(matches!(
            transport.send_observed(request(transport.address(), 1, 2)),
            Err(Error::Closed)
        ));
        assert_eq!(
            transport.handle.network.control.bytes.available_permits(),
            settings.control_bytes
        );
        assert_eq!(
            transport
                .handle
                .network
                .control
                .outgoing_slots
                .available_permits(),
            settings.control_messages
        );
    }
}

#[tokio::test]
async fn observed_receiver_cancellation_does_not_retain_or_detach_a_session() {
    let settings = Settings {
        callback_timeout: std::time::Duration::from_millis(100),
        ..Settings::default()
    };
    let (transport, _incoming) = PeerTransport::bind("127.0.0.1:0", settings.clone()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    transport
        .register_peer(&endpoint(&listener.local_addr().unwrap().to_string(), 2))
        .unwrap();
    let completion = transport
        .send_observed(request(transport.address(), 1, 2))
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    drop(completion);
    let started = Instant::now();
    transport.shutdown();
    assert!(started.elapsed() < std::time::Duration::from_millis(250));
    drop(socket);
    assert_eq!(
        transport.handle.network.control.bytes.available_permits(),
        settings.control_bytes
    );
    assert_eq!(
        transport
            .handle
            .network
            .control
            .outgoing_slots
            .available_permits(),
        settings.control_messages
    );
}

#[tokio::test]
async fn observed_unregistered_and_oversized_frames_never_take_a_queue_charge() {
    let settings = Settings::default();
    let (transport, _incoming) = PeerTransport::bind("127.0.0.1:0", settings.clone()).unwrap();
    assert!(matches!(
        transport.send_observed(request(transport.address(), 1, 2)),
        Err(Error::InvalidInput("unregistered peer actor"))
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    transport
        .register_peer(&endpoint(&listener.local_addr().unwrap().to_string(), 2))
        .unwrap();
    assert!(matches!(
        transport.send_observed(operation(transport.address(), 1, 2, 16 * 1024)),
        Err(Error::ResourceLimit("qualification frame bytes"))
    ));
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        settings.queue_bytes
    );
    assert_eq!(
        transport
            .handle
            .network
            .data
            .outgoing_slots
            .available_permits(),
        settings.queue_operations
    );
    assert!(
        timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    transport.shutdown();
}

#[tokio::test]
async fn fragmented_native_listen_instance_and_header_in_body_packets() {
    let (transport, mut incoming) =
        PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let published = "127.0.0.1:32101";
    let mut socket = native_socket(transport.address(), published).await;
    let packet = operation(published, 2, 1, 37);
    let bytes = packet
        .encode(true)
        .unwrap()
        .to_frame()
        .unwrap()
        .encode()
        .unwrap();
    for chunk in bytes.chunks(2) {
        socket.write_all(chunk).await.unwrap();
        tokio::task::yield_now().await;
    }
    assert_eq!(receive(incoming.recv_data()).await, packet);
    transport.send(request(transport.address(), 1, 2)).unwrap();
    let response = read_message(&mut socket).await.unwrap();
    assert_eq!(
        Packet::decode(&response).unwrap(),
        request(transport.address(), 1, 2)
    );
    transport.shutdown();
    expect_closed(&mut socket).await;
}

#[tokio::test]
async fn saturated_incoming_data_preserves_control_and_never_acknowledges() {
    let settings = Settings {
        operation_bytes: 4096,
        queue_operations: 1,
        queue_bytes: 8192,
        control_messages: 1,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let published = "127.0.0.1:32102";
    let mut socket = native_socket(transport.address(), published).await;
    let data = operation(published, 2, 1, 1024).encode(true).unwrap();
    write_message(&mut socket, &data).await.unwrap();
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    let data_free = transport.handle.network.data.bytes.available_permits();
    assert!(data_free < 8192);
    for _ in 0..63 {
        write_message(&mut socket, &data).await.unwrap();
    }
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    assert_eq!(
        receive(incoming.recv_control()).await.body,
        Body::RequestAck
    );
    assert_eq!(incoming.data.len(), 1);
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        data_free
    );
    assert!(timeout(
        std::time::Duration::from_millis(30),
        read_message(&mut socket)
    )
    .await
    .is_err());
    // Control saturation is terminal, but rejection must retain only the
    // already queued item, not the failed admission or its source.
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    timeout(std::time::Duration::from_secs(2), async {
        while incoming.control.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let control_free = transport.handle.network.control.bytes.available_permits();
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    expect_closed(&mut socket).await;
    assert_eq!(incoming.control.len(), 1);
    assert_eq!(
        transport.handle.network.control.bytes.available_permits(),
        control_free
    );
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        data_free
    );
    drop(incoming);
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        8192
    );
    assert_eq!(
        transport.handle.network.control.bytes.available_permits(),
        Settings::default().control_bytes
    );
    transport.shutdown();
}

#[tokio::test]
async fn incoming_cancel_priority_try_receive_and_drop_own_both_classes() {
    let settings = Settings {
        operation_bytes: 4096,
        queue_bytes: 4096,
        control_bytes: 4096,
        queue_operations: 2,
        control_messages: 2,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let published = "127.0.0.1:32114";
    let mut socket = native_socket(transport.address(), published).await;
    let wait = std::time::Duration::from_millis(1);
    assert!(timeout(wait, incoming.recv_control()).await.is_err());
    assert!(timeout(wait, incoming.recv_data()).await.is_err());
    assert!(timeout(wait, incoming.recv()).await.is_err());
    assert!(timeout(wait, incoming.recv_bound()).await.is_err());

    println!(
        "owned ingress layout: Packet={}, SourceTarget={}, permit={}, Ingress={}",
        std::mem::size_of::<Packet>(),
        std::mem::size_of::<SourceTarget>(),
        std::mem::size_of::<OwnedSemaphorePermit>(),
        std::mem::size_of::<Ingress>()
    );
    for (control_actor, data_actor) in [(2, 3), (4, 5), (6, 7), (8, 9)] {
        let control = request(published, control_actor, 1);
        let data = operation(published, data_actor, 1, 64);
        write_message(&mut socket, &data.encode(true).unwrap())
            .await
            .unwrap();
        write_message(&mut socket, &control.encode(true).unwrap())
            .await
            .unwrap();
        timeout(std::time::Duration::from_secs(2), async {
            while incoming.control.len() != 1 || incoming.data.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(transport.handle.network.control.bytes.available_permits() < 4096);
        assert!(transport.handle.network.data.bytes.available_permits() < 4096);
        match control_actor {
            2 => {
                let (received, source) = incoming.recv_bound().await.unwrap();
                assert_eq!(received, control, "ready control wins over data");
                assert!(source.matches(&received.from));
                assert!(source.is_live());
                assert_eq!(
                    transport.handle.network.control.bytes.available_permits(),
                    4096
                );
                assert_eq!(incoming.data.len(), 1);
                let received_data = receive(incoming.recv_data()).await;
                assert_eq!(received_data, data);
                assert_eq!(
                    transport.handle.network.data.bytes.available_permits(),
                    4096
                );
                assert!(
                    source.is_live(),
                    "identity does not depend on the byte lease"
                );
            }
            4 => {
                let received = incoming.try_recv_control().unwrap();
                assert_eq!(received, control);
                assert_eq!(
                    transport.handle.network.control.bytes.available_permits(),
                    4096
                );
                let received_data = incoming.try_recv_data().unwrap();
                assert_eq!(received_data, data);
                assert_eq!(
                    transport.handle.network.data.bytes.available_permits(),
                    4096
                );
                assert!(matches!(
                    incoming.try_recv_control(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
                assert!(matches!(
                    incoming.try_recv_data(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            }
            6 => {
                let received = receive(incoming.recv()).await;
                assert_eq!(
                    received, control,
                    "public combined receive prefers ready control"
                );
                assert_eq!(
                    transport.handle.network.control.bytes.available_permits(),
                    4096
                );
                assert_eq!(incoming.data.len(), 1);
                assert_eq!(receive(incoming.recv_data()).await, data);
                assert_eq!(
                    transport.handle.network.data.bytes.available_permits(),
                    4096
                );
            }
            _ => break, // Both remaining envelopes are deliberately undrained.
        }
    }
    drop(incoming);
    assert_eq!(
        transport.handle.network.control.bytes.available_permits(),
        4096
    );
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        4096
    );
    assert!(!transport.handle.stopped.load(Ordering::Acquire));
    transport.shutdown();
}

#[tokio::test]
async fn incoming_closed_control_wakes_pending_receive_without_discarding_buffered_data() {
    use std::future::Future;
    let (transport, mut incoming) =
        PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let published = "127.0.0.1:32115";
    let mut socket = native_socket(transport.address(), published).await;
    let data = operation(published, 2, 1, 64);
    write_message(&mut socket, &data.encode(true).unwrap())
        .await
        .unwrap();
    timeout(std::time::Duration::from_secs(2), async {
        while incoming.data.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut pending = std::pin::pin!(incoming.recv_control());
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                pending.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        transport.shutdown();
        assert!(timeout(std::time::Duration::from_secs(2), pending)
            .await
            .unwrap()
            .is_none());
    }
    assert!(incoming.recv().await.is_none());
    assert!(incoming.recv_bound().await.is_none());
    assert_eq!(incoming.data.len(), 1);
    let received = receive(incoming.recv_data()).await;
    assert_eq!(received, data);
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        Settings::default().queue_bytes
    );
    assert!(incoming.recv_data().await.is_none());
    assert!(matches!(
        incoming.try_recv_data(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn aggregate_bytes_include_queued_incoming_and_empty_segment_metadata() {
    let settings = Settings {
        operation_bytes: 2048,
        queue_operations: 8,
        queue_bytes: 2048,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let published = "127.0.0.1:32103";
    let mut socket = native_socket(transport.address(), published).await;
    let packet = operation(published, 2, 1, 1250);
    write_message(&mut socket, &packet.encode(true).unwrap())
        .await
        .unwrap();
    // A control packet is an ordering marker for the reader, independent of
    // the still-undrained data queue and its retained charge.
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    assert!(matches!(
        transport.send(operation(transport.address(), 1, 2, 1250)),
        Err(Error::ResourceLimit(_))
    ));
    let received = receive(incoming.recv_data()).await;
    assert_eq!(received, packet);
    // No subsequent admission is allowed to reconcile a forgotten lease.
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        2048
    );
    transport
        .send(operation(transport.address(), 1, 2, 1250))
        .unwrap();
    assert!(matches!(
        Packet::decode(&read_message(&mut socket).await.unwrap())
            .unwrap()
            .body,
        Body::Replication { .. }
    ));
    let mut empty = operation(published, 2, 1, 0);
    if let Body::Replication { operations, .. } = &mut empty.body {
        operations[0].data = vec![vec![]; 1024];
    }
    write_message(&mut socket, &empty.encode(true).unwrap())
        .await
        .unwrap();
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    assert!(incoming.data.is_empty());
    transport.shutdown();
}

#[tokio::test]
async fn data_admission_exhaustion_does_not_consume_control_capacity() {
    let settings = Settings {
        operation_bytes: 4096,
        queue_bytes: 4096,
        queue_operations: 1,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = endpoint(&listener.local_addr().unwrap().to_string(), 2);
    transport.register_peer(&peer).unwrap();
    transport
        .send(operation(transport.address(), 1, 2, 1024))
        .unwrap();
    // The peer has not exchanged ListenInstance. The admitted data therefore
    // retains its slot even after leaving the supervisor's top-level channel.
    let (mut socket, _) = listener.accept().await.unwrap();
    assert!(matches!(
        transport.send(operation(transport.address(), 1, 2, 1024)),
        Err(Error::ResourceLimit(_))
    ));
    transport.send(request(transport.address(), 1, 2)).unwrap();
    let hello = read_message(&mut socket).await.unwrap();
    assert_eq!(hello.properties().unwrap().actor, 1);
    write_message(
        &mut socket,
        &Message::listen_instance(
            MessageId {
                guid: [3; 16],
                index: 1,
            },
            &ListenInstance {
                address: peer.address.clone(),
                instance: 1,
                nonce: [4; 16],
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        Packet::decode(&read_message(&mut socket).await.unwrap())
            .unwrap()
            .body,
        Body::RequestAck
    );
    // A received packet is not an implicit semantic receipt.
    assert!(incoming.try_recv_control().is_err());
    transport.shutdown();
}

#[tokio::test]
async fn control_bytes_are_aggregated_and_released_after_receive() {
    let (transport, mut incoming) = PeerTransport::bind(
        "127.0.0.1:0",
        Settings {
            control_bytes: 512,
            ..Settings::default()
        },
    )
    .unwrap();
    let published = "127.0.0.1:32110";
    let mut socket = native_socket(transport.address(), published).await;
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    timeout(std::time::Duration::from_secs(2), async {
        while incoming.control.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        transport.send(request(transport.address(), 1, 2)),
        Err(Error::ResourceLimit(_))
    ));
    let received = receive(incoming.recv_control()).await;
    assert_eq!(received.body, Body::RequestAck);
    assert_eq!(
        transport.handle.network.control.bytes.available_permits(),
        512
    );
    transport.send(request(transport.address(), 1, 2)).unwrap();
    assert_eq!(
        Packet::decode(&read_message(&mut socket).await.unwrap())
            .unwrap()
            .body,
        Body::RequestAck
    );
    transport.shutdown();
}

#[tokio::test]
async fn accepted_and_initiated_sockets_share_one_connection_limit() {
    let settings = Settings {
        connections: 1,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let published = "127.0.0.1:32104";
    let mut occupied = native_socket(transport.address(), published).await;
    write_message(
        &mut occupied,
        &request(published, 2, 1).encode(true).unwrap(),
    )
    .await
    .unwrap();
    receive(incoming.recv_control()).await;
    let mut second = TcpStream::connect(transport.address()).await.unwrap();
    assert!(timeout(
        std::time::Duration::from_millis(40),
        read_message(&mut second)
    )
    .await
    .is_err());
    // Replies use the existing accepted socket despite the full connection cap.
    transport.send(request(transport.address(), 1, 2)).unwrap();
    assert_eq!(
        Packet::decode(&read_message(&mut occupied).await.unwrap())
            .unwrap()
            .body,
        Body::RequestAck
    );
    let other = TcpListener::bind("127.0.0.1:0").await.unwrap();
    transport
        .register_peer(&endpoint(&other.local_addr().unwrap().to_string(), 2))
        .unwrap();
    transport.send(request(transport.address(), 1, 2)).unwrap();
    assert!(
        timeout(std::time::Duration::from_millis(40), other.accept())
            .await
            .is_err()
    );
    transport.shutdown();
    expect_closed(&mut occupied).await;
}

#[tokio::test]
async fn malformed_security_and_identity_only_retire_the_offending_session() {
    let (transport, mut incoming) = PeerTransport::bind(
        "127.0.0.1:0",
        Settings {
            connections: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    for attack in 0..5 {
        let published = "127.0.0.1:32105";
        let mut bad = native_socket(transport.address(), published).await;
        let mut message = request(published, 2, 1).encode(true).unwrap();
        if attack == 1 {
            message = request("127.0.0.1:32106", 2, 1).encode(true).unwrap();
        } else if attack == 2 {
            message
                .headers
                .push(fube_transport::message::Header::actor(7).unwrap());
        } else if attack == 3 {
            message.headers.push(message.headers[0].clone());
        } else if attack == 4 {
            message
                .headers
                .push(fube_transport::message::Header::boolean(0x8029, true).unwrap());
        }
        let mut bytes = message.to_frame().unwrap().encode().unwrap();
        if attack == 0 {
            bytes[4] = 1;
        }
        bad.write_all(&bytes).await.unwrap();
        expect_closed(&mut bad).await;
        assert!(incoming.control.is_empty());
    }
    let published = "127.0.0.1:32107";
    let mut good = native_socket(transport.address(), published).await;
    write_message(&mut good, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    transport.shutdown();
}

#[tokio::test]
async fn invalid_listener_address_releases_its_session_and_byte_charge() {
    let (transport, mut incoming) = PeerTransport::bind(
        "127.0.0.1:0",
        Settings {
            connections: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    for address in ["", "not-a-listener"] {
        let mut bad = TcpStream::connect(transport.address()).await.unwrap();
        read_message(&mut bad).await.unwrap();
        write_message(
            &mut bad,
            &Message::listen_instance(
                MessageId {
                    guid: [1; 16],
                    index: 1,
                },
                &ListenInstance {
                    address: address.into(),
                    instance: 0,
                    nonce: [0; 16],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        expect_closed(&mut bad).await;
    }
    let published = "127.0.0.1:32111";
    let mut good = native_socket(transport.address(), published).await;
    write_message(&mut good, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    transport.shutdown();
}

#[tokio::test]
async fn native_zero_instance_acceptor_and_directional_nonce_are_wire_compatible() {
    let (transport, mut incoming) =
        PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap().to_string();
    let source = transport.address().to_owned();
    transport.register_peer(&endpoint(&remote, 2)).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let hello = read_message(&mut socket).await.unwrap();
        let hello = ListenInstance::decode(&hello.body).unwrap();
        assert_eq!(hello.address, source);
        assert_eq!(
            hello.nonce, [0; 16],
            "initiating connections have no listen-side nonce"
        );
        write_message(
            &mut socket,
            &Message::listen_instance(
                MessageId {
                    guid: [1; 16],
                    index: 1,
                },
                &ListenInstance {
                    address: remote.clone(),
                    instance: 0,
                    nonce: [7; 16],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let packet = Packet::decode(&read_message(&mut socket).await.unwrap()).unwrap();
        assert_eq!(packet.body, Body::RequestAck);
        let reply = request(&remote, 2, 1);
        write_message(&mut socket, &reply.encode(true).unwrap())
            .await
            .unwrap();
        reply
    });
    transport.send(request(transport.address(), 1, 2)).unwrap();
    assert_eq!(
        receive(incoming.recv_control()).await,
        server.await.unwrap()
    );
    transport.shutdown();
}

#[tokio::test]
async fn partial_frame_and_handshake_deadlines_release_connection_slots() {
    let settings = Settings {
        connections: 1,
        callback_timeout: std::time::Duration::from_millis(100),
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let mut no_hello = TcpStream::connect(transport.address()).await.unwrap();
    read_message(&mut no_hello).await.unwrap();
    expect_closed(&mut no_hello).await;
    let published = "127.0.0.1:32108";
    let mut partial = native_socket(transport.address(), published).await;
    partial.write_all(&[42]).await.unwrap();
    expect_closed(&mut partial).await;
    let mut good = native_socket(transport.address(), published).await;
    write_message(&mut good, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    // Idle, fully exchanged peers do not time out.
    sleep(std::time::Duration::from_millis(150)).await;
    write_message(&mut good, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    transport.shutdown();
}

#[tokio::test]
async fn shutdown_is_terminal_synchronous_and_drains_a_backpressured_writer() {
    let settings = Settings {
        queue_operations: 32,
        ..Settings::default()
    };
    let (transport, mut incoming) = PeerTransport::bind("127.0.0.1:0", settings).unwrap();
    let published = "127.0.0.1:32109";
    let mut socket = native_socket(transport.address(), published).await;
    write_message(&mut socket, &request(published, 2, 1).encode(true).unwrap())
        .await
        .unwrap();
    receive(incoming.recv_control()).await;
    for _ in 0..32 {
        let _ = transport.send(operation(transport.address(), 1, 2, 1024 * 1024 - 1));
    }
    sleep(std::time::Duration::from_millis(30)).await;
    // At least the first frame has actually become visible, and the large
    // queued train has backpressured the socket. Closing must not drop a
    // selected writer's partial suffix.
    let mut fixed = [0; FIXED_HEADER_SIZE];
    socket.read_exact(&mut fixed).await.unwrap();
    let closing = transport.clone();
    let joined = tokio::task::spawn_blocking(move || closing.shutdown());
    let draining = async {
        let mut frames = 0;
        loop {
            let size = validate_fixed(&fixed).unwrap().0;
            let mut bytes = fixed.to_vec();
            bytes.resize(size, 0);
            socket
                .read_exact(&mut bytes[FIXED_HEADER_SIZE..])
                .await
                .expect("shutdown retains every started frame suffix");
            let packet =
                Packet::decode(&Message::from_frame(Frame::decode(&bytes).unwrap()).unwrap())
                    .unwrap();
            assert!(matches!(packet.body, Body::Replication { .. }));
            frames += 1;
            if socket.read(&mut fixed[..1]).await.unwrap() == 0 {
                break;
            }
            socket
                .read_exact(&mut fixed[1..])
                .await
                .expect("no truncated next frame");
        }
        assert!(frames > 0);
    };
    timeout(std::time::Duration::from_secs(3), draining)
        .await
        .unwrap();
    timeout(std::time::Duration::from_secs(3), joined)
        .await
        .unwrap()
        .unwrap();
    transport.shutdown();
    assert!(matches!(
        transport.send(request(transport.address(), 1, 2)),
        Err(Error::Closed)
    ));
    assert_eq!(
        transport.handle.network.data.bytes.available_permits(),
        transport.handle.network.settings.queue_bytes
    );
    assert_eq!(
        transport
            .handle
            .network
            .data
            .outgoing_slots
            .available_permits(),
        32
    );
    assert!(incoming.recv_control().await.is_none());
    assert!(std::net::TcpStream::connect(transport.address()).is_err());
}

#[test]
fn invalid_limits_source_and_unknown_destinations_fail_without_socket_work() {
    let settings = Settings {
        connections: 0,
        ..Settings::default()
    };
    assert!(PeerTransport::bind("127.0.0.1:0", settings).is_err());
    let (transport, _) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    assert!(matches!(
        transport.send(request(transport.address(), 1, 2)),
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        transport.send(request("127.0.0.1:1", 1, 2)),
        Err(Error::InvalidInput(_))
    ));
    transport.shutdown();
    assert!(matches!(
        transport.register_peer(&endpoint("127.0.0.1:1234", 2)),
        Err(Error::Closed)
    ));
}
