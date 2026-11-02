use super::*;
use crate::transport::tests::native_socket;
use fube_transport::{
    connection::{read_message, write_message},
    message::Message,
};
use tokio::net::TcpStream;

async fn send(socket: &mut TcpStream, primary: &Endpoint, engine: &Engine, body: Body) {
    write_message(
        socket,
        &Packet {
            from: primary.clone(),
            to: engine.endpoint().actor,
            body,
        }
        .encode(true)
        .unwrap(),
    )
    .await
    .unwrap();
}

async fn ack(socket: &mut TcpStream) -> Packet {
    let message: Message = timeout(std::time::Duration::from_secs(2), read_message(socket))
        .await
        .unwrap()
        .unwrap();
    let packet = Packet::decode(&message).unwrap();
    assert!(matches!(packet.body, Body::Ack { .. }));
    packet
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_secondary_start_copy_and_apply_ack_keep_the_nonselected_source_session() {
    let partition = Uuid::new_v4();
    let secondary = engine(partition, 2, Arc::new(Provider::default()));
    secondary
        .change_role(EPOCH, ReplicaRole::IdleSecondary, cancel())
        .await
        .unwrap();
    let published = "127.0.0.1:32114";
    let primary = Endpoint {
        address: published.into(),
        actor: Actor {
            partition,
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        transport_id: None,
    };
    let mut selected = native_socket(&secondary.endpoint().address, published).await;
    let mut other = primary.clone();
    other.actor.replica = 3;
    send(&mut selected, &other, &secondary, Body::RequestAck).await;
    until(|| {
        secondary.transport.diagnostics()["events"]["received_control"][0]
            .as_u64()
            .unwrap_or(0)
            >= 1
    })
    .await;
    let mut original = native_socket(&secondary.endpoint().address, published).await;
    send(
        &mut original,
        &primary,
        &secondary,
        Body::StartCopy {
            epoch: EPOCH,
            target: 2,
            start: 1,
        },
    )
    .await;
    ack(&mut original).await;
    assert_eq!(
        secondary.snapshot().observed_primary,
        Some(primary.to_string())
    );
    // Owner::packet registers the source endpoint before this response. The
    // address-selected session must still not receive the semantic ACK.
    assert!(timeout(
        std::time::Duration::from_millis(30),
        read_message(&mut selected)
    )
    .await
    .is_err());
    send(
        &mut original,
        &primary,
        &secondary,
        Body::Copy {
            epoch: EPOCH,
            target: 2,
            sequence: 1,
            data: vec![vec![7]],
            last: false,
        },
    )
    .await;
    let item = secondary
        .copy_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    let receipt = timeout(std::time::Duration::from_secs(2), async {
        loop {
            let receipt = ack(&mut original).await;
            if matches!(
                receipt.body,
                Body::Ack {
                    copy_received: 1,
                    ..
                }
            ) {
                break receipt;
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        receipt.body,
        Body::Ack {
            copy_received: 1,
            copy_applied: 0,
            ..
        }
    ));
    item.acknowledge().unwrap();
    loop {
        if matches!(
            ack(&mut original).await.body,
            Body::Ack {
                copy_applied: 1,
                ..
            }
        ) {
            break;
        }
    }
    assert!(timeout(
        std::time::Duration::from_millis(30),
        read_message(&mut selected)
    )
    .await
    .is_err());
    abort_owned(&secondary).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_engine_ingress_cannot_reconstruct_a_retired_source_from_registration() {
    delayed_ingress_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_engine_ingress_without_replacement_never_dials_the_registered_address() {
    delayed_ingress_case(false).await;
}

async fn delayed_ingress_case(relearn: bool) {
    let partition = Uuid::new_v4();
    let secondary = engine(partition, 2, Arc::new(Provider::default()));
    secondary
        .change_role(EPOCH, ReplicaRole::IdleSecondary, cancel())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let published = listener.local_addr().unwrap().to_string();
    let primary = Endpoint {
        address: published.clone(),
        actor: Actor {
            partition,
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        transport_id: None,
    };
    let (entered, waiting) = std_mpsc::sync_channel(1);
    let (resume, resumed) = std_mpsc::sync_channel(1);
    *secondary.shared.ingress_pause.lock().unwrap() = Some((entered, resumed));
    let mut original = native_socket(&secondary.endpoint().address, &published).await;
    send(
        &mut original,
        &primary,
        &secondary,
        Body::StartCopy {
            epoch: EPOCH,
            target: 2,
            start: 1,
        },
    )
    .await;
    tokio::task::spawn_blocking(move || {
        waiting
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap()
    })
    .await
    .unwrap();
    drop(original);
    until(|| {
        secondary.transport.diagnostics()["events"]["session_end"][0]
            .as_u64()
            .unwrap_or(0)
            >= 1
    })
    .await;
    // Both no-current-route registration and same-source relearning happen
    // before the old packet reaches production validation/ACK enqueue.
    secondary.transport.register_peer(&primary).unwrap();
    let mut replacement = if relearn {
        let mut socket = native_socket(&secondary.endpoint().address, &published).await;
        send(&mut socket, &primary, &secondary, Body::RequestAck).await;
        until(|| {
            secondary.transport.diagnostics()["events"]["received_control"][0]
                .as_u64()
                .unwrap_or(0)
                >= 2
        })
        .await;
        Some(socket)
    } else {
        None
    };
    resume.send(()).unwrap();
    if let Some(socket) = replacement.as_mut() {
        assert!(
            timeout(std::time::Duration::from_millis(80), read_message(socket))
                .await
                .is_err()
        );
    }
    assert!(
        timeout(std::time::Duration::from_millis(80), listener.accept())
            .await
            .is_err(),
        "old ingress cannot turn into a registered-address dial"
    );
    assert!(
        secondary.snapshot().observed_primary.is_none(),
        "retired ingress never starts copy"
    );
    // A genuinely new source message is independently usable, not poisoned by
    // the stale ingress and not interpreted as remote acknowledgment.
    let mut replacement = match replacement {
        Some(socket) => socket,
        None => native_socket(&secondary.endpoint().address, &published).await,
    };
    send(
        &mut replacement,
        &primary,
        &secondary,
        Body::StartCopy {
            epoch: EPOCH,
            target: 2,
            start: 1,
        },
    )
    .await;
    ack(&mut replacement).await;
    assert_eq!(
        secondary.snapshot().observed_primary,
        Some(primary.to_string())
    );
    abort_owned(&secondary).await;
}
