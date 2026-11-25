use super::*;
use crate::control::wire::{deadline, FmReport, NodeClient, NodeRequest};
use fube_ra::protocol::{
    get_lsn_reply_message, FabricVersionInstance, GenerationNumber, GetLsnReply, NodeUp, NodeUpAck,
};
use std::sync::Arc;
use tokio::net::TcpListener;

fn node(id: u8, instance: u64) -> NodeInstance {
    NodeInstance {
        id: [id; 16],
        instance,
    }
}

fn generation() -> GenerationHeader {
    GenerationHeader {
        number: GenerationNumber {
            owner: [9; 16],
            generation: 7,
        },
        for_fm_replica: false,
    }
}

fn targets(local: NodeInstance, remote: NodeInstance, role: Role) -> Targets {
    Targets::new(
        local,
        vec![Target::new(remote, role, None, generation()).unwrap()],
    )
    .unwrap()
}

async fn establish(left: &mut Targets, right: &mut Targets) -> (Link, Link) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = left.local.clone();
    let remote = right.local.clone();
    left.set_address(&remote, listener.local_addr().unwrap())
        .unwrap();
    let (left, right) = tokio::join!(left.connect(&remote, deadline()), async {
        let (stream, _) = listener.accept().await.unwrap();
        right.accept(&local, stream, deadline()).await
    });
    (left.unwrap(), right.unwrap())
}

async fn registration(node: &Link, fm: &Link) {
    let version = FabricVersionInstance::native_default();
    let body = NodeUp::fresh_stateless(
        "node".into(),
        "target-routing".into(),
        "127.0.0.1:12345".parse().unwrap(),
        version.clone(),
    )
    .unwrap();
    let ack = NodeUpAck::fresh_activated(version, 1).unwrap();
    let (reply, ()) = tokio::join!(node.node_up(&body), async {
        let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
        incoming
            .reply
            .take()
            .unwrap()
            .node_up(fm, |actual| {
                assert_eq!(actual, body);
                Ok((ack.clone(), generation()))
            })
            .await
            .unwrap();
    });
    assert_eq!(reply.unwrap(), (ack, generation()));
}

#[tokio::test]
async fn node_registers_independently_of_tcp_direction_and_both_sides_reuse_it() {
    for node_dials in [true, false] {
        let mut node_targets = targets(node(1, 10), node(9, 90), Role::Node);
        let mut fm_targets = targets(node(9, 90), node(1, 10), Role::Fm);
        let (node_link, fm_link) = if node_dials {
            establish(&mut node_targets, &mut fm_targets).await
        } else {
            let (fm, node) = establish(&mut fm_targets, &mut node_targets).await;
            (node, fm)
        };
        let again = node_targets
            .connect(&node(9, 90), deadline())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&again.session, &node_link.session));
        let again = fm_targets.connect(&node(1, 10), deadline()).await.unwrap();
        assert!(Arc::ptr_eq(&again.session, &fm_link.session));
        registration(&node_link, &fm_link).await;
        node_link.close().await.unwrap();
        fm_link.close().await.unwrap();
    }
}

#[tokio::test]
async fn node_up_retries_a_missing_ack_with_fresh_correlation_on_the_same_target() {
    let mut node_targets = targets(node(1, 10), node(9, 90), Role::Node);
    let mut fm_targets = targets(node(9, 90), node(1, 10), Role::Fm);
    let (node_link, fm_link) = establish(&mut node_targets, &mut fm_targets).await;
    let until = Instant::now() + Duration::from_secs(8);
    let body = NodeUp::default();
    let fm = node(9, 90);
    let (reply, ()) = tokio::join!(node_targets.node_up(&fm, &body, until), async {
        let first = fm_link.receive_checked(until).await.unwrap().unwrap();
        let mut retry = fm_link.receive_checked(until).await.unwrap().unwrap();
        assert_ne!(
            first.message.headers().effective().message_id,
            retry.message.headers().effective().message_id
        );
        retry
            .reply
            .take()
            .unwrap()
            .node_up(&fm_link, |actual| {
                assert_eq!(actual, body);
                Ok((NodeUpAck::default(), generation()))
            })
            .await
            .unwrap();
        drop(first);
    });
    assert_eq!(reply.unwrap(), (NodeUpAck::default(), generation()));
    assert!(Arc::ptr_eq(
        &node_targets.link(&node(9, 90)).unwrap().unwrap().session,
        &node_link.session
    ));
    node_link.close().await.unwrap();
    fm_link.close().await.unwrap();
}

#[tokio::test]
async fn fresh_bootstrap_requires_the_matching_final_inventory_on_either_tcp_direction() {
    for (node_dials, valid_inventory) in [(true, true), (false, true), (true, false)] {
        let mut node_targets = targets(node(1, 10), node(9, 90), Role::Node);
        let mut fm_targets = targets(node(9, 90), node(1, 10), Role::Fm);
        let (node_link, fm_link) = if node_dials {
            establish(&mut node_targets, &mut fm_targets).await
        } else {
            let (fm, node) = establish(&mut fm_targets, &mut node_targets).await;
            (node, fm)
        };
        let body = NodeUp::fresh_stateless(
            "node".into(),
            "target-bootstrap".into(),
            "127.0.0.1:12345".parse().unwrap(),
            FabricVersionInstance::native_default(),
        )
        .unwrap();
        let ack = NodeUpAck::fresh_activated(body.node.version.clone(), 1).unwrap();
        let fm = node(9, 90);
        let (result, ()) = tokio::join!(
            node_targets.bootstrap_fresh(&fm, &body, deadline()),
            async {
                let mut incoming = fm_link.receive_checked(deadline()).await.unwrap().unwrap();
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .node_up(&fm_link, |_| Ok((ack.clone(), generation())))
                    .await
                    .unwrap();
                let incoming = fm_link.receive_checked(deadline()).await.unwrap().unwrap();
                let FmReport::ReplicaUp(payload) = incoming.fm_report(&fm_link).unwrap() else {
                    panic!("expected final bootstrap inventory");
                };
                let (mut inventory, _) = payload.into_parts().unwrap();
                assert!(inventory.last && inventory.replicas.0.is_empty());
                inventory.last = valid_inventory;
                NodeClient::new(&fm_link)
                    .unwrap()
                    .send(NodeRequest::replica_up_reply(&inventory, &generation()).unwrap())
                    .await
                    .unwrap();
            }
        );
        if valid_inventory {
            assert_eq!(result.unwrap(), (ack, generation()));
            assert!(node_link.is_live());
        } else {
            assert!(result.is_err());
            assert!(!node_link.is_live());
        }
        node_link.close().await.unwrap();
        fm_link.close().await.unwrap();
    }
}

#[tokio::test]
async fn fresh_demand_reconnects_without_replaying_requests_or_redirecting_old_replies() {
    let mut node_targets = targets(node(1, 10), node(9, 90), Role::Node);
    let mut fm_targets = targets(node(9, 90), node(1, 10), Role::Fm);
    let (old_node, old_fm) = establish(&mut node_targets, &mut fm_targets).await;
    let body = NodeUp::default();
    let (failed, mut old_incoming) = tokio::join!(old_node.node_up(&body), async {
        let incoming = old_fm.receive_checked(deadline()).await.unwrap().unwrap();
        old_node.close().await.unwrap();
        incoming
    });
    assert!(failed.is_err());
    old_fm.close().await.unwrap();
    let (fm_link, node_link) = establish(&mut fm_targets, &mut node_targets).await;
    assert!(!Arc::ptr_eq(&old_node.session, &node_link.session));
    assert!(!old_node.is_live());
    assert!(old_node.node_up(&body).await.is_err());
    assert!(old_incoming
        .reply
        .take()
        .unwrap()
        .node_up(&fm_link, |_| panic!(
            "old invocation reached a replacement handler"
        ))
        .await
        .is_err());
    assert!(fm_link
        .receive(Instant::now() + Duration::from_millis(20))
        .await
        .unwrap()
        .is_none());
    registration(&node_link, &fm_link).await;
    node_link.close().await.unwrap();
    fm_link.close().await.unwrap();
}

#[tokio::test]
async fn peer_reconnect_keeps_the_advanced_send_generation() {
    let mut left = targets(node(1, 10), node(2, 20), Role::Peer);
    let mut right = targets(node(2, 20), node(1, 10), Role::Peer);
    let (old_left, old_right) = establish(&mut left, &mut right).await;
    let mut next = generation();
    next.number.generation += 1;
    old_left.advance_generation(&next);
    old_right.advance_generation(&next);
    old_left.close().await.unwrap();
    old_right.close().await.unwrap();
    let (new_right, new_left) = establish(&mut right, &mut left).await;
    assert_eq!(new_left.generation(), next);
    assert_eq!(new_right.generation(), next);
    new_left
        .send(get_lsn_reply_message(&GetLsnReply::default(), &next).unwrap())
        .await
        .unwrap();
    let incoming = new_right.receive(deadline()).await.unwrap().unwrap();
    assert_eq!(incoming.action().unwrap(), "GetLSNReply");
    new_left.close().await.unwrap();
    new_right.close().await.unwrap();
}

#[tokio::test]
async fn incarnation_replacement_requires_exact_predecessor_and_retires_old_capabilities() {
    let mut left = targets(node(1, 10), node(2, 20), Role::Peer);
    let mut right = targets(node(2, 20), node(1, 10), Role::Peer);
    let (old_left, old_right) = establish(&mut left, &mut right).await;
    for (previous, replacement, role, number) in [
        (node(2, 19), node(2, 21), Role::Peer, 7),
        (node(2, 20), node(2, 20), Role::Peer, 7),
        (node(2, 20), node(3, 21), Role::Peer, 7),
        (node(2, 20), node(2, 21), Role::Node, 7),
        (node(2, 20), node(2, 21), Role::Peer, 6),
    ] {
        let mut header = generation();
        header.number.generation = number;
        assert!(left
            .replace_after_fencing(
                &previous,
                Target::new(replacement, role, None, header).unwrap(),
                deadline(),
            )
            .await
            .is_err());
        assert!(old_left.is_live());
    }
    left.replace_after_fencing(
        &node(2, 20),
        Target::new(node(2, 21), Role::Peer, None, generation()).unwrap(),
        deadline(),
    )
    .await
    .unwrap();
    assert!(!old_left.is_live());
    assert!(left.link(&node(2, 20)).is_err());
    assert!(left.link(&node(2, 21)).unwrap().is_none());
    old_right.close().await.unwrap();
    let mut successor = targets(node(2, 21), node(1, 10), Role::Peer);
    let (new_left, new_right) = establish(&mut left, &mut successor).await;
    assert_eq!(new_left.remote, node(2, 21));
    assert_eq!(new_right.remote, node(1, 10));
    assert!(old_left
        .send(get_lsn_reply_message(&GetLsnReply::default(), &generation()).unwrap())
        .await
        .is_err());
    new_left.close().await.unwrap();
    new_right.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_connections_and_unconfigured_incarnations_preserve_the_live_target() {
    let mut left = targets(node(1, 10), node(2, 20), Role::Peer);
    let mut right = targets(node(2, 20), node(1, 10), Role::Peer);
    let (left_link, right_link) = establish(&mut left, &mut right).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    assert!(left.accept(&node(2, 20), stream, deadline()).await.is_err());
    assert!(left.connect(&node(2, 21), deadline()).await.is_err());
    assert!(left
        .set_address(&node(2, 20), listener.local_addr().unwrap())
        .is_err());
    let body = NodeUp::fresh_stateless(
        "node".into(),
        "invalid-bootstrap-role".into(),
        "127.0.0.1:12345".parse().unwrap(),
        FabricVersionInstance::native_default(),
    )
    .unwrap();
    assert!(left
        .bootstrap_fresh(&node(2, 20), &body, deadline())
        .await
        .is_err());
    assert!(Arc::ptr_eq(
        &left.link(&node(2, 20)).unwrap().unwrap().session,
        &left_link.session
    ));
    assert!(left_link.is_live());
    left_link.close().await.unwrap();
    right_link.close().await.unwrap();
}

#[test]
fn logical_target_configuration_rejects_identity_aliases_and_invalid_routes() {
    for remote in [node(1, 10), node(0, 20), node(2, 0)] {
        let result = Target::new(remote, Role::Peer, None, generation())
            .and_then(|route| Targets::new(node(1, 10), vec![route]));
        assert!(result.is_err());
    }
    assert!(Targets::new(
        node(1, 10),
        vec![
            Target::new(node(2, 20), Role::Peer, None, generation()).unwrap(),
            Target::new(node(2, 21), Role::Peer, None, generation()).unwrap(),
        ]
    )
    .is_err());
    for address in [
        "0.0.0.0:1234",
        "127.0.0.1:0",
        "224.0.0.1:1234",
        "255.255.255.255:1234",
    ] {
        assert!(Target::new(
            node(2, 20),
            Role::Peer,
            Some(address.parse().unwrap()),
            generation(),
        )
        .is_err());
    }
}
