use super::*;
use crate::restart::verify_fencing;

pub(super) async fn fence(
    launch: &Launch,
    slot: usize,
    instance: u64,
    node: &mut Node,
    adapter: &mut Adapter,
    connections: &mut [PeerConnection],
) -> Result<()> {
    let peer = &launch.peers[slot];
    let name = format!("peer-down-{slot}-{instance}");
    let proof: NodeDownReceipt = read_json(&launch.directory.join(format!("{name}.json")))?;
    if proof.node != peer.node
        || proof.node.instance != instance
        || proof.session != peer.session
        || proof.deployment != launch.deployment
    {
        return Err("peer-down proof does not match the original configured incarnation".into());
    }
    verify_fencing(&proof)?;
    adapter.capture(node);
    adapter.retire_peer(&peer.node)?;
    connections[slot].incoming = None;
    if let Some(link) = connections[slot].link.take() {
        link.close().await?;
    }
    write_json(
        &launch.directory.join(format!("{name}-ready.json")),
        &PeerDownReceipt {
            deployment: launch.deployment.clone(),
            session: launch.session.clone(),
            node_pid: std::process::id(),
            peer: peer.node.clone(),
        },
    )
}

pub(super) async fn replace(
    launch: &mut Launch,
    update_key: (usize, u64),
    node: &mut Node,
    adapter: &mut Adapter,
    targets: &mut Targets,
    connections: &mut [PeerConnection],
    listeners: &mut [Option<TcpListener>],
) -> Result<bool> {
    let (slot, instance) = update_key;
    let name = format!("peer-update-{slot}-{instance}");
    let update: PeerUpdate = read_json(&launch.directory.join(format!("{name}.json")))?;
    if update.deployment != launch.deployment
        || update.session != launch.session
        || update.previous != launch.peers[slot]
        || update.replacement.node.id != update.previous.node.id
        || update.replacement.node.instance != instance
        || instance <= update.previous.node.instance
        || update.replacement.partition != launch.partition
        || update.replacement.replica <= update.previous.replica
        || update.replacement.incarnation <= update.previous.incarnation
        || update.fencing.node != update.previous.node
        || update.fencing.session != update.previous.session
        || update.fencing.deployment != launch.deployment
    {
        return Err("peer incarnation update does not match the original fenced topology".into());
    }
    verify_fencing(&update.fencing)?;
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
    let address = listener.local_addr().map_err(err)?;
    adapter.capture(node);
    match node.replace_peer_after_fencing(&update.previous.node, update.replacement.node.clone()) {
        Ok(()) => {}
        Err(fube_node::local::Error::Ra(ra::Failure::Capacity)) => return Ok(false),
        Err(error) => return Err(err(error)),
    }
    targets
        .replace_after_fencing(
            &update.previous.node,
            Target::new(
                update.replacement.node.clone(),
                wire::Role::Peer,
                None,
                GenerationHeader {
                    number: node.generation_state().send,
                    for_fm_replica: false,
                },
            )?,
            wire::deadline(),
        )
        .await?;
    adapter.retire_peer(&update.previous.node)?;
    connections[slot] = PeerConnection {
        link: None,
        incoming: None,
        dialed: false,
    };
    listeners[slot] = Some(listener);
    launch.peers[slot] = update.replacement.clone();
    write_json(
        &launch.directory.join(format!("{name}-ready.json")),
        &PeerUpdateReceipt {
            deployment: launch.deployment.clone(),
            session: launch.session.clone(),
            node_pid: std::process::id(),
            previous: update.previous.node,
            replacement: update.replacement.node,
            address,
        },
    )?;
    Ok(true)
}
