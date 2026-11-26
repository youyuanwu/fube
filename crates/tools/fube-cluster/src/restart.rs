use crate::{
    fm::Desired,
    management::Operation,
    manager::{Connection, Manager},
    process_control::{self, NodeControl},
    profile::*,
    wire::{self, Target},
    Cluster, Command,
};
use serde_json::{json, Value};
use std::{fs, time::Duration};
use tokio::time::Instant;

/// Read-only observation. Numeric PIDs are never reconstructed into signal authority.
fn group_stopped(group: u32) -> Result<bool> {
    for entry in fs::read_dir("/proc").map_err(err)? {
        let entry = entry.map_err(err)?;
        if entry.file_name().to_string_lossy().parse::<u32>().is_err() {
            continue;
        }
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                continue
            }
            Err(error) => return Err(err(error)),
        };
        let (_, fields) = stat.rsplit_once(')').ok_or("invalid process observation")?;
        let mut fields = fields.split_whitespace();
        let state = fields.next().ok_or("missing process state")?;
        fields.next().ok_or("missing process parent")?;
        let pgid: i64 = fields
            .next()
            .ok_or("missing process group")?
            .parse()
            .map_err(err)?;
        if pgid == i64::from(group) && !matches!(state, "Z" | "X") {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn verify_fencing(receipt: &NodeDownReceipt) -> Result<()> {
    if !fencing_observed(receipt)? {
        return Err("owned Node or application process group is not fenced".into());
    }
    Ok(())
}

fn fencing_observed(receipt: &NodeDownReceipt) -> Result<bool> {
    Ok(receipt.node_group_drained
        && receipt.app_group_drained
        && group_stopped(receipt.node_pid)?
        && receipt
            .profile
            .as_ref()
            .map(|profile| group_stopped(profile.app_pid))
            .transpose()?
            .is_none_or(|stopped| stopped))
}

impl Manager {
    pub(crate) async fn node_down(
        &mut self,
        index: usize,
        receipt: NodeDownReceipt,
    ) -> Result<Value> {
        if let Some(failure) = self
            .ledger
            .node_failures
            .iter()
            .find(|f| f.receipt.node == receipt.node)
        {
            if failure.receipt != receipt || failure.index != index {
                return Err("NodeDown receipt changed for a fenced incarnation".into());
            }
            return Ok(json!({"state":"down","node":index + 1,"duplicate":true,
                "replica_reconfiguration":if failure.recovery_error.is_some() {"blocked"} else if failure.desired == Desired::Absent || failure.body.as_ref().is_none_or(|body| !body.service.stateful) {"not_required"} else {"recorded"},
                "error":failure.recovery_error}));
        }
        if self.finished.is_some() {
            return Err("NodeDown requires the live original FM".into());
        }
        let child = self
            .children
            .get(index)
            .ok_or("unknown NodeDown placement")?;
        receipt.validate(
            &child.launch,
            child.node_pid,
            child
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.profile.as_ref()),
        )?;
        verify_fencing(&receipt)?;
        let body = self.ledger.record_node_down(index, receipt)?;
        if let Some(link) = self.children[index].link.take() {
            self.children[index].launch.generation = link.generation();
            link.close().await?;
        }
        if let Some(body) = &body {
            self.send_configuration(body).await?;
        }
        let failure = self.ledger.node_failures.last().unwrap();
        Ok(json!({"state":"down","node":index + 1,
            "replica_reconfiguration":if failure.recovery_error.is_some() {"blocked"} else if body.is_some() {"pending"} else if self.ledger.recovery_mode == crate::RecoveryMode::Manual && failure.body.as_ref().is_some_and(|body| body.service.stateful) && failure.desired == Desired::Running {"not_requested"} else {"not_required"},
            "error":failure.recovery_error}))
    }

    pub(crate) async fn prepare_node_restart(&mut self, index: usize) -> Result<Launch> {
        let mut launch = self.ledger.prepare_node_restart(index)?;
        launch.generation = self.children[index].generation();
        let previous = self.children[index].launch.node.clone();
        self.targets
            .replace_after_fencing(
                &previous,
                Target::new(
                    launch.node.clone(),
                    wire::Role::Fm,
                    None,
                    launch.generation.clone(),
                )?,
                wire::deadline(),
            )
            .await?;
        self.ledger.install_node_restart(index, launch.clone())?;
        self.children[index] = Connection {
            launch: launch.clone(),
            node_pid: 0,
            link: None,
            metadata: None,
        };
        Ok(launch)
    }

    pub(crate) async fn finish_node_restart(&mut self, index: usize) -> Result<Value> {
        if self.finished.is_some()
            || self.children.get(index).is_none()
            || self.ledger.placements.get(index).is_none_or(|placement| {
                !placement.link_alive || placement.bootstrap != crate::fm::Bootstrap::Admitted
            })
        {
            return Err("restart completion requires an admitted current Node incarnation".into());
        }
        let restore_replica = self.ledger.restart_desired(index)? == Desired::Running;
        if restore_replica {
            let launch = self.children[index].launch.clone();
            let failure = self
                .ledger
                .node_failures
                .iter()
                .find(|failure| {
                    failure
                        .replacement
                        .as_ref()
                        .is_some_and(|replacement| replacement.node == launch.node)
                })
                .ok_or("missing restarted replica predecessor")?;
            let body = failure
                .body
                .as_ref()
                .ok_or("restarted Node had no replica to restore")?;
            self.execute(Command {
                op: format!("restart-rebuild-{}", launch.session),
                action: "rebuild-replica".into(),
                node: Some((index + 1) as u8),
                partition: Some(launch.partition),
                predecessor: Some(crate::ReplicaRef {
                    node: (index + 1) as u8,
                    node_instance: failure.receipt.node.instance,
                    replica: body.replica.id,
                    incarnation: body.replica.instance,
                }),
                ..Command::default()
            })
            .await?;
        }
        Ok(
            json!({"state":if restore_replica {"pending"} else {"complete"},"kind":"node_restart","node":index + 1,
            "node_instance":self.children[index].launch.node.instance,
            "replica_recovery":if restore_replica {"pending"} else {"not_required"}}),
        )
    }
}

impl Cluster {
    pub(crate) fn refresh_node_controls(&mut self) {
        if !self.driver_commands.iter().any(|(command, result)| {
            command.action == "restart-node"
                && matches!(result, Some(Ok(outcome)) if outcome["state"] == "pending")
        }) {
            return;
        }
        let status = self
            .remote
            .as_ref()
            .map_or_else(|| self.manager.status(), crate::management::Client::status);
        for (command, result) in &mut self.driver_commands {
            let Some(Ok(outcome)) = result else {
                continue;
            };
            if command.action != "restart-node" || outcome["state"] != "pending" {
                continue;
            }
            let node = &status["nodes"][usize::from(command.node.unwrap() - 1)];
            if node["node_instance"] != outcome["node_instance"]
                || matches!(node["observed"].as_str(), Some("down" | "unknown"))
            {
                outcome["state"] = json!("unknown");
                outcome["error"] = json!("restarted Node lost authority before recovery completed");
            } else if node["admitted"] == true
                && (node["desired"] == "absent"
                    || (node["observed"] == "running"
                        && (!self.config.profile.stateful() || status["repair"]["ready"] == true)))
            {
                outcome["state"] = json!("complete");
                outcome["replica_recovery"] = json!("ready");
            }
        }
    }

    fn node_index(&self, node: u8) -> Result<usize> {
        let index = usize::from(node.checked_sub(1).ok_or("Node numbers start at one")?);
        if self.manager.finished.is_some() || index >= self.manager.children.len() {
            return Err("Node control requires an existing live cluster placement".into());
        }
        Ok(index)
    }

    /// Report a terminated, independently fenced original owner. Never kills a live Node.
    pub async fn node_down(&mut self, node: u8) -> Result<Value> {
        let index = self.node_index(node)?;
        let launch = self.manager.children[index].launch.clone();
        let metadata: Metadata = if let Some(remote) = &mut self.remote {
            serde_json::from_value(remote.request(Operation::NodeOwner { index }).await?)
                .map_err(err)?
        } else {
            self.manager.children[index]
                .metadata
                .clone()
                .ok_or("Node metadata is not admitted")?
        };
        if metadata.node != launch.node
            || metadata.session != launch.session
            || metadata.deployment != launch.deployment
        {
            return Err("Node owner metadata does not match the retained driver capability".into());
        }
        let process = self.processes[index]
            .as_mut()
            .ok_or("missing original Node process owner")?;
        if metadata.pid != process.pid() {
            return Err("Node metadata does not identify the original owned child".into());
        }
        if process.observed_exit().map_err(err)?.is_none() {
            return Err("Node is still alive; NodeDown does not initiate a kill".into());
        }
        process.cleanup().map_err(err)?;
        let status = process.status();
        let receipt = NodeDownReceipt {
            deployment: launch.deployment.clone(),
            session: launch.session.clone(),
            node: launch.node.clone(),
            node_pid: process.pid(),
            profile: metadata.profile.clone(),
            node_group_drained: status.reaped && status.group_drained && status.exit_observed,
            app_group_drained: true,
        };
        let until = Instant::now() + Duration::from_secs(3);
        while !fencing_observed(&receipt)? {
            if Instant::now() >= until {
                return Err("owned app process group has not retired after Node exit".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        receipt.validate(&launch, process.pid(), receipt.profile.as_ref())?;
        self.manager.children[index].metadata = Some(metadata);
        if !self.fenced_nodes.iter().any(|old| old.node == receipt.node) {
            if self.fenced_nodes.len() == 64 {
                return Err("64 fenced Node incarnations retained; capacity exhausted".into());
            }
            self.fenced_nodes.push(receipt.clone());
        }
        let acknowledgement = if let Some(remote) = &mut self.remote {
            remote
                .request(Operation::NodeDown {
                    index,
                    receipt: receipt.clone(),
                })
                .await?
        } else {
            self.manager.node_down(index, receipt.clone()).await?
        };
        self.fence_peer_routes(index, &receipt).await?;
        Ok(acknowledgement)
    }

    async fn fence_peer_routes(&mut self, index: usize, proof: &NodeDownReceipt) -> Result<()> {
        for survivor in 0..self.manager.children.len() {
            if survivor == index {
                continue;
            }
            let local = self.manager.children[survivor].launch.clone();
            let Some(slot) = local.peers.iter().position(|peer| peer.node == proof.node) else {
                continue;
            };
            if self
                .fenced_nodes
                .iter()
                .any(|proof| proof.node == local.node)
            {
                continue;
            }
            if self.processes[survivor]
                .as_mut()
                .ok_or("missing surviving Node owner")?
                .observed_exit()
                .map_err(err)?
                .is_some()
            {
                continue;
            }
            let name = format!("peer-down-{slot}-{}", proof.node.instance);
            let ready = local.directory.join(format!("{name}-ready.json"));
            if !ready.try_exists().map_err(err)? {
                write_json(&local.directory.join(format!("{name}.json")), proof)?;
                process_control::send(
                    self.processes[survivor]
                        .as_mut()
                        .ok_or("missing surviving Node owner")?,
                    &local,
                    NodeControl::PeerDown {
                        slot,
                        instance: proof.node.instance,
                    },
                )?;
                let until = Instant::now() + Duration::from_secs(10);
                while !ready.try_exists().map_err(err)? {
                    self.drive().await;
                    if Instant::now() >= until {
                        return Err("surviving Node did not fence the failed peer route".into());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
            let receipt: PeerDownReceipt = read_json(&ready)?;
            if receipt.deployment != local.deployment
                || receipt.session != local.session
                || receipt.node_pid != self.manager.children[survivor].node_pid
                || receipt.peer != proof.node
            {
                return Err("peer fencing receipt has a foreign owner".into());
            }
        }
        Ok(())
    }

    /// Explicitly retire an old owner and rejoin with fresh Node/runtime/replica identities.
    pub async fn restart_node(&mut self, node: u8) -> Result<Value> {
        self.restart_node_owned(node, true).await
    }

    pub(crate) async fn restart_node_owned(&mut self, node: u8, restore: bool) -> Result<Value> {
        let index = self.node_index(node)?;
        if !restore
            && !self
                .fenced_nodes
                .iter()
                .any(|proof| proof.node == self.manager.children[index].launch.node)
        {
            return Err("infrastructure restart requires previously confirmed NodeDown".into());
        }
        if let Some(remote) = &mut self.remote {
            remote.request(Operation::Status).await?;
        } else {
            self.manager.drive().await;
        }
        let status = self.status();
        if self.config.profile.stateful()
            && status["nodes"]
                .as_array()
                .is_some_and(|nodes| nodes.iter().any(|node| node["created_lifetime"] == true))
            && (status["configuration"]["ready"] != true
                || (status["repair"]["state"] != "absent" && status["repair"]["ready"] != true)
                || status["generation_handshake"]
                    .as_object()
                    .is_some_and(|generation| {
                        generation.get("complete") != Some(&Value::Bool(true))
                    }))
            && self.processes[index]
                .as_mut()
                .ok_or("missing original Node owner")?
                .observed_exit()
                .map_err(err)?
                .is_none()
        {
            return Err(
                "live Node restart requires completed membership, repair and inventory work".into(),
            );
        }
        let process = self.processes[index]
            .as_mut()
            .ok_or("missing original Node owner")?;
        if process.observed_exit().map_err(err)?.is_none() {
            process_control::send(
                process,
                &self.manager.children[index].launch,
                NodeControl::Stop {},
            )?;
            if let Err(error) =
                process.wait_exit(std::time::Instant::now() + Duration::from_secs(20))
            {
                eprintln!("explicit Node restart is fencing the original owner after stop failed: {error}");
                process.cleanup().map_err(err)?;
            }
        }
        let mut down = self.node_down(node).await?;
        if restore
            && self.manager.ledger.recovery_mode == crate::RecoveryMode::Manual
            && matches!(
                down["replica_reconfiguration"].as_str(),
                Some("not_requested" | "recorded")
            )
        {
            let launch = self.manager.children[index].launch.clone();
            let predecessor = crate::ReplicaRef {
                node,
                node_instance: launch.node.instance,
                replica: self.manager.ledger.placements[index].replica,
                incarnation: self.manager.ledger.placements[index].incarnation,
            };
            let result = self
                .reconfigure_partition(
                    &format!("restart-reconfigure-{}", launch.session),
                    &launch.partition,
                    predecessor,
                )
                .await?;
            down["replica_reconfiguration"] = result["state"].clone();
        }
        if restore && down["replica_reconfiguration"] == "blocked" {
            return Err(format!(
                "NodeDown recorded, restart recovery blocked: {}",
                down["error"]
            ));
        }
        if restore && self.config.profile.stateful() && down["replica_reconfiguration"] == "pending"
        {
            let until = Instant::now() + Duration::from_secs(30);
            while self.status()["configuration"]["ready"] != true {
                self.drive().await;
                if Instant::now() >= until {
                    return Err("restart is waiting for survivor reconfiguration completion".into());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        let predecessor = self.manager.children[index].launch.clone();
        let fencing = self
            .fenced_nodes
            .iter()
            .find(|proof| proof.node == predecessor.node)
            .cloned()
            .ok_or("missing predecessor fencing receipt")?;
        let launch: Launch = if let Some(remote) = &mut self.remote {
            serde_json::from_value(
                remote
                    .request(Operation::PrepareNodeRestart { index })
                    .await?,
            )
            .map_err(err)?
        } else {
            self.manager.prepare_node_restart(index).await?
        };
        if launch.node.id != predecessor.node.id
            || launch.node.instance <= predecessor.node.instance
            || launch.deployment != predecessor.deployment
            || launch.fm != predecessor.fm
        {
            return Err("FM returned an invalid restart incarnation".into());
        }
        self.processes[index].take();
        if self.remote.is_some() {
            // The remote FM owns the reliability ledger; this is only its driver mirror.
            self.manager.ledger.placements[index] = crate::fm::Placement::new(launch.clone());
            self.manager.children[index] = Connection {
                launch: launch.clone(),
                node_pid: 0,
                link: None,
                metadata: None,
            };
            private_dir(&launch.directory)?;
            private_dir(&launch.directory.join("scratch"))?;
            write_json(&launch.directory.join("launch.json"), &launch)?;
        }
        self.spawn(index).await?;
        self.rebind_node_peers(index, &predecessor, &fencing)
            .await?;
        if !restore {
            self.finish_node_infrastructure(index).await
        } else if let Some(remote) = &mut self.remote {
            remote.request(Operation::FinishNodeRestart { index }).await
        } else {
            self.manager.finish_node_restart(index).await
        }
    }

    async fn rebind_node_peers(
        &mut self,
        index: usize,
        predecessor: &Launch,
        fencing: &NodeDownReceipt,
    ) -> Result<()> {
        let replacement = self.manager.children[index].launch.clone();
        for survivor in 0..self.manager.children.len() {
            if survivor == index {
                continue;
            }
            let local = self.manager.children[survivor].launch.clone();
            let Some(slot) = local
                .peers
                .iter()
                .position(|peer| peer.node.id == predecessor.node.id)
            else {
                continue;
            };
            let peer = Peer {
                node: replacement.node.clone(),
                session: replacement.session.clone(),
                partition: replacement.partition.clone(),
                replica: replacement.replica,
                incarnation: replacement.incarnation,
            };
            let update = PeerUpdate {
                deployment: local.deployment.clone(),
                session: local.session.clone(),
                previous: local.peers[slot].clone(),
                replacement: peer.clone(),
                fencing: fencing.clone(),
            };
            let name = format!("peer-update-{slot}-{}", replacement.node.instance);
            write_json(&local.directory.join(format!("{name}.json")), &update)?;
            process_control::send(
                self.processes[survivor]
                    .as_mut()
                    .ok_or("missing surviving Node owner")?,
                &local,
                NodeControl::PeerUpdate {
                    slot,
                    instance: replacement.node.instance,
                },
            )?;
            let until = Instant::now() + Duration::from_secs(10);
            let path = local.directory.join(format!("{name}-ready.json"));
            while !path.try_exists().map_err(err)? {
                self.drive().await;
                if Instant::now() >= until {
                    return Err("surviving Node did not admit the peer incarnation update".into());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let receipt: PeerUpdateReceipt = read_json(&path)?;
            if receipt.deployment != local.deployment
                || receipt.session != local.session
                || receipt.node_pid != self.manager.children[survivor].node_pid
                || receipt.previous != predecessor.node
                || receipt.replacement != replacement.node
                || !receipt.address.ip().is_loopback()
                || receipt.address.port() == 0
            {
                return Err(
                    "peer replacement receipt is not from the original surviving owner".into(),
                );
            }
            self.manager.children[survivor].launch.peers[slot] = peer;
            let outgoing = replacement
                .peers
                .iter()
                .position(|peer| peer.node == local.node)
                .ok_or("restarted Node is missing the current survivor binding")?;
            process_control::send(
                self.processes[index]
                    .as_mut()
                    .ok_or("missing restarted Node owner")?,
                &replacement,
                NodeControl::DialPeer {
                    slot: outgoing,
                    address: receipt.address,
                },
            )?;
        }
        Ok(())
    }
}
