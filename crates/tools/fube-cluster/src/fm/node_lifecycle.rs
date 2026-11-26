use super::*;
use crate::profile::{NodeDownReceipt, Peer};

pub(crate) struct NodeFailure {
    pub index: usize,
    pub receipt: NodeDownReceipt,
    pub body: Option<AddInstance>,
    pub desired: Desired,
    pub replacement: Option<Launch>,
    pub recovery_error: Option<String>,
}

impl Ledger {
    fn latest_completed_configuration(&self) -> Option<&ActiveConfiguration> {
        self.repair
            .iter()
            .filter_map(|repair| repair.configuration.as_ref())
            .chain(self.swap.iter())
            .chain(self.configuration.iter())
            .filter(|configuration| configuration.reply.as_ref().is_some_and(|r| r.error == 0))
            .max_by_key(|configuration| {
                let epoch = configuration.reply.as_ref().unwrap().unit.current;
                (epoch.data_loss, epoch.configuration)
            })
    }

    pub(crate) fn record_node_down(
        &mut self,
        index: usize,
        receipt: NodeDownReceipt,
    ) -> Result<Option<DoReconfiguration>> {
        if let Some(old) = self
            .node_failures
            .iter()
            .find(|f| f.receipt.node == receipt.node)
        {
            return if old.receipt == receipt && old.index == index {
                Ok(None)
            } else {
                Err("NodeDown receipt changed for an already fenced incarnation".into())
            };
        }
        if self.node_failures.len() == 64 {
            return Err("64 fenced Node incarnations retained; capacity exhausted".into());
        }
        let affects_membership = self.latest_completed_configuration().map(|configuration| {
            configuration
                .reply
                .as_ref()
                .unwrap()
                .replicas
                .0
                .iter()
                .any(|replica| replica.node == receipt.node)
        });
        let placement = self
            .placements
            .get_mut(index)
            .ok_or("unknown NodeDown placement")?;
        if placement.launch.node != receipt.node {
            return Err("NodeDown targets a stale or foreign incarnation".into());
        }
        let failure = NodeFailure {
            index,
            body: placement.created.then(|| placement.body()).transpose()?,
            desired: if placement.launch.profile.stateful() && affects_membership == Some(false) {
                Desired::Absent
            } else {
                placement.desired
            },
            receipt: receipt.clone(),
            replacement: None,
            recovery_error: None,
        };
        placement.node_down = Some(receipt);
        placement.link_alive = false;
        placement.loss_reported = true;
        placement.endpoint = None;
        placement.observed = Observed::Down;
        placement.pending = None;
        placement.error = None;
        let operations = [placement.create_op.clone(), placement.delete_op.clone()];
        self.node_failures.push(failure);
        for operation in operations.into_iter().flatten() {
            self.complete(
                &operation,
                crate::OperationOutcome::new(&operation, crate::OperationState::Unknown)
                    .with_error("owned Node incarnation is confirmed down"),
            );
        }
        if !self.placements[index].launch.profile.stateful()
            || !self.placements[index].created
            || affects_membership == Some(false)
        {
            return Ok(None);
        }
        if let Some(configuration) = &mut self.configuration {
            configuration.unknown = true;
        }
        if let Some(swap) = &mut self.swap {
            swap.unknown = true;
        }
        if let Some(repair) = &mut self.repair {
            repair.unknown = true;
        }
        Ok(None)
    }

    pub(crate) fn reconfigure_node_down(
        &mut self,
        index: usize,
    ) -> Result<Option<DoReconfiguration>> {
        let failure = self
            .node_failures
            .iter()
            .rposition(|failure| failure.index == index)
            .ok_or("reconfiguration requires confirmed NodeDown")?;
        let lost = &self.node_failures[failure].receipt.node;
        if self.swap.as_ref().is_some_and(|configuration| {
            self.node_reconfiguration == Some(configuration.body.configuration.unit.current)
                && configuration
                    .body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .any(|r| r.node == *lost && !r.up)
        }) {
            return Ok(None);
        }
        match self.node_down_configuration(index) {
            Ok(body) => {
                self.install_primary_failover(body.clone(), Instant::now())?;
                self.node_reconfiguration = Some(body.configuration.unit.current);
                self.node_failures[failure].recovery_error = None;
                Ok(Some(body))
            }
            Err(error) => {
                eprintln!("NodeDown recorded; replica recovery blocked: {error}");
                self.node_failures[failure].recovery_error = Some(error.clone());
                Err(error)
            }
        }
    }

    fn node_down_configuration(&self, index: usize) -> Result<DoReconfiguration> {
        if !self.placements[index].launch.profile.mesh_peers()
            || self.generation_pending()
            || self
                .repair
                .as_ref()
                .is_some_and(|r| r.configuration.as_ref().is_none_or(|c| c.reply.is_none()))
            || self.swap.as_ref().is_some_and(|c| c.reply.is_none())
        {
            return Err(
                "node failure requires stable replicated membership without another transition"
                    .into(),
            );
        }
        let active = self
            .latest_completed_configuration()
            .ok_or("missing completed membership")?;
        let previous = active.reply.as_ref().unwrap();
        let lost = &self
            .node_failures
            .iter()
            .rev()
            .find(|f| f.index == index)
            .ok_or("missing NodeDown lineage")?
            .receipt
            .node;
        if !previous.replicas.0.iter().any(|r| &r.node == lost) {
            return Err("failed Node is not in the latest completed membership".into());
        }
        let available = |r: &&Replica| {
            self.placements.iter().any(|p| {
                p.launch.node == r.node
                    && p.link_alive
                    && !p.loss_reported
                    && p.observed == Observed::Running
                    && p.bootstrap == Bootstrap::Admitted
            })
        };
        let survivors: Vec<_> = previous.replicas.0.iter().filter(available).collect();
        if survivors.len() < previous.replicas.0.len() / 2 + 1 {
            return Err("insufficient surviving quorum/minimum for volatile Node recovery".into());
        }
        if survivors.len() + 1 != previous.replicas.0.len() {
            return Err(
                "Node recovery requires one confirmed failure and every other member live".into(),
            );
        }
        let primary = survivors
            .iter()
            .find(|r| r.current_role == 4)
            .or_else(|| survivors.iter().find(|r| r.current_role == 3))
            .ok_or("no surviving Primary candidate")?;
        let primary_changed = previous
            .replicas
            .0
            .iter()
            .any(|r| &r.node == lost && r.current_role == 4);
        let mut body = active.body.clone();
        body.configuration.unit = previous.unit.clone();
        body.configuration.unit.previous = previous.unit.current;
        body.configuration.unit.current.configuration = previous
            .unit
            .current
            .configuration
            .checked_add(if primary_changed { 0x1_0000_0001 } else { 1 })
            .ok_or("NodeDown configuration epoch exhausted")?;
        body.configuration.replicas = previous.replicas.clone();
        for replica in &mut body.configuration.replicas.0 {
            replica.previous_role = replica.current_role;
            if &replica.node == lost {
                replica.current_role = if survivors.len() >= previous.unit.minimum as usize {
                    1
                } else {
                    3
                };
                replica.up = false;
                replica.state = 3;
            } else if replica.node == primary.node {
                replica.current_role = 4;
            }
            replica.first_lsn = -1;
            replica.last_lsn = -1;
        }
        body.phase0_duration = i64::MAX;
        Ok(body)
    }

    pub(crate) fn prepare_node_restart(&mut self, index: usize) -> Result<Launch> {
        let placement = self
            .placements
            .get(index)
            .ok_or("unknown restart placement")?;
        let proof = placement
            .node_down
            .as_ref()
            .ok_or("restart requires confirmed NodeDown")?;
        let failure = self
            .node_failures
            .iter()
            .find(|failure| failure.receipt == *proof)
            .ok_or("missing original Node fencing owner")?;
        if self.generation_pending()
            || self
                .configuration
                .as_ref()
                .is_some_and(|configuration| configuration.reply.is_none())
            || self
                .swap
                .as_ref()
                .is_some_and(|configuration| configuration.reply.is_none())
            || self.repair.as_ref().is_some_and(|repair| {
                repair
                    .configuration
                    .as_ref()
                    .is_none_or(|configuration| configuration.reply.is_none())
            })
        {
            return Err("Node restart waits for completed membership and inventory work".into());
        }
        if placement.launch.profile.mesh_peers()
            && failure.body.is_some()
            && self
                .latest_completed_configuration()
                .is_none_or(|configuration| {
                    configuration
                        .reply
                        .as_ref()
                        .unwrap()
                        .replicas
                        .0
                        .iter()
                        .any(|replica| replica.node == proof.node && replica.up)
                })
        {
            return Err(
                "manual infrastructure restart waits for completed survivor failure metadata"
                    .into(),
            );
        }
        let next = self
            .placements
            .iter()
            .flat_map(|p| {
                std::iter::once((p.replica, p.incarnation)).chain(p.retired.iter().copied())
            })
            .fold(0, |n, (id, instance)| n.max(id).max(instance))
            .checked_add(1)
            .ok_or("restart replica identity exhausted")?;
        let mut launch = placement.launch.clone();
        launch.node.instance = launch
            .node
            .instance
            .checked_add(1)
            .ok_or("Node incarnation exhausted")?;
        launch.session = uuid::Uuid::new_v4().to_string();
        launch.capability = uuid::Uuid::new_v4().to_string();
        launch.directory = launch
            .directory
            .parent()
            .ok_or("missing deployment directory")?
            .join(format!(
                "node{}-instance-{}",
                launch.placement, launch.node.instance
            ));
        launch.replica = next;
        launch.incarnation = next;
        for peer in &mut launch.peers {
            let current = self
                .placements
                .iter()
                .find(|p| p.launch.node.id == peer.node.id)
                .ok_or("missing configured peer")?;
            *peer = Peer {
                node: current.launch.node.clone(),
                session: current.launch.session.clone(),
                partition: current.launch.partition.clone(),
                replica: current.replica,
                incarnation: current.incarnation,
            };
        }
        launch.validate()?;
        Ok(launch)
    }

    pub(crate) fn install_node_restart(&mut self, index: usize, launch: Launch) -> Result<()> {
        let previous = self.placements[index].launch.node.clone();
        let failure = self
            .node_failures
            .iter_mut()
            .find(|f| f.receipt.node == previous)
            .ok_or("missing NodeDown owner")?;
        failure.replacement = Some(launch.clone());
        let mut placement = Placement::new(launch);
        placement.link_alive = false;
        placement.retired = self.placements[index].retired.clone();
        placement.retired.push((
            self.placements[index].replica,
            self.placements[index].incarnation,
        ));
        self.placements[index] = placement;
        Ok(())
    }

    pub(crate) fn restart_desired(&self, index: usize) -> Result<Desired> {
        let node = &self.placements[index].launch.node;
        self.node_failures
            .iter()
            .find(|f| f.replacement.as_ref().is_some_and(|l| &l.node == node))
            .map(|f| f.desired)
            .ok_or("Node is not an admitted restart")
            .map_err(str::to_owned)
    }

    pub(crate) fn begin_node_replica_rebuild(
        &mut self,
        index: usize,
        destination: usize,
    ) -> Result<bool> {
        let target = self
            .placements
            .get(destination)
            .ok_or("unknown rebuild destination")?;
        if self
            .repair
            .as_ref()
            .is_some_and(|r| r.build.body.replica.node == target.launch.node)
        {
            return Ok(false);
        }
        let failure = self
            .node_failures
            .iter()
            .rev()
            .find(|f| f.index == index)
            .ok_or("missing rejoined Node lineage")?;
        let predecessor = failure
            .body
            .clone()
            .ok_or("restarted Node had no replica")?;
        let active = self
            .latest_completed_configuration()
            .cloned()
            .ok_or("missing completed failure reconfiguration")?;
        let previous = active.reply.as_ref().unwrap();
        if self.generation_pending()
            || self
                .repair
                .as_ref()
                .is_some_and(|r| r.configuration.as_ref().is_none_or(|c| c.reply.is_none()))
            || target.bootstrap != Bootstrap::Admitted
            || !target.link_alive
            || !target.activated
            || !target.type_enabled
            || target.node_control.is_some()
            || target.pending.is_some()
            || target.service_update.is_some()
            || active.unknown
            || previous
                .replicas
                .0
                .iter()
                .any(|r| r.node.id == target.launch.node.id && r.up)
            || target.created
            || (destination == index
                && failure
                    .replacement
                    .as_ref()
                    .is_none_or(|l| l.node != target.launch.node))
            || previous
                .replicas
                .0
                .iter()
                .filter(|r| r.up && r.current_role >= 3)
                .any(|r| {
                    self.placements
                        .iter()
                        .find(|p| p.launch.node == r.node)
                        .is_none_or(|p| {
                            !p.link_alive || p.loss_reported || p.observed != Observed::Running
                        })
                })
            || previous
                .replicas
                .0
                .iter()
                .filter(|r| r.up && r.current_role >= 3)
                .count()
                < previous.replicas.0.len() / 2 + 1
        {
            return Err("rebuild requires admitted fresh inventory, completed failure reconfiguration and a live majority".into());
        }
        let mut build = target.body()?;
        build.unit = previous.unit.clone();
        build.service = active.body.configuration.service.clone();
        build.replica.previous_role = 1;
        build.replica.current_role = 2;
        build.replica.state = 1;
        let repair = ReplicaRepair {
            predecessor,
            old_profile: failure
                .receipt
                .profile
                .clone()
                .ok_or("missing predecessor host identity")?,
            profile: target.profile.clone(),
            build: IdleBuild {
                body: build.clone(),
                reply: None,
                cancelled: false,
                unknown: false,
                started: Instant::now(),
                last: Instant::now(),
            },
            configuration: None,
            removal: None,
            cancellation: None,
            deletion: None,
            base: Some(active),
            host_requested: false,
            superseded_by: None,
            unknown: false,
        };
        let placement = &mut self.placements[destination];
        placement.active_unit = Some(build.unit);
        placement.service_description = Some(build.service);
        placement.ready_replica = Some(build.replica);
        placement.created = true;
        placement.desired = Desired::Running;
        placement.observed = Observed::Pending;
        if let Some(old) = self.repair.replace(repair) {
            self.repair_history.push(old);
        }
        Ok(true)
    }
}
