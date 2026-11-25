use crate::{
    fm::{self, Accepted, Bootstrap, Command, Ledger},
    profile::*,
    wire,
};
use fube_ra::protocol::*;
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::time::Instant;
use wire::{FmReport, Link, NodeClient, NodeRequest, Target, Targets};

pub(crate) struct Connection {
    pub(crate) launch: Launch,
    pub(crate) node_pid: u32,
    pub(crate) link: Option<Link>,
    pub(crate) metadata: Option<Metadata>,
}
impl Connection {
    pub(crate) fn generation(&self) -> GenerationHeader {
        self.link
            .as_ref()
            .map(Link::generation)
            .unwrap_or_else(|| self.launch.generation.clone())
    }
}

/// Ephemeral FM authority over original native Node sessions; never spawns a Node.
pub struct Manager {
    pub(crate) profile: ServiceProfile,
    pub(crate) deployment: String,
    pub(crate) ledger: Ledger,
    pub(crate) children: Vec<Connection>,
    pub(crate) peer_idle_requested: bool,
    pub(crate) finished: Option<Value>,
    targets: Targets,
}
impl Manager {
    // Fence prior ordered ingress without claiming native completion or cancelling a consuming pump.
    pub(crate) async fn quiesce(&mut self) -> Result<()> {
        self.finished = Some(json!({"state":"closing"}));
        for index in 0..self.children.len() {
            if !self.ledger.placements[index].link_alive {
                continue;
            }
            let Some(link) = self.children[index].link.clone() else {
                continue;
            };
            let generation = link.generation();
            let request = link.generation_proposal(&generation);
            tokio::pin!(request);
            let reply = loop {
                tokio::select! {
                    biased;
                    reply = &mut request => break reply?,
                    _ = std::future::ready(()) => {}
                }
                for peer in 0..self.children.len() {
                    if self.ledger.placements[peer].link_alive {
                        self.pump(peer).await?;
                    }
                }
            };
            if reply.proposed != generation.number || reply.error != 0 {
                return Err(
                    "FM shutdown ingress fence was not processed by the original Node".into(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn host_cleanup_owners(&self) -> Vec<HostCleanupOwner> {
        self.ledger
            .placements
            .iter()
            .enumerate()
            .flat_map(|(node, placement)| {
                self.ledger
                    .repair_history
                    .iter()
                    .chain(self.ledger.repair.iter())
                    .filter(move |repair| {
                        !repair.uses_spare()
                            && repair.predecessor.replica.node == placement.launch.node
                    })
                    .map(move |repair| HostCleanupOwner {
                        node,
                        replica: repair.predecessor.replica.id,
                        incarnation: repair.predecessor.replica.instance,
                        old_profile: repair.old_profile.clone(),
                        host_admitted: repair.profile.is_some(),
                    })
            })
            .collect()
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        self.finished = Some(json!({"state":"closed"}));
        let mut errors = Vec::new();
        for child in &mut self.children {
            if let Some(link) = child.link.take() {
                if let Err(error) = link.close().await {
                    errors.push(error);
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
    pub(crate) fn new(launches: Vec<Launch>) -> Result<Self> {
        let first = launches
            .first()
            .ok_or("FM requires a nonempty static Node pool")?;
        first
            .profile
            .control_profile()
            .validate_node_pool(launches.len())?;
        for (index, launch) in launches.iter().enumerate() {
            launch.validate()?;
            if usize::from(launch.placement) != index + 1
                || launch.profile != first.profile
                || launch.deployment != first.deployment
                || launch.parent_pid != first.parent_pid
                || launch.fm != first.fm
                || launch.generation != first.generation
                || launches[..index].iter().any(|prior| {
                    prior.node.id == launch.node.id
                        || prior.session == launch.session
                        || prior.directory == launch.directory
                })
                || launch.peers.iter().any(|peer| {
                    !launches.iter().any(|other| {
                        other.node == peer.node
                            && other.session == peer.session
                            && other.partition == peer.partition
                            && other.replica == peer.replica
                            && other.incarnation == peer.incarnation
                    })
                })
            {
                return Err("FM requires one exact ordered original topology".into());
            }
        }
        let targets = Targets::new(
            first.fm.clone(),
            launches
                .iter()
                .map(|launch| {
                    Target::new(
                        launch.node.clone(),
                        wire::Role::Fm,
                        None,
                        launch.generation.clone(),
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        )?;
        Ok(Self {
            profile: first.profile,
            deployment: first.deployment.clone(),
            ledger: Ledger::new(launches),
            children: Vec::new(),
            peer_idle_requested: false,
            finished: None,
            targets,
        })
    }

    pub(crate) async fn attach(&mut self, index: usize, metadata: Metadata) -> Result<Metadata> {
        let launch = self
            .ledger
            .placements
            .get(index)
            .ok_or("all original Nodes already attached")?
            .launch
            .clone();
        if metadata.pid == 0
            || self.finished.is_some()
            || index > self.children.len()
            || self.ledger.placements[index].bootstrap != Bootstrap::NodeUp
            || self
                .children
                .get(index)
                .is_some_and(|child| child.link.is_some() || child.node_pid != metadata.pid)
        {
            return Err("invalid original Node attachment".into());
        }
        if index == self.children.len() {
            let connection = Connection {
                launch: launch.clone(),
                node_pid: metadata.pid,
                link: None,
                metadata: None,
            };
            Self::check_metadata(&connection, &metadata, false)?;
            self.children.push(connection);
        }
        self.validate_metadata(index, &metadata, false)?;
        self.children[index].metadata = Some(metadata.clone());
        let end = Instant::now() + Duration::from_secs(30);
        self.targets.set_address(&launch.node, metadata.address)?;
        self.children[index].link = Some(self.targets.connect(&launch.node, end).await?);
        loop {
            self.pump(index).await?;
            if self.ledger.placements[index].bootstrap == Bootstrap::Metadata
                && launch
                    .directory
                    .join("profile.json")
                    .try_exists()
                    .map_err(err)?
            {
                let metadata: Metadata = read_json(&launch.directory.join("profile.json"))?;
                self.validate_metadata(index, &metadata, true)?;
                self.ledger.placements[index]
                    .admit_profile(metadata.profile.clone().ok_or("missing actual profile")?)?;
                self.children[index].metadata = Some(metadata.clone());
                return Ok(metadata);
            }
            if Instant::now() >= end {
                return Err("native bootstrap/profile startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    pub(crate) fn validate_metadata(
        &self,
        index: usize,
        metadata: &Metadata,
        profile: bool,
    ) -> Result<()> {
        Self::check_metadata(&self.children[index], metadata, profile)
    }

    fn check_metadata(owner: &Connection, metadata: &Metadata, profile: bool) -> Result<()> {
        if metadata.pid != owner.node_pid
            || metadata.deployment != owner.launch.deployment
            || metadata.session != owner.launch.session
            || metadata.node != owner.launch.node
            || !metadata.address.ip().is_loopback()
            || metadata.address.port() == 0
            || profile != metadata.profile.is_some()
            || metadata
                .peer_address
                .is_some_and(|address| !address.ip().is_loopback() || address.port() == 0)
            || metadata.peer_addresses.len() != owner.launch.peers.len()
            || metadata.peer_address != metadata.peer_addresses.first().copied()
            || metadata
                .peer_addresses
                .iter()
                .enumerate()
                .any(|(i, address)| {
                    !address.ip().is_loopback()
                        || address.port() == 0
                        || metadata.peer_addresses[..i].contains(address)
                })
            || owner.metadata.as_ref().is_some_and(|prior| {
                prior.address != metadata.address || prior.peer_addresses != metadata.peer_addresses
            })
        {
            return Err(
                "startup metadata not from the exact original expected child/session".into(),
            );
        }
        Ok(())
    }
    pub(crate) async fn pump(&mut self, index: usize) -> Result<()> {
        let Some(link) = self.children[index].link.as_ref() else {
            return Ok(());
        };
        let incoming = link
            .receive(Instant::now() + Duration::from_millis(1))
            .await?;
        let Some(incoming) = incoming else {
            return Ok(());
        };
        self.receive_message(index, incoming).await
    }
    pub(crate) async fn receive_message(
        &mut self,
        index: usize,
        mut incoming: wire::Incoming,
    ) -> Result<()> {
        let link = self.children[index]
            .link
            .as_ref()
            .ok_or("original node link absent")?;
        let action = incoming.action()?;
        let generation = self.children[index].generation();
        match action.as_str() {
            "LFUMUpload" => {
                let header = incoming
                    .reply
                    .take()
                    .ok_or("LFUMUpload missing consuming reply authority")?
                    .lfum_upload(link, |body| self.ledger.lfum_upload(index, body))
                    .await?;
                if self.ledger.generation.as_ref().is_some_and(|round| {
                    round.number == header.number && round.uploads[index].is_some()
                }) {
                    link.advance_generation(&header);
                }
                return Ok(());
            }
            "NodeUp" => {
                let placement = &mut self.ledger.placements[index];
                let metadata = self.children[index]
                    .metadata
                    .as_ref()
                    .ok_or("missing original listener")?;
                let context = incoming
                    .reply
                    .take()
                    .ok_or("NodeUp missing consuming reply authority")?;
                context
                    .node_up(link, |body| {
                        body.validate_fresh_stateless().map_err(err)?;
                        let expected = NodeUp::fresh_stateless(
                            placement.launch.name(),
                            "fube-cluster-local".into(),
                            metadata.address,
                            FabricVersionInstance::native_default(),
                        )
                        .map_err(err)?;
                        if body != expected {
                            return Err(
                                "unexpected/stale native NodeUp identity or recovery contents"
                                    .into(),
                            );
                        }
                        if placement.bootstrap == Bootstrap::NodeUp {
                            placement.node_description = Some(body.node.clone());
                            placement.activation_sequence = 1;
                            placement.activated = true;
                        }
                        let mut ack = NodeUpAck::fresh_activated(
                            body.node.version,
                            placement.activation_sequence,
                        )
                        .map_err(err)?;
                        ack.activated = placement.activated;
                        Ok((ack, generation))
                    })
                    .await?;
                if placement.bootstrap == Bootstrap::NodeUp {
                    placement.bootstrap = Bootstrap::FinalUpload;
                }
                return Ok(());
            }
            _ => {}
        }
        match incoming.fm_report(link)? {
            FmReport::ReplicaEndpointUpdated(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM endpoint publication is gated".into());
                }
                if let Some(reply) = self.ledger.endpoint_updated(index, body)? {
                    let request = NodeRequest::replica_endpoint_updated_reply(&reply, &generation)?;
                    NodeClient::new(link)?.send(request).await?;
                }
            }
            FmReport::DoReconfigurationReply(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM configuration reply is gated".into());
                }
                self.ledger.configuration_reply(index, body)?;
            }
            FmReport::ChangeConfiguration(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM candidate redirection is gated".into());
                }
                if let Some(reply) =
                    self.ledger
                        .change_configuration(index, body, Instant::now())?
                {
                    let request = NodeRequest::change_configuration_reply(&reply, &generation)?;
                    NodeClient::new(link)?.send(request).await?;
                    if self.finished.is_none() {
                        let next = &self
                            .ledger
                            .swap
                            .as_ref()
                            .ok_or("missing redirected failover")?
                            .body;
                        self.send_configuration(next).await?;
                    }
                }
            }
            FmReport::GenerationUpdateReject(payload) => {
                let (current, _) = payload.into_parts()?;
                if let Some(round) = &mut self.ledger.generation {
                    round.reject(&current)?;
                }
            }
            FmReport::ServiceTypeStatus { enabled, payload } => {
                let (body, _) = payload.into_parts()?;
                let reply = self.ledger.type_status(index, enabled, &body)?;
                let request = if enabled {
                    NodeRequest::service_type_enabled_reply(&reply, &generation)
                } else {
                    NodeRequest::service_type_disabled_reply(&reply, &generation)
                }?;
                NodeClient::new(link)?.send(request).await?;
            }
            FmReport::NodeActivationReply { activate, payload } => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM activation unsupported".into());
                }
                self.ledger.node_control_reply(index, activate, body)?;
            }
            FmReport::NodeUpdateServiceReply(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM service update unsupported".into());
                }
                self.ledger.service_reply(index, body)?;
            }
            FmReport::ReportLoad(payload) => {
                self.ledger.load(index, payload.into_parts()?.0)?;
            }
            FmReport::ReplicaUp(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM report unsupported".into());
                }
                if self.ledger.generation_pending() {
                    return Ok(()); // Rebuild cannot acknowledge inventory without LFUM support.
                }
                let response = self.ledger.report(index, &body)?;
                let Some(response) = response else {
                    return Ok(()); // Known retired lifetime: no semantic ACK or link loss.
                };
                if !body.replicas.0.is_empty()
                    && self.ledger.placements[index].bootstrap == Bootstrap::Admitted
                {
                    self.ledger.down(index);
                }
                let request = NodeRequest::replica_up_reply(&response, &generation)?;
                NodeClient::new(link)?.send(request).await?;
                if self.ledger.placements[index].bootstrap == Bootstrap::FinalUpload {
                    self.ledger.placements[index].bootstrap = Bootstrap::Metadata;
                }
            }
            FmReport::ServiceTypeNotification(payload) => {
                let (body, _) = payload.into_parts()?;
                let admitted = self.ledger.placements[index].notification(&body)?;
                if !admitted.infos.0.is_empty() {
                    self.ledger.placements[index].catalog_notified = true;
                }
                let request = NodeRequest::service_type_notification_reply(&admitted, &generation)?;
                NodeClient::new(link)?.send(request).await?;
            }
            FmReport::AddReplicaReply(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm || !self.profile.peer_replicas() {
                    return Err("AddReplicaReply requires the original peer-idle FM profile".into());
                }
                self.read_repair_host()?;
                self.ledger.idle_reply(index, body)?;
            }
            FmReport::RemoveReplicaReply(payload) => {
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm || !self.profile.peer_replicas() {
                    return Err(
                        "RemoveReplicaReply requires the original peer-idle FM profile".into(),
                    );
                }
                self.ledger.idle_removal_reply(index, body)?;
            }
            FmReport::LifecycleReply {
                primary,
                removing,
                payload,
            } => {
                if primary != self.children[index].launch.profile.stateful() {
                    return Err("reply action does not match admitted lifecycle profile".into());
                }
                let (body, owner) = payload.into_parts()?;
                if owner != Owner::Fm {
                    return Err("FMM semantic reply unsupported".into());
                }
                self.ledger.reply(index, body, removing)?;
            }
        }
        Ok(())
    }
    pub async fn drive(&mut self) {
        for index in 0..self.children.len() {
            if !self.ledger.placements[index].link_alive {
                continue;
            }
            let result = self.pump(index).await;
            let result = result.and_then(|()| self.read_retirement(index));
            let result = match result {
                Ok(()) if self.ledger.generation_pending() => self.retry_generation(index).await,
                Ok(()) => self.retry_primary(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) => self.retry_deletion(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) => self.retry_node_work(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) => self.retry_idle(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) => self.retry_idle_removal(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) => self.retry_idle_deletion(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) if index == self.ledger.primary_index() => {
                    if let Some(body) = self.ledger.configuration_retry(Instant::now()) {
                        self.send_configuration(&body).await
                    } else {
                        Ok(())
                    }
                }
                result => result,
            };
            if let Err(error) = result {
                self.ledger.lost(index, error);
                if let Some(link) = &self.children[index].link {
                    link.retire();
                }
            }
        }
        if let Err(error) = self.advance_initial_membership().await {
            self.ledger.lost(0, error);
        }
        self.start_failover().await;
        if let Err(error) = self.advance_repair().await {
            self.ledger
                .lost(self.ledger.repair_destination_index().unwrap_or(0), error);
        }
    }
    pub(crate) async fn advance_initial_membership(&mut self) -> Result<()> {
        if self.profile.configured_sizes().is_none()
            || self.ledger.generation_pending()
            || self.profile.node_count() <= 1
            || self.ledger.configuration.is_some()
            || self.ledger.placements[0].observed != fm::Observed::Running
            || self
                .ledger
                .placements
                .iter()
                .any(|p| !p.link_alive || p.loss_reported)
        {
            return Ok(());
        }
        if let Some(destination) = (1..self.profile.node_count()).find(|index| {
            !self
                .ledger
                .idles
                .iter()
                .any(|idle| idle.body.replica.node == self.children[*index].launch.node)
        }) {
            if self
                .ledger
                .idles
                .iter()
                .all(|idle| idle.reply.as_ref().is_some_and(|reply| reply.error == 0))
            {
                let body = self.ledger.request_idle_on(destination, Instant::now())?;
                self.send_idle(&body).await?;
            }
        } else if self.ledger.idle_status()["ready"] == true {
            let body = self.ledger.request_configuration(Instant::now())?;
            self.send_configuration(&body).await?;
        }
        Ok(())
    }
    /// Restore one lost volatile Primary as a fresh Secondary on its original live Node.
    pub async fn repair_primary_replica(&mut self) -> Result<Value> {
        if self.finished.is_some()
            || !self.profile.mesh_peers()
            || self.children.len() != self.ledger.placements.len()
        {
            return Err("Primary repair requires the original running configured cluster".into());
        }
        if self.ledger.begin_primary_repair(Instant::now())? {
            self.request_repair_build().await?;
        }
        Ok(self.ledger.repair_status())
    }

    /// Restore a lost Secondary without moving or reopening the current Primary.
    pub async fn repair_secondary_replica(&mut self, destination: u8) -> Result<Value> {
        if self.finished.is_some()
            || destination == 0
            || self.children.len() != self.ledger.placements.len()
        {
            return Err("Secondary repair requires the original running cluster".into());
        }
        if let Some(body) = self
            .ledger
            .begin_secondary_repair(usize::from(destination - 1), Instant::now())?
        {
            if let Err(error) = self.send_configuration(&body).await {
                self.ledger.lost(self.ledger.primary_index(), error.clone());
                return Err(error);
            }
        }
        Ok(self.ledger.repair_status())
    }

    /// Cancel one fresh repair Idle without authorizing another replica lifetime.
    pub async fn cancel_repair_build(&mut self) -> Result<Value> {
        if self.finished.is_some() {
            return Err("repair cancellation requires the original running cluster".into());
        }
        let body = self.ledger.cancel_repair_build(Instant::now())?;
        self.send_idle_removal(&body).await?;
        Ok(self.ledger.repair_status())
    }

    /// Delete the exact cancelled destination independently of Primary removal.
    pub async fn delete_cancelled_repair(&mut self) -> Result<Value> {
        if self.finished.is_some() {
            return Err("repair deletion requires the original running cluster".into());
        }
        let destination = self
            .ledger
            .repair_destination_index()
            .ok_or("missing repair destination")?;
        let body = self.ledger.delete_cancelled_repair()?;
        self.send_repair_deletion(destination, &body).await?;
        Ok(self.ledger.repair_status())
    }

    pub(crate) async fn send_repair_deletion(
        &self,
        destination: usize,
        body: &DeleteReplica,
    ) -> Result<()> {
        let child = &self.children[destination];
        let link = child
            .link
            .as_ref()
            .ok_or("original repair destination link absent")?;
        NodeClient::new(link)?
            .send(NodeRequest::delete_replica(body, &child.generation())?)
            .await
    }

    /// Replace one processed lost member on an explicitly selected unused Node.
    pub async fn replace_replica_on(&mut self, destination: u8) -> Result<Value> {
        if self.finished.is_some()
            || destination == 0
            || self.children.len() != self.ledger.placements.len()
        {
            return Err("replacement requires the original running Node pool".into());
        }
        if self
            .ledger
            .begin_spare_replacement(usize::from(destination - 1), Instant::now())?
        {
            self.request_repair_build().await?;
        }
        Ok(self.ledger.repair_status())
    }

    pub(crate) async fn request_repair_build(&mut self) -> Result<()> {
        let destination = self
            .ledger
            .repair_destination_index()
            .ok_or("missing failed replica")?;
        let body = self
            .ledger
            .repair
            .as_ref()
            .expect("repair")
            .build
            .body
            .clone();
        if let Err(error) = self.send_idle(&body).await {
            self.ledger.lost(destination, error.clone());
            return Err(error);
        }
        self.ledger.repair.as_mut().expect("repair").host_requested = true;
        Ok(())
    }

    pub(crate) fn read_repair_host(&mut self) -> Result<()> {
        if self
            .ledger
            .repair
            .as_ref()
            .is_some_and(|r| r.profile.is_none())
        {
            let destination = self
                .ledger
                .repair_destination_index()
                .ok_or("missing failed replica")?;
            let predecessor = &self
                .ledger
                .repair
                .as_ref()
                .expect("repair")
                .predecessor
                .replica;
            let path = self.children[destination]
                .launch
                .host_replacement_path(predecessor.id, predecessor.instance);
            if path.try_exists().map_err(err)? {
                let receipt: HostReplacementReceipt = read_json(&path)?;
                if receipt.predecessor.runtime_root.exists()
                    || Path::new(&format!("/proc/{}", receipt.predecessor.app_pid)).exists()
                    || !receipt.profile.runtime_root.exists()
                    || !Path::new(&format!("/proc/{}", receipt.profile.app_pid)).exists()
                {
                    return Err("physical host replacement is not independently observable".into());
                }
                let pid = self.children[destination].node_pid;
                if self.ledger.repaired_host(&receipt, pid)? {
                    self.children[destination]
                        .metadata
                        .as_mut()
                        .ok_or("missing node metadata")?
                        .profile = Some(receipt.profile);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn advance_repair(&mut self) -> Result<()> {
        if self
            .ledger
            .repair
            .as_ref()
            .is_some_and(|r| r.removal.as_ref().is_some_and(|c| c.reply.is_none()) || r.unknown)
        {
            return Ok(());
        }
        if self
            .ledger
            .repair
            .as_ref()
            .is_some_and(|r| !r.host_requested)
        {
            self.request_repair_build().await?;
        }
        self.read_repair_host()?;
        if let Some(body) = self.ledger.start_repair_configuration(Instant::now())? {
            self.send_configuration(&body).await?;
        }
        Ok(())
    }
    pub(crate) async fn start_failover(&mut self) {
        let failover = match self.ledger.start_primary_failover(Instant::now()) {
            Ok(Some(body)) => self.send_configuration(&body).await,
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        };
        if let Err(error) = failover {
            self.ledger.lost(self.ledger.primary_index(), error);
        }
    }
    /// Begin one live-session ordinary-FM inventory reconciliation. New
    /// mutations remain gated until all native LFUM uploads are processed.
    pub fn begin_generation_handshake(&mut self) -> Result<Value> {
        if self.finished.is_some() || self.children.len() != self.ledger.placements.len() {
            return Err("generation handshake requires the original running cluster".into());
        }
        self.ledger.begin_generation()?;
        Ok(self.status()["generation_handshake"].clone())
    }
    pub(crate) async fn retry_generation(&mut self, index: usize) -> Result<()> {
        let Some(work) = self
            .ledger
            .generation
            .as_mut()
            .and_then(|round| round.next(index, Instant::now()))
        else {
            return Ok(());
        };
        let link = self.children[index]
            .link
            .as_ref()
            .ok_or("original generation link absent")?;
        match work {
            fm::GenerationWork::Propose(header) => {
                if let Some(body) = link.generation_proposal_optional(&header).await? {
                    self.ledger
                        .generation
                        .as_mut()
                        .ok_or("generation owner lost")?
                        .proposal_reply(index, &body)?;
                }
            }
            fm::GenerationWork::Update(header) => {
                NodeClient::new(link)?
                    .send(NodeRequest::generation_update(
                        &GenerationUpdate {
                            fm_service_epoch: Epoch::default(),
                        },
                        &header,
                    )?)
                    .await?;
                link.advance_generation(&header);
            }
        }
        Ok(())
    }
    pub(crate) async fn retry_node_work(&mut self, index: usize) -> Result<()> {
        if self.ledger.generation_pending() {
            return Ok(());
        }
        let now = Instant::now();
        let control = self.ledger.node_control_retry(index, now);
        let service = self.ledger.service_update_retry(index, now);
        let child = &self.children[index];
        let link = child.link.as_ref().ok_or("original node link absent")?;
        if let Some((activate, body)) = control {
            let request = if activate {
                NodeRequest::node_activate(&body, &child.generation())
            } else {
                NodeRequest::node_deactivate(&body, &child.generation())
            }?;
            NodeClient::new(link)?.send(request).await?;
        }
        if let Some(body) = service {
            NodeClient::new(link)?
                .send(NodeRequest::node_update_service(
                    &body,
                    &child.generation(),
                )?)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn retry_primary(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.primary_retry(index, Instant::now())? else {
            return Ok(());
        };
        let child = &self.children[index];
        let request = NodeRequest::add_primary(&body, &child.generation())?;
        let link = child.link.as_ref().ok_or("original Primary link absent")?;
        NodeClient::new(link)?.send(request).await
    }
    pub(crate) async fn retry_idle(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.idle_retry(index, Instant::now()) else {
            return Ok(());
        };
        self.send_idle(&body).await
    }
    pub(crate) async fn send_idle(&self, body: &AddReplica) -> Result<()> {
        let child = self
            .children
            .get(self.ledger.primary_index())
            .ok_or("missing original Primary owner")?;
        let link = child.link.as_ref().ok_or("original Primary link absent")?;
        NodeClient::new(link)?
            .send(NodeRequest::add_replica(body, &child.generation())?)
            .await
    }
    pub(crate) async fn retry_idle_removal(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.idle_removal_retry(index, Instant::now()) else {
            return Ok(());
        };
        self.send_idle_removal(&body).await
    }
    pub(crate) async fn send_idle_removal(&self, body: &RemoveReplica) -> Result<()> {
        let child = self
            .children
            .get(self.ledger.primary_index())
            .ok_or("missing original Primary owner")?;
        let link = child.link.as_ref().ok_or("original Primary link absent")?;
        NodeClient::new(link)?
            .send(NodeRequest::remove_replica(body, &child.generation())?)
            .await
    }
    pub(crate) async fn retry_deletion(&mut self, index: usize) -> Result<()> {
        if self.ledger.generation_pending() {
            return Ok(());
        }
        let Some(body) = self.ledger.deletion_retry(index, Instant::now())? else {
            return Ok(());
        };
        let child = &self.children[index];
        let request = NodeRequest::delete_replica(&body, &child.generation())?;
        let link = child.link.as_ref().ok_or("original deletion link absent")?;
        NodeClient::new(link)?.send(request).await
    }
    pub(crate) async fn retry_idle_deletion(&mut self, index: usize) -> Result<()> {
        if let Some(body) = self.ledger.repair_deletion_retry(index, Instant::now()) {
            return self.send_repair_deletion(index, &body).await;
        }
        let Some(body) = self.ledger.idle_deletion_retry(index, Instant::now()) else {
            return Ok(());
        };
        self.send_idle_deletion(&body).await
    }
    pub(crate) async fn send_idle_deletion(&self, body: &DeleteReplica) -> Result<()> {
        let child = self
            .children
            .get(1)
            .ok_or("missing original destination owner")?;
        let link = child
            .link
            .as_ref()
            .ok_or("original destination link absent")?;
        NodeClient::new(link)?
            .send(NodeRequest::delete_replica(body, &child.generation())?)
            .await
    }
    pub(crate) fn read_retirement(&mut self, index: usize) -> Result<()> {
        let child = &self.children[index];
        let path = child.launch.directory.join("retirement.json");
        let node_pid = child.node_pid;
        self.ledger.read_retirement(index, &path, node_pid)?;
        self.ledger.read_absence(
            index,
            &child.launch.directory.join("absence.json"),
            node_pid,
        )?;
        self.ledger.read_pre_open_cancellation(
            index,
            &child.launch.directory.join("pre-open-cancellation.json"),
            node_pid,
        )?;
        self.ledger.read_failed_registration(
            index,
            &child
                .launch
                .directory
                .join("failed-registration-deletion.json"),
            node_pid,
        )?;
        self.ledger.read_failed_open(
            index,
            &child.launch.directory.join("failed-open-deletion.json"),
            node_pid,
        )?;
        self.ledger.read_host_loss(
            index,
            &child.launch.directory.join("host-loss.json"),
            node_pid,
        )
    }
    pub fn status(&self) -> Value {
        let mut status = self.ledger.status();
        status["deployment"] = json!(self.deployment);
        if let Some(nodes) = status["nodes"].as_array_mut() {
            for (index, node) in nodes.iter_mut().enumerate() {
                if let Some(child) = self.children.get(index) {
                    node["node_pid"] = json!(child.node_pid);
                    let path = child.launch.directory.join("observation.json");
                    if path.exists() {
                        match read_json::<Value>(&path) {
                            Ok(observation)
                                if observation["deployment"] == child.launch.deployment
                                    && observation["session"] == child.launch.session
                                    && observation["node_pid"] == json!(child.node_pid) =>
                            {
                                node["retained_work"] = observation;
                            }
                            _ => {
                                node["observation_error"] =
                                    json!("invalid source-bound observation");
                            }
                        }
                    }
                }
            }
        }
        if self.profile == ServiceProfile::StatefulPeerIdle {
            let source = &status["nodes"][0]["retained_work"];
            let target = &status["nodes"][1]["retained_work"];
            let reply = &source["peer_idle"]["reply"];
            let failed =
                self.peer_idle_requested && reply["error"].as_i64().is_some_and(|error| error != 0);
            let opened = self.peer_idle_requested
                && reply["error"] == 0
                && reply["replica"]["id"] == self.ledger.placements[1].replica
                && reply["replica"]["instance"] == self.ledger.placements[1].incarnation
                && reply["replica"]["current_role"] == 2
                && reply["replica"]["state"] == 1
                && reply["replica"]["replication_endpoint"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && target["local_replica"]["open"] == true;
            let removing = source["peer_idle"]["removal"].is_object();
            let removed = removing && source["peer_idle"]["removal"]["result"] == 0;
            let destination_lost = self.ledger.placements[1].loss_reported;
            let configuration = self.ledger.configuration_status();
            let configured = configuration["ready"] == true
                && configuration["kind"] == "same_primary_configuration";
            let retired = configuration["ready"] == true
                && configuration["kind"] == "same_primary_retirement"
                && self.ledger.idle_predecessor.is_none();
            let built = !destination_lost
                && opened
                && !removing
                && source["peer_idle"]["build"]["result"] == 0;
            status["peer_idle"] = json!({
                "requested":self.peer_idle_requested,
                "state": if configured { "active_secondary" } else if retired { "retired_secondary" } else if destination_lost { "lost" } else if removed { "removed" } else if removing { "removing" } else if failed { "failed" } else if opened { "open" } else if self.peer_idle_requested { "pending" } else { "absent" },
                "error": if failed { reply["error"].clone() } else { Value::Null },
                "built":built, "serving":false,
                "active_secondary":configured,
                "build":source["peer_idle"]["build"],
                "fm":self.ledger.idle_status(),
                "fm_removal":self.ledger.idle_removal_status(),
                "fm_deletion":self.ledger.idle_deletion_status(),
                "ready":!destination_lost && source["peer_idle"]["ready"] == true,
                "destination_lost":destination_lost,
                "removal":source["peer_idle"]["removal"],
                "source":source["peer_idle"], "target":target["local_replica"],
                "peer_link_alive":[source["peer_link_alive"],target["peer_link_alive"]],
                "peer_error":[source["peer_error"],target["peer_error"]],
                "command_error":[source["peer_command_error"],target["peer_command_error"]],
            });
        }
        status
    }
    /// FM AddReplica owns peer creation, RAP build and semantic Ready-Idle completion.
    pub async fn add_idle_replica(&mut self) -> Result<Value> {
        let current = self.ledger.idle_status();
        if self.ledger.configuration.is_some()
            || self.finished.is_some()
            || self.children.len() != 2
            || self.profile != ServiceProfile::StatefulPeerIdle
            || (self.ledger.idle_removal.is_some() && current["state"] != "failed")
            || self.peer_idle_requested && self.ledger.idles.is_empty()
        {
            return Err("FM AddReplica requires the original unused peer-idle profile".into());
        }
        if current["state"] != "absent" && current["state"] != "pending" {
            return Ok(current);
        }
        let body = self.ledger.request_idle(Instant::now())?;
        self.peer_idle_requested = true;
        if let Err(error) = self.send_idle(&body).await {
            self.ledger.lost(0, error.clone());
            return Err(error);
        }
        Ok(self.ledger.idle_status())
    }
    /// Build one original Secondary destination selected by its one-based node number.
    pub async fn add_idle_replica_on(&mut self, destination: u8) -> Result<Value> {
        if !(self.profile == ServiceProfile::StatefulThree
            || self.profile.configured_sizes().is_some())
            || self.finished.is_some()
            || self.children.len() != self.ledger.placements.len()
            || !(2..=self.children.len()).contains(&usize::from(destination))
        {
            return Err(
                "targeted build requires an original configured Secondary destination".into(),
            );
        }
        let body = self
            .ledger
            .request_idle_on(usize::from(destination - 1), Instant::now())?;
        if let Err(error) = self.send_idle(&body).await {
            self.ledger.lost(0, error.clone());
            return Err(error);
        }
        Ok(self.ledger.idle_status())
    }
    /// Explicitly authorize one fresh Idle after original FM removal and proved disposal.
    pub async fn replace_idle_replica(&mut self) -> Result<Value> {
        if self.finished.is_some()
            || self.children.len() != 2
            || self.profile != ServiceProfile::StatefulPeerIdle
        {
            return Err("Idle replacement requires the original peer-idle profile".into());
        }
        let current = self.ledger.idle_status();
        if self.ledger.idle_predecessor.is_some() && current["state"] == "ready" {
            return Ok(current);
        }
        let body = self.ledger.request_idle_replacement(Instant::now())?;
        self.peer_idle_requested = true;
        if let Err(error) = self.send_idle(&body).await {
            self.ledger.lost(0, error.clone());
            return Err(error);
        }
        Ok(self.ledger.idle_status())
    }
    pub async fn activate_idle_replica(&mut self) -> Result<Value> {
        if self.finished.is_some() || self.children.len() != self.ledger.placements.len() {
            return Err("configuration requires the original running cluster".into());
        }
        let body = self.ledger.request_configuration(Instant::now())?;
        if self.ledger.configuration_status()["ready"] == true {
            return Ok(self.ledger.configuration_status());
        }
        if let Err(error) = self.send_configuration(&body).await {
            self.ledger.lost(0, error.clone());
            return Err(error);
        }
        Ok(self.ledger.configuration_status())
    }
    pub async fn transfer_primary(&mut self) -> Result<Value> {
        if self.finished.is_some() || self.children.len() != self.ledger.placements.len() {
            return Err("Primary transfer requires the original running cluster".into());
        }
        let body = self.ledger.request_primary_transfer(Instant::now())?;
        if self.ledger.configuration_status()["ready"] != true {
            if let Err(error) = self.send_configuration(&body).await {
                self.ledger.lost(self.ledger.primary_index(), error.clone());
                return Err(error);
            }
        }
        Ok(self.ledger.configuration_status())
    }
    /// Request a handoff to a one-based current Secondary; retrying the same
    /// destination replays that transition until another handoff is requested.
    pub async fn transfer_primary_to(&mut self, destination: u8) -> Result<Value> {
        if self.finished.is_some()
            || destination == 0
            || self.children.len() != self.ledger.placements.len()
        {
            return Err(
                "Primary transfer requires a running cluster and explicit destination".into(),
            );
        }
        let body = self
            .ledger
            .request_primary_transfer_to(usize::from(destination - 1), Instant::now())?;
        if self.ledger.configuration_status()["ready"] != true {
            if let Err(error) = self.send_configuration(&body).await {
                self.ledger.lost(self.ledger.primary_index(), error.clone());
                return Err(error);
            }
        }
        Ok(self.ledger.configuration_status())
    }
    pub async fn retire_secondary_replica(&mut self) -> Result<Value> {
        if self.finished.is_some() || self.children.len() != 2 {
            return Err("Secondary retirement requires the original running cluster".into());
        }
        let body = self.ledger.request_secondary_retirement(Instant::now())?;
        if self.ledger.configuration_status()["ready"] != true {
            if let Err(error) = self.send_configuration(&body).await {
                self.ledger.lost(0, error.clone());
                return Err(error);
            }
        }
        Ok(self.ledger.configuration_status())
    }
    pub(crate) async fn send_configuration(&self, body: &DoReconfiguration) -> Result<()> {
        let child = self
            .children
            .get(self.ledger.configuration_target_index())
            .ok_or("missing original Primary owner")?;
        let link = child.link.as_ref().ok_or("missing original Primary link")?;
        NodeClient::new(link)?
            .send(NodeRequest::do_reconfiguration(body, &child.generation())?)
            .await
    }
    /// FM retires original creation or removes the opened/lost Idle's replication session.
    /// Only RemoveReplicaReply clears pending_remove, never destination deletion.
    pub async fn remove_idle_replica_via_fm(&mut self) -> Result<Value> {
        if self.finished.is_some()
            || self.children.len() != 2
            || self.profile != ServiceProfile::StatefulPeerIdle
        {
            return Err("FM RemoveReplica requires the original peer-idle profile".into());
        }
        let current = self.ledger.idle_removal_status();
        if current["state"] == "removed" || current["state"] == "unknown" {
            return Ok(current);
        }
        if self.ledger.idle_removal.is_none()
            && !matches!(
                self.ledger.idle_status()["state"].as_str(),
                Some("pending" | "ready" | "failed" | "lost")
            )
            && self.status()["peer_idle"]["state"] != "open"
        {
            return Err("FM removal requires original FM creation or an opened/lost Idle".into());
        }
        let body = self.ledger.request_idle_removal(Instant::now())?;
        if let Err(error) = self.send_idle_removal(&body).await {
            self.ledger.lost(0, error.clone());
            return Err(error);
        }
        Ok(self.ledger.idle_removal_status())
    }
    /// Delete the original abandoned destination independently of Primary Remove.
    /// Cleanup joins the native reply with original disposal, pre-Open
    /// cancellation, known creation failure or never-created absence;
    /// none authorizes a successor.
    pub async fn delete_idle_replica_via_fm_with_force(&mut self, force: bool) -> Result<Value> {
        if self.finished.is_some()
            || self.children.len() != 2
            || self.profile != ServiceProfile::StatefulPeerIdle
        {
            return Err("FM Idle deletion requires the original peer-idle profile".into());
        }
        let current = self.ledger.idle_deletion_status();
        if current["state"] == "deleted" || current["state"] == "unknown" {
            return Ok(current);
        }
        let mut target_kind = fm::IdleDeleteTarget::Runtime;
        if self.ledger.idle_deletion.is_none() {
            let status = self.status();
            let target = &status["peer_idle"]["target"];
            let unobserved = target.is_null() && self.ledger.idle_absence_allowed();
            let registered_open = target["phase"] == "Opening"
                && target["registration"].is_object()
                && target["retained_usage"] == 1;
            let pre_registration = target["phase"] == "Opening"
                && target["registration"].is_null()
                && target["retained_usage"] == 0
                && self.ledger.idle_absence_allowed();
            let failed_registration = target["phase"] == "Closed"
                && target["pre_open_failure"]["error"]
                    .as_i64()
                    .is_some_and(|error| self.ledger.idle_failed_creation_error() == Some(error));
            let failed_open = target["phase"] == "Closed"
                && target["runtime_open_failure"]["error"]
                    .as_i64()
                    .is_some_and(|error| self.ledger.idle_failed_creation_error() == Some(error));
            let terminal = target["phase"] == "Closed"
                || target["phase"] == "Closing"
                || target["phase"] == "Opening";
            if target["open"] != true
                && !registered_open
                && !pre_registration
                && !unobserved
                && !failed_registration
                && !failed_open
                && !terminal
            {
                return Err("FM Idle deletion requires an original opening/opened or known creation-failed destination, or an absent unanswered FM Add".into());
            }
            target_kind = if unobserved {
                fm::IdleDeleteTarget::Unobserved
            } else if pre_registration {
                fm::IdleDeleteTarget::PreRegistration
            } else if failed_registration {
                fm::IdleDeleteTarget::FailedRegistration
            } else if failed_open {
                fm::IdleDeleteTarget::FailedOpen
            } else {
                fm::IdleDeleteTarget::Runtime
            };
        }
        let body = if force {
            self.ledger
                .request_idle_deletion_with_force(Instant::now(), target_kind, true)?
        } else {
            self.ledger
                .request_idle_deletion(Instant::now(), target_kind)?
        };
        if let Err(error) = self.send_idle_deletion(&body).await {
            self.ledger.lost(1, error.clone());
            return Err(error);
        }
        Ok(self.ledger.idle_deletion_status())
    }
    /// Returns the retained operation outcome, never transport-write success.
    /// `quit` is accepted here; the foreground then calls owned shutdown.
    pub async fn execute(&mut self, command: Command) -> Result<Value> {
        if self.finished.is_some() {
            return Err("deployment already shut down".into());
        }
        let op = command.op.clone();
        match self.ledger.accept(command)? {
            Accepted::NodeControl(index, activate, body) => {
                let generation = &self.children[index].generation();
                let request = if activate {
                    NodeRequest::node_activate(&body, generation)
                } else {
                    NodeRequest::node_deactivate(&body, generation)
                }?;
                let link = self.children[index]
                    .link
                    .as_ref()
                    .ok_or("original link absent")?;
                let result = NodeClient::new(link)?.send(request).await;
                if let Err(error) = result {
                    self.ledger.lost(index, error);
                }
            }
            Accepted::Duplicate(outcome) => return Ok(outcome),
            Accepted::Create(index, body) => {
                let factory = if self.children[index].launch.profile.stateful() {
                    NodeRequest::add_primary
                } else {
                    NodeRequest::add_instance
                };
                let request = factory(&body, &self.children[index].generation())?;
                let link = self.children[index]
                    .link
                    .as_ref()
                    .ok_or("original link absent")?;
                let result = NodeClient::new(link)?.send(request).await;
                if let Err(error) = result {
                    self.ledger.lost(index, error);
                }
            }
            Accepted::Delete(index, body) => {
                let factory = if self.children[index].launch.profile.stateful() {
                    NodeRequest::delete_replica
                } else {
                    NodeRequest::remove_instance
                };
                let request = factory(&body, &self.children[index].generation())?;
                let link = self.children[index]
                    .link
                    .as_ref()
                    .ok_or("original link absent")?;
                let result = NodeClient::new(link)?.send(request).await;
                if let Err(error) = result {
                    self.ledger.lost(index, error);
                }
            }
            Accepted::Status => {
                self.ledger
                    .complete(&op, json!({"op":op,"state":"complete","kind":"status"}));
                let outcome = json!({"op":op,"state":"complete","status":self.status()});
                self.ledger.complete(&op, outcome.clone());
                return Ok(outcome);
            }
            Accepted::Quit => {}
        }
        Ok(self.ledger.result(&op))
    }
    pub async fn update_service(&mut self, index: usize, service: Service) -> Result<()> {
        if self.profile.peer_replicas() {
            return Err("peer-idle service description is fixed for the original lifetime".into());
        }
        if self.ledger.generation_pending() {
            return Err("service update is gated while generation upload is pending".into());
        }
        let placement = self
            .ledger
            .placements
            .get_mut(index)
            .ok_or("node index out of range")?;
        if !placement.link_alive
            || placement.bootstrap != Bootstrap::Admitted
            || placement.service_update.is_some()
        {
            return Err(
                "service update requires current admitted node without pending update".into(),
            );
        }
        let expected = placement.body()?.service;
        if service.name != expected.name
            || service.instance != expected.instance
            || service.stateful != expected.stateful
            || service.persisted
            || service.group
            || service.service_type != expected.service_type
            || service.application != expected.application
            || service.version != expected.version
            || service.target != expected.target
            || service.minimum != expected.minimum
            || service.partitions != expected.partitions
            || service.activation_mode != expected.activation_mode
            || !service.scaling.0.is_empty()
            || service.update_version < expected.update_version
        {
            return Err("service update is outside admitted fixed profile/version".into());
        }
        let request = NodeRequest::node_update_service(
            &NodeUpdateServiceRequest {
                service: service.clone(),
            },
            &self.children[index].generation(),
        )?;
        let link = self.children[index]
            .link
            .as_ref()
            .ok_or("original link absent")?;
        NodeClient::new(link)?.send(request).await?;
        placement.service_update = Some(service);
        placement.service_retry_at = Some(Instant::now() + Duration::from_secs(5));
        Ok(())
    }
}
