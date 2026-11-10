mod fm;
mod interlock;
mod node;
mod profile;
mod wire;

pub use fm::Command;
pub use profile::Result;
pub use profile::ServiceProfile;

use fm::{Accepted, Bootstrap, Ledger};
use fube_local_host::process::OwnedApp;
use fube_ra::protocol::*;
use profile::*;
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::Duration,
};
use tokio::time::Instant;
use wire::Link;

pub struct Config {
    pub profile: ServiceProfile,
    pub root: PathBuf,
    pub app: PathBuf,
    pub executable: PathBuf,
}
struct ChildOwner {
    launch: Launch,
    process: Option<OwnedApp>,
    link: Option<Link>,
    metadata: Option<Metadata>,
}

/// One ephemeral foreground authority; no reconnect, replacement or adoption.
/// Keep this owner through explicit shutdown, including after any error.
pub struct Cluster {
    interlock: interlock::Interlock,
    config: Config,
    ledger: Ledger,
    children: Vec<ChildOwner>,
    peer_idle_requested: bool,
    finished: Option<Value>,
}
impl Cluster {
    pub fn new(mut config: Config) -> Result<Self> {
        config.app = config.app.canonicalize().map_err(err)?;
        config.executable = config.executable.canonicalize().map_err(err)?;
        use std::os::unix::fs::PermissionsExt;
        if !config.app.is_file()
            || fs::metadata(&config.app).map_err(err)?.permissions().mode() & 0o111 == 0
            || fs::metadata(&config.app).map_err(err)?.len() > 256 * 1024 * 1024
            || !config.executable.is_file()
        {
            return Err("fixed app and internal node executable must be executable files".into());
        }
        let interlock = interlock::Interlock::acquire(&config.root)?;
        config.root = interlock.root.clone();
        let fm = NodeInstance {
            id: uuid::Uuid::new_v4().to_bytes_le(),
            instance: sequence(),
        };
        let generation = GenerationHeader {
            number: GenerationNumber {
                owner: fm.id,
                generation: (sequence() & i64::MAX as u64).max(1) as i64,
            },
            for_fm_replica: false,
        };
        let mut launches = [1, 2].map(|placement| Launch {
            profile: config.profile,
            deployment: interlock.deployment.clone(),
            session: uuid::Uuid::new_v4().to_string(),
            capability: uuid::Uuid::new_v4().to_string(),
            parent_pid: std::process::id(),
            node: NodeInstance {
                id: uuid::Uuid::new_v4().to_bytes_le(),
                instance: sequence(),
            },
            fm: fm.clone(),
            generation: generation.clone(),
            placement,
            directory: config
                .root
                .join("deployment")
                .join(format!("node{placement}")),
            package: config.root.join("deployment/package"),
            partition: uuid::Uuid::new_v4().to_string(),
            replica: i64::from(placement),
            incarnation: (sequence() & i64::MAX as u64).max(1) as i64,
            peer: None,
        });
        if config.profile == ServiceProfile::StatefulPeerIdle {
            launches[1].partition = launches[0].partition.clone();
            for index in 0..2 {
                let remote = &launches[1 - index];
                launches[index].peer = Some(Peer {
                    node: remote.node.clone(),
                    session: remote.session.clone(),
                    partition: remote.partition.clone(),
                    replica: remote.replica,
                    incarnation: remote.incarnation,
                });
            }
        }
        Ok(Self {
            interlock,
            config,
            ledger: Ledger::new(launches),
            children: Vec::with_capacity(2),
            finished: None,
            peer_idle_requested: false,
        })
    }
    pub async fn start(&mut self) -> Result<()> {
        if self.finished.is_some() || !self.children.is_empty() {
            return Err("one cluster start per owner".into());
        }
        self.prepare()?;
        for index in 0..2 {
            self.spawn(index).await?;
        }
        if self.config.profile == ServiceProfile::StatefulPeerIdle {
            let address = self.children[1]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.peer_address)
                .ok_or("missing original node2 peer listener")?;
            let child = &mut self.children[0];
            child
                .process
                .as_mut()
                .ok_or("missing original Primary owner")?
                .command(
                    format!(
                        "peer {} {} {address}\n",
                        child.launch.session, child.launch.capability
                    )
                    .as_bytes(),
                )
                .map_err(err)?;
        }
        Ok(())
    }
    fn prepare(&self) -> Result<()> {
        private_dir(&self.config.root.join("deployment"))?;
        self.assemble()
    }
    fn assemble(&self) -> Result<()> {
        let package = self.config.root.join("deployment/package");
        if self.config.profile.stateful() {
            private_dir(&package)?;
            private_dir(&package.join("LocalService"))?;
            private_dir(&package.join("LocalService/Code"))?;
            fs::write(
                package.join("ApplicationManifest.xml"),
                include_bytes!(
                    "../../../samples/hosting-local-app/pkg-stateful/ApplicationManifest.xml"
                ),
            )
            .map_err(err)?;
            fs::write(package.join("LocalService/ServiceManifest.xml"),
                include_bytes!("../../../samples/hosting-local-app/pkg-stateful/LocalService/ServiceManifest.xml")).map_err(err)?;
            fs::copy(
                &self.config.app,
                package.join("LocalService/Code/hosting-local-app"),
            )
            .map_err(err)?;
            return Ok(());
        }
        private_dir(&package)?;
        private_dir(&package.join("ClusterService"))?;
        private_dir(&package.join("ClusterService/Code"))?;
        fs::write(
            package.join("ApplicationManifest.xml"),
            include_bytes!("../../../samples/cluster-stateless-app/pkg/ApplicationManifest.xml"),
        )
        .map_err(err)?;
        fs::write(
            package.join("ClusterService/ServiceManifest.xml"),
            include_bytes!(
                "../../../samples/cluster-stateless-app/pkg/ClusterService/ServiceManifest.xml"
            ),
        )
        .map_err(err)?;
        fs::copy(
            &self.config.app,
            package.join("ClusterService/Code/cluster-stateless-app"),
        )
        .map_err(err)?;
        Ok(())
    }
    async fn spawn(&mut self, index: usize) -> Result<()> {
        let launch = self.ledger.placements[index].launch.clone();
        private_dir(&launch.directory)?;
        private_dir(&launch.directory.join("scratch"))?;
        write_json(&launch.directory.join("launch.json"), &launch)?;
        let mut command = ProcessCommand::new(&self.config.executable);
        command
            .arg("--internal-node")
            .arg(launch.directory.join("launch.json"))
            .env("TMPDIR", launch.directory.join("scratch"))
            .current_dir(&launch.directory);
        self.children.push(ChildOwner {
            launch: launch.clone(),
            process: None,
            link: None,
            metadata: None,
        });
        OwnedApp::start(&mut self.children[index].process, command).map_err(err)?;
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            self.children[index]
                .process
                .as_mut()
                .unwrap()
                .check_alive()
                .map_err(|error| {
                    format!(
                        "{error:?}; logs={:?}",
                        bounded_logs(self.children[index].process.as_ref().unwrap())
                    )
                })?;
            if launch.directory.join("listener.json").exists() {
                let metadata: Metadata = read_json(&launch.directory.join("listener.json"))?;
                self.validate_metadata(index, &metadata, false)?;
                self.children[index].metadata = Some(metadata.clone());
                self.children[index]
                    .process
                    .as_mut()
                    .unwrap()
                    .command(format!("admit {} {}\n", launch.session, launch.capability).as_bytes())
                    .map_err(err)?;
                let channel = fube_transport::Endpoint::unsecured_loopback(
                    metadata.address,
                    wire::composition(true).map_err(err)?,
                )
                .map_err(err)?
                .channel_limits(wire::limits())
                .connect_until(end)
                .await
                .map_err(err)?;
                self.children[index].link = Some(Link::new(
                    channel,
                    launch.fm.clone(),
                    launch.node.clone(),
                    launch.generation.clone(),
                )?);
                break;
            }
            if Instant::now() >= end {
                return Err("original child listener startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        loop {
            self.pump(index).await?;
            self.children[index]
                .process
                .as_mut()
                .unwrap()
                .check_alive()
                .map_err(|error| {
                    format!(
                        "{error:?}; logs={:?}",
                        bounded_logs(self.children[index].process.as_ref().unwrap())
                    )
                })?;
            if self.ledger.placements[index].bootstrap == Bootstrap::Metadata
                && launch.directory.join("profile.json").exists()
            {
                let metadata: Metadata = read_json(&launch.directory.join("profile.json"))?;
                self.validate_metadata(index, &metadata, true)?;
                self.ledger.placements[index]
                    .admit_profile(metadata.profile.clone().ok_or("missing actual profile")?)?;
                self.children[index].metadata = Some(metadata);
                break;
            }
            if Instant::now() >= end {
                return Err("native bootstrap/profile startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }
    fn validate_metadata(&self, index: usize, metadata: &Metadata, profile: bool) -> Result<()> {
        let owner = &self.children[index];
        if metadata.pid
            != owner
                .process
                .as_ref()
                .ok_or("no original child owner")?
                .pid()
            || metadata.deployment != owner.launch.deployment
            || metadata.session != owner.launch.session
            || metadata.node != owner.launch.node
            || !metadata.address.ip().is_loopback()
            || metadata.address.port() == 0
            || profile != metadata.profile.is_some()
            || (owner.launch.profile == ServiceProfile::StatefulPeerIdle)
                != metadata.peer_address.is_some()
            || metadata
                .peer_address
                .is_some_and(|address| !address.ip().is_loopback() || address.port() == 0)
            || owner.metadata.as_ref().is_some_and(|prior| {
                prior.address != metadata.address || prior.peer_address != metadata.peer_address
            })
        {
            return Err(
                "startup metadata not from the exact original expected child/session".into(),
            );
        }
        Ok(())
    }
    async fn pump(&mut self, index: usize) -> Result<()> {
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
    async fn receive_message(
        &mut self,
        index: usize,
        mut incoming: fube_transport::channel::IncomingMessage<
            fube_transport::native_channel::GatewayTarget,
        >,
    ) -> Result<()> {
        let link = self.children[index]
            .link
            .as_ref()
            .ok_or("original node link absent")?;
        wire::predecode(&incoming.message)?;
        let action = incoming.message.properties().map_err(err)?.action;
        let generation = self.children[index].launch.generation.clone();
        match action.as_str() {
            "GenerationUpdateReject" => {
                let current = decode_generation_update_reject(&incoming.message).map_err(err)?;
                if let Some(round) = &mut self.ledger.generation {
                    round.reject(&current)?;
                }
            }
            "ServiceTypeEnabled" | "ServiceTypeDisabled" => {
                let enabled = action == "ServiceTypeEnabled";
                let body = if enabled {
                    decode_service_type_enabled(&incoming.message)
                } else {
                    decode_service_type_disabled(&incoming.message)
                }
                .map_err(err)?;
                let reply = self.ledger.type_status(index, enabled, &body)?;
                let message = if enabled {
                    service_type_enabled_reply_message(&reply, &generation)
                } else {
                    service_type_disabled_reply_message(&reply, &generation)
                }
                .map_err(err)?;
                link.send(message).await?;
            }
            "NodeActivateReply" | "NodeDeactivateReply" => {
                let activate = action == "NodeActivateReply";
                let (body, owner) = if activate {
                    decode_node_activate_reply(&incoming.message)
                } else {
                    decode_node_deactivate_reply(&incoming.message)
                }
                .map_err(err)?;
                if owner != Owner::Fm {
                    return Err("FMM activation unsupported".into());
                }
                self.ledger.node_control_reply(index, activate, body)?;
            }
            "NodeUpdateServiceReply" => {
                let (body, owner) =
                    decode_node_update_service_reply(&incoming.message).map_err(err)?;
                if owner != Owner::Fm {
                    return Err("FMM service update unsupported".into());
                }
                self.ledger.service_reply(index, body)?;
            }
            "ReportLoad" => self
                .ledger
                .load(index, decode_report_load(&incoming.message).map_err(err)?)?,
            "NodeUp" => {
                let body = decode_node_up(&incoming.message).map_err(err)?;
                body.validate_fresh_stateless().map_err(err)?;
                let placement = &mut self.ledger.placements[index];
                let metadata = self.children[index]
                    .metadata
                    .as_ref()
                    .ok_or("missing original listener")?;
                let expected = NodeUp::fresh_stateless(
                    placement.launch.name(),
                    "fube-cluster-local".into(),
                    metadata.address,
                    FabricVersionInstance::native_default(),
                )
                .map_err(err)?;
                if placement.bootstrap != Bootstrap::NodeUp || body != expected {
                    return Err(
                        "unexpected/stale native NodeUp identity or recovery contents".into(),
                    );
                }
                let ack = NodeUpAck::fresh_activated(body.node.version, 1).map_err(err)?;
                placement.activation_sequence = ack.activation_sequence;
                placement.activated = ack.activated;
                let response = node_up_ack_message(&ack, &generation).map_err(err)?;
                let context = incoming
                    .reply
                    .take()
                    .ok_or("NodeUp missing consuming reply authority")?;
                context
                    .reply(link.seal(response)?, wire::deadline())
                    .await
                    .map_err(err)?;
                placement.bootstrap = Bootstrap::FinalUpload;
            }
            "ReplicaUp" => {
                let (body, owner) = decode_replica_up(&incoming.message).map_err(err)?;
                if owner != Owner::Fm {
                    return Err("FMM report unsupported".into());
                }
                if self.ledger.generation.is_some() {
                    if let Some(handoff) = incoming.handoff.as_mut() {
                        handoff.acknowledge();
                    }
                    return Ok(()); // Rebuild cannot acknowledge inventory without LFUM support.
                }
                let response = self.ledger.report(index, &body)?;
                let Some(response) = response else {
                    if let Some(handoff) = incoming.handoff.as_mut() {
                        handoff.acknowledge();
                    }
                    return Ok(()); // Known retired lifetime: no semantic ACK or link loss.
                };
                if !body.replicas.0.is_empty()
                    && self.ledger.placements[index].bootstrap == Bootstrap::Admitted
                {
                    self.ledger.down(index);
                }
                link.send(replica_up_reply_message(&response, &generation).map_err(err)?)
                    .await?;
                if self.ledger.placements[index].bootstrap == Bootstrap::FinalUpload {
                    self.ledger.placements[index].bootstrap = Bootstrap::Metadata;
                }
            }
            "ServiceTypeNotification" => {
                let body = decode_service_type_notification(&incoming.message).map_err(err)?;
                let admitted = self.ledger.placements[index].notification(&body)?;
                if !admitted.infos.0.is_empty() {
                    self.ledger.placements[index].catalog_notified = true;
                }
                link.send(
                    service_type_notification_reply_message(&admitted, &generation).map_err(err)?,
                )
                .await?;
            }
            "AddReplicaReply" => {
                let (body, owner) = decode_add_replica_reply(&incoming.message).map_err(err)?;
                if owner != Owner::Fm || self.config.profile != ServiceProfile::StatefulPeerIdle {
                    return Err("AddReplicaReply requires the original peer-idle FM profile".into());
                }
                self.ledger.idle_reply(index, body)?;
            }
            "RemoveReplicaReply" => {
                let (body, owner) = decode_remove_replica_reply(&incoming.message).map_err(err)?;
                if owner != Owner::Fm || self.config.profile != ServiceProfile::StatefulPeerIdle {
                    return Err(
                        "RemoveReplicaReply requires the original peer-idle FM profile".into(),
                    );
                }
                self.ledger.idle_removal_reply(index, body)?;
            }
            "AddInstanceReply"
            | "AddPrimaryReply"
            | "RemoveInstanceReply"
            | "DeleteReplicaReply" => {
                let removing = matches!(
                    action.as_str(),
                    "RemoveInstanceReply" | "DeleteReplicaReply"
                );
                let primary = self.children[index].launch.profile.stateful();
                let expected_action = match (primary, removing) {
                    (true, true) => "DeleteReplicaReply",
                    (true, false) => "AddPrimaryReply",
                    (false, true) => "RemoveInstanceReply",
                    (false, false) => "AddInstanceReply",
                };
                if action != expected_action {
                    return Err("reply action does not match admitted lifecycle profile".into());
                }
                let (body, owner) = if primary && removing {
                    decode_delete_replica_reply(&incoming.message)
                } else if removing {
                    decode_remove_instance_reply(&incoming.message)
                } else if primary {
                    decode_add_primary_reply(&incoming.message)
                } else {
                    decode_add_instance_reply(&incoming.message)
                }
                .map_err(err)?;
                if owner != Owner::Fm {
                    return Err("FMM semantic reply unsupported".into());
                }
                self.ledger.reply(index, body, removing)?;
            }
            _ => return Err("unsupported native ingress; no successful ACK".into()),
        }
        if let Some(handoff) = incoming.handoff.as_mut() {
            handoff.acknowledge();
        }
        Ok(())
    }
    pub async fn drive(&mut self) {
        for index in 0..self.children.len() {
            if !self.ledger.placements[index].link_alive {
                continue;
            }
            let result = match self.children[index].process.as_mut() {
                Some(process) => process.check_alive().map_err(err),
                None => Err("original child owner absent".into()),
            };
            let result = match result {
                Ok(()) => self.pump(index).await,
                Err(error) => Err(error),
            };
            let result = result.and_then(|()| self.read_retirement(index));
            let result = match result {
                Ok(()) if self.ledger.generation.is_some() => self.retry_generation(index).await,
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
            if let Err(error) = result {
                self.ledger.lost(index, error);
                if let Some(link) = &self.children[index].link {
                    link.channel.dispose();
                }
            }
        }
    }
    /// Begin the bounded native handshake, not a completed FM rebuild. New
    /// mutations remain gated until the future LFUM implementation completes.
    pub fn begin_generation_handshake(&mut self) -> Result<Value> {
        if self.config.profile == ServiceProfile::StatefulPeerIdle {
            return Err("peer-idle profile is bound to the original fixed generation".into());
        }
        if self.finished.is_some() || self.children.len() != 2 {
            return Err("generation handshake requires the original running cluster".into());
        }
        self.ledger.begin_generation()?;
        Ok(self.status()["generation_handshake"].clone())
    }
    async fn retry_generation(&mut self, index: usize) -> Result<()> {
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
                if let Some(message) = link
                    .request_optional(generation_proposal_message(&header).map_err(err)?)
                    .await?
                {
                    wire::predecode(&message)?;
                    let body = decode_generation_proposal_reply(&message).map_err(err)?;
                    self.ledger
                        .generation
                        .as_mut()
                        .ok_or("generation owner lost")?
                        .proposal_reply(index, &body)?;
                }
            }
            fm::GenerationWork::Update(header) => {
                link.send(
                    generation_update_message(
                        &GenerationUpdate {
                            fm_service_epoch: Epoch::default(),
                        },
                        &header,
                    )
                    .map_err(err)?,
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn retry_node_work(&mut self, index: usize) -> Result<()> {
        if self.ledger.generation.is_some() {
            return Ok(());
        }
        let now = Instant::now();
        let control = self.ledger.node_control_retry(index, now);
        let service = self.ledger.service_update_retry(index, now);
        let child = &self.children[index];
        let link = child.link.as_ref().ok_or("original node link absent")?;
        if let Some((activate, body)) = control {
            let message = if activate {
                node_activate_request_message(&body, &child.launch.generation)
            } else {
                node_deactivate_request_message(&body, &child.launch.generation)
            }
            .map_err(err)?;
            link.send(message).await?;
        }
        if let Some(body) = service {
            link.send(
                node_update_service_request_message(&body, &child.launch.generation)
                    .map_err(err)?,
            )
            .await?;
        }
        Ok(())
    }
    async fn retry_primary(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.primary_retry(index, Instant::now())? else {
            return Ok(());
        };
        let child = &self.children[index];
        let message = add_primary_message(&body, &child.launch.generation).map_err(err)?;
        child
            .link
            .as_ref()
            .ok_or("original Primary link absent")?
            .send(message)
            .await
    }
    async fn retry_idle(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.idle_retry(index, Instant::now()) else {
            return Ok(());
        };
        self.send_idle(&body).await
    }
    async fn send_idle(&self, body: &AddReplica) -> Result<()> {
        let child = self
            .children
            .first()
            .ok_or("missing original Primary owner")?;
        child
            .link
            .as_ref()
            .ok_or("original Primary link absent")?
            .send(add_replica_message(body, &child.launch.generation).map_err(err)?)
            .await
    }
    async fn retry_idle_removal(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.idle_removal_retry(index, Instant::now()) else {
            return Ok(());
        };
        self.send_idle_removal(&body).await
    }
    async fn send_idle_removal(&self, body: &RemoveReplica) -> Result<()> {
        let child = self
            .children
            .first()
            .ok_or("missing original Primary owner")?;
        child
            .link
            .as_ref()
            .ok_or("original Primary link absent")?
            .send(remove_replica_message(body, &child.launch.generation).map_err(err)?)
            .await
    }
    async fn retry_deletion(&mut self, index: usize) -> Result<()> {
        if self.ledger.generation.is_some() {
            return Ok(());
        }
        let Some(body) = self.ledger.deletion_retry(index, Instant::now())? else {
            return Ok(());
        };
        let child = &self.children[index];
        let message = delete_replica_message(&body, &child.launch.generation).map_err(err)?;
        child
            .link
            .as_ref()
            .ok_or("original deletion link absent")?
            .send(message)
            .await
    }
    async fn retry_idle_deletion(&mut self, index: usize) -> Result<()> {
        let Some(body) = self.ledger.idle_deletion_retry(index, Instant::now()) else {
            return Ok(());
        };
        self.send_idle_deletion(&body).await
    }
    async fn send_idle_deletion(&self, body: &DeleteReplica) -> Result<()> {
        let child = self
            .children
            .get(1)
            .ok_or("missing original destination owner")?;
        child
            .link
            .as_ref()
            .ok_or("original destination link absent")?
            .send(delete_replica_message(body, &child.launch.generation).map_err(err)?)
            .await
    }
    fn read_retirement(&mut self, index: usize) -> Result<()> {
        let child = &self.children[index];
        let path = child.launch.directory.join("retirement.json");
        let node_pid = child
            .process
            .as_ref()
            .ok_or("original node owner absent")?
            .pid();
        self.ledger.read_retirement(index, &path, node_pid)?;
        self.ledger.read_absence(
            index,
            &child.launch.directory.join("absence.json"),
            node_pid,
        )
    }
    pub fn status(&self) -> Value {
        let mut status = self.ledger.status();
        status["deployment"] = json!(self.interlock.deployment);
        if let Some(nodes) = status["nodes"].as_array_mut() {
            for (index, node) in nodes.iter_mut().enumerate() {
                if let Some(child) = self.children.get(index) {
                    node["node_pid"] = json!(child.process.as_ref().map(OwnedApp::pid));
                    let path = child.launch.directory.join("observation.json");
                    if path.exists() {
                        match read_json::<Value>(&path) {
                            Ok(observation)
                                if observation["deployment"] == child.launch.deployment
                                    && observation["session"] == child.launch.session
                                    && observation["node_pid"]
                                        == json!(child.process.as_ref().map(OwnedApp::pid)) =>
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
        if self.config.profile == ServiceProfile::StatefulPeerIdle {
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
            let built = opened && !removing && source["peer_idle"]["build"]["result"] == 0;
            status["peer_idle"] = json!({
                "requested":self.peer_idle_requested,
                "state": if removed { "removed" } else if removing { "removing" } else if failed { "failed" } else if opened { "open" } else if self.peer_idle_requested { "pending" } else { "absent" },
                "error": if failed { reply["error"].clone() } else { Value::Null },
                "built":built, "serving":false,
                "build":source["peer_idle"]["build"],
                "fm":self.ledger.idle_status(),
                "fm_removal":self.ledger.idle_removal_status(),
                "fm_deletion":self.ledger.idle_deletion_status(),
                "ready":source["peer_idle"]["ready"] == true,
                "removal":source["peer_idle"]["removal"],
                "source":source["peer_idle"], "target":target["local_replica"],
                "peer_link_alive":[source["peer_link_alive"],target["peer_link_alive"]],
                "peer_error":[source["peer_error"],target["peer_error"]],
                "command_error":[source["peer_command_error"],target["peer_command_error"]],
            });
        }
        status
    }
    /// Ask the original Primary RA to open its bound remote Idle. This is not
    /// an FM operation, build completion, or permission to serve.
    pub async fn create_idle_replica(&mut self) -> Result<Value> {
        if self.ledger.idle.is_some() {
            return Err("FM owns this Idle; use add_idle_replica".into());
        }
        if self.config.profile != ServiceProfile::StatefulPeerIdle
            || self.finished.is_some()
            || self.children.len() != 2
            || self.ledger.generation.is_some()
            || self.ledger.placements[0].observed != fm::Observed::Running
            || self.ledger.placements.iter().any(|placement| {
                !placement.link_alive
                    || placement.bootstrap != Bootstrap::Admitted
                    || !placement.activated
            })
        {
            return Err(
                "peer Idle requires the original admitted sessions and Ready node1 Primary".into(),
            );
        }
        let current = self.status()["peer_idle"].clone();
        if matches!(
            current["state"].as_str(),
            Some("failed" | "removing" | "removed")
        ) {
            return Ok(current);
        }
        let child = &mut self.children[0];
        child
            .process
            .as_mut()
            .ok_or("missing original Primary owner")?
            .command(
                format!(
                    "create-idle {} {}\n",
                    child.launch.session, child.launch.capability
                )
                .as_bytes(),
            )
            .map_err(err)?;
        self.peer_idle_requested = true;
        Ok(json!({"state":"pending","kind":"peer_idle_open","built":false,"serving":false}))
    }
    /// Ask the original Primary runtime to build its already opened peer Idle.
    /// Completion proves copy/application ACK, not active configuration or FM AddReplica.
    pub async fn build_idle_replica(&mut self) -> Result<Value> {
        if self.ledger.idle.is_some() {
            return Err("FM owns this Idle's build choreography".into());
        }
        if self.config.profile != ServiceProfile::StatefulPeerIdle
            || self.finished.is_some()
            || self.children.len() != 2
            || self.ledger.generation.is_some()
            || self.ledger.placements[0].observed != fm::Observed::Running
            || self.ledger.placements.iter().any(|placement| {
                !placement.link_alive
                    || placement.bootstrap != Bootstrap::Admitted
                    || !placement.activated
            })
            || self.status()["peer_idle"]["state"] != "open"
        {
            return Err("peer build requires the original Ready Primary and opened Idle".into());
        }
        let current = self.status()["peer_idle"].clone();
        if current["built"] == true {
            return Ok(current);
        }
        let child = &mut self.children[0];
        child
            .process
            .as_mut()
            .ok_or("missing original Primary owner")?
            .command(
                format!(
                    "build-idle {} {}\n",
                    child.launch.session, child.launch.capability
                )
                .as_bytes(),
            )
            .map_err(err)?;
        Ok(json!({"state":"pending","kind":"peer_idle_build","built":false,"serving":false}))
    }
    /// FM AddReplica owns peer creation, RAP build and semantic Ready-Idle completion.
    pub async fn add_idle_replica(&mut self) -> Result<Value> {
        let current = self.ledger.idle_status();
        if self.finished.is_some()
            || self.children.len() != 2
            || self.config.profile != ServiceProfile::StatefulPeerIdle
            || (self.ledger.idle_removal.is_some() && current["state"] != "failed")
            || self.peer_idle_requested && self.ledger.idle.is_none()
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
    /// Remove the original Idle from the Primary's replicator, not its remote
    /// application. Pending calls retry cancellation/cleanup; success is terminal.
    pub async fn remove_idle_replica(&mut self) -> Result<Value> {
        if self.ledger.idle_removal.is_some() {
            return Err("FM owns this removal; use remove_idle_replica_via_fm".into());
        }
        if self.config.profile != ServiceProfile::StatefulPeerIdle
            || self.finished.is_some()
            || self.children.len() != 2
            || self.ledger.generation.is_some()
            || self.ledger.placements[0].observed != fm::Observed::Running
            || self.ledger.placements.iter().any(|placement| {
                !placement.link_alive
                    || placement.bootstrap != Bootstrap::Admitted
                    || !placement.activated
            })
        {
            return Err("peer removal requires the original Ready Primary and opened Idle".into());
        }
        let current = self.status()["peer_idle"].clone();
        if current["state"] == "removed" {
            return Ok(current);
        }
        if !matches!(current["state"].as_str(), Some("open" | "removing")) {
            return Err("peer removal requires the original opened Idle".into());
        }
        let child = &mut self.children[0];
        child
            .process
            .as_mut()
            .ok_or("missing original Primary owner")?
            .command(
                format!(
                    "remove-idle {} {}\n",
                    child.launch.session, child.launch.capability
                )
                .as_bytes(),
            )
            .map_err(err)?;
        if let Some(idle) = &mut self.ledger.idle {
            idle.cancelled = true;
        }
        Ok(json!({"state":"pending","kind":"peer_idle_removal","built":false,"serving":false}))
    }
    /// FM retires pending/failed creation or removes the opened Idle's replication session.
    /// Only RemoveReplicaReply clears pending_remove, never destination deletion.
    pub async fn remove_idle_replica_via_fm(&mut self) -> Result<Value> {
        if self.finished.is_some()
            || self.children.len() != 2
            || self.config.profile != ServiceProfile::StatefulPeerIdle
        {
            return Err("FM RemoveReplica requires the original peer-idle profile".into());
        }
        let current = self.ledger.idle_removal_status();
        if current["state"] == "removed" || current["state"] == "unknown" {
            return Ok(current);
        }
        if self.ledger.idle_removal.is_none()
            && self.status()["peer_idle"]["state"] != "open"
            && !matches!(
                self.ledger.idle_status()["state"].as_str(),
                Some("pending" | "failed")
            )
        {
            return Err(
                "FM removal requires original pending/failed FM creation or an opened Idle".into(),
            );
        }
        let body = self.ledger.request_idle_removal(Instant::now())?;
        if let Err(error) = self.send_idle_removal(&body).await {
            self.ledger.lost(0, error.clone());
            return Err(error);
        }
        Ok(self.ledger.idle_removal_status())
    }
    /// Delete the original abandoned destination independently of Primary Remove.
    /// Cleanup joins the native reply with original disposal or committed
    /// never-created absence proof; neither authorizes a successor lifetime.
    pub async fn delete_idle_replica_via_fm(&mut self) -> Result<Value> {
        if self.finished.is_some()
            || self.children.len() != 2
            || self.config.profile != ServiceProfile::StatefulPeerIdle
        {
            return Err("FM Idle deletion requires the original peer-idle profile".into());
        }
        let current = self.ledger.idle_deletion_status();
        if current["state"] == "deleted" || current["state"] == "unknown" {
            return Ok(current);
        }
        let mut allow_absence = false;
        if self.ledger.idle_deletion.is_none() {
            let status = self.status();
            let target = &status["peer_idle"]["target"];
            allow_absence = target.is_null() && self.ledger.idle_absence_allowed();
            let registered_open = target["phase"] == "Opening"
                && target["registration"].is_object()
                && target["retained_usage"] == 1;
            if target["open"] != true && !registered_open && !allow_absence {
                return Err("FM Idle deletion requires the original opened/registered opening destination or an absent unanswered FM Add".into());
            }
        }
        let body = self
            .ledger
            .request_idle_deletion(Instant::now(), allow_absence)?;
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
                let generation = &self.children[index].launch.generation;
                let message = if activate {
                    node_activate_request_message(&body, generation)
                } else {
                    node_deactivate_request_message(&body, generation)
                }
                .map_err(err)?;
                let result = self.children[index]
                    .link
                    .as_ref()
                    .ok_or("original link absent")?
                    .send(message)
                    .await;
                if let Err(error) = result {
                    self.ledger.lost(index, error);
                }
            }
            Accepted::Duplicate(outcome) => return Ok(outcome),
            Accepted::Create(index, body) => {
                let factory = if self.children[index].launch.profile.stateful() {
                    add_primary_message
                } else {
                    add_instance_message
                };
                let message =
                    factory(&body, &self.children[index].launch.generation).map_err(err)?;
                let result = self.children[index]
                    .link
                    .as_ref()
                    .ok_or("original link absent")?
                    .send(message)
                    .await;
                if let Err(error) = result {
                    self.ledger.lost(index, error);
                }
            }
            Accepted::Delete(index, body) => {
                let factory = if self.children[index].launch.profile.stateful() {
                    delete_replica_message
                } else {
                    remove_instance_message
                };
                let message =
                    factory(&body, &self.children[index].launch.generation).map_err(err)?;
                let result = self.children[index]
                    .link
                    .as_ref()
                    .ok_or("original link absent")?
                    .send(message)
                    .await;
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
        if self.config.profile == ServiceProfile::StatefulPeerIdle {
            return Err("peer-idle service description is fixed for the original lifetime".into());
        }
        if self.ledger.generation.is_some() {
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
        let message = node_update_service_request_message(
            &NodeUpdateServiceRequest {
                service: service.clone(),
            },
            &placement.launch.generation,
        )
        .map_err(err)?;
        self.children[index]
            .link
            .as_ref()
            .ok_or("original link absent")?
            .send(message)
            .await?;
        placement.service_update = Some(service);
        placement.service_retry_at = Some(Instant::now() + Duration::from_secs(5));
        Ok(())
    }
    pub async fn shutdown(&mut self) -> Value {
        if let Some(receipt) = &self.finished {
            return receipt.clone();
        }
        let mut receipts = vec![];
        let mut verified = vec![];
        let mut complete = true;
        // Stop all original owners first, so one slow/lost sibling cannot hold
        // the other app alive while its bounded teardown is pending.
        for child in &mut self.children {
            if let Some(process) = child.process.as_mut() {
                let stop =
                    process.command(format!("stop {}\n", child.launch.capability).as_bytes());
                if let Err(error) = stop {
                    receipts.push(json!({"node":child.launch.placement,"stop_error":err(error)}));
                }
            }
        }
        for child in &mut self.children {
            let mut item = json!({"node":child.launch.placement,"session":child.launch.session});
            if let Some(link) = child.link.take() {
                if let Err(error) = link.close().await {
                    item["link_close_error"] = json!(error);
                }
            }
            let Some(process) = child.process.as_mut() else {
                continue;
            };
            let exit = process.wait_exit(std::time::Instant::now() + Duration::from_secs(25));
            if let Err(error) = exit {
                item["exit_error"] = json!(err(error));
            }
            let cleanup = process.cleanup();
            let physical = process.status();
            let receipt = read_json::<CleanupReceipt>(&child.launch.directory.join("cleanup.json"));
            let valid = receipt.as_ref().is_ok_and(|receipt| {
                receipt.deployment == child.launch.deployment
                    && receipt.session == child.launch.session
                    && receipt.node == child.launch.node
                    && receipt.node_pid == process.pid()
                    && (if receipt.activation_acquired {
                        receipt.original_activation_cleaned
                            && receipt.executor_closed
                            && receipt.store_closed
                    } else {
                        !receipt.original_activation_cleaned
                            && !receipt.executor_closed
                            && !receipt.store_closed
                            && receipt.app_pid.is_none()
                            && receipt.runtime_root.is_none()
                            && !child
                                .launch
                                .directory
                                .join("activation-acquired.json")
                                .exists()
                            && child
                                .metadata
                                .as_ref()
                                .is_none_or(|metadata| metadata.profile.is_none())
                    })
                    && physical.reaped
                    && physical.group_drained
                    && physical.exit_observed
                    && child
                        .metadata
                        .as_ref()
                        .and_then(|m| m.profile.as_ref())
                        .is_none_or(|profile| {
                            receipt.app_pid == Some(profile.app_pid)
                                && receipt.runtime_root.as_ref() == Some(&profile.runtime_root)
                        })
                    && receipt
                        .app_pid
                        .is_none_or(|pid| !Path::new(&format!("/proc/{pid}")).exists())
                    && receipt.runtime_root.as_ref().is_none_or(|path| {
                        path.starts_with(child.launch.directory.join("hosting")) && !path.exists()
                    })
            });
            if !valid || cleanup.is_err() {
                complete = false;
            }
            if valid {
                verified.push(receipt.as_ref().unwrap().clone());
            }
            item["original_cleanup_receipt_valid"] = json!(valid);
            item["receipt"] = match receipt {
                Ok(value) => json!(value),
                Err(error) => json!({"error":error}),
            };
            item["owned_process_cleanup"] = match cleanup {
                Ok(value) => value,
                Err(error) => json!({"error":err(error)}),
            };
            item["bounded_node_logs"] = json!(bounded_logs(process));
            receipts.push(item);
        }
        let tree = self.config.root.join("deployment");
        if complete && tree.exists() {
            if let Err(error) = fs::remove_dir_all(&tree) {
                complete = false;
                receipts.push(json!({"runtime_tree_error":err(error)}));
            }
        }
        if complete {
            if let Err(error) = self.interlock.publish_clean(&verified) {
                complete = false;
                receipts.push(json!({"handoff_error":error}));
            }
        }
        let result = json!({"event":"shutdown","deployment":self.interlock.deployment,
            "clean":complete,"nodes":receipts,"profile_root":self.config.root});
        self.finished = Some(result.clone());
        result
    }
}
fn sequence() -> u64 {
    (uuid::Uuid::new_v4().as_u128() as u64).max(1)
}
fn bounded_logs(process: &OwnedApp) -> Vec<String> {
    process
        .logs()
        .into_iter()
        .rev()
        .take(8)
        .map(|line| line.chars().take(256).collect())
        .collect()
}

pub async fn internal_node(config: &Path) -> Result<()> {
    node::run(config).await
}

#[cfg(test)]
mod tests {
    include!("tests.rs");

    #[tokio::test]
    async fn report_retry_owns_current_stage_until_real_ra_clearance() {
        let fixture = Fixture::new("report-retry");
        let mut cluster = Cluster::new(fixture.config()).unwrap();
        cluster.prepare().unwrap();
        let result = node::report_tests::run(&cluster.ledger.placements[0].launch)
            .await
            .unwrap();
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap();
    }

    #[tokio::test]
    async fn retired_requests_and_report_ack_cross_original_channel_without_touching_successor() {
        let fixture = Fixture::new("retired-replay");
        let mut cluster = Cluster::new(fixture.config()).unwrap();
        cluster.prepare().unwrap();
        let result = node::report_tests::run_reuse(&cluster.ledger.placements[0].launch)
            .await
            .unwrap();
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap();
    }

    mod reply_regressions {
        use super::*;

        fn failed(mut reply: AddInstanceReply) -> AddInstanceReply {
            reply.error = -7;
            reply
        }

        fn disposed(mut reply: AddInstanceReply) -> AddInstanceReply {
            reply.replica.state = 3;
            reply.replica.up = false;
            reply.replica.version = VersionInstance::invalid();
            reply.replica.service_endpoint.clear();
            reply
        }

        #[test]
        fn terminal_create_and_delete_results_reject_same_identity_rewrites() {
            let mut ledger = ledger();
            ledger.accept(command("create", "create", Some(1))).unwrap();
            let ready = ready_reply(&ledger.placements[0]);
            ledger.reply(0, failed(ready.clone()), false).unwrap();
            let terminal = ledger.status();
            for reply in [failed(ready.clone()), ready.clone()] {
                ledger.reply(0, reply, false).unwrap();
                assert_eq!(ledger.status(), terminal);
            }
            assert_eq!(
                ledger.result("create"),
                json!({
                    "op":"create", "state":"error", "native_error":-7,
                    "replica":1, "incarnation":801
                })
            );
            assert_eq!(ledger.placements[0].observed, Observed::Error);
            assert_eq!(ledger.placements[0].endpoint, None);
            assert_eq!(
                ledger.placements[0].error.as_deref(),
                Some("native semantic error -7")
            );

            ledger.accept(command("delete", "delete", Some(1))).unwrap();
            let removed = disposed(ready.clone());
            ledger.reply(0, removed.clone(), true).unwrap();
            ledger
                .retirement(0, retirement(&ledger.placements[0]), 555)
                .unwrap();
            let terminal = ledger.status();
            for (reply, removing) in [
                (ready.clone(), false),
                (failed(ready.clone()), false),
                (removed, true),
                (failed(ready), true),
            ] {
                ledger.reply(0, reply, removing).unwrap();
                assert_eq!(ledger.status(), terminal);
            }
            assert_eq!(ledger.placements[0].observed, Observed::Absent);
            assert_eq!(ledger.placements[0].endpoint, None);
            assert_eq!(ledger.placements[0].error, None);
            assert_eq!(
                ledger.result("delete"),
                json!({
                    "op":"delete", "state":"complete", "observed":"absent",
                    "replica":1, "incarnation":801, "cleanup":"verified_original_instance"
                })
            );

            ledger
                .accept(command("create2", "create", Some(2)))
                .unwrap();
            let ready = ready_reply(&ledger.placements[1]);
            ledger.reply(1, ready.clone(), false).unwrap();
            ledger
                .accept(command("delete2", "delete", Some(2)))
                .unwrap();
            ledger.reply(1, failed(ready.clone()), true).unwrap();
            let terminal = ledger.status();
            for reply in [failed(ready.clone()), disposed(ready)] {
                ledger.reply(1, reply, true).unwrap();
                assert_eq!(ledger.status(), terminal);
            }
            assert_eq!(
                ledger.result("delete2"),
                json!({
                    "op":"delete2", "state":"error", "native_error":-7,
                    "replica":2, "incarnation":802
                })
            );
            assert_eq!(ledger.placements[1].observed, Observed::Error);
            assert_eq!(ledger.placements[1].endpoint, None);
        }

        #[test]
        fn delete_supersedes_pending_create_but_only_its_own_reply_settles() {
            let mut ledger = ledger();
            ledger.accept(command("create", "create", Some(1))).unwrap();
            let ready = ready_reply(&ledger.placements[0]);
            ledger.accept(command("delete", "delete", Some(1))).unwrap();
            let pending = ledger.status();
            for reply in [ready.clone(), failed(ready.clone())] {
                ledger.reply(0, reply, false).unwrap();
                assert_eq!(ledger.status(), pending);
            }
            assert_eq!(ledger.result("create")["state"], "unknown");
            assert_eq!(ledger.result("delete")["state"], "pending");
            ledger.reply(0, disposed(ready), true).unwrap();
            assert_eq!(ledger.result("delete")["state"], "pending");
            assert!(ledger
                .accept(command("before-proof", "create", Some(1)))
                .is_err());
            ledger
                .retirement(0, retirement(&ledger.placements[0]), 555)
                .unwrap();
            assert_eq!(ledger.placements[0].observed, Observed::Absent);
            assert_eq!(ledger.result("delete")["state"], "complete");
        }

        #[test]
        fn admitted_down_revokes_add_completion_and_retained_serving_observation() {
            for already_ready in [false, true] {
                let mut ledger = ledger();
                ledger.accept(command("create", "create", Some(1))).unwrap();
                let ready = ready_reply(&ledger.placements[0]);
                if already_ready {
                    ledger.reply(0, ready.clone(), false).unwrap();
                    let settled = ledger.status();
                    for reply in [ready.clone(), failed(ready.clone())] {
                        ledger.reply(0, reply, false).unwrap();
                        assert_eq!(ledger.status(), settled);
                    }
                }
                let body = ledger.placements[0].body().unwrap();
                let report = ReplicaUp {
                    replicas: ObjectArray(vec![FailoverUnitInfo {
                        unit: body.unit,
                        service: body.service,
                        replicas: ObjectArray(vec![ReplicaInfo {
                            description: Replica {
                                up: false,
                                state: 2,
                                version: VersionInstance::invalid(),
                                ..body.replica
                            },
                            ..ReplicaInfo::default()
                        }]),
                        ..FailoverUnitInfo::default()
                    }]),
                    ..ReplicaUp::default()
                };
                ledger.placements[0].report(&report).unwrap();
                ledger.down(0);
                let down = ledger.status();
                for reply in [ready.clone(), failed(ready.clone())] {
                    ledger.reply(0, reply, false).unwrap();
                    assert_eq!(ledger.status(), down);
                }
                assert_eq!(ledger.placements[0].desired, Desired::Running);
                assert_eq!(ledger.placements[0].observed, Observed::Unknown);
                assert_eq!(ledger.placements[0].endpoint, None);
                assert_eq!(
                    ledger.placements[0].error.as_deref(),
                    Some("native report says original replica is down; replacement forbidden")
                );
                assert_eq!(ledger.result("create")["state"], "unknown");
                assert!(
                    matches!(ledger.accept(command("create", "create", Some(1))).unwrap(),
                    Accepted::Duplicate(outcome) if outcome["state"] == "unknown")
                );
                ledger.accept(command("delete", "delete", Some(1))).unwrap();
                ledger.down(0);
                ledger.reply(0, disposed(ready), true).unwrap();
                ledger
                    .retirement(0, retirement(&ledger.placements[0]), 555)
                    .unwrap();
                assert_eq!(ledger.placements[0].observed, Observed::Absent);
                assert_eq!(ledger.result("delete")["state"], "complete");
                let absent = ledger.status();
                ledger.down(0);
                assert_eq!(ledger.status(), absent);
            }
        }
    }
}
