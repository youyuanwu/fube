mod fm;
mod interlock;
mod management;
mod manager;
mod node;
mod process_control;
mod profile;
mod restart;
mod scenario;
pub(crate) use fube_node::control::wire;

pub use fm::Command;
pub use management::run as run_fm;
use manager::Manager;
pub use profile::Result;
pub use profile::ServiceProfile;
pub use scenario::{RecoveryMode, ReplicaRef};

use fm::Bootstrap;
#[cfg(test)]
use fm::{Accepted, Ledger};
use fube_local_host::process::OwnedApp;
use fube_ra::protocol::*;
use process_control::NodeControl;
use profile::*;
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::Duration,
};
use tokio::time::Instant;

pub struct Config {
    pub profile: ServiceProfile,
    pub root: PathBuf,
    pub app: PathBuf,
    pub executable: PathBuf,
    pub remote_replica_progress_query_wait_ms: u32,
}
/// Local process supervisor. Native FM state and Channels belong to the manager.
pub struct Cluster {
    interlock: interlock::Interlock,
    config: Config,
    manager: manager::Manager,
    processes: Vec<Option<OwnedApp>>,
    remote: Option<management::Client>,
    fenced_nodes: Vec<NodeDownReceipt>,
    driver_commands: Vec<(Command, Option<Result<Value>>)>,
}
macro_rules! fm_action {
    ($name:ident, $operation:ident) => {
        pub async fn $name(&mut self) -> Result<Value> {
            if let Some(remote) = &mut self.remote {
                remote.request(management::Operation::$operation).await
            } else {
                self.manager.$name().await
            }
        }
    };
    ($name:ident, $operation:ident, node) => {
        pub async fn $name(&mut self, node: u8) -> Result<Value> {
            if let Some(remote) = &mut self.remote {
                remote
                    .request(management::Operation::$operation { node })
                    .await
            } else {
                self.manager.$name(node).await
            }
        }
    };
}
#[cfg(test)]
impl std::ops::Deref for Cluster {
    type Target = manager::Manager;
    fn deref(&self) -> &Self::Target {
        &self.manager
    }
}
#[cfg(test)]
impl std::ops::DerefMut for Cluster {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.manager
    }
}
impl Cluster {
    fm_action!(repair_primary_replica, RepairPrimary);
    fm_action!(repair_secondary_replica, RepairSecondary, node);
    fm_action!(cancel_repair_build, CancelRepair);
    fm_action!(delete_cancelled_repair, DeleteCancelledRepair);
    fm_action!(replace_replica_on, ReplaceReplica, node);
    fm_action!(add_idle_replica, AddIdle);
    fm_action!(add_idle_replica_on, AddIdleOn, node);
    fm_action!(replace_idle_replica, ReplaceIdle);
    fm_action!(activate_idle_replica, ActivateIdle);
    fm_action!(transfer_primary, TransferPrimary);
    fm_action!(transfer_primary_to, TransferPrimaryTo, node);
    fm_action!(retire_secondary_replica, RetireSecondary);
    fm_action!(remove_idle_replica_via_fm, RemoveIdle);

    pub async fn delete_idle_replica_via_fm(&mut self) -> Result<Value> {
        self.delete_idle_replica_via_fm_with_force(false).await
    }

    pub async fn delete_idle_replica_via_fm_with_force(&mut self, force: bool) -> Result<Value> {
        if let Some(remote) = &mut self.remote {
            remote
                .request(management::Operation::DeleteIdle { force })
                .await
        } else {
            self.manager
                .delete_idle_replica_via_fm_with_force(force)
                .await
        }
    }

    pub fn begin_generation_handshake(&mut self) -> Result<Value> {
        if self.remote.is_some() {
            return Err("separate FM requires async begin_generation_round".into());
        }
        self.manager.begin_generation_handshake()
    }

    pub async fn begin_generation_round(&mut self) -> Result<Value> {
        if let Some(remote) = &mut self.remote {
            remote.request(management::Operation::Generation).await
        } else {
            self.manager.begin_generation_handshake()
        }
    }

    pub fn status(&self) -> Value {
        let mut status = self
            .remote
            .as_ref()
            .map_or_else(|| self.manager.status(), management::Client::status);
        status["node_controls"] = json!(self
            .driver_commands
            .iter()
            .map(|(command, result)| {
                match result {
                    Some(Ok(value)) => value.clone(),
                    Some(Err(error)) => json!({"op":command.op,"state":"rejected","error":error}),
                    None => json!({"op":command.op,"state":"unknown","error":"Node control has no observed terminal outcome"}),
                }
            })
            .collect::<Vec<_>>());
        status
    }

    pub async fn execute(&mut self, command: Command) -> Result<Value> {
        if self.manager.finished.is_some() {
            return Err("deployment already shut down".into());
        }
        self.refresh_node_controls();
        command.validate()?;
        if let Some((original, result)) = self
            .driver_commands
            .iter()
            .find(|(old, _)| old.op == command.op)
        {
            if original != &command {
                return Err("operation identity reused with different canonical content".into());
            }
            if let Some(result) = result {
                return result.clone();
            }
            return Err(
                "interrupted Node control cannot be replayed; inspect current ownership".into(),
            );
        }
        if command.supervisor_control() {
            if self.driver_commands.len() == 64 {
                return Err("64 driver operation identities retained; capacity exhausted".into());
            }
            let status = self
                .remote
                .as_ref()
                .map_or_else(|| self.manager.status(), management::Client::status);
            if status["operations"].as_array().is_some_and(|operations| {
                operations
                    .iter()
                    .any(|operation| operation["op"] == command.op)
            }) {
                return Err("operation identity is already owned by an FM command".into());
            }
            self.driver_commands.push((command.clone(), None));
            let node = command.node.unwrap();
            let result = match command.action.as_str() {
                "node-down" => self.node_down(node).await,
                "stop-node" => self.terminate_node_owned(node, false).await,
                "kill-node" => self.terminate_node_owned(node, true).await,
                "restart-node" if self.manager.ledger.recovery_mode == RecoveryMode::Manual => {
                    self.restart_node_infrastructure(node).await
                }
                "restart-node" => self.restart_node(node).await,
                _ => unreachable!(),
            }
            .map(|mut result| {
                result["op"] = json!(command.op);
                result
            });
            self.driver_commands
                .iter_mut()
                .find(|(old, _)| old.op == command.op)
                .unwrap()
                .1 = Some(result.clone());
            return result;
        }
        self.execute_fm_command(command).await
    }

    pub(crate) async fn execute_fm_command(&mut self, command: Command) -> Result<Value> {
        if self.manager.finished.is_some() {
            return Err("deployment already shut down".into());
        }
        command.validate()?;
        if self
            .driver_commands
            .iter()
            .any(|(original, _)| original.op == command.op)
        {
            return Err("operation identity is already owned by a Node control".into());
        }
        if let Some(remote) = &mut self.remote {
            let mut outcome = remote
                .request(management::Operation::Command { command })
                .await?;
            if outcome.get("status").is_some() {
                outcome["status"] = remote.status();
            }
            Ok(outcome)
        } else {
            self.manager.execute(command).await
        }
    }

    pub async fn drive(&mut self) {
        self.observe_node_owners();
        if let Some(remote) = &mut self.remote {
            // A failed exchange is retained as control uncertainty in every status.
            let _retained_result = remote.request(management::Operation::Status).await;
        } else {
            self.manager.drive().await;
        }
        self.refresh_node_controls();
    }

    pub(crate) fn observe_node_owners(&mut self) {
        for (index, process) in self.processes.iter_mut().enumerate() {
            if self.manager.children.get(index).is_some_and(|child| {
                self.fenced_nodes
                    .iter()
                    .any(|proof| proof.node == child.launch.node)
            }) {
                continue;
            }
            if let Some(process) = process {
                if let Err(error) = process.check_alive() {
                    self.manager.ledger.lost(index, err(error));
                }
            }
        }
    }

    pub async fn update_service(&mut self, index: usize, service: Service) -> Result<()> {
        if let Some(remote) = &mut self.remote {
            remote
                .request(management::Operation::UpdateService { index, service })
                .await?;
            Ok(())
        } else {
            self.manager.update_service(index, service).await
        }
    }

    pub fn new(config: Config) -> Result<Self> {
        let node_count = config.profile.node_count();
        Self::with_node_pool(config, node_count)
    }

    pub fn with_node_pool(mut config: Config, node_count: usize) -> Result<Self> {
        config
            .profile
            .control_profile()
            .validate_node_pool(node_count)?;
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
        let mut launches: Vec<_> = (1..=node_count as u8)
            .map(|placement| Launch {
                profile: config.profile,
                remote_replica_progress_query_wait_ms: config.remote_replica_progress_query_wait_ms,
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
                peers: vec![],
            })
            .collect();
        if config.profile.peer_replicas() || config.profile.configured_sizes().is_some() {
            let partition = launches[0].partition.clone();
            for launch in &mut launches {
                launch.partition = partition.clone();
            }
            for index in 0..launches.len() {
                let peers = launches
                    .iter()
                    .enumerate()
                    .filter(|(other, _)| {
                        if config.profile.mesh_peers() {
                            *other != index
                        } else if index == 0 {
                            *other != 0
                        } else {
                            *other == 0
                        }
                    })
                    .map(|(_, remote)| Peer {
                        node: remote.node.clone(),
                        session: remote.session.clone(),
                        partition: remote.partition.clone(),
                        replica: remote.replica,
                        incarnation: remote.incarnation,
                    })
                    .collect();
                launches[index].peers = peers;
            }
        }
        let manager = Manager::new(launches)?;
        Ok(Self {
            interlock,
            config,
            manager,
            processes: Vec::with_capacity(node_count),
            remote: None,
            fenced_nodes: Vec::new(),
            driver_commands: Vec::new(),
        })
    }
    pub async fn start(&mut self) -> Result<()> {
        self.start_owner(None).await
    }

    /// Run FM in an independently owned executable; this supervisor alone spawns Nodes.
    pub async fn start_with_fm(&mut self, executable: &Path) -> Result<()> {
        self.start_owner(Some(executable)).await
    }

    async fn start_owner(&mut self, executable: Option<&Path>) -> Result<()> {
        if self.manager.finished.is_some() || !self.manager.children.is_empty() {
            return Err("one cluster start per owner".into());
        }
        self.prepare()?;
        if let Some(executable) = executable {
            let executable = executable.canonicalize().map_err(err)?;
            let launches: Vec<_> = self
                .manager
                .ledger
                .placements
                .iter()
                .map(|placement| placement.launch.directory.join("launch.json"))
                .collect();
            for placement in &self.manager.ledger.placements {
                private_dir(&placement.launch.directory)?;
                private_dir(&placement.launch.directory.join("scratch"))?;
                write_json(
                    &placement.launch.directory.join("launch.json"),
                    &placement.launch,
                )?;
            }
            let path = self.config.root.join("deployment/fm.json");
            write_json(
                &path,
                &management::Manifest {
                    schema_version: 1,
                    launches,
                    recovery_mode: self.manager.ledger.recovery_mode,
                },
            )?;
            self.remote = Some(management::Client::new());
            self.remote
                .as_mut()
                .expect("stored FM owner")
                .start(&executable, &path)
                .await?;
        }
        for index in 0..self.manager.ledger.placements.len() {
            self.spawn(index).await?;
        }
        if self.config.profile.peer_replicas() || self.config.profile.configured_sizes().is_some() {
            for source in 0..self.manager.children.len() {
                for index in source + 1..self.manager.children.len() {
                    if (!self.config.profile.peer_replicas()
                        && self.config.profile.configured_sizes().is_none())
                        || (source != 0 && !self.config.profile.mesh_peers())
                    {
                        continue;
                    }
                    let source_node = &self.manager.children[source].launch.node;
                    let incoming_slot = self.manager.children[index]
                        .launch
                        .peers
                        .iter()
                        .position(|p| &p.node == source_node)
                        .ok_or("missing original incoming mesh binding")?;
                    let address = self.manager.children[index]
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.peer_addresses.get(incoming_slot))
                        .copied()
                        .ok_or("missing original peer-specific listener")?;
                    let target = &self.manager.children[index].launch.node;
                    let peer_index = self.manager.children[source]
                        .launch
                        .peers
                        .iter()
                        .position(|p| &p.node == target)
                        .ok_or("missing original mesh peer")?;
                    let child = &self.manager.children[source];
                    process_control::send(
                        self.processes[source]
                            .as_mut()
                            .ok_or("missing original Primary owner")?,
                        &child.launch,
                        NodeControl::DialPeer {
                            slot: peer_index,
                            address,
                        },
                    )?;
                }
            }
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
        let launch = self.manager.ledger.placements[index].launch.clone();
        if self.remote.is_none() {
            private_dir(&launch.directory)?;
            private_dir(&launch.directory.join("scratch"))?;
            write_json(&launch.directory.join("launch.json"), &launch)?;
        }
        let mut command = ProcessCommand::new(&self.config.executable);
        command
            .arg("--internal-node")
            .arg(launch.directory.join("launch.json"))
            .env("TMPDIR", launch.directory.join("scratch"))
            .current_dir(&launch.directory);
        if index == self.processes.len() {
            self.processes.push(None);
            self.manager.children.push(manager::Connection {
                launch: launch.clone(),
                node_pid: 0,
                link: None,
                metadata: None,
            });
        } else if self.processes[index].is_some() {
            return Err("restart process slot still owns a predecessor".into());
        }
        let started = OwnedApp::start(&mut self.processes[index], command).map_err(err);
        if let Some(process) = &self.processes[index] {
            self.manager.children[index].node_pid = process.pid();
        }
        started?;
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            self.processes[index]
                .as_mut()
                .unwrap()
                .check_alive()
                .map_err(|error| {
                    format!(
                        "{error:?}; logs={:?}",
                        bounded_logs(self.processes[index].as_ref().unwrap())
                    )
                })?;
            if launch.directory.join("listener.json").exists() {
                let metadata: Metadata = read_json(&launch.directory.join("listener.json"))?;
                let pid = self.processes[index].as_ref().expect("stored Node").pid();
                if metadata.pid != pid
                    || metadata.session != launch.session
                    || metadata.deployment != launch.deployment
                    || metadata.node != launch.node
                {
                    return Err("listener metadata is not the original owned Node".into());
                }
                process_control::send(
                    self.processes[index].as_mut().unwrap(),
                    &launch,
                    NodeControl::Admit {},
                )?;
                if let Some(remote) = &mut self.remote {
                    let attached = remote
                        .request(management::Operation::Attach { index, metadata })
                        .await?;
                    let attached: Metadata = serde_json::from_value(attached).map_err(err)?;
                    self.manager.validate_metadata(index, &attached, true)?;
                    self.manager.children[index].metadata = Some(attached);
                } else {
                    self.manager.attach(index, metadata).await?;
                }
                break;
            }
            if Instant::now() >= end {
                return Err("original child listener startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }
    /// Ask the original Primary RA to open its bound remote Idle. This is not
    /// an FM operation, build completion, or permission to serve.
    pub async fn create_idle_replica(&mut self) -> Result<Value> {
        if self.remote.is_some() {
            return Err("direct peer test controls are not FM management operations".into());
        }
        if !self.manager.ledger.idles.is_empty() {
            return Err("FM owns this Idle; use add_idle_replica".into());
        }
        if self.config.profile != ServiceProfile::StatefulPeerIdle
            || self.manager.finished.is_some()
            || self.manager.children.len() != 2
            || self.manager.ledger.generation.is_some()
            || self.manager.ledger.placements[0].observed != fm::Observed::Running
            || self.manager.ledger.placements.iter().any(|placement| {
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
        let child = &self.manager.children[0];
        process_control::send(
            self.processes[0]
                .as_mut()
                .ok_or("missing original Primary owner")?,
            &child.launch,
            NodeControl::CreateIdle {},
        )?;
        self.manager.peer_idle_requested = true;
        Ok(json!({"state":"pending","kind":"peer_idle_open","built":false,"serving":false}))
    }
    /// Ask the original Primary runtime to build its already opened peer Idle.
    /// Completion proves copy/application ACK, not active configuration or FM AddReplica.
    pub async fn build_idle_replica(&mut self) -> Result<Value> {
        if self.remote.is_some() {
            return Err("direct runtime test controls are not FM management operations".into());
        }
        if !self.manager.ledger.idles.is_empty() {
            return Err("FM owns this Idle's build choreography".into());
        }
        if self.config.profile != ServiceProfile::StatefulPeerIdle
            || self.manager.finished.is_some()
            || self.manager.children.len() != 2
            || self.manager.ledger.generation.is_some()
            || self.manager.ledger.placements[0].observed != fm::Observed::Running
            || self.manager.ledger.placements.iter().any(|placement| {
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
        let child = &self.manager.children[0];
        process_control::send(
            self.processes[0]
                .as_mut()
                .ok_or("missing original Primary owner")?,
            &child.launch,
            NodeControl::BuildIdle {},
        )?;
        Ok(json!({"state":"pending","kind":"peer_idle_build","built":false,"serving":false}))
    }
    /// Remove the original Idle from the Primary's replicator, not its remote
    /// application. Pending calls retry cancellation/cleanup; success is terminal.
    pub async fn remove_idle_replica(&mut self) -> Result<Value> {
        if self.remote.is_some() {
            return Err("direct runtime test controls are not FM management operations".into());
        }
        if self.manager.ledger.idle_removal.is_some() {
            return Err("FM owns this removal; use remove_idle_replica_via_fm".into());
        }
        if self.config.profile != ServiceProfile::StatefulPeerIdle
            || self.manager.finished.is_some()
            || self.manager.children.len() != 2
            || self.manager.ledger.generation.is_some()
            || self.manager.ledger.placements[0].observed != fm::Observed::Running
            || self.manager.ledger.placements.iter().any(|placement| {
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
        let child = &self.manager.children[0];
        process_control::send(
            self.processes[0]
                .as_mut()
                .ok_or("missing original Primary owner")?,
            &child.launch,
            NodeControl::RemoveIdle {},
        )?;
        if let Some(idle) = self.manager.ledger.idles.first_mut() {
            idle.cancelled = true;
        }
        Ok(json!({"state":"pending","kind":"peer_idle_removal","built":false,"serving":false}))
    }
    pub async fn shutdown(&mut self) -> Value {
        if let Some(receipt) = &self.manager.finished {
            return receipt.clone();
        }
        let mut receipts = vec![];
        let mut verified = vec![];
        let mut complete = true;
        let mut host_cleanup = self.manager.host_cleanup_owners();
        if let Some(remote) = &mut self.remote {
            match remote.stop().await {
                Ok(state) if state.metadata.len() == self.manager.children.len() => {
                    host_cleanup = state.hosts;
                    for (child, metadata) in self.manager.children.iter_mut().zip(state.metadata) {
                        if let Some(metadata) = metadata {
                            if metadata.node != child.launch.node
                                || metadata.pid != child.node_pid
                                || metadata.deployment != child.launch.deployment
                                || metadata.session != child.launch.session
                            {
                                complete = false;
                                receipts.push(json!({"fm_error":"invalid final Node metadata"}));
                            } else {
                                child.metadata = Some(metadata);
                            }
                        }
                    }
                }
                Ok(_) => {
                    complete = false;
                    receipts.push(json!({"fm_error":"incomplete final Node metadata"}));
                }
                Err(error) => {
                    complete = false;
                    receipts.push(json!({"fm_error":error}));
                }
            }
        }
        // FM is no longer issuing lifecycle intent. Stop all original Nodes
        // before waiting for any sibling's independently owned cleanup.
        for (child, process) in self.manager.children.iter().zip(&mut self.processes) {
            if let Some(process) = process.as_mut() {
                let stop = process_control::send(process, &child.launch, NodeControl::Stop {});
                if let Err(error) = stop {
                    receipts.push(json!({"node":child.launch.placement,"stop_error":error}));
                }
            }
        }
        for (index, child) in self.manager.children.iter_mut().enumerate() {
            let mut item = json!({"node":child.launch.placement,"session":child.launch.session});
            if let Some(link) = child.link.take() {
                if let Err(error) = link.close().await {
                    item["link_close_error"] = json!(error);
                }
            }
            let Some(process) = self.processes.get_mut(index).and_then(Option::as_mut) else {
                continue;
            };
            if let Some(proof) = self
                .fenced_nodes
                .iter()
                .find(|proof| proof.node == child.launch.node)
            {
                let cleanup = process.cleanup();
                let physical = process.status();
                let valid = proof
                    .validate(
                        &child.launch,
                        process.pid(),
                        child
                            .metadata
                            .as_ref()
                            .and_then(|metadata| metadata.profile.as_ref()),
                    )
                    .is_ok()
                    && physical.reaped
                    && physical.group_drained
                    && physical.exit_observed
                    && restart::verify_fencing(proof).is_ok();
                if !valid || cleanup.is_err() {
                    complete = false;
                }
                item["node_down_fencing_valid"] = json!(valid);
                item["node_down_fencing"] = json!(proof);
                item["owned_process_cleanup"] = match cleanup {
                    Ok(value) => value,
                    Err(error) => json!({"error":err(error)}),
                };
                receipts.push(item);
                continue;
            }
            let exit = process.wait_exit(std::time::Instant::now() + Duration::from_secs(25));
            if let Err(error) = exit {
                item["exit_error"] = json!(err(error));
            }
            let cleanup = process.cleanup();
            let physical = process.status();
            let receipt = read_json::<CleanupReceipt>(&child.launch.directory.join("cleanup.json"));
            for repair in host_cleanup.iter().filter(|owner| owner.node == index) {
                let path = child
                    .launch
                    .host_replacement_path(repair.replica, repair.incarnation);
                if path.exists() {
                    let replacement = read_json::<HostReplacementReceipt>(&path).and_then(|r| {
                        r.validate(&child.launch, &repair.old_profile, process.pid())?;
                        if r.predecessor.runtime_root.exists()
                            || Path::new(&format!("/proc/{}", r.predecessor.app_pid)).exists()
                        {
                            return Err(
                                "predecessor host physical cleanup remains unresolved".into()
                            );
                        }
                        Ok(r)
                    });
                    match replacement {
                        Ok(r) => {
                            if let Some(metadata) = &mut child.metadata {
                                metadata.profile = Some(r.profile);
                            }
                        }
                        Err(error) => {
                            complete = false;
                            item["host_replacement_error"] = json!(error);
                        }
                    }
                } else if repair.host_admitted {
                    complete = false;
                    item["host_replacement_error"] = json!("admitted replacement receipt missing");
                }
            }
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
            let handoff = if self.fenced_nodes.is_empty() {
                self.interlock.publish_clean(&verified)
            } else {
                self.interlock
                    .publish_clean_with_fencing(&verified, &self.fenced_nodes)
            };
            if let Err(error) = handoff {
                complete = false;
                receipts.push(json!({"handoff_error":error}));
            }
        }
        let result = json!({"event":"shutdown","deployment":self.interlock.deployment,
            "clean":complete,"nodes":receipts,"profile_root":self.config.root,
            "fm":self.remote.as_ref().map(management::Client::cleanup_status)});
        self.manager.finished = Some(result.clone());
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
    mod restart;

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
