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
        let launches = [1, 2].map(|placement| Launch {
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
        });
        Ok(Self {
            interlock,
            config,
            ledger: Ledger::new(launches),
            children: Vec::with_capacity(2),
            finished: None,
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
        Ok(())
    }
    fn prepare(&self) -> Result<()> {
        private_dir(&self.config.root.join("deployment"))?;
        self.assemble()
    }
    fn assemble(&self) -> Result<()> {
        let package = self.config.root.join("deployment/package");
        if self.config.profile == ServiceProfile::StatefulSingleton {
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
            || owner
                .metadata
                .as_ref()
                .is_some_and(|prior| prior.address != metadata.address)
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
        let Some(mut incoming) = incoming else {
            return Ok(());
        };
        wire::predecode(&incoming.message)?;
        let action = incoming.message.properties().map_err(err)?.action;
        let generation = self.children[index].launch.generation.clone();
        match action.as_str() {
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
                let response = self.ledger.placements[index].report(&body)?;
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
            "AddInstanceReply"
            | "AddPrimaryReply"
            | "RemoveInstanceReply"
            | "DeleteReplicaReply" => {
                let removing = matches!(
                    action.as_str(),
                    "RemoveInstanceReply" | "DeleteReplicaReply"
                );
                let primary =
                    self.children[index].launch.profile == ServiceProfile::StatefulSingleton;
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
                Ok(()) => self.retry_primary(index).await,
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(()) => self.retry_deletion(index).await,
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
    async fn retry_deletion(&mut self, index: usize) -> Result<()> {
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
    fn read_retirement(&mut self, index: usize) -> Result<()> {
        let child = &self.children[index];
        let path = child.launch.directory.join("retirement.json");
        let node_pid = child
            .process
            .as_ref()
            .ok_or("original node owner absent")?
            .pid();
        self.ledger.read_retirement(index, &path, node_pid)
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
        status
    }
    /// Returns the retained operation outcome, never transport-write success.
    /// `quit` is accepted here; the foreground then calls owned shutdown.
    pub async fn execute(&mut self, command: Command) -> Result<Value> {
        if self.finished.is_some() {
            return Err("deployment already shut down".into());
        }
        let op = command.op.clone();
        match self.ledger.accept(command)? {
            Accepted::Duplicate(outcome) => return Ok(outcome),
            Accepted::Create(index, body) => {
                let factory =
                    if self.children[index].launch.profile == ServiceProfile::StatefulSingleton {
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
                let factory =
                    if self.children[index].launch.profile == ServiceProfile::StatefulSingleton {
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
