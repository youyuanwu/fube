use crate::profile::{add_body, ActualProfile, Launch, Result, RetirementReceipt};
use fube_ra::protocol::*;
use fube_transport::serialization::serde::ObjectArray;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub op: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<u8>,
}
impl Command {
    pub fn validate(&self) -> Result<()> {
        if self.op.is_empty()
            || self.op.len() > 64
            || !self
                .op
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err("operation identity must be 1..64 ASCII identifier bytes".into());
        }
        match (self.action.as_str(), self.node) {
            ("create" | "delete", Some(1 | 2)) | ("status" | "quit", None) => Ok(()),
            _ => Err("expected create/delete with node 1|2, or status/quit without node".into()),
        }
    }
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Desired {
    Absent,
    Running,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Observed {
    Absent,
    Pending,
    Running,
    Unknown,
    Error,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bootstrap {
    NodeUp,
    FinalUpload,
    Metadata,
    Admitted,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingPhase {
    Create,
    Delete,
}

#[derive(Clone)]
pub struct Placement {
    pub launch: Launch,
    pub profile: Option<ActualProfile>,
    pub bootstrap: Bootstrap,
    pub desired: Desired,
    pub observed: Observed,
    pub created: bool,
    pub endpoint: Option<String>,
    pub create_op: Option<String>,
    pub delete_op: Option<String>,
    pub error: Option<String>,
    pub link_alive: bool,
    pub catalog_notified: bool,
    pub replica: i64,
    pub incarnation: i64,
    pub retired: Vec<(i64, i64)>,
    pub reusable: bool,
    pub retirement: Option<RetirementReceipt>,
    remove_received: bool,
    pending: Option<PendingPhase>,
    primary_created_at: Option<Instant>,
    primary_last_action: Option<Instant>,
}
#[derive(Clone)]
pub struct Operation {
    pub command: Command,
    pub outcome: Value,
}
pub struct Ledger {
    pub placements: [Placement; 2],
    pub operations: Vec<Operation>,
}
pub enum Accepted {
    Duplicate(Value),
    Create(usize, Box<AddInstance>),
    Delete(usize, Box<RemoveInstance>),
    Status,
    Quit,
}
impl Placement {
    pub fn new(launch: Launch) -> Self {
        Self {
            replica: launch.replica,
            incarnation: launch.incarnation,
            launch,
            profile: None,
            bootstrap: Bootstrap::NodeUp,
            desired: Desired::Absent,
            observed: Observed::Absent,
            created: false,
            endpoint: None,
            create_op: None,
            delete_op: None,
            error: None,
            link_alive: true,
            catalog_notified: false,
            retired: vec![],
            reusable: false,
            retirement: None,
            remove_received: false,
            pending: None,
            primary_created_at: None,
            primary_last_action: None,
        }
    }
    pub fn admit_profile(&mut self, profile: ActualProfile) -> Result<()> {
        if self.bootstrap != Bootstrap::Metadata {
            return Err("profile cannot replace native bootstrap".into());
        }
        profile.validate(&self.launch)?;
        self.profile = Some(profile);
        self.bootstrap = Bootstrap::Admitted;
        Ok(())
    }
    pub fn body(&self) -> Result<AddInstance> {
        self.body_for(self.replica, self.incarnation)
    }
    fn body_for(&self, replica: i64, incarnation: i64) -> Result<AddInstance> {
        let mut body = add_body(
            &self.launch,
            self.profile.as_ref().ok_or("profile not admitted")?,
        );
        body.replica.id = replica;
        body.replica.instance = incarnation;
        Ok(body)
    }
    fn retired_replica(&self, replica: &Replica, canonical: bool) -> Result<bool> {
        if self.retired.contains(&(replica.id, replica.instance)) {
            exact_replica(
                &self.body_for(replica.id, replica.instance)?.replica,
                replica,
                canonical,
            )?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub fn notification(&self, body: &ServiceTypeNotification) -> Result<ServiceTypeNotification> {
        if self.bootstrap != Bootstrap::Admitted || body.infos.0.len() > 1 {
            return Err("type notification outside admitted fixed profile".into());
        }
        let expected = self
            .profile
            .as_ref()
            .ok_or("missing actual profile")?
            .info();
        if body.infos.0.iter().any(|info| info != &expected) {
            return Err("foreign/stale service type, code package or actual version".into());
        }
        Ok(body.clone())
    }
    pub fn report(&self, body: &ReplicaUp) -> Result<Option<ReplicaUp>> {
        if body.from_fmm {
            return Err("FMM upload unsupported".into());
        }
        if self.bootstrap == Bootstrap::FinalUpload {
            if !body.last || !body.replicas.0.is_empty() || !body.dropped.0.is_empty() {
                return Err("fresh final upload must be empty and final".into());
            }
            return Ok(Some(body.clone()));
        }
        if self.bootstrap != Bootstrap::Admitted
            || body.last
            || body.replicas.0.len() + body.dropped.0.len() != 1
            || !self.created
        {
            return Err("unsupported replica report".into());
        }
        let expected = self.body()?;
        for (dropped, infos) in [(false, &body.replicas.0), (true, &body.dropped.0)] {
            for info in infos {
                if info.unit != expected.unit
                    || info.service != expected.service
                    || info.replicas.0.len() != 1
                {
                    return Err("foreign/stale report partition or service".into());
                }
                let replica = &info.replicas.0[0].description;
                if self.retired_replica(replica, true)? {
                    // Valid old lineage is not a protocol/link failure and is
                    // not an ACK of the successor's semantic obligations.
                    return Ok(None);
                }
                exact_replica(&expected.replica, replica, true)?;
                if dropped {
                    // This ephemeral FM can semantically forget the exact
                    // dropped instance only after desired deletion. Unexpected
                    // loss remains uncertain rather than enabling replacement.
                    if replica.state != 3 || self.desired != Desired::Absent {
                        return Err("unrequested dropped authority remains staged".into());
                    }
                } else if replica.up || replica.state == 3 || info.local_deleted {
                    return Err("unsupported normal report semantics".into());
                }
            }
        }
        Ok(Some(ReplicaUp {
            replicas: ObjectArray(body.replicas.0.clone()),
            dropped: ObjectArray(body.dropped.0.clone()),
            last: false,
            from_fmm: false,
        }))
    }
    pub fn status(&self) -> Value {
        json!({
            "node": self.launch.placement, "node_id": crate::profile::node_hex(&self.launch.node),
            "node_instance": self.launch.node.instance, "session": self.launch.session,
            "partition": self.launch.partition, "replica": self.replica,
            "incarnation": self.incarnation, "service": crate::profile::SERVICE,
            "service_type": crate::profile::TYPE,
            "desired": self.desired, "observed": self.observed,
            "endpoint": self.endpoint, "created_lifetime": self.created,
            "link_alive": self.link_alive, "error": self.error,
            "catalog_notified": self.catalog_notified,
            "reusable":self.reusable, "retired_lifetimes":self.retired.len(),
            "retirement":self.retirement,
            "app_pid": self.profile.as_ref().map(|p| p.app_pid),
            "runtime_root": self.profile.as_ref().map(|p| &p.runtime_root),
        })
    }
}
fn exact_replica(expected: &Replica, actual: &Replica, canonical: bool) -> Result<()> {
    if expected.node != actual.node
        || expected.id != actual.id
        || expected.instance != actual.instance
        || actual.version
            != if canonical {
                VersionInstance::invalid()
            } else {
                expected.version.clone()
            }
        || actual.current_role != expected.current_role
        || actual.previous_role != expected.previous_role
    {
        return Err("foreign/stale replica incarnation/version".into());
    }
    Ok(())
}
impl Ledger {
    pub fn read_retirement(
        &mut self,
        index: usize,
        path: &std::path::Path,
        node_pid: u32,
    ) -> Result<()> {
        if !path.try_exists().map_err(crate::profile::err)? {
            return Ok(());
        }
        let receipt = crate::profile::read_json(path)?;
        self.retirement(index, receipt, node_pid)
    }
    pub fn new(launches: [Launch; 2]) -> Self {
        let [one, two] = launches;
        Self {
            placements: [Placement::new(one), Placement::new(two)],
            operations: vec![],
        }
    }
    pub fn accept(&mut self, command: Command) -> Result<Accepted> {
        command.validate()?;
        if let Some(operation) = self
            .operations
            .iter()
            .find(|op| op.command.op == command.op)
        {
            return if operation.command == command {
                Ok(Accepted::Duplicate(operation.outcome.clone()))
            } else {
                Err("operation identity reused with different canonical content".into())
            };
        }
        if self.operations.len() == 64 {
            return Err(
                "64 operation identities retained; capacity exhausted (no eviction)".into(),
            );
        }
        let accepted = match command.action.as_str() {
            "create" => {
                let index = usize::from(command.node.unwrap() - 1);
                let placement = &mut self.placements[index];
                if placement.bootstrap != Bootstrap::Admitted
                    || !placement.link_alive
                    || (placement.created && !placement.reusable)
                {
                    return Err(
                        "node unadmitted, uncertain, or predecessor cleanup not verified".into(),
                    );
                }
                if placement.created {
                    let replica = placement
                        .replica
                        .checked_add(1)
                        .ok_or("replica identity exhausted")?;
                    let incarnation = placement
                        .incarnation
                        .checked_add(1)
                        .ok_or("instance identity exhausted")?;
                    placement.replica = replica;
                    placement.incarnation = incarnation;
                }
                let body = placement.body()?;
                placement.created = true;
                placement.reusable = false;
                placement.retirement = None;
                placement.remove_received = false;
                placement.delete_op = None;
                placement.endpoint = None;
                placement.error = None;
                placement.desired = Desired::Running;
                placement.observed = Observed::Pending;
                placement.create_op = Some(command.op.clone());
                placement.pending = Some(PendingPhase::Create);
                if placement.launch.profile == crate::ServiceProfile::StatefulSingleton {
                    let now = Instant::now();
                    placement.primary_created_at = Some(now);
                    placement.primary_last_action = Some(now);
                }
                Accepted::Create(index, Box::new(body))
            }
            "delete" => {
                let index = usize::from(command.node.unwrap() - 1);
                let placement = &mut self.placements[index];
                if placement.launch.profile == crate::ServiceProfile::StatefulSingleton {
                    return Err("stateful FM deletion is gated; use original owned shutdown".into());
                }
                if !placement.created
                    || !placement.link_alive
                    || placement.desired != Desired::Running
                {
                    return Err(
                        "delete requires the original live desired-running placement".into(),
                    );
                }
                let body = placement.body()?;
                let superseded = if placement.pending == Some(PendingPhase::Create) {
                    placement.create_op.clone()
                } else {
                    None
                };
                placement.desired = Desired::Absent;
                placement.observed = Observed::Pending;
                placement.delete_op = Some(command.op.clone());
                placement.pending = Some(PendingPhase::Delete);
                if let Some(op) = superseded {
                    self.complete(
                        &op,
                        json!({"op":op,"state":"unknown","error":"create superseded by deletion"}),
                    );
                }
                Accepted::Delete(index, Box::new(RemoveInstance::new(body, false)))
            }
            "status" => Accepted::Status,
            "quit" => Accepted::Quit,
            _ => unreachable!(),
        };
        let mut outcome = json!({"op":command.op,"state":"pending"});
        if let Some(node) = command.node {
            let placement = &self.placements[usize::from(node - 1)];
            outcome["replica"] = json!(placement.replica);
            outcome["incarnation"] = json!(placement.incarnation);
        }
        self.operations.push(Operation { outcome, command });
        Ok(accepted)
    }
    pub fn result(&self, op: &str) -> Value {
        self.operations
            .iter()
            .find(|item| item.command.op == op)
            .map(|item| item.outcome.clone())
            .unwrap_or_else(|| json!({"state":"rejected"}))
    }
    pub fn complete(&mut self, op: &str, mut outcome: Value) {
        if let Some(operation) = self
            .operations
            .iter_mut()
            .find(|item| item.command.op == op)
        {
            for key in ["replica", "incarnation"] {
                if outcome.get(key).is_none() {
                    if let Some(value) = operation.outcome.get(key) {
                        outcome[key] = value.clone();
                    }
                }
            }
            operation.outcome = outcome;
        }
    }
    pub fn lost(&mut self, index: usize, error: String) {
        let placement = &mut self.placements[index];
        placement.link_alive = false;
        placement.pending = None;
        placement.observed = Observed::Unknown;
        placement.error = Some(error.clone());
        let operations = if placement.reusable {
            [None, None]
        } else {
            [placement.create_op.clone(), placement.delete_op.clone()]
        };
        for operation in operations.into_iter().flatten() {
            // Even earlier serving observations cannot establish current life
            // after session loss. Desired authority and tombstones stay intact.
            self.complete(
                &operation,
                json!({"op":operation,"state":"unknown","error":error}),
            );
        }
    }
    pub fn down(&mut self, index: usize) {
        let placement = &mut self.placements[index];
        // A delayed down observation cannot undo confirmed disposal.
        if placement.observed == Observed::Absent {
            return;
        }
        let error = "native report says original replica is down; replacement forbidden";
        placement.observed = Observed::Unknown;
        placement.endpoint = None;
        placement.error = Some(error.into());
        if placement.pending == Some(PendingPhase::Create) {
            placement.pending = None;
        }
        let operations = [placement.create_op.clone(), placement.delete_op.clone()];
        for op in operations.into_iter().flatten() {
            self.complete(&op, json!({"op":op,"state":"unknown","error":error}));
        }
    }
    pub fn reply(&mut self, index: usize, body: AddInstanceReply, removing: bool) -> Result<()> {
        let placement = &mut self.placements[index];
        if !placement.link_alive || !placement.created || placement.bootstrap != Bootstrap::Admitted
        {
            return Err("unadmitted semantic reply".into());
        }
        let expected = placement.body()?;
        if body.unit != expected.unit {
            return Err("foreign/stale reply partition or epoch".into());
        }
        if placement.retired_replica(&body.replica, removing && body.error == 0)? {
            return Ok(());
        }
        exact_replica(
            &expected.replica,
            &body.replica,
            removing && body.error == 0,
        )?;
        let op = if removing {
            &placement.delete_op
        } else {
            &placement.create_op
        }
        .clone()
        .ok_or("unsolicited semantic reply")?;
        let mut endpoint = None;
        if body.error == 0 {
            if removing {
                if placement.desired != Desired::Absent || body.replica.state != 3 {
                    return Err("Remove reply is not the requested disposed instance".into());
                }
            } else {
                if body.replica.state != 2 || !body.replica.up {
                    return Err("Add reply is not a ready replica".into());
                }
                let address: SocketAddr = body
                    .replica
                    .service_endpoint
                    .parse()
                    .map_err(|_| "non-numeric app endpoint")?;
                if !address.ip().is_loopback()
                    || address.port() == 0
                    || if placement.launch.profile == crate::ServiceProfile::StatefulSingleton {
                        body.replica.replication_endpoint.is_empty()
                    } else {
                        !body.replica.replication_endpoint.is_empty()
                    }
                {
                    return Err("unsupported app endpoint".into());
                }
                endpoint = Some(address.to_string());
            }
        }
        let phase = if removing {
            PendingPhase::Delete
        } else {
            PendingPhase::Create
        };
        if placement.pending != Some(phase) {
            return Ok(());
        }
        if removing && placement.remove_received {
            return Ok(());
        }
        if body.error != 0 {
            placement.pending = None;
            placement.observed = Observed::Error;
            placement.endpoint = None;
            placement.error = Some(format!("native semantic error {}", body.error));
            self.complete(
                &op,
                json!({"op":op,"state":"error","native_error":body.error}),
            );
            return Ok(());
        }
        if removing {
            placement.remove_received = true;
            self.finish_delete(index);
            return Ok(());
        } else {
            placement.pending = None;
            placement.observed = Observed::Running;
            placement.endpoint = endpoint;
        }
        placement.error = None;
        let outcome = json!({"op":op,"state":"complete",
            "observed":"running", "replica":placement.replica,
            "incarnation":placement.incarnation, "endpoint":placement.endpoint});
        self.complete(&op, outcome);
        Ok(())
    }
    /// Called only by the production bounded sidecar reader, while the
    /// original node/link is still owned and live. Arrival order is irrelevant.
    pub fn retirement(
        &mut self,
        index: usize,
        proof: RetirementReceipt,
        node_pid: u32,
    ) -> Result<()> {
        let placement = &mut self.placements[index];
        proof.validate(
            &placement.launch,
            placement.profile.as_ref().ok_or("missing profile")?,
            node_pid,
        )?;
        let replica = &proof.disposal.body.replica;
        if placement.retired.contains(&(replica.id, replica.instance)) {
            return Ok(());
        }
        if replica.id != placement.replica || replica.instance != placement.incarnation {
            return Err("unrecognized retirement lifetime".into());
        }
        if !placement.link_alive
            || placement.desired != Desired::Absent
            || placement.pending != Some(PendingPhase::Delete)
        {
            return Ok(());
        }
        placement.retirement = Some(proof);
        self.finish_delete(index);
        Ok(())
    }
    fn finish_delete(&mut self, index: usize) {
        let placement = &mut self.placements[index];
        if !placement.link_alive
            || placement.pending != Some(PendingPhase::Delete)
            || !placement.remove_received
            || placement.retirement.is_none()
        {
            return;
        }
        placement.pending = None;
        placement.observed = Observed::Absent;
        placement.endpoint = None;
        placement.error = None;
        placement.reusable = true;
        placement
            .retired
            .push((placement.replica, placement.incarnation));
        let op = placement.delete_op.clone().unwrap();
        let outcome = json!({"op":op,"state":"complete","observed":"absent",
            "replica":placement.replica,"incarnation":placement.incarnation,
            "cleanup":"verified_original_instance"});
        self.complete(&op, outcome);
    }
    pub fn status(&self) -> Value {
        json!({"nodes":self.placements.iter().map(Placement::status).collect::<Vec<_>>(),
            "operations":self.operations.iter().map(|op|
                if op.command.action == "status" {
                    json!({"op":op.command.op,"state":op.outcome["state"],"kind":"status"})
                } else { op.outcome.clone() }).collect::<Vec<_>>(),
            "operation_capacity":64, "retained_operations":self.operations.len(),
            "unsupported":"all native families outside the selected fixed lifecycle profile"})
    }
    /// Native Replica::CanGenerateAction and PendingTask's creating-primary branch.
    pub fn primary_retry(&mut self, index: usize, now: Instant) -> Result<Option<AddPrimary>> {
        let placement = &mut self.placements[index];
        if placement.launch.profile != crate::ServiceProfile::StatefulSingleton
            || !placement.link_alive
            || placement.pending != Some(PendingPhase::Create)
        {
            return Ok(None);
        }
        let created = placement
            .primary_created_at
            .ok_or("missing original Primary retry owner")?;
        let last = placement
            .primary_last_action
            .ok_or("missing original Primary action time")?;
        let interval = if now > created + Duration::from_secs(60) {
            Duration::from_secs(60)
        } else {
            Duration::from_secs(10)
        };
        if now <= last + interval {
            return Ok(None);
        }
        let body = placement.body()?;
        placement.primary_last_action = Some(now);
        Ok(Some(body))
    }
}
