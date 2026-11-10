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
            ("create" | "delete" | "activate" | "deactivate", Some(1 | 2)) | ("status" | "quit", None) => Ok(()),
            _ => Err("expected create/delete/activate/deactivate with node 1|2, or status/quit without node".into()),
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
    Lost,
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
    pub activated: bool,
    pub activation_sequence: i64,
    node_control: Option<(String, bool, i64)>,
    node_control_retry_at: Option<Instant>,
    pub(crate) service_retry_at: Option<Instant>,
    pub last_load: Option<ReportLoad>,
    pub service_description: Option<Service>,
    pub service_update: Option<Service>,
    pub type_enabled: bool,
    pub type_sequence: u64,
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
    pub loss_reported: bool,
    remove_received: bool,
    pending: Option<PendingPhase>,
    action_updated_at: Option<Instant>,
    action_last_at: Option<Instant>,
}
#[derive(Clone)]
pub struct Operation {
    pub command: Command,
    pub outcome: Value,
}
pub struct Ledger {
    pub placements: [Placement; 2],
    pub operations: Vec<Operation>,
    pub(crate) generation: Option<GenerationHandshake>,
    pub(crate) idle: Option<IdleBuild>,
    pub(crate) idle_removal: Option<IdleRemoval>,
}
pub(crate) struct IdleRemoval {
    pub body: RemoveReplica,
    pub reply: Option<RemoveReplicaReply>,
    failed_creation: bool,
    unknown: bool,
    started: Instant,
    last: Instant,
}
pub(crate) struct IdleBuild {
    pub body: AddReplica,
    pub reply: Option<AddReplicaReply>,
    pub cancelled: bool,
    unknown: bool,
    started: Instant,
    last: Instant,
}
fn action_retry_due(started: Instant, last: Instant, now: Instant) -> bool {
    let interval = if now > started + Duration::from_secs(60) {
        60
    } else {
        10
    };
    now > last + Duration::from_secs(interval)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationPhase {
    Proposing,
    AwaitingUpload,
}
pub(crate) enum GenerationWork {
    Propose(GenerationHeader),
    Update(GenerationHeader),
}
pub(crate) struct GenerationHandshake {
    pub number: GenerationNumber,
    pub phase: GenerationPhase,
    proposed: [bool; 2],
    pub updates_sent: [bool; 2],
    retry_at: [Instant; 2],
}
impl GenerationHandshake {
    pub fn new(current: &GenerationNumber) -> Result<Self> {
        Ok(Self {
            number: Self::successor(current, current.owner)?,
            phase: GenerationPhase::Proposing,
            proposed: [false; 2],
            updates_sent: [false; 2],
            retry_at: [Instant::now(); 2],
        })
    }
    fn successor(current: &GenerationNumber, owner: [u8; 16]) -> Result<GenerationNumber> {
        Ok(GenerationNumber {
            generation: current
                .generation
                .checked_add(1)
                .ok_or("FM generation counter exhausted")?,
            owner,
        })
    }
    fn restart(&mut self, current: &GenerationNumber) -> Result<()> {
        let next = Self::successor(current, self.number.owner)?;
        self.number = next;
        self.phase = GenerationPhase::Proposing;
        self.proposed = [false; 2];
        self.updates_sent = [false; 2];
        self.retry_at = [Instant::now(); 2];
        Ok(())
    }
    pub fn proposal_reply(&mut self, index: usize, body: &GenerationProposalReply) -> Result<()> {
        if self.phase != GenerationPhase::Proposing
            || body.proposed != self.number
            || body.error != 0
        {
            return Ok(());
        }
        if body.current.older_than(&body.proposed) {
            return Err("invalid generation proposal high-water mark".into());
        }
        if body.current != self.number {
            return self.restart(&body.current);
        }
        self.proposed[index] = true;
        if self.proposed.iter().all(|accepted| *accepted) {
            self.phase = GenerationPhase::AwaitingUpload;
            self.retry_at = [Instant::now(); 2];
        }
        Ok(())
    }
    pub fn reject(&mut self, current: &GenerationNumber) -> Result<()> {
        if !current.older_than(&self.number) {
            self.restart(current)?;
        }
        Ok(())
    }
    pub fn next(&mut self, index: usize, now: Instant) -> Option<GenerationWork> {
        if now < self.retry_at[index] {
            return None;
        }
        let header = GenerationHeader {
            number: self.number.clone(),
            for_fm_replica: false,
        };
        match self.phase {
            GenerationPhase::Proposing if !self.proposed[index] => {
                self.retry_at[index] = now + Duration::from_secs(5);
                Some(GenerationWork::Propose(header))
            }
            GenerationPhase::AwaitingUpload => {
                let retry = if self.updates_sent[index] { 10 } else { 5 };
                self.updates_sent[index] = true;
                self.retry_at[index] = now + Duration::from_secs(retry);
                Some(GenerationWork::Update(header))
            }
            _ => None,
        }
    }
    fn status(&self) -> Value {
        json!({
            "generation": self.number,
            "phase": match self.phase {
                GenerationPhase::Proposing => "proposing",
                GenerationPhase::AwaitingUpload => "awaiting_upload",
            },
            "proposal_replies": self.proposed,
            "updates_sent": self.updates_sent,
            "upload_supported": false,
            "complete": false,
        })
    }
}
pub enum Accepted {
    NodeControl(usize, bool, NodeActivationRequest),
    Duplicate(Value),
    Create(usize, Box<AddInstance>),
    Delete(usize, Box<RemoveInstance>),
    Status,
    Quit,
}
impl Placement {
    pub fn new(launch: Launch) -> Self {
        Self {
            activated: true,
            activation_sequence: -1,
            node_control: None,
            node_control_retry_at: None,
            service_retry_at: None,
            last_load: None,
            service_description: None,
            service_update: None,
            type_enabled: true,
            type_sequence: 0,
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
            loss_reported: false,
            remove_received: false,
            pending: None,
            action_updated_at: None,
            action_last_at: None,
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
        if let Some(service) = &self.service_description {
            body.service = service.clone();
        }
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
                if self.launch.profile.stateful() {
                    if !dropped
                        || replica != &dropped_replica(&expected.replica)
                        || info.local_deleted
                        || info.report_from_primary
                        || info.intermediate != Epoch::default()
                        || info.replicas.0[0].intermediate_role != 1
                    {
                        return Err("unsupported stateful terminal report".into());
                    }
                    continue;
                }
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
        if self.launch.profile.stateful() {
            // Native ProcessDroppedReplica's result is processed_, not dropped_.
            return Ok(Some(ReplicaUp {
                replicas: body.dropped.clone(),
                ..ReplicaUp::default()
            }));
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
            "incarnation": self.incarnation, "service": self.launch.profile.service(),
            "service_type": crate::profile::fixed_type_for(self.launch.profile).name,
            "desired": self.desired, "observed": self.observed,
            "endpoint": self.endpoint, "created_lifetime": self.created,
            "link_alive": self.link_alive, "error": self.error,
            "catalog_notified": self.catalog_notified,
            "reusable":self.reusable, "retired_lifetimes":self.retired.len(),
            "retirement":self.retirement,
            "loss_reported":self.loss_reported, "native_replica_deleted":self.remove_received,
            "activated":self.activated, "activation_sequence":self.activation_sequence,
            "node_control_pending":self.node_control,
            "type_enabled":self.type_enabled, "type_sequence":self.type_sequence,
            "service_update_version":self.service_description.as_ref().map(|s| s.update_version),
            "service_update_pending":self.service_update.is_some(),
            "load_report_received":self.last_load.is_some(),
            "app_pid": self.profile.as_ref().map(|p| p.app_pid),
            "runtime_root": self.profile.as_ref().map(|p| &p.runtime_root),
        })
    }
}
fn dropped_replica(replica: &Replica) -> Replica {
    Replica {
        node: replica.node.clone(),
        id: replica.id,
        instance: replica.instance,
        up: false,
        state: 3,
        ..Replica::default()
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
        || actual.current_role != if canonical { 1 } else { expected.current_role }
        || actual.previous_role != if canonical { 1 } else { expected.previous_role }
    {
        return Err("foreign/stale replica incarnation/version".into());
    }
    Ok(())
}
impl Ledger {
    pub fn begin_generation(&mut self) -> Result<()> {
        if self.generation.is_some()
            || self.placements.iter().any(|placement| {
                placement.bootstrap != Bootstrap::Admitted
                    || !placement.link_alive
                    || placement.pending.is_some()
                    || placement.node_control.is_some()
                    || placement.service_update.is_some()
            })
        {
            return Err("generation handshake requires two admitted idle control sessions".into());
        }
        self.generation = Some(GenerationHandshake::new(
            &self.placements[0].launch.generation.number,
        )?);
        Ok(())
    }
    pub fn report(&mut self, index: usize, body: &ReplicaUp) -> Result<Option<ReplicaUp>> {
        if !self.placements[index].link_alive {
            return Err("report requires the original live node session".into());
        }
        let response = self.placements[index].report(body)?;
        let placement = &mut self.placements[index];
        if response.is_some()
            && placement.bootstrap == Bootstrap::Admitted
            && placement.launch.profile.stateful()
        {
            if !placement.loss_reported {
                placement.action_updated_at = Some(Instant::now());
            }
            placement.loss_reported = true;
            placement.observed = Observed::Lost;
            placement.endpoint = None;
            placement.reusable = false;
            let error = "original volatile singleton lost; no recovery or replacement";
            placement.error = Some(error.into());
            if placement.pending == Some(PendingPhase::Create) {
                placement.pending = None;
            }
            let operations = [placement.create_op.clone(), placement.delete_op.clone()];
            for op in operations.into_iter().flatten() {
                self.complete(&op, json!({"op":op,"state":"lost","error":error}));
            }
        }
        Ok(response)
    }
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
            generation: None,
            idle: None,
            idle_removal: None,
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
        if self.generation.is_some() && !matches!(command.action.as_str(), "status" | "quit") {
            return Err(
                "generation rebuild is awaiting gated LFUM upload; mutations disabled".into(),
            );
        }
        if self.placements[0].launch.profile == crate::ServiceProfile::StatefulPeerIdle
            && (matches!(
                command.action.as_str(),
                "delete" | "activate" | "deactivate"
            ) || (command.action == "create"
                && (command.node != Some(1) || self.placements[0].created)))
        {
            return Err("peer-idle profile admits only the original node1 Primary creation, status and quit".into());
        }
        if self.operations.len() == 64 {
            return Err(
                "64 operation identities retained; capacity exhausted (no eviction)".into(),
            );
        }
        let accepted = match command.action.as_str() {
            "activate" | "deactivate" => {
                let index = usize::from(command.node.unwrap() - 1);
                let placement = &mut self.placements[index];
                if placement.bootstrap != Bootstrap::Admitted
                    || !placement.link_alive
                    || placement
                        .node_control
                        .as_ref()
                        .is_some_and(|(_, intent, _)| *intent == (command.action == "activate"))
                {
                    return Err(
                        "node activation requires admitted current session without same-intent pending control"
                            .into(),
                    );
                }
                let activate = command.action == "activate";
                let sequence = placement
                    .activation_sequence
                    .checked_add(1)
                    .ok_or("activation sequence exhausted")?;
                let superseded =
                    placement
                        .node_control
                        .replace((command.op.clone(), activate, sequence));
                placement.activation_sequence = sequence;
                placement.node_control_retry_at = Some(Instant::now() + Duration::from_secs(5));
                if let Some((op, _, _)) = superseded {
                    self.complete(&op, json!({"op":op,"state":"unknown","error":"superseded by newer node intent"}));
                }
                Accepted::NodeControl(
                    index,
                    activate,
                    NodeActivationRequest {
                        sequence,
                        from_fmm: false,
                    },
                )
            }
            "create" => {
                let index = usize::from(command.node.unwrap() - 1);
                let placement = &mut self.placements[index];
                if placement.bootstrap != Bootstrap::Admitted
                    || !placement.activated
                    || !placement.type_enabled
                    || placement.node_control.is_some()
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
                placement.loss_reported = false;
                placement.remove_received = false;
                placement.delete_op = None;
                placement.endpoint = None;
                placement.error = None;
                placement.desired = Desired::Running;
                placement.observed = Observed::Pending;
                placement.create_op = Some(command.op.clone());
                placement.pending = Some(PendingPhase::Create);
                if placement.launch.profile.stateful() {
                    let now = Instant::now();
                    placement.action_updated_at = Some(now);
                    placement.action_last_at = Some(now);
                }
                Accepted::Create(index, Box::new(body))
            }
            "delete" => {
                let index = usize::from(command.node.unwrap() - 1);
                let placement = &mut self.placements[index];
                if placement.launch.profile.stateful() && placement.observed != Observed::Running {
                    return Err(
                        "stateful normal deletion requires the completed original Primary".into(),
                    );
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
                if placement.launch.profile.stateful() {
                    let now = Instant::now();
                    placement.action_updated_at = Some(now);
                    placement.action_last_at = Some(now);
                }
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
    pub fn node_control_reply(
        &mut self,
        index: usize,
        activate: bool,
        body: NodeActivationReply,
    ) -> Result<()> {
        let placement = &mut self.placements[index];
        let Some((op, expected, sequence)) = placement.node_control.clone() else {
            if body.sequence <= placement.activation_sequence {
                return Ok(());
            }
            return Err("unsolicited node activation reply".into());
        };
        if body.sequence < sequence {
            return Ok(());
        }
        if activate != expected || body.sequence != sequence {
            return Err("node activation reply does not match current sequence/intent".into());
        }
        placement.node_control = None;
        placement.node_control_retry_at = None;
        placement.activated = activate;
        placement.activation_sequence = sequence;
        self.complete(
            &op,
            json!({"op":op,"state":"complete","activated":activate,"sequence":sequence}),
        );
        Ok(())
    }
    pub fn node_control_retry(
        &mut self,
        index: usize,
        now: Instant,
    ) -> Option<(bool, NodeActivationRequest)> {
        let placement = &mut self.placements[index];
        let (_, activate, sequence) = placement.node_control.as_ref()?;
        if !placement.link_alive || placement.node_control_retry_at.is_none_or(|due| due > now) {
            return None;
        }
        placement.node_control_retry_at = Some(now + Duration::from_secs(5));
        Some((
            *activate,
            NodeActivationRequest {
                sequence: *sequence,
                from_fmm: false,
            },
        ))
    }
    pub fn service_update_retry(
        &mut self,
        index: usize,
        now: Instant,
    ) -> Option<NodeUpdateServiceRequest> {
        let placement = &mut self.placements[index];
        let service = placement.service_update.as_ref()?;
        if !placement.link_alive || placement.service_retry_at.is_none_or(|due| due > now) {
            return None;
        }
        placement.service_retry_at = Some(now + Duration::from_secs(5));
        Some(NodeUpdateServiceRequest {
            service: service.clone(),
        })
    }
    pub fn service_reply(&mut self, index: usize, body: NodeUpdateServiceReply) -> Result<()> {
        let placement = &mut self.placements[index];
        let expected = placement
            .service_update
            .as_ref()
            .or(placement.service_description.as_ref())
            .ok_or("unsolicited service update reply")?;
        if body.service != expected.name
            || body.instance != expected.instance
            || body.version > expected.update_version
        {
            return Err("service update reply does not match current request".into());
        }
        if body.version == expected.update_version && placement.service_update.is_some() {
            placement.service_description = placement.service_update.take();
            placement.service_retry_at = None;
        }
        Ok(())
    }
    pub fn load(&mut self, index: usize, body: ReportLoad) -> Result<()> {
        let placement = &mut self.placements[index];
        let expected = placement.body()?;
        if !body.legacy.0.is_empty()
            || body.reports.0.len() != 1
            || body.reports.0[0].partition != expected.unit.id.guid
            || body.reports.0[0].service != expected.service.name
            || body.reports.0[0].stateful != expected.service.stateful
        {
            return Err("load report outside admitted fixed partition/service".into());
        }
        placement.last_load = Some(body);
        Ok(())
    }
    pub fn type_status(
        &mut self,
        index: usize,
        enabled: bool,
        body: &ServiceTypeUpdate,
    ) -> Result<ServiceTypeUpdateReply> {
        let placement = &mut self.placements[index];
        let profile = placement.profile.as_ref().ok_or("profile not admitted")?;
        if body.types.0.len() != 1 || body.types.0[0] != profile.service_type {
            return Err("type status outside admitted fixed type".into());
        }
        if body.sequence > placement.type_sequence {
            placement.type_enabled = enabled;
            placement.type_sequence = body.sequence;
        }
        Ok(ServiceTypeUpdateReply {
            sequence: body.sequence,
        })
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
        if let Some(removal) = &mut self.idle_removal {
            if removal.reply.is_none() {
                removal.unknown = true;
            }
        }
        if let Some(idle) = &mut self.idle {
            idle.unknown = true;
        }
        let placement = &mut self.placements[index];
        placement.link_alive = false;
        placement.pending = None;
        placement.observed = Observed::Unknown;
        placement.error = Some(error.clone());
        let control = placement.node_control.take().map(|(op, _, _)| op);
        let operations = if placement
            .retired
            .contains(&(placement.replica, placement.incarnation))
        {
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
        if let Some(op) = control {
            self.complete(&op, json!({"op":op,"state":"unknown","error":error}));
        }
    }
    pub fn down(&mut self, index: usize) {
        if let Some(removal) = &mut self.idle_removal {
            if removal.reply.is_none() {
                removal.unknown = true;
            }
        }
        if let Some(idle) = &mut self.idle {
            idle.unknown = true;
        }
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
        if removing && placement.loss_reported {
            if body.error != 0 {
                placement.error = Some(format!("native loss cleanup error {}", body.error));
                return Ok(());
            }
            if body.replica != dropped_replica(&expected.replica) {
                return Err("loss cleanup reply is not the native dropped description".into());
            }
            // Native ACK processing deletes the closed FT and sends DeleteReplicaReply.
            // This confirms native metadata deletion, never original-runtime disposal.
            placement.remove_received = true;
            self.finish_delete(index);
            return Ok(());
        }
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
                if placement.launch.profile.stateful()
                    && body.replica != dropped_replica(&expected.replica)
                {
                    return Err("DeleteReplica reply is not the native dropped description".into());
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
                    || if placement.launch.profile.stateful() {
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
        if placement.loss_reported {
            // Graceful Close can supply genuine disposal after a dropped report.
            // Its validated sidecar is not retirement/recovery authority.
            return Ok(());
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
            || placement.loss_reported
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
            "idle_build":self.idle_status(),
            "idle_removal":self.idle_removal_status(),
            "generation_handshake": self.generation.as_ref().map(GenerationHandshake::status),
            "operations":self.operations.iter().map(|op|
                if op.command.action == "status" {
                    json!({"op":op.command.op,"state":op.outcome["state"],"kind":"status"})
                } else { op.outcome.clone() }).collect::<Vec<_>>(),
            "operation_capacity":64, "retained_operations":self.operations.len(),
            "unsupported":"all native families outside the selected fixed lifecycle profile"})
    }
    pub fn request_idle(&mut self, now: Instant) -> Result<AddReplica> {
        if self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
            || self.idle_removal.is_some()
            || self.generation.is_some()
            || self.placements[0].observed != Observed::Running
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || !p.activated || p.bootstrap != Bootstrap::Admitted)
        {
            return Err(
                "Idle build requires the original admitted two-node Primary profile".into(),
            );
        }
        if let Some(idle) = &mut self.idle {
            if idle.cancelled || idle.unknown || idle.reply.is_some() {
                return Err("original FM Idle build is no longer pending".into());
            }
            idle.last = now;
            return Ok(idle.body.clone());
        }
        let mut body = self.placements[0].body()?;
        body.replica = self.placements[1].body()?.replica;
        body.replica.current_role = 2;
        if body.replica.version != body.service.version {
            return Err("peer packages do not share the original deployment version".into());
        }
        self.idle = Some(IdleBuild {
            body: body.clone(),
            reply: None,
            cancelled: false,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    pub fn idle_reply(&mut self, index: usize, body: AddReplicaReply) -> Result<()> {
        let Some(idle) = &mut self.idle else {
            return Ok(());
        };
        if index != 0
            || idle.cancelled
            || idle.unknown
            || idle.reply.is_some()
            || self.placements[0].observed != Observed::Running
            || self.placements.iter().any(|p| !p.link_alive)
            || body.unit != idle.body.unit
            || body.replica.node != idle.body.replica.node
            || body.replica.id != idle.body.replica.id
            || body.replica.instance != idle.body.replica.instance
        {
            return Ok(());
        }
        if body.error == 0
            && (body.replica.current_role != 2
                || body.replica.previous_role != 1
                || body.replica.state != 2
                || !body.replica.up
                || body.replica.version != idle.body.replica.version
                || !body.replica.service_endpoint.is_empty()
                || body.replica.replication_endpoint.is_empty())
        {
            return Err("AddReplicaReply is not the original Ready Idle".into());
        }
        let failed = body.error != 0;
        idle.reply = Some(body);
        if failed {
            self.request_idle_removal(Instant::now())?;
        }
        Ok(())
    }
    pub fn idle_retry(&mut self, index: usize, now: Instant) -> Option<AddReplica> {
        let idle = self.idle.as_mut()?;
        if index != 0
            || self.generation.is_some()
            || idle.cancelled
            || idle.unknown
            || idle.reply.is_some()
            || self.placements[0].observed != Observed::Running
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || !p.activated)
        {
            return None;
        }
        if !action_retry_due(idle.started, idle.last, now) {
            return None;
        }
        idle.last = now;
        Some(idle.body.clone())
    }
    pub fn idle_status(&self) -> Value {
        let Some(idle) = &self.idle else {
            return json!({"state":"absent","ready":false});
        };
        let error = idle.reply.as_ref().map(|reply| reply.error);
        let state = if idle.unknown {
            "unknown"
        } else if idle.cancelled {
            "cancelled"
        } else if error == Some(0) {
            "ready"
        } else if error.is_some() {
            "failed"
        } else {
            "pending"
        };
        json!({"state":state, "kind":"fm_idle_build", "ready":state == "ready",
            "serving":false, "request":idle.body, "reply":idle.reply,
            "to_be_dropped":error.is_some_and(|e| e != 0),
            "destination_cleanup_required":error.is_some_and(|e| e != 0),
            "error":error, "cleanup_required":(idle.cancelled || error.is_some_and(|e| e != 0))
                && self.idle_removal.as_ref().is_none_or(|removal| removal.reply.is_none())})
    }
    pub fn request_idle_removal(&mut self, now: Instant) -> Result<RemoveReplica> {
        if self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
            || self.generation.is_some()
            || self.placements[0].observed != Observed::Running
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || !p.activated || p.bootstrap != Bootstrap::Admitted)
        {
            return Err("Idle removal requires the original admitted Primary profile".into());
        }
        if let Some(removal) = &mut self.idle_removal {
            if removal.unknown || removal.reply.is_some() {
                return Err("original FM Idle removal is no longer pending".into());
            }
            removal.last = now;
            return Ok(removal.body.clone());
        }
        if self
            .idle
            .as_ref()
            .is_some_and(|idle| idle.cancelled || idle.unknown)
        {
            return Err("cancelled or unknown Idle cleanup is gated".into());
        }
        let mut body = self.placements[0].body()?;
        body.replica = self.placements[1].body()?.replica;
        body.replica.current_role = 2;
        if body.replica.version != body.service.version {
            return Err("peer packages do not share the original deployment version".into());
        }
        let mut failed_creation = false;
        if let Some(idle) = &mut self.idle {
            body = idle.body.clone();
            if let Some(reply) = &idle.reply {
                failed_creation = reply.error != 0;
                if !failed_creation {
                    body.replica = reply.replica.clone();
                }
            }
            if !failed_creation {
                idle.cancelled = true;
            }
        }
        self.idle_removal = Some(IdleRemoval {
            body: body.clone(),
            reply: None,
            failed_creation,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    pub fn idle_removal_reply(&mut self, index: usize, body: RemoveReplicaReply) -> Result<()> {
        let Some(removal) = &mut self.idle_removal else {
            return Ok(());
        };
        if index != 0
            || removal.unknown
            || removal.reply.is_some()
            || self.placements[0].observed != Observed::Running
            || self.placements.iter().any(|p| !p.link_alive)
            || body.unit != removal.body.unit
            || body.replica.node != removal.body.replica.node
            || body.replica.id != removal.body.replica.id
            || body.replica.instance != removal.body.replica.instance
            || body.error != 0
        {
            return Ok(());
        }
        let echoed_creation = removal.body.replica.state == 1
            && removal.body.replica.replication_endpoint.is_empty()
            && body.replica == removal.body.replica;
        if !echoed_creation
            && (removal.failed_creation
                || body.replica.current_role != 2
                || body.replica.previous_role != 1
                || body.replica.state != 3
                || !body.replica.up
                || body.replica.version != removal.body.replica.version
                || !body.replica.service_endpoint.is_empty()
                || body.replica.replication_endpoint.is_empty()
                || (!removal.body.replica.replication_endpoint.is_empty()
                    && body.replica.replication_endpoint
                        != removal.body.replica.replication_endpoint))
        {
            return Err("RemoveReplicaReply is not the original removed Idle".into());
        }
        removal.reply = Some(body);
        Ok(())
    }
    pub fn idle_removal_retry(&mut self, index: usize, now: Instant) -> Option<RemoveReplica> {
        let removal = self.idle_removal.as_mut()?;
        if index != 0
            || self.generation.is_some()
            || removal.unknown
            || removal.reply.is_some()
            || self.placements[0].observed != Observed::Running
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || !p.activated)
        {
            return None;
        }
        if !action_retry_due(removal.started, removal.last, now) {
            return None;
        }
        removal.last = now;
        Some(removal.body.clone())
    }
    pub fn idle_removal_status(&self) -> Value {
        let Some(removal) = &self.idle_removal else {
            return json!({"state":"absent","pending_remove":false,"removed":false});
        };
        let state = if removal.reply.is_some() {
            "removed"
        } else if removal.unknown {
            "unknown"
        } else {
            "pending"
        };
        json!({"state":state, "kind":"fm_idle_removal",
            "pending_remove":removal.reply.is_none(), "removed":state == "removed",
            "completion":removal.reply.as_ref().map(|reply| if removal.failed_creation {
                "failed_creation_retired"
            } else if reply.replica.state == 1 {
                "creation_cancelled"
            } else {
                "replicator_removed"
            }),
            "request":removal.body, "reply":removal.reply,
            "destination_deleted":false, "serving":false})
    }
    /// Native Replica::CanGenerateAction and PendingTask's creating-primary branch.
    pub fn primary_retry(&mut self, index: usize, now: Instant) -> Result<Option<AddPrimary>> {
        self.retry_body(index, PendingPhase::Create, now)
    }
    pub fn deletion_retry(&mut self, index: usize, now: Instant) -> Result<Option<DeleteReplica>> {
        if self.placements[index].remove_received {
            return Ok(None);
        }
        Ok(self
            .retry_body(index, PendingPhase::Delete, now)?
            .map(|body| DeleteReplica::new(body, false)))
    }
    fn retry_body(
        &mut self,
        index: usize,
        phase: PendingPhase,
        now: Instant,
    ) -> Result<Option<AddInstance>> {
        let placement = &mut self.placements[index];
        if !placement.launch.profile.stateful()
            || !placement.link_alive
            || (placement.pending != Some(phase)
                && !(phase == PendingPhase::Delete && placement.loss_reported))
        {
            return Ok(None);
        }
        let created = placement
            .action_updated_at
            .ok_or("missing original stateful retry owner")?;
        let last = placement
            .action_last_at
            .ok_or("missing original stateful action time")?;
        if !action_retry_due(created, last, now) {
            return Ok(None);
        }
        let body = placement.body()?;
        placement.action_last_at = Some(now);
        Ok(Some(body))
    }
}
