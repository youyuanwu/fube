use crate::profile::{
    absent_replica_body, add_body, dropped_replica, pre_open_cancelled_body, AbsenceReceipt,
    ActualProfile, FailedOpenReceipt, FailedRegistrationReceipt, HostLossReceipt, Launch,
    PreOpenCancellationReceipt, Result, RetirementReceipt,
};
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
    pub node_description: Option<NodeDescription>,
    pub bootstrap: Bootstrap,
    pub desired: Desired,
    pub observed: Observed,
    pub created: bool,
    pub endpoint: Option<String>,
    pub ready_replica: Option<Replica>,
    pub active_unit: Option<FailoverUnit>,
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
    pub placements: Vec<Placement>,
    pub operations: Vec<Operation>,
    pub(crate) generation: Option<GenerationHandshake>,
    pub(crate) idles: Vec<IdleBuild>,
    pub(crate) idle_removal: Option<IdleRemoval>,
    pub(crate) idle_deletion: Option<IdleDeletion>,
    pub(crate) idle_predecessor: Option<Value>,
    pub(crate) configuration: Option<ActiveConfiguration>,
    pub(crate) swap: Option<ActiveConfiguration>,
    swap_endpoint: Option<AddInstance>,
    configuration_history: Vec<ActiveConfiguration>,
}
pub(crate) struct ActiveConfiguration {
    pub body: DoReconfiguration,
    pub reply: Option<ConfigurationReply>,
    unknown: bool,
    started: Instant,
    last: Instant,
}
pub(crate) struct IdleDeletion {
    pub body: DeleteReplica,
    pub reply: Option<DeleteReplicaReply>,
    target: IdleDeleteTarget,
    absence: Option<AbsenceReceipt>,
    cancellation: Option<PreOpenCancellationReceipt>,
    failed_registration: Option<FailedRegistrationReceipt>,
    failed_open: Option<FailedOpenReceipt>,
    host_loss: Option<HostLossReceipt>,
    failure_error: Option<i64>,
    unknown: bool,
    started: Instant,
    last: Instant,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleDeleteTarget {
    Runtime,
    Unobserved,
    PreRegistration,
    FailedRegistration,
    FailedOpen,
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
    Complete,
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
    pub uploads: [Option<LfumUpload>; 2],
    retry_at: [Instant; 2],
}
impl GenerationHandshake {
    pub fn new(current: &GenerationNumber) -> Result<Self> {
        Ok(Self {
            number: Self::successor(current, current.owner)?,
            phase: GenerationPhase::Proposing,
            proposed: [false; 2],
            updates_sent: [false; 2],
            uploads: [None, None],
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
        self.uploads = [None, None];
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
        if self.phase != GenerationPhase::Complete && !current.older_than(&self.number) {
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
            GenerationPhase::AwaitingUpload if self.uploads[index].is_none() => {
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
                GenerationPhase::Complete => "complete",
            },
            "proposal_replies": self.proposed,
            "updates_sent": self.updates_sent,
            "uploads":self.uploads,
            "upload_supported": true,
            "complete": self.phase == GenerationPhase::Complete,
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
            node_description: None,
            bootstrap: Bootstrap::NodeUp,
            desired: Desired::Absent,
            observed: Observed::Absent,
            created: false,
            endpoint: None,
            ready_replica: None,
            active_unit: None,
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
        if let Some(unit) = &self.active_unit {
            body.unit = unit.clone();
            if let Some(ready) = &self.ready_replica {
                body.replica = ready.clone();
                body.replica.id = replica;
                body.replica.instance = incarnation;
            }
        }
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
        self.report_known_creation(body, self.created)
    }
    fn report_known_creation(
        &self,
        body: &ReplicaUp,
        known_creation: bool,
    ) -> Result<Option<ReplicaUp>> {
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
            || !known_creation
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
    pub fn generation_pending(&self) -> bool {
        self.generation
            .as_ref()
            .is_some_and(|round| round.phase != GenerationPhase::Complete)
    }

    pub fn lfum_upload(&mut self, index: usize, body: LfumUpload) -> Result<GenerationHeader> {
        let placement = &self.placements[index];
        if !placement.link_alive || placement.bootstrap != Bootstrap::Admitted {
            return Err("LFUM upload requires the original admitted session".into());
        }
        let header = GenerationHeader {
            number: body.generation.clone(),
            for_fm_replica: false,
        };
        let Some(round) = &mut self.generation else {
            // Native ready FM acknowledges delayed uploads without rebuilding.
            return Ok(header);
        };
        if round.phase != GenerationPhase::AwaitingUpload
            || body.generation != round.number
            || round.uploads[index].is_some()
        {
            return Ok(header);
        }
        if placement.node_description.as_ref() != Some(&body.node)
            || body.units.0.len() > 1
            || body.any_replica_found != placement.created
            || (placement.observed == Observed::Running && body.units.0.len() != 1)
            || (!placement.created && !body.units.0.is_empty())
        {
            return Err("LFUM node/inventory does not match the admitted live profile".into());
        }
        let expected = placement.created.then(|| placement.body()).transpose()?;
        for info in &body.units.0 {
            let expected = expected.as_ref().ok_or("unowned LFUM partition")?;
            if info.service != expected.service
                || info.unit != expected.unit
                || info.intermediate != Epoch::default()
                || info.local_deleted
                || info.replicas.0.len() != 1
            {
                return Err(
                    "LFUM report changed the admitted partition/service/configuration".into(),
                );
            }
            let replica = &info.replicas.0[0];
            let dropped = replica.description.state == 3;
            exact_replica(&expected.replica, &replica.description, dropped)?;
            if replica.intermediate_role != 1
                || (placement.observed == Observed::Running
                    && (Some(&replica.description) != placement.ready_replica.as_ref() || dropped))
                || info.report_from_primary != (!dropped && expected.service.stateful)
                || (dropped
                    && (replica.description.up
                        || !replica.description.service_endpoint.is_empty()
                        || !replica.description.replication_endpoint.is_empty()))
            {
                return Err("LFUM replica state does not match the known original lifetime".into());
            }
        }
        round.uploads[index] = Some(body);
        if round.uploads.iter().all(Option::is_some) {
            round.phase = GenerationPhase::Complete;
        }
        Ok(header)
    }

    pub fn begin_generation(&mut self) -> Result<()> {
        if self.placements.len() != 2
            || self.configuration.is_some()
            || self.generation.is_some()
            || self.placements.iter().any(|placement| {
                placement.bootstrap != Bootstrap::Admitted
                    || !placement.link_alive
                    || matches!(
                        placement.observed,
                        Observed::Lost | Observed::Unknown | Observed::Error
                    )
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
        if let Some(swap) = &self.swap {
            let mut stable = swap.body.configuration.unit.clone();
            stable.previous = Epoch::default();
            if body
                .dropped
                .0
                .first()
                .is_some_and(|i| i.unit == stable && i.service == swap.body.configuration.service)
            {
                self.placements[index].active_unit = Some(stable);
            }
        }
        let peer_idle = index != 0 && self.placements[index].launch.profile.peer_replicas();
        if peer_idle
            && !body.last
            && !body.from_fmm
            && body.replicas.0.is_empty()
            && body.dropped.0.len() == 1
        {
            let info = &body.dropped.0[0];
            if info.replicas.0.len() == 1 {
                let replica = &info.replicas.0[0].description;
                let known_retirement =
                    self.configuration
                        .iter()
                        .chain(self.configuration_history.iter())
                        .any(|c| {
                            let config = &c.body.configuration;
                            config.unit.target == 1
                                && config.unit == info.unit
                                && config.service == info.service
                                && config.replicas.0.iter().any(|r| {
                                    r.node == replica.node && dropped_replica(r) == *replica
                                })
                        });
                if known_retirement
                    && self.placements[index]
                        .retired
                        .contains(&(replica.id, replica.instance))
                    && !info.local_deleted
                    && !info.report_from_primary
                    && info.intermediate == Epoch::default()
                    && info.replicas.0[0].intermediate_role == 1
                {
                    return Ok(None);
                }
            }
        }
        let response = if peer_idle && !self.idles.is_empty() {
            self.placements[index].report_known_creation(body, true)?
        } else {
            self.placements[index].report(body)?
        };
        if peer_idle && self.retiring_secondary() && self.idle_predecessor.is_none() {
            return Ok(response);
        }
        let placement = &mut self.placements[index];
        if response.is_some()
            && placement.bootstrap == Bootstrap::Admitted
            && placement.launch.profile.stateful()
        {
            if let Some(swap) = &mut self.swap {
                swap.unknown = true;
            }
            if let Some(configuration) = &mut self.configuration {
                configuration.unknown = true;
            }
            if !placement.loss_reported {
                let now = Instant::now();
                placement.action_updated_at = Some(now);
                if peer_idle {
                    placement.action_last_at = Some(now);
                }
            }
            placement.created = true;
            placement.loss_reported = true;
            placement.observed = Observed::Lost;
            placement.endpoint = None;
            placement.reusable = false;
            let error = if peer_idle {
                "original volatile destination Idle lost; no disposal or replacement"
            } else {
                "original volatile singleton lost; no recovery or replacement"
            };
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
    pub fn read_host_loss(
        &mut self,
        index: usize,
        path: &std::path::Path,
        node_pid: u32,
    ) -> Result<()> {
        if !path.try_exists().map_err(crate::profile::err)? {
            return Ok(());
        }
        self.host_loss(index, crate::profile::read_json(path)?, node_pid)
    }
    pub fn host_loss(&mut self, index: usize, proof: HostLossReceipt, node_pid: u32) -> Result<()> {
        let placement = &self.placements[index];
        proof.validate(
            &placement.launch,
            placement.profile.as_ref().ok_or("missing profile")?,
            node_pid,
        )?;
        let Some(deletion) = &mut self.idle_deletion else {
            return Ok(());
        };
        if deletion.unknown || !placement.link_alive {
            return Ok(());
        }
        if index != 1
            || !matches!(
                deletion.target,
                IdleDeleteTarget::Runtime
                    | IdleDeleteTarget::PreRegistration
                    | IdleDeleteTarget::Unobserved
            )
            || deletion.absence.is_some()
            || deletion.cancellation.is_some()
            || deletion.failed_registration.is_some()
            || deletion.failed_open.is_some()
            || deletion.host_loss.as_ref().is_some_and(|old| {
                old.loss.creation != proof.loss.creation
                    || old.loss.operation != proof.loss.operation
                    || old.loss.host != proof.loss.host
            })
            || proof.loss.body.unit != deletion.body.replica.0.unit
            || proof.loss.body.replica.node != deletion.body.replica.0.replica.node
            || proof.loss.body.replica.id != deletion.body.replica.0.replica.id
            || proof.loss.body.replica.instance != deletion.body.replica.0.replica.instance
            || deletion
                .reply
                .as_ref()
                .is_some_and(|reply| reply.unit != proof.loss.body.unit)
            || placement.retirement.is_some()
        {
            return Err("host loss cannot replace another destination cleanup origin".into());
        }
        deletion.host_loss = Some(proof);
        self.placements[1].created = true;
        self.finish_idle_deletion();
        Ok(())
    }
    pub fn read_absence(
        &mut self,
        index: usize,
        path: &std::path::Path,
        node_pid: u32,
    ) -> Result<()> {
        if !path.try_exists().map_err(crate::profile::err)? {
            return Ok(());
        }
        self.absence(index, crate::profile::read_json(path)?, node_pid)
    }
    pub fn absence(&mut self, index: usize, proof: AbsenceReceipt, node_pid: u32) -> Result<()> {
        let placement = &self.placements[index];
        proof.validate(
            &placement.launch,
            placement.profile.as_ref().ok_or("missing profile")?,
            node_pid,
        )?;
        let Some(deletion) = &mut self.idle_deletion else {
            return Ok(());
        };
        if deletion.unknown || !placement.link_alive {
            return Ok(());
        }
        if deletion.target != IdleDeleteTarget::Unobserved
            || deletion.host_loss.is_some()
            || placement.retirement.is_some()
            || deletion.cancellation.is_some()
            || deletion.failed_registration.is_some()
            || deletion.failed_open.is_some()
            || proof.absence.body != absent_replica_body(&deletion.body.replica.0)
            || deletion
                .reply
                .as_ref()
                .is_some_and(|reply| reply.unit != proof.absence.body.unit)
        {
            return Err("absence cannot retire an opened or unrelated destination lifetime".into());
        }
        deletion.absence = Some(proof);
        self.finish_idle_deletion();
        Ok(())
    }
    pub fn read_pre_open_cancellation(
        &mut self,
        index: usize,
        path: &std::path::Path,
        node_pid: u32,
    ) -> Result<()> {
        if !path.try_exists().map_err(crate::profile::err)? {
            return Ok(());
        }
        self.pre_open_cancellation(index, crate::profile::read_json(path)?, node_pid)
    }
    pub fn pre_open_cancellation(
        &mut self,
        index: usize,
        proof: PreOpenCancellationReceipt,
        node_pid: u32,
    ) -> Result<()> {
        let placement = &self.placements[index];
        proof.validate(
            &placement.launch,
            placement.profile.as_ref().ok_or("missing profile")?,
            node_pid,
        )?;
        let Some(deletion) = &mut self.idle_deletion else {
            return Ok(());
        };
        if deletion.unknown || !placement.link_alive {
            return Ok(());
        }
        if matches!(
            deletion.target,
            IdleDeleteTarget::Runtime
                | IdleDeleteTarget::FailedRegistration
                | IdleDeleteTarget::FailedOpen
        ) || deletion.absence.is_some()
            || deletion.host_loss.is_some()
            || placement.retirement.is_some()
            || deletion.failed_registration.is_some()
            || deletion.failed_open.is_some()
            || proof.cancellation.body != pre_open_cancelled_body(&deletion.body.replica.0)
            || deletion
                .reply
                .as_ref()
                .is_some_and(|reply| reply.unit != proof.cancellation.body.unit)
        {
            return Err(
                "pre-Open cancellation cannot replace another destination cleanup proof".into(),
            );
        }
        deletion.cancellation = Some(proof);
        self.finish_idle_deletion();
        Ok(())
    }
    pub fn read_failed_registration(
        &mut self,
        index: usize,
        path: &std::path::Path,
        node_pid: u32,
    ) -> Result<()> {
        if !path.try_exists().map_err(crate::profile::err)? {
            return Ok(());
        }
        self.failed_registration(index, crate::profile::read_json(path)?, node_pid)
    }
    pub fn failed_registration(
        &mut self,
        index: usize,
        proof: FailedRegistrationReceipt,
        node_pid: u32,
    ) -> Result<()> {
        let placement = &self.placements[index];
        proof.validate(
            &placement.launch,
            placement.profile.as_ref().ok_or("missing profile")?,
            node_pid,
        )?;
        let Some(deletion) = &mut self.idle_deletion else {
            return Ok(());
        };
        if deletion.unknown || !placement.link_alive {
            return Ok(());
        }
        let origin_matches = match deletion.target {
            IdleDeleteTarget::FailedRegistration => {
                deletion.failure_error == Some(proof.deletion.failure.error)
            }
            IdleDeleteTarget::PreRegistration | IdleDeleteTarget::Unobserved => {
                deletion.failure_error.is_none()
            }
            _ => false,
        };
        if !origin_matches
            || deletion.absence.is_some()
            || deletion.cancellation.is_some()
            || deletion.failed_open.is_some()
            || deletion.host_loss.is_some()
            || placement.retirement.is_some()
            || proof.deletion.failure.body != pre_open_cancelled_body(&deletion.body.replica.0)
            || deletion
                .reply
                .as_ref()
                .is_some_and(|reply| reply.unit != proof.deletion.failure.body.unit)
        {
            return Err(
                "failed registration proof cannot replace another failure or cleanup origin".into(),
            );
        }
        deletion.failed_registration = Some(proof);
        self.finish_idle_deletion();
        Ok(())
    }
    pub fn read_failed_open(
        &mut self,
        index: usize,
        path: &std::path::Path,
        node_pid: u32,
    ) -> Result<()> {
        if !path.try_exists().map_err(crate::profile::err)? {
            return Ok(());
        }
        self.failed_open(index, crate::profile::read_json(path)?, node_pid)
    }
    pub fn failed_open(
        &mut self,
        index: usize,
        proof: FailedOpenReceipt,
        node_pid: u32,
    ) -> Result<()> {
        let placement = &self.placements[index];
        proof.validate(
            &placement.launch,
            placement.profile.as_ref().ok_or("missing profile")?,
            node_pid,
        )?;
        let Some(deletion) = &mut self.idle_deletion else {
            return Ok(());
        };
        if deletion.unknown || !placement.link_alive {
            return Ok(());
        }
        let origin_matches = match deletion.target {
            IdleDeleteTarget::FailedOpen => {
                deletion.failure_error == Some(proof.deletion.failure.error)
            }
            IdleDeleteTarget::Runtime
            | IdleDeleteTarget::PreRegistration
            | IdleDeleteTarget::Unobserved => deletion.failure_error.is_none(),
            _ => false,
        };
        if !origin_matches
            || deletion.absence.is_some()
            || deletion.cancellation.is_some()
            || deletion.failed_registration.is_some()
            || deletion.host_loss.is_some()
            || placement.retirement.is_some()
            || proof.deletion.failure.disposal.body != deletion.body.replica.0
            || deletion
                .reply
                .as_ref()
                .is_some_and(|reply| reply.unit != proof.deletion.failure.disposal.body.unit)
        {
            return Err(
                "failed Open proof cannot replace another failure or cleanup origin".into(),
            );
        }
        deletion.failed_open = Some(proof);
        self.finish_idle_deletion();
        Ok(())
    }
    pub fn new(launches: impl IntoIterator<Item = Launch>) -> Self {
        Self {
            placements: launches.into_iter().map(Placement::new).collect(),
            operations: vec![],
            generation: None,
            idles: vec![],
            idle_removal: None,
            idle_deletion: None,
            idle_predecessor: None,
            configuration: None,
            swap: None,
            swap_endpoint: None,
            configuration_history: vec![],
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
        if self.generation_pending() && !matches!(command.action.as_str(), "status" | "quit") {
            return Err("generation rebuild is awaiting LFUM upload; mutations disabled".into());
        }
        if self.configuration.is_some() && !matches!(command.action.as_str(), "status" | "quit") {
            return Err("active-configuration lifecycle changes remain gated".into());
        }
        if self.placements[0].launch.profile.peer_replicas()
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
        if let Some(swap) = &mut self.swap {
            swap.unknown = true;
        }
        if let Some(configuration) = &mut self.configuration {
            configuration.unknown = true;
        }
        if index == 1 && !self.idle_deletion_complete() {
            if let Some(deletion) = &mut self.idle_deletion {
                deletion.unknown = true;
            }
        }
        if let Some(removal) = &mut self.idle_removal {
            if removal.reply.is_none() {
                removal.unknown = true;
            }
        }
        for idle in &mut self.idles {
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
        if self.placements[0].launch.profile == crate::ServiceProfile::StatefulThree {
            if let Some(configuration) = &mut self.configuration {
                configuration.unknown = true;
            }
        }
        if index == 1 && !self.idle_deletion_complete() {
            if let Some(deletion) = &mut self.idle_deletion {
                deletion.unknown = true;
            }
        }
        if let Some(removal) = &mut self.idle_removal {
            if removal.reply.is_none() {
                removal.unknown = true;
            }
        }
        for idle in &mut self.idles {
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
        if index == 1
            && removing
            && self.placements[index].launch.profile == crate::ServiceProfile::StatefulPeerIdle
            && (!self.placements[index].loss_reported || self.idle_deletion.is_some())
        {
            return self.idle_deletion_reply(index, body);
        }
        let placement = &mut self.placements[index];
        if !placement.link_alive || !placement.created || placement.bootstrap != Bootstrap::Admitted
        {
            return Err("unadmitted semantic reply".into());
        }
        let expected = placement.body()?;
        if placement.active_unit.is_some()
            && body.unit.id == expected.unit.id
            && body.unit.current.older_than(expected.unit.current)
        {
            exact_replica(
                &expected.replica,
                &body.replica,
                removing && body.error == 0,
            )?;
            return Ok(());
        }
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
            placement.ready_replica = Some(body.replica.clone());
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
        if let Some(config) = self
            .configuration
            .iter()
            .chain(self.configuration_history.iter())
            .find(|c| {
                c.body.configuration.unit.target == 1
                    && c.body.configuration.unit == proof.disposal.body.unit
                    && c.body.configuration.service == proof.disposal.body.service
            })
        {
            let mut replica = config
                .body
                .configuration
                .replicas
                .0
                .iter()
                .find(|r| r.node == placement.launch.node)
                .ok_or("retirement proof omitted configured destination")?
                .clone();
            replica.state = 3;
            if proof.disposal.body.replica != replica {
                return Err("retirement proof changed the configured Close identity".into());
            }
            let body = AddInstance {
                unit: config.body.configuration.unit.clone(),
                service: config.body.configuration.service.clone(),
                replica,
            };
            proof.validate_body(
                &placement.launch,
                placement.profile.as_ref().ok_or("missing profile")?,
                node_pid,
                &body,
            )?;
        } else {
            proof.validate(
                &placement.launch,
                placement.profile.as_ref().ok_or("missing profile")?,
                node_pid,
            )?;
        }
        let replica = &proof.disposal.body.replica;
        if placement.retired.contains(&(replica.id, replica.instance)) {
            return Ok(());
        }
        if replica.id != placement.replica || replica.instance != placement.incarnation {
            return Err("unrecognized retirement lifetime".into());
        }
        if index == 1 && placement.launch.profile == crate::ServiceProfile::StatefulPeerIdle {
            if placement.loss_reported && self.idle_deletion.is_none() {
                return Ok(());
            }
            if self.idle_deletion.as_ref().is_some_and(|deletion| {
                deletion.absence.is_some()
                    || deletion.cancellation.is_some()
                    || deletion.host_loss.is_some()
                    || deletion.failed_registration.is_some()
                    || deletion.failed_open.is_some()
                    || matches!(
                        deletion.target,
                        IdleDeleteTarget::FailedRegistration | IdleDeleteTarget::FailedOpen
                    )
            }) {
                return Err(
                    "Close disposal cannot replace a different destination cleanup origin".into(),
                );
            }
            if self
                .idle_deletion
                .as_ref()
                .and_then(|deletion| deletion.reply.as_ref())
                .is_some_and(|reply| reply.unit != proof.disposal.body.unit)
            {
                return Err("disposal proof does not match the destination Delete reply".into());
            }
            if placement.link_alive
                && self
                    .idle_deletion
                    .as_ref()
                    .is_some_and(|deletion| !deletion.unknown)
            {
                placement.retirement = Some(proof);
                placement.created = true;
                self.finish_idle_deletion();
            }
            return Ok(());
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
            "idle_deletion":self.idle_deletion_status(),
            "idle_predecessor":self.idle_predecessor,
            "configuration":self.configuration_status(),
            "generation_handshake": self.generation.as_ref().map(GenerationHandshake::status),
            "operations":self.operations.iter().map(|op|
                if op.command.action == "status" {
                    json!({"op":op.command.op,"state":op.outcome["state"],"kind":"status"})
                } else { op.outcome.clone() }).collect::<Vec<_>>(),
            "operation_capacity":64, "retained_operations":self.operations.len(),
            "unsupported":"all native families outside the selected fixed lifecycle profile"})
    }
    pub fn request_idle(&mut self, now: Instant) -> Result<AddReplica> {
        self.request_idle_on(1, now)
    }
    pub fn request_idle_on(&mut self, destination: usize, now: Instant) -> Result<AddReplica> {
        if destination == 0 || destination >= self.placements.len() {
            return Err("unknown original Idle destination".into());
        }
        if (self.configuration.is_some()
            && !(self.retired_secondary() && self.idle_predecessor.is_some()))
            || !self.placements[0].launch.profile.peer_replicas()
            || self.placements.iter().any(|p| p.loss_reported)
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.generation.is_some()
            || self.placements[0].observed != Observed::Running
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || !p.activated || p.bootstrap != Bootstrap::Admitted)
        {
            return Err("Idle build requires the original admitted healthy peer profile".into());
        }
        let target = &self.placements[destination].launch.node;
        if let Some(idle) = self
            .idles
            .iter_mut()
            .find(|i| &i.body.replica.node == target)
        {
            if idle.cancelled || idle.unknown || idle.reply.as_ref().is_some_and(|r| r.error != 0) {
                return Err("original FM Idle build is no longer pending".into());
            }
            idle.last = now;
            return Ok(idle.body.clone());
        }
        let mut body = self.placements[0].body()?;
        body.replica = self.placements[destination].body()?.replica;
        body.replica.current_role = 2;
        if body.replica.version != body.service.version {
            return Err("peer packages do not share the original deployment version".into());
        }
        self.idles.push(IdleBuild {
            body: body.clone(),
            reply: None,
            cancelled: false,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    pub fn request_configuration(&mut self, now: Instant) -> Result<DoReconfiguration> {
        let count = self.placements.len() as i32;
        if self.swap.is_some() {
            return Err("another configuration after Primary transfer is gated".into());
        }
        let successor = self.retired_secondary()
            && self.idle_predecessor.is_some()
            && self.idles.first().is_some_and(|i| {
                i.reply.as_ref().is_some_and(|r| r.error == 0) && !i.cancelled && !i.unknown
            });
        if !self.placements[0].launch.profile.peer_replicas()
            || self.generation.is_some()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.placements[0].observed != Observed::Running
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || !p.activated
                    || p.loss_reported
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
            })
        {
            return Err("configuration requires the original healthy admitted nodes".into());
        }
        if let Some(configuration) = &mut self.configuration {
            if configuration.unknown {
                return Err("configuration owner is unknown".into());
            }
            if !successor {
                if configuration.body.configuration.unit.target != count {
                    return Err(
                        "retired configuration requires a fresh successfully built successor"
                            .into(),
                    );
                }
                configuration.last = now;
                return Ok(configuration.body.clone());
            }
        }
        if self.idles.len() != self.placements.len() - 1 {
            return Err("configuration requires every original FM Idle build".into());
        }
        let mut primary = self.placements[0]
            .ready_replica
            .clone()
            .ok_or("missing original Primary reply")?;
        primary.previous_role = 4;
        let mut replicas = vec![primary];
        for idle in &self.idles {
            let reply = idle
                .reply
                .as_ref()
                .filter(|r| r.error == 0 && !idle.cancelled && !idle.unknown)
                .ok_or("configuration requires successful FM Idle build")?;
            let mut secondary = reply.replica.clone();
            secondary.previous_role = 2;
            secondary.current_role = 3;
            replicas.push(secondary);
        }
        let mut body = self.placements[0].body()?;
        body.unit.previous = body.unit.current;
        body.unit.current.configuration = body
            .unit
            .current
            .configuration
            .checked_add(1)
            .ok_or("configuration epoch exhausted")?;
        if body.unit.current.configuration & !0xffff_ffff
            != body.unit.previous.configuration & !0xffff_ffff
        {
            return Err("same-Primary configuration epoch exhausted".into());
        }
        body.unit.target = count;
        body.unit.minimum = if count == 3 { 2 } else { 1 };
        body.service.target = count;
        body.service.minimum = body.unit.minimum;
        body.service.update_version = body
            .service
            .update_version
            .checked_add(1)
            .ok_or("service version exhausted")?;
        let body = DoReconfiguration {
            configuration: Configuration {
                unit: body.unit,
                service: body.service,
                replicas: ObjectArray(replicas),
            },
            phase0_duration: i64::MAX,
        };
        if let Some(previous) = self.configuration.take() {
            self.configuration_history.push(previous);
        }
        self.configuration = Some(ActiveConfiguration {
            body: body.clone(),
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    fn retiring_secondary(&self) -> bool {
        self.configuration
            .as_ref()
            .is_some_and(|c| c.body.configuration.unit.target == 1 && !c.unknown)
    }
    fn retired_secondary(&self) -> bool {
        self.retiring_secondary()
            && self
                .configuration
                .as_ref()
                .is_some_and(|c| c.reply.is_some())
    }
    pub fn request_secondary_retirement(&mut self, now: Instant) -> Result<DoReconfiguration> {
        if self.swap.is_some() {
            return Err("retirement after Primary transfer is gated".into());
        }
        if self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
            || self.generation.is_some()
            || self.idle_predecessor.is_some()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || !p.activated
                    || p.loss_reported
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
            })
        {
            return Err("Secondary retirement requires healthy original admitted sessions".into());
        }
        if self.retiring_secondary() {
            let config = self.configuration.as_mut().expect("retirement");
            config.last = now;
            return Ok(config.body.clone());
        }
        if self
            .placements
            .iter()
            .any(|p| p.observed != Observed::Running)
        {
            return Err("Secondary retirement requires both active replicas Running".into());
        }
        let config = self
            .configuration
            .as_ref()
            .filter(|c| {
                c.reply.is_some()
                    && !c.unknown
                    && c.body.configuration.unit.target == 2
                    && self.configuration_history.is_empty()
            })
            .ok_or("Secondary retirement requires completed original active membership")?;
        let mut body = config.body.clone();
        body.configuration.unit.previous = body.configuration.unit.current;
        body.configuration.unit.current.configuration = body
            .configuration
            .unit
            .current
            .configuration
            .checked_add(1)
            .ok_or("configuration epoch exhausted")?;
        if body.configuration.unit.current.configuration & !0xffff_ffff
            != body.configuration.unit.previous.configuration & !0xffff_ffff
        {
            return Err("same-Primary configuration epoch exhausted".into());
        }
        body.configuration.unit.target = 1;
        body.configuration.service.target = 1;
        body.configuration.service.update_version = body
            .configuration
            .service
            .update_version
            .checked_add(1)
            .ok_or("service version exhausted")?;
        for r in &mut body.configuration.replicas.0 {
            r.previous_role = r.current_role;
            if r.current_role == 3 {
                r.current_role = 1;
            }
        }
        self.placements[1].active_unit = Some(body.configuration.unit.clone());
        self.placements[1].service_description = Some(body.configuration.service.clone());
        self.placements[1].ready_replica = body
            .configuration
            .replicas
            .0
            .iter()
            .find(|r| r.node == self.placements[1].launch.node)
            .cloned();
        self.configuration_history
            .push(self.configuration.take().expect("active configuration"));
        self.configuration = Some(ActiveConfiguration {
            body: body.clone(),
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    pub fn configuration_reply(&mut self, index: usize, body: ConfigurationReply) -> Result<()> {
        if self.swap.is_some() {
            return self.swap_reply(index, body);
        }
        let Some(config) = &mut self.configuration else {
            return Ok(());
        };
        if index != 0
            || config.unknown
            || config.reply.is_some()
            || body.error != 0
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || p.loss_reported)
        {
            return Ok(());
        }
        let mut expected = config.body.configuration.clone();
        expected.unit.previous = Epoch::default();
        for replica in &mut expected.replicas.0 {
            replica.previous_role = 1;
        }
        expected.replicas.0.retain(|r| r.current_role >= 3);
        if body.unit != expected.unit || body.replicas != expected.replicas {
            return Ok(());
        }
        for placement in &mut self.placements {
            if !expected
                .replicas
                .0
                .iter()
                .any(|r| r.node == placement.launch.node)
            {
                placement.observed = Observed::Absent;
                placement.desired = Desired::Absent;
                continue;
            }
            placement.active_unit = Some(expected.unit.clone());
            placement.service_description = Some(expected.service.clone());
            placement.ready_replica = expected
                .replicas
                .0
                .iter()
                .find(|r| r.node == placement.launch.node)
                .cloned();
            placement.desired = Desired::Running;
            placement.observed = Observed::Running;
            placement.created = true;
        }
        config.reply = Some(body);
        Ok(())
    }
    pub fn configuration_retry(&mut self, now: Instant) -> Option<DoReconfiguration> {
        if let Some(swap) = &mut self.swap {
            if swap.unknown
                || swap.reply.is_some()
                || self
                    .placements
                    .iter()
                    .any(|p| !p.link_alive || p.loss_reported)
                || !action_retry_due(swap.started, swap.last, now)
            {
                return None;
            }
            swap.last = now;
            return Some(swap.body.clone());
        }
        let config = self.configuration.as_mut()?;
        if config.unknown
            || config.reply.is_some()
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || p.loss_reported)
            || !action_retry_due(config.started, config.last, now)
        {
            return None;
        }
        config.last = now;
        Some(config.body.clone())
    }
    pub fn request_primary_transfer(&mut self, now: Instant) -> Result<DoReconfiguration> {
        if self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
            || self.generation.is_some()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || p.loss_reported
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
            })
        {
            return Err(
                "Primary transfer requires the two healthy original admitted sessions".into(),
            );
        }
        if let Some(swap) = &mut self.swap {
            if swap.unknown {
                return Err("Primary transfer ownership is unknown".into());
            }
            swap.last = now;
            return Ok(swap.body.clone());
        }
        let active = self
            .configuration
            .as_ref()
            .filter(|c| c.reply.is_some() && !c.unknown && c.body.configuration.unit.target == 2)
            .ok_or("Primary transfer requires completed two-member active membership")?;
        if self
            .placements
            .iter()
            .any(|p| p.observed != Observed::Running)
        {
            return Err("Primary transfer requires both members Running".into());
        }
        let mut body = active.body.clone();
        let reply = active.reply.as_ref().expect("completed configuration");
        body.configuration.unit = reply.unit.clone();
        body.configuration.unit.previous = reply.unit.current;
        body.configuration.unit.current.configuration = reply
            .unit
            .current
            .configuration
            .checked_add(0x1_0000_0001)
            .ok_or("Primary epoch exhausted")?;
        body.configuration.replicas = reply.replicas.clone();
        for r in &mut body.configuration.replicas.0 {
            r.previous_role = r.current_role;
            r.current_role = if r.current_role == 4 { 3 } else { 4 };
        }
        body.phase0_duration = i64::MAX;
        for p in &mut self.placements {
            p.active_unit = Some(body.configuration.unit.clone());
            p.ready_replica = body
                .configuration
                .replicas
                .0
                .iter()
                .find(|r| r.node == p.launch.node)
                .cloned();
            p.endpoint = None;
        }
        self.swap = Some(ActiveConfiguration {
            body: body.clone(),
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    pub fn endpoint_updated(
        &mut self,
        index: usize,
        body: AddInstance,
    ) -> Result<Option<AddInstanceReply>> {
        let Some(swap) = self
            .swap
            .as_ref()
            .filter(|s| !s.unknown && s.reply.is_none())
        else {
            return Ok(None);
        };
        if index != 1
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || p.loss_reported)
        {
            return Ok(None);
        }
        let expected = swap
            .body
            .configuration
            .replicas
            .0
            .iter()
            .find(|r| r.node == self.placements[1].launch.node)
            .ok_or("missing new Primary")?;
        let mut actual = body.replica.clone();
        actual.service_endpoint = expected.service_endpoint.clone();
        actual.first_lsn = expected.first_lsn;
        actual.last_lsn = expected.last_lsn;
        if body.unit != swap.body.configuration.unit
            || body.service != swap.body.configuration.service
            || actual != *expected
        {
            return Ok(None);
        }
        let endpoint: std::net::SocketAddr = body
            .replica
            .service_endpoint
            .parse()
            .map_err(|_| "invalid promoted service endpoint")?;
        if !endpoint.ip().is_loopback() || endpoint.port() == 0 {
            return Err("promoted endpoint must be numeric loopback".into());
        }
        let reply = AddInstanceReply {
            unit: body.unit.clone(),
            replica: body.replica.clone(),
            error: 0,
        };
        self.swap_endpoint = Some(body);
        Ok(Some(reply))
    }
    fn swap_reply(&mut self, index: usize, body: ConfigurationReply) -> Result<()> {
        let Some(swap) = self
            .swap
            .as_mut()
            .filter(|s| !s.unknown && s.reply.is_none())
        else {
            return Ok(());
        };
        let mut unit = swap.body.configuration.unit.clone();
        unit.previous = Epoch::default();
        if index != 1
            || body.error != 0
            || body.unit != unit
            || body.replicas.0.len() != 2
            || swap.body.configuration.replicas.0.iter().any(|expected| {
                body.replicas
                    .0
                    .iter()
                    .filter(|r| r.node == expected.node)
                    .count()
                    != 1
            })
            || self
                .placements
                .iter()
                .any(|p| !p.link_alive || p.loss_reported)
        {
            return Ok(());
        }
        for r in &body.replicas.0 {
            let Some(expected) = swap
                .body
                .configuration
                .replicas
                .0
                .iter()
                .find(|e| e.node == r.node)
            else {
                return Ok(());
            };
            let mut actual = r.clone();
            actual.previous_role = expected.previous_role;
            actual.first_lsn = expected.first_lsn;
            actual.last_lsn = expected.last_lsn;
            actual.service_endpoint = expected.service_endpoint.clone();
            if actual != *expected || r.previous_role != 1 || r.first_lsn < 0 || r.last_lsn < 0 {
                return Ok(());
            }
            if r.current_role == 4 {
                let endpoint: std::net::SocketAddr = r
                    .service_endpoint
                    .parse()
                    .map_err(|_| "invalid final Primary endpoint")?;
                if !endpoint.ip().is_loopback() || endpoint.port() == 0 {
                    return Err("unsupported final Primary endpoint".into());
                }
            } else if !r.service_endpoint.is_empty() {
                return Err("demoted Primary retained a published service endpoint".into());
            }
        }
        for p in &mut self.placements {
            let replica = body
                .replicas
                .0
                .iter()
                .find(|r| r.node == p.launch.node)
                .ok_or("swap omitted placement")?
                .clone();
            p.endpoint = (replica.current_role == 4).then(|| replica.service_endpoint.clone());
            p.ready_replica = Some(replica);
            p.active_unit = Some(body.unit.clone());
            p.observed = Observed::Running;
        }
        swap.reply = Some(body);
        Ok(())
    }
    pub fn configuration_status(&self) -> Value {
        if let Some(swap) = &self.swap {
            let state = if swap.unknown {
                "unknown"
            } else if swap.reply.is_some() {
                "ready"
            } else {
                "pending"
            };
            return json!({"state":state,"ready":state == "ready","kind":"planned_primary_transfer",
                "request":{"configuration":swap.body.configuration,"phase0_duration":swap.body.phase0_duration},
                "reply":swap.reply,"endpoint_update":self.swap_endpoint,"primary_transfer":true,"from":1,"to":2,
                "history":self.configuration_history.iter().chain(self.configuration.iter()).map(|c| json!({"request":c.body.configuration,"reply":c.reply})).collect::<Vec<_>>()});
        }
        let Some(config) = &self.configuration else {
            return json!({"state":"absent","ready":false});
        };
        let state = if config.unknown {
            "unknown"
        } else if config.reply.is_some() {
            "ready"
        } else {
            "pending"
        };
        json!({"state":state,"ready":state == "ready",
            "kind":if config.body.configuration.unit.target == 1 {"same_primary_retirement"} else {"same_primary_configuration"},
            "request":{"configuration":config.body.configuration,"phase0_duration":config.body.phase0_duration},
            "reply":config.reply,"primary_transfer":false,
            "history":self.configuration_history.iter().map(|c| json!({"request":c.body.configuration,"reply":c.reply})).collect::<Vec<_>>()})
    }
    pub fn request_idle_replacement(&mut self, now: Instant) -> Result<AddReplica> {
        if self.swap.is_some() {
            return Err("replacement after Primary transfer is gated".into());
        }
        if self.configuration.is_some() && !self.retired_secondary() {
            return Err("configured-secondary replacement is gated".into());
        }
        if self.idle_predecessor.is_some() {
            return self.request_idle(now);
        }
        if self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
            || self.generation.is_some()
            || self.placements[0].observed != Observed::Running
            || self.placements.iter().any(|placement| {
                !placement.link_alive
                    || !placement.activated
                    || placement.bootstrap != Bootstrap::Admitted
                    || placement.loss_reported
                    || placement.node_control.is_some()
                    || placement.service_update.is_some()
                    || placement.pending.is_some()
            })
            || self.idles.first().is_none_or(|idle| {
                idle.unknown || idle.reply.as_ref().is_none_or(|reply| reply.error != 0)
            })
            || (!self.retired_secondary()
                && self.idle_removal.as_ref().is_none_or(|removal| {
                    removal.unknown || removal.reply.is_none() || removal.failed_creation
                }))
            || self.idle_deletion.as_ref().is_none_or(|deletion| {
                deletion.unknown
                    || deletion.target != IdleDeleteTarget::Runtime
                    || deletion.body.deletion.0.force
                    || deletion.host_loss.is_some()
            })
            || !self.idle_deletion_complete()
            || self.placements[1].retirement.is_none()
        {
            return Err(
                "fresh Idle requires original FM removal, normal Delete and verified disposal on both live original nodes".into(),
            );
        }
        let (replica, incarnation) = self.placements[1].launch.idle_successor()?;
        let mut body = self.placements[0].body()?;
        body.replica = self.placements[1].body_for(replica, incarnation)?.replica;
        body.replica.previous_role = 1;
        body.replica.current_role = 2;
        body.replica.state = 1;
        body.replica.up = true;
        body.replica.service_endpoint.clear();
        body.replica.replication_endpoint.clear();
        if body.replica.version != body.service.version {
            return Err("peer packages do not share the original deployment version".into());
        }
        self.idle_predecessor = Some(json!({
            "build":self.idle_status(),
            "removal":self.idle_removal_status(),
            "deletion":self.idle_deletion_status(),
            "disposal":self.placements[1].retirement,
            "configuration":self.configuration_status(),
        }));
        let placement = &mut self.placements[1];
        placement
            .retired
            .push((placement.replica, placement.incarnation));
        placement.replica = replica;
        placement.incarnation = incarnation;
        placement.active_unit = Some(body.unit.clone());
        placement.ready_replica = Some(body.replica.clone());
        placement.service_description = Some(body.service.clone());
        placement.created = false;
        placement.observed = Observed::Absent;
        placement.remove_received = false;
        placement.retirement = None;
        placement.reusable = false;
        placement.error = None;
        placement.endpoint = None;
        self.idle_removal = None;
        self.idle_deletion = None;
        self.idles = vec![IdleBuild {
            body: body.clone(),
            reply: None,
            cancelled: false,
            unknown: false,
            started: now,
            last: now,
        }];
        Ok(body)
    }
    pub fn idle_reply(&mut self, index: usize, body: AddReplicaReply) -> Result<()> {
        let Some(idle) = self
            .idles
            .iter_mut()
            .find(|i| i.body.replica.node == body.replica.node)
        else {
            return Ok(());
        };
        if index != 0
            || idle.cancelled
            || idle.unknown
            || idle.reply.is_some()
            || self.placements[0].observed != Observed::Running
            || self.placements.iter().any(|p| !p.link_alive)
            || self.placements.iter().any(|p| p.loss_reported)
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
        if failed && self.placements[0].launch.profile == crate::ServiceProfile::StatefulPeerIdle {
            self.request_idle_removal(Instant::now())?;
        }
        Ok(())
    }
    pub fn idle_retry(&mut self, index: usize, now: Instant) -> Option<AddReplica> {
        let idle = self.idles.iter_mut().find(|i| {
            i.reply.is_none()
                && !i.cancelled
                && !i.unknown
                && action_retry_due(i.started, i.last, now)
        })?;
        if index != 0
            || self.generation.is_some()
            || idle.cancelled
            || idle.unknown
            || idle.reply.is_some()
            || self.placements[0].observed != Observed::Running
            || self.placements[1].loss_reported
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
        if self.placements[0].launch.profile == crate::ServiceProfile::StatefulThree {
            let builds: Vec<_> = self.idles.iter().map(|idle| {
                let error = idle.reply.as_ref().map(|r| r.error);
                let state = if idle.unknown { "unknown" }
                    else if idle.cancelled { "cancelled" }
                    else if error == Some(0) { "ready" }
                    else if error.is_some() { "failed" } else { "pending" };
                json!({"state":state,"ready":state == "ready","request":idle.body,"reply":idle.reply,
                    "to_be_dropped":error.is_some_and(|e| e != 0),
                    "cleanup_required":error.is_some_and(|e| e != 0)})
            }).collect();
            let ready = builds.len() == 2 && builds.iter().all(|b| b["ready"] == true);
            let state = if builds.iter().any(|b| b["state"] == "unknown") {
                "unknown"
            } else if builds.iter().any(|b| b["state"] == "failed") {
                "failed"
            } else if ready {
                "ready"
            } else if builds.is_empty() {
                "absent"
            } else {
                "pending"
            };
            return json!({"state":state,
                "ready":ready,"serving":false,"builds":builds});
        }
        let Some(idle) = self.idles.first() else {
            return json!({"state":"absent","ready":false});
        };
        let error = idle.reply.as_ref().map(|reply| reply.error);
        let destination_lost = self.placements[1].loss_reported;
        let state = if destination_lost {
            "lost"
        } else if idle.unknown {
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
            "destination_lost":destination_lost,
            "native_replica_deleted":self.placements[1].remove_received,
            "to_be_dropped":(error.is_some_and(|e| e != 0) || self.idle_deletion.is_some())
                && !self.idle_destination_deleted(),
            "destination_cleanup_required":(destination_lost || error.is_some_and(|e| e != 0) || self.idle_deletion.is_some())
                && !self.idle_deletion_complete(),
            "error":error, "cleanup_required":(destination_lost || idle.cancelled || error.is_some_and(|e| e != 0))
                && self.idle_removal.as_ref().is_none_or(|removal| removal.reply.is_none())})
    }
    pub fn request_idle_removal(&mut self, now: Instant) -> Result<RemoveReplica> {
        if self.configuration.is_some()
            || self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
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
            .idles
            .first()
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
        if let Some(idle) = self.idles.first_mut() {
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
            "destination_deleted":self.idle_destination_deleted(), "serving":false})
    }
    fn idle_destination_deleted(&self) -> bool {
        self.idle_deletion
            .as_ref()
            .is_some_and(|deletion| deletion.reply.is_some())
    }
    fn idle_deletion_complete(&self) -> bool {
        self.idle_destination_deleted()
            && (self.placements[1].retirement.is_some()
                || self.idle_deletion.as_ref().is_some_and(|deletion| {
                    deletion.absence.is_some()
                        || deletion.cancellation.is_some()
                        || deletion.failed_registration.is_some()
                        || deletion.failed_open.is_some()
                        || deletion.host_loss.is_some()
                }))
    }
    pub fn idle_absence_allowed(&self) -> bool {
        matches!((self.idles.first(), &self.idle_removal), (Some(idle), Some(removal))
            if idle.reply.is_none() && !idle.unknown && !removal.unknown && idle.body == removal.body)
    }
    pub fn idle_failed_creation_error(&self) -> Option<i64> {
        let (Some(idle), Some(removal)) = (self.idles.first(), &self.idle_removal) else {
            return None;
        };
        (!idle.unknown && !removal.unknown && removal.failed_creation && idle.body == removal.body)
            .then(|| {
                idle.reply
                    .as_ref()
                    .map(|reply| reply.error)
                    .filter(|error| *error != 0)
            })
            .flatten()
    }
    /// Native PendingTask schedules Delete independently of PendingRemove.
    /// Each failure/absence branch retains its original FM Add lineage.
    pub fn request_idle_deletion(
        &mut self,
        now: Instant,
        target: IdleDeleteTarget,
    ) -> Result<DeleteReplica> {
        self.request_idle_deletion_with_force(now, target, false)
    }
    pub fn request_idle_deletion_with_force(
        &mut self,
        now: Instant,
        target: IdleDeleteTarget,
        force: bool,
    ) -> Result<DeleteReplica> {
        if (self.configuration.is_some() && !self.retired_secondary())
            || self.placements[0].launch.profile != crate::ServiceProfile::StatefulPeerIdle
            || self.generation.is_some()
            || !self.placements[1].link_alive
            || !self.placements[1].activated
            || self.placements[1].bootstrap != Bootstrap::Admitted
        {
            return Err("Idle deletion requires the original admitted destination session".into());
        }
        if self.idle_deletion_complete() {
            return Err("original destination deletion is already complete".into());
        }
        if let Some(deletion) = &mut self.idle_deletion {
            if deletion.unknown {
                return Err("original destination deletion is unknown".into());
            }
            deletion.last = now;
            if force {
                deletion.body.deletion.0.force = true;
            }
            return Ok(deletion.body.clone());
        }
        let body = if self.retired_secondary() {
            if force || target != IdleDeleteTarget::Runtime {
                return Err("configured Secondary requires normal disposal-backed Delete".into());
            }
            DeleteReplica::new(self.placements[1].body()?, false)
        } else {
            let removal = self
                .idle_removal
                .as_ref()
                .ok_or("Idle deletion requires original FM abandonment")?;
            if removal.unknown || self.placements[0].observed != Observed::Running {
                return Err("original Idle abandonment owner is unknown".into());
            }
            DeleteReplica::new(removal.body.clone(), force)
        };
        let failure_error = self.idle_failed_creation_error();
        if (matches!(
            target,
            IdleDeleteTarget::FailedRegistration | IdleDeleteTarget::FailedOpen
        ) && failure_error.is_none())
            || (matches!(
                target,
                IdleDeleteTarget::Unobserved | IdleDeleteTarget::PreRegistration
            ) && !self.idle_absence_allowed())
        {
            return Err("destination cleanup requires matching original failed or unanswered FM Add lineage".into());
        }
        self.idle_deletion = Some(IdleDeletion {
            body: body.clone(),
            reply: None,
            target,
            absence: None,
            cancellation: None,
            failed_registration: None,
            failed_open: None,
            host_loss: None,
            failure_error: if matches!(
                target,
                IdleDeleteTarget::FailedRegistration | IdleDeleteTarget::FailedOpen
            ) {
                failure_error
            } else {
                None
            },
            unknown: false,
            started: now,
            last: now,
        });
        if target == IdleDeleteTarget::Runtime {
            self.placements[1].created = true;
        }
        self.placements[1].observed = Observed::Pending;
        Ok(body)
    }
    pub fn idle_deletion_reply(&mut self, index: usize, body: DeleteReplicaReply) -> Result<()> {
        let Some(deletion) = &mut self.idle_deletion else {
            return Ok(());
        };
        let expected = &deletion.body.replica.0;
        if index != 1
            || deletion.unknown
            || deletion.reply.is_some()
            || !self.placements[1].link_alive
            || (body.unit != expected.unit
                && !(deletion.target == IdleDeleteTarget::Unobserved
                    && body.unit == absent_replica_body(expected).unit))
            || body.replica.node != expected.replica.node
            || body.replica.id != expected.replica.id
            || body.replica.instance != expected.replica.instance
            || body.error != 0
        {
            return Ok(());
        }
        if body.replica != dropped_replica(&expected.replica) {
            return Err("Idle DeleteReplicaReply is not the native dropped description".into());
        }
        if deletion
            .absence
            .as_ref()
            .is_some_and(|proof| proof.absence.body.unit != body.unit)
            || deletion
                .host_loss
                .as_ref()
                .is_some_and(|proof| proof.loss.body.unit != body.unit)
            || deletion
                .cancellation
                .as_ref()
                .is_some_and(|proof| proof.cancellation.body.unit != body.unit)
            || deletion
                .failed_registration
                .as_ref()
                .is_some_and(|proof| proof.deletion.failure.body.unit != body.unit)
            || deletion
                .failed_open
                .as_ref()
                .is_some_and(|proof| proof.deletion.failure.disposal.body.unit != body.unit)
            || self.placements[1]
                .retirement
                .as_ref()
                .is_some_and(|proof| proof.disposal.body.unit != body.unit)
        {
            return Err(
                "destination Delete reply does not match its original cleanup proof".into(),
            );
        }
        deletion.reply = Some(body);
        self.placements[1].remove_received = true;
        self.finish_idle_deletion();
        Ok(())
    }
    fn finish_idle_deletion(&mut self) {
        if self.idle_deletion_complete() {
            let placement = &mut self.placements[1];
            placement.observed = Observed::Absent;
            placement.endpoint = None;
            placement.error = None;
            // Disposal of the predecessor is not fresh-lifetime authorization.
            placement.reusable = false;
        }
    }
    pub fn idle_deletion_retry(&mut self, index: usize, now: Instant) -> Option<DeleteReplica> {
        let deletion = self.idle_deletion.as_mut()?;
        if index != 1
            || deletion.unknown
            || deletion.reply.is_some()
            || self.generation.is_some()
            || !self.placements[1].link_alive
            || !self.placements[1].activated
            || !action_retry_due(deletion.started, deletion.last, now)
        {
            return None;
        }
        deletion.last = now;
        Some(deletion.body.clone())
    }
    pub fn idle_deletion_status(&self) -> Value {
        let Some(deletion) = &self.idle_deletion else {
            return json!({"state":"absent","destination_deleted":false,"disposed":false,"never_created":false,"pre_open_cancelled":false,"failed_registration":false,"failed_open":false,"complete":false});
        };
        let complete = self.idle_deletion_complete();
        json!({"state":if complete { "deleted" } else if deletion.unknown { "unknown" } else { "pending" },
            "cleanup_state":if complete { "complete" } else if deletion.unknown { "unknown" }
                else if deletion.reply.is_some() { "unproved" } else { "pending_reply" },
            "kind":"fm_idle_deletion", "request":deletion.body, "reply":deletion.reply,
            "destination_deleted":self.idle_destination_deleted(),
            "disposed":self.placements[1].retirement.is_some() || deletion.failed_open.is_some(),
            "host_lost":deletion.host_loss.is_some(),
            "force":deletion.body.deletion.0.force,
            "never_created":deletion.absence.is_some(),
            "pre_open_cancelled":deletion.cancellation.is_some(),
            "failed_registration":deletion.failed_registration.is_some(),
            "failed_open":deletion.failed_open.is_some(),
            "completion":if !complete { None } else if self.placements[1].retirement.is_some() { Some("disposed") }
                else if deletion.absence.is_some() { Some("never_created") }
                else if deletion.cancellation.is_some() { Some("pre_open_cancelled") }
                else if deletion.failed_registration.is_some() { Some("failed_registration") }
                else if deletion.failed_open.is_some() { Some("failed_open") }
                else if deletion.host_loss.is_some() { Some("host_lost") } else { None },
            "complete":complete, "cleanup_required":!complete,
            "pending_remove":self.idle_removal.as_ref().is_some_and(|removal| removal.reply.is_none()),
            "reusable":false, "serving":false})
    }
    /// Native Replica::CanGenerateAction and PendingTask's creating-primary branch.
    pub fn primary_retry(&mut self, index: usize, now: Instant) -> Result<Option<AddPrimary>> {
        self.retry_body(index, PendingPhase::Create, now)
    }
    pub fn deletion_retry(&mut self, index: usize, now: Instant) -> Result<Option<DeleteReplica>> {
        if index == 1 && self.idle_deletion.is_some() {
            return Ok(self.idle_deletion_retry(index, now));
        }
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
        let body = if phase == PendingPhase::Delete
            && index == 1
            && placement.launch.profile == crate::ServiceProfile::StatefulPeerIdle
            && placement.loss_reported
        {
            self.idles
                .first()
                .ok_or("missing original Idle creation")?
                .body
                .clone()
        } else {
            placement.body()?
        };
        placement.action_last_at = Some(now);
        Ok(Some(body))
    }
}
