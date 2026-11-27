pub use crate::control::Command;
use crate::control::{
    CommandAction, CommandRequest, OperationOutcome, OperationPhase, OperationState,
};
use crate::profile::{
    absent_replica_body, add_body, dropped_replica, pre_open_cancelled_body, AbsenceReceipt,
    ActualProfile, FailedOpenReceipt, FailedRegistrationReceipt, HostLossReceipt, Launch,
    PreOpenCancellationReceipt, Result, RetirementReceipt,
};
use crate::status::*;
use fube_ra::protocol::*;
use fube_ra::protocol::{NativeReplicaRole, NativeReplicaState};
use fubst::serde::ObjectArray;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

mod node_lifecycle;
pub(crate) use node_lifecycle::NodeFailure;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Desired {
    Absent,
    Running,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Observed {
    Absent,
    Pending,
    Running,
    Lost,
    Unknown,
    Error,
    Down,
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
    pub(crate) node_down: Option<crate::profile::NodeDownReceipt>,
    remove_received: bool,
    pending: Option<PendingPhase>,
    action_updated_at: Option<Instant>,
    action_last_at: Option<Instant>,
}
#[derive(Clone)]
pub struct Operation {
    pub command: CommandRequest,
    pub outcome: OperationOutcome,
}
pub struct Ledger {
    pub(crate) recovery_mode: crate::RecoveryMode,
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
    candidate_changes: Vec<Configuration>,
    configuration_history: Vec<ActiveConfiguration>,
    pub(crate) repair: Option<ReplicaRepair>,
    pub(crate) repair_history: Vec<ReplicaRepair>,
    pub(crate) restoration: Option<[AddInstance; 2]>,
    pub(crate) node_failures: Vec<NodeFailure>,
    node_reconfiguration: Option<Epoch>,
}
pub(crate) struct ReplicaRepair {
    pub predecessor: AddInstance,
    pub old_profile: ActualProfile,
    pub profile: Option<ActualProfile>,
    pub build: IdleBuild,
    pub configuration: Option<ActiveConfiguration>,
    pub removal: Option<ActiveConfiguration>,
    cancellation: Option<IdleRemoval>,
    deletion: Option<RepairDeletion>,
    base: Option<ActiveConfiguration>,
    pub host_requested: bool,
    superseded_by: Option<Epoch>,
    pub(crate) unknown: bool,
}
impl ReplicaRepair {
    pub(crate) fn uses_spare(&self) -> bool {
        self.predecessor.replica.node != self.build.body.replica.node
    }

    pub(crate) fn replicator_removed(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(|removal| removal.reply.is_some())
    }
}
struct RepairDeletion {
    reply: Option<DeleteReplicaReply>,
    disposal: Option<RetirementReceipt>,
    started: Instant,
    last: Instant,
}
#[derive(Clone)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationPhase {
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
    proposed: Vec<bool>,
    pub updates_sent: Vec<bool>,
    pub uploads: Vec<Option<LfumUpload>>,
    initial_membership: Option<Configuration>,
    initial_builds: Option<Vec<AddReplica>>,
    failover: Option<ConfigurationInventory>,
    transfer: Option<ConfigurationInventory>,
    repair: Option<RepairInventory>,
    removal: Option<RemovalInventory>,
    retry_at: Vec<Instant>,
}
struct ConfigurationInventory {
    previous: ConfigurationReply,
    configuration: Configuration,
}
struct RepairInventory {
    previous: ConfigurationReply,
    build: AddReplica,
    configuration: Option<Configuration>,
    cancelled: bool,
    deleting: bool,
}
struct RemovalInventory {
    previous: ConfigurationReply,
    removal: Configuration,
    supersession: Option<Configuration>,
}
impl GenerationHandshake {
    pub fn new(current: &GenerationNumber, nodes: usize) -> Result<Self> {
        Ok(Self {
            number: Self::successor(current, current.owner)?,
            phase: GenerationPhase::Proposing,
            proposed: vec![false; nodes],
            updates_sent: vec![false; nodes],
            uploads: vec![None; nodes],
            initial_membership: None,
            initial_builds: None,
            failover: None,
            transfer: None,
            repair: None,
            removal: None,
            retry_at: vec![Instant::now(); nodes],
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
        self.proposed.fill(false);
        self.updates_sent.fill(false);
        self.uploads.fill(None);
        self.retry_at.fill(Instant::now());
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
            self.retry_at.fill(Instant::now());
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
    fn status(&self) -> GenerationStatus {
        GenerationStatus {
            phase: self.phase,
            complete: self.phase == GenerationPhase::Complete,
            details: [
                ("generation".into(), json!(self.number)),
                ("proposal_replies".into(), json!(self.proposed)),
                ("updates_sent".into(), json!(self.updates_sent)),
                ("uploads".into(), json!(self.uploads)),
                ("upload_supported".into(), json!(true)),
            ]
            .into_iter()
            .collect(),
        }
    }
}
pub enum Accepted {
    NodeControl(usize, bool, NodeActivationRequest),
    Duplicate(Box<OperationOutcome>),
    Create(usize, Box<AddInstance>),
    Delete(usize, Box<RemoveInstance>),
    Reconfigure(Box<DoReconfiguration>),
    Build,
    Cancel(Box<RemoveReplica>),
    Progress,
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
            node_down: None,
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
                        || info.replicas.0[0].intermediate_role != NativeReplicaRole::None
                    {
                        return Err("unsupported stateful terminal report".into());
                    }
                    continue;
                }
                if dropped {
                    // This ephemeral FM can semantically forget the exact
                    // dropped instance only after desired deletion. Unexpected
                    // loss remains uncertain rather than enabling replacement.
                    if replica.state != NativeReplicaState::Dropped
                        || self.desired != Desired::Absent
                    {
                        return Err("unrequested dropped authority remains staged".into());
                    }
                } else if replica.up
                    || replica.state == NativeReplicaState::Dropped
                    || info.local_deleted
                {
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
    #[cfg(test)]
    pub fn status(&self) -> Value {
        json!(self.status_typed())
    }

    pub fn status_typed(&self) -> NodeStatus {
        NodeStatus {
            node: self.launch.placement, node_instance: self.launch.node.instance,
            desired: self.desired, observed: self.observed, endpoint: self.endpoint.clone(),
            created_lifetime: self.created, link_alive: self.link_alive,
            admitted: self.bootstrap == Bootstrap::Admitted, retained_work: None,
            details: [
            ("node_id".into(), json!(crate::profile::node_hex(&self.launch.node))),
            ("session".into(), json!(self.launch.session)),
            ("partition".into(), json!(self.launch.partition)),
            ("replica".into(), json!(self.replica)),
            ("incarnation".into(), json!(self.incarnation)),
            ("service".into(), json!(self.launch.profile.service())),
            ("service_type".into(), json!(crate::profile::fixed_type_for(self.launch.profile).name)),
            ("error".into(), json!(self.error)),
            ("node_down".into(), json!(self.node_down.as_ref().map(|proof| json!({
                "instance":proof.node.instance,"session":proof.session,
                "node_pid":proof.node_pid,"fenced":proof.node_group_drained && proof.app_group_drained
            })))),
            ("catalog_notified".into(), json!(self.catalog_notified)),
            ("reusable".into(), json!(self.reusable)),
            ("retired_lifetimes".into(), json!(self.retired.len())),
            ("retirement".into(), json!(self.retirement)),
            ("loss_reported".into(), json!(self.loss_reported)),
            ("native_replica_deleted".into(), json!(self.remove_received)),
            ("activated".into(), json!(self.activated)),
            ("activation_sequence".into(), json!(self.activation_sequence)),
            ("node_control_pending".into(), json!(self.node_control)),
            ("type_enabled".into(), json!(self.type_enabled)),
            ("type_sequence".into(), json!(self.type_sequence)),
            ("service_update_version".into(), json!(self.service_description.as_ref().map(|s| s.update_version))),
            ("service_update_pending".into(), json!(self.service_update.is_some())),
            ("load_report_received".into(), json!(self.last_load.is_some())),
            ("app_pid".into(), json!(self.profile.as_ref().map(|p| p.app_pid))),
            ("runtime_root".into(), json!(self.profile.as_ref().map(|p| &p.runtime_root))),
            ].into_iter().collect(),
        }
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
        || actual.current_role
            != if canonical {
                NativeReplicaRole::None
            } else {
                expected.current_role
            }
        || actual.previous_role
            != if canonical {
                NativeReplicaRole::None
            } else {
                expected.previous_role
            }
    {
        return Err("foreign/stale replica incarnation/version".into());
    }
    Ok(())
}
fn completed_inventory(configuration: &Configuration) -> Configuration {
    let mut completed = configuration.clone();
    completed.unit.previous = Epoch::default();
    completed.replicas.0.retain(|r| r.current_role.is_active());
    for replica in &mut completed.replicas.0 {
        replica.previous_role = NativeReplicaRole::None;
    }
    completed
}

fn inventory_view_matches(
    info: &FailoverUnitInfo,
    configuration: &Configuration,
    local: &NodeInstance,
    intermediate: Option<&Configuration>,
    promoted: Option<&NodeInstance>,
) -> bool {
    if info.unit != configuration.unit
        || info.service != configuration.service
        || info.local_deleted
        || info.intermediate != intermediate.map_or(Epoch::default(), |c| c.unit.current)
        || info.replicas.0.len() != configuration.replicas.0.len()
        || info.report_from_primary
            != configuration
                .replicas
                .0
                .iter()
                .any(|r| &r.node == local && r.current_role == NativeReplicaRole::Primary)
    {
        return false;
    }
    configuration.replicas.0.iter().all(|expected| {
        let mut matches = info
            .replicas
            .0
            .iter()
            .filter(|r| r.description.node == expected.node);
        let Some(actual) = matches.next() else {
            return false;
        };
        if matches.next().is_some() {
            return false;
        }
        let mut expected = expected.clone();
        expected.first_lsn = -1;
        expected.last_lsn = -1;
        if promoted == Some(&expected.node) && expected.current_role == NativeReplicaRole::Primary {
            expected.service_endpoint = actual.description.service_endpoint.clone();
        }
        let intermediate_role = intermediate
            .and_then(|c| {
                c.replicas.0.iter().find(|r| {
                    r.node == expected.node
                        && r.id == expected.id
                        && r.instance == expected.instance
                })
            })
            .map_or(NativeReplicaRole::None, |r| r.current_role);
        actual.description == expected && actual.intermediate_role == intermediate_role
    })
}

fn primary_failover_configuration(body: &Configuration) -> bool {
    body.replicas.0.iter().any(|r| {
        r.previous_role == NativeReplicaRole::Primary
            && matches!(
                r.current_role,
                NativeReplicaRole::None | NativeReplicaRole::Secondary
            )
            && !r.up
    })
}
impl Ledger {
    pub fn primary_failover(&self) -> bool {
        self.swap
            .as_ref()
            .is_some_and(|s| primary_failover_configuration(&s.body.configuration))
    }

    fn primary_transition_live(&self) -> bool {
        if self.node_reconfiguration.is_some_and(|epoch| {
            self.swap
                .as_ref()
                .is_some_and(|swap| swap.body.configuration.unit.current == epoch)
        }) || self.placements[0]
            .launch
            .profile
            .configured_sizes()
            .is_some()
        {
            let planned = !self.primary_failover();
            return self.swap.as_ref().is_some_and(|s| {
                s.body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.up && r.current_role.is_active())
                    .all(|r| {
                        self.placements.iter().any(|p| {
                            p.launch.node == r.node
                                && p.link_alive
                                && (!p.loss_reported
                                    || (planned && r.current_role == NativeReplicaRole::Secondary))
                        })
                    })
            });
        }
        let lost = self.failed_primary_index();
        self.placements
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                Some(*i) != lost || self.repair.as_ref().is_some_and(|r| r.profile.is_some())
            })
            .all(|(_, p)| p.link_alive && !p.loss_reported)
    }

    pub fn start_primary_failover(&mut self, now: Instant) -> Result<Option<DoReconfiguration>> {
        if let Some(body) = self.secondary_removal_failover()? {
            self.install_primary_failover(body.clone(), now)?;
            return Ok(Some(body));
        }
        if self.restoration.is_some() {
            return Ok(None);
        }
        let configured = self.placements[0]
            .launch
            .profile
            .configured_sizes()
            .is_some();
        if (!configured
            && self.placements[0].launch.profile != crate::ServiceProfile::StatefulThree)
            || self.generation_pending()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self
                .repair
                .as_ref()
                .is_some_and(|r| r.configuration.as_ref().is_none_or(|c| c.reply.is_none()))
            || self.swap.as_ref().is_some_and(|s| {
                !self
                    .repair
                    .as_ref()
                    .is_some_and(|r| r.configuration.as_ref().is_some_and(|c| c.reply.is_some()))
                    && (self.primary_failover()
                        || s.unknown
                        || s.reply.as_ref().is_none_or(|reply| reply.error != 0))
            })
        {
            return Ok(None);
        }
        let Some(active) = self
            .repair
            .as_ref()
            .and_then(|r| r.configuration.as_ref())
            .into_iter()
            .chain(self.swap.iter())
            .chain(self.configuration.iter())
            .filter(|c| {
                c.reply.as_ref().is_some_and(|reply| reply.error == 0)
                    && (configured
                        || (c.body.configuration.unit.target == 3
                            && c.body.configuration.unit.minimum == 2))
            })
            .max_by_key(|c| {
                let epoch = c
                    .reply
                    .as_ref()
                    .expect("completed configuration")
                    .unit
                    .current;
                (epoch.data_loss, epoch.configuration)
            })
        else {
            return Ok(None);
        };
        let previous = active.reply.as_ref().expect("completed configuration");
        if previous.replicas.0.len() < 2 {
            return Ok(None);
        }
        let lost = self.placements.iter().position(|p| {
            previous.replicas.0.iter().any(|r| {
                r.node == p.launch.node
                    && r.current_role == NativeReplicaRole::Primary
                    && p.loss_reported
            })
        });
        let Some(lost) = lost else {
            return Ok(None);
        };
        if !self.placements[lost].link_alive
            || self.placements.iter().enumerate().any(|(i, p)| {
                if !previous.replicas.0.iter().any(|r| r.node == p.launch.node) {
                    return false;
                }
                if configured && i != lost && (!p.link_alive || p.loss_reported) {
                    return false;
                }
                !p.link_alive
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
                    || (i != lost && (p.loss_reported || p.observed != Observed::Running))
            })
        {
            return Ok(None);
        }
        let available =
            |p: &Placement| p.link_alive && !p.loss_reported && p.observed == Observed::Running;
        if configured
            && previous
                .replicas
                .0
                .iter()
                .filter(|r| {
                    r.current_role.is_active()
                        && self
                            .placements
                            .iter()
                            .any(|p| p.launch.node == r.node && available(p))
                })
                .count()
                < previous.replicas.0.len().div_ceil(2)
        {
            return Ok(None);
        }
        let candidate = self
            .placements
            .iter()
            .position(|p| {
                previous.replicas.0.iter().any(|r| {
                    r.node == p.launch.node
                        && r.current_role == NativeReplicaRole::Secondary
                        && r.up
                        && r.state == NativeReplicaState::Ready
                        && available(p)
                })
            })
            .ok_or("missing current ActiveSecondary candidate")?;
        let mut body = active.body.clone();
        body.configuration.unit = previous.unit.clone();
        body.configuration.unit.previous = previous.unit.current;
        body.configuration.unit.current.configuration = previous
            .unit
            .current
            .configuration
            .checked_add(0x1_0000_0001)
            .ok_or("Primary failover epoch exhausted")?;
        body.configuration.replicas = previous.replicas.clone();
        for r in &mut body.configuration.replicas.0 {
            r.previous_role = r.current_role;
            if r.node == self.placements[lost].launch.node {
                r.current_role = NativeReplicaRole::Secondary;
                r.up = false;
                r.state = NativeReplicaState::Dropped;
            } else if r.node == self.placements[candidate].launch.node {
                r.current_role = NativeReplicaRole::Primary;
            } else if configured {
                let placement = self
                    .placements
                    .iter()
                    .find(|p| p.launch.node == r.node)
                    .ok_or("missing configured member owner")?;
                r.up = available(placement);
                if placement.loss_reported {
                    r.state = NativeReplicaState::Dropped;
                }
            }
            r.first_lsn = -1;
            r.last_lsn = -1;
        }
        let mut count = body
            .configuration
            .replicas
            .0
            .iter()
            .filter(|r| r.current_role.is_active())
            .count();
        for r in &mut body.configuration.replicas.0 {
            if !r.up
                && r.current_role == NativeReplicaRole::Secondary
                && count > body.configuration.unit.minimum as usize
            {
                r.current_role = NativeReplicaRole::None;
                count -= 1;
            }
        }
        body.phase0_duration = i64::MAX;
        self.install_primary_failover(body.clone(), now)?;
        Ok(Some(body))
    }

    fn secondary_removal_failover(&self) -> Result<Option<DoReconfiguration>> {
        let Some(repair) = self.repair.as_ref().filter(|r| {
            !r.host_requested
                && r.profile.is_none()
                && r.configuration.is_none()
                && r.build.reply.is_none()
        }) else {
            return Ok(None);
        };
        let Some(removal) = repair.removal.as_ref().filter(|c| c.reply.is_none()) else {
            return Ok(None);
        };
        let configuration = &removal.body.configuration;
        let Some(primary) = configuration.replicas.0.iter().find(|r| {
            r.previous_role == NativeReplicaRole::Primary
                && r.current_role == NativeReplicaRole::Primary
        }) else {
            return Ok(None);
        };
        if (self.generation_pending()
            && self
                .generation
                .as_ref()
                .and_then(|g| g.removal.as_ref())
                .is_none_or(|inventory| inventory.removal != *configuration))
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
                    || if p.launch.node == primary.node
                        || p.launch.node == repair.predecessor.replica.node
                    {
                        !p.loss_reported
                    } else {
                        p.loss_reported || p.observed != Observed::Running
                    }
            })
        {
            return Ok(None);
        }
        let available = |r: &&Replica| r.up && r.node != primary.node;
        for previous in [true, false] {
            let members: Vec<_> = configuration
                .replicas
                .0
                .iter()
                .filter(|r| {
                    if previous {
                        r.previous_role.is_active()
                    } else {
                        r.current_role.is_active()
                    }
                })
                .collect();
            if members.iter().copied().filter(available).count() < members.len().div_ceil(2) {
                return Ok(None);
            }
        }
        let candidate = configuration
            .replicas
            .0
            .iter()
            .find(|r| r.up && r.current_role == NativeReplicaRole::Secondary)
            .ok_or("missing surviving Secondary during removal")?;
        let mut body = removal.body.clone();
        body.configuration.unit.current.configuration = configuration
            .unit
            .current
            .configuration
            .checked_add(0x1_0000_0001)
            .ok_or("interrupted Secondary removal epoch exhausted")?;
        // SF preserves PC and the CC set during an in-flight Primary change.
        for r in &mut body.configuration.replicas.0 {
            if r.node == primary.node {
                r.current_role = NativeReplicaRole::Secondary;
                r.up = false;
                r.state = NativeReplicaState::Dropped;
            } else if r.node == candidate.node {
                r.current_role = NativeReplicaRole::Primary;
            }
            r.first_lsn = -1;
            r.last_lsn = -1;
        }
        Ok(Some(body))
    }

    pub(crate) fn supersede_rebuild(
        &mut self,
        predecessor: usize,
        operation: &str,
        superseding_operation: &str,
    ) -> Result<DoReconfiguration> {
        let target = self
            .operations
            .iter()
            .find(|target| target.command.op == operation)
            .ok_or("unknown rebuild operation to supersede")?;
        let repair = self
            .repair
            .as_ref()
            .ok_or("no rebuild owner to supersede")?;
        let removal = repair
            .removal
            .as_ref()
            .ok_or("supersession requires pending Secondary removal")?;
        if !matches!(target.command.action, CommandAction::RebuildReplica { .. })
            || !matches!(
                target.outcome.state,
                OperationState::Pending | OperationState::Unknown
            )
            || removal.reply.is_some()
            || repair.base.is_some()
            || repair.host_requested
            || repair.profile.is_some()
            || repair.build.reply.is_some()
            || repair.configuration.is_some()
            || !target.outcome.owns_replica(&repair.build.body.replica)
            || !removal.body.configuration.replicas.0.iter().any(|replica| {
                replica.node == self.placements[predecessor].launch.node
                    && replica.id == self.placements[predecessor].replica
                    && replica.instance == self.placements[predecessor].incarnation
                    && replica.previous_role == NativeReplicaRole::Primary
                    && replica.current_role == NativeReplicaRole::Primary
            })
        {
            return Err("supersession must bind the original pending removal and exact failed Primary before replacement".into());
        }
        let mut outcome = target.outcome.clone();
        let body = self.secondary_removal_failover()?.ok_or(
            "supersession requires the original live sessions and both surviving read quorums",
        )?;
        self.install_primary_failover(body.clone(), Instant::now())?;
        outcome.state = OperationState::Superseded;
        outcome.phase = Some(OperationPhase::Superseded);
        outcome.superseded_by = Some(superseding_operation.into());
        outcome.error = Some(json!(
            "pending removal superseded by explicit Primary failure reconfiguration"
        ));
        self.complete(operation, outcome);
        Ok(body)
    }

    fn install_primary_failover(&mut self, body: DoReconfiguration, now: Instant) -> Result<()> {
        if let Some(repair) = self
            .repair
            .as_ref()
            .filter(|r| r.removal.as_ref().is_some_and(|c| c.reply.is_none()))
        {
            let lost = self
                .placements
                .iter()
                .find(|p| {
                    body.configuration.replicas.0.iter().any(|r| {
                        r.node == p.launch.node
                            && r.previous_role == NativeReplicaRole::Primary
                            && !r.up
                    })
                })
                .ok_or("missing superseded Primary owner")?;
            self.restoration = Some([lost.body()?, repair.predecessor.clone()]);
        }
        for p in &mut self.placements {
            let Some(replica) = body
                .configuration
                .replicas
                .0
                .iter()
                .find(|r| r.node == p.launch.node && r.up && r.current_role.is_active())
            else {
                continue;
            };
            p.active_unit = Some(body.configuration.unit.clone());
            p.ready_replica = Some(replica.clone());
            p.endpoint = None;
        }
        if let Some(mut repair) = self.repair.take() {
            if repair.removal.as_ref().is_some_and(|c| c.reply.is_none()) {
                repair.superseded_by = Some(body.configuration.unit.current);
            }
            self.repair_history.push(repair);
        }
        if let Some(swap) = self.swap.take() {
            self.configuration_history.push(swap);
        }
        self.candidate_changes.clear();
        self.swap_endpoint = None;
        self.swap = Some(ActiveConfiguration {
            body: body.clone(),
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        if let Some(inventory) = self
            .generation
            .as_mut()
            .and_then(|g| g.removal.as_mut())
            .filter(|inventory| {
                inventory.removal.unit.previous == body.configuration.unit.previous
                    && inventory
                        .removal
                        .unit
                        .current
                        .configuration
                        .checked_add(0x1_0000_0001)
                        == Some(body.configuration.unit.current.configuration)
            })
        {
            inventory.supersession = Some(body.configuration.clone());
        }
        Ok(())
    }

    pub fn change_configuration(
        &mut self,
        index: usize,
        body: Configuration,
        now: Instant,
    ) -> Result<Option<FailoverUnitReply>> {
        let Some(swap) = self
            .swap
            .as_ref()
            .filter(|s| self.primary_failover() && !s.unknown)
        else {
            return Ok(None);
        };
        if self.candidate_changes.iter().any(|previous| {
            previous == &body
                && previous.replicas.0.iter().any(|r| {
                    r.current_role == NativeReplicaRole::Primary
                        && r.node == self.placements[index].launch.node
                })
        }) {
            return Ok(Some(FailoverUnitReply {
                unit: swap.body.configuration.unit.clone(),
                error: 0,
            }));
        }
        if swap.reply.is_some()
            || index != self.transition_primary_index()
            || body.unit != swap.body.configuration.unit
            || body.service != swap.body.configuration.service
            || body.replicas.0.len() != swap.body.configuration.replicas.0.len()
            || !self.primary_transition_live()
        {
            return Ok(None);
        }
        let mut selected = None;
        for expected in &swap.body.configuration.replicas.0 {
            let actual: Vec<_> = body
                .replicas
                .0
                .iter()
                .filter(|r| r.node == expected.node)
                .collect();
            let [actual] = actual.as_slice() else {
                return Ok(None);
            };
            let mut normalized = (*actual).clone();
            normalized.first_lsn = expected.first_lsn;
            normalized.last_lsn = expected.last_lsn;
            if normalized != *expected {
                return Ok(None);
            }
            if actual.last_lsn >= 0 {
                if actual.first_lsn < 0
                    || !actual.up
                    || actual.current_role != NativeReplicaRole::Secondary
                    || selected.is_some()
                {
                    return Ok(None);
                }
                selected = Some(actual.node.clone());
            } else if actual.first_lsn != -1 || actual.last_lsn != -1 {
                return Ok(None);
            }
        }
        let Some(selected) = selected else {
            return Ok(None);
        };
        if self.candidate_changes.len() >= self.placements.len() {
            return Err("configured failover candidate redirection budget exhausted".into());
        }
        let mut next = swap.body.clone();
        next.configuration.unit.current.configuration = next
            .configuration
            .unit
            .current
            .configuration
            .checked_add(0x1_0000_0001)
            .ok_or("Primary redirection epoch exhausted")?;
        for r in &mut next.configuration.replicas.0 {
            if r.current_role == NativeReplicaRole::Primary {
                r.current_role = NativeReplicaRole::Secondary;
            }
            if r.node == selected {
                r.current_role = NativeReplicaRole::Primary;
            }
            r.first_lsn = -1;
            r.last_lsn = -1;
        }
        for p in &mut self.placements {
            if next
                .configuration
                .replicas
                .0
                .iter()
                .any(|r| r.node == p.launch.node && r.up)
            {
                p.active_unit = Some(next.configuration.unit.clone());
                p.ready_replica = next
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .find(|r| r.node == p.launch.node)
                    .cloned();
                p.endpoint = None;
            }
        }
        let reply = FailoverUnitReply {
            unit: next.configuration.unit.clone(),
            error: 0,
        };
        self.candidate_changes.push(body);
        self.swap_endpoint = None;
        self.swap = Some(ActiveConfiguration {
            body: next,
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(Some(reply))
    }
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
        let Some(round) = &self.generation else {
            // Native ready FM acknowledges delayed uploads without rebuilding.
            return Ok(header);
        };
        if round.phase != GenerationPhase::AwaitingUpload
            || body.generation != round.number
            || round.uploads[index].is_some()
        {
            return Ok(header);
        }
        if placement.launch.profile.peer_replicas() {
            self.validate_peer_lfum(index, &body)?;
            let round = self.generation.as_mut().expect("owned generation round");
            round.uploads[index] = Some(body);
            if round.uploads.iter().all(Option::is_some) {
                round.phase = GenerationPhase::Complete;
            }
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
            let dropped = replica.description.state == NativeReplicaState::Dropped;
            exact_replica(&expected.replica, &replica.description, dropped)?;
            if replica.intermediate_role != NativeReplicaRole::None
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
        let round = self.generation.as_mut().expect("owned generation round");
        round.uploads[index] = Some(body);
        if round.uploads.iter().all(Option::is_some) {
            round.phase = GenerationPhase::Complete;
        }
        Ok(header)
    }

    fn validate_peer_lfum(&self, index: usize, body: &LfumUpload) -> Result<()> {
        if let Some(removal) = self.generation.as_ref().and_then(|g| g.removal.as_ref()) {
            return self.validate_removal_lfum(index, body, removal);
        }
        if let Some(transfer) = self
            .generation
            .as_ref()
            .and_then(|round| round.transfer.as_ref())
        {
            return self.validate_transfer_lfum(index, body, transfer);
        }
        if let Some(repair) = self
            .generation
            .as_ref()
            .and_then(|round| round.repair.as_ref())
        {
            return self.validate_repair_lfum(index, body, repair);
        }
        if let Some(failover) = self
            .generation
            .as_ref()
            .and_then(|round| round.failover.as_ref())
        {
            return self.validate_failover_lfum(index, body, failover);
        }
        if let Some(builds) = self
            .generation
            .as_ref()
            .and_then(|round| round.initial_builds.as_ref())
        {
            return self.validate_build_lfum(index, body, builds);
        }
        let placement = &self.placements[index];
        let initial = self
            .generation
            .as_ref()
            .and_then(|round| round.initial_membership.as_ref());
        let configuration = self
            .configuration
            .as_ref()
            .ok_or("LFUM upload requires a known test configuration")?;
        if !placement.created
            && !configuration
                .body
                .configuration
                .replicas
                .0
                .iter()
                .any(|r| r.node == placement.launch.node)
        {
            return if placement.node_description.as_ref() == Some(&body.node)
                && !body.any_replica_found
                && body.units.0.is_empty()
            {
                Ok(())
            } else {
                Err("unused spare LFUM must report its exact empty inventory".into())
            };
        }
        if placement.node_description.as_ref() != Some(&body.node)
            || !body.any_replica_found
            || body.units.0.len() != 1
        {
            return Err("LFUM node/inventory does not match the admitted peer profile".into());
        }
        let info = &body.units.0[0];
        let request = initial.unwrap_or(&configuration.body.configuration);
        let in_progress = initial.is_some() && info.unit == request.unit;
        let mut unit = request.unit.clone();
        if !in_progress {
            unit.previous = Epoch::default();
        }
        let primary = request.replicas.0.iter().any(|r| {
            r.node == placement.launch.node && r.current_role == NativeReplicaRole::Primary
        });
        if (initial.is_none() && configuration.reply.is_none())
            || info.service
                != if initial.is_some() {
                    request.service.clone()
                } else {
                    placement.body()?.service
                }
            || info.unit != unit
            || (info.intermediate != Epoch::default()
                && !(in_progress && primary && info.intermediate == unit.current))
            || info.local_deleted
            || info.report_from_primary != primary
            || info.replicas.0.len() != request.replicas.0.len()
        {
            return Err("LFUM report changed the known test configuration".into());
        }
        for expected in &request.replicas.0 {
            let mut expected = expected.clone();
            if !in_progress {
                expected.previous_role = NativeReplicaRole::None;
            }
            expected.first_lsn = -1;
            expected.last_lsn = -1;
            let intermediate_role = if info.intermediate == Epoch::default() {
                NativeReplicaRole::None
            } else {
                expected.current_role
            };
            if info
                .replicas
                .0
                .iter()
                .filter(|r| r.description == expected && r.intermediate_role == intermediate_role)
                .count()
                != 1
            {
                return Err("LFUM report changed or omitted a known peer replica".into());
            }
        }
        Ok(())
    }

    fn validate_build_lfum(
        &self,
        index: usize,
        body: &LfumUpload,
        builds: &[AddReplica],
    ) -> Result<()> {
        let placement = &self.placements[index];
        let primary = &self.placements[0];
        let expected = primary.body()?;
        if placement.node_description.as_ref() != Some(&body.node)
            || !body.any_replica_found
            || body.units.0.len() != 1
        {
            return Err("LFUM build inventory requires the original participating RA".into());
        }
        let info = &body.units.0[0];
        if info.service != expected.service
            || info.unit != expected.unit
            || info.intermediate != Epoch::default()
            || info.local_deleted
            || info.report_from_primary != (index == 0)
            || info.replicas.0.len() != if index == 0 { builds.len() + 1 } else { 1 }
        {
            return Err("LFUM build inventory changed the original FT or build set".into());
        }
        let mut replicas = vec![];
        if index == 0 {
            replicas.push(
                primary
                    .ready_replica
                    .clone()
                    .ok_or("missing test Primary")?,
            );
        }
        replicas.extend(
            builds
                .iter()
                .filter(|build| index == 0 || build.replica.node == placement.launch.node)
                .map(|build| build.replica.clone()),
        );
        for mut expected in replicas {
            expected.first_lsn = -1;
            expected.last_lsn = -1;
            let mut matching = info
                .replicas
                .0
                .iter()
                .filter(|r| r.description.node == expected.node);
            let replica = matching.next().ok_or("missing original build replica")?;
            if matching.next().is_some() {
                return Err("duplicate build replica".into());
            }
            let actual = &replica.description;
            exact_replica(&expected, actual, false)?;
            if replica.intermediate_role != NativeReplicaRole::None
                || !actual.up
                || actual.first_lsn != -1
                || actual.last_lsn != -1
                || if expected.current_role == NativeReplicaRole::Primary {
                    *actual != expected
                } else {
                    !actual.service_endpoint.is_empty()
                        || !matches!(
                            actual.state,
                            NativeReplicaState::InBuild | NativeReplicaState::Ready
                        )
                        || (index != 0 && actual.state != NativeReplicaState::InBuild)
                        || (actual.state == NativeReplicaState::Ready
                            && actual.replication_endpoint.is_empty())
                }
            {
                return Err(format!(
                    "LFUM build state is not an original InBuild/Ready Idle: expected {expected:?}, got {replica:?}"
                ));
            }
        }
        Ok(())
    }

    fn validate_failover_lfum(
        &self,
        index: usize,
        body: &LfumUpload,
        failover: &ConfigurationInventory,
    ) -> Result<()> {
        let placement = &self.placements[index];
        let original = &failover.previous;
        let intent = &failover.configuration;
        if placement.node_description.as_ref() != Some(&body.node)
            || !body.any_replica_found
            || body.units.0.len() > 1
        {
            return Err("LFUM failover node/map does not match the original session".into());
        }
        let lost = intent
            .replicas
            .0
            .iter()
            .find(|r| r.previous_role == NativeReplicaRole::Primary)
            .ok_or("missing original lost Primary")?;
        if placement.launch.node == lost.node {
            if let Some(info) = body.units.0.first() {
                let expected = original
                    .replicas
                    .0
                    .iter()
                    .find(|r| r.node == lost.node)
                    .ok_or("missing original Primary identity")?;
                if info.service != intent.service
                    || info.unit != original.unit
                    || info.intermediate != Epoch::default()
                    || info.report_from_primary
                    || info.local_deleted
                    || info.replicas.0.len() != 1
                    || info.replicas.0[0].intermediate_role != NativeReplicaRole::None
                    || info.replicas.0[0].description != dropped_replica(expected)
                {
                    return Err("closed former Primary must report canonical dropped metadata or omit its deleted entry".into());
                }
            }
            return Ok(());
        }
        let info = body
            .units
            .0
            .first()
            .ok_or("missing surviving failover inventory")?;
        let mut expected = Configuration {
            unit: original.unit.clone(),
            service: intent.service.clone(),
            replicas: original.replicas.clone(),
        };
        let query = info.unit == intent.unit
            && info.replicas.0.iter().any(|r| {
                r.description.node == lost.node
                    && r.description.current_role == NativeReplicaRole::Primary
            });
        let mut complete = intent.unit.clone();
        complete.previous = Epoch::default();
        let transitioning = info.unit == intent.unit && !query;
        if query {
            expected.unit = intent.unit.clone();
            for r in &mut expected.replicas.0 {
                r.previous_role = r.current_role;
            }
        } else if transitioning || info.unit == complete {
            expected = intent.clone();
            if !transitioning {
                expected.unit = complete;
                expected.replicas.0.retain(|r| r.current_role.is_active());
                for r in &mut expected.replicas.0 {
                    r.previous_role = NativeReplicaRole::None;
                }
            }
        }
        let primary = expected.replicas.0.iter().any(|r| {
            r.node == placement.launch.node && r.current_role == NativeReplicaRole::Primary
        });
        if info.service != expected.service
            || info.unit != expected.unit
            || info.local_deleted
            || info.report_from_primary != primary
            || info.replicas.0.len() != expected.replicas.0.len()
            || (info.intermediate != Epoch::default()
                && !(transitioning && info.intermediate == intent.unit.current))
        {
            return Err("LFUM report changed the known original failover phase".into());
        }
        for mut expected in expected.replicas.0 {
            expected.first_lsn = -1;
            expected.last_lsn = -1;
            let mut matching = info
                .replicas
                .0
                .iter()
                .filter(|r| r.description.node == expected.node);
            let actual = matching.next().ok_or("missing original failover member")?;
            if expected.current_role == NativeReplicaRole::Primary
                && expected.node != lost.node
                && !query
            {
                // Role completion may publish a new service endpoint; replication identity stays fixed.
                expected.service_endpoint = actual.description.service_endpoint.clone();
            }
            let intermediate_role = if info.intermediate == Epoch::default() {
                NativeReplicaRole::None
            } else {
                expected.current_role
            };
            if matching.next().is_some()
                || actual.description != expected
                || actual.intermediate_role != intermediate_role
            {
                return Err(
                    "LFUM report changed an original failover replica or its phase roles".into(),
                );
            }
        }
        Ok(())
    }

    fn validate_transfer_lfum(
        &self,
        index: usize,
        body: &LfumUpload,
        transfer: &ConfigurationInventory,
    ) -> Result<()> {
        let placement = &self.placements[index];
        if placement.node_description.as_ref() != Some(&body.node)
            || !body.any_replica_found
            || body.units.0.len() != 1
        {
            return Err("LFUM transfer requires the known live Node inventory".into());
        }
        let info = &body.units.0[0];
        let intent = &transfer.configuration;
        let old = intent
            .replicas
            .0
            .iter()
            .find(|r| r.previous_role == NativeReplicaRole::Primary)
            .ok_or("missing transfer source")?;
        let candidate = intent
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == NativeReplicaRole::Primary)
            .ok_or("missing transfer candidate")?;
        let mut complete = intent.unit.clone();
        complete.previous = Epoch::default();
        let mut expected = intent.clone();
        let primary;
        let mut intermediate = Epoch::default();
        if info.unit == transfer.previous.unit {
            expected.unit = transfer.previous.unit.clone();
            expected.replicas = transfer.previous.replicas.clone();
            primary = placement.launch.node == old.node;
        } else if info.unit == complete {
            expected.unit = complete.clone();
            for r in &mut expected.replicas.0 {
                r.previous_role = NativeReplicaRole::None;
            }
            primary = placement.launch.node == candidate.node;
        } else if info.unit == intent.unit {
            if placement.launch.node == old.node {
                primary = info.report_from_primary;
                if !primary {
                    // A completed demotion is no longer reconfiguring locally.
                    let local = expected
                        .replicas
                        .0
                        .iter_mut()
                        .find(|r| r.node == old.node)
                        .unwrap();
                    local.state = NativeReplicaState::InBuild;
                    local.service_endpoint.clear();
                }
            } else if placement.launch.node == candidate.node {
                primary = true;
                if info.intermediate == intent.unit.current {
                    intermediate = intent.unit.current;
                }
            } else {
                // GetLSN copies the old CC into PC; Activate commits the new stable CC.
                expected.replicas = transfer.previous.replicas.clone();
                for r in &mut expected.replicas.0 {
                    r.previous_role = r.current_role;
                }
                primary = false;
            }
        } else {
            return Err("LFUM transfer changed a known configuration epoch".into());
        }
        let new_membership = info.unit == complete
            || (info.unit == intent.unit && placement.launch.node == candidate.node);
        if new_membership {
            expected
                .replicas
                .0
                .iter_mut()
                .find(|r| r.node == old.node)
                .unwrap()
                .service_endpoint
                .clear();
        }
        if info.service != intent.service
            || info.local_deleted
            || info.report_from_primary != primary
            || info.intermediate != intermediate
            || info.replicas.0.len() != expected.replicas.0.len()
        {
            return Err("LFUM report changed the known planned transfer phase".into());
        }
        for mut expected in expected.replicas.0 {
            expected.first_lsn = -1;
            expected.last_lsn = -1;
            let mut matching = info
                .replicas
                .0
                .iter()
                .filter(|r| r.description.node == expected.node);
            let actual = matching.next().ok_or("missing planned transfer member")?;
            if new_membership && expected.node == candidate.node {
                // Role completion can precede its independently delivered endpoint notification.
                if info.unit == complete && actual.description.service_endpoint.is_empty() {
                    return Err("completed transfer inventory has no Primary endpoint".into());
                }
                expected.service_endpoint = actual.description.service_endpoint.clone();
            }
            if matching.next().is_some()
                || actual.description != expected
                || actual.intermediate_role
                    != if intermediate == Epoch::default() {
                        NativeReplicaRole::None
                    } else {
                        expected.current_role
                    }
            {
                return Err(
                    "LFUM transfer changed a known replica lifetime or its phase roles".into(),
                );
            }
        }
        Ok(())
    }

    fn validate_repair_lfum(
        &self,
        index: usize,
        body: &LfumUpload,
        repair: &RepairInventory,
    ) -> Result<()> {
        let placement = &self.placements[index];
        let fresh = placement.launch.node == repair.build.replica.node;
        if fresh
            && repair.deleting
            && placement.node_description.as_ref() == Some(&body.node)
            && body.units.0.is_empty()
        {
            return Ok(());
        }
        if placement.node_description.as_ref() != Some(&body.node)
            || !body.any_replica_found
            || body.units.0.len() != 1
        {
            return Err("LFUM repair requires the known live Node inventory".into());
        }
        let info = &body.units.0[0];
        let mut expected = Configuration {
            unit: repair.previous.unit.clone(),
            service: repair.build.service.clone(),
            replicas: repair.previous.replicas.clone(),
        };
        let primary = expected.replicas.0.iter().any(|r| {
            r.node == placement.launch.node && r.current_role == NativeReplicaRole::Primary
        });
        let mut transitioning = false;
        if let Some(request) = &repair.configuration {
            let mut complete = request.unit.clone();
            complete.previous = Epoch::default();
            if info.unit == request.unit || info.unit == complete {
                transitioning = info.unit == request.unit;
                expected = request.clone();
                if !transitioning {
                    expected.unit = complete;
                    for r in &mut expected.replicas.0 {
                        r.previous_role = NativeReplicaRole::None;
                    }
                }
            }
        }
        let building = expected.unit == repair.previous.unit;
        if building {
            if fresh {
                expected.replicas.0.clear();
            }
            if (primary || fresh)
                && !(primary
                    && repair.cancelled
                    && !info
                        .replicas
                        .0
                        .iter()
                        .any(|r| r.description.node == repair.build.replica.node))
            {
                expected.replicas.0.push(repair.build.replica.clone());
            }
        }
        if info.unit != expected.unit
            || info.service != expected.service
            || info.local_deleted
            || info.report_from_primary != primary
            || info.replicas.0.len() != expected.replicas.0.len()
            || (info.intermediate != Epoch::default()
                && !(transitioning && !fresh && info.intermediate == expected.unit.current))
        {
            return Err("LFUM repair changed the admitted membership or phase".into());
        }
        for mut expected in expected.replicas.0 {
            expected.first_lsn = -1;
            expected.last_lsn = -1;
            let mut matching = info
                .replicas
                .0
                .iter()
                .filter(|r| r.description.node == expected.node);
            let actual = matching.next().ok_or("missing known repair replica")?;
            let duplicate = matching.next().is_some();
            if building && expected.node == repair.build.replica.node {
                let dropped = actual.description.state == NativeReplicaState::Dropped;
                if dropped && !(repair.cancelled && (primary || repair.deleting)) {
                    return Err("LFUM repair drop requires explicit cancellation/deletion".into());
                }
                if !matches!(
                    actual.description.state,
                    NativeReplicaState::InBuild
                        | NativeReplicaState::Ready
                        | NativeReplicaState::Dropped
                ) || (fresh && actual.description.state == NativeReplicaState::Ready)
                    || actual.description.replication_endpoint.is_empty()
                        && !(fresh && dropped && repair.deleting)
                    || !actual.description.service_endpoint.is_empty()
                {
                    return Err("LFUM repair build must remain an InBuild/Ready Idle".into());
                }
                expected.state = actual.description.state;
                expected.replication_endpoint = actual.description.replication_endpoint.clone();
                if fresh && dropped {
                    expected = dropped_replica(&expected);
                }
            }
            if duplicate
                || actual.description != expected
                || actual.intermediate_role
                    != if info.intermediate == Epoch::default() {
                        NativeReplicaRole::None
                    } else {
                        expected.current_role
                    }
            {
                return Err("LFUM repair changed a known replica lifetime or its roles".into());
            }
        }
        Ok(())
    }

    fn removal_inventory(&self) -> Option<RemovalInventory> {
        let sizes = self.placements[0].launch.profile.configured_sizes()?;
        if !matches!(sizes, (5, 3) | (3, 2))
            || self.placements.len() != sizes.0 as usize
            || !self.configuration_history.is_empty()
            || !self.candidate_changes.is_empty()
        {
            return None;
        }
        let (repair, supersession) = if let Some(repair) = &self.repair {
            if self.swap.is_some() || !self.repair_history.is_empty() || self.restoration.is_some()
            {
                return None;
            }
            (repair, None)
        } else {
            let swap = self.swap.as_ref().filter(|s| !s.unknown)?;
            let repair = self.repair_history.last().filter(|r| {
                self.repair_history.len() == 1
                    && r.superseded_by == Some(swap.body.configuration.unit.current)
                    && self.restoration.is_some()
            })?;
            (repair, Some(&swap.body.configuration))
        };
        let removal = &repair
            .removal
            .as_ref()
            .filter(|c| !c.unknown && c.reply.is_none())?
            .body
            .configuration;
        let previous = self
            .configuration
            .as_ref()?
            .reply
            .as_ref()
            .filter(|r| r.error == 0)?;
        let primary = previous
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == NativeReplicaRole::Primary)?;
        let lost = &repair.predecessor.replica;
        if repair.host_requested
            || repair.profile.is_some()
            || repair.configuration.is_some()
            || repair.base.is_some()
            || repair.build.reply.is_some()
            || repair.build.cancelled
            || previous.replicas.0.len() != sizes.0 as usize
            || !previous
                .replicas
                .0
                .iter()
                .any(|r| r == lost && r.current_role == NativeReplicaRole::Secondary)
            || !self
                .placements
                .iter()
                .any(|p| p.launch.node == lost.node && p.loss_reported && p.remove_received)
            || (repair.unknown
                && !self
                    .placements
                    .iter()
                    .any(|p| p.launch.node == primary.node && p.loss_reported))
        {
            return None;
        }
        let mut expected = Configuration {
            unit: previous.unit.clone(),
            service: removal.service.clone(),
            replicas: previous.replicas.clone(),
        };
        if expected.service != self.configuration.as_ref()?.body.configuration.service {
            return None;
        }
        expected.unit.previous = previous.unit.current;
        expected.unit.current.configuration = previous.unit.current.configuration.checked_add(1)?;
        for r in &mut expected.replicas.0 {
            r.previous_role = r.current_role;
            if r.node == lost.node {
                r.current_role = NativeReplicaRole::None;
                r.up = false;
                r.state = NativeReplicaState::Dropped;
            }
        }
        if expected != *removal {
            return None;
        }
        if let Some(supersession) = supersession {
            let candidate = supersession
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == NativeReplicaRole::Primary)?;
            if !self
                .placements
                .iter()
                .any(|p| p.launch.node == primary.node && p.loss_reported)
                || !expected.replicas.0.iter().any(|r| {
                    r.node == candidate.node
                        && r.current_role == NativeReplicaRole::Secondary
                        && r.up
                })
            {
                return None;
            }
            expected.unit.current.configuration = expected
                .unit
                .current
                .configuration
                .checked_add(0x1_0000_0001)?;
            for r in &mut expected.replicas.0 {
                if r.node == primary.node {
                    r.current_role = NativeReplicaRole::Secondary;
                    r.up = false;
                    r.state = NativeReplicaState::Dropped;
                } else if r.node == candidate.node {
                    r.current_role = NativeReplicaRole::Primary;
                }
                r.first_lsn = -1;
                r.last_lsn = -1;
            }
            if expected != *supersession {
                return None;
            }
        }
        Some(RemovalInventory {
            previous: previous.clone(),
            removal: removal.clone(),
            supersession: supersession.cloned(),
        })
    }

    fn validate_removal_lfum(
        &self,
        index: usize,
        body: &LfumUpload,
        inventory: &RemovalInventory,
    ) -> Result<()> {
        let placement = &self.placements[index];
        if placement.node_description.as_ref() != Some(&body.node)
            || !body.any_replica_found
            || body.units.0.len() > 1
        {
            return Err("LFUM removal requires the original known Node inventory".into());
        }
        let original = Configuration {
            unit: inventory.previous.unit.clone(),
            service: inventory.removal.service.clone(),
            replicas: inventory.previous.replicas.clone(),
        };
        let completed_removal = completed_inventory(&inventory.removal);
        let local = original
            .replicas
            .0
            .iter()
            .find(|r| r.node == placement.launch.node)
            .ok_or("removal inventory omitted its original owner")?;
        let known_loss =
            placement.loss_reported
                && (local.current_role == NativeReplicaRole::Primary
                    || inventory.removal.replicas.0.iter().any(|r| {
                        r.node == local.node && r.current_role == NativeReplicaRole::None
                    }));
        if known_loss {
            let Some(info) = body.units.0.first() else {
                return Ok(());
            };
            if [
                original.unit.clone(),
                inventory.removal.unit.clone(),
                completed_removal.unit.clone(),
            ]
            .contains(&info.unit)
                && info.service == original.service
                && !info.report_from_primary
                && !info.local_deleted
                && (info.intermediate == Epoch::default()
                    || info.intermediate == inventory.removal.unit.current)
                && info.replicas.0
                    == vec![ReplicaInfo {
                        description: dropped_replica(local),
                        intermediate_role: NativeReplicaRole::None,
                    }]
            {
                return Ok(());
            }
            if local.current_role == NativeReplicaRole::Secondary {
                return Err("deleted removal destination cannot report a live membership".into());
            }
        }
        let info = body
            .units
            .0
            .first()
            .ok_or("live removal owner omitted its committed inventory")?;
        let mut views = vec![
            (original.clone(), None),
            (inventory.removal.clone(), None),
            (inventory.removal.clone(), Some(&inventory.removal)),
            (completed_removal.clone(), None),
        ];
        let promoted = inventory.supersession.as_ref().and_then(|f| {
            f.replicas
                .0
                .iter()
                .find(|r| r.current_role == NativeReplicaRole::Primary)
        });
        if let Some(failover) = inventory.supersession.as_ref().filter(|_| !known_loss) {
            views.extend([
                (failover.clone(), None),
                (failover.clone(), Some(failover)),
                (completed_inventory(failover), None),
            ]);
            if !known_loss && promoted.is_some_and(|p| p.node != placement.launch.node) {
                for (mut query, intermediate) in [
                    (original, None),
                    (inventory.removal.clone(), None),
                    (inventory.removal.clone(), Some(&inventory.removal)),
                    (completed_removal, None),
                ] {
                    query.unit = failover.unit.clone();
                    for r in &mut query.replicas.0 {
                        r.previous_role = inventory
                            .removal
                            .replicas
                            .0
                            .iter()
                            .find(|old| old.node == r.node)
                            .ok_or("query changed removal membership")?
                            .previous_role;
                    }
                    views.push((query, intermediate));
                }
            }
        }
        if views.iter().any(|(view, intermediate)| {
            inventory_view_matches(
                info,
                view,
                &placement.launch.node,
                *intermediate,
                promoted.map(|p| &p.node),
            )
        }) {
            Ok(())
        } else {
            Err(
                "LFUM report changed the known removal/supersession PC, IC, CC or replica owner"
                    .into(),
            )
        }
    }

    pub fn begin_generation(&mut self) -> Result<()> {
        let profile = self.placements[0].launch.profile;
        let peer = profile.peer_replicas();
        let removal = self.removal_inventory();
        let stable_peer = self.configuration.as_ref().is_some_and(|c| {
            (profile.configured_sizes().is_some()
                || profile == crate::ServiceProfile::StatefulThree)
                && c.reply
                    .as_ref()
                    .is_some_and(|r| r.error == 0 && r.replicas.0.len() == profile.node_count())
                && !c.unknown
        });
        let initial_membership = self
            .configuration
            .as_ref()
            .filter(|c| {
                c.reply.is_none()
                    && !c.unknown
                    && self.configuration_history.is_empty()
                    && c.body.configuration.service.target == 3
                    && c.body.configuration.service.minimum == 2
                    && self.idles.len() + 1 == self.placements.len()
                    && self.idles.iter().all(|idle| {
                        !idle.cancelled
                            && !idle.unknown
                            && idle.reply.as_ref().is_some_and(|reply| reply.error == 0)
                    })
            })
            .map(|c| c.body.configuration.clone());
        let initial_builds = (peer
            && self.configuration.is_none()
            && self.configuration_history.is_empty()
            && self.placements[0].body()?.service.target == 3
            && self.placements[0].body()?.service.minimum == 2
            && self.idles.len() + 1 == self.placements.len()
            && self.idles.iter().all(|idle| {
                !idle.cancelled
                    && !idle.unknown
                    && idle.reply.as_ref().is_none_or(|reply| reply.error == 0)
            }))
        .then(|| self.idles.iter().map(|idle| idle.body.clone()).collect());
        let failover = self.swap.as_ref().and_then(|swap| {
            let previous = match self.repair_history.last() {
                Some(repair) => repair.configuration.as_ref().filter(|_| {
                    repair.removal.is_none()
                        && repair.base.is_none()
                        && repair.superseded_by.is_none()
                }),
                None => self.configuration.as_ref(),
            }?;
            // Host loss invalidates observations, not completed membership.
            let original = previous.reply.as_ref()?;
            (!swap.unknown
                && self.configuration_history.iter().all(|previous| {
                    previous.reply.is_some()
                        && primary_failover_configuration(&previous.body.configuration)
                })
                && self.candidate_changes.is_empty()
                && self.restoration.is_none()
                && original.error == 0
                && original.replicas.0.len() == 3
                && swap.body.configuration.unit.previous == original.unit.current
                && swap.body.configuration.unit.current.data_loss
                    == original.unit.current.data_loss
                && swap
                    .body
                    .configuration
                    .unit
                    .current
                    .configuration
                    .checked_sub(original.unit.current.configuration)
                    == Some(0x1_0000_0001)
                && swap.body.configuration.service.target == 3
                && swap.body.configuration.service.minimum == 2
                && swap.body.configuration.replicas.0.len() == 3
                && swap
                    .body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .filter(|r| !r.up)
                    .count()
                    == 1
                && swap.body.configuration.replicas.0.iter().any(|r| {
                    r.previous_role == NativeReplicaRole::Primary
                        && r.current_role == NativeReplicaRole::None
                        && !r.up
                        && r.state == NativeReplicaState::Dropped
                        && self.placements.iter().any(|p| {
                            p.launch.node == r.node
                                && (p.loss_reported
                                    || self.repair.as_ref().is_some_and(|repair| {
                                        repair.predecessor.replica.node == r.node
                                    }))
                        })
                }))
            .then(|| ConfigurationInventory {
                previous: original.clone(),
                configuration: swap.body.configuration.clone(),
            })
        });
        let repair = self
            .repair
            .as_ref()
            .filter(|r| {
                !r.unknown
                    && r.profile.is_some()
                    && r.host_requested
                    && r.removal.is_none()
                    && r.base.is_none()
                    && r.build.body.service.target == 3
                    && r.build.body.service.minimum == 2
                    && r.build.reply.as_ref().is_none_or(|reply| reply.error == 0)
                    && r.configuration.as_ref().is_none_or(|c| !c.unknown)
                    && failover.is_some()
                    && self
                        .swap
                        .as_ref()
                        .and_then(|s| s.reply.as_ref())
                        .is_some_and(|reply| reply.error == 0 && reply.replicas.0.len() == 2)
            })
            .map(|r| RepairInventory {
                previous: self
                    .swap
                    .as_ref()
                    .and_then(|s| s.reply.clone())
                    .expect("completed repair base"),
                build: r.build.body.clone(),
                configuration: r
                    .configuration
                    .as_ref()
                    .map(|c| c.body.configuration.clone()),
                cancelled: r.build.cancelled,
                deleting: r.deletion.is_some(),
            });
        let transfer = self.swap.as_ref().and_then(|swap| {
            let intent = &swap.body.configuration;
            if swap.unknown
                || self.primary_failover()
                || self.placements.len() != 3
                || intent.service.target != 3
                || intent.service.minimum != 2
                || !self.candidate_changes.is_empty()
                || self.restoration.is_some()
                || self.repair.is_some()
                || !self.healthy_transfer_members(&intent.replicas.0)
                || swap.reply.as_ref().is_some_and(|reply| {
                    reply.error != 0 || !self.healthy_transfer_members(&reply.replicas.0)
                })
            {
                return None;
            }
            self.configuration
                .iter()
                .chain(&self.configuration_history)
                .chain(
                    self.repair_history
                        .iter()
                        .filter_map(|r| r.configuration.as_ref()),
                )
                .filter(|c| !c.unknown && c.body.configuration.service == intent.service)
                .filter_map(|c| c.reply.as_ref())
                .find(|reply| reply.error == 0 && reply.unit.current == intent.unit.previous)
                .map(|previous| ConfigurationInventory {
                    previous: previous.clone(),
                    configuration: intent.clone(),
                })
        });
        if (peer
            && !stable_peer
            && initial_membership.is_none()
            && initial_builds.is_none()
            && failover.is_none()
            && transfer.is_none()
            && removal.is_none()
            && repair.is_none())
            || (self.swap.is_some()
                && failover.is_none()
                && transfer.is_none()
                && repair.is_none()
                && removal.is_none())
            || (self.repair.is_some() && repair.is_none() && removal.is_none())
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self
                .generation
                .as_ref()
                .is_some_and(|round| round.phase != GenerationPhase::Complete)
            || self
                .placements
                .iter()
                .enumerate()
                .any(|(index, placement)| {
                    let lost_primary = (repair.is_none()
                        && failover.as_ref().is_some_and(|inventory| {
                            inventory.configuration.replicas.0.iter().any(|r| {
                                r.node == placement.launch.node
                                    && r.previous_role == NativeReplicaRole::Primary
                                    && !r.up
                            })
                        }))
                        || (placement.loss_reported
                            && removal.as_ref().is_some_and(|inventory| {
                                inventory.removal.replicas.0.iter().any(|r| {
                                    r.node == placement.launch.node
                                        && (r.previous_role == NativeReplicaRole::Primary
                                            || r.current_role == NativeReplicaRole::None)
                                })
                            }));
                    placement.bootstrap != Bootstrap::Admitted
                        || !placement.link_alive
                        || (!lost_primary
                            && matches!(
                                placement.observed,
                                Observed::Lost | Observed::Unknown | Observed::Error
                            ))
                        || placement.pending.is_some()
                        || placement.node_control.is_some()
                        || placement.service_update.is_some()
                        || (peer
                            && ((!lost_primary
                                && placement.observed != Observed::Running
                                && !((initial_membership.is_some() || initial_builds.is_some())
                                    && index != 0
                                    && placement.observed == Observed::Absent)
                                && !(stable_peer
                                    && !placement.created
                                    && placement.observed == Observed::Absent)
                                && !(repair.as_ref().is_some_and(|r| {
                                    r.build.replica.node == placement.launch.node
                                }) && placement.observed == Observed::Pending))
                                || (placement.loss_reported && !lost_primary)))
                })
        {
            return Err("generation handshake requires admitted sessions and a supported live membership transition".into());
        }
        let current = self
            .generation
            .as_ref()
            .map_or(&self.placements[0].launch.generation.number, |round| {
                &round.number
            });
        self.generation = Some(GenerationHandshake::new(current, self.placements.len())?);
        let round = self.generation.as_mut().expect("owned round");
        round.initial_membership = initial_membership;
        round.initial_builds = initial_builds;
        round.failover = failover;
        round.transfer = transfer;
        round.repair = repair;
        round.removal = removal;
        Ok(())
    }
    pub fn report(&mut self, index: usize, body: &ReplicaUp) -> Result<Option<ReplicaUp>> {
        if !self.placements[index].link_alive {
            return Err("report requires the original live node session".into());
        }
        if self
            .repair
            .iter()
            .chain(&self.repair_history)
            .any(|repair| {
                self.placements[index].launch.node == repair.predecessor.replica.node
                    && body.replicas.0.is_empty()
                    && body.dropped.0.len() == 1
                    && body.dropped.0[0].unit == repair.predecessor.unit
                    && body.dropped.0[0].service == repair.predecessor.service
                    && body.dropped.0[0].replicas.0.len() == 1
                    && body.dropped.0[0].replicas.0[0].description
                        == dropped_replica(&repair.predecessor.replica)
                    && !body.last
                    && !body.from_fmm
            })
        {
            return Ok(None);
        }
        if let Some(swap) = self
            .repair
            .as_ref()
            .and_then(|r| r.configuration.as_ref().or(r.removal.as_ref()))
            .or(self.swap.as_ref())
        {
            let mut stable = swap.body.configuration.unit.clone();
            stable.previous = Epoch::default();
            if body.dropped.0.first().is_some_and(|i| {
                (i.unit == stable || i.unit == swap.body.configuration.unit)
                    && i.service == swap.body.configuration.service
            }) {
                self.placements[index].active_unit = Some(body.dropped.0[0].unit.clone());
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
                    && info.replicas.0[0].intermediate_role == NativeReplicaRole::None
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
        let failover = self.primary_failover();
        let old_failover_primary = self.failed_primary_index() == Some(index);
        let planned_secondary = !failover
            && self.swap.as_ref().is_some_and(|s| {
                s.body.configuration.unit.target == 3
                    && s.body.configuration.unit.minimum == 2
                    && s.body.configuration.replicas.0.iter().any(|r| {
                        r.node == self.placements[index].launch.node
                            && r.current_role == NativeReplicaRole::Secondary
                    })
                    && s.reply.as_ref().is_none_or(|reply| {
                        reply.replicas.0.iter().any(|r| {
                            r.node == self.placements[index].launch.node
                                && !r.up
                                && r.state == NativeReplicaState::Dropped
                        })
                    })
            });
        let placement = &mut self.placements[index];
        if response.is_some()
            && placement.bootstrap == Bootstrap::Admitted
            && placement.launch.profile.stateful()
        {
            if let Some(swap) = self.swap.as_mut().filter(|s| {
                (s.reply.is_none() || failover)
                    && !old_failover_primary
                    && !planned_secondary
                    && s.body.configuration.replicas.0.iter().any(|r| {
                        r.node == placement.launch.node && r.up && r.current_role.is_active()
                    })
            }) {
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
                if let Some(repair) = &mut self.repair {
                    repair.unknown = true;
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
                self.complete(
                    &op,
                    OperationOutcome::new(&op, OperationState::Lost).with_error(error),
                );
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
            recovery_mode: crate::RecoveryMode::default(),
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
            candidate_changes: vec![],
            configuration_history: vec![],
            repair: None,
            repair_history: Vec::new(),
            restoration: None,
            node_failures: Vec::new(),
            node_reconfiguration: None,
        }
    }
    pub fn accept(&mut self, command: Command) -> Result<Accepted> {
        let command = command.request()?;
        if command.supervisor_control() {
            return Err(
                "Node controls require the original process supervisor and fencing proof".into(),
            );
        }
        if command
            .node()
            .is_some_and(|node| usize::from(node) > self.placements.len())
        {
            return Err("node is outside the configured topology".into());
        }
        if let Some(operation) = self
            .operations
            .iter()
            .find(|op| op.command.op == command.op)
        {
            return if operation.command == command {
                Ok(Accepted::Duplicate(Box::new(operation.outcome.clone())))
            } else {
                Err("operation identity reused with different canonical content".into())
            };
        }
        if self.generation_pending()
            && !matches!(command.action, CommandAction::Status | CommandAction::Quit)
            && !matches!(
                command.action,
                CommandAction::ReconfigurePartition {
                    operation: Some(_),
                    ..
                }
            )
        {
            return Err("generation rebuild is awaiting LFUM upload; mutations disabled".into());
        }
        let scenario = matches!(
            command.action,
            CommandAction::ReconfigurePartition { .. }
                | CommandAction::RebuildReplica { .. }
                | CommandAction::CancelRebuild { .. }
        );
        if let Some(partition) = command.partition() {
            self.validate_partition(partition)?;
        }
        if self.configuration.is_some()
            && !scenario
            && !matches!(command.action, CommandAction::Status | CommandAction::Quit)
        {
            return Err("active-configuration lifecycle changes remain gated".into());
        }
        if self.placements[0].launch.profile.peer_replicas()
            && (matches!(
                command.action,
                CommandAction::Delete { .. }
                    | CommandAction::Activate { .. }
                    | CommandAction::Deactivate { .. }
            ) || (matches!(
                command.action,
                CommandAction::Create { .. } | CommandAction::CreatePartition { .. }
            ) && (command.node() != Some(1) || self.placements[0].created)))
        {
            return Err("peer-idle profile admits only the original node1 Primary creation, status and quit".into());
        }
        if self.operations.len() == 64 {
            return Err(
                "64 operation identities retained; capacity exhausted (no eviction)".into(),
            );
        }
        let accepted = match &command.action {
            CommandAction::Activate { node } | CommandAction::Deactivate { node } => {
                let index = usize::from(node - 1);
                let placement = &mut self.placements[index];
                if placement.bootstrap != Bootstrap::Admitted
                    || !placement.link_alive
                    || placement
                        .node_control
                        .as_ref()
                        .is_some_and(|(_, intent, _)| {
                            *intent == matches!(command.action, CommandAction::Activate { .. })
                        })
                {
                    return Err(
                        "node activation requires admitted current session without same-intent pending control"
                            .into(),
                    );
                }
                let activate = matches!(command.action, CommandAction::Activate { .. });
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
                    self.complete(
                        &op,
                        OperationOutcome::new(&op, OperationState::Unknown)
                            .with_error("superseded by newer node intent"),
                    );
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
            CommandAction::Create { node } | CommandAction::CreatePartition { node, .. } => {
                let index = usize::from(node - 1);
                let placement = &self.placements[index];
                if command
                    .partition()
                    .is_some_and(|partition| partition != placement.launch.partition)
                {
                    return Err("creation destination belongs to another partition".into());
                }
                if matches!(command.action, CommandAction::CreatePartition { .. })
                    && placement.launch.profile.mesh_peers()
                    && (index != 0 || self.placements.iter().any(|p| p.created))
                {
                    return Err("partition creation requires the unused profile's initial Primary placement".into());
                }
                self.begin_creation(index, &command.op)?
            }
            CommandAction::Delete { node } => {
                let index = usize::from(node - 1);
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
                        OperationOutcome::new(&op, OperationState::Unknown)
                            .with_error("create superseded by deletion"),
                    );
                }
                Accepted::Delete(index, Box::new(RemoveInstance::new(body, false)))
            }
            CommandAction::Status => Accepted::Status,
            CommandAction::Quit => Accepted::Quit,
            CommandAction::ReconfigurePartition {
                partition,
                predecessor,
                operation,
            } => {
                let body = if let Some(operation) = operation {
                    let predecessor = self.predecessor_index(partition, predecessor)?;
                    Some(self.supersede_rebuild(predecessor, operation, &command.op)?)
                } else {
                    self.reconfigure_failed_replica(partition, predecessor)?
                };
                match body {
                    Some(body) => Accepted::Reconfigure(Box::new(body)),
                    None => Accepted::Progress,
                }
            }
            CommandAction::RebuildReplica {
                partition,
                predecessor,
                node,
            } => self.begin_explicit_rebuild(
                partition,
                predecessor,
                usize::from(node - 1),
                &command.op,
            )?,
            CommandAction::CancelRebuild { operation } => {
                let target = self
                    .operations
                    .iter()
                    .find(|op| &op.command.op == operation)
                    .ok_or("unknown rebuild operation")?;
                if !matches!(target.command.action, CommandAction::RebuildReplica { .. })
                    || target.outcome.state != OperationState::Pending
                    || self.repair.as_ref().is_none_or(|repair| {
                        !target.outcome.owns_replica(&repair.build.body.replica)
                    })
                {
                    return Err("cancellation does not identify the current pending rebuild".into());
                }
                Accepted::Cancel(Box::new(self.cancel_repair_build(Instant::now())?))
            }
            _ => unreachable!(),
        };
        let mut outcome = OperationOutcome::new(&command.op, OperationState::Pending);
        if let Some(partition) = command.partition() {
            outcome.partition = Some(partition.into());
        }
        if let Some(node) = command.node() {
            let placement = &self.placements[usize::from(node - 1)];
            outcome.replica = Some(placement.replica);
            outcome.incarnation = Some(placement.incarnation);
            outcome.node_instance = Some(placement.launch.node.instance);
        }
        if scenario {
            if let CommandAction::ReconfigurePartition { operation, .. } = &command.action {
                outcome.epoch = Some(
                    self.configuration_view()
                        .request
                        .as_ref()
                        .map(|request| request.configuration.unit.current),
                );
                outcome.operation = operation.clone();
            }
            if matches!(command.action, CommandAction::RebuildReplica { .. })
                && self.placements[usize::from(command.node().unwrap() - 1)]
                    .launch
                    .profile
                    .stateful()
            {
                let build = &self
                    .repair
                    .as_ref()
                    .ok_or("rebuild did not retain a repair owner")?
                    .build
                    .body
                    .replica;
                outcome.bind_replica(build);
                outcome.phase = Some(OperationPhase::Building);
            }
            if let CommandAction::CancelRebuild { operation } = &command.action {
                let build = &self
                    .repair
                    .as_ref()
                    .ok_or("missing cancelled repair owner")?
                    .build
                    .body
                    .replica;
                outcome.bind_replica(build);
                outcome.operation = Some(operation.clone());
                outcome.phase = Some(OperationPhase::Removing);
            }
        }
        self.operations.push(Operation { outcome, command });
        Ok(accepted)
    }
    pub(crate) fn begin_creation(&mut self, index: usize, op: &str) -> Result<Accepted> {
        if self.placements[index].launch.profile.stateful()
            && self.node_failures.iter().any(|failure| {
                failure.index == index
                    && failure.body.is_some()
                    && failure.desired == Desired::Running
            })
        {
            return Err("an existing stateful partition requires survivor rebuild; data-loss recreation is not enabled".into());
        }
        let placement = &mut self.placements[index];
        if placement.bootstrap != Bootstrap::Admitted
            || !placement.activated
            || !placement.type_enabled
            || placement.node_control.is_some()
            || !placement.link_alive
            || (placement.created && !placement.reusable)
        {
            return Err("node unadmitted, uncertain, or predecessor cleanup not verified".into());
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
        placement.create_op = Some(op.into());
        placement.pending = Some(PendingPhase::Create);
        if placement.launch.profile.stateful() {
            let now = Instant::now();
            placement.action_updated_at = Some(now);
            placement.action_last_at = Some(now);
        }
        Ok(Accepted::Create(index, Box::new(body)))
    }
    pub fn result(&self, op: &str) -> Value {
        self.operations
            .iter()
            .find(|item| item.command.op == op)
            .map(|item| item.outcome.json())
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
            OperationOutcome::new(&op, OperationState::Complete)
                .detail("activated", activate)
                .detail("sequence", sequence),
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
    pub fn complete(&mut self, op: &str, mut outcome: OperationOutcome) {
        if let Some(operation) = self
            .operations
            .iter_mut()
            .find(|item| item.command.op == op)
        {
            outcome.retain_owner(&operation.outcome);
            operation.outcome = outcome;
            if operation.outcome.state == OperationState::Complete {
                if matches!(
                    operation.command.action,
                    CommandAction::CreatePartition { .. }
                ) {
                    operation.outcome.phase = Some(OperationPhase::MembershipReady);
                } else if matches!(
                    operation.command.action,
                    CommandAction::RebuildReplica { .. }
                ) {
                    operation.outcome.phase = Some(OperationPhase::Ready);
                }
            }
        }
    }
    pub fn lost(&mut self, index: usize, error: String) {
        if let Some(repair) = &mut self.repair {
            repair.unknown = true;
        }
        let failover = self.primary_failover();
        let old_primary = self.failed_primary_index() == Some(index);
        if let Some(swap) = self.swap.as_mut().filter(|s| {
            !old_primary
                && s.body.configuration.replicas.0.iter().any(|r| {
                    r.node == self.placements[index].launch.node
                        && r.up
                        && r.current_role.is_active()
                })
        }) {
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
        if failover && !old_primary {
            placement.endpoint = None;
        }
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
                OperationOutcome::new(&operation, OperationState::Unknown)
                    .with_error(error.clone()),
            );
        }
        if let Some(op) = control {
            self.complete(
                &op,
                OperationOutcome::new(&op, OperationState::Unknown).with_error(error),
            );
        }
    }
    pub fn down(&mut self, index: usize) {
        if self.placements[0].launch.profile == crate::ServiceProfile::StatefulThree
            || self.placements[0]
                .launch
                .profile
                .configured_sizes()
                .is_some()
        {
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
            self.complete(
                &op,
                OperationOutcome::new(&op, OperationState::Unknown).with_error(error),
            );
        }
    }
    pub fn reply(&mut self, index: usize, body: AddInstanceReply, removing: bool) -> Result<()> {
        if removing
            && self.repair.as_ref().is_some_and(|r| {
                r.deletion.is_some()
                    && r.build.body.replica.node == body.replica.node
                    && r.build.body.replica.id == body.replica.id
                    && r.build.body.replica.instance == body.replica.instance
            })
        {
            return self.repair_deletion_reply(index, body);
        }
        if self.repair.iter().chain(&self.repair_history).any(|r| {
            self.placements[index].launch.node == r.predecessor.replica.node
                && body.unit.id == r.predecessor.unit.id
                && body.unit.current.older_than(r.build.body.unit.current)
                && body.replica.node == r.predecessor.replica.node
                && body.replica.id == r.predecessor.replica.id
                && body.replica.instance == r.predecessor.replica.instance
        }) {
            return Ok(());
        }
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
                if placement.desired != Desired::Absent
                    || body.replica.state != NativeReplicaState::Dropped
                {
                    return Err("Remove reply is not the requested disposed instance".into());
                }
                if placement.launch.profile.stateful()
                    && body.replica != dropped_replica(&expected.replica)
                {
                    return Err("DeleteReplica reply is not the native dropped description".into());
                }
            } else {
                if body.replica.state != NativeReplicaState::Ready || !body.replica.up {
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
                OperationOutcome::new(&op, OperationState::Error)
                    .detail("native_error", body.error),
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
        let configuring = placement
            .launch
            .profile
            .configured_sizes()
            .is_some_and(|(target, _)| target > 1);
        let mut outcome = OperationOutcome::new(
            &op,
            if configuring {
                OperationState::Pending
            } else {
                OperationState::Complete
            },
        )
        .detail("observed", Observed::Running)
        .detail("endpoint", &placement.endpoint);
        outcome.replica = Some(placement.replica);
        outcome.incarnation = Some(placement.incarnation);
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
        if let Some(repair) = self
            .repair
            .as_mut()
            .filter(|r| r.deletion.is_some() && r.build.body.replica.node == placement.launch.node)
        {
            let expected = &repair.build.body;
            let replica = &proof.disposal.body.replica;
            if replica.node != expected.replica.node
                || replica.id != expected.replica.id
                || replica.instance != expected.replica.instance
                || replica.current_role != NativeReplicaRole::Idle
                || replica.previous_role != NativeReplicaRole::None
                || replica.state != NativeReplicaState::Dropped
                || !replica.up
                || !replica.service_endpoint.is_empty()
            {
                return Err("cancelled repair disposal changed the exact Idle lifetime".into());
            }
            proof.validate_body(
                &placement.launch,
                repair.profile.as_ref().ok_or("missing cancelled host")?,
                node_pid,
                expected,
            )?;
            repair
                .deletion
                .as_mut()
                .expect("requested repair deletion")
                .disposal = Some(proof);
            return Ok(());
        }
        if let Some(config) = self
            .configuration
            .iter()
            .chain(self.configuration_history.iter())
            .chain(self.swap.iter())
            .find(|c| {
                (c.body.configuration.unit.target == 1
                    || (c.body.configuration.unit.target == 3
                        && c.body.configuration.unit.minimum == 2
                        && c.body
                            .configuration
                            .replicas
                            .0
                            .iter()
                            .all(|r| r.up && r.state == NativeReplicaState::Ready)
                        && c.body.configuration.replicas.0.iter().any(|r| {
                            r.previous_role == NativeReplicaRole::Primary
                                && r.current_role == NativeReplicaRole::Secondary
                        })))
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
            replica.state = NativeReplicaState::Dropped;
            if config.body.configuration.unit.target == 3 {
                if replica.current_role != NativeReplicaRole::Secondary {
                    return Err("planned restart disposal requires a Secondary".into());
                }
                replica.first_lsn = proof.disposal.body.replica.first_lsn;
                replica.last_lsn = proof.disposal.body.replica.last_lsn;
                replica.service_endpoint.clear();
            }
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
        let mut outcome = OperationOutcome::new(&op, OperationState::Complete)
            .detail("observed", Observed::Absent)
            .detail("cleanup", "verified_original_instance");
        outcome.replica = Some(placement.replica);
        outcome.incarnation = Some(placement.incarnation);
        self.complete(&op, outcome);
    }
    #[cfg(test)]
    pub fn status(&self) -> Value {
        self.status_typed().json()
    }

    pub fn status_typed(&self) -> ClusterStatus {
        ClusterStatus {
            nodes: self.placements.iter().map(Placement::status_typed).collect(),
            configuration: self.configuration_view(), repair: self.repair_view(),
            idle_build: self.idle_view(), idle_removal: self.idle_removal_view(), idle_deletion: self.idle_deletion_view(),
            generation_handshake: self.generation.as_ref().map(GenerationHandshake::status),
            node_controls: None, peer_idle: None,
            operations: self.operations.iter().map(|op|
                if matches!(op.command.action, CommandAction::Status) {
                    OperationOutcome::new(&op.command.op, op.outcome.state).detail("kind", "status")
                } else { op.outcome.clone() }).collect(),
            details: [
            ("recovery_mode".into(), json!(self.recovery_mode)),
            ("replica_set".into(), json!(self.placements[0].launch.profile.configured_sizes().map(|(target, minimum)|
                json!({"target":target,"minimum":minimum,"immutable":true})))),
            ("idle_predecessor".into(), json!(self.idle_predecessor)),
            ("node_failures".into(), json!(self.node_failures.iter().map(|failure| json!({
                "node":failure.index + 1,"instance":failure.receipt.node.instance,
                "session":failure.receipt.session,"recovery_error":failure.recovery_error,
                "partition":self.placements[failure.index].launch.partition,
                "predecessor":failure.body.as_ref().map(|body| crate::ReplicaRef {
                    node:(failure.index + 1) as u8, node_instance:failure.receipt.node.instance,
                    replica:body.replica.id, incarnation:body.replica.instance,
                }),
                "replacement_instance":failure.replacement.as_ref().map(|launch| launch.node.instance)
            })).collect::<Vec<_>>())),
            ("operation_capacity".into(), json!(64)),
            ("retained_operations".into(), json!(self.operations.len())),
            ("unsupported".into(), json!("all native families outside the selected fixed lifecycle profile")),
            ].into_iter().collect(),
        }
    }
    pub fn request_idle(&mut self, now: Instant) -> Result<AddReplica> {
        self.request_idle_on(1, now)
    }
    pub fn primary_index(&self) -> usize {
        if self.primary_failover() || self.swap.as_ref().is_some_and(|s| s.reply.is_some()) {
            self.transition_primary_index()
        } else {
            self.swap
                .as_ref()
                .and_then(|s| {
                    self.placements.iter().position(|p| {
                        s.body.configuration.replicas.0.iter().any(|r| {
                            r.node == p.launch.node && r.previous_role == NativeReplicaRole::Primary
                        })
                    })
                })
                .unwrap_or(0)
        }
    }

    pub fn configuration_target_index(&self) -> usize {
        let primary = self.primary_index();
        if self
            .swap
            .as_ref()
            .is_some_and(|s| s.reply.is_none() && !s.unknown)
            && !self.primary_failover()
            && self.placements[primary].loss_reported
        {
            self.transition_primary_index()
        } else {
            primary
        }
    }

    fn transition_primary_index(&self) -> usize {
        self.swap
            .as_ref()
            .and_then(|s| {
                self.placements.iter().position(|p| {
                    s.body.configuration.replicas.0.iter().any(|r| {
                        r.node == p.launch.node && r.current_role == NativeReplicaRole::Primary
                    })
                })
            })
            .unwrap_or(0)
    }

    pub fn failed_primary_index(&self) -> Option<usize> {
        self.swap
            .as_ref()
            .filter(|_| self.primary_failover())
            .and_then(|s| {
                self.placements.iter().position(|p| {
                    s.body.configuration.replicas.0.iter().any(|r| {
                        r.node == p.launch.node
                            && r.previous_role == NativeReplicaRole::Primary
                            && matches!(
                                r.current_role,
                                NativeReplicaRole::None | NativeReplicaRole::Secondary
                            )
                            && !r.up
                    })
                })
            })
    }

    pub fn repair_destination_index(&self) -> Option<usize> {
        let repair = self.repair.as_ref()?;
        self.placements
            .iter()
            .position(|p| p.launch.node == repair.build.body.replica.node)
    }

    pub fn begin_spare_replacement(
        &mut self,
        destination: usize,
        predecessor: Option<usize>,
        now: Instant,
    ) -> Result<bool> {
        let profile = self.placements[0].launch.profile;
        if profile.configured_sizes().is_none() || destination >= self.placements.len() {
            return Err("replacement requires an explicit configured spare placement".into());
        }
        let predecessor_node = match predecessor {
            Some(index) => Some(
                self.placements
                    .get(index)
                    .ok_or("unknown rebuild predecessor placement")?
                    .launch
                    .node
                    .clone(),
            ),
            None => None,
        };
        if let Some(repair) = &self.repair {
            if repair.uses_spare()
                && self.repair_destination_index() == Some(destination)
                && !repair.unknown
            {
                if predecessor_node
                    .as_ref()
                    .is_some_and(|node| *node != repair.predecessor.replica.node)
                {
                    return Err(
                        "rebuild predecessor does not identify this spare-replacement owner".into(),
                    );
                }
                return Ok(false);
            }
            if !repair.uses_spare()
                || repair
                    .configuration
                    .as_ref()
                    .is_none_or(|c| c.reply.is_none())
            {
                return Err("another repair owns the current cycle".into());
            }
        }
        let active = self
            .repair
            .as_ref()
            .and_then(|r| r.configuration.as_ref())
            .or(self.swap.as_ref())
            .or(self.configuration.as_ref())
            .filter(|c| c.reply.is_some())
            .ok_or("replacement requires completed current membership")?;
        let previous = active.reply.as_ref().expect("completed membership");
        let target = &self.placements[destination];
        // The completed reply omits members removed during failover.
        let lost: Vec<_> = self
            .placements
            .iter()
            .filter(|p| {
                p.loss_reported
                    && !self.known_spare_loss(p)
                    && active.body.configuration.replicas.0.iter().any(|r| {
                        r.node == p.launch.node
                            && (r.previous_role.is_active() || r.current_role.is_active())
                    })
            })
            .collect();
        let [lost] = lost.as_slice() else {
            return Err(
                "replacement requires exactly one known lost member of the completed transition"
                    .into(),
            );
        };
        if predecessor_node
            .as_ref()
            .is_some_and(|node| *node != lost.launch.node)
        {
            return Err(
                "rebuild predecessor is not the lost member of the latest completed membership"
                    .into(),
            );
        }
        let retained = previous
            .replicas
            .0
            .iter()
            .any(|r| r.node == lost.launch.node);
        let primary = previous
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == NativeReplicaRole::Primary)
            .ok_or("replacement requires a surviving Primary")?;
        if self.generation_pending()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || !lost.remove_received
            || lost.launch.node == primary.node
            || previous.unit.target != profile.node_count() as i32
            || previous.replicas.0.len() + usize::from(!retained) != previous.unit.target as usize
            || previous
                .replicas
                .0
                .iter()
                .filter(|r| r.node != lost.launch.node)
                .count()
                < previous.replicas.0.len() / 2 + 1
            || target.created
            || target.observed != Observed::Absent
            || previous
                .replicas
                .0
                .iter()
                .any(|r| r.node == target.launch.node)
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
                    || (p.loss_reported
                        && p.launch.node != lost.launch.node
                        && !self.known_spare_loss(p))
                    || (p.launch.node != lost.launch.node
                        && previous.replicas.0.iter().any(|r| r.node == p.launch.node)
                        && (p.loss_reported || p.observed != Observed::Running))
            })
        {
            return Err("replacement requires a processed down Secondary, a live majority and an unused admitted spare".into());
        }
        let mut build = target.body()?;
        build.unit = previous.unit.clone();
        build.service = active.body.configuration.service.clone();
        build.replica.previous_role = NativeReplicaRole::None;
        build.replica.current_role = NativeReplicaRole::Idle;
        build.replica.state = NativeReplicaState::InBuild;
        let repair = ReplicaRepair {
            predecessor: lost.body()?,
            old_profile: lost.profile.clone().ok_or("missing lost host profile")?,
            profile: target.profile.clone(),
            build: IdleBuild {
                body: build,
                reply: None,
                cancelled: false,
                unknown: false,
                started: now,
                last: now,
            },
            configuration: None,
            removal: None,
            cancellation: None,
            deletion: None,
            base: Some(active.clone()),
            host_requested: false,
            superseded_by: None,
            unknown: false,
        };
        let placement = &mut self.placements[destination];
        placement.active_unit = Some(repair.build.body.unit.clone());
        placement.service_description = Some(repair.build.body.service.clone());
        placement.ready_replica = Some(repair.build.body.replica.clone());
        placement.created = true;
        placement.desired = Desired::Running;
        placement.observed = Observed::Pending;
        if let Some(previous) = self.repair.replace(repair) {
            self.repair_history.push(previous);
        }
        Ok(true)
    }

    pub fn begin_secondary_repair(
        &mut self,
        destination: usize,
        now: Instant,
    ) -> Result<Option<DoReconfiguration>> {
        if !self.placements[0].launch.profile.mesh_peers() || destination >= self.placements.len() {
            return Err("Secondary repair requires an original configured placement".into());
        }
        if self.restoration.is_some() {
            return self.begin_restoration_repair(destination, now);
        }
        if let Some(repair) = &self.repair {
            let completed = repair
                .configuration
                .as_ref()
                .is_some_and(|c| c.reply.is_some());
            if !completed || !self.placements[destination].loss_reported {
                if self.repair_destination_index() != Some(destination)
                    || (repair.removal.is_none() && repair.base.is_none())
                {
                    return Err("another repair owns the current cycle".into());
                }
                return Ok(None);
            }
        }
        let active = self
            .repair
            .as_ref()
            .and_then(|r| r.configuration.as_ref())
            .or(self.swap.as_ref())
            .or(self.configuration.as_ref())
            .filter(|c| c.reply.is_some())
            .ok_or("Secondary repair requires completed membership")?;
        let previous = active.reply.as_ref().expect("completed configuration");
        let target = &self.placements[destination];
        if self.generation.is_some()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || !target.loss_reported
            || !target.remove_received
            || !previous.replicas.0.iter().any(|r| {
                r.node == target.launch.node && r.current_role == NativeReplicaRole::Secondary
            })
            || previous.replicas.0.len() != self.placements.len()
            || self.placements.iter().enumerate().any(|(i, p)| {
                !p.link_alive
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
                    || (i != destination && (p.loss_reported || p.observed != Observed::Running))
            })
        {
            return Err(
                "Secondary repair requires one processed loss/deletion and every other live member"
                    .into(),
            );
        }
        if previous.replicas.0.len() <= previous.unit.minimum as usize {
            return Err("Secondary removal is blocked at minimum; this topology has no ready spare placement".into());
        }
        let mut body = active.body.clone();
        body.configuration.unit = previous.unit.clone();
        body.configuration.unit.previous = previous.unit.current;
        body.configuration.unit.current.configuration = previous
            .unit
            .current
            .configuration
            .checked_add(1)
            .ok_or("Secondary removal epoch exhausted")?;
        if body.configuration.unit.current.configuration & !0xffff_ffff
            != previous.unit.current.configuration & !0xffff_ffff
        {
            return Err("same-Primary Secondary removal epoch exhausted".into());
        }
        body.configuration.replicas = previous.replicas.clone();
        for r in &mut body.configuration.replicas.0 {
            r.previous_role = r.current_role;
            if r.node == target.launch.node {
                r.current_role = NativeReplicaRole::None;
                r.state = NativeReplicaState::Dropped;
                r.up = false;
            }
        }
        let mut unit = body.configuration.unit.clone();
        unit.previous = Epoch::default();
        let repair = self.new_repair(
            destination,
            unit,
            now,
            Some(ActiveConfiguration {
                body: body.clone(),
                reply: None,
                unknown: false,
                started: now,
                last: now,
            }),
        )?;
        if let Some(previous) = self.repair.replace(repair) {
            self.repair_history.push(previous);
        }
        Ok(Some(body))
    }

    fn known_restoration_loss(&self, placement: &Placement) -> bool {
        self.restoration.as_ref().is_some_and(|lost| {
            lost.iter().any(|old| {
                old.replica.node == placement.launch.node
                    && old.replica.id == placement.replica
                    && old.replica.instance == placement.incarnation
                    && placement.loss_reported
            })
        })
    }

    pub(crate) fn known_spare_loss(&self, placement: &Placement) -> bool {
        placement.loss_reported
            && placement.remove_received
            && self.repair.iter().chain(&self.repair_history).any(|r| {
                r.uses_spare()
                    && r.predecessor.replica.node == placement.launch.node
                    && r.predecessor.replica.id == placement.replica
                    && r.predecessor.replica.instance == placement.incarnation
                    && r.configuration.as_ref().is_some_and(|c| {
                        c.reply.as_ref().is_some_and(|reply| {
                            reply.error == 0
                                && !reply
                                    .replicas
                                    .0
                                    .iter()
                                    .any(|replica| replica.node == placement.launch.node)
                        })
                    })
            })
    }

    fn repair_members_live(&self) -> bool {
        self.placements.iter().all(|p| {
            if self.repair.as_ref().is_some_and(|repair| {
                repair.predecessor.replica.node == p.launch.node
                    && p.node_down
                        .as_ref()
                        .is_some_and(|proof| proof.node == p.launch.node)
            }) {
                return true;
            }
            p.link_alive
                && (!p.loss_reported
                    || self.known_restoration_loss(p)
                    || self.known_spare_loss(p)
                    || self.repair.as_ref().is_some_and(|r| {
                        r.uses_spare()
                            && p.launch.node == r.predecessor.replica.node
                            && p.remove_received
                    }))
        })
    }

    fn begin_restoration_repair(
        &mut self,
        destination: usize,
        now: Instant,
    ) -> Result<Option<DoReconfiguration>> {
        if let Some(repair) = &self.repair {
            if repair.unknown {
                return Err("restoration ownership is unknown after another loss".into());
            }
            if repair
                .configuration
                .as_ref()
                .is_none_or(|c| c.reply.is_none())
            {
                if self.repair_destination_index() != Some(destination) {
                    return Err("another repair owns the current cycle".into());
                }
                return Ok(None);
            }
        }
        let target = &self.placements[destination];
        if !self.known_restoration_loss(target) || !target.remove_received {
            if self.repair_destination_index() == Some(destination)
                && self.repair.as_ref().is_some_and(|r| {
                    !r.unknown && r.configuration.as_ref().is_some_and(|c| c.reply.is_some())
                })
            {
                return Ok(None);
            }
            return Err(
                "restoration requires one of the two exact processed predecessor losses/deletions"
                    .into(),
            );
        }
        let active = self
            .repair
            .as_ref()
            .and_then(|r| r.configuration.as_ref())
            .or(self.swap.as_ref())
            .filter(|c| c.reply.is_some() && !c.unknown)
            .ok_or("restoration requires completed supersession or preceding repair")?;
        if self.generation_pending()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
                    || if p.loss_reported {
                        !self.known_restoration_loss(p)
                    } else {
                        p.observed != Observed::Running
                    }
            })
        {
            return Err(
                "restoration requires the original sessions and every retained survivor".into(),
            );
        }
        let previous = active.reply.as_ref().expect("completed configuration");
        let in_cc = previous.replicas.0.iter().any(|r| {
            r.node == target.launch.node && r.current_role == NativeReplicaRole::Secondary && !r.up
        });
        if !in_cc
            && previous.replicas.0.iter().any(|r| !r.up)
            && (previous.replicas.0.len() != previous.unit.minimum as usize
                || previous.replicas.0.iter().filter(|r| !r.up).count() != 1
                || previous
                    .replicas
                    .0
                    .iter()
                    .any(|r| !r.up && r.current_role != NativeReplicaRole::Secondary))
        {
            return Err(
                "repair the down current-configuration member before the removed placement".into(),
            );
        }
        let mut body = active.body.clone();
        body.configuration.unit = previous.unit.clone();
        body.configuration.replicas = previous.replicas.clone();
        let removal = if in_cc {
            if previous.replicas.0.len() <= previous.unit.minimum as usize {
                return Err("Secondary removal is blocked at minimum; rebuild the already-removed placement to a Ready Idle first".into());
            }
            body.configuration.unit.previous = previous.unit.current;
            body.configuration.unit.current.configuration = previous
                .unit
                .current
                .configuration
                .checked_add(1)
                .ok_or("restoration removal epoch exhausted")?;
            if body.configuration.unit.current.configuration & !0xffff_ffff
                != previous.unit.current.configuration & !0xffff_ffff
            {
                return Err("same-Primary restoration removal epoch exhausted".into());
            }
            for r in &mut body.configuration.replicas.0 {
                r.previous_role = r.current_role;
                if r.node == target.launch.node {
                    r.current_role = NativeReplicaRole::None;
                }
            }
            Some(ActiveConfiguration {
                body: body.clone(),
                reply: None,
                unknown: false,
                started: now,
                last: now,
            })
        } else {
            None
        };
        let mut unit = body.configuration.unit.clone();
        unit.previous = Epoch::default();
        let mut repair = self.new_repair(destination, unit, now, removal)?;
        repair.base = Some(active.clone());
        let request = repair.removal.as_ref().map(|c| c.body.clone());
        if let Some(old) = self.repair.replace(repair) {
            self.repair_history.push(old);
        }
        Ok(request)
    }

    pub fn begin_primary_repair(&mut self, now: Instant) -> Result<bool> {
        if !self.placements[0].launch.profile.mesh_peers() {
            return Err("Primary repair requires a configured replica-set profile".into());
        }
        if let Some(repair) = &self.repair {
            if repair.removal.is_some() || repair.base.is_some() {
                return Err("Secondary repair owns the current cycle".into());
            }
            return Ok(false);
        }
        let swap = self
            .swap
            .as_ref()
            .filter(|s| self.primary_failover() && !s.unknown && s.reply.is_some())
            .ok_or("repair requires completed Primary failover")?;
        let destination = self
            .failed_primary_index()
            .ok_or("missing failed Primary placement")?;
        if self.generation_pending()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || !self.placements[destination].loss_reported
            || !self.placements[destination].remove_received
            || self.placements.iter().any(|p| {
                !p.link_alive
                    || !p.activated
                    || p.bootstrap != Bootstrap::Admitted
                    || p.pending.is_some()
                    || p.node_control.is_some()
                    || p.service_update.is_some()
            })
            || self.placements.iter().enumerate().any(|(i, p)| {
                i != destination && (p.loss_reported || p.observed != Observed::Running)
            })
        {
            return Err(
                "repair requires processed old loss/deletion and every live survivor".into(),
            );
        }
        let reply = swap.reply.as_ref().expect("completed failover");
        if reply.replicas.0.len() + 1 != self.placements.len()
            || reply.replicas.0.iter().any(|r| {
                !r.up
                    || r.state != NativeReplicaState::Ready
                    || !matches!(
                        r.current_role,
                        NativeReplicaRole::Secondary | NativeReplicaRole::Primary
                    )
            })
        {
            return Err("repair requires exactly one missing configured member".into());
        }
        let unit = reply.unit.clone();
        self.repair = Some(self.new_repair(destination, unit, now, None)?);
        Ok(true)
    }

    fn new_repair(
        &self,
        destination: usize,
        unit: FailoverUnit,
        now: Instant,
        removal: Option<ActiveConfiguration>,
    ) -> Result<ReplicaRepair> {
        let next = self
            .placements
            .iter()
            .flat_map(|p| {
                std::iter::once((p.replica, p.incarnation)).chain(p.retired.iter().copied())
            })
            .fold(0, |n, (id, instance)| n.max(id).max(instance))
            .checked_add(1)
            .ok_or("repair identity exhausted")?;
        let mut body = self.placements[self.primary_index()].body()?;
        body.unit = unit;
        body.replica = self.placements[destination].body()?.replica;
        body.replica.id = next;
        body.replica.instance = next;
        body.replica.previous_role = NativeReplicaRole::None;
        body.replica.current_role = NativeReplicaRole::Idle;
        body.replica.state = NativeReplicaState::InBuild;
        body.replica.up = true;
        body.replica.first_lsn = -1;
        body.replica.last_lsn = -1;
        body.replica.service_endpoint.clear();
        body.replica.replication_endpoint.clear();
        Ok(ReplicaRepair {
            predecessor: self.placements[destination].body()?,
            old_profile: self.placements[destination]
                .profile
                .clone()
                .ok_or("missing original host profile")?,
            profile: None,
            build: IdleBuild {
                body,
                reply: None,
                cancelled: false,
                unknown: false,
                started: now,
                last: now,
            },
            configuration: None,
            removal,
            cancellation: None,
            deletion: None,
            base: None,
            host_requested: false,
            superseded_by: None,
            unknown: false,
        })
    }

    pub fn repaired_host(
        &mut self,
        receipt: &crate::profile::HostReplacementReceipt,
        node_pid: u32,
    ) -> Result<bool> {
        let destination = self
            .repair_destination_index()
            .ok_or("missing failed replica placement")?;
        let repair = self.repair.as_mut().ok_or("unsolicited host replacement")?;
        if repair.uses_spare() {
            return Err("spare placement cannot adopt a predecessor host receipt".into());
        }
        let placement = &mut self.placements[destination];
        receipt.validate(&placement.launch, &repair.old_profile, node_pid)?;
        if repair.unknown
            || !placement.link_alive
            || repair.removal.as_ref().is_some_and(|c| c.reply.is_none())
        {
            return Err("replacement owner is unknown".into());
        }
        if repair.profile.is_some() {
            if repair.profile.as_ref() != Some(&receipt.profile) {
                return Err("replacement host receipt changed after admission".into());
            }
            return Ok(false);
        }
        placement
            .retired
            .push((placement.replica, placement.incarnation));
        placement.replica = repair.build.body.replica.id;
        placement.incarnation = repair.build.body.replica.instance;
        placement.active_unit = Some(repair.build.body.unit.clone());
        placement.ready_replica = Some(repair.build.body.replica.clone());
        placement.service_description = Some(repair.build.body.service.clone());
        placement.profile = Some(receipt.profile.clone());
        placement.loss_reported = false;
        placement.remove_received = false;
        placement.observed = Observed::Pending;
        placement.endpoint = None;
        placement.error = None;
        repair.profile = Some(receipt.profile.clone());
        Ok(true)
    }

    fn repair_build_reply(&mut self, index: usize, body: AddReplicaReply) -> Result<()> {
        let live = self.repair_members_live();
        let primary = self.primary_index();
        let repair = self.repair.as_mut().expect("repair");
        let expected = &repair.build.body;
        if index != primary
            || !live
            || repair.unknown
            || repair.build.cancelled
            || repair.profile.is_none()
            || repair.build.reply.is_some()
            || body.unit != expected.unit
            || body.replica.node != expected.replica.node
            || body.replica.id != expected.replica.id
            || body.replica.instance != expected.replica.instance
        {
            return Ok(());
        }
        if body.error == 0
            && (body.replica.current_role != NativeReplicaRole::Idle
                || body.replica.previous_role != NativeReplicaRole::None
                || body.replica.state != NativeReplicaState::Ready
                || !body.replica.up
                || body.replica.version != expected.replica.version
                || !body.replica.service_endpoint.is_empty()
                || body.replica.replication_endpoint.is_empty())
        {
            return Err("repair build reply is not the fresh Ready Idle".into());
        }
        repair.build.reply = Some(body);
        Ok(())
    }

    pub fn start_repair_configuration(
        &mut self,
        now: Instant,
    ) -> Result<Option<DoReconfiguration>> {
        let live = self.repair_members_live();
        let Some(repair) = &mut self.repair else {
            return Ok(None);
        };
        if repair.unknown || repair.build.cancelled || repair.configuration.is_some() || !live {
            return Ok(None);
        }
        let Some(built) = repair.build.reply.as_ref().filter(|r| r.error == 0) else {
            return Ok(None);
        };
        let previous = repair
            .removal
            .as_ref()
            .or(repair.base.as_ref())
            .or(self.swap.as_ref())
            .and_then(|s| s.reply.as_ref())
            .ok_or("missing loss reconfiguration completion")?;
        let mut body = repair
            .removal
            .as_ref()
            .or(repair.base.as_ref())
            .or(self.swap.as_ref())
            .expect("loss configuration")
            .body
            .clone();
        body.configuration.unit = previous.unit.clone();
        body.configuration.unit.previous = previous.unit.current;
        body.configuration.unit.current.configuration = previous
            .unit
            .current
            .configuration
            .checked_add(1)
            .ok_or("repair configuration epoch exhausted")?;
        if body.configuration.unit.current.configuration & !0xffff_ffff
            != body.configuration.unit.previous.configuration & !0xffff_ffff
        {
            return Err("same-Primary repair epoch exhausted".into());
        }
        body.configuration.replicas = previous.replicas.clone();
        for r in &mut body.configuration.replicas.0 {
            r.previous_role = r.current_role;
            if !r.up || (repair.uses_spare() && r.node == repair.predecessor.replica.node) {
                r.current_role = NativeReplicaRole::None;
                r.up = false;
                r.state = NativeReplicaState::Dropped;
            }
        }
        let mut replica = built.replica.clone();
        replica.previous_role = NativeReplicaRole::Idle;
        replica.current_role = NativeReplicaRole::Secondary;
        body.configuration.replicas.0.push(replica);
        repair.configuration = Some(ActiveConfiguration {
            body: body.clone(),
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(Some(body))
    }

    fn repair_configuration_reply(&mut self, index: usize, body: ConfigurationReply) -> Result<()> {
        let live = self.repair_members_live();
        let primary = self.primary_index();
        let repair = self.repair.as_mut().expect("repair");
        let config = repair.configuration.as_mut().expect("repair configuration");
        let mut expected = config.body.configuration.clone();
        expected.unit.previous = Epoch::default();
        expected.replicas.0.retain(|r| r.current_role.is_active());
        for r in &mut expected.replicas.0 {
            r.previous_role = NativeReplicaRole::None;
        }
        if index != primary
            || !live
            || repair.unknown
            || config.reply.is_some()
            || body.error != 0
            || body.unit != expected.unit
            || body.replicas != expected.replicas
        {
            return Ok(());
        }
        for p in &mut self.placements {
            if !body.replicas.0.iter().any(|r| r.node == p.launch.node) {
                continue;
            }
            p.active_unit = Some(body.unit.clone());
            p.ready_replica = body
                .replicas
                .0
                .iter()
                .find(|r| r.node == p.launch.node)
                .cloned();
            p.service_description = Some(expected.service.clone());
            p.observed = Observed::Running;
            p.desired = Desired::Running;
            p.created = true;
            if p.launch.node == repair.build.body.replica.node {
                p.replica = repair.build.body.replica.id;
                p.incarnation = repair.build.body.replica.instance;
            }
        }
        config.reply = Some(body);
        if self.placements.iter().all(|p| !p.loss_reported)
            && expected.replicas.0.len() == self.placements.len()
        {
            self.restoration = None;
        }
        Ok(())
    }

    pub fn cancel_repair_build(&mut self, now: Instant) -> Result<RemoveReplica> {
        let live = self.repair_members_live();
        let repair = self.repair.as_mut().ok_or("no repair build to cancel")?;
        if !live
            || repair.unknown
            || repair.uses_spare()
            || repair.removal.is_some()
            || repair.base.is_some()
            || repair.configuration.is_some()
            || repair.profile.is_none()
            || !repair.host_requested
            || repair.build.body.service.target != 3
            || repair.build.body.service.minimum != 2
        {
            return Err("cancellation requires the known fixed-3/2 original-Node repair Idle before admission".into());
        }
        let cancellation = repair.cancellation.get_or_insert_with(|| IdleRemoval {
            body: repair.build.body.clone(),
            reply: None,
            failed_creation: false,
            unknown: false,
            started: now,
            last: now,
        });
        if cancellation.unknown {
            return Err("repair cancellation outcome is unknown".into());
        }
        cancellation.last = now;
        repair.build.cancelled = true;
        let body = cancellation.body.clone();
        if let Some(inventory) = self
            .generation
            .as_mut()
            .and_then(|g| g.repair.as_mut())
            .filter(|r| r.build == body)
        {
            inventory.cancelled = true;
        }
        Ok(body)
    }

    fn repair_removal_reply(&mut self, index: usize, body: RemoveReplicaReply) -> Result<()> {
        let primary = self.primary_index();
        let repair = self.repair.as_mut().ok_or("unsolicited repair removal")?;
        let cancellation = repair
            .cancellation
            .as_mut()
            .ok_or("repair was not cancelled")?;
        let expected = &cancellation.body;
        if index != primary
            || repair.unknown
            || cancellation.unknown
            || body.unit != expected.unit
            || body.replica.node != expected.replica.node
            || body.replica.id != expected.replica.id
            || body.replica.instance != expected.replica.instance
            || body.error != 0
        {
            return Ok(());
        }
        let mut replica = expected.replica.clone();
        replica.state = NativeReplicaState::Dropped;
        replica.replication_endpoint = body.replica.replication_endpoint.clone();
        if body.replica != replica || replica.replication_endpoint.is_empty() {
            return Err("repair RemoveReplicaReply changed the cancelled Idle".into());
        }
        if cancellation
            .reply
            .as_ref()
            .is_some_and(|previous| previous != &body)
        {
            return Err("repair removal completion changed after acceptance".into());
        }
        cancellation.reply = Some(body);
        Ok(())
    }

    pub fn delete_cancelled_repair(&mut self) -> Result<DeleteReplica> {
        let repair = self.repair.as_mut().ok_or("no repair to delete")?;
        if repair.unknown || repair.cancellation.as_ref().is_none_or(|c| c.unknown) {
            return Err("destination deletion requires the known cancelled repair owner".into());
        }
        let now = Instant::now();
        repair
            .deletion
            .get_or_insert(RepairDeletion {
                reply: None,
                disposal: None,
                started: now,
                last: now,
            })
            .last = now;
        let body = repair.build.body.clone();
        if let Some(inventory) = self
            .generation
            .as_mut()
            .and_then(|g| g.repair.as_mut())
            .filter(|r| r.build == body)
        {
            inventory.deleting = true;
        }
        Ok(DeleteReplica::new(body, false))
    }

    fn repair_deletion_reply(&mut self, index: usize, body: DeleteReplicaReply) -> Result<()> {
        let destination = self
            .repair_destination_index()
            .ok_or("missing cancelled destination")?;
        let repair = self.repair.as_mut().expect("repair");
        if index != destination
            || repair.deletion.is_none()
            || repair.unknown
            || body.unit != repair.build.body.unit
            || body.error != 0
            || body.replica.node != repair.build.body.replica.node
            || body.replica.id != repair.build.body.replica.id
            || body.replica.instance != repair.build.body.replica.instance
        {
            return Ok(());
        }
        if body.replica != dropped_replica(&repair.build.body.replica) {
            return Err("repair deletion reply is not the cancelled dropped lifetime".into());
        }
        repair
            .deletion
            .as_mut()
            .expect("requested repair deletion")
            .reply = Some(body);
        Ok(())
    }

    pub fn repair_deletion_retry(&mut self, index: usize, now: Instant) -> Option<DeleteReplica> {
        if self.repair_destination_index()? != index {
            return None;
        }
        let repair = self.repair.as_mut()?;
        let deletion = repair.deletion.as_mut()?;
        if repair.unknown
            || deletion.reply.is_some()
            || !action_retry_due(deletion.started, deletion.last, now)
        {
            return None;
        }
        deletion.last = now;
        Some(DeleteReplica::new(repair.build.body.clone(), false))
    }

    pub fn repair_status(&self) -> Value {
        json!(self.repair_view())
    }

    pub fn repair_view(&self) -> RepairStatus {
        let Some(r) = self.repair.as_ref().or_else(|| {
            self.repair_history
                .last()
                .filter(|r| r.superseded_by.is_some())
        }) else {
            return RepairStatus::new(RepairState::Absent, false, None);
        };
        let state = if r.superseded_by.is_some() {
            RepairState::Superseded
        } else if r.unknown {
            RepairState::Unknown
        } else if r.build.cancelled {
            RepairState::Cancelled
        } else if r.configuration.as_ref().is_some_and(|c| c.reply.is_some()) {
            RepairState::Ready
        } else if r.build.reply.as_ref().is_some_and(|b| b.error != 0) {
            RepairState::Failed
        } else {
            RepairState::Pending
        };
        RepairStatus::new(
            state,
            state == RepairState::Ready,
            Some(if r.uses_spare() {
                RepairKind::SpareReplacement
            } else if r.base.is_some() {
                RepairKind::Restoration
            } else if r.removal.is_some() {
                RepairKind::Secondary
            } else {
                RepairKind::Primary
            }),
        )
        .detail("predecessor", &r.predecessor)
        .detail(
            "target_restored",
            state == RepairState::Ready && self.restoration.is_none(),
        )
        .detail(
            "remaining_lost",
            self.placements
                .iter()
                .enumerate()
                .filter(|(_, p)| p.loss_reported)
                .map(|(i, _)| i + 1)
                .collect::<Vec<_>>(),
        )
        .detail("superseded_by", r.superseded_by)
        .detail(
            "removed",
            r.removal.as_ref().is_some_and(|c| c.reply.is_some()),
        )
        .detail("old_profile", &r.old_profile)
        .detail("profile", &r.profile)
        .detail("request", &r.build.body)
        .detail("build_reply", &r.build.reply)
        .detail("host_ready", r.profile.is_some())
        .detail(
            "built",
            r.build.reply.as_ref().is_some_and(|b| b.error == 0),
        )
        .detail("cancelled", r.build.cancelled)
        .detail(
            "pending_remove",
            r.cancellation.as_ref().is_some_and(|c| c.reply.is_none()),
        )
        .detail(
            "replicator_removed",
            r.cancellation.as_ref().is_some_and(|c| c.reply.is_some()),
        )
        .detail(
            "destination_deleted",
            r.deletion.as_ref().is_some_and(|d| d.reply.is_some()),
        )
        .detail(
            "disposed",
            r.deletion.as_ref().is_some_and(|d| d.disposal.is_some()),
        )
    }
    pub fn request_idle_on(&mut self, destination: usize, now: Instant) -> Result<AddReplica> {
        let count = self.placements[0].launch.profile.node_count();
        if destination == 0
            || destination >= self.placements.len()
            || (self.placements[0]
                .launch
                .profile
                .configured_sizes()
                .is_some()
                && destination >= count)
        {
            return Err("unknown original Idle destination".into());
        }
        if (self.configuration.is_some()
            && !(self.retired_secondary() && self.idle_predecessor.is_some()))
            || !self.placements[0].launch.profile.peer_replicas()
            || self.placements.iter().any(|p| p.loss_reported)
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.generation_pending()
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
        body.replica.current_role = NativeReplicaRole::Idle;
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
        let configured = self.placements[0].launch.profile.configured_sizes();
        let count = configured.map_or(self.placements.len() as i32, |(target, _)| target);
        if self.swap.is_some() {
            return Err("another configuration after Primary transfer is gated".into());
        }
        let successor = self.retired_secondary()
            && self.idle_predecessor.is_some()
            && self.idles.first().is_some_and(|i| {
                i.reply.as_ref().is_some_and(|r| r.error == 0) && !i.cancelled && !i.unknown
            });
        if !self.placements[0].launch.profile.peer_replicas()
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
        if self.idles.len() != count as usize - 1 {
            return Err("configuration requires every original FM Idle build".into());
        }
        let mut primary = self.placements[0]
            .ready_replica
            .clone()
            .ok_or("missing original Primary reply")?;
        primary.previous_role = NativeReplicaRole::Primary;
        let mut replicas = vec![primary];
        for idle in &self.idles {
            let reply = idle
                .reply
                .as_ref()
                .filter(|r| r.error == 0 && !idle.cancelled && !idle.unknown)
                .ok_or("configuration requires successful FM Idle build")?;
            let mut secondary = reply.replica.clone();
            secondary.previous_role = NativeReplicaRole::Idle;
            secondary.current_role = NativeReplicaRole::Secondary;
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
        if let Some((target, minimum)) = configured {
            if target != count || body.service.target != target || body.service.minimum != minimum {
                return Err("initial membership cannot resize the configured service".into());
            }
        } else {
            body.unit.target = count;
            body.unit.minimum = if count == 3 { 2 } else { 1 };
            body.service.target = count;
            body.service.minimum = body.unit.minimum;
            body.service.update_version = body
                .service
                .update_version
                .checked_add(1)
                .ok_or("service version exhausted")?;
        }
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
            if r.current_role == NativeReplicaRole::Secondary {
                r.current_role = NativeReplicaRole::None;
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
        if self
            .repair
            .as_ref()
            .is_some_and(|r| r.removal.as_ref().is_some_and(|c| c.reply.is_none()))
        {
            let primary = self.primary_index();
            let destination = self.repair_destination_index().expect("repair destination");
            let live = self.placements.iter().enumerate().all(|(i, p)| {
                p.link_alive
                    && (i == destination || !p.loss_reported || self.known_restoration_loss(p))
            });
            let repair = self.repair.as_mut().expect("repair");
            let config = repair.removal.as_mut().expect("removal");
            let mut expected = config.body.configuration.clone();
            expected.unit.previous = Epoch::default();
            expected.replicas.0.retain(|r| r.current_role.is_active());
            for r in &mut expected.replicas.0 {
                r.previous_role = NativeReplicaRole::None;
            }
            if index != primary
                || !live
                || repair.unknown
                || body.error != 0
                || body.unit != expected.unit
                || body.replicas != expected.replicas
            {
                return Ok(());
            }
            for p in &mut self.placements {
                if let Some(r) = body.replicas.0.iter().find(|r| r.node == p.launch.node) {
                    p.active_unit = Some(body.unit.clone());
                    p.ready_replica = Some(r.clone());
                }
            }
            config.reply = Some(body);
            return Ok(());
        }
        if self
            .repair
            .as_ref()
            .is_some_and(|r| r.configuration.is_some())
        {
            return self.repair_configuration_reply(index, body);
        }
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
            replica.previous_role = NativeReplicaRole::None;
        }
        expected.replicas.0.retain(|r| r.current_role.is_active());
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
        if self.placements[0]
            .launch
            .profile
            .configured_sizes()
            .is_some()
        {
            if let Some(op) = self.placements[0].create_op.clone() {
                let mut outcome = self
                    .operations
                    .iter()
                    .find(|operation| operation.command.op == op)
                    .ok_or("initial membership lacks its retained creation operation")?
                    .outcome
                    .clone();
                outcome.state = OperationState::Complete;
                self.complete(&op, outcome);
            }
        }
        Ok(())
    }
    pub fn configuration_retry(&mut self, now: Instant) -> Option<DoReconfiguration> {
        if let Some(repair) = &mut self.repair {
            let config = repair.configuration.as_mut().or(repair.removal.as_mut())?;
            if repair.unknown
                || config.unknown
                || config.reply.is_some()
                || !action_retry_due(config.started, config.last, now)
            {
                return None;
            }
            config.last = now;
            return Some(config.body.clone());
        }
        let live = self.primary_transition_live();
        if let Some(swap) = &mut self.swap {
            if swap.unknown
                || swap.reply.is_some()
                || !live
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
        let destination =
            if self.repair.is_none() && self.swap.is_some() && !self.primary_failover() {
                self.transition_primary_index()
            } else {
                self.repair
                    .as_ref()
                    .and_then(|r| r.configuration.as_ref())
                    .or(self.swap.as_ref())
                    .or(self.configuration.as_ref())
                    .and_then(|c| c.reply.as_ref())
                    .and_then(|reply| {
                        self.placements.iter().position(|p| {
                            reply.replicas.0.iter().any(|r| {
                                r.node == p.launch.node
                                    && r.current_role == NativeReplicaRole::Secondary
                                    && r.up
                            })
                        })
                    })
                    .ok_or("Primary transfer requires a completed active Secondary")?
            };
        self.request_primary_transfer_to(destination, now)
    }

    pub fn request_primary_transfer_to(
        &mut self,
        destination: usize,
        now: Instant,
    ) -> Result<DoReconfiguration> {
        if !matches!(
            self.placements[0].launch.profile,
            crate::ServiceProfile::StatefulPeerIdle
                | crate::ServiceProfile::StatefulThree
                | crate::ServiceProfile::Stateful {
                    target: 2,
                    minimum: 1
                }
                | crate::ServiceProfile::Stateful {
                    target: 3,
                    minimum: 2
                }
        ) || self.generation_pending()
            || destination >= self.placements.len()
            || self.restoration.is_some()
            || self.idle_removal.is_some()
            || self.idle_deletion.is_some()
            || self.repair.as_ref().is_some_and(|r| {
                r.unknown
                    || r.configuration
                        .as_ref()
                        .is_none_or(|c| c.unknown || c.reply.is_none())
            })
        {
            return Err("Primary transfer requires completed healthy 2/1 or 3/2 membership".into());
        }
        let candidate = self.placements[destination].launch.node.clone();
        if let Some(swap) = self.swap.as_ref().filter(|_| self.repair.is_none()) {
            if swap.unknown {
                return Err("Primary transfer ownership is unknown".into());
            }
            if !self.primary_failover()
                && swap
                    .body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .any(|r| r.node == candidate && r.current_role == NativeReplicaRole::Primary)
            {
                if !self.healthy_transfer_members(&swap.body.configuration.replicas.0) {
                    return Err("Primary transfer requires healthy current members".into());
                }
                let swap = self.swap.as_mut().expect("swap");
                swap.last = now;
                return Ok(swap.body.clone());
            }
            if swap.reply.is_none() {
                return Err("another Primary transition is still pending".into());
            }
        }
        let active = self
            .repair
            .as_ref()
            .and_then(|r| r.configuration.as_ref())
            .or(self.swap.as_ref())
            .or(self.configuration.as_ref())
            .filter(|c| c.reply.is_some() && !c.unknown)
            .ok_or("Primary transfer requires completed current membership")?;
        let reply = active.reply.as_ref().expect("completed configuration");
        if reply.error != 0
            || reply.replicas.0.len() != reply.unit.target as usize
            || !reply
                .replicas
                .0
                .iter()
                .any(|r| r.node == candidate && r.current_role == NativeReplicaRole::Secondary)
            || !self.healthy_transfer_members(&reply.replicas.0)
        {
            return Err("Primary transfer requires a healthy current Secondary and every current member Running".into());
        }
        let mut body = active.body.clone();
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
            r.current_role = if r.node == candidate {
                NativeReplicaRole::Primary
            } else {
                NativeReplicaRole::Secondary
            };
        }
        body.phase0_duration = i64::MAX;
        for p in &mut self.placements {
            let Some(replica) = body
                .configuration
                .replicas
                .0
                .iter()
                .find(|r| r.node == p.launch.node)
            else {
                continue;
            };
            p.active_unit = Some(body.configuration.unit.clone());
            p.ready_replica = Some(replica.clone());
            p.endpoint = None;
        }
        if let Some(repair) = self.repair.take() {
            self.repair_history.push(repair);
        }
        if let Some(swap) = self.swap.take() {
            self.configuration_history.push(swap);
        }
        self.candidate_changes.clear();
        self.swap_endpoint = None;
        self.swap = Some(ActiveConfiguration {
            body: body.clone(),
            reply: None,
            unknown: false,
            started: now,
            last: now,
        });
        Ok(body)
    }
    fn healthy_transfer_members(&self, replicas: &[Replica]) -> bool {
        replicas.iter().all(|r| {
            r.up && r.state == NativeReplicaState::Ready
                && matches!(
                    r.current_role,
                    NativeReplicaRole::Secondary | NativeReplicaRole::Primary
                )
                && self.placements.iter().any(|p| {
                    p.launch.node == r.node
                        && p.link_alive
                        && !p.loss_reported
                        && p.activated
                        && p.bootstrap == Bootstrap::Admitted
                        && p.observed == Observed::Running
                        && p.pending.is_none()
                        && p.node_control.is_none()
                        && p.service_update.is_none()
                })
        })
    }
    pub fn endpoint_updated(
        &mut self,
        index: usize,
        body: AddInstance,
    ) -> Result<Option<AddInstanceReply>> {
        let live = self.primary_transition_live();
        let Some(swap) = self
            .swap
            .as_ref()
            .filter(|s| !s.unknown && s.reply.is_none())
        else {
            return Ok(None);
        };
        if index != self.transition_primary_index() || !live {
            return Ok(None);
        }
        let expected = swap
            .body
            .configuration
            .replicas
            .0
            .iter()
            .find(|r| r.node == self.placements[index].launch.node)
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
        let failover = self.primary_failover();
        let same_primary_node_down = !failover
            && self.swap.as_ref().is_some_and(|swap| {
                self.node_reconfiguration == Some(swap.body.configuration.unit.current)
            });
        let live = self.primary_transition_live();
        let primary = self.transition_primary_index();
        let lost = self.failed_primary_index();
        let Some(swap) = self
            .swap
            .as_mut()
            .filter(|s| !s.unknown && s.reply.is_none())
        else {
            return Ok(());
        };
        let mut unit = swap.body.configuration.unit.clone();
        unit.previous = Epoch::default();
        if index != primary
            || body.error != 0
            || body.unit != unit
            || body.replicas.0.len()
                != swap
                    .body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.current_role.is_active())
                    .count()
            || swap
                .body
                .configuration
                .replicas
                .0
                .iter()
                .filter(|r| r.current_role.is_active())
                .any(|expected| {
                    body.replicas
                        .0
                        .iter()
                        .filter(|r| r.node == expected.node)
                        .count()
                        != 1
                })
            || !live
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
            let restarted = !failover
                && body.unit.target == 3
                && body.unit.minimum == 2
                && r.current_role == NativeReplicaRole::Secondary
                && !r.up
                && r.state == NativeReplicaState::Dropped
                && body
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.up && r.state == NativeReplicaState::Ready)
                    .count()
                    >= 2;
            if restarted {
                actual.up = expected.up;
                actual.state = expected.state;
            }
            actual.previous_role = expected.previous_role;
            actual.first_lsn = expected.first_lsn;
            actual.last_lsn = expected.last_lsn;
            actual.service_endpoint = expected.service_endpoint.clone();
            if actual != *expected
                || r.previous_role != NativeReplicaRole::None
                || (r.up
                    && if same_primary_node_down {
                        r.first_lsn != expected.first_lsn || r.last_lsn != expected.last_lsn
                    } else {
                        r.first_lsn < 0 || r.last_lsn < 0
                    })
            {
                return Ok(());
            }
            if r.current_role == NativeReplicaRole::Primary {
                let endpoint: std::net::SocketAddr = r
                    .service_endpoint
                    .parse()
                    .map_err(|_| "invalid final Primary endpoint")?;
                if !endpoint.ip().is_loopback() || endpoint.port() == 0 {
                    return Err("unsupported final Primary endpoint".into());
                }
            } else if r.up && !r.service_endpoint.is_empty() {
                return Err("demoted Primary retained a published service endpoint".into());
            }
        }
        for (i, p) in self.placements.iter_mut().enumerate() {
            if (failover && Some(i) == lost)
                || !swap
                    .body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .any(|r| r.node == p.launch.node && r.up && r.current_role.is_active())
            {
                continue;
            }
            let replica = body
                .replicas
                .0
                .iter()
                .find(|r| r.node == p.launch.node)
                .ok_or("swap omitted placement")?
                .clone();
            if !replica.up {
                p.endpoint = None;
                continue;
            }
            p.endpoint = (replica.current_role == NativeReplicaRole::Primary)
                .then(|| replica.service_endpoint.clone());
            p.ready_replica = Some(replica);
            p.active_unit = Some(body.unit.clone());
            p.observed = Observed::Running;
        }
        swap.reply = Some(body);
        Ok(())
    }
    pub fn configuration_status(&self) -> Value {
        json!(self.configuration_view())
    }

    pub fn configuration_view(&self) -> ConfigurationStatus {
        let view = |config: &ActiveConfiguration, unknown, kind| ConfigurationStatus {
            state: if unknown {
                ConfigurationState::Unknown
            } else if config.reply.is_some() {
                ConfigurationState::Ready
            } else {
                ConfigurationState::Pending
            },
            ready: !unknown && config.reply.is_some(),
            kind: Some(kind),
            request: Some(ConfigurationRequest {
                configuration: (&config.body.configuration).into(),
                phase0_duration: config.body.phase0_duration,
            }),
            details: [("reply".into(), json!(config.reply))]
                .into_iter()
                .collect(),
        };
        if let Some(repair) = &self.repair {
            if let Some(config) = repair.configuration.as_ref().or(repair.removal.as_ref()) {
                let kind = if repair.uses_spare() {
                    ConfigurationKind::SpareReplacement
                } else if repair.base.is_some() {
                    ConfigurationKind::SequentialRestoration
                } else if repair.configuration.is_some() {
                    if repair.removal.is_some() {
                        ConfigurationKind::SecondaryRepair
                    } else {
                        ConfigurationKind::PostFailoverRepair
                    }
                } else {
                    ConfigurationKind::SecondaryLossRemoval
                };
                return view(config, repair.unknown || config.unknown, kind);
            }
        }
        if let Some(swap) = &self.swap {
            let kind = if self.node_reconfiguration == Some(swap.body.configuration.unit.current) {
                ConfigurationKind::NodeDownReconfiguration
            } else if self.primary_failover() {
                ConfigurationKind::PrimaryFailover
            } else {
                ConfigurationKind::PlannedPrimaryTransfer
            };
            return view(swap, swap.unknown, kind)
                .detail("endpoint_update", &self.swap_endpoint)
                .detail("primary_transfer", true)
                .detail(
                    "from",
                    self.placements
                        .iter()
                        .position(|p| {
                            swap.body.configuration.replicas.0.iter().any(|r| {
                                r.node == p.launch.node
                                    && r.previous_role == NativeReplicaRole::Primary
                            })
                        })
                        .map(|i| i + 1),
                )
                .detail("to", self.transition_primary_index() + 1)
                .detail("candidate_redirections", &self.candidate_changes)
                .detail(
                    "history",
                    self.configuration_history
                        .iter()
                        .chain(self.configuration.iter())
                        .map(|c| json!({"request":c.body.configuration,"reply":c.reply}))
                        .collect::<Vec<_>>(),
                );
        }
        let Some(config) = &self.configuration else {
            return ConfigurationStatus {
                state: ConfigurationState::Absent,
                ready: false,
                kind: None,
                request: None,
                details: Default::default(),
            };
        };
        view(
            config,
            config.unknown,
            if config.body.configuration.unit.target == 1 {
                ConfigurationKind::SamePrimaryRetirement
            } else {
                ConfigurationKind::SamePrimaryConfiguration
            },
        )
        .detail("primary_transfer", false)
        .detail(
            "history",
            self.configuration_history
                .iter()
                .map(|c| json!({"request":c.body.configuration,"reply":c.reply}))
                .collect::<Vec<_>>(),
        )
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
        body.replica.previous_role = NativeReplicaRole::None;
        body.replica.current_role = NativeReplicaRole::Idle;
        body.replica.state = NativeReplicaState::InBuild;
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
        if self.repair.is_some() {
            return self.repair_build_reply(index, body);
        }
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
            && (body.replica.current_role != NativeReplicaRole::Idle
                || body.replica.previous_role != NativeReplicaRole::None
                || body.replica.state != NativeReplicaState::Ready
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
        let generation_blocks = self.generation.as_ref().is_some_and(|round| {
            round.phase != GenerationPhase::Complete && round.initial_builds.is_none()
        });
        let primary = self.primary_index();
        if let Some(repair) = &mut self.repair {
            if index != primary
                || repair.unknown
                || repair.build.cancelled
                || repair.profile.is_none()
                || repair.build.reply.is_some()
                || !action_retry_due(repair.build.started, repair.build.last, now)
            {
                return None;
            }
            repair.build.last = now;
            return Some(repair.build.body.clone());
        }
        let idle = self.idles.iter_mut().find(|i| {
            i.reply.is_none()
                && !i.cancelled
                && !i.unknown
                && action_retry_due(i.started, i.last, now)
        })?;
        if index != 0
            || generation_blocks
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
        json!(self.idle_view())
    }

    pub fn idle_view(&self) -> IdleStatus {
        if self.placements[0].launch.profile == crate::ServiceProfile::StatefulThree
            || self.placements[0]
                .launch
                .profile
                .configured_sizes()
                .is_some()
        {
            let builds: Vec<_> = self
                .idles
                .iter()
                .map(|idle| {
                    let error = idle.reply.as_ref().map(|r| r.error);
                    let state = if idle.unknown {
                        IdleState::Unknown
                    } else if idle.cancelled {
                        IdleState::Cancelled
                    } else if error == Some(0) {
                        IdleState::Ready
                    } else if error.is_some() {
                        IdleState::Failed
                    } else {
                        IdleState::Pending
                    };
                    IdleStatus::new(state, state == IdleState::Ready, None)
                        .detail("request", &idle.body)
                        .detail("reply", &idle.reply)
                        .detail("to_be_dropped", error.is_some_and(|e| e != 0))
                        .detail("cleanup_required", error.is_some_and(|e| e != 0))
                })
                .collect();
            let ready = builds.len() == self.placements[0].launch.profile.node_count() - 1
                && builds.iter().all(|b| b.ready);
            let state = if builds.iter().any(|b| b.state == IdleState::Unknown) {
                IdleState::Unknown
            } else if builds.iter().any(|b| b.state == IdleState::Failed) {
                IdleState::Failed
            } else if ready {
                IdleState::Ready
            } else if builds.is_empty() {
                IdleState::Absent
            } else {
                IdleState::Pending
            };
            return IdleStatus::new(state, ready, None)
                .detail("serving", false)
                .detail("builds", builds);
        }
        let Some(idle) = self.idles.first() else {
            return IdleStatus::new(IdleState::Absent, false, None);
        };
        let error = idle.reply.as_ref().map(|reply| reply.error);
        let destination_lost = self.placements[1].loss_reported;
        let state = if destination_lost {
            IdleState::Lost
        } else if idle.unknown {
            IdleState::Unknown
        } else if idle.cancelled {
            IdleState::Cancelled
        } else if error == Some(0) {
            IdleState::Ready
        } else if error.is_some() {
            IdleState::Failed
        } else {
            IdleState::Pending
        };
        IdleStatus::new(
            state,
            state == IdleState::Ready,
            Some(IdleKind::FmIdleBuild),
        )
        .detail("serving", false)
        .detail("request", &idle.body)
        .detail("reply", &idle.reply)
        .detail("destination_lost", destination_lost)
        .detail("native_replica_deleted", self.placements[1].remove_received)
        .detail(
            "to_be_dropped",
            (error.is_some_and(|e| e != 0) || self.idle_deletion.is_some())
                && !self.idle_destination_deleted(),
        )
        .detail(
            "destination_cleanup_required",
            (destination_lost || error.is_some_and(|e| e != 0) || self.idle_deletion.is_some())
                && !self.idle_deletion_complete(),
        )
        .detail("error", error)
        .detail(
            "cleanup_required",
            (destination_lost || idle.cancelled || error.is_some_and(|e| e != 0))
                && self
                    .idle_removal
                    .as_ref()
                    .is_none_or(|removal| removal.reply.is_none()),
        )
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
        body.replica.current_role = NativeReplicaRole::Idle;
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
        if self.repair.as_ref().is_some_and(|r| r.build.cancelled) {
            return self.repair_removal_reply(index, body);
        }
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
        let echoed_creation = removal.body.replica.state == NativeReplicaState::InBuild
            && removal.body.replica.replication_endpoint.is_empty()
            && body.replica == removal.body.replica;
        if !echoed_creation
            && (removal.failed_creation
                || body.replica.current_role != NativeReplicaRole::Idle
                || body.replica.previous_role != NativeReplicaRole::None
                || body.replica.state != NativeReplicaState::Dropped
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
        let primary = self.primary_index();
        if let Some(repair) = &mut self.repair {
            let removal = repair.cancellation.as_mut()?;
            if index != primary
                || repair.unknown
                || removal.unknown
                || removal.reply.is_some()
                || !action_retry_due(removal.started, removal.last, now)
            {
                return None;
            }
            removal.last = now;
            return Some(removal.body.clone());
        }
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
        json!(self.idle_removal_view())
    }

    pub fn idle_removal_view(&self) -> RemovalStatus {
        let Some(removal) = &self.idle_removal else {
            return RemovalStatus {
                state: RemovalState::Absent,
                pending_remove: false,
                removed: false,
                completion: None,
                details: Default::default(),
            };
        };
        let state = if removal.reply.is_some() {
            RemovalState::Removed
        } else if removal.unknown {
            RemovalState::Unknown
        } else {
            RemovalState::Pending
        };
        RemovalStatus {
            state,
            pending_remove: removal.reply.is_none(),
            removed: state == RemovalState::Removed,
            completion: Some(removal.reply.as_ref().map(|reply| {
                if removal.failed_creation {
                    RemovalCompletion::FailedCreationRetired
                } else if reply.replica.state == NativeReplicaState::InBuild {
                    RemovalCompletion::CreationCancelled
                } else {
                    RemovalCompletion::ReplicatorRemoved
                }
            })),
            details: [
                ("kind".into(), json!("fm_idle_removal")),
                ("request".into(), json!(removal.body)),
                ("reply".into(), json!(removal.reply)),
                (
                    "destination_deleted".into(),
                    json!(self.idle_destination_deleted()),
                ),
                ("serving".into(), json!(false)),
            ]
            .into_iter()
            .collect(),
        }
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
        json!(self.idle_deletion_view())
    }

    pub fn idle_deletion_view(&self) -> DeletionStatus {
        let Some(deletion) = &self.idle_deletion else {
            return DeletionStatus {
                state: DeletionState::Absent,
                destination_deleted: false,
                disposed: false,
                never_created: false,
                pre_open_cancelled: false,
                failed_registration: false,
                failed_open: false,
                complete: false,
                cleanup_state: None,
                completion: None,
                details: Default::default(),
            };
        };
        let complete = self.idle_deletion_complete();
        DeletionStatus {
            state: if complete {
                DeletionState::Deleted
            } else if deletion.unknown {
                DeletionState::Unknown
            } else {
                DeletionState::Pending
            },
            destination_deleted: self.idle_destination_deleted(),
            disposed: self.placements[1].retirement.is_some() || deletion.failed_open.is_some(),
            never_created: deletion.absence.is_some(),
            pre_open_cancelled: deletion.cancellation.is_some(),
            failed_registration: deletion.failed_registration.is_some(),
            failed_open: deletion.failed_open.is_some(),
            complete,
            cleanup_state: Some(if complete {
                CleanupState::Complete
            } else if deletion.unknown {
                CleanupState::Unknown
            } else if deletion.reply.is_some() {
                CleanupState::Unproved
            } else {
                CleanupState::PendingReply
            }),
            completion: Some(if !complete {
                None
            } else if self.placements[1].retirement.is_some() {
                Some(DeletionCompletion::Disposed)
            } else if deletion.absence.is_some() {
                Some(DeletionCompletion::NeverCreated)
            } else if deletion.cancellation.is_some() {
                Some(DeletionCompletion::PreOpenCancelled)
            } else if deletion.failed_registration.is_some() {
                Some(DeletionCompletion::FailedRegistration)
            } else if deletion.failed_open.is_some() {
                Some(DeletionCompletion::FailedOpen)
            } else if deletion.host_loss.is_some() {
                Some(DeletionCompletion::HostLost)
            } else {
                None
            }),
            details: [
                ("kind".into(), json!("fm_idle_deletion")),
                ("request".into(), json!(deletion.body)),
                ("reply".into(), json!(deletion.reply)),
                ("host_lost".into(), json!(deletion.host_loss.is_some())),
                ("force".into(), json!(deletion.body.deletion.0.force)),
                ("cleanup_required".into(), json!(!complete)),
                (
                    "pending_remove".into(),
                    json!(self
                        .idle_removal
                        .as_ref()
                        .is_some_and(|removal| removal.reply.is_none())),
                ),
                ("reusable".into(), json!(false)),
                ("serving".into(), json!(false)),
            ]
            .into_iter()
            .collect(),
        }
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
