//! Production, caller-driven bounded local RA/RAP on one owned activation.
//!
//! This is trusted local command authority, not FM/HM serving readiness.
//! `drive` advances retained adapter work; observation/drop never cancels it.
//! RA, IPC, lease and process supervision remain independently owned. Retain
//! this owner on Busy, Unknown or error; abandonment is not a cleanup receipt.
use super::{command, service_type, version};
use fube_hosting::{self as h, ipc, local::Activation};
use fube_ra::{entity as ra, protocol::*};
use fube_ra_store::KeyValueStore;
use fube_transport::serialization::serde::{from_slice_with_budget, to_vec, ObjectArray};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const SLOTS: usize = 32;
const BODY: usize = 64 * 1024;
// Includes native RAP's runtime/remote/flags wrapper, RA normalization and full
// transport headers/CRC. Never acquire usage for an unsendable near-limit Add.
const ADD_BODY: usize = BODY - 4096;
const RETRY: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Activation(h::local::LocalError),
    Hosting(h::Error),
    Ra(ra::Failure),
    Capacity,
    Unsupported,
    Protocol,
    OwnerLost,
    Busy,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<ra::Failure> for Error {
    fn from(v: ra::Failure) -> Self {
        Self::Ra(v)
    }
}
impl From<h::Error> for Error {
    fn from(v: h::Error) -> Self {
        Self::Hosting(v)
    }
}
impl From<h::local::LocalError> for Error {
    fn from(v: h::local::LocalError) -> Self {
        Self::Activation(v)
    }
}

/// Native ProxyRequest/ProxyReply shapes. These are production codecs, not the
/// runtime's private records or a dependency on a test host.
pub mod rap {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Request {
        pub runtime: String,
        pub unit: FailoverUnit,
        pub local: Replica,
        pub remote: ObjectArray<Vec<Replica>>,
        pub service: Service,
        pub flags: i64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ProxyError {
        pub error: i64,
        pub message: String,
        pub api: ra::ApiNameDescription,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Reply {
        pub unit: FailoverUnit,
        pub local: Replica,
        pub remote: ObjectArray<Vec<Replica>>,
        pub error: ProxyError,
        pub flags: i64,
    }
    pub fn request(
        registration: &ra::RuntimeRegistration,
        body: &AddInstance,
        flags: i64,
    ) -> Result<Vec<u8>> {
        to_vec(&Request {
            runtime: registration.runtime.clone(),
            unit: body.unit.clone(),
            local: body.replica.clone(),
            remote: ObjectArray(vec![]),
            service: body.service.clone(),
            flags,
        })
        .map_err(|_| Error::Protocol)
    }
    pub fn idle_request(body: &AddInstance, replica: &Replica) -> Result<Vec<u8>> {
        to_vec(&Request {
            runtime: String::new(),
            unit: body.unit.clone(),
            local: body.replica.clone(),
            remote: ObjectArray(vec![replica.clone()]),
            service: Service::default(),
            flags: 0,
        })
        .map_err(|_| Error::Protocol)
    }
    pub use idle_request as build_request;
}

struct Gate {
    live: Arc<AtomicBool>,
    ra: ra::ConsumerLifetime,
}
impl ipc::RegistrationConsumerGate for Gate {
    fn is_accepting(&self) -> bool {
        self.live.load(Ordering::SeqCst) && self.ra.is_accepting()
    }
}
#[derive(Debug)]
pub enum LocalDisposition {
    /// Transport write only; the semantic RA completion is a separate command.
    Written,
    WriteFailed,
    ExactUsageReleased,
    /// Exact original app host physically retired; logical H2 cleanup may remain.
    HostTerminated,
    RegistrationReferenceReleased,
    /// Constructed local health work under explicitly disabled HM processing.
    HealthProcessingDisabled,
    /// Trusted local semantic response; not an FM network send or ACK.
    LocalResponse,
    /// FM send/ACK is unsupported: original work remains in `external_pending`.
    ExternalPending,
}
#[derive(Debug)]
pub struct EffectReceipt {
    pub dispatch: ra::Dispatched,
    pub disposition: LocalDisposition,
}
#[derive(Debug)]
pub struct CommandReceipt {
    pub id: u64,
    pub result: ra::Result<ra::Disposition>,
}
#[derive(Debug, Clone)]
pub struct PackageRegistration {
    pub registered: bool,
    pub unregistered_at: Option<Instant>,
    pub health_sources_released: u64,
}
#[derive(Clone)]
enum Work {
    BuildIdle(PartitionId),
    Built(ra::BuildCompletion),
    RemoveIdle(PartitionId),
    Removed(ra::RemovalCompletion),
    PeerIdle(PartitionId, Replica),
    PeerCreate(Arc<CreateReplica>, NodeInstance, GenerationHeader),
    PeerReply(Arc<CreateReplicaReply>, NodeInstance, GenerationHeader),
    Status(ServiceType, u64, bool),
    Load(Box<ReportLoad>),
    Description(Box<ra::ServiceDescriptionCompletion>),
    Add(Arc<AddInstance>),
    StatefulOpen(Arc<AddInstance>),
    PrimaryOpen(Arc<AddInstance>),
    AddPrimary(Arc<AddPrimary>, NodeInstance, GenerationHeader),
    AddReplica(Arc<AddReplica>, NodeInstance, GenerationHeader),
    RemoveReplica(Arc<RemoveReplica>, NodeInstance, GenerationHeader),
    Fm(FmWork),
    Close(PartitionId, ra::CloseMode),
    Registered(ra::RegistrationCompletion),
    Open(ra::OpenCompletion),
    Closed(ra::CloseCompletion),
    Event(ipc::Delivery, ra::EntityId),
    Retry(PartitionId, bool),
    RetryDescription(PartitionId),
}
/// Narrow network adapter commands. The adapter owns the admitted session;
/// source/generation are still validated here and by the RA. Captured work is
/// never semantic acknowledgement.
#[derive(Clone)]
pub enum FmWork {
    CaptureTypeStatus,
    CleanupTypeStatus,
    CheckNodeDeactivation(i64),
    TypeStatusReply(bool, ServiceTypeUpdateReply, NodeInstance, GenerationHeader),
    NodeActivate(NodeActivateRequest, NodeInstance, GenerationHeader),
    NodeDeactivate(NodeDeactivateRequest, NodeInstance, GenerationHeader),
    NodeUpdateService(
        Box<NodeUpdateServiceRequest>,
        NodeInstance,
        GenerationHeader,
    ),
    CloseNodeUnit(PartitionId, i64),
    UpdateNodeUnit(PartitionId, u64, Box<NodeUpdateServiceRequest>),
    Remove(Box<RemoveInstance>, NodeInstance, GenerationHeader),
    DeleteReplica(Box<DeleteReplica>, NodeInstance, GenerationHeader),
    ReplicaUpReply(ReplicaUp, NodeInstance, GenerationHeader),
    ServiceTypeReply(ServiceTypeNotification, NodeInstance, GenerationHeader),
    CaptureServiceTypeWork(u64),
    CompleteServiceTypeWork(u64),
    ServiceTypeRetryTimer(u64),
    RetryReport(PartitionId),
}
struct Pending {
    id: u64,
    work: Work,
    ticket: Option<ra::Ticket>,
    pre_open: Option<u64>,
}
struct Usage {
    lookup: u64,
    owner: Option<h::Acquisition>,
    paired: bool,
}
struct Sending {
    dispatch: ra::Dispatched,
    send: ipc::LifecycleSend,
}
struct Route {
    runtime: String,
    lease: ipc::HostLease,
}

/// Positive original-runtime instance-disposal lineage. This is a trusted-local
/// observation, not a native reply or permission to adopt a process. IDs belong
/// to distinct namespaces; only the retained original owners authorize release.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceDisposal {
    #[serde(with = "disposal_body")]
    pub body: AddInstance,
    pub host: String,
    pub runtime: String,
    pub command: u64,
    pub ra_operation: u64,
    pub lookup: u64,
    pub acquisition: u64,
}
/// An applied Delete of an original peer replica that never issued Open/Close.
/// This certifies committed absence, not disposal of a runtime/service object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaAbsence {
    #[serde(with = "disposal_body")]
    pub body: AddInstance,
    pub command: u64,
    pub ra_operation: u64,
}
/// Original peer creation cancelled before registration adoption/RAP Open.
/// Outstanding lookup offers must drain before this can be published.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreOpenCancellation {
    #[serde(with = "disposal_body")]
    pub body: AddInstance,
    pub creation: u64,
    pub command: u64,
    pub ra_operation: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreOpenRegistrationFailure {
    #[serde(with = "disposal_body")]
    pub body: AddInstance,
    pub creation: u64,
    pub lookup: u64,
    pub command: u64,
    pub ra_operation: u64,
    pub error: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailedRegistrationDeletion {
    pub failure: PreOpenRegistrationFailure,
    pub command: u64,
    pub ra_operation: u64,
}
fn pre_open_cancelled(state: &ra::EntityState, creation: u64) -> bool {
    state.phase == ra::Phase::Closed
        && state.local_deleted
        && state.cleanup_pending
        && state.open_token == creation
        && creation != 0
        && state.close_token != 0
        && state.runtime.is_none()
        && state.usage.is_none()
        && state.peer_creation.is_some()
        && state.remote_idle.is_none()
        && state.fm_stage == ra::FmStage::None
}
fn never_created(state: &ra::EntityState) -> bool {
    state.phase == ra::Phase::Closed
        && state.local_deleted
        && state.cleanup_pending
        && state.open_token == 0
        && state.close_token == 0
        && state.runtime.is_none()
        && state.usage.is_none()
        && state.peer_creation.is_none()
        && state.remote_idle.is_none()
        && state.registration_lookups.is_empty()
        && state.fm_stage == ra::FmStage::None
}
mod disposal_body {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        body: &AddInstance,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        to_vec(body)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<AddInstance, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        decode(&bytes).map_err(|error| serde::de::Error::custom(format!("{error:?}")))
    }
}
struct DisposalWitness {
    proof: InstanceDisposal,
    applied: bool,
    released: bool,
}

/// One singleton service/package owner. Finite adapter escrow: 32 commands,
/// effects/receipts and acquisitions each, 64KiB wire bodies, 32 external pending
/// obligations. Canonical decode bounds allocation before command acceptance.
pub struct Node {
    activation: Activation,
    ra: Arc<ra::Executor>,
    admission: ra::Admission,
    live: Arc<AtomicBool>,
    pending: VecDeque<Pending>,
    commands: VecDeque<CommandReceipt>,
    effects: VecDeque<ra::Dispatched>,
    receipts: VecDeque<EffectReceipt>,
    external: Vec<ra::Dispatched>,
    usage: Vec<Usage>,
    sending: Option<Sending>,
    selected: Option<AddInstance>,
    next: u64,
    retry_at: Instant,
    statuses: ra::LocalStatusConsumer,
    package_registration: PackageRegistration,
    retained_delivery: Option<ipc::Delivery>,
    last_error: Option<Error>,
    route: Option<Route>,
    held_reply: Option<ipc::LifecycleReply>,
    rejected_reply: Option<ipc::LifecycleReply>,
    disposal: Option<DisposalWitness>,
    absence: Option<ReplicaAbsence>,
    pre_open: Option<PreOpenCancellation>,
    registration_failure: Option<PreOpenRegistrationFailure>,
    failed_registration: Option<FailedRegistrationDeletion>,
    #[cfg(feature = "qualification")]
    hold_replies: bool,
}
impl Node {
    /// Takes an unstarted Activation and creates its dedicated RA executor.
    /// The original lifetime gate is installed before process/session admission.
    pub fn new(
        mut activation: Activation,
        store: Arc<dyn KeyValueStore>,
        admission: ra::Admission,
        mut settings: ra::Settings,
    ) -> Result<Self> {
        if activation.profile() == h::local::Profile::VolatileStatefulRegistration
            || activation.identity().is_some()
            || !admission.owner_ready
            || !admission.node_open
            || !admission.node_activated
            || admission.fabric_upgrading
        {
            return Err(Error::Unsupported);
        }
        settings.max_entities = 1;
        settings.max_command_bytes = settings.max_command_bytes.min(BODY);
        settings.max_effects = settings.max_effects.min(SLOTS);
        settings.max_registration_lookups = settings.max_registration_lookups.min(SLOTS);
        let ra = Arc::new(ra::Executor::new(store, admission.clone(), settings)?);
        let live = Arc::new(AtomicBool::new(true));
        activation.bind_consumer(Arc::new(Gate {
            live: live.clone(),
            ra: ra.consumer_lifetime(),
        }))?;
        Ok(Self {
            activation,
            ra,
            admission,
            live,
            pending: VecDeque::new(),
            commands: VecDeque::new(),
            effects: VecDeque::new(),
            receipts: VecDeque::new(),
            external: vec![],
            usage: vec![],
            sending: None,
            selected: None,
            next: 0,
            retry_at: Instant::now() + RETRY,
            statuses: ra::LocalStatusConsumer::new(1, BODY)?,
            package_registration: PackageRegistration {
                registered: false,
                unregistered_at: None,
                health_sources_released: 0,
            },
            retained_delivery: None,
            last_error: None,
            route: None,
            held_reply: None,
            rejected_reply: None,
            disposal: None,
            absence: None,
            pre_open: None,
            registration_failure: None,
            failed_registration: None,
            #[cfg(feature = "qualification")]
            hold_replies: false,
        })
    }
    pub fn start(
        &mut self,
        package: &Path,
        application: &str,
        parameters: &[(String, String)],
    ) -> Result<()> {
        if !self.live.load(Ordering::SeqCst) || !self.ra.consumer_lifetime().is_accepting() {
            return Err(Error::OwnerLost);
        }
        self.activation.start(package, application, parameters)?;
        Ok(())
    }
    pub fn activation(&self) -> &Activation {
        &self.activation
    }
    pub fn snapshot(&self, unit: ra::EntityId) -> ra::Snapshot {
        self.ra.snapshot(unit)
    }
    pub fn retained_usage(&self) -> usize {
        self.usage.len()
    }
    pub fn external_pending(&self) -> &[ra::Dispatched] {
        &self.external
    }
    /// Transfer original external obligations into a bounded FM adapter escrow.
    /// This does not acknowledge them. The caller must retain each transferred
    /// item until its scheduling/send responsibility completes; semantic
    /// notification/report pending state remains in RA until a matched reply.
    pub fn take_external_up_to(&mut self, maximum: usize) -> Vec<ra::Dispatched> {
        self.external
            .drain(..maximum.min(self.external.len()))
            .collect()
    }
    fn admit_fm(&self, source: &NodeInstance, generation: &GenerationHeader) -> Result<()> {
        if source != &self.admission.fm_source
            || generation.for_fm_replica
            || generation.number != self.admission.generation
        {
            return Err(Error::Protocol);
        }
        Ok(())
    }
    /// A fixed-profile FM session cannot silently widen local Add authority.
    pub fn add_from_fm(
        &mut self,
        body: AddInstance,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::Stateless {
            return Err(Error::Unsupported);
        }
        self.admit_fm(source, generation)?;
        self.add(body)
    }
    pub fn fm_work(&mut self, work: FmWork) -> Result<u64> {
        let stateless = self.activation.profile() == h::local::Profile::Stateless;
        let peer_work = self.activation.profile() == h::local::Profile::VolatileStatefulLifecycle
            && self.ra.peer().is_some()
            && matches!(
                work,
                FmWork::CaptureTypeStatus
                    | FmWork::CleanupTypeStatus
                    | FmWork::TypeStatusReply(..)
                    | FmWork::ServiceTypeReply(..)
                    | FmWork::CaptureServiceTypeWork(_)
                    | FmWork::CompleteServiceTypeWork(_)
                    | FmWork::ServiceTypeRetryTimer(_)
                    | FmWork::DeleteReplica(..)
            );
        if !stateless
            && self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary
            && !peer_work
        {
            return Err(Error::Unsupported);
        }
        if !stateless
            && !matches!(
                work,
                FmWork::NodeActivate(..)
                    | FmWork::CaptureTypeStatus
                    | FmWork::CleanupTypeStatus
                    | FmWork::CheckNodeDeactivation(..)
                    | FmWork::TypeStatusReply(..)
                    | FmWork::NodeDeactivate(..)
                    | FmWork::NodeUpdateService(..)
                    | FmWork::CloseNodeUnit(..)
                    | FmWork::UpdateNodeUnit(..)
                    | FmWork::DeleteReplica(..)
                    | FmWork::ReplicaUpReply(..)
                    | FmWork::RetryReport(_)
                    | FmWork::ServiceTypeReply(..)
                    | FmWork::CaptureServiceTypeWork(_)
                    | FmWork::CompleteServiceTypeWork(_)
                    | FmWork::ServiceTypeRetryTimer(_)
            )
        {
            return Err(Error::Unsupported);
        }
        if stateless && matches!(work, FmWork::DeleteReplica(..)) {
            return Err(Error::Unsupported);
        }
        match &work {
            FmWork::NodeActivate(_, source, generation)
            | FmWork::TypeStatusReply(_, _, source, generation)
            | FmWork::NodeDeactivate(_, source, generation) => self.admit_fm(source, generation)?,
            FmWork::Remove(body, source, generation) => {
                self.admit_fm(source, generation)?;
                if self
                    .selected
                    .as_ref()
                    .is_none_or(|b| b.unit.id != body.replica.0.unit.id)
                {
                    return Err(Error::Unsupported);
                }
            }
            FmWork::NodeUpdateService(body, source, generation) => {
                self.admit_fm(source, generation)?;
                if let Some(selected) = &self.selected {
                    let service = &body.service;
                    if service.name == selected.service.name
                        && service.instance == selected.service.instance
                        && (service.stateful != selected.service.stateful
                            || service.persisted
                            || service.group
                            || service.service_type != selected.service.service_type
                            || service.application != selected.service.application
                            || service.version != selected.service.version
                            || service.target != 1
                            || service.minimum != 1
                            || service.partitions != 1
                            || service.activation_mode != 0
                            || !service.scaling.0.is_empty())
                    {
                        return Err(Error::Unsupported);
                    }
                }
            }
            FmWork::DeleteReplica(body, source, generation) => {
                self.admit_fm(source, generation)?;
                let incoming = &body.replica.0;
                self.check_service(incoming, true)?;
                let current = self.ra.snapshot(incoming.unit.id.guid.0).committed;
                let closed = current
                    .as_ref()
                    .is_some_and(|state| state.phase == ra::Phase::Closed);
                if peer_work {
                    let eligible = match &current {
                        None => self.selected.is_none() || self.pending.iter().any(|pending| {
                            matches!(&pending.work, Work::Fm(FmWork::DeleteReplica(original, ..))
                                if original == body)
                        }),
                        Some(state) => {
                            state.body.replica.id == incoming.replica.id
                                && ((never_created(state)
                                    && state.body.replica.instance == incoming.replica.instance)
                                    || (state.peer_creation.is_some()
                                        && state.body.replica.current_role == 2
                                        && (state.phase == ra::Phase::Ready
                                            || (state.phase == ra::Phase::Opening
                                                && state.runtime.is_some()
                                                    == state.usage.is_some())
                                            || (state.failed_registration_error().is_some()
                                                && self.registration_failure.is_some()
                                                && self.route.is_none())
                                            || state.local_deleted)))
                        }
                    };
                    if body.deletion.0.force
                        || !eligible
                        || (current.is_none()
                            && (incoming.replica.current_role != 2
                                || incoming.replica.previous_role != 1
                                || incoming.replica.state != 1
                                || !incoming.replica.up
                                || incoming.replica.version != incoming.service.version
                                || !incoming.replica.service_endpoint.is_empty()
                                || !incoming.replica.replication_endpoint.is_empty()))
                    {
                        return Err(Error::Unsupported);
                    }
                }
                if (!self.live.load(Ordering::SeqCst) && !closed)
                    || !self.ra.consumer_lifetime().is_accepting()
                    || self.selected.as_ref().is_some_and(|selected| {
                        incoming.unit.id != selected.unit.id
                            || incoming.unit.consistency != selected.unit.consistency
                            || incoming.service != selected.service
                    })
                    || incoming.replica.id < 0
                    || incoming.replica.instance < 0
                    || to_vec(body).map_err(|_| Error::Protocol)?.len() > ADD_BODY
                {
                    return Err(Error::Unsupported);
                }
            }
            FmWork::ReplicaUpReply(_, source, generation)
            | FmWork::ServiceTypeReply(_, source, generation) => {
                self.admit_fm(source, generation)?;
            }
            _ => {}
        }
        if !stateless {
            match &work {
                FmWork::RetryReport(unit) => {
                    if self
                        .selected
                        .as_ref()
                        .is_none_or(|body| &body.unit.id != unit)
                    {
                        return Err(Error::Unsupported);
                    }
                }
                FmWork::ReplicaUpReply(body, ..) => {
                    let info = body.replicas.0.first().or_else(|| body.dropped.0.first());
                    if body.last
                        || body.from_fmm
                        || body.replicas.0.len() + body.dropped.0.len() != 1
                        || self.selected.as_ref().is_none_or(|selected| {
                            info.is_none_or(|info| info.unit.id != selected.unit.id)
                        })
                    {
                        return Err(Error::Unsupported);
                    }
                }
                _ => {}
            }
        }
        let selected = match &work {
            FmWork::DeleteReplica(body, ..) if self.selected.is_none() => {
                Some(body.replica.0.clone())
            }
            _ => None,
        };
        let id = self.enqueue(Work::Fm(work))?;
        if let Some(selected) = selected {
            self.selected = Some(selected);
        }
        Ok(id)
    }
    pub fn package_registration(&self) -> &PackageRegistration {
        &self.package_registration
    }
    pub fn has_service_type_status(&self) -> bool {
        self.ra.has_service_type_status()
    }
    pub fn native_config(&self) -> Arc<ra::NativeConfig> {
        self.ra.config()
    }
    pub fn pending_node_deactivation(&self) -> Option<i64> {
        self.ra.pending_node_deactivation()
    }
    pub fn generation_state(&self) -> ra::GenerationState {
        self.ra.generation_state()
    }
    pub fn bind_peer(&self, source: NodeInstance) -> Result<()> {
        Ok(self.ra.bind_peer(source)?)
    }
    pub fn create_idle_on_peer(&mut self, replica: Replica) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary
            || !self.live.load(Ordering::SeqCst)
            || self.ra.generation_state().upload_pending
            || self.ra.peer().as_ref() != Some(&replica.node)
        {
            return Err(Error::Unsupported);
        }
        let unit = self
            .selected
            .as_ref()
            .ok_or(Error::Unsupported)?
            .unit
            .id
            .clone();
        if !self.ra.node_activation().activated
            || self
                .ra
                .snapshot(unit.guid.0)
                .committed
                .is_none_or(|state| state.phase != ra::Phase::Ready || !state.fm_primary)
        {
            return Err(Error::Busy);
        }
        self.enqueue(Work::PeerIdle(unit, replica))
    }
    /// Copy into the already opened original peer; no FM or quorum completion.
    pub fn build_idle_on_peer(&mut self) -> Result<u64> {
        self.control_idle_on_peer(false)
    }
    /// Remove only the original Primary's Idle session, not the remote service.
    pub fn remove_idle_on_peer(&mut self) -> Result<u64> {
        self.control_idle_on_peer(true)
    }
    fn control_idle_on_peer(&mut self, remove: bool) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary
            || !self.live.load(Ordering::SeqCst)
            || self.ra.generation_state().upload_pending
            || self.ra.peer().is_none()
        {
            return Err(Error::Unsupported);
        }
        let unit = self
            .selected
            .as_ref()
            .ok_or(Error::Unsupported)?
            .unit
            .id
            .clone();
        self.enqueue(if remove {
            Work::RemoveIdle(unit)
        } else {
            Work::BuildIdle(unit)
        })
    }
    pub fn create_replica_from_peer(
        &mut self,
        body: CreateReplica,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulLifecycle
            || self.ra.peer().as_ref() != Some(source)
            || generation.for_fm_replica
            || !self.live.load(Ordering::SeqCst)
            || !self.ra.consumer_lifetime().is_accepting()
        {
            return Err(Error::Unsupported);
        }
        let base = &body.replica.0;
        self.check_service(base, true)?;
        if to_vec(&body).map_err(|_| Error::Protocol)?.len() > ADD_BODY
            || self.selected.as_ref().is_some_and(|selected| {
                selected.unit.id != base.unit.id || selected.service != base.service
            })
        {
            return Err(Error::Unsupported);
        }
        let selected = base.clone();
        let id = self.enqueue(Work::PeerCreate(
            Arc::new(body),
            source.clone(),
            generation.clone(),
        ))?;
        if self.selected.is_none()
            && !generation
                .number
                .older_than(&self.ra.generation_state().receive)
        {
            self.selected = Some(selected);
        }
        Ok(id)
    }
    pub fn create_replica_reply_from_peer(
        &mut self,
        body: CreateReplicaReply,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary
            || self.ra.peer().as_ref() != Some(source)
            || generation.for_fm_replica
        {
            return Err(Error::Unsupported);
        }
        self.enqueue(Work::PeerReply(
            Arc::new(body),
            source.clone(),
            generation.clone(),
        ))
    }
    pub fn process_generation_proposal(
        &self,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<GenerationProposalReply> {
        Ok(self.ra.process_generation_proposal(source, generation)?)
    }
    pub fn process_generation_update(
        &self,
        source: &NodeInstance,
        body: &GenerationUpdate,
        generation: &GenerationHeader,
    ) -> Result<ra::GenerationUpdateOutcome> {
        Ok(self
            .ra
            .process_generation_update(source, body, generation)?)
    }
    pub fn apply_fresh_node_up_ack(
        &self,
        body: &NodeUpAck,
        generation: &GenerationHeader,
    ) -> Result<()> {
        self.admit_fm(&self.admission.fm_source, generation)?;
        self.ra.seed_fresh_node_activation(ra::NodeActivationInfo {
            activated: body.activated,
            sequence: body.activation_sequence,
        })?;
        Ok(())
    }
    pub fn pending_status(&self) -> &[ra::PendingStatus] {
        // Diagnostic local Hosting projection; the RA owns FM pending/ACK state.
        self.statuses.pending()
    }
    pub fn last_error(&self) -> Option<&Error> {
        self.last_error.as_ref()
    }
    pub fn take_command_receipts(&mut self) -> Vec<CommandReceipt> {
        self.commands.drain(..).collect()
    }
    pub fn take_effect_receipts(&mut self) -> Vec<EffectReceipt> {
        self.receipts.drain(..).collect()
    }
    pub fn pending_commands(&self) -> usize {
        self.pending.len()
    }
    pub fn statistics(&self) -> ra::Statistics {
        self.ra.statistics()
    }
    /// Local execution is drained; externally pending FM obligations are still
    /// explicitly retained, not acknowledged by this observation.
    pub fn quiescent(&self) -> bool {
        let stats = self.ra.statistics();
        self.usage.is_empty()
            && self.pending.is_empty()
            && self.sending.is_none()
            && self.effects.is_empty()
            && self.held_reply.is_none()
            && self.retained_delivery.is_none()
            && stats.outstanding == 0
            && stats.effects == 0
            && self.activation.deliveries_drained()
    }

    /// The original runtime awaited owned disposal before its successful Close
    /// reply. Publish only after that exact command committed and the original
    /// paired acquisition was released. Replica FM escrow is separately owned
    /// by the caller and must also retire before admitting a successor.
    pub fn instance_disposal(&self) -> Option<InstanceDisposal> {
        let witness = self.disposal.as_ref()?;
        if !witness.applied
            || !witness.released
            || !self.live.load(Ordering::SeqCst)
            || !self.ra.consumer_lifetime().is_accepting()
            || !self.quiescent()
            || !self.commands.is_empty()
            || !self.receipts.is_empty()
            || self.rejected_reply.is_some()
            || self.last_error.is_some()
            || !self.activation.session_status().ok().flatten()?.connected
            || self.activation.process_status()?.exit_observed
        {
            return None;
        }
        let snapshot = self.ra.snapshot(witness.proof.body.unit.id.guid.0);
        let state = snapshot.committed?;
        let old = &witness.proof.body;
        if snapshot.fenced
            || state.phase != ra::Phase::Closed
            || state.body.unit != old.unit
            || state.body.service != old.service
            || state.body.replica.node != old.replica.node
            || state.body.replica.id != old.replica.id
            || state.body.replica.instance != old.replica.instance
            || state.runtime.is_some()
            || state.usage.is_some()
            || !state.registration_lookups.is_empty()
            || !self.usage.is_empty()
            || self.route.as_ref()?.runtime != witness.proof.runtime
            || self.activation.identity()?.host_id != witness.proof.host
        {
            return None;
        }
        Some(witness.proof.clone())
    }

    /// Publish applied absent-Delete lineage only while the never-opened
    /// tombstone remains committed and all original local work is drained.
    pub fn replica_absence(&self) -> Option<ReplicaAbsence> {
        let proof = self.absence.as_ref()?;
        if self.activation.profile() != h::local::Profile::VolatileStatefulLifecycle
            || self.ra.peer().is_none()
            || !self.live.load(Ordering::SeqCst)
            || !self.ra.consumer_lifetime().is_accepting()
            || !self.quiescent()
            || !self.commands.is_empty()
            || !self.receipts.is_empty()
            || self.rejected_reply.is_some()
            || self.last_error.is_some()
            || self.route.is_some()
            || self.disposal.is_some()
            || !self.activation.session_status().ok().flatten()?.connected
            || self.activation.process_status()?.exit_observed
        {
            return None;
        }
        let snapshot = self.ra.snapshot(proof.body.unit.id.guid.0);
        let state = snapshot.committed?;
        (!snapshot.fenced && never_created(&state) && *state.body == proof.body)
            .then(|| proof.clone())
    }

    /// Certify the applied pre-registration Delete only after every original
    /// lookup offer and exact acquisition has drained without a RAP route.
    pub fn pre_open_cancellation(&self) -> Option<PreOpenCancellation> {
        let proof = self.pre_open.as_ref()?;
        if self.activation.profile() != h::local::Profile::VolatileStatefulLifecycle
            || self.ra.peer().is_none()
            || !self.live.load(Ordering::SeqCst)
            || !self.ra.consumer_lifetime().is_accepting()
            || !self.quiescent()
            || !self.commands.is_empty()
            || !self.receipts.is_empty()
            || self.rejected_reply.is_some()
            || self.last_error.is_some()
            || self.route.is_some()
            || self.disposal.is_some()
            || self.absence.is_some()
            || !self.activation.session_status().ok().flatten()?.connected
            || self.activation.process_status()?.exit_observed
        {
            return None;
        }
        let snapshot = self.ra.snapshot(proof.body.unit.id.guid.0);
        let state = snapshot.committed?;
        (!snapshot.fenced
            && pre_open_cancelled(&state, proof.creation)
            && state.close_token == proof.ra_operation
            && state.registration_lookups.is_empty()
            && *state.body == proof.body)
            .then(|| proof.clone())
    }

    /// Applied native registration failure, not a cleanup or Delete receipt.
    pub fn pre_open_registration_failure(&self) -> Option<PreOpenRegistrationFailure> {
        let failure = self.registration_failure.as_ref()?;
        let snapshot = self.ra.snapshot(failure.body.unit.id.guid.0);
        let state = snapshot.committed?;
        (!snapshot.fenced
            && self.live.load(Ordering::SeqCst)
            && self.ra.consumer_lifetime().is_accepting()
            && state.failed_registration_error() == Some(failure.error)
            && state.open_token == failure.creation
            && *state.body == failure.body
            && self.route.is_none()
            && self.last_error.is_none()
            && self.activation.session_status().ok().flatten()?.connected
            && !self.activation.process_status()?.exit_observed)
            .then(|| failure.clone())
    }

    pub fn failed_registration_deletion(&self) -> Option<FailedRegistrationDeletion> {
        let proof = self.failed_registration.as_ref()?;
        let failure = self.pre_open_registration_failure()?;
        let snapshot = self.ra.snapshot(failure.body.unit.id.guid.0);
        let state = snapshot.committed?;
        (!snapshot.fenced
            && state.local_deleted
            && state.cleanup_pending
            && state.registration_lookups.is_empty()
            && self.quiescent()
            && self.commands.is_empty()
            && self.receipts.is_empty()
            && self.rejected_reply.is_none()
            && self.disposal.is_none()
            && self.absence.is_none()
            && self.pre_open.is_none())
        .then(|| proof.clone())
    }

    fn room(&self) -> bool {
        self.pending.len() + self.commands.len() < SLOTS
    }
    fn enqueue(&mut self, work: Work) -> Result<u64> {
        if !self.room() {
            return Err(Error::Capacity);
        }
        let id = self.next.checked_add(1).ok_or(Error::Capacity)?;
        self.pending.push_back(Pending {
            id,
            work,
            ticket: None,
            pre_open: None,
        });
        self.next = id;
        Ok(id)
    }
    /// Authorize a supported local Add, never registration-as-readiness.
    /// Description updates and foreign package/profile inputs reject before RA.
    pub fn add(&mut self, body: AddInstance) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::Stateless {
            return Err(Error::Unsupported);
        }
        self.authorize_open(body, 1, None)
    }
    /// One volatile Idle lifetime. No primary, replacement, build or FM/peer authority.
    pub fn open_stateful(&mut self, body: AddInstance) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulLifecycle {
            return Err(Error::Unsupported);
        }
        self.authorize_open(body, 2, None)
    }
    /// One fresh volatile 1/1 Primary lifetime, never promotion/adoption or FM authority.
    pub fn open_stateful_primary(&mut self, body: AddInstance) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulPrimaryLifecycle {
            return Err(Error::Unsupported);
        }
        self.authorize_open(body, 4, None)
    }
    pub fn add_primary_from_fm(
        &mut self,
        body: AddPrimary,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<u64> {
        if self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary {
            return Err(Error::Unsupported);
        }
        self.admit_fm(source, generation)?;
        self.authorize_open(body, 4, Some((source.clone(), generation.clone())))
    }
    pub fn add_replica_from_fm(
        &mut self,
        body: AddReplica,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<u64> {
        self.admit_remote_control(&body, source, generation)?;
        self.enqueue(Work::AddReplica(
            Arc::new(body),
            source.clone(),
            generation.clone(),
        ))
    }
    pub fn remove_replica_from_fm(
        &mut self,
        body: RemoveReplica,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<u64> {
        self.admit_remote_control(&body, source, generation)?;
        self.enqueue(Work::RemoveReplica(
            Arc::new(body),
            source.clone(),
            generation.clone(),
        ))
    }
    fn admit_remote_control(
        &self,
        body: &AddInstance,
        source: &NodeInstance,
        generation: &GenerationHeader,
    ) -> Result<()> {
        self.admit_fm(source, generation)?;
        if self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary
            || !self.live.load(Ordering::SeqCst)
            || self.ra.generation_state().upload_pending
            || self.ra.peer().as_ref() != Some(&body.replica.node)
            || self.selected.as_ref().is_none_or(|selected| {
                body.unit != selected.unit || body.service != selected.service
            })
            || to_vec(&body).map_err(|_| Error::Protocol)?.len() > ADD_BODY
        {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
    fn check_service(&self, body: &AddInstance, stateful: bool) -> Result<()> {
        let premise = self.activation.premise().ok_or(Error::Unsupported)?;
        let expected = self.activation.declared_type().ok_or(Error::Unsupported)?;
        if service_type(&expected) != body.service.service_type
            || body.service.version != version(&premise.version)
            || body.service.application != premise.application_name
            || body.service.name != premise.service_name
            || body.replica.node != self.admission.local_node
            || body.service.stateful != stateful
            || body.service.persisted
            || body.service.group
            || body.service.activation_mode != 0
            || !body.service.scaling.0.is_empty()
            || body.service.partitions != 1
            || body.service.target != 1
            || body.service.minimum != 1
            || body.unit.consistency.kind != 1
            || !body.unit.consistency.name.is_empty()
            || body.unit.consistency.low != 0
            || body.unit.consistency.high != 0
            || body.unit.consistency.id != body.unit.id
        {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
    fn authorize_open(
        &mut self,
        body: AddInstance,
        role: i64,
        fm: Option<(NodeInstance, GenerationHeader)>,
    ) -> Result<u64> {
        let stateful = role != 1;
        let current = self.ra.snapshot(body.unit.id.guid.0).committed;
        let closed_request = fm.is_some()
            && current.as_ref().is_some_and(|state| {
                state.phase == ra::Phase::Closed
                    && body.replica.instance <= state.body.replica.instance
            });
        if (!self.live.load(Ordering::SeqCst) && !closed_request)
            || !self.ra.consumer_lifetime().is_accepting()
        {
            return Err(Error::Busy);
        }
        let bytes = to_vec(&body).map_err(|_| Error::Unsupported)?;
        if bytes.len() > ADD_BODY {
            return Err(Error::Capacity);
        }
        let body: AddInstance = decode(&bytes)?;
        self.check_service(&body, stateful)?;
        let premise = self.activation.premise().ok_or(Error::Unsupported)?;
        let identity = self.activation.identity().ok_or(Error::Unsupported)?;
        let expected = self.activation.declared_type().ok_or(Error::Unsupported)?;
        let lookup = self.activation.lookup_registration(&h::LookupRequest {
            service_type: expected.clone(),
            version: premise.version.clone(),
            activation: h::Activation::SHARED,
            service_name: premise.service_name.clone(),
            application_name: premise.application_name.clone(),
            should_activate: false,
        })?;
        // Type admission can precede registration; the declared package/type is
        // still exact. An arbitrary unregistered type is not an allowed profile.
        if (lookup.result == Err(h::Error::InvalidServiceType) && !closed_request)
            || body.replica.version != version(&premise.version)
            || body.replica.current_role != role
            || body.replica.state != 1
            || !body.replica.up
            || body.replica.id < 0
            || body.replica.instance < 0
            || identity.code.package_sequence <= 0
        {
            return Err(Error::Unsupported);
        }
        if role == 4 && fm.is_none() && body.unit.previous != Epoch::default() {
            return Err(Error::Unsupported);
        }
        if stateful
            && (body.replica.previous_role != 1
                || body.unit.current.configuration <= 0
                || body.unit.current.data_loss < 0
                || !body.replica.service_endpoint.is_empty()
                || !body.replica.replication_endpoint.is_empty())
        {
            return Err(Error::Unsupported);
        }
        if let Some(selected) = &self.selected {
            if body.unit.id != selected.unit.id || body.service != selected.service {
                return Err(Error::Unsupported);
            }
        }
        if stateful
            && fm.is_none()
            && (self
                .selected
                .as_ref()
                .is_some_and(|selected| selected != &body)
                || current
                    .as_ref()
                    .is_some_and(|state| state.phase == ra::Phase::Closed))
        {
            return Err(Error::Unsupported);
        }
        if fm.is_some() {
            if let Some(current) = &current {
                let admitted_retry = self.pending.iter().any(|pending| {
                    matches!(&pending.work, Work::AddPrimary(approved, ..) if approved.as_ref() == &body)
                        && pending.ticket.as_ref().is_none_or(|ticket| {
                            matches!(
                                ticket.observation(),
                                ra::TicketObservation::Status(
                                    ra::Status::Pending
                                        | ra::Status::Unknown
                                        | ra::Status::Complete(Ok(_))
                                )
                            )
                        })
                });
                if current.phase != ra::Phase::Closed && body.replica.id > current.body.replica.id {
                    return Err(Error::Unsupported);
                }
                if current.phase == ra::Phase::Closed
                    && body.replica.instance > current.body.replica.instance
                    && (!current.local_deleted
                        || current.fm_stage != ra::FmStage::None
                        || body.replica.id <= current.body.replica.id
                        || (!admitted_retry
                            && ((self.instance_disposal().is_none() && current.open_token != 0)
                                || !self.quiescent()
                                || !self.commands.is_empty()
                                || self
                                    .external
                                    .iter()
                                    .any(|work| work.effect.is_replica_reporting()))))
                {
                    return Err(Error::Busy);
                }
            }
        }
        // `selected` is the immutable admitted profile, NOT current replica
        // authority. An ignored stale Add cannot replace committed identity.
        let reference = current
            .as_ref()
            .filter(|s| s.phase != ra::Phase::Closed)
            .map(|s| &s.body.replica)
            .or_else(|| {
                if current.is_none() {
                    self.selected.as_ref().map(|b| &b.replica)
                } else {
                    None
                }
            });
        if reference.is_some_and(|r| body.replica.id == r.id && body.replica.instance > r.instance)
        {
            return Err(Error::Unsupported);
        }
        let work = match (role, fm) {
            (4, Some((source, generation))) => {
                Work::AddPrimary(Arc::new(body.clone()), source, generation)
            }
            (4, None) => Work::PrimaryOpen(Arc::new(body.clone())),
            (2, _) => Work::StatefulOpen(Arc::new(body.clone())),
            _ => Work::Add(Arc::new(body.clone())),
        };
        let id = self.enqueue(work)?;
        if self.disposal.as_ref().is_some_and(|witness| {
            body.replica.id > witness.proof.body.replica.id
                && body.replica.instance > witness.proof.body.replica.instance
        }) {
            self.disposal = None;
        }
        if self.selected.is_none() {
            self.selected = Some(body);
        }
        Ok(id)
    }
    pub fn close(&mut self, unit: PartitionId, mode: ra::CloseMode) -> Result<u64> {
        // Withdrawal forbids new serving work, not the existing owner's native
        // Drop after matching host retirement. Never decrement on process exit.
        let cleanup = mode == ra::CloseMode::Drop
            && self.activation.cleanup_status().logical_retired
            && self.ra.snapshot(unit.guid.0).committed.is_some_and(|s| {
                s.phase == ra::Phase::Opening
                    && s.runtime.is_none()
                    && s.usage
                        .is_some_and(|id| self.usage.iter().any(|u| u.lookup == id))
            });
        if (!self.live.load(Ordering::SeqCst) && !cleanup)
            || !self.ra.consumer_lifetime().is_accepting()
            || !matches!(
                mode,
                ra::CloseMode::Close
                    | ra::CloseMode::Drop
                    | ra::CloseMode::Abort
                    | ra::CloseMode::ForceAbort
            )
            || (mode == ra::CloseMode::Abort
                && self.activation.profile() != h::local::Profile::VolatileStatefulFmPrimary)
            || self.selected.as_ref().is_none_or(|b| b.unit.id != unit)
        {
            return Err(Error::Unsupported);
        }
        self.enqueue(Work::Close(unit, mode))
    }
    /// Explicit local retry uses current committed RA policy/identity.
    pub fn retry(&mut self, unit: PartitionId) -> Result<u64> {
        if !self.live.load(Ordering::SeqCst) || !self.ra.consumer_lifetime().is_accepting() {
            return Err(Error::Busy);
        }
        let state = self
            .ra
            .snapshot(unit.guid.0)
            .committed
            .ok_or(Error::Unsupported)?;
        if !matches!(state.phase, ra::Phase::Opening | ra::Phase::Closing) {
            return Err(Error::Unsupported);
        }
        self.enqueue(Work::Retry(unit, state.phase == ra::Phase::Closing))
    }
    fn build(&self, work: &Work) -> ra::Command {
        match work {
            Work::BuildIdle(unit) => ra::Command::BuildRemoteIdle(unit.clone()),
            Work::Built(event) => ra::Command::CompleteBuild(Box::new(event.clone())),
            Work::AddReplica(body, source, generation) => ra::Command::AddReplica {
                body: Box::new((**body).clone()),
                source: source.clone(),
                generation: generation.clone(),
            },
            Work::RemoveReplica(body, source, generation) => ra::Command::RemoveReplica {
                body: Box::new((**body).clone()),
                source: source.clone(),
                generation: generation.clone(),
            },
            Work::RemoveIdle(unit) => ra::Command::RemoveRemoteIdle(unit.clone()),
            Work::Removed(event) => ra::Command::CompleteRemoval(Box::new(event.clone())),
            Work::PeerIdle(unit, replica) => ra::Command::CreateRemoteIdle {
                unit: unit.clone(),
                replica: replica.clone(),
            },
            Work::PeerCreate(body, source, generation) => ra::Command::CreateReplica {
                body: Box::new((**body).clone()),
                source: source.clone(),
                generation: generation.clone(),
            },
            Work::PeerReply(body, source, generation) => ra::Command::CreateReplicaReply {
                body: Box::new((**body).clone()),
                source: source.clone(),
                generation: generation.clone(),
            },
            Work::Status(id, sequence, enabled) => ra::Command::HostingServiceTypeStatus {
                service_type: id.clone(),
                sequence: *sequence,
                enabled: *enabled,
            },
            Work::Load(body) => ra::Command::ReportLoad(body.clone()),
            Work::Description(body) => ra::Command::CompleteServiceDescription(body.clone()),
            Work::StatefulOpen(body) => ra::Command::OpenLocalStateful(Box::new((**body).clone())),
            Work::PrimaryOpen(body) => ra::Command::OpenLocalPrimary(Box::new((**body).clone())),
            Work::AddPrimary(body, source, generation) => ra::Command::AddPrimary {
                body: Box::new((**body).clone()),
                source: source.clone(),
                generation: generation.clone(),
            },
            Work::Add(body) => ra::Command::Add {
                body: Box::new((**body).clone()),
                source: self.admission.fm_source.clone(),
                generation: GenerationHeader {
                    number: self.admission.generation.clone(),
                    for_fm_replica: false,
                },
            },
            Work::Fm(work) => match work {
                FmWork::CaptureTypeStatus => ra::Command::CaptureServiceTypeStatus,
                FmWork::CleanupTypeStatus => ra::Command::CleanupServiceTypeStatus,
                FmWork::CheckNodeDeactivation(sequence) => ra::Command::CheckNodeDeactivation {
                    sequence: *sequence,
                },
                FmWork::TypeStatusReply(enabled, body, source, generation) => {
                    ra::Command::ServiceTypeStatusReply {
                        enabled: *enabled,
                        body: body.clone(),
                        source: source.clone(),
                        generation: generation.clone(),
                    }
                }
                FmWork::NodeActivate(body, source, generation) => ra::Command::NodeActivate {
                    body: body.clone(),
                    source: source.clone(),
                    generation: generation.clone(),
                },
                FmWork::NodeDeactivate(body, source, generation) => ra::Command::NodeDeactivate {
                    body: body.clone(),
                    source: source.clone(),
                    generation: generation.clone(),
                },
                FmWork::NodeUpdateService(body, source, generation) => {
                    ra::Command::NodeUpdateService {
                        body: body.clone(),
                        source: source.clone(),
                        generation: generation.clone(),
                    }
                }
                FmWork::CloseNodeUnit(unit, sequence) => ra::Command::CloseForNodeDeactivation {
                    unit: unit.clone(),
                    sequence: *sequence,
                },
                FmWork::UpdateNodeUnit(unit, work, body) => ra::Command::UpdateServiceForNode {
                    unit: unit.clone(),
                    work: *work,
                    body: body.clone(),
                },
                FmWork::Remove(body, source, generation) => ra::Command::Remove {
                    body: body.clone(),
                    source: source.clone(),
                    generation: generation.clone(),
                },
                FmWork::DeleteReplica(body, source, generation) => ra::Command::DeleteReplica {
                    body: body.clone(),
                    source: source.clone(),
                    generation: generation.clone(),
                },
                FmWork::ReplicaUpReply(body, source, generation) => ra::Command::ReplicaUpReply {
                    body: Box::new(body.clone()),
                    source: source.clone(),
                    generation: generation.clone(),
                },
                FmWork::ServiceTypeReply(body, source, generation) => {
                    ra::Command::ServiceTypeReply {
                        body: Box::new(body.clone()),
                        source: source.clone(),
                        generation: generation.clone(),
                    }
                }
                FmWork::CaptureServiceTypeWork(work) => {
                    ra::Command::CaptureServiceTypeWork { work: *work }
                }
                FmWork::CompleteServiceTypeWork(work) => {
                    ra::Command::CompleteServiceTypeWork { work: *work }
                }
                FmWork::ServiceTypeRetryTimer(timer) => {
                    ra::Command::ServiceTypeRetryTimer { timer: *timer }
                }
                FmWork::RetryReport(unit) => ra::Command::RetryReport(unit.clone()),
            },
            Work::Close(unit, mode) => ra::Command::CloseLocal {
                unit: unit.clone(),
                mode: *mode,
            },
            Work::Registered(v) => ra::Command::Registered(Box::new(v.clone())),
            Work::Open(v) => ra::Command::CompleteOpen(Box::new(v.clone())),
            Work::Closed(v) => ra::Command::CompleteClose(Box::new(v.clone())),
            Work::Retry(unit, true) => ra::Command::RetryClose(unit.clone()),
            Work::Retry(unit, false) => ra::Command::RetryOpen(unit.clone()),
            Work::RetryDescription(unit) => ra::Command::RetryServiceDescription(unit.clone()),
            Work::Event(v, id) => command(
                v,
                *id,
                &ra::HostingReadiness {
                    local_lifecycle: true,
                    fm_ready: false,
                    node_active: true,
                    ra_open: true,
                    ..Default::default()
                },
            ),
        }
    }
    fn pending(&mut self) -> Result<()> {
        let Some(front) = self.pending.front() else {
            return Ok(());
        };
        if front.ticket.is_none() {
            let command = self.build(&front.work);
            let pre_open = match &front.work {
                Work::Fm(FmWork::DeleteReplica(body, ..))
                    if self.activation.profile()
                        == h::local::Profile::VolatileStatefulLifecycle
                        && self.ra.peer().is_some()
                        && self.route.is_none() =>
                {
                    self.ra
                        .snapshot(body.replica.0.unit.id.guid.0)
                        .committed
                        .filter(|state| {
                            state.phase == ra::Phase::Opening
                                && state.runtime.is_none()
                                && state.usage.is_none()
                                && state.peer_creation.is_some()
                                && state.close_token == 0
                        })
                        .map(|state| state.open_token)
                }
                _ => None,
            };
            match self.ra.submit(command) {
                Ok(ticket) => {
                    let pending = self.pending.front_mut().unwrap();
                    pending.ticket = Some(ticket);
                    pending.pre_open = pre_open;
                }
                Err(ra::Failure::Capacity) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        let front = self.pending.front().unwrap();
        match front.ticket.as_ref().unwrap().observation() {
            ra::TicketObservation::OwnerLost => return Err(Error::OwnerLost),
            ra::TicketObservation::Status(ra::Status::Complete(result)) => {
                // Peer commands own no acquired registration offer. Report their
                // definite failure without stranding unrelated cleanup callbacks.
                if let Err(error) = &result {
                    if matches!(
                        front.work,
                        Work::PeerIdle(..)
                            | Work::PeerCreate(..)
                            | Work::PeerReply(..)
                            | Work::BuildIdle(..)
                            | Work::RemoveIdle(..)
                    ) {
                        let front = self.pending.pop_front().unwrap();
                        self.commands.push_back(CommandReceipt {
                            id: front.id,
                            result,
                        });
                        return Ok(());
                    }
                    return Err(error.clone().into());
                }
                if let Work::Registered(event) = &front.work {
                    if let Some(i) = self
                        .usage
                        .iter()
                        .position(|u| u.lookup == event.lookup && !u.paired)
                    {
                        let consumed = self
                            .ra
                            .snapshot(event.unit.guid.0)
                            .committed
                            .is_some_and(|s| !s.registration_lookups.contains_key(&event.lookup));
                        if consumed {
                            self.release(i)?;
                        }
                    }
                }
                let front = self.pending.pop_front().unwrap();
                if result == Ok(ra::Disposition::Applied)
                    && self.activation.profile() == h::local::Profile::VolatileStatefulLifecycle
                    && self.ra.peer().is_some()
                    && self.route.is_none()
                {
                    if let Work::Registered(event) = &front.work {
                        let snapshot = self.ra.snapshot(event.unit.guid.0);
                        if let Some(state) = snapshot.committed.filter(|_| !snapshot.fenced) {
                            if !state.local_deleted
                                && state.failed_registration_error() == Some(event.error)
                                && state.open_token == event.token
                                && self.registration_failure.is_none()
                            {
                                self.registration_failure = Some(PreOpenRegistrationFailure {
                                    body: (*state.body).clone(),
                                    creation: event.token,
                                    lookup: event.lookup,
                                    command: front.id,
                                    ra_operation: front.ticket.as_ref().unwrap().id,
                                    error: event.error,
                                });
                            }
                        }
                    }
                    if let Work::Fm(FmWork::DeleteReplica(body, ..)) = &front.work {
                        if let Some(failure) = self.pre_open_registration_failure() {
                            let state = self.ra.snapshot(body.replica.0.unit.id.guid.0);
                            if !state.fenced
                                && state
                                    .committed
                                    .is_some_and(|s| s.local_deleted && s.cleanup_pending)
                                && self.failed_registration.is_none()
                            {
                                self.failed_registration = Some(FailedRegistrationDeletion {
                                    failure,
                                    command: front.id,
                                    ra_operation: front.ticket.as_ref().unwrap().id,
                                });
                            }
                        }
                    }
                }
                if result == Ok(ra::Disposition::Applied) && self.pre_open.is_none() {
                    if let (Some(creation), Work::Fm(FmWork::DeleteReplica(body, ..))) =
                        (front.pre_open, &front.work)
                    {
                        let ticket = front.ticket.as_ref().unwrap();
                        let snapshot = self.ra.snapshot(body.replica.0.unit.id.guid.0);
                        if let Some(state) = snapshot.committed.filter(|_| !snapshot.fenced) {
                            if pre_open_cancelled(&state, creation)
                                && state.close_token == ticket.id
                            {
                                self.pre_open = Some(PreOpenCancellation {
                                    body: (*state.body).clone(),
                                    creation,
                                    command: front.id,
                                    ra_operation: ticket.id,
                                });
                            }
                        }
                    }
                }
                if result == Ok(ra::Disposition::Applied)
                    && self.absence.is_none()
                    && self.activation.profile() == h::local::Profile::VolatileStatefulLifecycle
                    && self.ra.peer().is_some()
                {
                    if let Work::Fm(FmWork::DeleteReplica(body, ..)) = &front.work {
                        let snapshot = self.ra.snapshot(body.replica.0.unit.id.guid.0);
                        if let Some(state) = snapshot.committed.filter(|_| !snapshot.fenced) {
                            if never_created(&state) {
                                self.absence = Some(ReplicaAbsence {
                                    body: (*state.body).clone(),
                                    command: front.id,
                                    ra_operation: front.ticket.as_ref().unwrap().id,
                                });
                            }
                        }
                    }
                }
                if let Some(witness) = &mut self.disposal {
                    if witness.proof.command == front.id
                        && matches!(front.work, Work::Closed(_))
                        && result == Ok(ra::Disposition::Applied)
                    {
                        witness.proof.ra_operation = front.ticket.as_ref().unwrap().id;
                        witness.applied = true;
                    }
                }
                self.commands.push_back(CommandReceipt {
                    id: front.id,
                    result,
                });
            }
            _ => {} // Pending/Unknown retain this original accepted ticket.
        }
        Ok(())
    }
    /// Bounded cooperative progress. Capacity returns without losing original
    /// work; drain receipts to provide downstream room. Errors retain this owner.
    pub fn drive(&mut self) -> Result<()> {
        if !self.ra.consumer_lifetime().is_accepting() {
            self.live.store(false, Ordering::SeqCst);
            if let Some(lease) = self.activation.host_lease() {
                lease.revoke();
            }
            self.last_error = Some(Error::OwnerLost);
            return Err(Error::OwnerLost);
        }
        let result = self.drive_inner();
        if let Err(error) = &result {
            self.last_error = Some(error.clone());
            if matches!(
                error,
                Error::Protocol | Error::Unsupported | Error::OwnerLost
            ) {
                if self.rejected_reply.is_none() {
                    self.rejected_reply = self.held_reply.take();
                }
                self.stop_admission();
            }
        }
        result
    }
    fn drive_inner(&mut self) -> Result<()> {
        // Observation failure withdraws new activation use, not existing owners.
        let _ = self.activation.poll_registration();
        self.pending()?;
        if let Some(sending) = &self.sending {
            if let Some(result) = sending.send.result() {
                let sending = self.sending.take().unwrap();
                self.receipts.push_back(EffectReceipt {
                    dispatch: sending.dispatch,
                    disposition: if result.is_ok() {
                        LocalDisposition::Written
                    } else {
                        LocalDisposition::WriteFailed
                    },
                });
            }
        }
        let occupied =
            self.effects.len() + self.receipts.len() + usize::from(self.sending.is_some());
        self.effects
            .extend(self.ra.take_effects_up_to(SLOTS.saturating_sub(occupied)));
        if self.receipts.len() < SLOTS && self.sending.is_none() {
            self.effect()?;
        }
        if self.room() {
            self.reply()?;
        }
        if self.room() {
            self.delivery()?;
        }
        if self.live.load(Ordering::SeqCst)
            && self.pending.is_empty()
            && Instant::now() >= self.retry_at
        {
            self.retry_at = Instant::now() + RETRY;
            if let Some(body) = &self.selected {
                let state = self.ra.snapshot(body.unit.id.guid.0);
                if let Some(state) = state.committed {
                    let unit = body.unit.id.clone();
                    if matches!(state.phase, ra::Phase::Opening | ra::Phase::Closing) {
                        self.retry(unit)?;
                    } else if state.service_description_pending {
                        self.enqueue(Work::RetryDescription(unit))?;
                    }
                }
            }
        }
        Ok(())
    }
    fn release(&mut self, index: usize) -> Result<()> {
        let use_ = &mut self.usage[index];
        let owner = use_.owner.take().ok_or(Error::Protocol)?;
        let lineage = (use_.lookup, owner.id(), use_.paired);
        let result = if use_.paired {
            self.activation.release_usage(owner)
        } else {
            self.activation.compensate_usage(owner)
        };
        match result {
            h::UsageRelease::Released => {
                self.usage.remove(index);
                if let Some(witness) = &mut self.disposal {
                    if lineage == (witness.proof.lookup, witness.proof.acquisition, true) {
                        witness.released = true;
                    }
                }
                Ok(())
            }
            h::UsageRelease::Unresolved { ownership, .. } => {
                self.usage[index].owner = Some(ownership);
                Err(Error::Busy)
            }
        }
    }
    fn lookup(
        &mut self,
        token: u64,
        lookup: u64,
        body: &AddInstance,
        usage: Option<u64>,
    ) -> Result<()> {
        if self.usage.iter().any(|u| u.lookup == lookup) {
            return Err(Error::Protocol);
        }
        if !self.room() || (usage.is_none() && self.usage.len() == SLOTS) {
            return Err(Error::Capacity);
        }
        let premise = self.activation.premise().ok_or(Error::Protocol)?;
        let result = self.activation.lookup_registration(&h::LookupRequest {
            service_type: h::ServiceTypeId {
                package: premise.package.package.clone(),
                name: body.service.service_type.name.clone(),
            },
            version: premise.version.clone(),
            activation: h::Activation::SHARED,
            service_name: premise.service_name.clone(),
            application_name: premise.application_name.clone(),
            should_activate: false,
        })?;
        let mut event = ra::RegistrationCompletion {
            unit: body.unit.id.clone(),
            token,
            lookup,
            ..Default::default()
        };
        match result.result {
            Err(h::Error::TypeNotRegistered) => {
                event.error = ra::HOSTING_SERVICE_TYPE_NOT_REGISTERED
            }
            Err(h::Error::TypeDisabled) => event.error = 0x80071d33u32 as i32 as i64,
            Err(error) => return Err(error.into()),
            Ok(()) => {
                let registration = result.candidate.ok_or(Error::Protocol)?;
                let identity = self.activation.identity().ok_or(Error::Protocol)?;
                if registration.host_id != identity.host_id
                    || registration.package_instance_sequence != identity.code.package_sequence
                    || registration.version != premise.version
                    || service_type(&registration.type_instance.service_type)
                        != body.service.service_type
                {
                    return Err(Error::Protocol);
                }
                event.registration = ra::RuntimeRegistration {
                    service_type: service_type(&registration.type_instance.service_type),
                    host: registration.host_id,
                    runtime: registration.runtime_id,
                    acquisition: lookup,
                    code_package: registration.code_package,
                };
                event.version = version(&registration.version);
                if let Some(existing) = usage {
                    let owner = self
                        .usage
                        .iter()
                        .find(|u| u.lookup == existing && u.paired)
                        .and_then(|u| u.owner.as_ref())
                        .ok_or(Error::Protocol)?;
                    if owner.target().type_instance != registration.type_instance
                        || owner.target().version != registration.version
                        || owner.target().package_instance_id
                            != registration.package_instance_sequence
                    {
                        return Err(Error::Protocol);
                    }
                } else {
                    match self.activation.acquire_usage() {
                        h::UsageIncrement::Acquired(owner) => {
                            event.usage_acquired = true;
                            self.usage.push(Usage {
                                lookup,
                                owner: Some(owner),
                                paired: true,
                            });
                        }
                        h::UsageIncrement::Unresolved { ownership, .. } => {
                            event.usage_error = -1;
                            self.usage.push(Usage {
                                lookup,
                                owner: Some(ownership),
                                paired: false,
                            });
                        }
                        h::UsageIncrement::Rejected(error) => return Err(error.into()),
                        h::UsageIncrement::Unmanaged => return Err(Error::Unsupported),
                    }
                }
            }
        }
        // Capacity was reserved before increment; no fallible operation between
        // the acquisition and placing its frozen completion in owned escrow.
        self.enqueue(Work::Registered(event))?;
        Ok(())
    }
    fn effect(&mut self) -> Result<()> {
        let Some(dispatch) = self.effects.front().cloned() else {
            return Ok(());
        };
        let disposition = match &dispatch.effect {
            ra::Effect::NodeDeactivateUnits { sequence, units } => {
                if units.len() > 1 {
                    return Err(Error::Unsupported);
                }
                if !self.room() {
                    return Ok(());
                }
                for unit in units {
                    self.enqueue(Work::Fm(FmWork::CloseNodeUnit(
                        PartitionId {
                            guid: fube_transport::serialization::serde::Guid(*unit),
                        },
                        *sequence,
                    )))?;
                }
                LocalDisposition::LocalResponse
            }
            ra::Effect::NodeUpdateServiceUnits { work, body, units } => {
                if units.len() > 1 {
                    return Err(Error::Unsupported);
                }
                if !self.room() {
                    return Ok(());
                }
                for unit in units {
                    self.enqueue(Work::Fm(FmWork::UpdateNodeUnit(
                        PartitionId {
                            guid: fube_transport::serialization::serde::Guid(*unit),
                        },
                        *work,
                        body.clone(),
                    )))?;
                }
                LocalDisposition::LocalResponse
            }
            ra::Effect::Open {
                token,
                lookup,
                body,
                usage,
            }
            | ra::Effect::FindRegistration {
                token,
                lookup,
                body,
                usage,
            } => {
                if !self.room() {
                    return Ok(());
                }
                self.lookup(*token, *lookup, body, *usage)?;
                LocalDisposition::LocalResponse
            }
            ra::Effect::RuntimeOpen {
                body, registration, ..
            }
            | ra::Effect::Close {
                body, registration, ..
            }
            | ra::Effect::UpdateServiceDescription { body, registration }
            | ra::Effect::BuildIdle {
                body, registration, ..
            }
            | ra::Effect::RemoveIdle {
                body, registration, ..
            } => {
                let Some(lease) = self.activation.host_lease() else {
                    self.effects.pop_front();
                    self.receipts.push_back(EffectReceipt {
                        dispatch,
                        disposition: LocalDisposition::WriteFailed,
                    });
                    return Ok(());
                };
                let identity = self.activation.identity().ok_or(Error::Protocol)?;
                if registration.host != identity.host_id {
                    return Err(Error::Protocol);
                }
                let (action, flags) = match &dispatch.effect {
                    ra::Effect::RuntimeOpen { flags, .. } => ("ReplicaOpen", *flags),
                    ra::Effect::Close { flags, .. } => ("ReplicaClose", *flags),
                    ra::Effect::UpdateServiceDescription { .. } => {
                        ("ProxyUpdateServiceDescription", 0)
                    }
                    ra::Effect::BuildIdle { .. } => ("ReplicatorBuildIdleReplica", 0),
                    ra::Effect::RemoveIdle { .. } => ("ReplicatorRemoveIdleReplica", 0),
                    _ => unreachable!(),
                };
                let bytes = if let ra::Effect::BuildIdle { replica, .. }
                | ra::Effect::RemoveIdle { replica, .. } = &dispatch.effect
                {
                    rap::idle_request(body, replica)?
                } else {
                    rap::request(registration, body, flags)?
                };
                if bytes.len() > BODY {
                    return Err(Error::Capacity);
                }
                match lease.send_lifecycle(action, bytes) {
                    Ok(send) => {
                        self.route = Some(Route {
                            runtime: registration.runtime.clone(),
                            lease,
                        });
                        self.effects.pop_front();
                        self.sending = Some(Sending { dispatch, send });
                        return Ok(());
                    }
                    Err(fube_transport::Error::ResourceLimit(_)) => return Ok(()),
                    // This is a local failed write observation, NOT a semantic
                    // outcome. HostClosed and RA retries remain authoritative.
                    Err(_) => LocalDisposition::WriteFailed,
                }
            }
            ra::Effect::ReleaseUsage { acquisition } => {
                let index = self
                    .usage
                    .iter()
                    .position(|u| u.lookup == *acquisition)
                    .ok_or(Error::Protocol)?;
                self.release(index)?;
                LocalDisposition::ExactUsageReleased
            }
            ra::Effect::ReleaseRegistration(_) => LocalDisposition::RegistrationReferenceReleased,
            ra::Effect::TerminateHost { registration } => {
                if self
                    .activation
                    .identity()
                    .is_none_or(|identity| identity.host_id != registration.host)
                    || self
                        .route
                        .as_ref()
                        .is_none_or(|route| route.runtime != registration.runtime)
                {
                    return Err(Error::Unsupported);
                }
                self.stop_admission();
                match self.activation.terminate() {
                    Ok(true) => {}
                    Err(h::local::LocalError::Busy)
                        if self.activation.cleanup_status().logical_retired => {}
                    Ok(false) | Err(h::local::LocalError::Busy) => return Ok(()),
                    Err(error) => return Err(error.into()),
                }
                LocalDisposition::HostTerminated
            }
            ra::Effect::Health(_) | ra::Effect::OpeningHealth(_) => {
                LocalDisposition::HealthProcessingDisabled
            }
            ra::Effect::NodeUpdateServiceReply(body) => {
                if let Some(selected) = &mut self.selected {
                    if body.service == selected.service.name
                        && body.instance == selected.service.instance
                    {
                        if let Some(state) = self.ra.snapshot(selected.unit.id.guid.0).committed {
                            if state.body.service.update_version == body.version {
                                selected.service = state.body.service.clone();
                            }
                        }
                    }
                }
                LocalDisposition::LocalResponse
            }
            ra::Effect::Reply(_)
            | ra::Effect::PeerCreateReplica { .. }
            | ra::Effect::PeerCreateReplicaReply { .. }
            | ra::Effect::NodeActivateReply(_)
            | ra::Effect::NodeDeactivateReply(_)
            | ra::Effect::PrimaryReply(_)
            | ra::Effect::AddReplicaReply(_)
            | ra::Effect::RemoveReplicaReply(_)
            | ra::Effect::LocalStatefulOpenReply(_)
            | ra::Effect::RemoveReply(_)
            | ra::Effect::DeleteReplicaReply(_) => LocalDisposition::LocalResponse,
            ra::Effect::RequestReportRetry
            | ra::Effect::ServiceTypeStatusWork
            | ra::Effect::ServiceTypeStatus { .. }
            | ra::Effect::ArmNodeDeactivationCheck { .. }
            | ra::Effect::ReportLoad(_)
            | ra::Effect::StatefulClosePending { .. }
            | ra::Effect::StatefulDroppedPending(_)
            | ra::Effect::ReplicaUp(_)
            | ra::Effect::RequestServiceTypeWork { .. }
            | ra::Effect::ServiceTypeWorkCaptured { .. }
            | ra::Effect::ArmServiceTypeRetry { .. }
            | ra::Effect::CancelServiceTypeRetry { .. } => {
                if self.external.len() == SLOTS {
                    return Ok(());
                }
                self.external.push(dispatch.clone());
                LocalDisposition::ExternalPending
            } // These are outside the preflighted local command/action allowlist.
              // Retain original effect, fail explicitly, never acknowledge success.
        };
        self.effects.pop_front();
        self.receipts.push_back(EffectReceipt {
            dispatch,
            disposition,
        });
        Ok(())
    }
    fn reply(&mut self) -> Result<()> {
        #[cfg(feature = "qualification")]
        if self.hold_replies && self.held_reply.is_none() {
            return Ok(());
        }
        // Retain first malformed input diagnostically without blocking required
        // HostClosed/cleanup consumers after authority retirement.
        if self.rejected_reply.is_some() {
            return Ok(());
        }
        let Some(route) = &self.route else {
            return Ok(());
        };
        if self.held_reply.is_none() {
            self.held_reply = route.lease.take_lifecycle_reply();
        }
        let Some(reply) = &self.held_reply else {
            return Ok(());
        };
        if !reply.belongs_to(&route.lease) {
            return Err(Error::Protocol);
        }
        let action = reply.action().map_err(|_| Error::Protocol)?;
        if action == "RAPReportLoad" {
            let report: ReportLoad = decode(reply.body())?;
            self.enqueue(Work::Load(Box::new(report)))?;
            self.held_reply = None;
            return Ok(());
        }
        if action == "ProxyUpdateServiceDescriptionReply" {
            #[derive(Deserialize)]
            struct Reply {
                error: i64,
                unit: FailoverUnit,
                replica: Replica,
                service: Service,
            }
            let body: Reply = decode(reply.body())?;
            self.enqueue(Work::Description(Box::new(
                ra::ServiceDescriptionCompletion {
                    error: body.error,
                    unit: body.unit,
                    replica: body.replica,
                    service: body.service,
                },
            )))?;
            self.held_reply = None;
            return Ok(());
        }
        let body: rap::Reply = decode(reply.body())?;
        let idle = matches!(
            action.as_str(),
            "ReplicatorBuildIdleReplicaReply" | "ReplicatorRemoveIdleReplicaReply"
        );
        if (idle && (body.remote.0.len() != 1 || body.flags != 0))
            || (!idle && (!body.remote.0.is_empty() || body.flags & !12 != 0))
        {
            return Err(Error::Protocol);
        }
        let Some(state) = self.ra.snapshot(body.unit.id.guid.0).committed else {
            self.held_reply = None;
            return Ok(());
        };
        let Some(registration) = &state.runtime else {
            self.held_reply = None;
            return Ok(());
        };
        if self
            .activation
            .identity()
            .is_none_or(|id| id.host_id != registration.host)
            || registration.runtime != route.runtime
        {
            return Err(Error::Protocol);
        }
        let work = match reply.action().map_err(|_| Error::Protocol)?.as_str() {
            "ReplicatorBuildIdleReplicaReply" | "ReplicatorRemoveIdleReplicaReply" => {
                let event = ra::BuildCompletion {
                    unit: body.unit,
                    local: body.local,
                    remote: body
                        .remote
                        .0
                        .into_iter()
                        .next()
                        .expect("checked one remote"),
                    error: body.error.error,
                    detail: ra::ProxyErrorDetail {
                        message: body.error.message,
                        api: body.error.api,
                    },
                };
                if action == "ReplicatorBuildIdleReplicaReply" {
                    Work::Built(event)
                } else {
                    Work::Removed(event)
                }
            }
            "ReplicaOpenReply" => Work::Open(ra::OpenCompletion {
                unit: body.unit,
                replica: body.local,
                error: body.error.error,
                flags: body.flags,
                detail: ra::ProxyErrorDetail {
                    message: body.error.message,
                    api: body.error.api,
                },
            }),
            "ReplicaCloseReply" => Work::Closed(ra::CloseCompletion {
                unit: body.unit,
                replica: body.local,
                error: body.error.error,
            }),
            _ => return Err(Error::Unsupported),
        };
        // Capture provenance only for an exact currently eligible successful
        // original-runtime Close, never from Closed/host-down/Remove accounting.
        let witness = if matches!(&work, Work::Closed(event) if event.error == 0
            && event.unit == state.body.unit
            && event.replica.node == state.body.replica.node
            && event.replica.id == state.body.replica.id
            && event.replica.instance == state.body.replica.instance
            && event.replica.current_role == state.body.replica.current_role)
            && state.phase == ra::Phase::Closing
            && state.close_mode != Some(ra::CloseMode::Obliterate)
            && state.body.replica.up
            && self.disposal.is_none()
        {
            state.usage.and_then(|lookup| {
                self.usage
                    .iter()
                    .find(|use_| use_.lookup == lookup && use_.paired)
                    .and_then(|use_| use_.owner.as_ref())
                    .map(|owner| DisposalWitness {
                        proof: InstanceDisposal {
                            body: (*state.body).clone(),
                            host: registration.host.clone(),
                            runtime: registration.runtime.clone(),
                            command: 0,
                            ra_operation: 0,
                            lookup,
                            acquisition: owner.id(),
                        },
                        applied: false,
                        released: false,
                    })
            })
        } else {
            None
        };
        let id = self.enqueue(work)?;
        if let Some(mut witness) = witness {
            witness.proof.command = id;
            self.disposal = Some(witness);
        }
        self.held_reply = None;
        Ok(())
    }
    fn delivery(&mut self) -> Result<()> {
        if self.retained_delivery.is_none() {
            for lane in [
                ipc::DeliveryLane::Registration,
                ipc::DeliveryLane::Status,
                ipc::DeliveryLane::Obligation,
            ] {
                if let Some(delivery) = self.activation.next_delivery(lane, BODY)? {
                    self.retained_delivery = Some(delivery);
                    break;
                }
            }
        }
        let Some(delivery) = self.retained_delivery.clone() else {
            return Ok(());
        };
        match &delivery {
            ipc::Delivery::Registration(event) => {
                if matches!(event, h::ClosedRegisteredEvent::Registered { .. }) {
                    self.package_registration.registered = true;
                    self.package_registration.unregistered_at = None;
                }
                if let Some(body) = &self.selected {
                    self.enqueue(Work::Event(delivery.clone(), body.unit.id.guid.0))?;
                }
            }
            ipc::Delivery::Status(status) => {
                let (sequence, id, enabled) = match status {
                    h::EnabledDisabledEvent::Enabled {
                        sequence,
                        type_instance,
                    } => (*sequence, &type_instance.service_type, true),
                    h::EnabledDisabledEvent::Disabled {
                        sequence,
                        type_instance,
                    } => (*sequence, &type_instance.service_type, false),
                };
                self.statuses
                    .consume_local(service_type(id), sequence, enabled, 0)?;
                self.enqueue(Work::Status(service_type(id), sequence, enabled))?;
            }
            ipc::Delivery::Obligation(h::Obligation::TypesUnregistered { types }) => {
                let premise = self.activation.premise().ok_or(Error::Protocol)?;
                if types.iter().any(|t| {
                    t.service_type.package != premise.package.package
                        || t.activation != h::Activation::SHARED
                }) {
                    return Err(Error::Unsupported);
                }
                self.package_registration.registered = false;
                self.package_registration.unregistered_at = Some(Instant::now());
            }
            ipc::Delivery::Obligation(h::Obligation::HealthSourceReleased { .. }) => {
                // remove_declaration already performed the bounded disabled-health
                // source removal. This is its local completion, not a second HM send.
                self.package_registration.health_sources_released = self
                    .package_registration
                    .health_sources_released
                    .checked_add(1)
                    .ok_or(Error::Capacity)?;
            }
            ipc::Delivery::Obligation(_) => return Err(Error::Unsupported),
        }
        self.retained_delivery = None;
        Ok(())
    }
    /// Withdraw new user work; retained original work remains driveable.
    pub fn stop_admission(&self) {
        self.live.store(false, Ordering::SeqCst);
        if let Some(lease) = self.activation.host_lease() {
            lease.revoke();
        }
    }
    /// Explicit physical cleanup, not implied by service Close. Busy/error
    /// retains this owner; drive HostClosed/exact release and retry.
    pub fn terminate(&mut self) -> Result<bool> {
        self.stop_admission();
        self.activation.terminate().map_err(Into::into)
    }
    pub async fn close_executor(&self, budget: Duration) -> Result<()> {
        if !self.activation.producers_stopped() || !self.quiescent() {
            return Err(Error::Busy);
        }
        self.ra.close(budget).await?;
        Ok(())
    }
    /// Deliberate RA-owner failure injection, never a production shutdown API.
    #[cfg(feature = "qualification")]
    #[doc(hidden)]
    pub async fn qualification_close_owner(&self, budget: Duration) -> Result<()> {
        self.ra.close(budget).await?;
        Ok(())
    }
    /// Qualification-only input replacement using an opaque real Entry token.
    /// Production delivery still comes exclusively from Hosting's inbox.
    #[cfg(feature = "qualification")]
    #[doc(hidden)]
    pub fn qualification_ingress(&mut self, reply: ipc::LifecycleReply) -> Result<()> {
        if self.held_reply.is_some() || !self.room() {
            return Err(Error::Capacity);
        }
        self.held_reply = Some(reply);
        Ok(())
    }
    #[cfg(feature = "qualification")]
    #[doc(hidden)]
    pub fn qualification_hold_replies(&mut self, hold: bool) {
        self.hold_replies = hold;
    }
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > BODY {
        return Err(Error::Capacity);
    }
    let mut budget = 4 * BODY;
    from_slice_with_budget(bytes, &mut |capacity| {
        budget = budget
            .checked_sub(capacity)
            .ok_or(fube_transport::Error::ResourceLimit("local RAP decode"))?;
        Ok(())
    })
    .map_err(|_| Error::Protocol)
}
