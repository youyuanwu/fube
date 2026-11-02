//! Production, caller-driven stateless RA/RAP on one owned local activation.
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
    Add(Arc<AddInstance>),
    Close(PartitionId, ra::CloseMode),
    Registered(ra::RegistrationCompletion),
    Open(ra::OpenCompletion),
    Closed(ra::CloseCompletion),
    Event(ipc::Delivery, ra::EntityId),
    Retry(PartitionId, bool),
}
struct Pending {
    id: u64,
    work: Work,
    ticket: Option<ra::Ticket>,
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
        if activation.identity().is_some()
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
    pub fn package_registration(&self) -> &PackageRegistration {
        &self.package_registration
    }
    pub fn pending_status(&self) -> &[ra::PendingStatus] {
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
        });
        self.next = id;
        Ok(id)
    }
    /// Authorize a supported local Add, never registration-as-readiness.
    /// Description updates and foreign package/profile inputs reject before RA.
    pub fn add(&mut self, body: AddInstance) -> Result<u64> {
        if !self.live.load(Ordering::SeqCst) || !self.ra.consumer_lifetime().is_accepting() {
            return Err(Error::Busy);
        }
        let bytes = to_vec(&body).map_err(|_| Error::Unsupported)?;
        if bytes.len() > ADD_BODY {
            return Err(Error::Capacity);
        }
        let body: AddInstance = decode(&bytes)?;
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
        if lookup.result == Err(h::Error::InvalidServiceType)
            || service_type(&expected) != body.service.service_type
            || body.service.version != version(&premise.version)
            || body.replica.version != version(&premise.version)
            || body.service.application != premise.application_name
            || body.service.name != premise.service_name
            || body.replica.node != self.admission.local_node
            || body.replica.current_role != 1
            || body.replica.state != 1
            || !body.replica.up
            || body.replica.id < 0
            || body.replica.instance < 0
            || body.service.stateful
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
            || identity.code.package_sequence <= 0
        {
            return Err(Error::Unsupported);
        }
        if let Some(selected) = &self.selected {
            if body.unit.id != selected.unit.id || body.service != selected.service {
                return Err(Error::Unsupported);
            }
        }
        let current = self.ra.snapshot(body.unit.id.guid.0).committed;
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
        let id = self.enqueue(Work::Add(Arc::new(body.clone())))?;
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
                ra::CloseMode::Close | ra::CloseMode::Drop | ra::CloseMode::ForceAbort
            )
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
            Work::Add(body) => ra::Command::Add {
                body: Box::new((**body).clone()),
                source: self.admission.fm_source.clone(),
                generation: GenerationHeader {
                    number: self.admission.generation.clone(),
                    for_fm_replica: false,
                },
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
            match self.ra.submit(command) {
                Ok(ticket) => self.pending.front_mut().unwrap().ticket = Some(ticket),
                Err(ra::Failure::Capacity) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        let front = self.pending.front().unwrap();
        match front.ticket.as_ref().unwrap().observation() {
            ra::TicketObservation::OwnerLost => return Err(Error::OwnerLost),
            ra::TicketObservation::Status(ra::Status::Complete(result)) => {
                // A definite failed accepted command is retained, never silently
                // resubmitted or treated as consuming a registration offer.
                if let Err(error) = &result {
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
                if state
                    .committed
                    .is_some_and(|s| matches!(s.phase, ra::Phase::Opening | ra::Phase::Closing))
                {
                    self.retry(body.unit.id.clone())?;
                }
            }
        }
        Ok(())
    }
    fn release(&mut self, index: usize) -> Result<()> {
        let use_ = &mut self.usage[index];
        let owner = use_.owner.take().ok_or(Error::Protocol)?;
        let result = if use_.paired {
            self.activation.release_usage(owner)
        } else {
            self.activation.compensate_usage(owner)
        };
        match result {
            h::UsageRelease::Released => {
                self.usage.remove(index);
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
                body,
                registration,
                flags,
                ..
            }
            | ra::Effect::Close {
                body,
                registration,
                flags,
                ..
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
                let bytes = rap::request(registration, body, *flags)?;
                if bytes.len() > BODY {
                    return Err(Error::Capacity);
                }
                let action = if matches!(dispatch.effect, ra::Effect::RuntimeOpen { .. }) {
                    "ReplicaOpen"
                } else {
                    "ReplicaClose"
                };
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
            ra::Effect::Health(_) | ra::Effect::OpeningHealth(_) => {
                LocalDisposition::HealthProcessingDisabled
            }
            ra::Effect::Reply(_) | ra::Effect::RemoveReply(_) => LocalDisposition::LocalResponse,
            ra::Effect::RequestReportRetry
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
            }
            // These are outside the preflighted local command/action allowlist.
            // Retain original effect, fail explicitly, never acknowledge success.
            ra::Effect::TerminateHost { .. } | ra::Effect::UpdateServiceDescription { .. } => {
                return Err(Error::Unsupported)
            }
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
        let body: rap::Reply = decode(reply.body())?;
        if !body.remote.0.is_empty() || body.flags & !12 != 0 {
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
        self.enqueue(work)?;
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
