//! Minimal stateless entity execution, not a running RA, Hosting adapter or outbox.
//!
//! One owned supervisor hosts bounded per-entity workers independently of caller
//! runtimes. Native source grounding: EntityState/LockedEntityPtr, EntityMap and
//! EntityJobItem; see the catalog's selected stateless decision/test tables.
use crate::protocol::*;
use fube_ra_store::{self as store, KeyValueStore, Output, Record, RecordId, Request};
use fube_transport::serialization::serde::{from_slice, to_vec};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::sync::{mpsc, watch, Notify};
#[path = "lifecycle.rs"]
mod lifecycle;
pub use lifecycle::*;

pub type EntityId = [u8; 16];
pub type Result<T> = std::result::Result<T, Failure>;
pub const RA_NOT_READY: i64 = -2_147_017_447;
pub const SERVICE_TYPE_NOT_REGISTERED: i64 = -2_147_017_448;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Admission(&'static str),
    Unsupported(&'static str),
    Capacity,
    Closed,
    Store(store::Error),
    /// Observation is uncertain, not rollback or permission to replace a worker.
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Applied,
    IgnoredStale,
    IgnoredPending,
    IgnoredNotReady,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Pending,
    Unknown,
    Complete(Result<Disposition>),
}
#[derive(Clone)]
pub struct Ticket {
    pub id: u64,
    receiver: watch::Receiver<Status>,
}
impl Ticket {
    pub fn status(&self) -> Status {
        self.receiver.borrow().clone()
    }
    /// Finite caller observation. Cancellation never owns/retracts accepted work.
    pub async fn wait(&self, budget: Duration) -> Result<Disposition> {
        self.observe(budget, false).await
    }
    /// Continue observing the same accepted work after an unknown report.
    pub async fn resolve(&self, budget: Duration) -> Result<Disposition> {
        self.observe(budget, true).await
    }
    async fn observe(&self, budget: Duration, resolve: bool) -> Result<Disposition> {
        let mut receiver = self.receiver.clone();
        tokio::time::timeout(budget, async {
            loop {
                match receiver.borrow().clone() {
                    Status::Complete(result) => return result,
                    Status::Unknown if !resolve => return Err(Failure::Unknown),
                    _ => {}
                }
                receiver.changed().await.map_err(|_| Failure::Unknown)?;
            }
        })
        .await
        .map_err(|_| Failure::Unknown)?
    }
}

/// Trusted configured-session admission, additional to native fields.
/// This is not cryptographic source authentication or Federation membership.
#[derive(Debug, Clone)]
pub struct Admission {
    pub local_node: NodeInstance,
    pub fm_source: NodeInstance,
    pub generation: GenerationNumber,
    pub owner_ready: bool,
    pub node_open: bool,
    pub node_activated: bool,
    pub fabric_upgrading: bool,
}
#[derive(Debug, Clone)]
pub struct Settings {
    pub max_entities: usize,
    pub max_outstanding: usize,
    pub max_command_bytes: usize,
    pub max_queued_bytes: usize,
    pub max_effects: usize,
    pub resolution_poll: Duration,
    pub close_warning: u64,
    pub delete_drop: u64,
    pub close_drop: u64,
    pub registration_warning: u64,
    pub registration_drop: u64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            max_entities: 32,
            max_outstanding: 64,
            max_command_bytes: 256 * 1024,
            max_queued_bytes: 8 * 1024 * 1024,
            max_effects: 64,
            resolution_poll: Duration::from_millis(20),
            close_warning: 10,
            delete_drop: 40,
            close_drop: 40320,
            registration_warning: 240,
            registration_drop: 1920,
        }
    }
}
impl Settings {
    fn validate(&self) -> Result<()> {
        if self.max_entities == 0
            || self.max_entities > 1024
            || self.max_outstanding == 0
            || self.max_outstanding > 65536
            || self.max_command_bytes == 0
            || self.max_command_bytes > 4 * 1024 * 1024
            || self.max_queued_bytes == 0
            || self.max_queued_bytes > 64 * 1024 * 1024
            || self.max_effects == 0
            || self.max_effects > 65536
            || self.resolution_poll.is_zero()
            || self.resolution_poll > Duration::from_secs(60)
            || self.close_warning == 0
            || self.delete_drop == 0
            || self.close_drop == 0
            || self.registration_warning == 0
            || self.registration_drop == 0
        {
            return Err(Failure::Admission("invalid finite executor settings"));
        }
        Ok(())
    }
}

/// Result supplied by a trusted future Hosting/RAP adapter (tests supply it now).
/// It does not itself grant application authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenCompletion {
    pub token: u64,
    pub unit: FailoverUnit,
    pub replica: Replica,
    pub error: i64,
}
pub enum Command {
    Add {
        body: Box<AddInstance>,
        source: NodeInstance,
        generation: GenerationHeader,
    },
    CompleteOpen(Box<OpenCompletion>),
    /// Explicit new retry work, not an automatic duplicate dispatch.
    RetryOpen(PartitionId),
    Remove {
        body: Box<RemoveInstance>,
        source: NodeInstance,
        generation: GenerationHeader,
    },
    ReplicaUpReply {
        body: Box<ReplicaUp>,
        source: NodeInstance,
        generation: GenerationHeader,
    },
    Registered(Box<RegistrationCompletion>),
    CompleteClose(Box<CloseCompletion>),
    HostDown(Box<HostDown>),
    RetryClose(PartitionId),
    RetryReport(PartitionId),
}
enum Kind {
    Add {
        source: NodeInstance,
        generation: GenerationHeader,
    },
    Complete,
    Retry,
    Remove {
        source: NodeInstance,
        generation: GenerationHeader,
    },
    ReplicaUpReply {
        source: NodeInstance,
        generation: GenerationHeader,
    },
    Registered,
    CompleteClose,
    HostDown,
    RetryClose,
    RetryReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Opening,
    Ready,
    Closing,
    Closed,
}
#[derive(Debug, Clone, PartialEq)]
pub struct EntityState {
    pub body: Arc<AddInstance>,
    pub phase: Phase,
    pub open_token: u64,
    /// Volatile retry bookkeeping; not a native disk record or durable outbox.
    pub retry_attempts: u64,
    pub last_retry_error: i64,
    pub runtime: Option<RuntimeRegistration>,
    pub close_mode: Option<CloseMode>,
    pub close_token: u64,
    pub local_deleted: bool,
    pub cleanup_pending: bool,
    pub close_failures: u64,
    pub registration_failures: u64,
    pub close_warning: bool,
    pub registration_warning: bool,
    pub fm_stage: FmStage,
    pub report_sequence: u64,
}
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub committed: Option<Arc<EntityState>>,
    pub fenced: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// Resolve service-type registration. RAP Open may run only after Registered
    /// has published RuntimeOpen for this token.
    Open {
        token: u64,
        body: Arc<AddInstance>,
    },
    Reply(Box<AddInstanceReply>),
    RuntimeOpen {
        token: u64,
        body: Arc<AddInstance>,
        registration: RuntimeRegistration,
    },
    Close {
        token: u64,
        body: Arc<AddInstance>,
        registration: RuntimeRegistration,
        flags: i64,
    },
    FindRegistration {
        token: u64,
        body: Arc<AddInstance>,
    },
    TerminateHost {
        registration: RuntimeRegistration,
    },
    ReleaseRegistration(RuntimeRegistration),
    Health(HealthEvent),
    RequestReportRetry,
    ReplicaUp(Box<ReplicaUp>),
    RemoveReply(Box<RemoveInstanceReply>),
}
#[derive(Debug, Clone, PartialEq)]
pub struct Dispatched {
    pub operation: u64,
    pub entity: EntityId,
    pub effect: Effect,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Statistics {
    pub accepted: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub outstanding: usize,
    pub queued_bytes: usize,
    pub effects: usize,
}
#[derive(Default)]
struct View {
    entities: BTreeMap<EntityId, Snapshot>,
    effects: VecDeque<Dispatched>,
    stats: Statistics,
    reserved_effects: usize,
    accepting: bool,
}
struct Shared {
    view: Mutex<View>,
    settings: Settings,
    shutdown: Notify,
    #[cfg(test)]
    resolution_polls: std::sync::atomic::AtomicUsize,
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
struct Submission {
    id: u64,
    entity: EntityId,
    kind: Kind,
    bytes: Box<[u8]>,
    charge: usize,
    reserved_effects: usize,
    result: watch::Sender<Status>,
}

/// Cloneable admission/observation handle. Dropping every handle closes admission
/// and drains accepted work; unknown work may retain its supervisor indefinitely.
#[derive(Clone)]
pub struct Executor {
    shared: Arc<Shared>,
    sender: mpsc::Sender<Submission>,
    closed: watch::Receiver<Option<Result<Statistics>>>,
}
impl Executor {
    pub fn new(
        store: Arc<dyn KeyValueStore>,
        admission: Admission,
        settings: Settings,
    ) -> Result<Self> {
        settings.validate()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Failure::Admission("cannot create owned executor runtime"))?;
        let (sender, receiver) = mpsc::channel(settings.max_outstanding);
        let (done, closed) = watch::channel(None);
        let shared = Arc::new(Shared {
            view: Mutex::new(View {
                accepting: true,
                ..View::default()
            }),
            settings,
            shutdown: Notify::new(),
            #[cfg(test)]
            resolution_polls: std::sync::atomic::AtomicUsize::new(0),
        });
        let owned = shared.clone();
        std::thread::Builder::new()
            .name("ra-entities".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(supervise(owned.clone(), receiver, store, admission))
                }))
                .unwrap_or(Err(Failure::Unknown));
                let mut view = lock(&owned.view);
                view.accepting = false;
                if result.is_err() {
                    for entry in view.entities.values_mut() {
                        entry.fenced = true;
                    }
                }
                drop(view);
                done.send_replace(Some(result));
            })
            .map_err(|_| Failure::Admission("cannot start owned executor"))?;
        Ok(Self {
            shared,
            sender,
            closed,
        })
    }

    pub fn submit(&self, command: Command) -> Result<Ticket> {
        let (entity, kind, bytes) = match command {
            Command::Add {
                body,
                source,
                generation,
            } => {
                let entity = body.unit.id.guid.0;
                (entity, Kind::Add { source, generation }, to_vec(&body))
            }
            Command::CompleteOpen(body) => (body.unit.id.guid.0, Kind::Complete, to_vec(&body)),
            Command::RetryOpen(unit) => (unit.guid.0, Kind::Retry, to_vec(&unit)),
            Command::Remove {
                body,
                source,
                generation,
            } => (
                body.replica.0.unit.id.guid.0,
                Kind::Remove { source, generation },
                to_vec(&body),
            ),
            Command::ReplicaUpReply {
                body,
                source,
                generation,
            } => {
                if body.last || body.from_fmm || body.replicas.0.len() + body.dropped.0.len() != 1 {
                    return Err(Failure::Unsupported(
                        "only one-FT non-last FM ReplicaUpReply is enabled",
                    ));
                }
                let info = body
                    .replicas
                    .0
                    .first()
                    .or_else(|| body.dropped.0.first())
                    .unwrap();
                (
                    info.unit.id.guid.0,
                    Kind::ReplicaUpReply { source, generation },
                    to_vec(&body),
                )
            }
            Command::Registered(body) => (body.unit.guid.0, Kind::Registered, to_vec(&body)),
            Command::CompleteClose(body) => {
                (body.unit.id.guid.0, Kind::CompleteClose, to_vec(&body))
            }
            Command::HostDown(body) => (body.unit.guid.0, Kind::HostDown, to_vec(&body)),
            Command::RetryClose(unit) => (unit.guid.0, Kind::RetryClose, to_vec(&unit)),
            Command::RetryReport(unit) => (unit.guid.0, Kind::RetryReport, to_vec(&unit)),
        };
        // Add may report a closed replica then return StaleRequest. Lifecycle
        // commands may release, clear warnings, report health, schedule FM work
        // and reply. Bounds are checked again before any persistence/effects.
        let reserved_effects = match &kind {
            Kind::Add { .. } => 2,
            Kind::Complete | Kind::Retry | Kind::RetryReport => 1,
            _ => 8,
        };
        let bytes = bytes
            .map_err(|_| Failure::Admission("unsupported/oversized native shape"))?
            .into_boxed_slice();
        let settings = &self.shared.settings;
        if bytes.len() > settings.max_command_bytes {
            return Err(Failure::Capacity);
        }
        let charge = bytes.len() + 256;
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        let new_entity = !view.entities.contains_key(&entity);
        if view.stats.outstanding >= settings.max_outstanding
            || view.stats.queued_bytes + charge > settings.max_queued_bytes
            || view.effects.len() + view.reserved_effects + reserved_effects > settings.max_effects
            || (new_entity && view.entities.len() >= settings.max_entities)
            || view.stats.accepted == u64::MAX
        {
            return Err(Failure::Capacity);
        }
        let id = view.stats.accepted + 1;
        let (result, receiver) = watch::channel(Status::Pending);
        let work = Submission {
            id,
            entity,
            kind,
            bytes,
            charge,
            reserved_effects,
            result,
        };
        // Reservation and enqueue are atomic with all other submitters.
        self.sender.try_send(work).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Failure::Capacity,
            mpsc::error::TrySendError::Closed(_) => Failure::Closed,
        })?;
        view.entities.entry(entity).or_default();
        view.stats.accepted = id;
        view.stats.outstanding += 1;
        view.stats.queued_bytes += charge;
        view.reserved_effects += reserved_effects;
        Ok(Ticket { id, receiver })
    }

    pub fn snapshot(&self, entity: EntityId) -> Snapshot {
        lock(&self.shared.view)
            .entities
            .get(&entity)
            .cloned()
            .unwrap_or_default()
    }
    pub fn statistics(&self) -> Statistics {
        lock(&self.shared.view).stats.clone()
    }
    /// Transfer each dispatched action once. Sending/ack/retry policy is separate.
    pub fn take_effects(&self) -> Vec<Dispatched> {
        let mut view = lock(&self.shared.view);
        let effects = view.effects.drain(..).collect();
        view.stats.effects = 0;
        effects
    }
    /// Close only this executor, not the caller-owned shared store.
    pub async fn close(&self, budget: Duration) -> Result<Statistics> {
        lock(&self.shared.view).accepting = false;
        self.shared.shutdown.notify_one();
        let mut receiver = self.closed.clone();
        tokio::time::timeout(budget, async {
            loop {
                if let Some(result) = receiver.borrow().clone() {
                    return result;
                }
                receiver.changed().await.map_err(|_| Failure::Unknown)?;
            }
        })
        .await
        .map_err(|_| Failure::Unknown)?
    }
}

async fn supervise(
    shared: Arc<Shared>,
    mut receiver: mpsc::Receiver<Submission>,
    store: Arc<dyn KeyValueStore>,
    admission: Admission,
) -> Result<Statistics> {
    let mut entities = BTreeMap::<EntityId, mpsc::UnboundedSender<Submission>>::new();
    let mut workers = tokio::task::JoinSet::new();
    loop {
        if !lock(&shared.view).accepting {
            receiver.close();
        }
        tokio::select! {
            work = receiver.recv() => {
                let Some(work) = work else { break; };
                let worker = entities.entry(work.entity).or_insert_with(|| {
                    // This internal channel is bounded by global outstanding count
                    // AND retained-byte admission, held through actual completion.
                    let (sender, receiver) = mpsc::unbounded_channel();
                    workers.spawn(entity_worker(shared.clone(), receiver, store.clone(), admission.clone()));
                    sender
                });
                if worker.send(work).is_err() { return Err(Failure::Unknown); }
            }
            _ = shared.shutdown.notified() => { receiver.close(); }
            result = workers.join_next(), if !workers.is_empty() => {
                if result.is_some() { return Err(Failure::Unknown); }
            }
        }
    }
    drop(entities); // close each worker after all accepted submissions dispatched
    while let Some(result) = workers.join_next().await {
        result.map_err(|_| Failure::Unknown)?;
    }
    let view = lock(&shared.view);
    if view.stats.outstanding != 0 {
        return Err(Failure::Unknown);
    }
    Ok(view.stats.clone())
}

enum Update {
    None,
    Memory(EntityState),
    Durable(EntityState),
}
struct Transition {
    update: Update,
    effects: Vec<Effect>,
    disposition: Disposition,
}
impl Transition {
    fn ignored(disposition: Disposition) -> Self {
        Self {
            update: Update::None,
            effects: Vec::new(),
            disposition,
        }
    }
}
fn reply(body: &AddInstance, error: i64) -> Effect {
    Effect::Reply(Box::new(AddInstanceReply {
        unit: body.unit.clone(),
        replica: body.replica.clone(),
        error,
    }))
}
fn transition(
    work: &Submission,
    state: Option<&EntityState>,
    admission: &Admission,
    settings: &Settings,
) -> Result<Transition> {
    let decode_error = |_| Failure::Admission("invalid native command payload");
    match &work.kind {
        Kind::Add { source, generation } => {
            let body: AddInstance = from_slice(&work.bytes).map_err(decode_error)?;
            if source != &admission.fm_source || body.replica.node != admission.local_node {
                return Err(Failure::Admission(
                    "configured session/node incarnation mismatch",
                ));
            }
            if generation.for_fm_replica {
                return Err(Failure::Unsupported(
                    "FM-service/FMM is not a stateless profile",
                ));
            }
            if generation.number != GenerationNumber::default()
                && generation.number != admission.generation
            {
                return Err(Failure::Admission(
                    "generation protocol must reconcile before dispatch",
                ));
            }
            if !admission.node_open || !admission.owner_ready || admission.fabric_upgrading {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            if body.service.stateful
                || body.service.persisted
                || body.service.group
                || body.replica.current_role != 1
                || body.replica.state != 1
                || !body.replica.up
                || body.replica.id < 0
                || body.replica.instance < 0
                || body.replica.version != body.service.version
            {
                return Err(Failure::Unsupported(
                    "not the admitted stateless InBuild package profile",
                ));
            }
            if !admission.node_activated {
                return Ok(Transition {
                    update: Update::None,
                    effects: vec![reply(&body, RA_NOT_READY)],
                    disposition: Disposition::Applied,
                });
            }
            let body = lifecycle::create_body(body);
            if let Some(current) = state {
                if let Some(result) = lifecycle::add_existing(work, current, &body) {
                    return result;
                }
                let local = &current.body;
                if body.unit.current.older_than(local.unit.current)
                    || body.replica.instance < local.replica.instance
                    || body.replica.id < local.replica.id
                {
                    return Ok(Transition::ignored(Disposition::IgnoredStale));
                }
                if body.replica.id != local.replica.id
                    || body.replica.instance != local.replica.instance
                {
                    return Err(Failure::Unsupported(
                        "replacement requires native close/drop choreography",
                    ));
                }
                if body.unit != local.unit || body.service != local.service {
                    return Err(Failure::Unsupported(
                        "epoch/service-description update choreography is staged",
                    ));
                }
                return Ok(if current.phase == Phase::Ready {
                    Transition {
                        update: Update::None,
                        effects: vec![reply(local, 0)],
                        disposition: Disposition::Applied,
                    }
                } else {
                    Transition::ignored(Disposition::IgnoredPending)
                });
            }
            let body = Arc::new(body);
            Ok(Transition {
                update: Update::Durable(EntityState {
                    body: body.clone(),
                    phase: Phase::Opening,
                    open_token: work.id,
                    retry_attempts: 0,
                    last_retry_error: 0,
                    runtime: None,
                    close_mode: None,
                    close_token: 0,
                    local_deleted: false,
                    cleanup_pending: false,
                    close_failures: 0,
                    registration_failures: 0,
                    close_warning: false,
                    registration_warning: false,
                    fm_stage: FmStage::None,
                    report_sequence: 0,
                }),
                effects: vec![Effect::Open {
                    token: work.id,
                    body,
                }],
                disposition: Disposition::Applied,
            })
        }
        Kind::Complete => {
            let completion: OpenCompletion = from_slice(&work.bytes).map_err(decode_error)?;
            let current = state.ok_or(Failure::Admission("no opening entity"))?;
            if current.phase != Phase::Opening
                || completion.token != current.open_token
                || completion.unit != current.body.unit
                || completion.replica.id != current.body.replica.id
                || completion.replica.instance != current.body.replica.instance
                || completion.replica.node != current.body.replica.node
                || completion.replica.version != current.body.replica.version
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut proposed = current.clone();
            if completion.error == SERVICE_TYPE_NOT_REGISTERED {
                if current.runtime.is_some() {
                    return Err(Failure::Admission(
                        "registration result cannot revoke an active runtime",
                    ));
                }
                proposed.last_retry_error = completion.error;
                return Ok(Transition {
                    update: Update::Memory(proposed),
                    effects: vec![],
                    disposition: Disposition::Applied,
                });
            }
            if completion.error != 0 {
                return Err(Failure::Unsupported(
                    "native retry/terminal-open error policy is staged",
                ));
            }
            if current.runtime.is_none() {
                return Err(Failure::Admission(
                    "Open success requires committed registration handoff",
                ));
            }
            if completion.replica.state != 2
                || !completion.replica.up
                || completion.replica.current_role != 1
            {
                return Err(Failure::Admission(
                    "open success must carry native Ready stateless replica",
                ));
            }
            let mut body = (*proposed.body).clone();
            body.replica = completion.replica;
            proposed.body = Arc::new(body);
            proposed.phase = Phase::Ready;
            Ok(Transition {
                effects: vec![reply(&proposed.body, 0)],
                update: Update::Durable(proposed),
                disposition: Disposition::Applied,
            })
        }
        Kind::Retry => {
            let current = state.ok_or(Failure::Admission("memory-only insert is forbidden"))?;
            if current.phase != Phase::Opening {
                return Err(Failure::Admission("only pending open may retry"));
            }
            let mut proposed = current.clone();
            proposed.retry_attempts = proposed
                .retry_attempts
                .checked_add(1)
                .ok_or(Failure::Capacity)?;
            Ok(Transition {
                effects: vec![if let Some(registration) = &current.runtime {
                    Effect::RuntimeOpen {
                        token: current.open_token,
                        body: current.body.clone(),
                        registration: registration.clone(),
                    }
                } else {
                    Effect::Open {
                        token: current.open_token,
                        body: current.body.clone(),
                    }
                }],
                update: Update::Memory(proposed),
                disposition: Disposition::Applied,
            })
        }
        _ => lifecycle::transition(work, state, admission, settings),
    }
}

#[derive(Serialize)]
struct Stored<'a> {
    format: u32,
    body: &'a AddInstance,
    phase: i64,
    open_token: u64,
    deleted: bool,
    cleanup_pending: bool,
    close_mode: i64,
    close_token: u64,
    fm_stage: i64,
}
async fn persist(
    store: &dyn KeyValueStore,
    shared: &Shared,
    work: &Submission,
    state: &EntityState,
    insert: bool,
) -> Result<()> {
    let value = to_vec(&Stored {
        format: 2,
        body: &state.body,
        phase: match state.phase {
            Phase::Opening => 0,
            Phase::Ready => 1,
            Phase::Closing => 2,
            Phase::Closed => 3,
        },
        open_token: state.open_token,
        deleted: state.local_deleted,
        cleanup_pending: state.cleanup_pending,
        close_mode: state.close_mode.map_or(0, |mode| mode as i64),
        close_token: state.close_token,
        fm_stage: state.fm_stage as i64,
    })
    .map_err(|_| Failure::Capacity)?;
    let record = Record {
        key: RecordId::new(
            "ra/entity/v1",
            work.entity
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        ),
        value,
    };
    // Exactly one submit; uncertainty never resubmits/reopens a conflicting write.
    let operation = match store.submit(if insert {
        Request::Insert(record)
    } else {
        Request::Update(record)
    }) {
        Ok(operation) => operation,
        Err(error) if error.kind == store::ErrorKind::Unknown => {
            lock(&shared.view)
                .entities
                .get_mut(&work.entity)
                .unwrap()
                .fenced = true;
            work.result.send_replace(Status::Unknown);
            return std::future::pending().await;
        }
        Err(error) => return Err(Failure::Store(error)),
    };
    if let Err(error) = operation.wait().await {
        if error.kind != store::ErrorKind::Unknown {
            return Err(Failure::Store(error));
        }
        lock(&shared.view)
            .entities
            .get_mut(&work.entity)
            .unwrap()
            .fenced = true;
        work.result.send_replace(Status::Unknown);
    }
    loop {
        match operation.status() {
            store::Status::Complete(Ok(output)) if *output == Output::Written => return Ok(()),
            store::Status::Complete(Ok(_)) => {
                lock(&shared.view)
                    .entities
                    .get_mut(&work.entity)
                    .unwrap()
                    .fenced = true;
                work.result.send_replace(Status::Unknown);
                // A trusted backend violated the mutation-result contract.
                // A completed read is not proof this write committed or rolled back.
                return std::future::pending().await;
            }
            store::Status::Complete(Err(error)) if error.kind != store::ErrorKind::Unknown => {
                return Err(Failure::Store(error))
            }
            _ => {}
        }
        // Retain the SAME ticket/fence through supervisor timeout or lost result.
        let next_poll = tokio::time::Instant::now() + shared.settings.resolution_poll;
        #[cfg(test)]
        shared
            .resolution_polls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = operation.resolve(shared.settings.resolution_poll).await;
        match operation.status() {
            store::Status::Complete(Ok(_)) => {}
            store::Status::Complete(Err(error)) if error.kind != store::ErrorKind::Unknown => {}
            // Observation loss can leave Pending/Unknown with an already closed
            // channel. Resolve then returns immediately: retain the fence AND
            // honor the polling interval rather than relying on cooperative yield.
            _ => tokio::time::sleep_until(next_poll).await,
        }
    }
}

async fn entity_worker(
    shared: Arc<Shared>,
    mut receiver: mpsc::UnboundedReceiver<Submission>,
    store: Arc<dyn KeyValueStore>,
    admission: Admission,
) {
    let mut committed: Option<Arc<EntityState>> = None;
    while let Some(work) = receiver.recv().await {
        let mut outcome = transition(&work, committed.as_deref(), &admission, &shared.settings);
        if outcome
            .as_ref()
            .is_ok_and(|t| t.effects.len() > work.reserved_effects)
        {
            outcome = Err(Failure::Capacity);
        }
        if let Ok(Transition {
            update: Update::Durable(state),
            ..
        }) = &outcome
        {
            if let Err(error) = persist(&*store, &shared, &work, state, committed.is_none()).await {
                outcome = Err(error);
            }
        }
        let mut view = lock(&shared.view);
        let result = match outcome {
            Ok(transition) => {
                match transition.update {
                    Update::None => {}
                    Update::Memory(state) | Update::Durable(state) => {
                        committed = Some(Arc::new(state));
                        view.entities.get_mut(&work.entity).unwrap().committed = committed.clone();
                    }
                }
                for effect in transition.effects {
                    view.effects.push_back(Dispatched {
                        operation: work.id,
                        entity: work.entity,
                        effect,
                    });
                }
                view.stats.succeeded += 1;
                Ok(transition.disposition)
            }
            Err(error) => {
                view.stats.failed += 1;
                Err(error)
            }
        };
        view.entities.get_mut(&work.entity).unwrap().fenced = false;
        view.stats.outstanding -= 1;
        view.stats.queued_bytes -= work.charge;
        view.reserved_effects -= work.reserved_effects;
        view.stats.effects = view.effects.len();
        drop(view);
        work.result.send_replace(Status::Complete(result));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use store::{Backend, BackendContext, MemoryBackend, Store, StoreFuture};

    struct FaultBackend {
        inner: MemoryBackend,
        fault: Arc<AtomicBool>,
        terminal: bool,
    }
    impl Backend for FaultBackend {
        fn initialize(
            &mut self,
            settings: store::Settings,
            context: BackendContext,
        ) -> StoreFuture<'_, ()> {
            self.inner.initialize(settings, context)
        }
        fn execute(&mut self, request: Request) -> StoreFuture<'_, Output> {
            Box::pin(async move {
                if self.fault.load(Ordering::SeqCst) {
                    if self.terminal {
                        return Err(store::Error::new(
                            store::ErrorKind::Unknown,
                            "terminal uncertainty",
                        ));
                    }
                    panic!("injected loss of observation owner");
                }
                self.inner.execute(request).await
            })
        }
        fn close(&mut self) -> StoreFuture<'_, ()> {
            self.inner.close()
        }
    }
    // Return a REAL public store ticket while dropping its last client on fault.
    // P1 then loses the observation sender on its panic path. No fabricated
    // Operation or private store constructor is used.
    struct ReleasedOwner {
        inner: Mutex<Option<Store>>,
        fault: Arc<AtomicBool>,
        observed: Arc<Mutex<Option<store::Operation>>>,
    }
    impl KeyValueStore for ReleasedOwner {
        fn submit(&self, request: Request) -> store::Result<store::Operation> {
            let mut inner = lock(&self.inner);
            let operation = inner.as_ref().unwrap().submit(request)?;
            if self.fault.load(Ordering::SeqCst) {
                *lock(&self.observed) = Some(operation.clone());
                inner.take();
            }
            Ok(operation)
        }
        fn stats(&self) -> store::Stats {
            store::Stats {
                lifecycle: store::Lifecycle::Fenced,
                outstanding: 0,
                queued_bytes: 0,
            }
        }
        fn close(&self) -> StoreFuture<'_, store::CloseReport> {
            Box::pin(async {
                Err(store::Error::new(
                    store::ErrorKind::Unknown,
                    "owner released",
                ))
            })
        }
    }
    fn body(id: u8, node: &NodeInstance) -> AddInstance {
        let mut body = AddInstance::default();
        body.unit.id.guid.0 = [id; 16];
        body.unit.consistency.id = body.unit.id.clone();
        body.replica.node = node.clone();
        body.replica.id = 1;
        body.replica.instance = 1;
        body.replica.state = 1;
        body.replica.version = body.service.version.clone();
        body
    }

    #[test]
    fn permanent_uncertainty_child() {
        let Ok(mode) = std::env::var("FUBE_RA_UNCERTAINTY_CHILD") else {
            return;
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let fault = Arc::new(AtomicBool::new(false));
            let observed = Arc::new(Mutex::new(None));
            let store = Store::new(
                FaultBackend {
                    inner: MemoryBackend::new(),
                    fault: fault.clone(),
                    terminal: mode == "terminal",
                },
                store::Settings::default(),
            )
            .await
            .unwrap();
            let local = NodeInstance {
                id: [4; 16],
                instance: 1,
            };
            let source = NodeInstance {
                id: [8; 16],
                instance: 1,
            };
            let executor = Executor::new(
                Arc::new(ReleasedOwner {
                    inner: Mutex::new(Some(store)),
                    fault: fault.clone(),
                    observed: observed.clone(),
                }),
                Admission {
                    local_node: local.clone(),
                    fm_source: source.clone(),
                    generation: GenerationNumber::default(),
                    owner_ready: true,
                    node_open: true,
                    node_activated: true,
                    fabric_upgrading: false,
                },
                Settings {
                    resolution_poll: Duration::from_secs(60),
                    ..Settings::default()
                },
            )
            .unwrap();
            let add = |id| Command::Add {
                body: Box::new(body(id, &local)),
                source: source.clone(),
                generation: GenerationHeader::default(),
            };
            let budget = Duration::from_secs(3);
            executor.submit(add(2)).unwrap().wait(budget).await.unwrap();
            executor.take_effects();
            fault.store(true, Ordering::SeqCst);
            let uncertain = executor.submit(add(1)).unwrap();
            assert_eq!(uncertain.wait(budget).await, Err(Failure::Unknown));
            let operation = lock(&observed).as_ref().unwrap().clone();
            let error = operation.resolve(budget).await.unwrap_err();
            if mode == "closed" {
                assert_eq!(error.message, "supervisor lost");
            } else {
                assert!(matches!(operation.status(), store::Status::Complete(Err(e))
                    if e.kind == store::ErrorKind::Unknown));
            }
            tokio::time::timeout(budget, async {
                while executor.shared.resolution_polls.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let conflicting = executor
                .submit(Command::RetryOpen(body(1, &local).unit.id))
                .unwrap();
            let unrelated = executor
                .submit(Command::RetryOpen(body(2, &local).unit.id))
                .unwrap();
            unrelated.wait(budget).await.unwrap();
            assert_eq!(
                executor.close(Duration::from_millis(10)).await,
                Err(Failure::Unknown)
            );
            assert_eq!(uncertain.status(), Status::Unknown);
            assert_eq!(conflicting.status(), Status::Pending);
            let snapshot = executor.snapshot([1; 16]);
            assert!(snapshot.fenced && snapshot.committed.is_none());
            assert_eq!(executor.statistics().outstanding, 2);
            assert!(executor.statistics().queued_bytes > 0);
            let effects = executor.take_effects();
            assert_eq!(effects.len(), 1);
            assert_eq!(effects[0].entity, [2; 16]);
            // Instrumented backoff assertion, not a CPU-usage threshold:
            // both immediate-error shapes have polled once and parked for the
            // configured interval while the unrelated operation completed.
            assert_eq!(executor.shared.resolution_polls.load(Ordering::SeqCst), 1);
            assert!(executor.take_effects().is_empty());
        });
        // Permanent uncertainty is intentionally retained until process exit.
        // This bounded child exits; no unknown owner is replaced or "cleaned".
    }

    #[test]
    fn permanent_observation_loss_and_terminal_unknown_back_off_without_releasing_ownership() {
        for mode in ["closed", "terminal"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "entity::tests::permanent_uncertainty_child",
                    "--nocapture",
                ])
                .env("FUBE_RA_UNCERTAINTY_CHILD", mode)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed;"));
        }
    }
}
