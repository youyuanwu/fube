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
}
enum Kind {
    Add {
        source: NodeInstance,
        generation: GenerationHeader,
    },
    Complete,
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Opening,
    Ready,
}
#[derive(Debug, Clone, PartialEq)]
pub struct EntityState {
    pub body: Arc<AddInstance>,
    pub phase: Phase,
    pub open_token: u64,
    /// Volatile retry bookkeeping; not a native disk record or durable outbox.
    pub retry_attempts: u64,
    pub last_retry_error: i64,
}
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub committed: Option<Arc<EntityState>>,
    pub fenced: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    Open { token: u64, body: Arc<AddInstance> },
    Reply(Box<AddInstanceReply>),
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
            || view.effects.len() + view.reserved_effects >= settings.max_effects
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
        view.reserved_effects += 1; // every supported operation emits at most one action
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
    effect: Option<Effect>,
    disposition: Disposition,
}
impl Transition {
    fn ignored(disposition: Disposition) -> Self {
        Self {
            update: Update::None,
            effect: None,
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
                    effect: Some(reply(&body, RA_NOT_READY)),
                    disposition: Disposition::Applied,
                });
            }
            if let Some(current) = state {
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
                        effect: Some(reply(local, 0)),
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
                }),
                effect: Some(Effect::Open {
                    token: work.id,
                    body,
                }),
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
                proposed.last_retry_error = completion.error;
                return Ok(Transition {
                    update: Update::Memory(proposed),
                    effect: None,
                    disposition: Disposition::Applied,
                });
            }
            if completion.error != 0 {
                return Err(Failure::Unsupported(
                    "native retry/terminal-open error policy is staged",
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
                effect: Some(reply(&proposed.body, 0)),
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
                effect: Some(Effect::Open {
                    token: current.open_token,
                    body: current.body.clone(),
                }),
                update: Update::Memory(proposed),
                disposition: Disposition::Applied,
            })
        }
    }
}

#[derive(Serialize)]
struct Stored<'a> {
    format: u32,
    body: &'a AddInstance,
    phase: i64,
    open_token: u64,
}
async fn persist(
    store: &dyn KeyValueStore,
    shared: &Shared,
    work: &Submission,
    state: &EntityState,
    insert: bool,
) -> Result<()> {
    let value = to_vec(&Stored {
        format: 1,
        body: &state.body,
        phase: if state.phase == Phase::Opening { 0 } else { 1 },
        open_token: state.open_token,
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
        let _ = operation.resolve(shared.settings.resolution_poll).await;
        if matches!(operation.status(), store::Status::Complete(Err(e)) if e.kind == store::ErrorKind::Unknown)
        {
            tokio::time::sleep(shared.settings.resolution_poll).await;
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
        let mut outcome = transition(&work, committed.as_deref(), &admission);
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
                if let Some(effect) = transition.effect {
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
        view.reserved_effects -= 1;
        view.stats.effects = view.effects.len();
        drop(view);
        work.result.send_replace(Status::Complete(result));
    }
}
