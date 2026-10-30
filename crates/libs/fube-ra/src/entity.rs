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
#[path = "notification.rs"]
mod notification;
pub use notification::{
    compare_service_types, native_upper_unit, NotificationClock, ServiceTypeEntry,
};
#[path = "opening.rs"]
mod opening;
pub use opening::*;
#[path = "description.rs"]
mod description;
pub use description::ServiceDescriptionCompletion;
#[path = "config.rs"]
mod config;
pub use config::*;

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
    /// Outstanding acquired-result obligations retained even across recreation.
    pub max_registration_lookups: usize,
    pub max_service_types: usize,
    pub notification_batch_count: usize,
    pub notification_batch_bytes: usize,
    pub notification_node_ms: u64,
    pub notification_type_ms: u64,
    pub notification_retry_ms: u64,
    pub open_retry: RetryPolicy,
    pub open_registration: RetryPolicy,
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
            max_registration_lookups: 64,
            max_service_types: 64,
            notification_batch_count: 100,
            notification_batch_bytes: 256 * 1024,
            notification_node_ms: 5_000,
            notification_type_ms: 45_000,
            notification_retry_ms: 30_000,
            open_retry: RetryPolicy::open(),
            open_registration: RetryPolicy::registration(),
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
            || self.max_registration_lookups == 0
            || self.max_registration_lookups > 65536
            || self.max_service_types == 0
            || self.max_service_types > 65536
            || self.notification_batch_bytes == 0
            || self.notification_batch_bytes > 4 * 1024 * 1024
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
    pub unit: FailoverUnit,
    pub replica: Replica,
    pub error: i64,
    pub flags: i64,
    pub detail: ProxyErrorDetail,
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
    /// Honor pending Add-carried RAP description work; not an FM replacement retry.
    RetryServiceDescription(PartitionId),
    CompleteServiceDescription(Box<ServiceDescriptionCompletion>),
    /// Explicit BGM Request, independent of the periodic retry timer.
    RequestServiceTypeWork,
    /// Honor RequestServiceTypeWork's obligation; capture before simulated send.
    CaptureServiceTypeWork {
        work: u64,
    },
    /// Record the send and complete the same captured pass, even for an empty body.
    CompleteServiceTypeWork {
        work: u64,
    },
    /// Honor the currently armed periodic retry timer.
    ServiceTypeRetryTimer {
        timer: u64,
    },
    /// Native BackgroundWorkManagerWithRetry::SetTimer, not a work request.
    SetServiceTypeRetryTimer,
    ServiceTypeReply {
        body: Box<ServiceTypeNotification>,
        source: NodeInstance,
        generation: GenerationHeader,
    },
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
    RetryServiceDescription,
    CompleteServiceDescription,
    NotificationRequest,
    NotificationCapture,
    NotificationComplete,
    NotificationTimer,
    NotificationSetTimer,
    NotificationReply {
        source: NodeInstance,
        generation: GenerationHeader,
    },
}
impl Kind {
    fn node_work(&self) -> bool {
        matches!(
            self,
            Self::NotificationRequest
                | Self::NotificationCapture
                | Self::NotificationComplete
                | Self::NotificationTimer
                | Self::NotificationSetTimer
                | Self::NotificationReply { .. }
        )
    }
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
    /// Admitted creation package, distinct from Hosting's selected version.
    /// Session-only: no restart/adoption loader is provided.
    pub requested_version: VersionInstance,
    pub phase: Phase,
    pub open_token: u64,
    /// Volatile retry bookkeeping; not a native disk record or durable outbox.
    pub retry_attempts: u64,
    pub open_failures: u64,
    /// Separately owned package usage, retained across opening host crashes.
    pub usage: Option<u64>,
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
    /// Volatile RAP obligation, independent of FM Add reply and cleared by Close.
    pub service_description_pending: bool,
    /// lookup ID -> (owning lifecycle token, close rather than open lookup).
    /// Consumed once by Registered; callback replay acquires no second reference.
    pub registration_lookups: BTreeMap<u64, (u64, bool)>,
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
        lookup: u64,
        body: Arc<AddInstance>,
        /// None requests a new usage lease; Some retains this existing lease.
        usage: Option<u64>,
    },
    Reply(Box<AddInstanceReply>),
    RuntimeOpen {
        token: u64,
        body: Arc<AddInstance>,
        registration: RuntimeRegistration,
        flags: i64,
    },
    Close {
        token: u64,
        body: Arc<AddInstance>,
        registration: RuntimeRegistration,
        flags: i64,
    },
    /// Trusted local RAP adapter obligation, NOT an enabled IPC wire codec.
    /// The captured body remains sendable even if a subsequent Close clears pending.
    UpdateServiceDescription {
        body: Arc<AddInstance>,
        registration: RuntimeRegistration,
    },
    FindRegistration {
        token: u64,
        lookup: u64,
        body: Arc<AddInstance>,
        usage: Option<u64>,
    },
    TerminateHost {
        registration: RuntimeRegistration,
    },
    ReleaseRegistration(RuntimeRegistration),
    Health(HealthEvent),
    RequestReportRetry,
    ReplicaUp(Box<ReplicaUp>),
    RemoveReply(Box<RemoveInstanceReply>),
    /// Zero enqueues immediately; otherwise arm the BGM's remaining-delay timer.
    RequestServiceTypeWork {
        work: u64,
        after_ticks: i64,
    },
    /// This owned old body may still be sent after replacement/removal. Always
    /// complete this work ID once, including when body is None (native count=0).
    ServiceTypeWorkCaptured {
        work: u64,
        body: Option<Box<ServiceTypeNotification>>,
    },
    /// Separate one-shot periodic timer. i64::MAX is native TimeSpan::MaxValue
    /// (disabled); zero/negative configuration due-times have been clamped to 1.
    ArmServiceTypeRetry {
        timer: u64,
        after_ticks: i64,
    },
    CancelServiceTypeRetry {
        timer: u64,
    },
    ReleaseUsage {
        acquisition: u64,
    },
    OpeningHealth(Box<OpeningHealthReport>),
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
    notification: notification::Coordinator,
}
struct Shared {
    view: Mutex<View>,
    settings: Settings,
    config: Arc<NativeConfig>,
    notification_clock: Arc<dyn NotificationClock>,
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
        Self::new_with_clock(store, admission, settings, notification::native_clock())
    }

    pub fn new_with_clock(
        store: Arc<dyn KeyValueStore>,
        admission: Admission,
        settings: Settings,
        notification_clock: Arc<dyn NotificationClock>,
    ) -> Result<Self> {
        settings.validate()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Failure::Admission("cannot create owned executor runtime"))?;
        let (sender, receiver) = mpsc::channel(settings.max_outstanding);
        let (done, closed) = watch::channel(None);
        let shared = Arc::new(Shared {
            config: Arc::new(NativeConfig::new(&settings)),
            notification_clock,
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
            Command::RetryServiceDescription(unit) => {
                (unit.guid.0, Kind::RetryServiceDescription, to_vec(&unit))
            }
            Command::CompleteServiceDescription(body) => (
                body.unit.id.guid.0,
                Kind::CompleteServiceDescription,
                to_vec(&body),
            ),
            Command::RequestServiceTypeWork => ([0; 16], Kind::NotificationRequest, to_vec(&0_u64)),
            Command::CaptureServiceTypeWork { work } => {
                ([0; 16], Kind::NotificationCapture, to_vec(&work))
            }
            Command::CompleteServiceTypeWork { work } => {
                ([0; 16], Kind::NotificationComplete, to_vec(&work))
            }
            Command::ServiceTypeRetryTimer { timer } => {
                ([0; 16], Kind::NotificationTimer, to_vec(&timer))
            }
            Command::SetServiceTypeRetryTimer => {
                ([0; 16], Kind::NotificationSetTimer, to_vec(&0_u64))
            }
            Command::ServiceTypeReply {
                body,
                source,
                generation,
            } => (
                [0; 16],
                Kind::NotificationReply { source, generation },
                to_vec(&body),
            ),
        };
        // Add may report a closed replica then return StaleRequest. Lifecycle
        // commands may release, clear warnings, report health, schedule FM work
        // and reply. Bounds are checked again before any persistence/effects.
        let reserved_effects = match &kind {
            // Ready always owns an active runtime in this executor: Add can
            // capture update + Close (or duplicate reply), but cannot release
            // resources synchronously. Closed Add reports + stale-replies at most.
            Kind::Add { .. } => 2,
            Kind::Retry | Kind::RetryReport | Kind::RetryServiceDescription => 1,
            Kind::CompleteServiceDescription => 0,
            kind if kind.node_work() => 2,
            _ => 8,
        };
        let node_work = kind.node_work();
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
        let new_entity = !node_work && !view.entities.contains_key(&entity);
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
        if !node_work {
            view.entities.entry(entity).or_default();
        }
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
    pub fn config(&self) -> Arc<NativeConfig> {
        self.shared.config.clone()
    }
    pub fn service_types(&self) -> Vec<ServiceTypeEntry> {
        lock(&self.shared.view).notification.snapshot()
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
                if work.kind.node_work() {
                    finish_node(&shared, work, &admission);
                    continue;
                }
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

fn finish_node(shared: &Shared, work: Submission, admission: &Admission) {
    let mut view = lock(&shared.view);
    // Work/byte/effect admission is already reserved. A fallible clock/counter/
    // codec preparation never publishes a partially advanced coordinator.
    let mut notification = view.notification.clone();
    let mut disposition = Disposition::Applied;
    let outcome = if !admission.owner_ready {
        Ok(vec![])
    } else {
        match &work.kind {
            Kind::NotificationRequest => {
                notification.request(&shared.config, &*shared.notification_clock)
            }
            Kind::NotificationSetTimer => notification.set_timer(&shared.config),
            Kind::NotificationCapture | Kind::NotificationComplete | Kind::NotificationTimer => {
                from_slice::<u64>(&work.bytes)
                    .map_err(|_| Failure::Admission("invalid notification obligation"))
                    .and_then(|id| match work.kind {
                        Kind::NotificationCapture => notification.capture(
                            id,
                            &shared.settings,
                            &shared.config,
                            &*shared.notification_clock,
                        ),
                        Kind::NotificationComplete => {
                            notification.complete(id, &shared.config, &*shared.notification_clock)
                        }
                        Kind::NotificationTimer => {
                            notification.timer(id, &shared.config, &*shared.notification_clock)
                        }
                        _ => unreachable!(),
                    })
                    .map(|effects| {
                        effects.unwrap_or_else(|| {
                            disposition = Disposition::IgnoredStale;
                            vec![]
                        })
                    })
            }
            Kind::NotificationReply { source, generation } => {
                if source != &admission.fm_source || generation.for_fm_replica {
                    Err(Failure::Admission(
                        "notification requires configured normal FM",
                    ))
                } else if generation.number.older_than(&admission.generation) {
                    // Generic MessageHandler admission occurs before node routing.
                    view.stats.succeeded += 1;
                    view.stats.outstanding -= 1;
                    view.stats.queued_bytes -= work.charge;
                    view.reserved_effects -= work.reserved_effects;
                    work.result
                        .send_replace(Status::Complete(Ok(Disposition::IgnoredStale)));
                    return;
                } else {
                    from_slice::<ServiceTypeNotification>(&work.bytes)
                        .map_err(|_| Failure::Admission("invalid notification reply"))
                        .map(|body| {
                            notification.acknowledge(&body);
                            vec![]
                        })
                }
            }
            _ => unreachable!(),
        }
    };
    let result = match outcome {
        Ok(effects) => {
            assert!(effects.len() <= work.reserved_effects);
            view.notification = notification;
            for effect in effects {
                view.effects.push_back(Dispatched {
                    operation: work.id,
                    entity: [0; 16],
                    effect,
                });
            }
            view.stats.succeeded += 1;
            Ok(disposition)
        }
        Err(error) => {
            view.stats.failed += 1;
            Err(error)
        }
    };
    view.stats.outstanding -= 1;
    view.stats.queued_bytes -= work.charge;
    view.reserved_effects -= work.reserved_effects;
    view.stats.effects = view.effects.len();
    drop(view);
    work.result.send_replace(Status::Complete(result));
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
    config: &NativeConfig,
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
            if generation.number.older_than(&admission.generation) {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            if !admission.node_open || !admission.owner_ready {
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
            // Native locked-entity TCP admission precedes processor prechecks,
            // description updates and identity decisions. No receipt-time latch.
            if let Some(current) = state {
                if matches!(current.phase, Phase::Opening | Phase::Closing) {
                    return Ok(Transition::ignored(Disposition::IgnoredPending));
                }
                if !(current.phase == Phase::Closed && current.local_deleted)
                    && body.unit.current.older_than(current.body.unit.current)
                {
                    return Ok(Transition::ignored(Disposition::IgnoredStale));
                }
                if current.phase != Phase::Closed {
                    if body.replica.instance < current.body.replica.instance {
                        return Ok(Transition::ignored(Disposition::IgnoredStale));
                    }
                    if body.replica.id == current.body.replica.id
                        && body.replica.instance > current.body.replica.instance
                    {
                        return Err(Failure::Admission(
                            "native impossible newer replica instance",
                        ));
                    }
                }
            }
            if admission.fabric_upgrading {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            if !admission.node_activated {
                return Ok(Transition {
                    update: Update::None,
                    effects: vec![reply(&body, RA_NOT_READY)],
                    disposition: Disposition::Applied,
                });
            }
            if let Some(current) = state {
                return lifecycle::add_existing(work, current, &body);
            }
            let body = Arc::new(lifecycle::create_body(body));
            Ok(Transition {
                update: Update::Durable(EntityState {
                    body: body.clone(),
                    requested_version: body.service.version.clone(),
                    phase: Phase::Opening,
                    open_token: work.id,
                    retry_attempts: 0,
                    open_failures: 0,
                    usage: None,
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
                    service_description_pending: false,
                    registration_lookups: BTreeMap::new(),
                }),
                effects: vec![Effect::Open {
                    token: work.id,
                    lookup: 0, // assigned atomically by track_lookups before persistence
                    body,
                    usage: None,
                }],
                disposition: Disposition::Applied,
            })
        }
        Kind::Complete => opening::complete(work, state, config),
        Kind::CompleteServiceDescription | Kind::RetryServiceDescription => {
            description::transition(work, state, admission)
        }
        Kind::Retry => {
            if !admission.owner_ready {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            let current = state.ok_or(Failure::Admission("memory-only insert is forbidden"))?;
            if current.phase != Phase::Opening {
                return Ok(Transition::ignored(Disposition::IgnoredPending));
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
                        flags: config
                            .policy(RetryState::Opening)
                            .flags(current.open_failures),
                    }
                } else {
                    Effect::Open {
                        token: current.open_token,
                        lookup: 0,
                        body: current.body.clone(),
                        usage: current.usage,
                    }
                }],
                update: Update::Memory(proposed),
                disposition: Disposition::Applied,
            })
        }
        _ => lifecycle::transition(work, state, admission, config),
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

/// Bind every issued lookup to a unique accepted operation. Tracking is private
/// with the same proposal, survives removal/recreation, and is logically bounded.
fn track_lookups(
    work: &Submission,
    current: Option<&EntityState>,
    transition: &mut Transition,
    settings: &Settings,
) -> Result<()> {
    let mut purpose = None;
    for effect in &mut transition.effects {
        let (token, lookup, close) = match effect {
            Effect::Open { token, lookup, .. } => (*token, lookup, false),
            Effect::FindRegistration { token, lookup, .. } => (*token, lookup, true),
            _ => continue,
        };
        if purpose.replace((token, close)).is_some() {
            return Err(Failure::Capacity);
        }
        *lookup = work.id;
    }
    let Some(purpose) = purpose else {
        return Ok(());
    };
    if matches!(transition.update, Update::None) {
        transition.update = Update::Memory(
            current
                .ok_or(Failure::Admission("lookup needs an entity"))?
                .clone(),
        );
    }
    let (Update::Memory(state) | Update::Durable(state)) = &mut transition.update else {
        unreachable!()
    };
    if state.registration_lookups.len() >= settings.max_registration_lookups {
        return Err(Failure::Capacity);
    }
    state.registration_lookups.insert(work.id, purpose);
    Ok(())
}

async fn entity_worker(
    shared: Arc<Shared>,
    mut receiver: mpsc::UnboundedReceiver<Submission>,
    store: Arc<dyn KeyValueStore>,
    admission: Admission,
) {
    let mut committed: Option<Arc<EntityState>> = None;
    while let Some(work) = receiver.recv().await {
        let mut outcome = transition(&work, committed.as_deref(), &admission, &shared.config);
        if let Ok(transition) = &mut outcome {
            if let Err(error) =
                track_lookups(&work, committed.as_deref(), transition, &shared.settings)
            {
                outcome = Err(error);
            }
        }
        if outcome
            .as_ref()
            .is_ok_and(|t| t.effects.len() > work.reserved_effects)
        {
            outcome = Err(Failure::Capacity);
        }
        // All enabled attachments are memory-only. They project the ACTUAL
        // retained map under the publication lock below, without any await.
        // A future durable attach must introduce an equivalent precommit
        // reservation; never discover a fallible capacity check after writing.
        if let Ok(Transition {
            update: Update::Durable(state),
            ..
        }) = &outcome
        {
            if state.runtime.is_some()
                && committed.as_ref().and_then(|s| s.runtime.as_ref()) != state.runtime.as_ref()
            {
                outcome = Err(Failure::Unsupported(
                    "durable registration attachment requires precommit reservation",
                ));
            }
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
        let mut prepared_notification = None;
        if let Ok(Transition {
            update: Update::Memory(state),
            effects,
            ..
        }) = &mut outcome
        {
            let before = committed.as_ref().and_then(|s| s.runtime.as_ref());
            if before != state.runtime.as_ref() && state.runtime.is_some() {
                let mut proposed = view.notification.clone();
                if let Some(before) = before {
                    proposed.remove(&before.service_type);
                }
                match proposed.add(
                    notification::info(state),
                    &shared.settings,
                    &shared.config,
                    &*shared.notification_clock,
                ) {
                    Ok(mut requests) if effects.len() + requests.len() <= work.reserved_effects => {
                        requests.append(effects);
                        *effects = requests;
                        prepared_notification = Some(proposed);
                    }
                    Ok(_) => outcome = Err(Failure::Capacity),
                    Err(error) => outcome = Err(error),
                }
            }
        }
        let result = match outcome {
            Ok(transition) => {
                let effects = transition.effects;
                match transition.update {
                    Update::None => {}
                    Update::Memory(state) | Update::Durable(state) => {
                        let before = committed.as_ref().and_then(|s| s.runtime.as_ref());
                        if before != state.runtime.as_ref() {
                            if let Some(proposed) = prepared_notification.take() {
                                view.notification = proposed;
                            } else if let Some(before) = before {
                                view.notification.remove(&before.service_type);
                            }
                        }
                        assert!(effects.len() <= work.reserved_effects);
                        committed = Some(Arc::new(state));
                        view.entities.get_mut(&work.entity).unwrap().committed = committed.clone();
                    }
                }
                for effect in effects {
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
    fn pending_add_is_checked_before_processor_deactivation_and_upgrade() {
        // Native MessageContext.h:109–124; entity traits:166–178; RA:4313–4342.
        // Pure transition fixture permits changing node context between jobs;
        // the public executor's configured admission remains immutable.
        let mut admission = Admission {
            local_node: NodeInstance::default(),
            fm_source: NodeInstance::default(),
            generation: GenerationNumber::default(),
            owner_ready: true,
            node_open: true,
            node_activated: true,
            fabric_upgrading: false,
        };
        let config = NativeConfig::new(&Settings::default());
        let request = body(1, &admission.local_node);
        let (result, _) = watch::channel(Status::Pending);
        let mut work = Submission {
            id: 1,
            entity: [1; 16],
            kind: Kind::Add {
                source: admission.fm_source.clone(),
                generation: GenerationHeader::default(),
            },
            bytes: to_vec(&request).unwrap().into_boxed_slice(),
            charge: 0,
            reserved_effects: 2,
            result,
        };
        let Update::Durable(mut state) =
            transition(&work, None, &admission, &config).unwrap().update
        else {
            panic!("creation");
        };
        let mut changed = request;
        changed.replica.id += 1;
        changed.replica.instance = 0;
        changed.unit.current.configuration = -1;
        changed.service.update_version += 1;
        work.bytes = to_vec(&changed).unwrap().into_boxed_slice();
        for phase in [Phase::Opening, Phase::Closing] {
            state.phase = phase;
            for upgrading in [false, true] {
                admission.node_activated = false;
                admission.fabric_upgrading = upgrading;
                let result = transition(&work, Some(&state), &admission, &config).unwrap();
                assert_eq!(result.disposition, Disposition::IgnoredPending);
                assert!(matches!(result.update, Update::None));
                assert!(result.effects.is_empty());
            }
        }
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
