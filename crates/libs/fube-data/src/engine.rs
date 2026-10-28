// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Source-guided: Service Fabric Reliability/Replication/{ReplicaManager,
// SecondaryReplicationReceiver,ReplicationSession,CopySender,OperationQueue}
// at 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.

//! Owned volatile, explicit-service-ACK classic replication.
//!
//! Only the native [`Replicator`] controls select roles and membership. A
//! listener, a role, a receipt, and an application ACK are different facts.
//! Loopback tests of this module are not native interoperability qualification.
//!
//! The primary service owns its local mutation and must apply an admitted
//! sequence before publishing its result. Like native classic replication, the
//! primary's local membership vote is not a secondary-stream ACK. Partition
//! lifetime/access checks remain the application's independent responsibility.
//!
//! One joined owner thread supervises one epoch/data-loss callback, at most 64
//! copy enumerations, and at most 64 operation captures across both streams.
//! Replication, pending writes and incoming/outgoing copy share one finite data
//! budget (1024 operations / 16 MiB by default, including application captures).
//! Until incoming copy completes, other data admission leaves one operation and
//! the largest budget-fitting copy item free. Small budgets can therefore reject
//! all staged replication; the sender must retry after copy. Primary writes also
//! leave that byte headroom for future outgoing copy when it is smaller than the
//! byte budget, and one operation while enumeration is unfinished. Build admission
//! atomically checks that existing ownership (including queued submits) leaves a
//! full enumeration slot, or returns ResourceLimit before installing the build.
//! When the copy reservation fills the entire byte budget, writes are allowed
//! without a build, but rejected before LSN assignment during unfinished copy;
//! occupied-budget builds must be retried after ownership drains, not left stuck.
//! Control admission and the transport's bounded frame budget are separate.
//! Callback futures are aborted and actually
//! joined before transport disposal. Uncooperative synchronous user code or
//! destructors are enrolled in the runtime's independent fatal watchdog by
//! [`Engine::bind_partition`]. Standalone [`Engine::bind`] callers must supply
//! their own blocking fence; a Tokio timeout cannot fence a blocked executor.
//! This is the volatile legacy-null-EOS, trusted clear-TCP profile, not durable
//! storage, persisted copy context, an election system, or a native qualification.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc as std_mpsc, Arc, Mutex, Weak,
    },
    thread::{self, JoinHandle},
};

use mssf_runtime::{
    CatchUpMode, ConfigurationStage, ReplicaInformation, ReplicaLifecycle, ReplicaRemoval,
    ReplicaSetConfiguration, ReplicaStatus, ReplicationProgress, Replicator, ServiceFuture,
};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use tokio::{
    sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{interval, timeout, Instant, MissedTickBehavior},
};

use crate::{
    queue::OperationQueue,
    quorum::{configuration_progress, ConfigurationProgress, PeerProgress},
    transport::{Incoming, PeerTransport},
    wire::{Actor, Body, Endpoint, Packet, WireOperation},
    Acknowledgment, Cancellation, Epoch, Error, Operation, OperationData, OperationDataStream,
    OperationStream, Progress, ReplicaRole, Result, Settings, StateProvider, StatefulPartition,
};

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;

/// Diagnostic boundaries, never an independent grant of native write access.
#[derive(Clone, Debug)]
pub struct EngineSnapshot {
    pub role: ReplicaRole,
    pub epoch: Option<Epoch>,
    pub progress: Progress,
    pub copy_done: bool,
    pub ready: bool,
    pub closed: bool,
    pub fault: Option<String>,
    pub provider_callbacks: usize,
    pub dispatched_unacknowledged: usize,
    pub operation_captures: usize,
    pub owned_operations: usize,
    pub owned_bytes: usize,
    pub peers: Vec<PeerSnapshot>,
    pub observed_primary: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PeerSnapshot {
    pub replica: i64,
    pub incarnation: String,
    pub address: String,
    pub received: i64,
    pub applied: i64,
    pub maximum_sent: i64,
    pub copy_received: i64,
    pub copy_applied: i64,
    pub copy_horizon: Option<i64>,
    pub copy_finish: Option<i64>,
    pub configured: bool,
    pub building: bool,
    pub cleanup_required: bool,
}

impl Serialize for EngineSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_struct("EngineSnapshot", 14)?;
        out.serialize_field("role", &format!("{:?}", self.role))?;
        out.serialize_field(
            "epoch",
            &self.epoch.map(
                |e| serde_json::json!({"data_loss": e.data_loss, "configuration": e.configuration}),
            ),
        )?;
        out.serialize_field("progress", &self.progress)?;
        out.serialize_field("copy_done", &self.copy_done)?;
        out.serialize_field("ready", &self.ready)?;
        out.serialize_field("closed", &self.closed)?;
        out.serialize_field("fault", &self.fault)?;
        out.serialize_field("provider_callbacks", &self.provider_callbacks)?;
        out.serialize_field("dispatched_unacknowledged", &self.dispatched_unacknowledged)?;
        out.serialize_field("operation_captures", &self.operation_captures)?;
        out.serialize_field("owned_operations", &self.owned_operations)?;
        out.serialize_field("owned_bytes", &self.owned_bytes)?;
        out.serialize_field("peers", &self.peers)?;
        out.serialize_field("observed_primary", &self.observed_primary)?;
        out.end()
    }
}

/// An admitted write. Dropping this handle cannot undo admission.
pub struct Replication {
    pub sequence: i64,
    completion: watch::Receiver<Option<Result<i64>>>,
}

impl Replication {
    /// Canceling this wait is ambiguous: the admitted write remains owned.
    pub async fn wait(&mut self, cancellation: Cancellation) -> Result<i64> {
        loop {
            if let Some(result) = self.completion.borrow_and_update().clone() {
                return result;
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::Closed),
                result = self.completion.changed() => result.map_err(|_| Error::Closed)?,
            }
        }
    }
}

struct Budget {
    operations: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

struct Charge {
    _operation: OwnedSemaphorePermit,
    bytes: OwnedSemaphorePermit,
}

impl Budget {
    fn acquire(&self, bytes: usize) -> Result<Charge> {
        self.acquire_reserved(bytes, 0, 0)
    }

    fn acquire_data(&self, bytes: usize, settings: &Settings, copying: bool) -> Result<Charge> {
        if copying {
            self.acquire_reserved(bytes, 1, maximum_copy_bytes(settings))
        } else {
            self.acquire(bytes)
        }
    }

    fn acquire_reserved(
        &self,
        bytes: usize,
        reserved_operations: u32,
        reserved_bytes: usize,
    ) -> Result<Charge> {
        // Acquire the payload AND its headroom before releasing the headroom.
        // Checking available_permits first would race concurrent admissions.
        let mut operations = self
            .operations
            .clone()
            .try_acquire_many_owned(1 + reserved_operations)
            .map_err(|_| Error::ResourceLimit("engine operation budget"))?;
        let mut byte_permits = self
            .bytes
            .clone()
            .try_acquire_many_owned(
                bytes
                    .checked_add(reserved_bytes)
                    .ok_or(Error::ResourceLimit("engine byte budget"))?
                    .try_into()
                    .map_err(|_| Error::ResourceLimit("engine byte budget"))?,
            )
            .map_err(|_| Error::ResourceLimit("engine byte budget"))?;
        Ok(Charge {
            _operation: operations.split(1).unwrap(),
            bytes: byte_permits.split(bytes).unwrap(),
        })
    }
}

fn maximum_copy_bytes(settings: &Settings) -> usize {
    (settings.operation_bytes
        + settings.operation_segments * std::mem::size_of::<Arc<[u8]>>()
        + 128)
        .min(settings.queue_bytes)
}

struct Shared {
    admission: Mutex<()>,
    stopped: AtomicBool,
    revision: AtomicU64,
    snapshot: Mutex<EngineSnapshot>,
    budget: Budget,
    callbacks: Arc<Semaphore>,
}

trait NativeControlView: Send + Sync {
    fn status(&self) -> Result<ReplicaStatus>;
    fn report_fault(
        &self,
        fault: mssf_runtime::FaultType,
    ) -> ServiceFuture<'_, mssf_runtime::FaultDelivery>;
}

impl NativeControlView for StatefulPartition {
    fn status(&self) -> Result<ReplicaStatus> {
        StatefulPartition::status(self)
    }
    fn report_fault(
        &self,
        fault: mssf_runtime::FaultType,
    ) -> ServiceFuture<'_, mssf_runtime::FaultDelivery> {
        Box::pin(StatefulPartition::report_fault(self, fault))
    }
}

#[derive(Clone, Copy)]
struct NativePreWrite {
    current: Epoch,
    previous: Epoch,
}

impl NativePreWrite {
    fn from_status(status: ReplicaStatus) -> Option<Self> {
        (status.lifecycle == ReplicaLifecycle::Open
            && status.replicator_open
            && status.current_role == ReplicaRole::Secondary
            && status.service_role == ReplicaRole::Primary
            && status.replicator_role == ReplicaRole::Primary
            && status.configuration_stage == ConfigurationStage::PreWriteRevoke)
            .then_some(Self {
                current: status.current_epoch.primary(),
                previous: status.previous_epoch.primary(),
            })
    }

    fn matches(
        self,
        current: &ReplicaSetConfiguration,
        previous: Option<&ReplicaSetConfiguration>,
    ) -> bool {
        self.current == current.epoch.primary()
            && previous
                .filter(|pc| pc.write_quorum != 0)
                .is_none_or(|pc| self.previous == pc.epoch.primary())
    }
}

/// The last engine owner synchronously stops admission, drains buffered service
/// operations, joins callbacks/the engine thread, then joins peer transport.
/// Captures already published to application code stay charged until dropped.
pub struct Engine {
    endpoint: Endpoint,
    settings: Settings,
    commands: mpsc::Sender<Command>,
    writes: mpsc::Sender<Submit>,
    stop: watch::Sender<bool>,
    shared: Arc<Shared>,
    replication: Arc<OperationStream>,
    copy: Arc<OperationStream>,
    thread: Mutex<Option<JoinHandle<()>>>,
    transport: PeerTransport,
    native_control: Option<Arc<dyn NativeControlView>>,
}

impl Engine {
    /// Bind an SF-activated replica and enroll every provider callback,
    /// enumeration and owned capture disposal in the runtime's existing fence.
    /// Qualified activated applications must use this entry point.
    ///
    /// Call from the runtime-owned service Open callback: the enclosing runtime
    /// guard also supervises rejected argument destruction/initialization errors.
    /// Accounting does not grant application access or authorize late results.
    /// The partition's native status also authorizes pre-write-revoke membership
    /// metadata ahead of UpdateEpoch, never an engine epoch or access change.
    /// Terminal engine faults report a native transient fault through this same
    /// generation; normal Close/Abort do not. Delivery is not a replacement ack.
    pub fn bind_partition(
        partition: StatefulPartition,
        actor: Actor,
        address: &str,
        provider: Arc<dyn StateProvider>,
        settings: Settings,
    ) -> Result<Arc<Self>> {
        settings.validate()?;
        let provider = Arc::new(crate::supervised::SupervisedProvider::new(
            partition.clone(),
            settings.callback_timeout,
            provider,
        ));
        partition.ensure_valid()?;
        if actor.partition != partition.identity().partition_id
            || actor.replica != partition.identity().replica_id
        {
            return Err(Error::InvalidInput(
                "replication actor does not match native partition",
            ));
        }
        Self::bind_with_native_control(
            actor,
            address,
            provider,
            settings,
            Some(Arc::new(partition)),
        )
    }

    /// Standalone binding for Rust source fixtures or use outside an SF host.
    /// The caller is responsible for a blocking callback/destructor fence:
    /// Tokio deadlines alone cannot supervise synchronous provider code.
    /// SF-activated qualification must instead use [`Self::bind_partition`].
    pub fn bind(
        actor: Actor,
        address: &str,
        provider: Arc<dyn StateProvider>,
        settings: Settings,
    ) -> Result<Arc<Self>> {
        Self::bind_with_native_control(actor, address, provider, settings, None)
    }

    fn bind_with_native_control(
        actor: Actor,
        address: &str,
        provider: Arc<dyn StateProvider>,
        settings: Settings,
        native_control: Option<Arc<dyn NativeControlView>>,
    ) -> Result<Arc<Self>> {
        settings.validate()?;
        let (transport, incoming) = PeerTransport::bind(address, settings.clone())?;
        let endpoint = Endpoint {
            actor,
            address: transport.address().to_owned(),
            // EndOpen's public address always includes the transport suffix,
            // even when empty. Native ReplicaInformation.cpp requires the
            // second ';' before splitting transport ID from the actor endpoint.
            transport_id: Some(String::new()),
        };
        endpoint.to_string().parse::<Endpoint>()?;
        let (commands, command_rx) = mpsc::channel(settings.control_messages);
        let (writes, write_rx) = mpsc::channel(settings.queue_operations);
        let (ack_tx, ack_rx) = mpsc::channel(settings.provider_operations);
        let (replication_tx, replication_rx) = mpsc::channel(settings.provider_operations);
        let (copy_tx, copy_rx) = mpsc::channel(settings.provider_operations);
        let replication = Arc::new(OperationStream::new(replication_rx));
        let copy = Arc::new(OperationStream::new(copy_rx));
        let (stop, stop_rx) = watch::channel(false);
        let shared = Arc::new(Shared {
            admission: Mutex::new(()),
            stopped: AtomicBool::new(false),
            revision: AtomicU64::new(1),
            snapshot: Mutex::new(EngineSnapshot {
                role: ReplicaRole::Unknown,
                epoch: None,
                progress: Progress::default(),
                copy_done: false,
                ready: false,
                closed: false,
                fault: None,
                provider_callbacks: 0,
                dispatched_unacknowledged: 0,
                operation_captures: 0,
                owned_operations: 0,
                owned_bytes: 0,
                peers: Vec::new(),
                observed_primary: None,
            }),
            budget: Budget {
                operations: Arc::new(Semaphore::new(settings.queue_operations)),
                bytes: Arc::new(Semaphore::new(settings.queue_bytes)),
            },
            callbacks: Arc::new(Semaphore::new(settings.provider_operations)),
        });
        let state_shared = shared.clone();
        let state_endpoint = endpoint.clone();
        let state_transport = transport.clone();
        let state_settings = settings.clone();
        let state_replication = replication.clone();
        let state_copy = copy.clone();
        let state_native_control = native_control.clone();
        let (ready, readiness) = std_mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("fube-engine".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(Error::Io(error)));
                        return;
                    }
                };
                runtime.block_on(async move {
                    // Spawn also supervises a synchronous progress callback panic.
                    let initial_provider = provider.clone();
                    let mut initial =
                        tokio::spawn(async move { initial_provider.last_committed() });
                    let initial = match timeout(state_settings.callback_timeout, &mut initial).await
                    {
                        Ok(Ok(result)) => result,
                        Ok(Err(_)) => Err(Error::Protocol("provider progress panicked")),
                        Err(_) => {
                            initial.abort();
                            let _ = initial.await;
                            Err(Error::Timeout)
                        }
                    };
                    let initial = match initial.and_then(|n| {
                        OperationQueue::new(state_settings.clone(), n).map(|queue| (n, queue))
                    }) {
                        Ok(initial) => initial,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                    let mut owner = Owner::new(
                        state_endpoint,
                        state_transport,
                        provider,
                        state_settings,
                        state_shared,
                        initial.1,
                        ack_tx,
                        replication_tx,
                        copy_tx,
                        state_native_control,
                    );
                    owner.publish();
                    if ready.send(Ok(())).is_ok() {
                        owner
                            .run(incoming, command_rx, write_rx, ack_rx, stop_rx)
                            .await;
                    }
                    state_replication.shutdown();
                    state_copy.shutdown();
                });
            })?;
        if let Err(error) = readiness.recv().unwrap_or(Err(Error::Closed)) {
            let _ = thread.join();
            transport.shutdown();
            return Err(error);
        }
        Ok(Arc::new(Self {
            endpoint,
            settings,
            commands,
            writes,
            stop,
            shared,
            replication,
            copy,
            thread: Mutex::new(Some(thread)),
            transport,
            native_control,
        }))
    }

    fn native_prewrite(&self) -> Result<Option<NativePreWrite>> {
        // Capture the runtime-owned view before entering the owner queue and
        // without holding engine locks (the runtime has its own status lock).
        Ok(self
            .native_control
            .as_ref()
            .map(|view| view.status())
            .transpose()?
            .and_then(NativePreWrite::from_status))
    }

    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    /// Bounded transport observations for the qualification fixture, not authority.
    #[cfg(feature = "qualification")]
    pub fn qualification_transport_snapshot(&self) -> serde_json::Value {
        self.transport.diagnostics()
    }

    pub fn replication_stream(&self) -> Arc<OperationStream> {
        self.replication.clone()
    }

    pub fn copy_stream(&self) -> Arc<OperationStream> {
        self.copy.clone()
    }

    pub fn snapshot(&self) -> EngineSnapshot {
        let mut snapshot = self.shared.snapshot.lock().unwrap().clone();
        snapshot.closed |= self.shared.stopped.load(Ordering::Acquire);
        snapshot.ready &= !snapshot.closed;
        snapshot.operation_captures =
            self.settings.provider_operations - self.shared.callbacks.available_permits();
        snapshot.owned_operations =
            self.settings.queue_operations - self.shared.budget.operations.available_permits();
        snapshot.owned_bytes =
            self.settings.queue_bytes - self.shared.budget.bytes.available_permits();
        snapshot
    }

    /// Test-fixture-only invalid/obsolete peer traffic on existing connections.
    /// Destinations are restricted to native-selected current engine sessions;
    /// this never changes membership, epochs, roles, queues or progress.
    #[cfg(feature = "qualification")]
    pub fn qualification_probe(
        &self,
        kind: &str,
        target: Option<i64>,
    ) -> ServiceFuture<'_, serde_json::Value> {
        let kind = kind.to_owned();
        Box::pin(async move {
            if kind.len() > 32 {
                return Err(Error::InvalidInput("probe kind bound"));
            }
            self.request(|reply| Command::Probe {
                kind,
                target,
                reply,
            })
            .await
        })
    }

    /// Admit without conflating acceptance with quorum completion. The primary
    /// service applies the returned sequence locally before waiting/publishing.
    /// ResourceLimit, including outgoing-copy headroom, rejects before assigning
    /// an LSN. A queued submit is already charged when a build checks admission.
    pub fn submit(
        &self,
        data: OperationData,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Replication> {
        Box::pin(async move {
            self.check()?;
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            if data.bytes() > self.settings.operation_bytes
                || data.segments().len() > self.settings.operation_segments
            {
                return Err(Error::ResourceLimit("operation limits"));
            }
            let (reply, result) = oneshot::channel();
            {
                // Serialize reservation changes and shutdown with producers,
                // but never hold admission while awaiting owner/user work.
                let _admission = self.shared.admission.lock().unwrap();
                self.check()?;
                let snapshot = self.shared.snapshot.lock().unwrap();
                let copying = !snapshot.copy_done
                    || snapshot.peers.iter().any(|peer| {
                        peer.copy_horizon.is_some()
                            && peer.copy_finish.is_none()
                            && !peer.cleanup_required
                    });
                let maximum = maximum_copy_bytes(&self.settings);
                let reserve_bytes = if copying
                    || (snapshot.role == ReplicaRole::Primary
                        && maximum < self.settings.queue_bytes)
                {
                    maximum
                } else {
                    0
                };
                let charge = self.shared.budget.acquire_reserved(
                    data.retained_bytes(),
                    u32::from(copying),
                    reserve_bytes,
                )?;
                drop(snapshot);
                self.writes
                    .try_send(Submit {
                        data,
                        charge,
                        cancellation,
                        reply,
                    })
                    .map_err(|error| match error {
                        mpsc::error::TrySendError::Full(_) => {
                            Error::ResourceLimit("write admission")
                        }
                        mpsc::error::TrySendError::Closed(_) => Error::Closed,
                    })?;
            }
            // Once queued, learn whether admission happened before reporting
            // cancellation. The separately cancelable wait is intentionally later.
            result.await.map_err(|_| Error::Closed)?
        })
    }

    fn check(&self) -> Result<()> {
        if self.shared.stopped.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    async fn request<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T>>) -> Command,
    ) -> Result<T> {
        self.check()?;
        let (reply, result) = oneshot::channel();
        let command = make(reply);
        #[cfg(feature = "qualification")]
        let kind = std::mem::discriminant(&command);
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::ResourceLimit("control admission"),
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        let result = result.await.map_err(|_| Error::Closed)?;
        #[cfg(feature = "qualification")]
        if let Err(error) = &result {
            eprintln!("FUBE_CONTROL_ERROR {kind:?}: {error}");
        }
        result
    }

    fn shutdown(&self) {
        let admission = self
            .shared
            .admission
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.revision.fetch_add(1, Ordering::AcqRel);
        self.stop.send_replace(true);
        drop(admission);
        self.replication.shutdown();
        self.copy.shutdown();
        let mut thread = self.thread.lock().unwrap();
        if let Some(thread) = thread.take() {
            let _ = thread.join();
        }
        self.transport.shutdown();
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;

fn same_endpoint(left: &Endpoint, right: &Endpoint) -> bool {
    left.actor == right.actor && left.address == right.address
}

struct Submit {
    data: OperationData,
    charge: Charge,
    cancellation: Cancellation,
    reply: Reply<Replication>,
}

enum Command {
    #[cfg(feature = "qualification")]
    Probe {
        kind: String,
        target: Option<i64>,
        reply: Reply<serde_json::Value>,
    },
    Epoch {
        epoch: Epoch,
        role: Option<ReplicaRole>,
        cancellation: Cancellation,
        reply: Reply<()>,
    },
    Loss(Cancellation, Reply<bool>),
    Configure {
        current: ReplicaSetConfiguration,
        previous: Option<ReplicaSetConfiguration>,
        native_prewrite: Option<NativePreWrite>,
        cancellation: Cancellation,
        reply: Reply<()>,
    },
    CatchUp(CatchUpMode, Cancellation, Reply<()>),
    Build(ReplicaInformation, Cancellation, Reply<()>),
    Remove(ReplicaInformation, ReplicaRemoval, Cancellation, Reply<()>),
}

#[derive(Clone, Copy)]
struct ApplyAck {
    revision: u64,
    copy: bool,
    sequence: i64,
}

struct SentCopy {
    data: OperationData,
    last: bool,
    _charge: Charge,
}

struct Build {
    epoch: Epoch,
    horizon: i64,
    start_sent: bool,
    established: bool,
    stream: Option<Arc<dyn OperationDataStream>>,
    job: bool,
    next: i64,
    pending: BTreeMap<i64, SentCopy>,
    eos: Option<i64>,
    finish: Option<i64>,
    cancellation: Cancellation,
    reply: Option<Reply<()>>,
}

struct Peer {
    endpoint: Endpoint,
    native_version: i64,
    session: u64,
    configured: bool,
    received: i64,
    applied: i64,
    maximum_sent: i64,
    receipt_seen: bool,
    copy_received: i64,
    copy_applied: i64,
    maximum_copy_sent: i64,
    cleanup_required: bool,
    build: Option<Build>,
}

struct SecondaryCopy {
    primary: Endpoint,
    primary_epoch: Epoch,
    copy_epoch: Epoch,
    horizon: i64,
    queue: OperationQueue,
    charges: BTreeMap<i64, Arc<Charge>>,
    dispatched: i64,
    eos: Option<i64>,
}

enum ControlKind {
    Epoch {
        epoch: Epoch,
        role: ReplicaRole,
        boundary: i64,
        reply: Reply<()>,
    },
    Barrier {
        epoch: Epoch,
        boundary: i64,
    },
    Loss(Reply<bool>),
}

struct Control {
    kind: ControlKind,
    cancellation: Cancellation,
    running: bool,
    started: Instant,
}

#[derive(Clone, Copy)]
enum Job {
    Control,
    CopyOpen(Actor, u64),
    CopyNext(Actor, u64),
}

enum JobValue {
    Epoch(i64),
    Loss(bool, Option<i64>),
    CopyOpen(Arc<dyn OperationDataStream>),
    CopyNext(Option<OperationData>, Charge),
}

enum Event {
    Ack(ApplyAck),
    Command(Command),
    Job(Option<std::result::Result<(Job, Result<JobValue>), tokio::task::JoinError>>),
    Packet(Packet),
    Write(Submit),
    Retry,
}

struct CatchUp {
    mode: CatchUpMode,
    cancellation: Cancellation,
    reply: Reply<()>,
}

struct Removal {
    replica: ReplicaInformation,
    reason: ReplicaRemoval,
    cancellation: Cancellation,
    reply: Reply<()>,
}

struct Owner {
    endpoint: Endpoint,
    transport: PeerTransport,
    provider: Arc<dyn StateProvider>,
    settings: Settings,
    shared: Arc<Shared>,
    queue: OperationQueue,
    charges: BTreeMap<i64, Arc<Charge>>,
    completions: BTreeMap<i64, watch::Sender<Option<Result<i64>>>>,
    role: ReplicaRole,
    epoch: Option<Epoch>,
    provider_epoch: Option<Epoch>,
    copy_done: bool,
    reconciled: bool,
    current: Option<ReplicaSetConfiguration>,
    previous: Option<ReplicaSetConfiguration>,
    previous_quorum: i64,
    catch_up_fence: i64,
    peers: HashMap<i64, Peer>,
    tombstones: HashSet<(Actor, i64)>,
    session: u64,
    secondary: Option<SecondaryCopy>,
    dispatched: i64,
    announced_completed: i64,
    apply_deadlines: BTreeMap<(bool, i64), Instant>,
    dispatch_permits: BTreeMap<(bool, i64), Arc<OwnedSemaphorePermit>>,
    control: Option<Control>,
    catchups: Vec<CatchUp>,
    removals: Vec<Removal>,
    jobs: JoinSet<(Job, Result<JobValue>)>,
    ack_tx: mpsc::Sender<ApplyAck>,
    replication_tx: Option<mpsc::Sender<Operation>>,
    copy_tx: Option<mpsc::Sender<Operation>>,
    fault: Option<Error>,
    native_control: Option<Arc<dyn NativeControlView>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.revision.fetch_add(1, Ordering::AcqRel);
        let mut snapshot = self.shared.snapshot.lock().unwrap();
        if !snapshot.closed {
            snapshot.closed = true;
            snapshot.ready = false;
            snapshot.fault = Some("engine owner terminated unexpectedly".into());
        }
    }
}

impl Owner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Endpoint,
        transport: PeerTransport,
        provider: Arc<dyn StateProvider>,
        settings: Settings,
        shared: Arc<Shared>,
        queue: OperationQueue,
        ack_tx: mpsc::Sender<ApplyAck>,
        replication_tx: mpsc::Sender<Operation>,
        copy_tx: mpsc::Sender<Operation>,
        native_control: Option<Arc<dyn NativeControlView>>,
    ) -> Self {
        let dispatched = queue.progress().applied;
        Self {
            endpoint,
            transport,
            provider,
            settings,
            shared,
            queue,
            charges: BTreeMap::new(),
            completions: BTreeMap::new(),
            role: ReplicaRole::Unknown,
            epoch: None,
            provider_epoch: None,
            copy_done: false,
            reconciled: false,
            current: None,
            previous: None,
            previous_quorum: 0,
            catch_up_fence: 0,
            peers: HashMap::new(),
            tombstones: HashSet::new(),
            session: 0,
            secondary: None,
            dispatched,
            announced_completed: 0,
            apply_deadlines: BTreeMap::new(),
            dispatch_permits: BTreeMap::new(),
            control: None,
            catchups: Vec::new(),
            removals: Vec::new(),
            jobs: JoinSet::new(),
            ack_tx,
            replication_tx: Some(replication_tx),
            copy_tx: Some(copy_tx),
            fault: None,
            native_control,
        }
    }

    async fn run(
        &mut self,
        mut incoming: Incoming,
        mut commands: mpsc::Receiver<Command>,
        mut writes: mpsc::Receiver<Submit>,
        mut acks: mpsc::Receiver<ApplyAck>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut retry = interval(self.settings.retry);
        retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if self.shared.stopped.load(Ordering::Acquire) || self.fault.is_some() {
                break;
            }
            let event = tokio::select! {
                biased;
                _ = stop.changed() => break,
                Some(ack) = acks.recv() => Event::Ack(ack),
                Some(command) = commands.recv() => Event::Command(command),
                result = self.jobs.join_next(), if !self.jobs.is_empty() => Event::Job(result),
                Some(packet) = incoming.control.recv() => Event::Packet(packet),
                Some(packet) = incoming.data.recv() => Event::Packet(packet),
                Some(write) = writes.recv() => Event::Write(write),
                _ = retry.tick() => Event::Retry,
            };
            // Stop/Abort and successful publication have one linearization
            // boundary. No provider future is constructed or polled under it.
            let shared = self.shared.clone();
            let _admission = shared.admission.lock().unwrap();
            if shared.stopped.load(Ordering::Acquire) {
                break;
            }
            match event {
                Event::Ack(ack) => {
                    if let Err(error) = self.apply_ack(ack) {
                        self.fail(error);
                    }
                }
                Event::Command(command) => self.command(command),
                Event::Job(result) => match result {
                    Some(Ok((job, result))) => self.job_done(job, result),
                    Some(Err(_)) => self.fail(Error::Protocol("provider callback panicked")),
                    None => {}
                },
                Event::Packet(packet) => self.packet(packet),
                Event::Write(write) => self.submit(write),
                Event::Retry => self.retry(),
            }
            if self.fault.is_none() {
                if let Err(error) = self.advance() {
                    self.fail(error);
                }
            }
            self.publish();
        }
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.revision.fetch_add(1, Ordering::AcqRel);
        if self.fault.is_some() {
            if let Some(view) = &self.native_control {
                // Poll notification first to install the runtime-owned retry
                // stage even when Close/Abort has already requested shutdown.
                // Never wait for our own native disposal to release a receipt.
                tokio::select! {
                    biased;
                    _ = view.report_fault(mssf_runtime::FaultType::Transient) => {},
                    _ = async {
                        if !*stop.borrow_and_update() {
                            let _ = stop.changed().await;
                        }
                    } => {},
                }
            }
        }
        if let Some(control) = &self.control {
            control.cancellation.cancel();
        }
        for peer in self.peers.values_mut() {
            if let Some(build) = &mut peer.build {
                build.cancellation.cancel();
            }
        }
        self.jobs.abort_all();
        while self.jobs.join_next().await.is_some() {}
        self.replication_tx.take();
        self.copy_tx.take();
        let error = self.fault.clone().unwrap_or(Error::Closed);
        for (_, sender) in std::mem::take(&mut self.completions) {
            sender.send_replace(Some(Err(error.clone())));
        }
        if let Some(control) = self.control.take() {
            match control.kind {
                ControlKind::Epoch { reply, .. } => {
                    let _ = reply.send(Err(error.clone()));
                }
                ControlKind::Loss(reply) => {
                    let _ = reply.send(Err(error.clone()));
                }
                ControlKind::Barrier { .. } => {}
            }
        }
        for waiter in self.catchups.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        for waiter in self.removals.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        for peer in self.peers.values_mut() {
            if let Some(mut build) = peer.build.take() {
                if let Some(reply) = build.reply.take() {
                    let _ = reply.send(Err(error.clone()));
                }
            }
        }
        // Close before draining so no producer can refill these queues.
        commands.close();
        writes.close();
        while commands.try_recv().is_ok() {}
        while writes.try_recv().is_ok() {}
        self.charges.clear();
        self.dispatch_permits.clear();
        self.secondary = None;
        self.queue
            .reset(self.queue.progress().applied)
            .expect("an applied boundary is an initialized LSN");
        self.transport.shutdown();
        self.publish();
    }

    fn fail(&mut self, error: Error) {
        if self.fault.is_none() {
            self.fault = Some(error);
            self.reconciled = false;
        }
    }

    fn progress_for(
        &self,
        configuration: &ReplicaSetConfiguration,
        previous: bool,
    ) -> Option<ConfigurationProgress> {
        configuration_progress(
            configuration,
            self.queue.progress().assigned,
            previous,
            |id| {
                self.peers
                    .get(&id)
                    .filter(|peer| !peer.cleanup_required)
                    .map(|peer| PeerProgress {
                        received: peer.received,
                        applied: peer.applied,
                    })
            },
        )
    }

    fn ready(&self) -> bool {
        self.role == ReplicaRole::Primary
            && self.reconciled
            && self.copy_done
            && self.control.is_none()
            && self.fault.is_none()
            && !self.shared.stopped.load(Ordering::Acquire)
            && self.current.as_ref().is_some_and(|c| {
                c.local_member
                    && c.write_quorum > 0
                    && self.progress_for(c, false).is_some()
                    && Some(c.epoch.primary()) == self.epoch
            })
            && self
                .previous
                .as_ref()
                .is_none_or(|c| self.progress_for(c, true).is_some())
    }

    fn publish(&self) {
        let mut peers: Vec<_> = self
            .peers
            .values()
            .map(|peer| PeerSnapshot {
                replica: peer.endpoint.actor.replica,
                incarnation: peer.endpoint.actor.incarnation.to_string(),
                address: peer.endpoint.to_string(),
                received: peer.received,
                applied: peer.applied,
                maximum_sent: peer.maximum_sent,
                copy_received: peer.copy_received,
                copy_applied: peer.copy_applied,
                copy_horizon: peer.build.as_ref().map(|b| b.horizon),
                copy_finish: peer.build.as_ref().and_then(|b| b.finish),
                configured: peer.configured,
                building: peer.build.as_ref().is_some_and(|b| b.reply.is_some()),
                cleanup_required: peer.cleanup_required,
            })
            .collect();
        peers.sort_by_key(|peer| peer.replica);
        let dispatched = (self.dispatched - self.queue.progress().applied).max(0) as usize
            + self.secondary.as_ref().map_or(0, |copy| {
                (copy.dispatched - copy.queue.progress().applied).max(0) as usize
            });
        let mut progress = self.queue.progress();
        if self.secondary.is_some() && !self.copy_done {
            progress.applied = 0;
            progress.committed = 0;
            progress.completed = 0;
            progress.client_completed = 0;
        }
        *self.shared.snapshot.lock().unwrap() = EngineSnapshot {
            role: self.role,
            epoch: self.epoch,
            progress,
            copy_done: self.copy_done,
            ready: self.ready(),
            closed: self.shared.stopped.load(Ordering::Acquire),
            fault: self.fault.as_ref().map(ToString::to_string),
            provider_callbacks: self.jobs.len(),
            dispatched_unacknowledged: dispatched,
            operation_captures: self.settings.provider_operations
                - self.shared.callbacks.available_permits(),
            owned_operations: self.settings.queue_operations
                - self.shared.budget.operations.available_permits(),
            owned_bytes: self.settings.queue_bytes - self.shared.budget.bytes.available_permits(),
            peers,
            observed_primary: self.secondary.as_ref().map(|copy| copy.primary.to_string()),
        };
    }

    fn submit(&mut self, write: Submit) {
        if write.cancellation.is_cancelled() {
            let _ = write.reply.send(Err(Error::Closed));
            return;
        }
        if !self.ready() {
            let _ = write
                .reply
                .send(Err(Error::InvalidInput("primary is not write ready")));
            return;
        }
        match self
            .queue
            .enqueue(self.epoch.unwrap().primary(), write.data)
        {
            Ok(sequence) => {
                self.charges.insert(sequence, Arc::new(write.charge));
                let (completion, receiver) = watch::channel(None);
                self.completions.insert(sequence, completion);
                self.publish();
                let _ = write.reply.send(Ok(Replication {
                    sequence,
                    completion: receiver,
                }));
                self.send_replication();
            }
            Err(error) => {
                let _ = write.reply.send(Err(error));
            }
        }
    }
}

impl Owner {
    fn command(&mut self, command: Command) {
        match command {
            #[cfg(feature = "qualification")]
            Command::Probe {
                kind,
                target,
                reply,
            } => {
                let result = self.qualification_probe(&kind, target);
                let _ = reply.send(result);
            }
            Command::Epoch {
                epoch,
                role,
                cancellation,
                reply,
            } => {
                if cancellation.is_cancelled() {
                    let _ = reply.send(Err(Error::Closed));
                    return;
                }
                if self.control.is_some() {
                    let _ = reply.send(Err(Error::ResourceLimit("provider control callback")));
                    return;
                }
                if epoch.data_loss < 0
                    || epoch.configuration < 0
                    || self
                        .epoch
                        .is_some_and(|old| epoch.primary() < old.primary())
                    || self
                        .secondary
                        .as_ref()
                        .is_some_and(|copy| epoch.primary() < copy.primary_epoch)
                {
                    let _ = reply.send(Err(Error::InvalidInput("stale epoch")));
                    return;
                }
                let role = role.unwrap_or(self.role);
                if self.secondary.is_some() && !self.copy_done {
                    let error = Error::Protocol("epoch or promotion during unfinished copy");
                    let _ = reply.send(Err(error.clone()));
                    self.fail(error);
                    return;
                }
                if matches!(role, ReplicaRole::Unknown) {
                    let _ = reply.send(Err(Error::InvalidInput("unknown native role")));
                    return;
                }
                if self.role == ReplicaRole::Primary
                    && role != ReplicaRole::Primary
                    && self.queue.progress().assigned != self.queue.progress().committed
                {
                    // Native queue transfer requires no uncommitted tail.
                    // Refuse without retiring accepted writes or their epoch:
                    // the caller may drain commitment and retry the transition.
                    let _ = reply.send(Err(Error::InvalidInput(
                        "primary demotion requires all assigned operations committed",
                    )));
                    return;
                }
                let boundary = if self.role == ReplicaRole::Primary {
                    self.queue.progress().assigned
                } else {
                    self.dispatched
                };
                if self.role == ReplicaRole::Primary && role != ReplicaRole::Primary {
                    for peer in self.peers.values_mut() {
                        if peer.copy_applied == -1
                            && peer.build.as_ref().is_some_and(|build| {
                                !build.job
                                    && build.finish.is_some_and(|finish| peer.applied >= finish)
                            })
                        {
                            peer.build = None;
                        }
                        if let Some(build) = peer.build.as_mut() {
                            build.cancellation.cancel();
                            peer.cleanup_required = true;
                        }
                    }
                }
                self.reconciled = false;
                self.control = Some(Control {
                    kind: ControlKind::Epoch {
                        epoch: epoch.primary(),
                        role,
                        boundary,
                        reply,
                    },
                    cancellation,
                    running: false,
                    started: Instant::now(),
                });
            }
            Command::Loss(cancellation, reply) => {
                if cancellation.is_cancelled() {
                    let _ = reply.send(Err(Error::Closed));
                } else if self.role != ReplicaRole::Primary || self.control.is_some() {
                    let _ = reply.send(Err(Error::InvalidInput(
                        "data loss requires an idle primary control slot",
                    )));
                } else {
                    self.reconciled = false;
                    self.control = Some(Control {
                        kind: ControlKind::Loss(reply),
                        cancellation,
                        running: false,
                        started: Instant::now(),
                    });
                }
            }
            Command::Configure {
                current,
                previous,
                native_prewrite,
                cancellation,
                reply,
            } => {
                let result = if cancellation.is_cancelled() {
                    Err(Error::Closed)
                } else {
                    self.configure(current, previous, native_prewrite)
                };
                let _ = reply.send(result);
            }
            Command::CatchUp(mode, cancellation, reply) => {
                if self.role != ReplicaRole::Primary || self.current.is_none() {
                    let _ = reply.send(Err(Error::InvalidInput(
                        "catch-up requires configured primary",
                    )));
                } else if self.catchups.len() >= self.settings.control_messages {
                    let _ = reply.send(Err(Error::ResourceLimit("catch-up waiters")));
                } else {
                    self.catchups.push(CatchUp {
                        mode,
                        cancellation,
                        reply,
                    });
                }
            }
            Command::Build(replica, cancellation, reply) => {
                self.build(replica, cancellation, reply);
            }
            Command::Remove(replica, reason, cancellation, reply) => {
                let result = if cancellation.is_cancelled() {
                    Err(Error::Closed)
                } else {
                    self.remove(replica.clone(), reason)
                };
                if matches!(
                    result,
                    Err(Error::ResourceLimit("enumeration disposal still pending"))
                ) && self.removals.len() < self.settings.control_messages
                {
                    self.removals.push(Removal {
                        replica,
                        reason,
                        cancellation,
                        reply,
                    });
                } else {
                    let _ = reply.send(result);
                }
            }
        }
    }

    #[cfg(feature = "qualification")]
    fn qualification_probe(&self, kind: &str, target: Option<i64>) -> Result<serde_json::Value> {
        if self.control.is_some() || !self.copy_done || self.fault.is_some() {
            return Err(Error::Closed);
        }
        let primary = self.role == ReplicaRole::Primary;
        let peer = if primary {
            self.peers
                .values()
                .find(|peer| {
                    peer.configured
                        && !peer.cleanup_required
                        && target.is_none_or(|id| peer.endpoint.actor.replica == id)
                })
                .map(|peer| peer.endpoint.clone())
        } else if self.role == ReplicaRole::Secondary {
            self.secondary
                .as_ref()
                .map(|copy| copy.primary.clone())
                .filter(|peer| target.is_none_or(|id| peer.actor.replica == id))
        } else {
            None
        }
        .ok_or(Error::InvalidInput(
            "probe requires native-selected live peer",
        ))?;
        let epoch = self.epoch.ok_or(Error::Closed)?;
        let progress = self.queue.progress();
        let sequence = progress
            .assigned
            .checked_add(1)
            .filter(|n| *n > 0 && *n < i64::MAX)
            .ok_or(Error::InvalidInput("probe sequence exhausted"))?;
        let mut from = self.endpoint.clone();
        let mut destination = peer.clone();
        let mut sent_epoch = epoch;
        let body = match kind {
            "stale_epoch" if primary => {
                sent_epoch = if epoch.configuration >= 0x1_0000_0000 {
                    Epoch {
                        configuration: epoch.configuration - 0x1_0000_0000,
                        ..epoch
                    }
                } else if epoch.data_loss > 0 {
                    Epoch {
                        data_loss: epoch.data_loss - 1,
                        ..epoch
                    }
                } else {
                    return Err(Error::InvalidInput("no earlier valid native epoch"));
                };
                let payload = crate::Mutation {
                    id: format!("__probe-{}", uuid::Uuid::new_v4()),
                    key: "__invalid_stale_epoch".into(),
                    value: "must-not-apply".into(),
                }
                .operation()?;
                Body::Replication {
                    primary_epoch: sent_epoch,
                    operation_epoch: sent_epoch,
                    operations: vec![WireOperation {
                        sequence,
                        kind: 1,
                        atomic_group: -1,
                        data: payload.to_segments(),
                    }],
                    batch_end: sequence,
                    completed: progress.completed,
                }
            }
            "wrong_sender" => {
                from.actor.incarnation = uuid::Uuid::new_v4();
                if primary {
                    Body::RequestAck
                } else {
                    Body::Ack {
                        received: progress.received,
                        applied: progress.applied,
                        copy_received: -1,
                        copy_applied: -1,
                        error: 0,
                    }
                }
            }
            "stale_copy_ack" if !primary => Body::Ack {
                received: progress.received,
                applied: progress.applied,
                copy_received: 0,
                copy_applied: 0,
                error: 0,
            },
            "wrong_target" => {
                destination.actor.incarnation = uuid::Uuid::new_v4();
                Body::RequestAck
            }
            _ => return Err(Error::InvalidInput("unsupported role/probe kind")),
        };
        let temporary = destination.actor != peer.actor;
        if temporary {
            self.transport.register_peer(&destination)?;
        }
        let result = self.transport.send(Packet {
            from: from.clone(),
            to: destination.actor,
            body,
        });
        if temporary {
            let _ = self.transport.unregister_peer(&destination.actor);
        }
        result?;
        Ok(serde_json::json!({"qualification_only":true,"kind":kind,
                "from":from.to_string(),"target":destination.to_string(),"admission_only":true,
                "native_selected_peer":peer.to_string(),"sequence":sequence,
                "native_epoch":{"data_loss":epoch.data_loss,"configuration":epoch.configuration},
                "sent_epoch":{"data_loss":sent_epoch.data_loss,"configuration":sent_epoch.configuration}}))
    }

    fn validate_configuration(&self, configuration: &ReplicaSetConfiguration) -> Result<()> {
        if self.role != ReplicaRole::Primary
            || configuration.replicas.len() > self.settings.connections
            || configuration.write_quorum > 65
        {
            return Err(Error::InvalidInput("configuration role, epoch or limit"));
        }
        let mut ids = HashSet::new();
        for replica in &configuration.replicas {
            if replica.replica_id == self.endpoint.actor.replica || !ids.insert(replica.replica_id)
            {
                return Err(Error::InvalidInput(
                    "duplicate/local remote configuration member",
                ));
            }
            if !replica.address.is_empty() {
                self.validate_replica(replica)?;
            } else if replica.up && replica.ready {
                return Err(Error::InvalidInput(
                    "ready member lacks replication endpoint",
                ));
            }
        }
        Ok(())
    }

    fn validate_replica(&self, replica: &ReplicaInformation) -> Result<Endpoint> {
        let endpoint: Endpoint = replica.address.parse()?;
        if endpoint.actor.partition != self.endpoint.actor.partition
            || endpoint.actor.replica != replica.replica_id
            || endpoint.actor == self.endpoint.actor
        {
            return Err(Error::InvalidInput("replica endpoint identity mismatch"));
        }
        if self.tombstones.iter().any(|(actor, version)| {
            *actor == endpoint.actor
                || actor.replica == endpoint.actor.replica && *version >= replica.incarnation
        }) {
            return Err(Error::InvalidInput("removed replica incarnation"));
        }
        if let Some(old) = self.peers.get(&replica.replica_id) {
            if replica.incarnation < old.native_version {
                return Err(Error::InvalidInput("stale native replica incarnation"));
            }
            if old.endpoint.actor == endpoint.actor && old.native_version != replica.incarnation {
                return Err(Error::InvalidInput(
                    "native incarnation reused replication actor",
                ));
            }
        }
        Ok(endpoint)
    }

    fn configure(
        &mut self,
        current: ReplicaSetConfiguration,
        previous: Option<ReplicaSetConfiguration>,
        native_prewrite: Option<NativePreWrite>,
    ) -> Result<()> {
        let epoch = self
            .epoch
            .ok_or(Error::InvalidInput("configuration before native epoch"))?;
        let current_epoch = current.epoch.primary();
        // Native Update*ReplicaSetConfiguration has no epoch argument. Owned
        // CC/PC epochs describe distinct node-selected configurations, not one
        // shared quorum epoch (ComReplicator.cpp:815-861,893-915). RAP demotion
        // installs CC before UpdateEpoch, but only in PreWriteRevoke.
        let prewrite = current_epoch > epoch
            && native_prewrite.is_some_and(|proof| proof.matches(&current, previous.as_ref()));
        if current.epoch.data_loss < 0
            || current.epoch.configuration < 0
            || (current_epoch != epoch && !prewrite)
            || self
                .current
                .as_ref()
                .is_some_and(|old| current_epoch < old.epoch.primary())
        {
            return Err(Error::InvalidInput("configuration epoch"));
        }
        if let Some(pc) = &previous {
            if pc.epoch.data_loss < 0
                || pc.epoch.configuration < 0
                || pc.epoch.primary() > current_epoch
                || (pc.write_quorum != 0 && pc.epoch.primary() == Epoch::INVALID)
            {
                return Err(Error::InvalidInput("previous configuration epoch"));
            }
        }
        self.validate_configuration(&current)?;
        if let Some(previous) = &previous {
            self.validate_configuration(previous)?;
        }
        let mut desired: HashMap<i64, (&ReplicaInformation, Endpoint)> = HashMap::new();
        for replica in current.replicas.iter().chain(
            previous
                .as_ref()
                .into_iter()
                .flat_map(|configuration| configuration.replicas.iter()),
        ) {
            if replica.address.is_empty() || !replica.up || !replica.ready {
                continue;
            }
            let endpoint = self.validate_replica(replica)?;
            if let Some((old, address)) = desired.get(&replica.replica_id) {
                if old.incarnation != replica.incarnation || address.actor != endpoint.actor {
                    return Err(Error::InvalidInput("current/previous incarnation conflict"));
                }
            }
            desired.insert(replica.replica_id, (replica, endpoint));
        }
        let additions = desired
            .iter()
            .filter(|(id, _)| !self.peers.contains_key(id))
            .count();
        if self.peers.len() + additions > self.settings.connections {
            return Err(Error::ResourceLimit("peer sessions"));
        }
        for (&id, (_, endpoint)) in &desired {
            if self.peers.get(&id).is_some_and(|peer| {
                !same_endpoint(&peer.endpoint, endpoint)
                    && peer.build.as_ref().is_some_and(|build| build.job)
            }) {
                return Err(Error::ResourceLimit(
                    "old incarnation enumeration requires cleanup",
                ));
            }
        }
        for (_, endpoint) in desired.values() {
            if let Some(old) = self.peers.get(&endpoint.actor.replica) {
                if old.endpoint.actor != endpoint.actor {
                    self.transport.unregister_peer(&old.endpoint.actor)?;
                }
            }
            self.transport.register_peer(endpoint)?;
        }
        if let Some(previous) = &self.previous {
            if let Some(progress) = self.progress_for(previous, true) {
                self.previous_quorum = self.previous_quorum.max(progress.quorum);
            }
        }
        for peer in self.peers.values_mut() {
            peer.configured = false;
        }
        for (id, (replica, endpoint)) in desired {
            let replace = self
                .peers
                .get(&id)
                .is_some_and(|peer| !same_endpoint(&peer.endpoint, &endpoint));
            if replace {
                let old = self.peers.get(&id).unwrap();
                if old.build.as_ref().is_some_and(|b| b.job) {
                    return Err(Error::ResourceLimit(
                        "old incarnation enumeration requires cleanup",
                    ));
                }
                self.retire_peer(id, Error::Closed);
                self.transport.register_peer(&endpoint)?;
            }
            if !self.peers.contains_key(&id) {
                let peer = self.new_peer(endpoint.clone(), replica.incarnation);
                self.peers.insert(id, peer);
            }
            let peer = self.peers.get_mut(&id).unwrap();
            peer.endpoint = endpoint;
            peer.configured = true;
        }
        self.current = Some(current);
        // Updating CC alone terminates the prior joint configuration, just as
        // the native UpdateCurrentReplicaSetConfiguration control does.
        self.previous = previous.filter(|configuration| configuration.write_quorum != 0);
        if let Some(previous) = &self.previous {
            if let Some(progress) = self.progress_for(previous, true) {
                self.previous_quorum = self.previous_quorum.max(progress.quorum);
                self.catch_up_fence = self.catch_up_fence.max(progress.quorum);
            }
        }
        self.send_replication();
        Ok(())
    }

    fn new_peer(&mut self, endpoint: Endpoint, native_version: i64) -> Peer {
        self.session += 1;
        Peer {
            endpoint,
            native_version,
            session: self.session,
            configured: false,
            received: 0,
            applied: 0,
            // A promoted primary may have an authoritative reconciled prefix
            // but no retained suffix to resend. Native existing peers still
            // ACK that prefix. It is an upper bound, never a peer vote; newer
            // receipts remain bounded by actual subsequent sends.
            maximum_sent: self.queue.progress().completed,
            receipt_seen: false,
            copy_received: 0,
            copy_applied: 0,
            maximum_copy_sent: 0,
            cleanup_required: false,
            build: None,
        }
    }

    fn build(&mut self, replica: ReplicaInformation, cancellation: Cancellation, reply: Reply<()>) {
        let result = self.prepare_build(&replica, cancellation.clone());
        match result {
            Ok(id) => {
                self.peers
                    .get_mut(&id)
                    .unwrap()
                    .build
                    .as_mut()
                    .unwrap()
                    .reply = Some(reply);
                self.send_start(id);
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn prepare_build(
        &mut self,
        replica: &ReplicaInformation,
        cancellation: Cancellation,
    ) -> Result<i64> {
        if cancellation.is_cancelled() {
            return Err(Error::Closed);
        }
        if self.role != ReplicaRole::Primary || !self.reconciled || self.control.is_some() {
            return Err(Error::InvalidInput("build requires reconciled primary"));
        }
        let endpoint = self.validate_replica(replica)?;
        let id = replica.replica_id;
        if self
            .peers
            .get(&id)
            .is_some_and(|peer| peer.build.is_some() || peer.cleanup_required)
        {
            return Err(Error::InvalidInput("previous build requires cleanup"));
        }
        if self
            .peers
            .values()
            .filter(|peer| peer.build.is_some())
            .count()
            >= self.settings.copies
        {
            return Err(Error::ResourceLimit("copy enumerations"));
        }
        if !self.peers.contains_key(&id) && self.peers.len() >= self.settings.connections {
            return Err(Error::ResourceLimit("peer sessions"));
        }
        let horizon = self.queue.progress().committed.max(self.previous_quorum);
        if horizon > self.queue.progress().assigned {
            return Err(Error::Protocol("copy horizon beyond assigned history"));
        }
        if horizon + 1 < self.queue.progress().first_retained
            && horizon < self.queue.progress().assigned
        {
            return Err(Error::ResourceLimit("copy replication history unavailable"));
        }
        // The owner and external submits hold the same admission mutex. Queued
        // writes already own their charges, so this probe covers either ordering
        // of a build/submit race, before any peer or transport state is changed.
        // Copy items may use this headroom until their real receipts release it;
        // competing writes must leave a whole next-enumeration slot available.
        let _headroom = self
            .shared
            .budget
            .acquire(maximum_copy_bytes(&self.settings))?;
        self.transport.register_peer(&endpoint)?;
        if self
            .peers
            .get(&id)
            .is_some_and(|peer| !same_endpoint(&peer.endpoint, &endpoint))
        {
            self.retire_peer(id, Error::Closed);
            self.transport.register_peer(&endpoint)?;
        }
        if !self.peers.contains_key(&id) {
            let peer = self.new_peer(endpoint, replica.incarnation);
            self.peers.insert(id, peer);
        }
        let peer = self.peers.get_mut(&id).unwrap();
        // A build is a new reliable sender, not permission to inherit a receipt.
        peer.received = 0;
        peer.applied = 0;
        peer.maximum_sent = 0;
        peer.receipt_seen = false;
        peer.copy_received = 0;
        peer.copy_applied = 0;
        peer.maximum_copy_sent = 0;
        peer.build = Some(Build {
            epoch: self.epoch.unwrap(),
            horizon,
            start_sent: false,
            established: false,
            stream: None,
            job: false,
            next: 1,
            pending: BTreeMap::new(),
            eos: None,
            finish: None,
            cancellation,
            reply: None,
        });
        Ok(id)
    }

    fn remove(&mut self, replica: ReplicaInformation, reason: ReplicaRemoval) -> Result<()> {
        let endpoint: Endpoint = replica.address.parse()?;
        if endpoint.actor.partition != self.endpoint.actor.partition
            || endpoint.actor.replica != replica.replica_id
        {
            return Err(Error::InvalidInput("remove identity mismatch"));
        }
        if reason == ReplicaRemoval::Idle {
            if self
                .tombstones
                .contains(&(endpoint.actor, replica.incarnation))
            {
                return Ok(());
            }
            let adopted = self
                .peers
                .get(&replica.replica_id)
                .is_some_and(|peer| peer.configured && !peer.cleanup_required);
            if !adopted && self.tombstones.len() >= self.settings.queue_operations {
                return Err(Error::ResourceLimit("replica removal tombstones"));
            }
        }
        if let Some(peer) = self.peers.get_mut(&replica.replica_id) {
            if peer.endpoint.actor != endpoint.actor || peer.native_version != replica.incarnation {
                return Err(Error::InvalidInput("stale remove incarnation"));
            }
            // RAP may adopt an idle build into CC before its delayed RemoveIdle
            // arrives. That message no longer owns this configured session.
            if peer.configured && !peer.cleanup_required {
                return Ok(());
            }
            if let Some(build) = peer.build.as_mut() {
                build.cancellation.cancel();
                if build.job {
                    peer.cleanup_required = true;
                    return Err(Error::ResourceLimit("enumeration disposal still pending"));
                }
            }
            self.retire_peer(replica.replica_id, Error::Closed);
        }
        if reason == ReplicaRemoval::Idle {
            self.tombstones
                .insert((endpoint.actor, replica.incarnation));
        }
        Ok(())
    }

    fn retire_peer(&mut self, id: i64, error: Error) {
        if let Some(mut peer) = self.peers.remove(&id) {
            let _ = self.transport.unregister_peer(&peer.endpoint.actor);
            if let Some(mut build) = peer.build.take() {
                build.cancellation.cancel();
                if let Some(reply) = build.reply.take() {
                    let _ = reply.send(Err(error));
                }
            }
        }
    }

    fn send(&self, endpoint: &Endpoint, body: Body) -> Result<()> {
        self.transport.send(Packet {
            from: self.endpoint.clone(),
            to: endpoint.actor,
            body,
        })
    }

    fn send_start(&mut self, id: i64) {
        let Some(peer) = self.peers.get(&id) else {
            return;
        };
        let Some(build) = &peer.build else { return };
        if build.established || peer.cleanup_required {
            return;
        }
        let horizon = build.horizon;
        if self
            .send(
                &peer.endpoint,
                Body::StartCopy {
                    epoch: build.epoch,
                    target: id,
                    start: horizon + 1,
                },
            )
            .is_ok()
        {
            let peer = self.peers.get_mut(&id).unwrap();
            peer.maximum_sent = horizon;
            peer.build.as_mut().unwrap().start_sent = true;
        }
    }

    fn send_replication(&mut self) {
        self.send_replication_mode(false);
    }

    fn send_replication_mode(&mut self, retransmit: bool) {
        if self.role != ReplicaRole::Primary || self.control.is_some() {
            return;
        }
        let progress = self.queue.progress();
        let primary_epoch = self.epoch.unwrap();
        for peer in self.peers.values_mut() {
            if peer.cleanup_required
                || peer.build.as_ref().is_some_and(|build| !build.established)
                || (!peer.configured && peer.build.is_none())
            {
                continue;
            }
            let mut from = peer
                .received
                .saturating_add(1)
                .max(peer.build.as_ref().map_or(1, |build| build.horizon + 1));
            if !retransmit {
                // New admission/progress sends only the unsent suffix. A
                // duplicate cumulative ACK must not amplify into another copy
                // of every outstanding operation and starve newer data.
                from = from.max(peer.maximum_sent.saturating_add(1));
            }
            if from < progress.first_retained && from <= progress.assigned {
                if !peer.receipt_seen && peer.build.is_none() {
                    // A promoted active peer can already own this authoritative
                    // prefix. Sending the retained suffix grants no vote until
                    // an explicit receipt within the sent boundary arrives.
                    from = progress.first_retained;
                } else {
                    peer.cleanup_required = true;
                    if let Some(build) = &mut peer.build {
                        if let Some(reply) = build.reply.take() {
                            let _ = reply
                                .send(Err(Error::ResourceLimit("replication history unavailable")));
                        }
                    }
                    continue;
                }
            }
            for entry in self
                .queue
                .range(from)
                .take(self.settings.provider_operations)
            {
                let packet = Packet {
                    from: self.endpoint.clone(),
                    to: peer.endpoint.actor,
                    body: Body::Replication {
                        primary_epoch,
                        operation_epoch: entry.epoch,
                        operations: vec![WireOperation {
                            sequence: entry.sequence,
                            kind: 1,
                            atomic_group: -1,
                            data: entry.data.to_segments(),
                        }],
                        batch_end: entry.sequence,
                        completed: progress.completed,
                    },
                };
                if self.transport.send(packet).is_err() {
                    break;
                }
                peer.maximum_sent = peer.maximum_sent.max(entry.sequence);
            }
        }
    }

    fn send_copies(&mut self) {
        for peer in self.peers.values_mut() {
            if peer.cleanup_required {
                continue;
            }
            let Some(build) = &peer.build else { continue };
            for (&sequence, copy) in build.pending.iter().take(self.settings.provider_operations) {
                if self
                    .transport
                    .send(Packet {
                        from: self.endpoint.clone(),
                        to: peer.endpoint.actor,
                        body: Body::Copy {
                            epoch: build.epoch,
                            target: peer.endpoint.actor.replica,
                            sequence,
                            data: copy.data.to_segments(),
                            last: copy.last,
                        },
                    })
                    .is_err()
                {
                    break;
                }
                peer.maximum_copy_sent = peer.maximum_copy_sent.max(sequence);
            }
        }
    }

    fn retry(&mut self) {
        let ids: Vec<_> = self.peers.keys().copied().collect();
        for id in ids {
            self.send_start(id);
        }
        self.send_copies();
        self.send_replication_mode(true);
        if self.role == ReplicaRole::Primary {
            for peer in self.peers.values() {
                if !peer.cleanup_required {
                    let _ = self.send(&peer.endpoint, Body::RequestAck);
                }
            }
        } else {
            self.send_ack();
        }
    }
}

impl Owner {
    fn packet(&mut self, packet: Packet) {
        if packet.to != self.endpoint.actor
            || packet.from.actor.partition != self.endpoint.actor.partition
            || packet.from.actor == self.endpoint.actor
        {
            return;
        }
        let from = packet.from;
        match packet.body {
            Body::StartCopy {
                epoch,
                target,
                start,
            } => {
                if target != self.endpoint.actor.replica
                    || start <= 0
                    || !matches!(self.role, ReplicaRole::Idle | ReplicaRole::Secondary)
                    || self.control.is_some()
                    || self.epoch.is_none_or(|minimum| epoch < minimum)
                {
                    return;
                }
                if let Some(copy) = &self.secondary {
                    if epoch == copy.copy_epoch && copy.primary == from && copy.horizon == start - 1
                    {
                        self.send_ack();
                    } else if epoch > copy.copy_epoch {
                        self.fail(Error::Protocol(
                            "new copy epoch requires a new secondary lifetime",
                        ));
                    }
                    return;
                }
                if self.dispatched > self.queue.progress().applied {
                    self.fail(Error::Protocol(
                        "copy cannot replace dispatched replication",
                    ));
                    return;
                }
                if self.transport.register_peer(&from).is_err() {
                    return;
                }
                if self.queue.reset(start - 1).is_err() {
                    return;
                }
                self.shared.revision.fetch_add(1, Ordering::AcqRel);
                self.charges.clear();
                self.dispatched = start - 1;
                self.announced_completed = 0;
                self.copy_done = false;
                self.provider_epoch = None;
                self.secondary = Some(SecondaryCopy {
                    primary: from,
                    primary_epoch: epoch,
                    copy_epoch: epoch,
                    horizon: start - 1,
                    queue: OperationQueue::new(self.settings.clone(), 0).unwrap(),
                    charges: BTreeMap::new(),
                    dispatched: 0,
                    eos: None,
                });
                self.send_ack();
            }
            Body::Replication {
                primary_epoch,
                operation_epoch,
                operations,
                completed,
                ..
            } => {
                if operation_epoch > primary_epoch
                    || operations
                        .iter()
                        .any(|op| op.kind != 1 || op.atomic_group != -1)
                {
                    return;
                }
                if self.role == ReplicaRole::Secondary
                    && self.copy_done
                    && self.secondary.is_none()
                    && self.control.is_none()
                    && self.epoch == Some(primary_epoch)
                    && self.transport.register_peer(&from).is_ok()
                {
                    self.secondary = Some(SecondaryCopy {
                        primary: from.clone(),
                        primary_epoch,
                        copy_epoch: primary_epoch,
                        horizon: self.queue.progress().applied,
                        queue: OperationQueue::new(self.settings.clone(), 1).unwrap(),
                        charges: BTreeMap::new(),
                        dispatched: 1,
                        eos: Some(1),
                    });
                    self.copy_tx.take();
                }
                if self.role == ReplicaRole::Secondary
                    && self.copy_done
                    && self.control.is_none()
                    && self.epoch == Some(primary_epoch)
                    && self.secondary.as_ref().is_some_and(|copy| {
                        primary_epoch > copy.primary_epoch && copy.primary != from
                    })
                    && self.transport.register_peer(&from).is_ok()
                {
                    let copy = self.secondary.as_mut().unwrap();
                    copy.primary = from.clone();
                    copy.primary_epoch = primary_epoch;
                }
                if !self.valid_primary(&from, primary_epoch) {
                    return;
                }
                self.secondary.as_mut().unwrap().primary_epoch = primary_epoch;
                for operation in operations {
                    if operation.sequence <= self.dispatched {
                        if let Some(old) = self.queue.get(operation.sequence) {
                            if old.epoch != operation_epoch
                                || old.data.to_segments() != operation.data
                            {
                                self.fail(Error::Protocol("conflicting dispatched replication"));
                                return;
                            }
                        }
                        continue;
                    }
                    if self
                        .provider_epoch
                        .is_some_and(|epoch| operation_epoch < epoch)
                    {
                        continue;
                    }
                    let data = match OperationData::with_settings(operation.data, &self.settings) {
                        Ok(data) => data,
                        Err(_) => return,
                    };
                    let exists = self.queue.get(operation.sequence).is_some();
                    let charge = if exists {
                        None
                    } else {
                        match self.shared.budget.acquire_data(
                            data.retained_bytes(),
                            &self.settings,
                            !self.copy_done,
                        ) {
                            Ok(charge) => Some(charge),
                            Err(_) => break,
                        }
                    };
                    match self
                        .queue
                        .receive(operation.sequence, operation_epoch, data)
                    {
                        Ok(true) => {
                            self.charges
                                .insert(operation.sequence, Arc::new(charge.unwrap()));
                        }
                        Ok(false) => {}
                        Err(Error::ResourceLimit(_)) => break,
                        Err(error) => {
                            self.fail(error);
                            return;
                        }
                    }
                }
                self.announced_completed = self.announced_completed.max(completed);
                self.send_ack();
            }
            Body::Copy {
                epoch,
                target,
                sequence,
                data,
                last,
            } => {
                // Copy retains StartCopy's epoch even when the same sender
                // pipelines replication in a newer primary epoch.
                if target != self.endpoint.actor.replica
                    || !matches!(self.role, ReplicaRole::Idle | ReplicaRole::Secondary)
                    || self.epoch.is_none_or(|minimum| epoch < minimum)
                    || self
                        .secondary
                        .as_ref()
                        .is_none_or(|copy| copy.primary != from || copy.copy_epoch != epoch)
                {
                    return;
                }
                // This profile deliberately retains native legacy null EOS:
                // an ordinary empty operation is data, an empty last item is not.
                if last && !data.is_empty() {
                    self.fail(Error::Protocol("nonempty legacy copy EOS"));
                    return;
                }
                let Some(copy) = &mut self.secondary else {
                    return;
                };
                if last && copy.queue.progress().assigned > sequence
                    || !last && copy.eos == Some(sequence)
                {
                    self.fail(Error::Protocol("copy EOS conflicts with buffered data"));
                    return;
                }
                if copy
                    .eos
                    .is_some_and(|end| sequence > end || (last && sequence != end))
                {
                    self.fail(Error::Protocol("copy after EOS"));
                    return;
                }
                let data = match OperationData::with_settings(data, &self.settings) {
                    Ok(data) => data,
                    Err(_) => return,
                };
                let charge = if copy.queue.get(sequence).is_some()
                    || sequence <= copy.queue.progress().completed
                {
                    None
                } else {
                    match self.shared.budget.acquire(data.retained_bytes()) {
                        Ok(charge) => Some(charge),
                        Err(_) => return,
                    }
                };
                match copy.queue.receive(sequence, epoch, data) {
                    Ok(true) => {
                        copy.charges.insert(sequence, Arc::new(charge.unwrap()));
                        if last {
                            copy.eos = Some(sequence);
                        }
                    }
                    Ok(false) => {}
                    Err(Error::ResourceLimit(_)) => return,
                    Err(error) => {
                        self.fail(error);
                        return;
                    }
                }
                self.send_ack();
            }
            Body::Ack {
                received,
                applied,
                copy_received,
                copy_applied,
                error,
            } => {
                self.peer_ack(from, received, applied, copy_received, copy_applied, error);
            }
            Body::RequestAck => {
                if self
                    .secondary
                    .as_ref()
                    .is_some_and(|copy| copy.primary == from)
                {
                    self.send_ack();
                }
            }
            Body::InduceFault {
                target,
                incarnation,
                ..
            } => {
                if target == self.endpoint.actor.replica
                    && incarnation == self.endpoint.actor.incarnation
                    && self
                        .secondary
                        .as_ref()
                        .is_some_and(|copy| copy.primary == from)
                {
                    self.fail(Error::Protocol("primary induced replica fault"));
                }
            }
            // Native volatile mode never initiates persisted copy context.
            Body::CopyContext { .. } | Body::CopyContextAck { .. } => {}
        }
    }

    fn valid_primary(&self, from: &Endpoint, epoch: Epoch) -> bool {
        matches!(self.role, ReplicaRole::Idle | ReplicaRole::Secondary)
            && self.epoch.is_some_and(|minimum| epoch >= minimum)
            && self
                .secondary
                .as_ref()
                .is_some_and(|copy| copy.primary == *from && epoch >= copy.primary_epoch)
    }

    fn peer_ack(
        &mut self,
        from: Endpoint,
        received: i64,
        applied: i64,
        mut copy_received: i64,
        mut copy_applied: i64,
        error: i32,
    ) {
        if self.role != ReplicaRole::Primary || self.control.is_some() {
            return;
        }
        let Some(peer) = self.peers.get_mut(&from.actor.replica) else {
            return;
        };
        // ACK has no epoch. Its identity/session, maximum legitimately sent
        // boundaries and monotone receipts are the available native fences.
        if peer.endpoint.actor != from.actor
            || peer.endpoint.address != from.address
            || peer.cleanup_required
            || received < peer.received
            || applied < peer.applied
            || received > peer.maximum_sent
            || applied > received
            || applied < 0
            || copy_received < -1
            || copy_applied < -1
            || (copy_applied == -1 && copy_received != -1)
            || (copy_received >= 0 && copy_applied > copy_received)
        {
            return;
        }
        if let Some(build) = &peer.build {
            if !build.start_sent {
                return;
            }
            if !build.established {
                // StartCopy is the receipt proof for the horizon only. A
                // retrying secondary may report an old partial/finished copy;
                // that establishes contact, never the new copy's progress.
                copy_received = 0;
                copy_applied = 0;
            }
            if peer.copy_received == -1 && copy_received != -1
                || peer.copy_applied == -1 && copy_applied != -1
                || peer.copy_received >= 0
                    && copy_received >= 0
                    && copy_received < peer.copy_received
                || peer.copy_applied >= 0 && copy_applied >= 0 && copy_applied < peer.copy_applied
                || copy_received > peer.maximum_copy_sent
                || copy_applied > peer.maximum_copy_sent
                || ((copy_received == -1 || copy_applied == -1)
                    && build.eos.is_none_or(|eos| eos > peer.maximum_copy_sent))
            {
                return;
            }
        } else if copy_received != -1 || copy_applied != -1 {
            // A configured sender without a build cannot invent copy state.
            return;
        }
        if error != 0 {
            peer.cleanup_required = true;
            if let Some(build) = &mut peer.build {
                build.cancellation.cancel();
                if let Some(reply) = build.reply.take() {
                    let _ = reply.send(Err(Error::Remote {
                        code: error as i64,
                        message: "secondary replication error".into(),
                    }));
                }
            }
            return;
        }
        let receive_advanced = received > peer.received;
        let establishing = peer.build.as_ref().is_some_and(|build| !build.established);
        peer.received = received;
        peer.applied = applied;
        peer.receipt_seen = true;
        peer.copy_received = copy_received;
        peer.copy_applied = copy_applied;
        if let Some(build) = &mut peer.build {
            if !build.established && received >= build.horizon {
                build.established = true;
            }
            let through = if copy_received == -1 {
                build.eos.unwrap_or(0)
            } else {
                copy_received
            };
            build.pending.retain(|sequence, _| *sequence > through);
        }
        if receive_advanced || establishing {
            self.send_replication();
        }
    }

    fn send_ack(&self) {
        let Some(copy) = &self.secondary else { return };
        let copy_progress = copy.queue.progress();
        let received_all = copy.eos.is_some_and(|eos| copy_progress.received >= eos);
        let applied_all = copy.eos.is_some_and(|eos| copy_progress.applied >= eos);
        let _ = self.send(
            &copy.primary,
            Body::Ack {
                received: self.queue.progress().received,
                applied: if self.copy_done {
                    self.queue.progress().applied
                } else {
                    0
                },
                copy_received: if received_all {
                    -1
                } else {
                    copy_progress.received
                },
                copy_applied: if applied_all {
                    -1
                } else {
                    copy_progress.applied
                },
                error: 0,
            },
        );
    }

    fn operation(
        &self,
        copy: bool,
        sequence: i64,
        epoch: Epoch,
        data: OperationData,
    ) -> Result<(Operation, Arc<OwnedSemaphorePermit>)> {
        let permit = Arc::new(
            self.shared
                .callbacks
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::ResourceLimit("dispatched provider operations"))?,
        );
        let capture_permit = permit.clone();
        let charge = if copy {
            self.secondary
                .as_ref()
                .and_then(|copy| copy.charges.get(&sequence))
        } else {
            self.charges.get(&sequence)
        }
        .cloned()
        .ok_or(Error::Protocol("dispatch lost its data ownership charge"))?;
        let shared: Weak<Shared> = Arc::downgrade(&self.shared);
        let revision = self.shared.revision.load(Ordering::Acquire);
        let sender = self.ack_tx.clone();
        Ok((
            Operation {
                sequence,
                epoch,
                data,
                acknowledgment: Acknowledgment::new(move || {
                    let _keep_permit_until_capture_disposal = &capture_permit;
                    let _keep_bytes_until_capture_disposal = &charge;
                    let shared = shared.upgrade().ok_or(Error::Closed)?;
                    if shared.stopped.load(Ordering::Acquire)
                        || shared.revision.load(Ordering::Acquire) != revision
                    {
                        return Err(Error::Closed);
                    }
                    sender
                        .try_send(ApplyAck {
                            revision,
                            copy,
                            sequence,
                        })
                        .map_err(|_| Error::Closed)
                }),
            },
            permit,
        ))
    }

    fn apply_ack(&mut self, ack: ApplyAck) -> Result<()> {
        if ack.revision != self.shared.revision.load(Ordering::Acquire) {
            return Ok(());
        }
        if ack.copy {
            let copy = self.secondary.as_mut().ok_or(Error::Closed)?;
            if ack.sequence > copy.dispatched {
                return Err(Error::Protocol("copy ACK beyond dispatch"));
            }
            copy.queue.acknowledge(ack.sequence)?;
        } else {
            if ack.sequence > self.dispatched {
                return Err(Error::Protocol("replication ACK beyond dispatch"));
            }
            self.queue.acknowledge(ack.sequence)?;
        }
        self.apply_deadlines.remove(&(ack.copy, ack.sequence));
        self.dispatch_permits.remove(&(ack.copy, ack.sequence));
        self.send_ack();
        Ok(())
    }

    fn dispatch(&mut self) -> Result<()> {
        if !matches!(self.role, ReplicaRole::Idle | ReplicaRole::Secondary) {
            return Ok(());
        }
        loop {
            let next_copy = self.secondary.as_ref().and_then(|copy| {
                let next = copy.dispatched + 1;
                if next <= copy.queue.progress().received {
                    copy.queue
                        .get(next)
                        .map(|entry| (entry.clone(), copy.eos == Some(next)))
                } else {
                    None
                }
            });
            let Some((entry, eos)) = next_copy else { break };
            if eos {
                let copy = self.secondary.as_mut().unwrap();
                copy.dispatched = entry.sequence;
                copy.queue.acknowledge(entry.sequence)?;
                break;
            }
            let (operation, permit) =
                match self.operation(true, entry.sequence, entry.epoch, entry.data) {
                    Ok(operation) => operation,
                    Err(Error::ResourceLimit(_)) => break,
                    Err(error) => return Err(error),
                };
            let sender = self.copy_tx.as_ref().ok_or(Error::Closed)?;
            match sender.try_send(operation) {
                Ok(()) => {
                    self.dispatch_permits.insert((true, entry.sequence), permit);
                    self.secondary.as_mut().unwrap().dispatched = entry.sequence;
                    self.apply_deadlines
                        .insert((true, entry.sequence), Instant::now());
                }
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(_) => return Err(Error::Closed),
            }
        }

        if let Some(copy) = &mut self.secondary {
            let applied = copy.queue.progress().applied;
            if applied > copy.queue.progress().completed {
                copy.queue.commit(applied)?;
                copy.queue.complete(applied)?;
                copy.queue.release_through(applied)?;
                copy.charges.retain(|sequence, _| *sequence > applied);
            }
            if copy.eos.is_some_and(|eos| applied >= eos) {
                self.copy_done = true;
                self.copy_tx.take();
            }
        }
        if !self.copy_done || self.control.is_some() {
            self.send_ack();
            return Ok(());
        }
        loop {
            let next = self.dispatched + 1;
            if next > self.queue.progress().received {
                break;
            }
            let entry = self
                .queue
                .get(next)
                .ok_or(Error::Protocol("missing received dispatch entry"))?
                .clone();
            if self.provider_epoch.is_none() {
                // Node admission and original operation epochs are different.
                // In canceled-swap history the first original epoch can precede
                // StartCopy; do not advance the real provider before seeing it.
                let copy_epoch = self.secondary.as_ref().unwrap().copy_epoch;
                self.provider_epoch = Some(entry.epoch.min(copy_epoch));
            }
            if self.provider_epoch != Some(entry.epoch) {
                if self.provider_epoch.is_some_and(|epoch| entry.epoch < epoch) {
                    return Err(Error::Protocol("operation original epoch regressed"));
                }
                self.control = Some(Control {
                    kind: ControlKind::Barrier {
                        epoch: entry.epoch,
                        boundary: next - 1,
                    },
                    cancellation: Cancellation::default(),
                    running: false,
                    started: Instant::now(),
                });
                break;
            }
            let (operation, permit) =
                match self.operation(false, entry.sequence, entry.epoch, entry.data) {
                    Ok(operation) => operation,
                    Err(Error::ResourceLimit(_)) => break,
                    Err(error) => return Err(error),
                };
            let sender = self.replication_tx.as_ref().ok_or(Error::Closed)?;
            match sender.try_send(operation) {
                Ok(()) => {
                    self.dispatch_permits
                        .insert((false, entry.sequence), permit);
                    self.dispatched = entry.sequence;
                    self.apply_deadlines
                        .insert((false, entry.sequence), Instant::now());
                }
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(_) => return Err(Error::Closed),
            }
        }
        self.send_ack();
        Ok(())
    }
}

impl Owner {
    fn advance(&mut self) -> Result<()> {
        // RAP can complete a native singleton Primary without a separate
        // Update*ReplicaSetConfiguration callback. Project only its actual
        // stable singleton view; role alone (or standalone binding) is not proof.
        if self.current.is_none()
            && self.control.is_none()
            && self.role == ReplicaRole::Primary
            && self.reconciled
            && self.copy_done
        {
            if let Some(status) = self
                .native_control
                .as_ref()
                .and_then(|view| view.status().ok())
            {
                if status.lifecycle == ReplicaLifecycle::Open
                    && status.replicator_open
                    && status.current_role == ReplicaRole::Primary
                    && status.service_role == ReplicaRole::Primary
                    && status.replicator_role == ReplicaRole::Primary
                    && status.configuration_stage == ConfigurationStage::Current
                    && status.catch_up == mssf_runtime::CatchUpResult::Completed
                    && status.minimum_replica_set_size == 1
                    && status.configuration.is_empty()
                    && Some(status.current_epoch.primary()) == self.epoch
                {
                    self.current = Some(ReplicaSetConfiguration {
                        epoch: status.current_epoch.primary(),
                        replicas: Vec::new(),
                        write_quorum: 1,
                        local_member: true,
                    });
                }
            }
        }
        if self
            .apply_deadlines
            .values()
            .any(|started| started.elapsed() >= self.settings.callback_timeout)
        {
            return Err(Error::Timeout);
        }
        if let Some(control) = &self.control {
            if control.cancellation.is_cancelled() {
                return Err(Error::Protocol("epoch/data-loss drain canceled"));
            }
            if control.started.elapsed() >= self.settings.callback_timeout {
                control.cancellation.cancel();
                return Err(Error::Timeout);
            }
        }
        self.start_control();
        let mut removals = Vec::new();
        for removal in std::mem::take(&mut self.removals) {
            let result = self.remove(removal.replica.clone(), removal.reason);
            if matches!(
                result,
                Err(Error::ResourceLimit("enumeration disposal still pending"))
            ) {
                removals.push(removal);
            } else {
                let result = if removal.cancellation.is_cancelled() {
                    Err(Error::Closed)
                } else {
                    result
                };
                let _ = removal.reply.send(result);
            }
        }
        self.removals = removals;
        self.dispatch()?;
        self.start_control();
        if self.role == ReplicaRole::Primary && self.control.is_none() {
            self.update_commit()?;
            self.advance_builds()?;
        }
        if self.role != ReplicaRole::Primary {
            let release = self
                .announced_completed
                .min(self.queue.progress().applied)
                .max(self.queue.progress().completed);
            if self.copy_done && release > self.queue.progress().completed {
                self.queue.commit(release)?;
                self.queue.complete(release)?;
                self.queue.release_through(release)?;
                self.charges.retain(|sequence, _| *sequence > release);
            }
        }
        let current = self
            .current
            .as_ref()
            .and_then(|current| self.progress_for(current, false));
        let ready_quorum = current.is_some_and(|progress| {
            progress.quorum >= self.catch_up_fence
                && self
                    .current
                    .as_ref()
                    .unwrap()
                    .replicas
                    .iter()
                    .filter(|replica| replica.must_catch_up)
                    .all(|replica| {
                        self.peers.get(&replica.replica_id).is_some_and(|peer| {
                            !peer.cleanup_required
                                && peer.applied >= progress.quorum
                                && peer.configured
                                && replica.up
                                && replica.ready
                        })
                    })
        });
        let ready_all = ready_quorum
            && current
                .is_some_and(|progress| progress.all_applied >= self.queue.progress().assigned);
        let mut pending = Vec::new();
        for waiter in self.catchups.drain(..) {
            if waiter.cancellation.is_cancelled() {
                let _ = waiter.reply.send(Err(Error::Closed));
            } else if self.role != ReplicaRole::Primary {
                let _ = waiter
                    .reply
                    .send(Err(Error::InvalidInput("catch-up role retired")));
            } else if match waiter.mode {
                CatchUpMode::All => ready_all,
                CatchUpMode::WriteQuorum => ready_quorum,
            } {
                let _ = waiter.reply.send(Ok(()));
            } else if !waiter.reply.is_closed() {
                pending.push(waiter);
            }
        }
        self.catchups = pending;
        Ok(())
    }

    fn start_control(&mut self) {
        let Some(control) = &mut self.control else {
            return;
        };
        if control.running {
            return;
        }
        let boundary = match &control.kind {
            ControlKind::Epoch { boundary, .. } | ControlKind::Barrier { boundary, .. } => {
                *boundary
            }
            ControlKind::Loss(_) => self.dispatched,
        };
        if matches!(&control.kind, ControlKind::Epoch { role, .. } if *role != ReplicaRole::Primary)
            && self.role == ReplicaRole::Primary
        {
            if self
                .peers
                .values()
                .any(|peer| peer.build.as_ref().is_some_and(|build| build.job))
            {
                return;
            }
            for peer in self.peers.values_mut() {
                if let Some(build) = peer.build.as_mut() {
                    build.stream = None;
                    build.pending.clear();
                    if let Some(reply) = build.reply.take() {
                        let _ = reply.send(Err(Error::Closed));
                    }
                }
            }
        }
        if self.role != ReplicaRole::Primary && self.queue.progress().applied < boundary {
            return;
        }
        // Secondary node controls drain admission, not original-operation
        // epochs. Advancing the provider here would poison historical replay.
        let epoch = match &control.kind {
            ControlKind::Epoch { epoch, role, .. } => {
                (*role == ReplicaRole::Primary).then_some(*epoch)
            }
            ControlKind::Barrier { epoch, .. } => Some(*epoch),
            ControlKind::Loss(_) => None,
        };
        let admission_only = matches!(&control.kind, ControlKind::Epoch { role, .. } if *role != ReplicaRole::Primary);
        control.running = true;
        let provider = self.provider.clone();
        let cancellation = control.cancellation.clone();
        let deadline = self.settings.callback_timeout;
        self.jobs.spawn(async move {
            let result = timeout(deadline, async {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(Error::Closed),
                    result = async {
                        if admission_only {
                            Ok(JobValue::Epoch(provider.last_committed()?))
                        } else if let Some(epoch) = epoch {
                            provider.update_epoch(epoch, boundary, cancellation.clone()).await?;
                            Ok(JobValue::Epoch(provider.last_committed()?))
                        } else {
                            let changed = provider.on_data_loss(cancellation.clone()).await?;
                            let progress = if changed { Some(provider.last_committed()?) } else { None };
                            Ok(JobValue::Loss(changed, progress))
                        }
                    } => result,
                }
            })
            .await
            .unwrap_or(Err(Error::Timeout));
            (Job::Control, result)
        });
    }

    fn job_done(&mut self, job: Job, result: Result<JobValue>) {
        match job {
            Job::Control => {
                if let Err(error) = self.finish_control(result) {
                    self.fail(error);
                }
            }
            Job::CopyOpen(actor, session) | Job::CopyNext(actor, session) => {
                let Some(peer) = self.peers.get_mut(&actor.replica) else {
                    return;
                };
                if peer.endpoint.actor != actor || peer.session != session {
                    return;
                }
                let Some(build) = peer.build.as_mut() else {
                    return;
                };
                build.job = false;
                if peer.cleanup_required || build.cancellation.is_cancelled() {
                    // This join is the point at which the callback capture was
                    // actually disposed. Keep the cleanup obligation itself.
                    return;
                }
                match result {
                    Ok(JobValue::CopyOpen(stream)) => build.stream = Some(stream),
                    Ok(JobValue::CopyNext(data, mut charge)) => {
                        let last = data.is_none();
                        let data = data.unwrap_or_else(|| OperationData::new(Vec::new()).unwrap());
                        if data.bytes() > self.settings.operation_bytes
                            || data.segments().len() > self.settings.operation_segments
                            || data.retained_bytes() > charge.bytes.num_permits()
                        {
                            self.fail(Error::ResourceLimit("provider copy item"));
                            return;
                        }
                        let retained = charge.bytes.split(data.retained_bytes()).unwrap();
                        charge.bytes = retained;
                        let sequence = build.next;
                        let Some(next) = build.next.checked_add(1).filter(|next| *next < i64::MAX)
                        else {
                            self.fail(Error::InvalidInput("copy sequence exhausted"));
                            return;
                        };
                        build.next = next;
                        if last {
                            build.eos = Some(sequence);
                            build.finish = Some(self.queue.progress().assigned);
                            build.stream = None;
                        }
                        build.pending.insert(
                            sequence,
                            SentCopy {
                                data,
                                last,
                                _charge: charge,
                            },
                        );
                    }
                    Ok(_) => self.fail(Error::Protocol("provider callback category mismatch")),
                    Err(error) => self.fail(error),
                }
            }
        }
    }

    fn finish_control(&mut self, result: Result<JobValue>) -> Result<()> {
        // Preserve the reply until disposal/terminal cleanup when a callback
        // fails, rather than accidentally dropping it as a successful barrier.
        let value = result?;
        let control = self
            .control
            .take()
            .ok_or(Error::Protocol("orphan provider control"))?;
        if control.cancellation.is_cancelled() {
            self.control = Some(control);
            return Err(Error::Protocol("provider control canceled at publication"));
        }
        match (control.kind, value) {
            (
                ControlKind::Epoch {
                    epoch,
                    role,
                    boundary,
                    reply,
                },
                JobValue::Epoch(authoritative),
            ) => {
                if authoritative != boundary {
                    let error = Error::Protocol("provider epoch reconciliation boundary mismatch");
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                }
                if self.role != ReplicaRole::Primary {
                    self.queue.truncate_after(boundary)?;
                    self.charges.retain(|sequence, _| *sequence <= boundary);
                    if role == ReplicaRole::Primary {
                        self.queue.commit(boundary)?;
                        self.catch_up_fence = boundary;
                        self.previous_quorum = boundary;
                        self.current = None;
                        self.previous = None;
                        for peer in self.peers.values_mut() {
                            peer.received = 0;
                            peer.applied = 0;
                            // Retained peers need the same reconciled-prefix
                            // bound as new peers, but no vote before fresh ACK.
                            peer.maximum_sent = boundary;
                            peer.receipt_seen = false;
                            peer.configured = false;
                        }
                    }
                }
                if role != self.role || self.epoch != Some(epoch) {
                    self.shared.revision.fetch_add(1, Ordering::AcqRel);
                    self.apply_deadlines.clear();
                }
                if role != ReplicaRole::Primary && self.role == ReplicaRole::Primary {
                    for (_, completion) in std::mem::take(&mut self.completions) {
                        completion.send_replace(Some(Err(Error::Closed)));
                    }
                    self.current = None;
                    self.previous = None;
                    self.queue.reset(boundary)?;
                    self.charges.clear();
                    self.dispatched = boundary;
                    self.secondary = None;
                    self.announced_completed = boundary;
                }
                if role == ReplicaRole::Primary {
                    if self.role != ReplicaRole::Primary {
                        self.secondary = None;
                        self.copy_done = true;
                        self.dispatched = boundary;
                    }
                } else if matches!(role, ReplicaRole::None) {
                    self.replication_tx.take();
                    self.copy_tx.take();
                    self.secondary = None;
                    self.copy_done = false;
                }
                self.role = role;
                self.epoch = Some(epoch);
                if role == ReplicaRole::Primary {
                    self.provider_epoch = Some(epoch);
                }
                self.reconciled = true;
                let _ = reply.send(Ok(()));
            }
            (ControlKind::Barrier { epoch, boundary }, JobValue::Epoch(authoritative)) => {
                if authoritative != boundary {
                    return Err(Error::Protocol("provider barrier reconciliation mismatch"));
                }
                self.provider_epoch = Some(epoch);
            }
            (ControlKind::Loss(reply), JobValue::Loss(changed, authoritative)) => {
                if let Some(authoritative) = authoritative {
                    for (_, completion) in std::mem::take(&mut self.completions) {
                        completion.send_replace(Some(Err(Error::Closed)));
                    }
                    self.shared.revision.fetch_add(1, Ordering::AcqRel);
                    self.queue.reset(authoritative)?;
                    self.charges.clear();
                    self.dispatched = authoritative;
                    self.previous_quorum = authoritative;
                    self.catch_up_fence = authoritative;
                    self.current = None;
                    self.previous = None;
                    let ids: Vec<_> = self.peers.keys().copied().collect();
                    for id in ids {
                        if self
                            .peers
                            .get(&id)
                            .is_some_and(|peer| peer.build.as_ref().is_some_and(|build| build.job))
                        {
                            let _ = reply
                                .send(Err(Error::Protocol("data loss during copy enumeration")));
                            return Err(Error::Protocol("data loss during copy enumeration"));
                        }
                        self.retire_peer(id, Error::Closed);
                    }
                }
                self.reconciled = true;
                let _ = reply.send(Ok(changed));
            }
            (kind, _) => {
                self.control = Some(Control { kind, ..control });
                return Err(Error::Protocol("provider control result mismatch"));
            }
        }
        Ok(())
    }

    fn update_commit(&mut self) -> Result<()> {
        let Some(current) = &self.current else {
            return Ok(());
        };
        let Some(current) = self.progress_for(current, false) else {
            return Ok(());
        };
        let previous = match &self.previous {
            Some(configuration) => match self.progress_for(configuration, true) {
                Some(progress) => {
                    self.previous_quorum = self.previous_quorum.max(progress.quorum);
                    Some(progress)
                }
                None => return Ok(()),
            },
            None => None,
        };
        let commit = current
            .quorum
            .min(previous.map_or(i64::MAX, |p| p.quorum))
            .max(self.queue.progress().completed);
        let _ = self.queue.commit(commit)?;
        let completed: Vec<_> = self
            .completions
            .range(..=commit)
            .map(|(&sequence, _)| sequence)
            .collect();
        for sequence in completed {
            if let Some(completion) = self.completions.remove(&sequence) {
                completion.send_replace(Some(Ok(sequence)));
            }
        }
        let mut release = commit
            .min(current.all_received)
            .min(previous.map_or(i64::MAX, |p| p.all_received));
        for peer in self.peers.values() {
            if let Some(build) = &peer.build {
                let done = peer.copy_applied == -1
                    && build.finish.is_some_and(|finish| peer.applied >= finish);
                // Build success is not CC adoption. A copied but still idle
                // session remains a history-retention owner while writes
                // continue (native ReplicationSession::IdleReplicaProgress).
                if !done || !peer.configured {
                    let idle_release = build
                        .finish
                        .map_or(peer.received, |finish| {
                            peer.received.min(finish).max(peer.applied)
                        })
                        .max(build.horizon);
                    release = release.min(idle_release);
                }
            }
        }
        release = release.max(self.queue.progress().completed);
        if release > self.queue.progress().completed {
            self.queue.complete(release)?;
            self.queue.release_through(release)?;
            self.charges.retain(|sequence, _| *sequence > release);
        }
        Ok(())
    }

    fn advance_builds(&mut self) -> Result<()> {
        for peer in self.peers.values_mut() {
            let Some(build) = peer.build.as_mut() else {
                continue;
            };
            if build.cancellation.is_cancelled() {
                peer.cleanup_required = true;
                if let Some(reply) = build.reply.take() {
                    let _ = reply.send(Err(Error::Closed));
                }
            }
            if peer.cleanup_required {
                if !build.job {
                    build.stream = None;
                    build.pending.clear();
                }
                continue;
            }
            if peer.copy_applied == -1 && build.finish.is_some_and(|finish| peer.applied >= finish)
            {
                if let Some(reply) = build.reply.take() {
                    if reply.send(Ok(())).is_ok() {
                        // The invocation is complete. RAP may now retire that
                        // call's token; the established session remains owned
                        // by native membership/explicit removal, not that token.
                        build.cancellation = Cancellation::default();
                    } else {
                        peer.cleanup_required = true;
                    }
                }
                continue;
            }
            if !build.established || build.job || build.eos.is_some() {
                continue;
            }
            let actor = peer.endpoint.actor;
            let session = peer.session;
            let deadline = self.settings.callback_timeout;
            let cancellation = build.cancellation.clone();
            if let Some(stream) = &build.stream {
                // Reserve the largest legal result BEFORE invoking the provider.
                // One enumeration per copy, and all retained data share budgets.
                let maximum = maximum_copy_bytes(&self.settings);
                let charge = match self.shared.budget.acquire(maximum) {
                    Ok(charge) => charge,
                    Err(Error::ResourceLimit(_)) => continue,
                    Err(error) => return Err(error),
                };
                let stream = stream.clone();
                build.job = true;
                self.jobs.spawn(async move {
                    let result = timeout(deadline, async {
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => Err(Error::Closed),
                            result = stream.next(cancellation.clone()) => {
                                result.map(|data| JobValue::CopyNext(data, charge))
                            }
                        }
                    })
                    .await
                    .unwrap_or(Err(Error::Timeout));
                    (Job::CopyNext(actor, session), result)
                });
            } else {
                let provider = self.provider.clone();
                let horizon = build.horizon;
                build.job = true;
                self.jobs.spawn(async move {
                    let result = timeout(deadline, async {
                        if cancellation.is_cancelled() {
                            return Err(Error::Closed);
                        }
                        // Assignment/quorum progress can precede the primary
                        // application's local continuation after submit. Keep
                        // the exact horizon, but wait under this already-owned
                        // callback deadline before consuming provider history.
                        // Never turn this normal scheduling window into a
                        // fabricated lower snapshot or a premature source fault.
                        while provider.last_committed()? < horizon {
                            tokio::select! {
                                _ = cancellation.cancelled() => return Err(Error::Closed),
                                _ = tokio::time::sleep(std::time::Duration::from_millis(2)) => {},
                            }
                        }
                        provider.copy_state(horizon, None).map(JobValue::CopyOpen)
                    })
                    .await
                    .unwrap_or(Err(Error::Timeout));
                    (Job::CopyOpen(actor, session), result)
                });
            }
        }
        self.send_copies();
        self.send_replication();
        Ok(())
    }
}

impl Replicator for Engine {
    fn open(&self, cancellation: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.check()?;
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            Ok(self.endpoint.to_string())
        })
    }

    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request(|reply| Command::Epoch {
                epoch,
                role: Some(role),
                cancellation,
                reply,
            })
            .await
        })
    }

    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request(|reply| Command::Epoch {
                epoch,
                role: None,
                cancellation,
                reply,
            })
            .await
        })
    }

    fn progress(&self) -> Result<ReplicationProgress> {
        self.check()?;
        let snapshot = self.snapshot();
        let last = if snapshot.role == ReplicaRole::Primary {
            snapshot.progress.committed
        } else {
            snapshot.progress.received
        };
        // Native queue capability is zero when no eligible committed
        // (primary) / application-completed (secondary) history is retained;
        // it is not the next sequence nor all received-but-unapplied history.
        // ReplicationQueueManager.cpp:206-227; OperationQueue.h:80-87.
        let available_through = if snapshot.role == ReplicaRole::Primary {
            snapshot.progress.committed
        } else {
            snapshot.progress.applied
        };
        let first = if snapshot.progress.retained_operations > 0
            && snapshot.progress.first_retained <= available_through
        {
            snapshot.progress.first_retained
        } else {
            0
        };
        Ok(ReplicationProgress { first, last })
    }

    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            self.request(|reply| Command::Loss(cancellation, reply))
                .await
        })
    }

    fn update_catch_up_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let native_prewrite = self.native_prewrite()?;
            self.request(|reply| Command::Configure {
                current,
                previous: Some(previous),
                native_prewrite,
                cancellation,
                reply,
            })
            .await
        })
    }

    fn update_current_configuration(
        &self,
        current: ReplicaSetConfiguration,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let native_prewrite = self.native_prewrite()?;
            self.request(|reply| Command::Configure {
                current,
                previous: None,
                native_prewrite,
                cancellation,
                reply,
            })
            .await
        })
    }

    fn wait_for_catch_up(
        &self,
        mode: CatchUpMode,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request(|reply| Command::CatchUp(mode, cancellation, reply))
                .await
        })
    }

    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request(|reply| Command::Build(replica, cancellation, reply))
                .await
        })
    }

    fn remove_replica(
        &self,
        replica: ReplicaInformation,
        reason: ReplicaRemoval,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request(|reply| Command::Remove(replica, reason, cancellation, reply))
                .await
        })
    }

    fn close(&self, cancellation: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.shutdown();
            if cancellation.is_cancelled() {
                Err(Error::Closed)
            } else {
                Ok(())
            }
        })
    }

    fn abort(&self) {
        self.shutdown();
    }
}
