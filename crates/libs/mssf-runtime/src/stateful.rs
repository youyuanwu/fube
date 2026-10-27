//! Native stateful RAP lifecycle. Wire records are never the public API.
use crate::{
    lease::Authority, stateful_api::*, wire as w, ActivationContextHandle, Cancellation,
    PartitionKind, RuntimeMetrics,
};
use mssf_transport::{
    connection::{deadline_after, Connection, IpcReplyContext, SendStatus},
    serialization::serde::{to_vec, Guid, ObjectArray, Pointer},
    Error, Result,
};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSED: i64 = 0x80071bfeu32 as i32 as i64;

struct State {
    request: w::ProxyRequest,
    status: Arc<Mutex<ReplicaStatus>>,
    valid: Arc<AtomicBool>,
    cancel: Cancellation,
    service: Option<Arc<dyn StatefulService>>,
    replicator: Option<Arc<dyn Replicator>>,
    service_open: bool,
    generation: u64,
    live: Option<OwnedSemaphorePermit>,
    operation: i64,
    operation_start: i64,
}
struct Unit {
    state: Mutex<State>,
    gate: Arc<Semaphore>,
}

pub(crate) struct Registry {
    units: Mutex<HashMap<[u8; 16], Arc<Unit>>>,
    factories: Mutex<HashMap<String, Arc<dyn StatefulFactory>>>,
    current: ActivationContextHandle,
    authority: Arc<Authority>,
    connection: Connection,
    live: Arc<Semaphore>,
    metrics: Arc<RuntimeMetrics>,
}
impl Registry {
    pub(crate) fn new(
        current: ActivationContextHandle,
        authority: Arc<Authority>,
        connection: Connection,
        live: Arc<Semaphore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            units: Mutex::new(HashMap::new()),
            factories: Mutex::new(HashMap::new()),
            current,
            authority,
            connection,
            live,
            metrics,
        }
    }
    pub(crate) fn has_factory(&self, name: &str) -> bool {
        self.factories.lock().unwrap().contains_key(name)
    }
    pub(crate) fn factory_count(&self) -> usize {
        self.factories.lock().unwrap().len()
    }
    pub(crate) fn insert_factory(
        &self,
        name: String,
        factory: Arc<dyn StatefulFactory>,
    ) -> Result<()> {
        let mut factories = self.factories.lock().unwrap();
        if factories.contains_key(&name) {
            return Err(Error::InvalidInput(
                "service type already registered locally",
            ));
        }
        factories.insert(name, factory);
        Ok(())
    }
    fn get(&self, id: [u8; 16]) -> Option<Arc<Unit>> {
        self.units.lock().unwrap().get(&id).cloned()
    }

    pub(crate) async fn dispatch(
        &self,
        action: &str,
        request: w::ProxyRequest,
        correlation: Option<IpcReplyContext>,
    ) -> Result<()> {
        self.authority.check()?;
        if action == "RAPQuery" {
            if request.runtime != self.current.snapshot().runtime.id
                || request.flags != 0
                || !request.remote.0.is_empty()
            {
                return Err(Error::Protocol("invalid stateful RAPQuery context"));
            }
            self.metrics.queries.fetch_add(1, Ordering::Relaxed);
            return correlation
                .ok_or(Error::Protocol("RAPQuery requires correlation"))?
                .reply(
                    12,
                    Some("RAPQueryReply"),
                    to_vec(&self.query(&request)?)?,
                    deadline_after(Duration::from_secs(5))?,
                )
                .await;
        }
        if correlation.is_some() {
            return Err(Error::Protocol(
                "stateful lifecycle uses one-way semantic replies",
            ));
        }
        match action {
            "ReplicaOpen" | "ReplicaClose" | "ProxyUpdateServiceDescription" => {
                self.validate_service(&request)?;
            }
            _ => return Err(Error::Protocol("unsupported stateful RAP action")),
        }
        if request.local.node.id.0.len() != 16
            || request.local.id <= 0
            || request.local.instance <= 0
        {
            return Err(Error::Protocol("invalid stateful replica identity"));
        }
        let unit = match self.get(request.unit.id.id.0) {
            Some(unit) => unit,
            None if matches!(action, "ReplicaOpen" | "ReplicaClose") => {
                let mut units = self.units.lock().unwrap();
                units
                    .entry(request.unit.id.id.0)
                    .or_insert_with(|| {
                        Arc::new(Unit {
                            state: Mutex::new(State {
                                request: request.clone(),
                                status: Arc::new(Mutex::new(initial_status(&request))),
                                valid: Arc::new(AtomicBool::new(false)),
                                cancel: Cancellation::default(),
                                service: None,
                                replicator: None,
                                service_open: false,
                                generation: 0,
                                live: None,
                                operation: 1,
                                operation_start: 0,
                            }),
                            gate: Arc::new(Semaphore::new(1)),
                        })
                    })
                    .clone()
            }
            None => return Ok(()),
        };
        {
            let state = unit.state.lock().unwrap();
            if stale(&state.request, &request) {
                return Ok(());
            }
            if state.request.service.name != request.service.name
                || state.request.service.instance != request.service.instance
                || state.request.service.service_type.name != request.service.service_type.name
                || state.request.local.node.id.0 != request.local.node.id.0
                || state.request.local.node.instance != request.local.node.instance
            {
                return Err(Error::Protocol("stateful command changed owner identity"));
            }
        }
        if action == "ReplicaClose" {
            self.metrics.closes.fetch_add(1, Ordering::Relaxed);
            if request.flags & 4 != 0 {
                self.metrics.aborts.fetch_add(1, Ordering::Relaxed);
            }
            // Close-for-business and cancel before trying operation ownership.
            // Native retries until the old callback owner drains.
            let state = unit.state.lock().unwrap();
            state.valid.store(false, Ordering::Release);
            state.cancel.cancel();
            state.status.lock().unwrap().lifecycle = ReplicaLifecycle::Closing;
        }
        let Ok(_gate) = unit.gate.clone().try_acquire_owned() else {
            return Ok(());
        };
        if stale(&unit.state.lock().unwrap().request, &request) {
            return Ok(());
        }
        match action {
            "ReplicaOpen" => self.open(&unit, request).await,
            "ReplicaClose" => self.close(&unit, request).await,
            "ProxyUpdateServiceDescription" => self.update(&unit, request).await,
            _ => unreachable!(),
        }
    }

    fn validate_service(&self, request: &w::ProxyRequest) -> Result<()> {
        let current = self.current.snapshot();
        if request.runtime != current.runtime.id
            || !request.service.stateful
            || request.service.persisted
            || request.service.group
            || request.service.service_type.package != current.runtime.code.id.package.package
            || request.service.application != current.application_name
            || !request.remote.0.is_empty()
            || request.flags & !15 != 0
            || request.service.minimum < 1
            || request.service.target < request.service.minimum
            || request.service.target as usize > MAX_REPLICA_SET_SIZE
            || request.unit.current.configuration < 0
            || request.unit.current.data_loss < 0
            || (request.unit.current.configuration == 0 && request.unit.current.data_loss != 0)
        {
            return Err(Error::Protocol(
                "unsupported/mismatched volatile stateful context",
            ));
        }
        match request.unit.consistency.kind {
            1 | 3 => {}
            2 if request.unit.consistency.low <= request.unit.consistency.high => {}
            _ => return Err(Error::Protocol("invalid stateful partition kind/range")),
        }
        Ok(())
    }

    async fn open(&self, unit: &Unit, request: w::ProxyRequest) -> Result<()> {
        self.metrics.opens.fetch_add(1, Ordering::Relaxed);
        let role = role(request.local.current_role)?;
        if !matches!(role, ReplicaRole::Primary | ReplicaRole::Idle)
            || request.flags & !4 != 0
            || api_epoch(&request.unit.current) == Epoch::INVALID
        {
            return Err(Error::Protocol("invalid volatile stateful Open"));
        }
        let existing = {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            if status.lifecycle == ReplicaLifecycle::Open {
                if state.request.local.instance != request.local.instance {
                    return Err(Error::Protocol(
                        "new stateful incarnation overlaps live owner",
                    ));
                }
                if status.service_role != role || status.replicator_role != role {
                    return Ok(());
                }
                Some(Ok(()))
            } else if state.generation != 0
                && state.request.local.instance >= request.local.instance
            {
                Some(Err(Error::Closed))
            } else {
                None
            }
        };
        if let Some(result) = existing {
            return self
                .reply(unit, "ReplicaOpenReply", request, result.err(), 1)
                .await;
        }
        let (valid, cancel, generation, snapshot) = {
            let mut state = unit.state.lock().unwrap();
            state.live = Some(
                self.live
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::ResourceLimit("live service units"))?,
            );
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or(Error::ResourceLimit("stateful generations"))?;
            state.valid = Arc::new(AtomicBool::new(true));
            state.cancel = Cancellation::default();
            state.request = request.clone();
            state.status = Arc::new(Mutex::new(initial_status(&request)));
            {
                let mut status = state.status.lock().unwrap();
                status.lifecycle = ReplicaLifecycle::Opening;
                status.current_role = role;
            }
            state.operation = 2;
            state.operation_start = super::lifecycle::filetime();
            (
                state.valid.clone(),
                state.cancel.clone(),
                state.generation,
                state.status.clone(),
            )
        };
        let identity = ReplicaIdentity {
            service_name: request.service.name.clone(),
            service_type: request.service.service_type.name.clone(),
            partition_id: uuid::Uuid::from_bytes_le(request.unit.id.id.0),
            replica_id: request.local.id,
            incarnation: request.local.instance,
        };
        let partition = StatefulPartition {
            identity: identity.clone(),
            kind: match request.unit.consistency.kind {
                1 => PartitionKind::Singleton,
                2 => PartitionKind::Int64Range {
                    low: request.unit.consistency.low,
                    high: request.unit.consistency.high,
                },
                3 => PartitionKind::Named(request.unit.consistency.name.clone()),
                _ => unreachable!(),
            },
            valid: valid.clone(),
            authority: self.authority.clone(),
            status: snapshot.clone(),
        };
        let factory = self
            .factories
            .lock()
            .unwrap()
            .get(&request.service.service_type.name)
            .cloned();
        let outcome = async {
            let factory = factory.ok_or(Error::Remote {
                code: 0x80071bd2u32 as i32 as i64,
                message: "stateful service type not registered".into(),
            })?;
            let service = tokio::time::timeout(
                CALLBACK_TIMEOUT,
                factory.create(StatefulServiceContext {
                    identity,
                    initialization_data: request.service.initialization.0.clone(),
                    activation: (*self.current.snapshot()).clone(),
                }),
            )
            .await
            .map_err(|_| Error::Timeout)??;
            unit.state.lock().unwrap().service = Some(service.clone());
            let replicator = run(&cancel, service.open(partition, cancel.clone())).await?;
            {
                let mut state = unit.state.lock().unwrap();
                state.service_open = true;
                state.replicator = Some(replicator.clone());
            }
            let address = run(&cancel, replicator.open(cancel.clone())).await?;
            {
                let mut status = snapshot.lock().unwrap();
                status.replicator_open = true;
                status.replication_address = address;
            }
            self.change_role(
                unit,
                role,
                api_epoch(&request.unit.current).primary(),
                cancel.clone(),
            )
            .await?;
            if role == ReplicaRole::Primary {
                let previous = api_epoch(&request.unit.previous);
                if previous != Epoch::INVALID
                    && previous.data_loss != request.unit.current.data_loss
                {
                    let changed = run(&cancel, replicator.on_data_loss(cancel.clone())).await?;
                    if changed {
                        return Err(Error::Remote {
                            code: 0x80071d28u32 as i32 as i64,
                            message: "application state changed on data loss".into(),
                        });
                    }
                }
                snapshot.lock().unwrap().catch_up = CatchUpResult::Completed;
            }
            self.authority.check()?;
            if !valid.load(Ordering::Acquire) || cancel.is_cancelled() {
                return Err(Error::Closed);
            }
            Ok(())
        }
        .await;
        if let Err(error) = &outcome {
            valid.store(false, Ordering::Release);
            cancel.cancel();
            self.metrics.open_failures.fetch_add(1, Ordering::Relaxed);
            let (service, replicator) = {
                let state = unit.state.lock().unwrap();
                (state.service.clone(), state.replicator.clone())
            };
            if service.is_none() && matches!(error, Error::Timeout) {
                return Err(Error::Protocol(
                    "stateful factory timeout needs process fencing",
                ));
            }
            if service.is_some() {
                self.metrics.abort_callbacks.fetch_add(1, Ordering::Relaxed);
            }
            dispose(replicator, service).await?;
        }
        let disposed = {
            let mut state = unit.state.lock().unwrap();
            if state.generation != generation {
                return Err(Error::Protocol("stale stateful Open completion"));
            }
            state.operation = 1;
            state.status.lock().unwrap().lifecycle = if outcome.is_ok() {
                ReplicaLifecycle::Open
            } else {
                ReplicaLifecycle::Closed
            };
            if outcome.is_err() {
                state.live = None;
                state.service_open = false;
                (state.service.take(), state.replicator.take())
            } else {
                (None, None)
            }
        };
        drop(disposed);
        self.reply(unit, "ReplicaOpenReply", request, outcome.err(), 1)
            .await
    }

    async fn change_role(
        &self,
        unit: &Unit,
        role: ReplicaRole,
        epoch: Epoch,
        cancel: Cancellation,
    ) -> Result<()> {
        let (service, replicator, status) = {
            let state = unit.state.lock().unwrap();
            (
                state.service.clone().ok_or(Error::Closed)?,
                state.replicator.clone().ok_or(Error::Closed)?,
                state.status.clone(),
            )
        };
        let old_replicator_role = status.lock().unwrap().replicator_role;
        if old_replicator_role != role {
            run(&cancel, replicator.change_role(epoch, role, cancel.clone())).await?;
            status.lock().unwrap().replicator_role = role;
        }
        let old_service_role = status.lock().unwrap().service_role;
        if old_service_role != role {
            let endpoint = run(&cancel, service.change_role(role, cancel.clone())).await?;
            if role == ReplicaRole::Primary && endpoint.is_empty() {
                return Err(Error::InvalidInput(
                    "stateful Primary returned empty service endpoint",
                ));
            }
            let mut snapshot = status.lock().unwrap();
            snapshot.service_role = role;
            snapshot.service_address = endpoint;
        }
        Ok(())
    }

    async fn close(&self, unit: &Unit, request: w::ProxyRequest) -> Result<()> {
        let (service, replicator, open, replicator_open, snapshot) = {
            let mut state = unit.state.lock().unwrap();
            state.operation = if request.flags & 4 != 0 { 16 } else { 8 };
            state.operation_start = super::lifecycle::filetime();
            let replicator_open = state.status.lock().unwrap().replicator_open;
            (
                state.service.clone(),
                state.replicator.clone(),
                state.service_open,
                replicator_open,
                state.status.clone(),
            )
        };
        let cleanup = Cancellation::default();
        let role_result = if request.flags & 12 != 0 && open && replicator_open {
            self.change_role(
                unit,
                ReplicaRole::None,
                api_epoch(&request.unit.current).primary(),
                cleanup.clone(),
            )
            .await
        } else {
            Ok(())
        };
        let graceful = async {
            role_result?;
            if let Some(replicator) = &replicator {
                run(&cleanup, replicator.close(cleanup.clone())).await?;
            }
            if let Some(service) = &service {
                run(&cleanup, service.close(cleanup.clone())).await?;
            }
            Ok::<_, Error>(())
        };
        if request.flags & 4 != 0 {
            dispose(replicator.clone(), service.clone()).await?;
            self.metrics.abort_callbacks.fetch_add(1, Ordering::Relaxed);
        } else if graceful.await.is_err() {
            self.metrics.close_failures.fetch_add(1, Ordering::Relaxed);
            dispose(replicator.clone(), service.clone()).await?;
            self.metrics.abort_callbacks.fetch_add(1, Ordering::Relaxed);
        }
        let (old_service, old_replicator) = {
            let mut state = unit.state.lock().unwrap();
            state.request = request.clone();
            if state.generation == 0 {
                // Close-before-Open owns a tombstone too; delayed same-instance
                // Open must not resurrect an already acknowledged lifetime.
                state.generation = 1;
            }
            state.live = None;
            state.service_open = false;
            state.operation = 1;
            let mut status = snapshot.lock().unwrap();
            status.lifecycle = ReplicaLifecycle::Closed;
            status.replicator_open = false;
            status.service_address.clear();
            status.replication_address.clear();
            (state.service.take(), state.replicator.take())
        };
        drop((old_service, old_replicator));
        self.reply(unit, "ReplicaCloseReply", request, None, 2)
            .await
    }

    async fn update(&self, unit: &Unit, request: w::ProxyRequest) -> Result<()> {
        if request.flags != 0 {
            return Err(Error::Protocol("invalid stateful service update flags"));
        }
        let response = {
            let mut state = unit.state.lock().unwrap();
            if state.request.local.id != request.local.id
                || state.request.local.instance != request.local.instance
                || request.service.update_version < state.request.service.update_version
            {
                return Ok(());
            }
            if state.request.service.name != request.service.name
                || state.request.service.service_type.name != request.service.service_type.name
                || state.request.service.instance != request.service.instance
            {
                return Err(Error::Protocol(
                    "stateful update changed immutable identity",
                ));
            }
            if request.service.update_version > state.request.service.update_version {
                state.status.lock().unwrap().minimum_replica_set_size =
                    usize::try_from(request.service.minimum)
                        .map_err(|_| Error::Protocol("invalid stateful minimum size"))?;
                state.request.service = request.service.clone();
            }
            self.metrics.updates.fetch_add(1, Ordering::Relaxed);
            w::UpdateServiceReply {
                error: 0,
                unit: state.request.unit.clone(),
                local: state.request.local.clone(),
                service: state.request.service.clone(),
            }
        };
        self.connection
            .send_one_way(
                12,
                "ProxyUpdateServiceDescriptionReply",
                to_vec(&response)?,
                deadline_after(Duration::from_secs(5))?,
            )
            .await
    }

    fn query(&self, request: &w::ProxyRequest) -> Result<w::QueryReply> {
        let mut error = 0x80071d4au32 as i32 as i64;
        let mut local = request.local.clone();
        let mut detail = w::ReplicaDetail {
            kind: 2,
            service: String::new(),
            partition: Guid([0; 16]),
            replica_id: -1,
            instance_id: -1,
            operation: 0,
            operation_start: 0,
            replicator_operation: 0,
            loads: ObjectArray(vec![]),
            read_status: 0,
            write_status: 0,
            replicator: Pointer(None),
            replica: Pointer(None),
            deployed_replica: Pointer(None),
        };
        if let Some(unit) = self.get(request.unit.id.id.0) {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            if !stale(&state.request, request) {
                error = if status.lifecycle == ReplicaLifecycle::Closed {
                    CLOSED
                } else {
                    0
                };
                local = state.request.local.clone();
                local.service_endpoint = status.service_address.clone();
                local.replication_endpoint = status.replication_address.clone();
                detail.service = state.request.service.name.clone();
                detail.partition = state.request.unit.id.id;
                detail.replica_id = local.id;
                detail.operation = state.operation;
                detail.operation_start = state.operation_start;
                let (read, write) = status.access();
                detail.read_status = read.native_value();
                detail.write_status = write.native_value();
            }
        }
        Ok(w::QueryReply {
            error,
            timestamp: super::lifecycle::stopwatch(),
            unit: request.unit.clone(),
            local,
            detail,
        })
    }

    async fn reply(
        &self,
        unit: &Unit,
        action: &str,
        request: w::ProxyRequest,
        error: Option<Error>,
        api: i64,
    ) -> Result<()> {
        self.authority.check()?;
        let (endpoint, address, valid) = {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            (
                status.service_address.clone(),
                status.replication_address.clone(),
                state.valid.clone(),
            )
        };
        let mut local = request.local;
        local.service_endpoint = endpoint;
        local.replication_endpoint = address;
        let failed = error.is_some();
        let reply = w::ProxyReply {
            unit: request.unit,
            local,
            remote: request.remote,
            flags: request.flags,
            error: w::ProxyError {
                error: error.as_ref().map_or(0, native_error),
                message: error.map_or_else(String::new, |error| error.to_string()),
                api: w::ApiName {
                    api: if failed { api } else { 0 },
                    interface: if failed { 3 } else { 0 },
                    metadata: String::new(),
                },
            },
        };
        let authority = Arc::downgrade(&self.authority);
        let publish = action == "ReplicaOpenReply" && !failed;
        self.connection
            .send_one_way_fenced(
                12,
                action,
                to_vec(&reply)?,
                deadline_after(Duration::from_secs(5))?,
                SendStatus::fenced(Arc::new(move || {
                    authority
                        .upgrade()
                        .is_some_and(|authority| authority.check().is_ok())
                        && (!publish || valid.load(Ordering::Acquire))
                })),
            )
            .await
    }

    pub(crate) async fn quiesce(&self) -> Result<()> {
        let participants: Vec<_> = self
            .units
            .lock()
            .unwrap()
            .values()
            .map(|unit| {
                let mut state = unit.state.lock().unwrap();
                state.valid.store(false, Ordering::Release);
                state.cancel.cancel();
                state.live = None;
                state.status.lock().unwrap().lifecycle = ReplicaLifecycle::Closed;
                (state.replicator.take(), state.service.take())
            })
            .collect();
        for (replicator, service) in participants {
            dispose(replicator, service).await?;
        }
        Ok(())
    }
}

async fn run<T>(cancel: &Cancellation, operation: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(CALLBACK_TIMEOUT, async {
        tokio::select! {
            result = operation => result,
            _ = cancel.cancelled() => Err(Error::Closed),
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}
async fn dispose(
    replicator: Option<Arc<dyn Replicator>>,
    service: Option<Arc<dyn StatefulService>>,
) -> Result<()> {
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::task::spawn_blocking(move || {
            if let Some(replicator) = replicator {
                replicator.abort();
            }
            if let Some(service) = service {
                service.abort();
            }
        }),
    )
    .await
    .map_err(|_| Error::Timeout)?
    .map_err(|_| Error::Protocol("stateful Abort panicked"))
}
fn initial_status(request: &w::ProxyRequest) -> ReplicaStatus {
    ReplicaStatus {
        lifecycle: ReplicaLifecycle::Closed,
        service_role: ReplicaRole::Unknown,
        replicator_role: ReplicaRole::Unknown,
        replicator_open: false,
        current_role: ReplicaRole::Unknown,
        current_epoch: api_epoch(&request.unit.current),
        previous_epoch: api_epoch(&request.unit.previous),
        configuration_stage: ConfigurationStage::Current,
        catch_up: CatchUpResult::NotStarted,
        minimum_replica_set_size: request.service.minimum as usize,
        configuration: vec![],
        service_address: String::new(),
        replication_address: String::new(),
        progress: ReplicationProgress::EMPTY,
    }
}
fn api_epoch(epoch: &w::Epoch) -> Epoch {
    Epoch {
        data_loss: epoch.data_loss,
        configuration: epoch.configuration,
    }
}
fn role(value: i64) -> Result<ReplicaRole> {
    match value {
        0 => Ok(ReplicaRole::Unknown),
        1 => Ok(ReplicaRole::None),
        2 => Ok(ReplicaRole::Idle),
        3 => Ok(ReplicaRole::Secondary),
        4 => Ok(ReplicaRole::Primary),
        _ => Err(Error::Protocol("invalid native replica role")),
    }
}
fn stale(current: &w::ProxyRequest, incoming: &w::ProxyRequest) -> bool {
    api_epoch(&current.unit.current) > api_epoch(&incoming.unit.current)
        || current.local.instance > incoming.local.instance
        || (current.local.instance == incoming.local.instance
            && current.local.id != incoming.local.id)
}
fn native_error(error: &Error) -> i64 {
    match error {
        Error::Remote { code, .. } => *code,
        Error::Closed => CLOSED,
        Error::Timeout => 0x80071bffu32 as i32 as i64,
        _ => 0x80004005u32 as i32 as i64,
    }
}
