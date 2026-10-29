//! Native stateful RAP lifecycle. Wire records are never the public API.
use crate::{
    lease::Authority, stateful_api::*, wire as w, ActivationContextHandle, Cancellation,
    PartitionKind, RuntimeMetrics,
};
use fube_transport::{
    connection::{deadline_after, Connection, IpcReplyContext, SendStatus},
    serialization::serde::{to_vec, Guid, ObjectArray, Pointer},
    Error, Result,
};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[path = "stateful_reconfiguration.rs"]
mod reconfiguration;

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
    close_through: i64,
    last_primary_epoch: Epoch,
    cancellation_epoch: Option<Epoch>,
    catch_up_operation: Option<reconfiguration::Pending>,
    remotes: HashMap<[u8; 16], reconfiguration::Remote>,
    endpoint_pending: Option<Cancellation>,
    error_api: (i64, i64),
    replicator_operation: i64,
    sequence: u64,
    full_revision: Arc<AtomicU64>,
    primary_revision: Arc<AtomicU64>,
    role_changing: bool,
    configuring: bool,
    ready: bool,
    progress_initialized: bool,
    reporting: Option<Arc<crate::reporting::Reporting>>,
    open_retry: bool,
    removing: bool,
    applied_configuration: Option<reconfiguration::AppliedConfiguration>,
}
struct Unit {
    state: Arc<Mutex<State>>,
    gate: Arc<Semaphore>,
    configuration_gate: Arc<Semaphore>,
}
#[derive(Default)]
struct ReplyControl {
    api: Option<(i64, i64)>,
    revision: Option<RevisionFence>,
}
type RevisionFence = (Arc<AtomicU64>, u64, Option<Cancellation>);

pub(crate) struct Registry {
    units: Mutex<HashMap<[u8; 16], Arc<Unit>>>,
    factories: Mutex<HashMap<String, Arc<dyn StatefulFactory>>>,
    current: ActivationContextHandle,
    authority: Arc<Authority>,
    connection: Connection,
    live: Arc<Semaphore>,
    metrics: Arc<RuntimeMetrics>,
    reports: Arc<Semaphore>,
    retries: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    callback_timeout_millis: AtomicU64,
    #[cfg(test)]
    open_admission_hook: Mutex<Option<Arc<OpenAdmissionHook>>>,
    #[cfg(test)]
    close_admission_hook: Mutex<Option<Arc<OpenAdmissionHook>>>,
    #[cfg(test)]
    configuration_reply_hook: Mutex<Option<Arc<OpenAdmissionHook>>>,
    #[cfg(test)]
    remote_reply_hook: Mutex<Option<Arc<OpenAdmissionHook>>>,
}
#[cfg(test)]
#[derive(Default)]
pub(crate) struct OpenAdmissionHook {
    pub(crate) reached: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}
impl Registry {
    pub(crate) fn new(
        current: ActivationContextHandle,
        authority: Arc<Authority>,
        connection: Connection,
        live: Arc<Semaphore>,
        metrics: Arc<RuntimeMetrics>,
        reports: Arc<Semaphore>,
    ) -> Self {
        Self {
            units: Mutex::new(HashMap::new()),
            factories: Mutex::new(HashMap::new()),
            current,
            authority,
            connection,
            live,
            metrics,
            reports,
            retries: Mutex::new(Vec::new()),
            callback_timeout_millis: AtomicU64::new(CALLBACK_TIMEOUT.as_millis() as u64),
            #[cfg(test)]
            open_admission_hook: Mutex::new(None),
            #[cfg(test)]
            close_admission_hook: Mutex::new(None),
            #[cfg(test)]
            configuration_reply_hook: Mutex::new(None),
            #[cfg(test)]
            remote_reply_hook: Mutex::new(None),
        }
    }
    #[cfg(test)]
    pub(crate) fn pause_open_admission(&self) -> Arc<OpenAdmissionHook> {
        let hook = Arc::new(OpenAdmissionHook::default());
        *self.open_admission_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
    #[cfg(test)]
    pub(crate) fn pause_configuration_reply(&self) -> Arc<OpenAdmissionHook> {
        let hook = Arc::new(OpenAdmissionHook::default());
        *self.configuration_reply_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
    #[cfg(test)]
    pub(crate) fn pause_remote_reply(&self) -> Arc<OpenAdmissionHook> {
        let hook = Arc::new(OpenAdmissionHook::default());
        *self.remote_reply_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
    #[cfg(test)]
    async fn hold_remote_reply(&self) {
        let hook = self.remote_reply_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook.reached.notify_one();
            hook.release.notified().await;
        }
    }
    #[cfg(test)]
    pub(crate) fn close_through(&self, id: [u8; 16]) -> i64 {
        self.get(id).unwrap().state.lock().unwrap().close_through
    }
    #[cfg(test)]
    pub(crate) fn pause_close_admission(&self) -> Arc<OpenAdmissionHook> {
        let hook = Arc::new(OpenAdmissionHook::default());
        *self.close_admission_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
    #[cfg(test)]
    pub(crate) async fn wait_idle(&self, id: [u8; 16]) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.get(id).unwrap().gate.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[cfg(test)]
    pub(crate) async fn wait_remote_idle(&self, id: [u8; 16], node: [u8; 16]) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let busy = self
                    .get(id)
                    .unwrap()
                    .state
                    .lock()
                    .unwrap()
                    .remotes
                    .get(&node)
                    .is_some_and(|remote| remote.busy);
                if !busy {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[cfg(test)]
    pub(crate) fn endpoint_is_pending(&self, id: [u8; 16]) -> bool {
        self.get(id)
            .unwrap()
            .state
            .lock()
            .unwrap()
            .endpoint_pending
            .is_some()
    }
    #[cfg(test)]
    pub(crate) fn catchup_is_cancelled(&self, id: [u8; 16]) -> bool {
        self.get(id)
            .unwrap()
            .state
            .lock()
            .unwrap()
            .cancellation_epoch
            .is_some()
    }
    #[cfg(test)]
    pub(crate) fn configuring(&self, id: [u8; 16]) -> bool {
        self.get(id).unwrap().state.lock().unwrap().configuring
    }
    #[cfg(test)]
    pub(crate) fn revisions(&self, id: [u8; 16]) -> (u64, u64) {
        let unit = self.get(id).unwrap();
        let state = unit.state.lock().unwrap();
        (
            state.full_revision.load(Ordering::Acquire),
            state.primary_revision.load(Ordering::Acquire),
        )
    }
    #[cfg(test)]
    pub(crate) fn set_callback_timeout(&self, timeout: Duration) {
        self.callback_timeout_millis
            .store(timeout.as_millis() as u64, Ordering::Release);
    }

    /// Supervise invocation and destruction separately. Cancellation is only a
    /// signal; dropping a user's future is not proof of operation completion.
    async fn complete<T: Send + 'static>(
        &self,
        operation: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        let timeout = Duration::from_millis(self.callback_timeout_millis.load(Ordering::Acquire));
        let guard = match self.authority.callback_deadline(timeout, false) {
            Ok(guard) => guard,
            Err(error) => {
                // Even rejecting admission may destroy application captures.
                self.authority.retire();
                self.connection.abort();
                return Err(error);
            }
        };
        let mut operation = tokio::spawn(async move {
            let result = operation.await;
            drop(guard);
            result
        });
        match tokio::time::timeout(timeout, &mut operation).await {
            Ok(Ok(result)) => result,
            failure => {
                // Retire before abort can run arbitrary callback destructors.
                self.authority.retire();
                self.connection.abort();
                operation.abort();
                Err(if failure.is_err() {
                    Error::Timeout
                } else {
                    Error::Protocol("stateful callback task panicked")
                })
            }
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
                || request.local.node.id.0.len() != 16
            {
                return Err(Error::Protocol("invalid stateful RAPQuery context"));
            }
            self.metrics.queries.fetch_add(1, Ordering::Relaxed);
            let authority = Arc::downgrade(&self.authority);
            return correlation
                .ok_or(Error::Protocol("RAPQuery requires correlation"))?
                .reply_with_action_fenced(
                    12,
                    Some("RAPQueryReply"),
                    to_vec(&self.query(&request)?)?,
                    deadline_after(Duration::from_secs(5))?,
                    Arc::new(move || {
                        authority
                            .upgrade()
                            .is_some_and(|authority| authority.check().is_ok())
                    }),
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
            "UpdateConfiguration"
            | "ReplicatorBuildIdleReplica"
            | "ReplicatorRemoveIdleReplica"
            | "ReplicatorGetStatus"
            | "ReplicatorUpdateEpochAndGetStatus"
            | "CancelCatchupReplicaSet"
            | "ProxyReplicaEndpointUpdatedReply" => {
                self.validate_control(action, &request)?;
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
                            state: Arc::new(Mutex::new(State {
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
                                close_through: 0,
                                last_primary_epoch: Epoch::INVALID,
                                cancellation_epoch: None,
                                catch_up_operation: None,
                                remotes: HashMap::new(),
                                endpoint_pending: None,
                                error_api: (1, 1),
                                replicator_operation: 1,
                                sequence: 0,
                                full_revision: Arc::new(AtomicU64::new(0)),
                                primary_revision: Arc::new(AtomicU64::new(0)),
                                role_changing: false,
                                configuring: false,
                                ready: false,
                                progress_initialized: false,
                                reporting: None,
                                open_retry: false,
                                removing: false,
                                applied_configuration: None,
                            })),
                            gate: Arc::new(Semaphore::new(1)),
                            configuration_gate: Arc::new(Semaphore::new(1)),
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
            let full = matches!(
                action,
                "ReplicaOpen" | "ReplicaClose" | "ProxyUpdateServiceDescription"
            );
            if (full
                && (state.request.service.name != request.service.name
                    || state.request.service.instance != request.service.instance
                    || state.request.service.service_type.name
                        != request.service.service_type.name))
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
            #[cfg(test)]
            {
                let hook = self.close_admission_hook.lock().unwrap().take();
                if let Some(hook) = hook {
                    hook.reached.notify_one();
                    hook.release.notified().await;
                }
            }
            let mut state = unit.state.lock().unwrap();
            if stale(&state.request, &request) {
                return Ok(());
            }
            state.close_through = state.close_through.max(request.local.instance);
            state.open_retry = false;
            state.valid.store(false, Ordering::Release);
            state.cancel.cancel();
            if let Some(pending) = &state.catch_up_operation {
                pending.cancel.cancel();
            }
            for remote in state.remotes.values() {
                remote.cancel.cancel();
            }
            if let Some(pending) = state.endpoint_pending.take() {
                pending.cancel();
            }
            state.status.lock().unwrap().lifecycle = ReplicaLifecycle::Closing;
        }
        if !matches!(
            action,
            "ReplicaOpen" | "ReplicaClose" | "ProxyUpdateServiceDescription"
        ) {
            let result = self.control(&unit, action, request).await;
            if matches!(result, Err(Error::Closed))
                && self.authority.check().is_ok()
                && !unit.state.lock().unwrap().valid.load(Ordering::Acquire)
            {
                // Close-for-business raced a read-only admission snapshot.
                // Suppress incompatible work; do not retire unrelated units.
                return Ok(());
            }
            return result;
        }
        let Ok(_gate) = unit.gate.clone().try_acquire_owned() else {
            return Ok(());
        };
        // A compatible configuration dispatcher may retain participants after
        // its callback flag clears (e.g. behind a queued semantic reply).
        // Its gate covers that entire reference lifetime, not just the callback.
        let _configuration_drain = if action == "ReplicaClose" {
            let Ok(permit) = unit.configuration_gate.clone().try_acquire_owned() else {
                return Ok(());
            };
            Some(permit)
        } else {
            None
        };
        if stale(&unit.state.lock().unwrap().request, &request) {
            return Ok(());
        }
        let draining = {
            let state = unit.state.lock().unwrap();
            state.configuring
                || state.role_changing
                || state.remotes.values().any(|remote| remote.busy)
        };
        if action == "ReplicaClose" && draining {
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
            if matches!(
                status.lifecycle,
                ReplicaLifecycle::Opening | ReplicaLifecycle::Closing
            ) {
                return Ok(());
            }
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
            } else if !state.open_retry
                && state.generation != 0
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
        #[cfg(test)]
        {
            let hook = self.open_admission_hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook.reached.notify_one();
                hook.release.notified().await;
            }
        }
        let installation = {
            let mut state = unit.state.lock().unwrap();
            // This check and generation installation share Close's lock.
            if state.close_through >= request.local.instance {
                None
            } else {
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
                state.error_api = (1, 1);
                state.replicator_operation = 1;
                state.last_primary_epoch = Epoch::INVALID;
                state.cancellation_epoch = None;
                state.catch_up_operation = None;
                state.remotes.clear();
                state.status.lock().unwrap().configuration.clear();
                state.full_revision = Arc::new(AtomicU64::new(0));
                state.primary_revision = Arc::new(AtomicU64::new(0));
                state.role_changing = false;
                state.configuring = false;
                state.ready = false;
                state.progress_initialized = false;
                state.open_retry = false;
                state.removing = false;
                state.applied_configuration = None;
                state.operation_start = super::lifecycle::filetime();
                Some((
                    state.valid.clone(),
                    state.cancel.clone(),
                    state.generation,
                    state.status.clone(),
                ))
            }
        };
        let Some((valid, cancel, generation, snapshot)) = installation else {
            return self
                .reply(unit, "ReplicaOpenReply", request, Some(Error::Closed), 1)
                .await;
        };
        let identity = ReplicaIdentity {
            service_name: request.service.name.clone(),
            service_type: request.service.service_type.name.clone(),
            partition_id: uuid::Uuid::from_bytes_le(request.unit.id.id.0),
            replica_id: request.local.id,
            incarnation: request.local.instance,
            node_id: request
                .local
                .node
                .id
                .0
                .as_slice()
                .try_into()
                .map_err(|_| Error::Protocol("invalid native node identity"))?,
            node_instance: request.local.node.instance,
        };
        let reporting = crate::reporting::Reporting::new(
            self.connection.clone(),
            valid.clone(),
            self.authority.clone(),
            request.clone(),
            cancel.clone(),
            self.reports.clone(),
        );
        unit.state.lock().unwrap().reporting = Some(reporting.clone());
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
            reporting: Some(reporting),
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
            let context = StatefulServiceContext {
                identity,
                initialization_data: request.service.initialization.0.clone(),
                activation: (*self.current.snapshot()).clone(),
            };
            let service = self
                .complete(async move { factory.create(context).await })
                .await?;
            unit.state.lock().unwrap().service = Some(service.clone());
            if cancel.is_cancelled() || !valid.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let service_open = service.clone();
            let open_cancel = cancel.clone();
            let replicator = self
                .complete(async move { service_open.open(partition, open_cancel).await })
                .await?;
            {
                let mut state = unit.state.lock().unwrap();
                state.service_open = true;
                state.replicator = Some(replicator.clone());
                state.operation = 1;
                state.error_api = (1, 4);
                state.replicator_operation = 2;
            }
            if cancel.is_cancelled() || !valid.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let replicator_open = replicator.clone();
            let open_cancel = cancel.clone();
            let address = self
                .complete(async move { replicator_open.open(open_cancel).await })
                .await?;
            {
                let mut status = snapshot.lock().unwrap();
                status.replicator_open = true;
                status.replication_address = address;
            }
            if cancel.is_cancelled() || !valid.load(Ordering::Acquire) {
                return Err(Error::Closed);
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
                    let changed = self.data_loss(unit, &replicator, &cancel).await?;
                    if changed {
                        snapshot.lock().unwrap().catch_up = CatchUpResult::DataLossReported;
                        unit.state.lock().unwrap().ready = true;
                        self.progress(unit, replicator.clone()).await?;
                        return Err(Error::Remote {
                            code: 0x80071d28u32 as i32 as i64,
                            message: "application state changed on data loss".into(),
                        });
                    }
                }
                snapshot.lock().unwrap().catch_up = CatchUpResult::Completed;
                unit.state.lock().unwrap().ready = true;
            }
            self.authority.check()?;
            if !valid.load(Ordering::Acquire) || cancel.is_cancelled() {
                return Err(Error::Closed);
            }
            Ok(())
        }
        .await;
        let state_changed = matches!(&outcome, Err(Error::Remote { code, .. })
            if *code == 0x80071d28u32 as i32 as i64);
        let cancelled_before_cleanup = cancel.is_cancelled() || !valid.load(Ordering::Acquire);
        if let Err(error) = &outcome {
            if !state_changed {
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
                dispose(
                    replicator,
                    service,
                    Some(unit.state.clone()),
                    self.authority.clone(),
                )
                .await?;
            }
        }
        let disposed = {
            let mut state = unit.state.lock().unwrap();
            if state.generation != generation {
                return Err(Error::Protocol("stale stateful Open completion"));
            }
            state.operation = 1;
            state.replicator_operation = 1;
            state.status.lock().unwrap().lifecycle = if outcome.is_ok() || state_changed {
                ReplicaLifecycle::Open
            } else {
                ReplicaLifecycle::Closed
            };
            if outcome.is_err() && !state_changed {
                state.live = None;
                state.service_open = false;
                state.open_retry = request.flags & 4 == 0
                    && !cancelled_before_cleanup
                    && state.close_through < request.local.instance;
                (state.service.take(), state.replicator.take())
            } else {
                (None, None)
            }
        };
        final_release(disposed, self.authority.clone()).await?;
        self.reply(
            unit,
            "ReplicaOpenReply",
            request,
            outcome.err(),
            if state_changed { 0 } else { 1 },
        )
        .await
    }

    async fn change_role(
        &self,
        unit: &Unit,
        role: ReplicaRole,
        epoch: Epoch,
        cancel: Cancellation,
    ) -> Result<()> {
        let (service, status) = {
            let state = unit.state.lock().unwrap();
            (
                state.service.clone().ok_or(Error::Closed)?,
                state.status.clone(),
            )
        };
        self.change_replicator_role(unit, role, epoch, &cancel)
            .await?;
        let old_service_role = status.lock().unwrap().service_role;
        if old_service_role != role {
            {
                let mut state = unit.state.lock().unwrap();
                state.error_api = (8, 1);
                state.operation = 4;
                state.operation_start = super::lifecycle::filetime();
            }
            let operation_cancel = cancel.clone();
            let result = self
                .complete(async move { service.change_role(role, operation_cancel).await })
                .await;
            unit.state.lock().unwrap().operation = 1;
            let endpoint = result?;
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

    async fn change_replicator_role(
        &self,
        unit: &Unit,
        role: ReplicaRole,
        epoch: Epoch,
        cancel: &Cancellation,
    ) -> Result<()> {
        let replicator = {
            let mut state = unit.state.lock().unwrap();
            if state.status.lock().unwrap().replicator_role == role {
                return Ok(());
            }
            if state.role_changing
                || state.configuring
                || state.remotes.values().any(|remote| remote.busy)
            {
                return Err(Error::Closed);
            }
            state.role_changing = true;
            state.error_api = (8, 4);
            state.replicator_operation = 4;
            state.replicator.clone().ok_or(Error::Closed)?
        };
        let operation_cancel = cancel.clone();
        let result = self
            .complete(async move { replicator.change_role(epoch, role, operation_cancel).await })
            .await;
        let mut state = unit.state.lock().unwrap();
        state.role_changing = false;
        state.replicator_operation = 1;
        if role != ReplicaRole::None
            && (cancel.is_cancelled() || !state.valid.load(Ordering::Acquire))
        {
            return Err(Error::Closed);
        }
        if result.is_ok() {
            state.status.lock().unwrap().replicator_role = role;
            state.last_primary_epoch = epoch;
            if role != ReplicaRole::Primary {
                reconfiguration::advance(&state.primary_revision)?;
                state.remotes.clear();
                state.status.lock().unwrap().configuration.clear();
                state.applied_configuration = None;
                if let Some(pending) = state.endpoint_pending.take() {
                    pending.cancel();
                }
            }
        }
        result
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
                {
                    let mut state = unit.state.lock().unwrap();
                    state.operation = 1;
                    state.replicator_operation = 16;
                }
                let replicator = replicator.clone();
                let operation_cancel = cleanup.clone();
                let result = self
                    .complete(async move { replicator.close(operation_cancel).await })
                    .await;
                {
                    let state = unit.state.lock().unwrap();
                    if result.is_ok() {
                        state.status.lock().unwrap().replicator_open = false;
                    }
                }
                unit.state.lock().unwrap().replicator_operation = 1;
                result?;
            }
            if let Some(service) = &service {
                {
                    let mut state = unit.state.lock().unwrap();
                    state.operation = 8;
                    state.operation_start = super::lifecycle::filetime();
                }
                let service = service.clone();
                let operation_cancel = cleanup.clone();
                let result = self
                    .complete(async move { service.close(operation_cancel).await })
                    .await;
                unit.state.lock().unwrap().operation = 1;
                result?;
            }
            Ok::<_, Error>(())
        };
        if request.flags & 4 != 0 {
            drop(graceful);
            dispose(
                replicator.clone(),
                service.clone(),
                Some(unit.state.clone()),
                self.authority.clone(),
            )
            .await?;
            self.metrics.abort_callbacks.fetch_add(1, Ordering::Relaxed);
        } else if graceful.await.is_err() {
            self.metrics.close_failures.fetch_add(1, Ordering::Relaxed);
            dispose(
                replicator.clone(),
                service.clone(),
                Some(unit.state.clone()),
                self.authority.clone(),
            )
            .await?;
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
            state.replicator_operation = 1;
            let mut status = snapshot.lock().unwrap();
            status.lifecycle = ReplicaLifecycle::Closed;
            status.replicator_open = false;
            status.service_address.clear();
            status.replication_address.clear();
            (state.service.take(), state.replicator.take())
        };
        // Include the local callback clones: releasing only State's Arcs is
        // not final runtime-owned destruction. No Close reply precedes it.
        final_release(
            (old_service, old_replicator, service, replicator),
            self.authority.clone(),
        )
        .await?;
        self.reply(unit, "ReplicaCloseReply", request, None, 2)
            .await
    }

    async fn update(&self, unit: &Unit, request: w::ProxyRequest) -> Result<()> {
        if request.flags != 0 {
            return Err(Error::Protocol("invalid stateful service update flags"));
        }
        let (response, valid) = {
            let mut state = unit.state.lock().unwrap();
            if !state.valid.load(Ordering::Acquire)
                || state.status.lock().unwrap().lifecycle != ReplicaLifecycle::Open
            {
                return Ok(());
            }
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
                if let Some(reporting) = &state.reporting {
                    reporting.update(&state.request);
                }
            }
            self.metrics.updates.fetch_add(1, Ordering::Relaxed);
            (
                w::UpdateServiceReply {
                    error: 0,
                    unit: state.request.unit.clone(),
                    local: state.request.local.clone(),
                    service: state.request.service.clone(),
                },
                state.valid.clone(),
            )
        };
        let authority = Arc::downgrade(&self.authority);
        let frame_valid = valid.clone();
        let sent = self
            .connection
            .send_one_way_fenced(
                12,
                "ProxyUpdateServiceDescriptionReply",
                to_vec(&response)?,
                deadline_after(Duration::from_secs(5))?,
                SendStatus::fenced(Arc::new(move || {
                    frame_valid.load(Ordering::Acquire)
                        && authority
                            .upgrade()
                            .is_some_and(|authority| authority.check().is_ok())
                })),
            )
            .await;
        match sent {
            Err(Error::Closed)
                if !valid.load(Ordering::Acquire) && self.authority.check().is_ok() =>
            {
                Ok(())
            }
            result => result,
        }
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
                } else if !state.service_open || state.replicator.is_none() {
                    0x80071bc7u32 as i32 as i64 // NotReady, before replicator owner exists.
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
                detail.replicator_operation = state.replicator_operation;
                if state.remotes.values().any(|remote| remote.building()) {
                    detail.replicator_operation = (detail.replicator_operation & !1) | 256;
                }
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
        self.reply_guarded(
            unit,
            action,
            request,
            error,
            ReplyControl {
                api: (api == 0).then_some((0, 0)),
                revision: None,
            },
        )
        .await
    }

    async fn reply_guarded(
        &self,
        unit: &Unit,
        action: &str,
        request: w::ProxyRequest,
        error: Option<Error>,
        control: ReplyControl,
    ) -> Result<()> {
        self.authority.check()?;
        let revision = control.revision;
        let (endpoint, address, valid, failed_api, progress) = {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            (
                status.service_address.clone(),
                status.replication_address.clone(),
                state.valid.clone(),
                control.api.unwrap_or(state.error_api),
                state.progress_initialized.then_some(status.progress),
            )
        };
        let mut local = request.local;
        local.service_endpoint = endpoint;
        local.replication_endpoint = address;
        if let Some(progress) = progress {
            local.first_lsn = progress.first;
            local.last_lsn = progress.last;
        }
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
                    api: if failed { failed_api.0 } else { 0 },
                    interface: if failed { failed_api.1 } else { 0 },
                    metadata: String::new(),
                },
            },
        };
        let authority = Arc::downgrade(&self.authority);
        let publish = action != "ReplicaCloseReply" && !failed;
        let frame_valid = valid.clone();
        let frame_revision = revision.clone();
        #[cfg(test)]
        if matches!(
            action,
            "ReplicatorBuildIdleReplicaReply" | "ReplicatorRemoveIdleReplicaReply"
        ) {
            self.hold_remote_reply().await;
        }
        let sent = self
            .connection
            .send_one_way_fenced(
                12,
                action,
                to_vec(&reply)?,
                deadline_after(Duration::from_secs(5))?,
                SendStatus::fenced(Arc::new(move || {
                    authority
                        .upgrade()
                        .is_some_and(|authority| authority.check().is_ok())
                        && (!publish || frame_valid.load(Ordering::Acquire))
                        && frame_revision.as_ref().is_none_or(
                            |(revision, expected, cancellation)| {
                                revision.load(Ordering::Acquire) == *expected
                                    && cancellation.as_ref().is_none_or(|c| !c.is_cancelled())
                            },
                        )
                })),
            )
            .await;
        match sent {
            // A locally revoked queued Open reply is suppressed, not a
            // successful endpoint publication or a process-wide fault.
            Err(Error::Closed)
                if (publish && !valid.load(Ordering::Acquire)
                    || revision
                        .as_ref()
                        .is_some_and(|(revision, expected, cancellation)| {
                            revision.load(Ordering::Acquire) != *expected
                                || cancellation.as_ref().is_some_and(|c| c.is_cancelled())
                        }))
                    && self.authority.check().is_ok() =>
            {
                Ok(())
            }
            result => result,
        }
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
                if let Some(pending) = state.endpoint_pending.take() {
                    pending.cancel();
                }
                state.live = None;
                state.status.lock().unwrap().lifecycle = ReplicaLifecycle::Closed;
                (state.replicator.take(), state.service.take())
            })
            .collect();
        for (replicator, service) in participants {
            dispose(replicator, service, None, self.authority.clone()).await?;
        }
        let retries = std::mem::take(&mut *self.retries.lock().unwrap());
        for retry in retries {
            retry.abort();
            match retry.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(_) => return Err(Error::Protocol("endpoint retry task panicked")),
            }
        }
        let factories = std::mem::take(&mut *self.factories.lock().unwrap());
        final_release(factories, self.authority.clone()).await?;
        Ok(())
    }
}

async fn final_release<T: Send + 'static>(owned: T, authority: Arc<Authority>) -> Result<()> {
    let guard = match authority.callback_deadline(Duration::from_millis(250), true) {
        Ok(guard) => guard,
        Err(error) => {
            authority.retire();
            return Err(error);
        }
    };
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            drop(owned);
        }),
    )
    .await
    .map_err(|_| Error::Timeout)?
    .map_err(|_| Error::Protocol("stateful final participant destruction panicked"))
}

async fn dispose(
    replicator: Option<Arc<dyn Replicator>>,
    service: Option<Arc<dyn StatefulService>>,
    monitor: Option<Arc<Mutex<State>>>,
    authority: Arc<Authority>,
) -> Result<()> {
    let guard = match authority.callback_deadline(Duration::from_millis(250), true) {
        Ok(guard) => guard,
        Err(error) => {
            authority.retire();
            return Err(error);
        }
    };
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            if let Some(replicator) = replicator {
                if let Some(state) = &monitor {
                    let mut state = state.lock().unwrap();
                    state.operation = 1;
                    state.replicator_operation = 32;
                }
                replicator.abort();
                if let Some(state) = &monitor {
                    state.lock().unwrap().replicator_operation = 1;
                }
            }
            if let Some(service) = service {
                if let Some(state) = &monitor {
                    let mut state = state.lock().unwrap();
                    state.operation = 16;
                    state.operation_start = super::lifecycle::filetime();
                }
                service.abort();
                if let Some(state) = &monitor {
                    state.lock().unwrap().operation = 1;
                }
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
