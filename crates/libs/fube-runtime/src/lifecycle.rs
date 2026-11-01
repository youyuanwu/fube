use crate::{api::*, lease::Authority, wire::*, ActivationContext, RuntimeMetrics};
use crate::{Error, Result};
#[cfg(test)]
#[path = "lifecycle_tests.rs"]
pub(crate) mod tests;
use fube_transport::{
    connection::{deadline_after, Connection, IpcRequest},
    serialization::serde::{from_slice, to_vec, Guid, ObjectArray, Pointer},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{broadcast, mpsc, watch, OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
};

pub const MAX_SERVICE_UNITS: usize = 32;
pub const MAX_CALLBACKS: usize = 32;
pub const MAX_TOMBSTONES: usize = 4096;
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const OBJECT_CLOSED: i64 = 0x80071bfeu32 as i32 as i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Absent,
    Opening,
    Open,
    Closing,
    Closed,
}
struct Snapshot {
    request: ProxyRequest,
    phase: Phase,
    service: Option<Arc<dyn IStatelessServiceInstance>>,
    endpoint: String,
    valid: Arc<AtomicBool>,
    cancel: Cancellation,
    generation: u64,
    operation_start: i64,
    operation_kind: i64,
    live: Option<OwnedSemaphorePermit>,
    reporting: Option<Arc<crate::reporting::Reporting>>,
}
struct Unit {
    state: Mutex<Snapshot>,
    operation: Arc<Semaphore>,
}
struct Shared {
    events: broadcast::Sender<crate::RuntimeEvent>,
    units: Mutex<HashMap<[u8; 16], Arc<Unit>>>,
    owners: Mutex<HashMap<[u8; 16], bool>>,
    stateful: crate::stateful::Registry,
    factories: Mutex<HashMap<String, Arc<dyn IStatelessServiceFactory>>>,
    registered_types: Mutex<HashSet<String>>,
    callbacks: Arc<Semaphore>,
    live: Arc<Semaphore>,
    active: AtomicUsize,
    activation: ActivationContext,
    authority: Arc<Authority>,
    connection: Connection,
    metrics: Arc<RuntimeMetrics>,
    reports: Arc<Semaphore>,
    execution: crate::extensions::ExecutionRuntime,
    core: tokio::runtime::Handle,
    #[cfg(test)]
    test_hooks: tests::DispatchHooks,
}
pub(crate) struct Engine {
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
    task: JoinHandle<Option<mpsc::Receiver<IpcRequest>>>,
    retired_incoming: Option<mpsc::Receiver<IpcRequest>>,
    active_at_stop: bool,
}
struct FactorySlot {
    shared: Arc<Shared>,
    name: String,
    committed: bool,
}
impl Drop for FactorySlot {
    fn drop(&mut self) {
        if !self.committed {
            self.shared
                .registered_types
                .lock()
                .unwrap()
                .remove(&self.name);
        }
    }
}
impl Engine {
    fn reserve_factory_name(&self, name: &str) -> Result<FactorySlot> {
        let mut types = self.shared.registered_types.lock().unwrap();
        if types.contains(name) {
            return Err(Error::InvalidInput(
                "service type already registered locally",
            ));
        }
        if types.len() >= 32 {
            return Err(Error::ResourceLimit("service factories"));
        }
        types.insert(name.to_owned());
        Ok(FactorySlot {
            shared: self.shared.clone(),
            name: name.to_owned(),
            committed: false,
        })
    }
    pub(crate) fn instance_integration_provider(
        &self,
    ) -> crate::extensions::InstanceIntegrationProvider {
        let shared = Arc::downgrade(&self.shared);
        crate::extensions::InstanceIntegrationProvider::new(move |partition_id, instance_id| {
            let shared = shared.upgrade().ok_or(Error::Closed)?;
            let unit = shared
                .units
                .lock()
                .unwrap()
                .get(&partition_id.to_bytes_le())
                .cloned()
                .ok_or(Error::Closed)?;
            let state = unit.state.lock().unwrap();
            if state.request.local.id != instance_id {
                return Err(Error::Closed);
            }
            let request = &state.request;
            let kind = match request.unit.consistency.kind {
                1 => PartitionKind::Singleton,
                2 => PartitionKind::Int64Range {
                    low: request.unit.consistency.low,
                    high: request.unit.consistency.high,
                },
                3 => PartitionKind::Named(request.unit.consistency.name.clone()),
                _ => return Err(Error::Protocol("invalid native partition kind")),
            };
            let integration = crate::extensions::InstanceRuntimeIntegration {
                partition: Partition {
                    identity: ServiceIdentity {
                        service_name: request.service.name.clone(),
                        service_type: request.service.service_type.name.clone(),
                        partition_id,
                        instance_id,
                        incarnation: request.local.instance,
                    },
                    kind,
                    valid: state.valid.clone(),
                    authority: shared.authority.clone(),
                    reporting: state.reporting.clone().ok_or(Error::Closed)?,
                },
                execution: shared.execution.clone(),
            };
            integration.validate()?;
            Ok(integration)
        })
    }
    pub(crate) fn native_integration_provider(
        &self,
    ) -> crate::extensions::NativeIntegrationProvider {
        let shared = Arc::downgrade(&self.shared);
        crate::extensions::NativeIntegrationProvider::new(move |partition, replica_id| {
            shared
                .upgrade()
                .ok_or(Error::Closed)?
                .stateful
                .native_integration(partition, replica_id)
        })
    }
    #[cfg(test)]
    pub(crate) fn set_stateful_callback_timeout(&self, timeout: Duration) {
        self.shared.stateful.set_callback_timeout(timeout);
    }
    pub(crate) fn metrics(&self) -> Arc<RuntimeMetrics> {
        self.shared.metrics.clone()
    }
    #[cfg(test)]
    pub(crate) fn start(
        current: crate::ActivationContextHandle,
        authority: Arc<Authority>,
        connection: Connection,
        incoming: mpsc::Receiver<IpcRequest>,
    ) -> Self {
        let execution = crate::extensions::ExecutionRuntime::new(
            authority.clone(),
            tokio::runtime::Handle::current(),
        );
        Self::start_with_execution(current, authority, connection, incoming, execution)
    }
    pub(crate) fn start_with_execution(
        current: crate::ActivationContextHandle,
        authority: Arc<Authority>,
        connection: Connection,
        mut incoming: mpsc::Receiver<IpcRequest>,
        execution: crate::extensions::ExecutionRuntime,
    ) -> Self {
        let activation = (*current.snapshot()).clone();
        let live = Arc::new(Semaphore::new(MAX_SERVICE_UNITS));
        let metrics = Arc::new(RuntimeMetrics::default());
        let reports = Arc::new(Semaphore::new(crate::reporting::MAX_CONCURRENT_REPORTS));
        let stateful = crate::stateful::Registry::new(
            current.clone(),
            authority.clone(),
            connection.clone(),
            live.clone(),
            metrics.clone(),
            reports.clone(),
            execution.clone(),
        );
        let shared = Arc::new(Shared {
            events: broadcast::channel(crate::diagnostics::EVENT_CAPACITY).0,
            units: Mutex::new(HashMap::new()),
            owners: Mutex::new(HashMap::new()),
            stateful,
            factories: Mutex::new(HashMap::new()),
            registered_types: Mutex::new(HashSet::new()),
            callbacks: Arc::new(Semaphore::new(MAX_CALLBACKS)),
            live,
            active: AtomicUsize::new(0),
            activation,
            authority,
            connection,
            metrics,
            reports,
            execution,
            core: tokio::runtime::Handle::current(),
            #[cfg(test)]
            test_hooks: tests::DispatchHooks::default(),
        });
        let (stop, mut stopped) = watch::channel(false);
        let task_shared = shared.clone();
        let task = tokio::spawn(async move {
            let mut work = JoinSet::new();
            let mut dispositions = Vec::<crate::extensions::OwnedDisposition>::new();
            let mut deadline_poll = tokio::time::interval(Duration::from_millis(10));
            deadline_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _=stopped.changed() => break,
                    _=deadline_poll.tick(), if !work.is_empty() => {
                        if task_shared.authority.callback_expired() {
                            task_shared.authority.retire();
                            task_shared.connection.abort_with_error(Error::Timeout.transport_fault());
                            break;
                        }
                    }
                    result=work.join_next(), if !work.is_empty() => {
                        task_shared.active.fetch_sub(1,Ordering::SeqCst);
                        #[cfg(test)]
                        task_shared.test_hooks.reach(tests::DispatchPoint::CompletionSelected).await;
                        // Stop can be requested after select polled its watch
                        // branch. A retired normal root then returns Closed at
                        // its next poll; this is not a successful RAP command.
                        // Use the existing owned drain and retain unregister IPC.
                        // Real faults/panics and active-callback fencing remain.
                        if *stopped.borrow() && matches!(&result,Some(Ok(Err(Error::Closed)))) {
                            break;
                        }
                        if !matches!(result,Some(Ok(Ok(())))) {
                            let error=match result {
                                Some(Ok(Err(error)))=>error,
                                _=>Error::Protocol("lifecycle callback task panicked"),
                            };
                            task_shared.authority.retire();
                            task_shared.connection.abort_with_error(error.transport_fault()); break;
                        }
                    }
                    request=incoming.recv() => {
                        let Some(request)=request else { break };
                        if *stopped.borrow() || task_shared.authority.check().is_err() { break }
                        #[cfg(test)]
                        task_shared.test_hooks.reach(tests::DispatchPoint::BeforeReservation).await;
                        let permit=match task_shared.callbacks.clone().try_acquire_owned() {
                            Ok(p)=>p,
                            Err(_)=>{task_shared.authority.retire();task_shared.connection.abort();break}
                        };
                        let call_permit=match task_shared.execution.reserve(
                            crate::extensions::CallLane::Native,
                            crate::extensions::CallOrigin::default(), CALLBACK_TIMEOUT, false) {
                            Ok(permit)=>permit,
                            Err(error)=>{
                                // The selected request passed admission before
                                // orderly retirement raced reserve. No callback
                                // was accepted; do not poison unregister IPC.
                                if !(*stopped.borrow() && matches!(error,Error::Closed)) {
                                    task_shared.authority.retire();
                                    task_shared.connection.abort();
                                }
                                break;
                            }
                        };
                        task_shared.active.fetch_add(1,Ordering::SeqCst);
                        let state=task_shared.clone();
                        let call=call_permit.run_future(async move {
                            let _permit=permit;
                            #[cfg(test)]
                            state.test_hooks.reach(tests::DispatchPoint::BeforeDispatch).await;
                            dispatch(state,request).await
                        });
                        dispositions.retain(|record| !record.is_finished());
                        dispositions.push(call.disposition());
                        work.spawn(call);
                    }
                }
            }
            // Dropping callback futures is never accepted as proof of cleanup.
            // `quiesce` refuses successful cleanup when any were active.
            work.abort_all();
            while work.join_next().await.is_some() {
                task_shared.active.fetch_sub(1, Ordering::SeqCst);
            }
            for record in dispositions {
                record.cancel();
                record.wait().await;
            }
            (*stopped.borrow()).then_some(incoming)
        });
        Self {
            shared,
            stop,
            task,
            retired_incoming: None,
            active_at_stop: false,
        }
    }
    pub(crate) fn insert_factory(
        &self,
        name: String,
        factory: Arc<dyn IStatelessServiceFactory>,
    ) -> Result<()> {
        let mut slot = self.reserve_factory_name(&name)?;
        let factory =
            crate::managed_participants::stateless_factory(&self.shared.execution, factory)?;
        self.shared.factories.lock().unwrap().insert(name, factory);
        slot.committed = true;
        Ok(())
    }
    pub(crate) fn insert_stateful_factory(
        &self,
        name: String,
        factory: Arc<dyn crate::IStatefulServiceFactory>,
    ) -> Result<()> {
        let mut slot = self.reserve_factory_name(&name)?;
        self.shared.stateful.insert_factory(name, factory)?;
        slot.committed = true;
        Ok(())
    }
    pub(crate) fn begin_quiesce(&mut self) {
        self.active_at_stop |= self.shared.execution.has_running_callbacks();
        self.stop.send_replace(true);
    }
    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<crate::RuntimeEvent> {
        self.shared.events.subscribe()
    }
    pub(crate) async fn quiesce(&mut self) -> Result<()> {
        self.begin_quiesce();
        // Late one-way native commands must not poison the connection while
        // Runtime still needs it for unregistration. The queue stays bounded.
        self.retired_incoming = (&mut self.task)
            .await
            .map_err(|_| Error::Protocol("lifecycle dispatcher panic"))?;
        let _ = self
            .shared
            .events
            .send(crate::RuntimeEvent::DispatchStopped);
        if self.active_at_stop || self.shared.execution.has_running_callbacks() {
            return Err(Error::Protocol(
                "shutdown with active callbacks requires process fencing",
            ));
        }
        let services: Vec<_> = self
            .shared
            .units
            .lock()
            .unwrap()
            .values()
            .map(|unit| {
                let mut state = unit.state.lock().unwrap();
                state.valid.store(false, Ordering::Release);
                state.cancel.cancel();
                state.phase = Phase::Closed;
                state.live = None;
                (state.service.take(), state.reporting.clone())
            })
            .collect();
        for (service, reporting) in services {
            if let Some(reporting) = reporting {
                reporting.quiesce().await?;
            }
            if let Some(service) = service {
                abort(service, self.shared.execution.clone()).await?;
            }
        }
        self.shared.stateful.quiesce().await?;
        let factories = std::mem::take(&mut *self.shared.factories.lock().unwrap());
        self.shared
            .execution
            .reserve_cleanup(Duration::from_millis(250))?
            .dispose(factories)
            .wait()
            .await?;
        Ok(())
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.task.abort();
    }
}

fn validate(shared: &Shared, request: &ProxyRequest) -> Result<()> {
    if request.runtime != shared.activation.runtime.id
        || request.service.stateful
        || request.service.persisted
        || request.service.group
        || !request.remote.0.is_empty()
        || request.service.service_type.package != shared.activation.runtime.code.id.package.package
        || request.service.application != shared.activation.application_name
        || request.local.node.id.0.len() != 16
        || request.flags & !15 != 0
    {
        return Err(Error::Protocol(
            "unsupported/mismatched stateless command context",
        ));
    }
    match request.unit.consistency.kind {
        1 | 3 => {}
        2 if request.unit.consistency.low <= request.unit.consistency.high => {}
        _ => return Err(Error::Protocol("invalid partition kind/range")),
    }
    Ok(())
}
fn epoch(request: &ProxyRequest) -> (i64, i64) {
    (
        request.unit.current.data_loss,
        request.unit.current.configuration,
    )
}
fn stale(current: &ProxyRequest, incoming: &ProxyRequest) -> bool {
    epoch(current) > epoch(incoming)
        || current.local.instance > incoming.local.instance
        || (current.local.instance == incoming.local.instance
            && current.local.id != incoming.local.id)
}
fn unit(shared: &Shared, request: &ProxyRequest, create: bool) -> Result<Option<Arc<Unit>>> {
    let mut units = shared.units.lock().unwrap();
    if let Some(unit) = units.get(&request.unit.id.id.0) {
        return Ok(Some(unit.clone()));
    }
    if !create {
        return Ok(None);
    }
    if units.len() >= MAX_TOMBSTONES {
        return Err(Error::ResourceLimit("service tombstones"));
    }
    let live = units
        .values()
        .filter(|u| !matches!(u.state.lock().unwrap().phase, Phase::Closed))
        .count();
    if live >= MAX_SERVICE_UNITS {
        return Err(Error::ResourceLimit("service units"));
    }
    let unit = Arc::new(Unit {
        operation: Arc::new(Semaphore::new(1)),
        state: Mutex::new(Snapshot {
            request: request.clone(),
            phase: Phase::Absent,
            service: None,
            endpoint: String::new(),
            valid: Arc::new(AtomicBool::new(false)),
            cancel: Cancellation::default(),
            generation: 0,
            operation_start: 0,
            operation_kind: 0,
            live: None,
            reporting: None,
        }),
    });
    units.insert(request.unit.id.id.0, unit.clone());
    Ok(Some(unit))
}
async fn dispatch(shared: Arc<Shared>, incoming: IpcRequest) -> Result<()> {
    let props = incoming.message.properties()?;
    let request: ProxyRequest = from_slice(&incoming.message.body)?;
    let owner = {
        let mut owners = shared.owners.lock().unwrap();
        let id = request.unit.id.id.0;
        match owners.get(&id).copied() {
            Some(stateful) => {
                if matches!(props.action.as_str(), "ReplicaOpen" | "ReplicaClose")
                    && stateful != request.service.stateful
                {
                    return Err(Error::Protocol(
                        "partition changed stateless/stateful ownership",
                    ));
                }
                Some(stateful)
            }
            None if matches!(props.action.as_str(), "ReplicaOpen" | "ReplicaClose") => {
                if owners.len() >= MAX_TOMBSTONES {
                    return Err(Error::ResourceLimit("service tombstones"));
                }
                owners.insert(id, request.service.stateful);
                Some(request.service.stateful)
            }
            None => None,
        }
    };
    if owner == Some(true) || request.service.stateful {
        let result = shared
            .stateful
            .dispatch(&props.action, request, incoming.reply)
            .await;
        if props.action == "RAPQuery" && result.is_ok() {
            let _ = shared.events.send(crate::RuntimeEvent::QueryReplySent {
                request_id: props.id,
            });
        }
        return result;
    }
    if owner.is_none()
        && matches!(
            props.action.as_str(),
            "UpdateConfiguration"
                | "ReplicatorBuildIdleReplica"
                | "ReplicatorRemoveIdleReplica"
                | "ReplicatorGetStatus"
                | "ReplicatorUpdateEpochAndGetStatus"
                | "CancelCatchupReplicaSet"
                | "ProxyReplicaEndpointUpdatedReply"
        )
    {
        // Native retries/delayed controls cannot create a missing replica.
        return Ok(());
    }
    if props.action == "RAPQuery" {
        // Native Query.HostQueryAsyncOperation uses a fully serialized DEFAULT
        // ServiceDescription, not the manifest identity from ReplicaOpen.
        // Query authority is the routed runtime + existing unit/replica, not
        // that intentionally empty service description or partition metadata.
        if request.runtime != shared.activation.runtime.id
            || request.local.node.id.0.len() != 16
            || request.flags != 0
        {
            return Err(Error::Protocol("invalid native RAPQuery context"));
        }
        shared.metrics.queries.fetch_add(1, Ordering::Relaxed);
        let reply = incoming
            .reply
            .ok_or(Error::Protocol("RAPQuery requires correlation"))?;
        let result = reply
            .reply(
                12,
                Some("RAPQueryReply"),
                to_vec(&query(&shared, &request)?)?,
                deadline_after(Duration::from_secs(5))?,
            )
            .await
            .map_err(Error::from);
        if result.is_ok() {
            let _ = shared.events.send(crate::RuntimeEvent::QueryReplySent {
                request_id: props.id,
            });
        }
        return result;
    }
    validate(&shared, &request)?;
    if incoming.reply.is_some() {
        return Err(Error::Protocol("lifecycle uses semantic one-way replies"));
    }
    if !matches!(
        props.action.as_str(),
        "ReplicaOpen" | "ReplicaClose" | "ProxyUpdateServiceDescription"
    ) {
        return Err(Error::Protocol("unsupported stateless RAP action"));
    }
    if props.action == "ReplicaOpen" {
        shared.metrics.opens.fetch_add(1, Ordering::Relaxed);
    }
    if props.action == "ReplicaClose" {
        shared.metrics.closes.fetch_add(1, Ordering::Relaxed);
        if request.flags & 4 != 0 {
            shared.metrics.aborts.fetch_add(1, Ordering::Relaxed);
        }
    }
    let create = matches!(props.action.as_str(), "ReplicaOpen" | "ReplicaClose");
    let Some(unit) = unit(&shared, &request, create)? else {
        return Ok(());
    };
    shared
        .execution
        .set_current_generation(unit.state.lock().unwrap().generation);
    {
        let state = unit.state.lock().unwrap();
        if stale(&state.request, &request) {
            return Ok(());
        }
    }
    let _operation = match unit.operation.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            // Native RA retries incompatible work. Abort/drop cancels opening;
            // never invoke a second callback while the first still owns the unit.
            if props.action == "ReplicaClose" && request.flags & 12 != 0 {
                let state = unit.state.lock().unwrap();
                state.valid.store(false, Ordering::Release);
                state.cancel.cancel();
            }
            return Ok(());
        }
    };
    // Repeat admission after obtaining operation ownership.
    if stale(&unit.state.lock().unwrap().request, &request) {
        return Ok(());
    }
    {
        let mut state = unit.state.lock().unwrap();
        if epoch(&request) > epoch(&state.request) {
            state.request.unit = request.unit.clone();
            if let Some(reporting) = &state.reporting {
                reporting.update(&state.request);
            }
        }
    }
    match props.action.as_str() {
        "ReplicaOpen" => open(&shared, &unit, request).await,
        "ReplicaClose" => close(&shared, &unit, request).await,
        "ProxyUpdateServiceDescription" => update(&shared, &unit, request).await,
        _ => unreachable!(),
    }
}
fn native_error(error: &Error) -> i64 {
    error.native_result_code()
}
async fn reply(
    shared: &Shared,
    action: &str,
    request: ProxyRequest,
    endpoint: String,
    error: Option<Error>,
    api: i64,
) -> Result<()> {
    // All successful endpoint publication, including cached duplicate Open,
    // crosses the same terminal-authority guard at the reply boundary.
    if action == "ReplicaOpenReply" && error.is_none() {
        shared.authority.check()?;
    }
    let mut local = request.local;
    local.service_endpoint = endpoint;
    let failed = error.is_some();
    let body = ProxyReply {
        unit: request.unit,
        local,
        remote: ObjectArray(vec![]),
        flags: request.flags,
        error: ProxyError {
            error: error.as_ref().map_or(0, native_error),
            message: error.map_or_else(String::new, |e| e.to_string()),
            api: ApiName {
                api: if failed { api } else { 0 },
                interface: if failed { 2 } else { 0 },
                metadata: String::new(),
            },
        },
    };
    shared
        .connection
        .send_one_way(
            12,
            action,
            to_vec(&body)?,
            deadline_after(Duration::from_secs(5))?,
        )
        .await
        .map_err(Error::from)
}
async fn abort(
    service: Arc<dyn IStatelessServiceInstance>,
    execution: crate::extensions::ExecutionRuntime,
) -> Result<()> {
    let cleanup = execution.clone();
    execution
        .invoke_cleanup(Duration::from_millis(250), async move {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service.abort()))
                .map_err(|_| Error::Protocol("service abort panicked"))?;
            cleanup
                .reserve_cleanup(Duration::from_millis(250))?
                .dispose(service)
                .wait()
                .await
        })
        .await
}
async fn abort_unit(
    shared: &Shared,
    unit: &Unit,
    service: Arc<dyn IStatelessServiceInstance>,
) -> Result<()> {
    {
        let mut state = unit.state.lock().unwrap();
        state.operation_kind = 16;
        state.operation_start = filetime();
    }
    let reporting = unit.state.lock().unwrap().reporting.clone();
    if let Some(reporting) = reporting {
        reporting.quiesce().await?;
    }
    abort(service, shared.execution.clone()).await?;
    shared
        .metrics
        .abort_callbacks
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}
async fn open(shared: &Shared, unit: &Unit, request: ProxyRequest) -> Result<()> {
    let duplicate = {
        let state = unit.state.lock().unwrap();
        match state.phase {
            Phase::Open if state.request.local.instance == request.local.instance => {
                Some(Ok(state.endpoint.clone()))
            }
            Phase::Closed if state.request.local.instance >= request.local.instance => {
                Some(Err(Error::Closed))
            }
            Phase::Absent | Phase::Closed => None,
            _ => return Ok(()),
        }
    };
    if let Some(result) = duplicate {
        return match result {
            Ok(endpoint) => reply(shared, "ReplicaOpenReply", request, endpoint, None, 1).await,
            Err(error) => {
                reply(
                    shared,
                    "ReplicaOpenReply",
                    request,
                    String::new(),
                    Some(error),
                    1,
                )
                .await
            }
        };
    }
    shared.authority.check()?;
    let (generation, cancel, valid) = {
        let mut state = unit.state.lock().unwrap();
        state.live = Some(
            shared
                .live
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::ResourceLimit("live service units"))?,
        );
        state.phase = Phase::Opening;
        state.request = request.clone();
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit("unit generations"))?;
        state.valid = Arc::new(AtomicBool::new(true));
        state.cancel = Cancellation::default();
        state.operation_start = filetime();
        state.operation_kind = 2;
        (state.generation, state.cancel.clone(), state.valid.clone())
    };
    let factory = shared
        .factories
        .lock()
        .unwrap()
        .get(&request.service.service_type.name)
        .cloned();
    shared.execution.set_current_generation(generation);
    let identity = ServiceIdentity {
        service_name: request.service.name.clone(),
        service_type: request.service.service_type.name.clone(),
        partition_id: uuid::Uuid::from_bytes_le(request.unit.id.id.0),
        instance_id: request.local.id,
        incarnation: request.local.instance,
    };
    let reporting = crate::reporting::Reporting::new(
        shared.connection.clone(),
        valid.clone(),
        shared.authority.clone(),
        request.clone(),
        cancel.clone(),
        shared.reports.clone(),
        shared.core.clone(),
    );
    unit.state.lock().unwrap().reporting = Some(reporting.clone());
    let partition = Partition {
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
        authority: shared.authority.clone(),
        reporting,
    };
    let outcome: Result<String> = async {
        let factory = factory.ok_or(Error::Remote {
            code: 0x80071bd2u32 as i32 as i64,
            message: "service type not registered".into(),
        })?;
        // Do not cancel a factory future and then call that successful cleanup.
        // Deadline/cancellation while creating instead retires the whole host.
        let initialization = request.service.initialization.0.clone();
        let (service, publication) = shared
            .execution
            .invoke_creation(CALLBACK_TIMEOUT, async move {
                let service = factory.create_instance(
                    identity.service_type,
                    identity.service_name,
                    &initialization,
                    identity.partition_id,
                    identity.instance_id,
                )?;
                Ok(Arc::<dyn IStatelessServiceInstance>::from(service))
            })
            .await?;
        {
            let mut state = unit.state.lock().unwrap();
            if state.generation != generation
                || !valid.load(Ordering::Acquire)
                || cancel.is_cancelled()
            {
                return Err(Error::Closed);
            }
            publication.accept()?;
            state.service = Some(service.clone());
        }
        let service_open = service.clone();
        let open_cancel = cancel.clone();
        let result = shared
            .execution
            .invoke(CALLBACK_TIMEOUT, async move {
                tokio::select! {
                    result=service_open.open(Arc::new(partition),open_cancel.clone())=>result,
                    _=open_cancel.cancelled()=>Err(Error::Closed),
                }
            })
            .await;
        let endpoint = result?;
        if endpoint.is_empty() {
            return Err(Error::InvalidInput("open returned empty endpoint"));
        }
        shared.authority.check()?;
        if !valid.load(Ordering::Acquire) || cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        Ok(endpoint)
    }
    .await;
    if let Err(error) = &outcome {
        // Failed generations lose report authority before application cleanup.
        // In particular, wake their retained fault worker without a retry delay.
        valid.store(false, Ordering::Release);
        cancel.cancel();
        let reporting = unit.state.lock().unwrap().reporting.clone();
        if let Some(reporting) = reporting {
            reporting.quiesce().await?;
        }
        shared.metrics.open_failures.fetch_add(1, Ordering::Relaxed);
        let service = unit.state.lock().unwrap().service.take();
        if let Some(service) = service {
            abort_unit(shared, unit, service).await?;
        } else if matches!(error, Error::Timeout) {
            // Unknown factory side effects cannot be proven disposed.
            return Err(Error::Protocol(
                "factory deadline requires terminal host fencing",
            ));
        }
    }
    let endpoint = outcome.as_ref().cloned().unwrap_or_default();
    {
        let mut state = unit.state.lock().unwrap();
        if state.generation != generation {
            return Err(Error::Protocol("stale open completion"));
        }
        state.phase = if outcome.is_ok() {
            Phase::Open
        } else {
            Phase::Closed
        };
        state.endpoint = endpoint.clone();
        state.operation_kind = 1;
        if outcome.is_err() {
            state.valid.store(false, Ordering::Release);
            state.live = None;
        }
    }
    // A terminal host may not publish even a successful obsolete completion.
    shared.authority.check()?;
    reply(
        shared,
        "ReplicaOpenReply",
        request,
        endpoint,
        outcome.err(),
        1,
    )
    .await
}
async fn close(shared: &Shared, unit: &Unit, request: ProxyRequest) -> Result<()> {
    let (service, cancel, already_closed, reporting) = {
        let mut state = unit.state.lock().unwrap();
        let closed = matches!(state.phase, Phase::Closed | Phase::Absent);
        state.valid.store(false, Ordering::Release);
        state.cancel.cancel();
        state.phase = Phase::Closing;
        state.operation_start = filetime();
        state.operation_kind = if request.flags & 4 != 0 { 16 } else { 8 };
        (
            state.service.clone(),
            Cancellation::default(),
            closed,
            state.reporting.clone(),
        )
    };
    if let Some(reporting) = reporting {
        reporting.quiesce().await?;
    }
    if !already_closed {
        if let Some(service) = service {
            if request.flags & 4 != 0 {
                abort_unit(shared, unit, service).await?;
            } else {
                let close_service = service.clone();
                let result = shared
                    .execution
                    .invoke(CALLBACK_TIMEOUT, async move {
                        close_service.close(cancel).await
                    })
                    .await;
                if result.is_err() {
                    shared
                        .metrics
                        .close_failures
                        .fetch_add(1, Ordering::Relaxed);
                    abort_unit(shared, unit, service).await?;
                }
            }
        } else {
            return Err(Error::Protocol("open service missing at close"));
        }
    }
    let disposed = {
        let mut state = unit.state.lock().unwrap();
        state.phase = Phase::Closed;
        state.endpoint.clear();
        state.live = None;
        state.request = request.clone();
        state.operation_kind = 1;
        state.service.take()
    };
    shared
        .execution
        .reserve_cleanup(Duration::from_millis(250))?
        .dispose(disposed)
        .wait()
        .await?;
    // Native normalizes a graceful-close error only after abort/disposal.
    reply(shared, "ReplicaCloseReply", request, String::new(), None, 2).await
}
async fn update(shared: &Shared, unit: &Unit, request: ProxyRequest) -> Result<()> {
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
            return Err(Error::Protocol("service update changed immutable identity"));
        }
        if request.service.update_version > state.request.service.update_version {
            state.request.service = request.service;
            if let Some(reporting) = &state.reporting {
                reporting.update(&state.request);
            }
        }
        shared.metrics.updates.fetch_add(1, Ordering::Relaxed);
        UpdateServiceReply {
            error: 0,
            unit: state.request.unit.clone(),
            local: state.request.local.clone(),
            service: state.request.service.clone(),
        }
    };
    shared
        .connection
        .send_one_way(
            12,
            "ProxyUpdateServiceDescriptionReply",
            to_vec(&response)?,
            deadline_after(Duration::from_secs(5))?,
        )
        .await
        .map_err(Error::from)
}
fn query(shared: &Shared, request: &ProxyRequest) -> Result<QueryReply> {
    let mut error = 0x80071d4au32 as i32 as i64;
    let mut local = request.local.clone();
    let mut detail = ReplicaDetail {
        kind: 0,
        service: String::new(),
        partition: Guid([0; 16]),
        replica_id: 0,
        instance_id: 0,
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
    if let Some(unit) = unit(shared, request, false)? {
        let state = unit.state.lock().unwrap();
        error = match state.phase {
            Phase::Absent => 0x80071bc7u32 as i32 as i64,
            Phase::Closed => OBJECT_CLOSED,
            _ => 0,
        };
        if stale(&state.request, request) {
            error = 0x80071d4au32 as i32 as i64;
        }
        if error == 0 {
            local = state.request.local.clone();
            local.service_endpoint = state.endpoint.clone();
            detail.kind = 1;
            detail.service = state.request.service.name.clone();
            detail.partition = state.request.unit.id.id;
            detail.replica_id = -1;
            detail.instance_id = local.id;
            detail.operation = state.operation_kind;
            detail.operation_start = state.operation_start;
            if let Some(reporting) = &state.reporting {
                detail.loads = ObjectArray(reporting.loads());
            }
        }
    }
    Ok(QueryReply {
        error,
        timestamp: stopwatch(),
        unit: request.unit.clone(),
        local,
        detail,
    })
}
pub(crate) fn filetime() -> i64 {
    ((SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        / 100)
        + 116_444_736_000_000_000)
        .min(i64::MAX as u128) as i64
}
pub(crate) fn stopwatch() -> i64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return 0;
    }
    time.tv_sec.saturating_mul(10_000_000) + time.tv_nsec / 100
}
