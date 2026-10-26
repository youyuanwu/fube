use crate::{api::*, lease::Authority, wire::*, ActivationContext};
#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
use mssf_transport::{
    connection::{deadline_after, Connection, IpcRequest},
    serialization::serde::{from_slice, to_vec, EmptyObjectArray, Guid, Pointer},
    Error, Result,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
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
    service: Option<Arc<dyn StatelessService>>,
    endpoint: String,
    valid: Arc<AtomicBool>,
    cancel: Cancellation,
    generation: u64,
    operation_start: i64,
    operation_kind: i64,
    live: Option<OwnedSemaphorePermit>,
}
struct Unit {
    state: Mutex<Snapshot>,
    operation: Arc<Semaphore>,
}
struct Shared {
    units: Mutex<HashMap<[u8; 16], Arc<Unit>>>,
    factories: Mutex<HashMap<String, Arc<dyn StatelessFactory>>>,
    callbacks: Arc<Semaphore>,
    live: Arc<Semaphore>,
    active: AtomicUsize,
    activation: ActivationContext,
    authority: Arc<Authority>,
    connection: Connection,
}
pub(crate) struct Engine {
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Engine {
    pub(crate) fn start(
        activation: ActivationContext,
        authority: Arc<Authority>,
        connection: Connection,
        mut incoming: mpsc::Receiver<IpcRequest>,
    ) -> Self {
        let shared = Arc::new(Shared {
            units: Mutex::new(HashMap::new()),
            factories: Mutex::new(HashMap::new()),
            callbacks: Arc::new(Semaphore::new(MAX_CALLBACKS)),
            live: Arc::new(Semaphore::new(MAX_SERVICE_UNITS)),
            active: AtomicUsize::new(0),
            activation,
            authority,
            connection,
        });
        let (stop, mut stopped) = watch::channel(false);
        let task_shared = shared.clone();
        let task = tokio::spawn(async move {
            let mut work = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _=stopped.changed() => break,
                    result=work.join_next(), if !work.is_empty() => {
                        task_shared.active.fetch_sub(1,Ordering::SeqCst);
                        if !matches!(result,Some(Ok(Ok(())))) {
                            task_shared.authority.retire(); task_shared.connection.abort(); break;
                        }
                    }
                    request=incoming.recv() => {
                        let Some(request)=request else { break };
                        if task_shared.authority.check().is_err() { break }
                        let permit=match task_shared.callbacks.clone().try_acquire_owned() {
                            Ok(p)=>p,
                            Err(_)=>{task_shared.authority.retire();task_shared.connection.abort();break}
                        };
                        task_shared.active.fetch_add(1,Ordering::SeqCst);
                        let state=task_shared.clone();
                        work.spawn(async move { let _permit=permit; dispatch(state,request).await });
                    }
                }
            }
            // Dropping callback futures is never accepted as proof of cleanup.
            // `quiesce` refuses successful cleanup when any were active.
            work.abort_all();
        });
        Self { shared, stop, task }
    }
    pub(crate) fn insert_factory(
        &self,
        name: String,
        factory: Arc<dyn StatelessFactory>,
    ) -> Result<()> {
        let mut factories = self.shared.factories.lock().unwrap();
        if factories.contains_key(&name) {
            return Err(Error::InvalidInput(
                "service type already registered locally",
            ));
        }
        if factories.len() >= 32 {
            return Err(Error::ResourceLimit("service factories"));
        }
        factories.insert(name, factory);
        Ok(())
    }
    pub(crate) async fn quiesce(&mut self) -> Result<()> {
        let active = self.shared.active.load(Ordering::SeqCst);
        self.stop.send_replace(true);
        (&mut self.task)
            .await
            .map_err(|_| Error::Protocol("lifecycle dispatcher panic"))?;
        if active != 0 || self.shared.active.load(Ordering::SeqCst) != 0 {
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
            .filter_map(|unit| {
                let mut state = unit.state.lock().unwrap();
                state.valid.store(false, Ordering::Release);
                state.cancel.cancel();
                state.phase = Phase::Closed;
                state.live = None;
                state.service.take()
            })
            .collect();
        for service in services {
            abort(service).await?;
        }
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
        }),
    });
    units.insert(request.unit.id.id.0, unit.clone());
    Ok(Some(unit))
}
async fn dispatch(shared: Arc<Shared>, incoming: IpcRequest) -> Result<()> {
    let props = incoming.message.properties()?;
    let request: ProxyRequest = from_slice(&incoming.message.body)?;
    validate(&shared, &request)?;
    if props.action == "RAPQuery" {
        let reply = incoming
            .reply
            .ok_or(Error::Protocol("RAPQuery requires correlation"))?;
        return reply
            .reply(
                12,
                Some("RAPQueryReply"),
                to_vec(&query(&shared, &request)?)?,
                deadline_after(Duration::from_secs(5))?,
            )
            .await;
    }
    if incoming.reply.is_some() {
        return Err(Error::Protocol("lifecycle uses semantic one-way replies"));
    }
    if !matches!(
        props.action.as_str(),
        "ReplicaOpen" | "ReplicaClose" | "ProxyUpdateServiceDescription"
    ) {
        return Err(Error::Protocol("unsupported stateless RAP action"));
    }
    let create = matches!(props.action.as_str(), "ReplicaOpen" | "ReplicaClose");
    let Some(unit) = unit(&shared, &request, create)? else {
        return Ok(());
    };
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
    match error {
        Error::Remote { code, .. } => *code,
        Error::Closed => OBJECT_CLOSED,
        Error::Timeout => 0x80071bffu32 as i32 as i64,
        _ => 0x80004005u32 as i32 as i64,
    }
}
async fn reply(
    shared: &Shared,
    action: &str,
    request: ProxyRequest,
    endpoint: String,
    error: Option<Error>,
    api: i64,
) -> Result<()> {
    let mut local = request.local;
    local.service_endpoint = endpoint;
    let failed = error.is_some();
    let body = ProxyReply {
        unit: request.unit,
        local,
        remote: EmptyObjectArray,
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
}
async fn abort(service: Arc<dyn StatelessService>) -> Result<()> {
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::task::spawn_blocking(move || service.abort()),
    )
    .await
    .map_err(|_| Error::Timeout)?
    .map_err(|_| Error::Protocol("service abort panicked"))?;
    Ok(())
}
async fn abort_unit(unit: &Unit, service: Arc<dyn StatelessService>) -> Result<()> {
    {
        let mut state = unit.state.lock().unwrap();
        state.operation_kind = 16;
        state.operation_start = filetime();
    }
    abort(service).await
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
    let identity = ServiceIdentity {
        service_name: request.service.name.clone(),
        service_type: request.service.service_type.name.clone(),
        partition_id: uuid::Uuid::from_bytes_le(request.unit.id.id.0),
        instance_id: request.local.id,
        incarnation: request.local.instance,
    };
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
    };
    let outcome: Result<String> = async {
        let factory = factory.ok_or(Error::Remote {
            code: 0x80071bd2u32 as i32 as i64,
            message: "service type not registered".into(),
        })?;
        // Do not cancel a factory future and then call that successful cleanup.
        // Deadline/cancellation while creating instead retires the whole host.
        let service = tokio::time::timeout(
            CALLBACK_TIMEOUT,
            factory.create(ServiceContext {
                identity,
                initialization_data: request.service.initialization.0.clone(),
                activation: shared.activation.clone(),
            }),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        {
            unit.state.lock().unwrap().service = Some(service.clone());
        }
        let result = tokio::time::timeout(CALLBACK_TIMEOUT, async {
            tokio::select! {
                result=service.open(partition,cancel.clone())=>result,
                _=cancel.cancelled()=>Err(Error::Closed),
            }
        })
        .await
        .map_err(|_| Error::Timeout)?;
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
        let service = unit.state.lock().unwrap().service.take();
        if let Some(service) = service {
            abort_unit(unit, service).await?;
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
    let (service, cancel, already_closed) = {
        let mut state = unit.state.lock().unwrap();
        let closed = matches!(state.phase, Phase::Closed | Phase::Absent);
        state.valid.store(false, Ordering::Release);
        state.cancel.cancel();
        state.phase = Phase::Closing;
        state.operation_start = filetime();
        state.operation_kind = if request.flags & 4 != 0 { 16 } else { 8 };
        (state.service.clone(), Cancellation::default(), closed)
    };
    if !already_closed {
        if let Some(service) = service {
            if request.flags & 4 != 0 {
                abort_unit(unit, service).await?;
            } else {
                let result = tokio::time::timeout(CALLBACK_TIMEOUT, service.close(cancel)).await;
                if !matches!(result, Ok(Ok(()))) {
                    abort_unit(unit, service).await?;
                }
            }
        } else {
            return Err(Error::Protocol("open service missing at close"));
        }
    }
    {
        let mut state = unit.state.lock().unwrap();
        state.phase = Phase::Closed;
        state.endpoint.clear();
        state.service = None;
        state.live = None;
        state.request = request.clone();
        state.operation_kind = 1;
    }
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
        }
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
        loads: EmptyObjectArray,
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
        if stale(&state.request, request)
            || state.request.local.id != request.local.id
            || state.request.local.instance != request.local.instance
        {
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
fn filetime() -> i64 {
    ((SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        / 100)
        + 116_444_736_000_000_000)
        .min(i64::MAX as u128) as i64
}
fn stopwatch() -> i64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return 0;
    }
    time.tv_sec.saturating_mul(10_000_000) + time.tv_nsec / 100
}
