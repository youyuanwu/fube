//! Opt-in, volatile qualification fixture; HTTP is never the replication wire.
use super::*;
use fube_data::{
    wire::Actor, Engine, EngineSnapshot, MemoryProvider, Mutation, Operation, OperationData,
    OperationDataStream, OperationStream, Replication, Settings, StateProvider,
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{atomic::AtomicU64, Weak},
    thread::JoinHandle,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[cfg(test)]
#[path = "data_tests.rs"]
mod tests;

const PROFILE: &str = "fube-data-v1";
const REQUEST_LIMIT: usize = 4096;
const WRITE_WAIT: Duration = Duration::from_secs(5);

pub(super) fn selected(initialization_data: &[u8]) -> bool {
    initialization_data == PROFILE.as_bytes()
}

trait NativeView: Send + Sync {
    fn snapshot(&self) -> RuntimeResult<ReplicaStatus>;
}
impl NativeView for StatefulPartition {
    fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
        self.status()
    }
}

fn authoritative(native: &ReplicaStatus, engine: &EngineSnapshot, write: bool) -> bool {
    let access = native.access();
    native.lifecycle == ReplicaLifecycle::Open
        && native.replicator_open
        && native.current_role == ReplicaRole::Primary
        && native.service_role == ReplicaRole::Primary
        && native.replicator_role == ReplicaRole::Primary
        && engine.role == ReplicaRole::Primary
        && engine.ready
        && !engine.closed
        && engine.fault.is_none()
        && engine.epoch == Some(native.current_epoch.primary())
        && if write {
            access.1 == AccessStatus::Granted
        } else {
            access.0 == AccessStatus::Granted
        }
}

fn check_authority(
    native: &dyn NativeView,
    engine: &Engine,
    stopped: &Cancellation,
    write: bool,
) -> RuntimeResult<ReplicaStatus> {
    if stopped.is_cancelled() {
        return Err(RuntimeError::Closed);
    }
    let status = native.snapshot()?;
    if !authoritative(&status, &engine.snapshot(), write) {
        return Err(RuntimeError::Closed);
    }
    Ok(status)
}

enum Request {
    Read,
    Diagnostics,
    Write(Mutation),
    Test(u64),
    CopyTest(u64),
    WireProbe(String),
    DataLoss(i64),
}

fn decode(value: &str) -> RuntimeResult<String> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        decoded.push(match byte {
            b'%' => {
                let digit = |byte: Option<u8>| {
                    byte.and_then(|v| (v as char).to_digit(16))
                        .ok_or(RuntimeError::InvalidInput("invalid percent encoding"))
                };
                (digit(bytes.next())? * 16 + digit(bytes.next())?) as u8
            }
            b'+' => b' ',
            other => other,
        });
    }
    String::from_utf8(decoded).map_err(|_| RuntimeError::InvalidInput("invalid query UTF-8"))
}

fn parse(operation: &str, query: &str) -> RuntimeResult<Request> {
    if query.len() > REQUEST_LIMIT {
        return Err(RuntimeError::InvalidInput("data query limit"));
    }
    let mut parameters = HashMap::new();
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair
                .split_once('=')
                .ok_or(RuntimeError::InvalidInput("invalid query parameter"))?;
            if parameters.insert(decode(key)?, decode(value)?).is_some() {
                return Err(RuntimeError::InvalidInput("duplicate query parameter"));
            }
        }
    }
    match operation {
        "service" if parameters.is_empty() => Ok(Request::Read),
        "diagnostics" if parameters.is_empty() => Ok(Request::Diagnostics),
        "write" if parameters.len() == 3 => {
            let mut take = |name| {
                parameters
                    .remove(name)
                    .ok_or(RuntimeError::InvalidInput("missing write parameter"))
            };
            let mutation = Mutation {
                id: take("id")?,
                key: take("key")?,
                value: take("value")?,
            };
            mutation.operation()?;
            Ok(Request::Write(mutation))
        }
        "test" if parameters.len() == 1 => {
            if let Some(value) = parameters.get("wire_probe") {
                if matches!(
                    value.as_str(),
                    "stale_epoch" | "wrong_sender" | "stale_copy_ack" | "wrong_target"
                ) {
                    return Ok(Request::WireProbe(value.clone()));
                }
                return Err(RuntimeError::InvalidInput("unsupported bounded wire probe"));
            }
            if let Some(value) = parameters.get("data_loss_boundary") {
                let boundary = value
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .filter(|_| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                    .ok_or(RuntimeError::InvalidInput(
                        "invalid data loss qualification boundary",
                    ))?;
                return Ok(Request::DataLoss(boundary));
            }
            if let Some(value) = parameters.get("copy_delay_ms") {
                let delay = value
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n <= 100)
                    .filter(|_| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                    .ok_or(RuntimeError::InvalidInput("copy delay must be 0..100 ms"))?;
                return Ok(Request::CopyTest(delay));
            }
            let delay = parameters
                .get("ack_delay_ms")
                .filter(|s| !s.is_empty() && s.bytes().all(|v| v.is_ascii_digit()))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|v| *v <= 2000)
                .ok_or(RuntimeError::InvalidInput("ACK delay must be 0..2000 ms"))?;
            Ok(Request::Test(delay))
        }
        _ => Err(RuntimeError::InvalidInput(
            "unsupported data operation/parameters",
        )),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Committed,
    Ambiguous,
    Rejected,
}
#[derive(Debug)]
struct WriteResult {
    outcome: Outcome,
    request_id: String,
    sequence: Option<i64>,
    duplicate: bool,
    error: Option<String>,
}
impl WriteResult {
    fn rejected(id: String, error: impl ToString) -> Self {
        Self {
            outcome: Outcome::Rejected,
            request_id: id,
            sequence: None,
            duplicate: false,
            error: Some(error.to_string()),
        }
    }
    fn accepted(id: String, sequence: i64, duplicate: bool, committed: bool) -> Self {
        Self {
            outcome: if committed {
                Outcome::Committed
            } else {
                Outcome::Ambiguous
            },
            request_id: id,
            sequence: Some(sequence),
            duplicate,
            error: None,
        }
    }
    fn json(&self) -> serde_json::Value {
        let outcome = match self.outcome {
            Outcome::Committed => "committed",
            Outcome::Ambiguous => "ambiguous",
            Outcome::Rejected => "rejected",
        };
        let mut value = serde_json::json!({
            "outcome":outcome,"request_id":self.request_id,"duplicate":self.duplicate
        });
        if let Some(sequence) = self.sequence {
            value["sequence"] = sequence.into();
        }
        if let Some(error) = &self.error {
            value["error"] = error.clone().into();
        }
        value
    }
}

enum Admission {
    Result(WriteResult),
    Accepted(Replication),
}
struct Submit {
    mutation: Mutation,
    reply: oneshot::Sender<Admission>,
}

// This future is owned by the joined pump, not by an HTTP waiter. Once submit
// starts, disconnect/cancellation cannot drop its accepted LSN before local apply.
async fn admit(
    native: &dyn NativeView,
    engine: &Engine,
    provider: &MemoryProvider,
    stopped: &Cancellation,
    mutation: &Mutation,
) -> Admission {
    let reject = |error| Admission::Result(WriteResult::rejected(mutation.id.clone(), error));
    if let Err(error) = check_authority(native, engine, stopped, true) {
        return reject(error);
    }
    match provider.admission(mutation) {
        Ok(Some(sequence)) => {
            let committed = check_authority(native, engine, stopped, true).is_ok()
                && engine.snapshot().progress.committed >= sequence;
            return Admission::Result(WriteResult::accepted(
                mutation.id.clone(),
                sequence,
                true,
                committed,
            ));
        }
        Err(error) => return reject(error),
        Ok(None) => {}
    }
    let data = match mutation.operation() {
        Ok(data) => data,
        Err(error) => return reject(error),
    };
    if let Err(error) = check_authority(native, engine, stopped, true) {
        return reject(error);
    }
    let replication = match engine.submit(data.clone(), stopped.clone()).await {
        Ok(replication) => replication,
        Err(error) => return reject(error),
    };
    if check_authority(native, engine, stopped, true).is_err()
        || provider
            .apply_replication(replication.sequence, &data)
            .is_err()
    {
        // Never continue admission with a missing local LSN, or advertise quorum
        // success for state we could not legally apply.
        engine.abort();
        return Admission::Result(WriteResult::accepted(
            mutation.id.clone(),
            replication.sequence,
            false,
            false,
        ));
    }
    Admission::Accepted(replication)
}

struct Pump {
    stopped: Cancellation,
    thread: Option<JoinHandle<()>>,
}

/// Qualification-only copy pacing. The engine owns enumeration/deadlines; this
/// wrapper never delays transport/control callbacks or changes snapshot data.
struct CopyProvider {
    inner: Arc<dyn StateProvider>,
    delay: Arc<AtomicU64>,
}
struct CopyStream {
    inner: Arc<dyn OperationDataStream>,
    delay: Arc<AtomicU64>,
}
impl OperationDataStream for CopyStream {
    fn next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            let delay = self.delay.load(Ordering::Acquire);
            if delay > 0 {
                tokio::select! {
                    _=cancellation.cancelled()=>return Err(RuntimeError::Closed),
                    _=tokio::time::sleep(Duration::from_millis(delay))=>{},
                }
            }
            self.inner.next(cancellation).await
        })
    }
}
impl StateProvider for CopyProvider {
    fn last_committed(&self) -> RuntimeResult<i64> {
        self.inner.last_committed()
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.inner.update_epoch(epoch, previous, cancellation)
    }
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        self.inner.on_data_loss(cancellation)
    }
    fn copy_context(&self) -> RuntimeResult<Arc<dyn OperationDataStream>> {
        self.inner.copy_context()
    }
    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> RuntimeResult<Arc<dyn OperationDataStream>> {
        Ok(Arc::new(CopyStream {
            inner: self.inner.copy_state(up_to, context)?,
            delay: self.delay.clone(),
        }))
    }
}
struct PumpInput {
    native: Arc<dyn NativeView>,
    engine: Weak<Engine>,
    provider: Arc<MemoryProvider>,
    copy: Arc<OperationStream>,
    replication: Arc<OperationStream>,
    requests: mpsc::Receiver<Submit>,
    delay: Arc<AtomicU64>,
    fault: Arc<Mutex<Option<String>>>,
}
impl Pump {
    fn start(input: PumpInput, stopped: Cancellation) -> RuntimeResult<Self> {
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let stop = stopped.clone();
        let thread = std::thread::Builder::new()
            .name("fube-data-service".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                let _ = ready.send(Ok(()));
                runtime.block_on(async move {
                    let fault = input.fault.clone();
                    let engine = input.engine.clone();
                    if let Err(error) = pump(input, stop.clone()).await {
                        if !stop.is_cancelled() {
                            *fault.lock().unwrap() = Some(error.to_string());
                            if let Some(engine) = engine.upgrade() {
                                engine.abort();
                            }
                        }
                    }
                });
            })?;
        if let Err(error) = readiness
            .recv()
            .unwrap_or_else(|_| Err(std::io::Error::other("data pump startup failed")))
        {
            thread
                .join()
                .expect("data pump startup requires fatal fencing");
            return Err(error.into());
        }
        Ok(Self {
            stopped,
            thread: Some(thread),
        })
    }
    fn join(&mut self) {
        self.stopped.cancel();
        if let Some(thread) = self.thread.take() {
            thread.join().expect("data pump requires fatal fencing");
        }
    }
}
impl Drop for Pump {
    fn drop(&mut self) {
        self.join();
    }
}

async fn apply_ack(
    native: &dyn NativeView,
    provider: &MemoryProvider,
    operation: Operation,
    copy: bool,
    delay: &AtomicU64,
    stopped: &Cancellation,
) -> RuntimeResult<()> {
    if stopped.is_cancelled() {
        return Err(RuntimeError::Closed);
    }
    native.snapshot()?;
    if copy {
        provider.apply_copy(&operation.data)?;
    } else {
        provider.apply_replication(operation.sequence, &operation.data)?;
    }
    let delay = delay.load(Ordering::Acquire);
    if delay != 0 {
        tokio::select! {
            biased;
            _ = stopped.cancelled() => return Err(RuntimeError::Closed),
            _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
        }
    }
    if stopped.is_cancelled() {
        return Err(RuntimeError::Closed);
    }
    native.snapshot()?;
    operation.acknowledge()
}

async fn pump(mut input: PumpInput, stopped: Cancellation) -> RuntimeResult<()> {
    // Both streams have independent EOS. In particular copy EOS never stops
    // the replication consumer. No task/capture is detached from this thread.
    let (mut copy_done, mut replication_done) = (false, false);
    // Only this loop receives from requests, serializing admission + local apply.
    loop {
        tokio::select! {
            biased;
            _ = stopped.cancelled() => return Ok(()),
            item = input.copy.next(stopped.clone()), if !copy_done => {
                match item? {
                    Some(operation) => apply_ack(input.native.as_ref(), &input.provider, operation, true, &input.delay, &stopped).await?,
                    None => copy_done = true,
                }
            }
            item = input.replication.next(stopped.clone()), if !replication_done => {
                match item? {
                    Some(operation) => apply_ack(input.native.as_ref(), &input.provider, operation, false, &input.delay, &stopped).await?,
                    None => replication_done = true,
                }
            }
            command = input.requests.recv() => {
                let Some(command) = command else { return Ok(()) };
                if command.reply.is_closed() { continue; }
                let result = if let Some(engine) = input.engine.upgrade() {
                    admit(input.native.as_ref(), &engine, &input.provider, &stopped, &command.mutation).await
                } else {
                    Admission::Result(WriteResult::rejected(command.mutation.id, RuntimeError::Closed))
                };
                let _ = command.reply.send(result);
            }
        }
    }
}

pub(super) struct Route {
    native: Arc<dyn NativeView>,
    identity: ReplicaIdentity,
    engine: Arc<Engine>,
    provider: Arc<MemoryProvider>,
    active: AtomicUsize,
    stopped: Cancellation,
    pump: Mutex<Option<Pump>>,
    requests: mpsc::Sender<Submit>,
    delay: Arc<AtomicU64>,
    copy_delay: Arc<AtomicU64>,
    fault: Arc<Mutex<Option<String>>>,
    service_address: String,
    generation: u64,
}
impl Route {
    fn start(
        native: Arc<dyn NativeView>,
        identity: ReplicaIdentity,
        engine: Arc<Engine>,
        provider: Arc<MemoryProvider>,
        service_address: String,
        generation: u64,
        copy_delay: Arc<AtomicU64>,
    ) -> RuntimeResult<Arc<Self>> {
        let (sender, receiver) = mpsc::channel(64);
        let stopped = Cancellation::default();
        let delay = Arc::new(AtomicU64::new(0));
        let fault = Arc::new(Mutex::new(None));
        let pump = Pump::start(
            PumpInput {
                native: native.clone(),
                engine: Arc::downgrade(&engine),
                provider: provider.clone(),
                copy: engine.copy_stream(),
                replication: engine.replication_stream(),
                requests: receiver,
                delay: delay.clone(),
                fault: fault.clone(),
            },
            stopped.clone(),
        )?;
        Ok(Arc::new(Self {
            native,
            identity,
            engine,
            provider,
            active: AtomicUsize::new(0),
            stopped,
            pump: Mutex::new(Some(pump)),
            requests: sender,
            delay,
            copy_delay,
            fault,
            service_address,
            generation,
        }))
    }
    fn shutdown(&self) {
        self.stopped.cancel();
        if let Some(mut pump) = self.pump.lock().unwrap().take() {
            pump.join();
        }
        self.engine.abort();
    }
    fn lifetime(&self) -> RuntimeResult<ReplicaStatus> {
        if self.stopped.is_cancelled() {
            return Err(RuntimeError::Closed);
        }
        self.native.snapshot()
    }
    fn authority(&self, write: bool) -> RuntimeResult<ReplicaStatus> {
        if self.fault.lock().unwrap().is_some() {
            return Err(RuntimeError::Closed);
        }
        check_authority(self.native.as_ref(), &self.engine, &self.stopped, write)
    }
    async fn write(&self, mutation: Mutation) -> WriteResult {
        let id = mutation.id.clone();
        if let Err(error) = self.authority(true) {
            return WriteResult::rejected(id, error);
        }
        let (reply, result) = oneshot::channel();
        if self.requests.try_send(Submit { mutation, reply }).is_err() {
            return WriteResult::rejected(id, "data admission unavailable");
        }
        let mut result = match result.await {
            Ok(Admission::Result(result)) => result,
            Ok(Admission::Accepted(mut replication)) => {
                let committed = matches!(
                    tokio::time::timeout(WRITE_WAIT, replication.wait(self.stopped.clone())).await,
                    Ok(Ok(_))
                );
                WriteResult::accepted(id, replication.sequence, false, committed)
            }
            Err(error) => WriteResult::rejected(id, error),
        };
        self.fence_result(&mut result);
        result
    }
    fn fence_result(&self, result: &mut WriteResult) {
        if result.sequence.is_some() && self.authority(true).is_err() {
            result.outcome = Outcome::Ambiguous;
        }
    }
}
impl Drop for Route {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(super) struct Service {
    rpc: Arc<Rpc>,
    context: StatefulServiceContext,
    path: String,
    incarnation: Uuid,
    opened: AtomicBool,
    route: Mutex<Option<Arc<Route>>>,
}
impl Service {
    pub(super) fn new(rpc: Arc<Rpc>, context: StatefulServiceContext) -> Arc<Self> {
        let incarnation = Uuid::new_v4();
        Arc::new(Self {
            path: format!(
                "/data/{}/{}/{}/{}-{incarnation}",
                context.identity.partition_id,
                context.identity.replica_id,
                context.identity.incarnation,
                context.activation.runtime_id()
            ),
            rpc,
            context,
            incarnation,
            opened: AtomicBool::new(false),
            route: Mutex::new(None),
        })
    }
    fn remove(&self) -> Option<Arc<Route>> {
        let route = self.route.lock().unwrap().take()?;
        remove_owned(
            &mut self.rpc.data_routes.lock().unwrap(),
            &self.path,
            &route,
        );
        route.shutdown();
        Some(route)
    }
}

fn remove_owned(routes: &mut HashMap<String, Arc<Route>>, path: &str, owner: &Arc<Route>) {
    if routes
        .get(path)
        .is_some_and(|route| Arc::ptr_eq(route, owner))
    {
        routes.remove(path);
    }
}

impl StatefulService for Service {
    fn open(
        &self,
        partition: StatefulPartition,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Arc<dyn Replicator>> {
        Box::pin(async move {
            if self.opened.swap(true, Ordering::SeqCst)
                || cancellation.is_cancelled()
                || partition.identity() != &self.context.identity
            {
                return Err(RuntimeError::Closed);
            }
            partition.ensure_valid()?;
            self.rpc.listen().await?;
            let provider = MemoryProvider::new();
            let copy_delay = Arc::new(AtomicU64::new(0));
            let address = SocketAddr::new(
                self.rpc
                    .ip
                    .parse::<IpAddr>()
                    .map_err(|_| RuntimeError::InvalidInput("invalid data listener IP"))?,
                0,
            );
            let engine = Engine::bind(
                Actor {
                    partition: self.context.identity.partition_id,
                    replica: self.context.identity.replica_id,
                    incarnation: self.incarnation,
                },
                &address.to_string(),
                Arc::new(CopyProvider {
                    inner: provider.clone(),
                    delay: copy_delay.clone(),
                }),
                Settings::default(),
            )?;
            let route = Route::start(
                Arc::new(partition.clone()),
                self.context.identity.clone(),
                engine.clone(),
                provider,
                format!(
                    "http://{}{}/service",
                    SocketAddr::new(address.ip(), self.rpc.port),
                    self.path
                ),
                self.context.activation.generation(),
                copy_delay,
            )?;
            if cancellation.is_cancelled() || partition.ensure_valid().is_err() {
                route.shutdown();
                return Err(RuntimeError::Closed);
            }
            self.rpc
                .data_routes
                .lock()
                .unwrap()
                .insert(self.path.clone(), route.clone());
            *self.route.lock().unwrap() = Some(route);
            Ok(engine as Arc<dyn Replicator>)
        })
    }
    fn change_role(
        &self,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            let route = self
                .route
                .lock()
                .unwrap()
                .clone()
                .ok_or(RuntimeError::Closed)?;
            if role != ReplicaRole::None {
                route.lifetime()?;
            }
            Ok(route.service_address.clone())
        })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if let Some(route) = self.remove() {
                while route.active.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            Ok(())
        })
    }
    fn abort(&self) {
        if let Some(route) = self.remove() {
            let deadline = Instant::now() + Duration::from_millis(150);
            while route.active.load(Ordering::SeqCst) != 0 {
                assert!(
                    Instant::now() < deadline,
                    "data RPC cleanup requires fencing"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

impl Rpc {
    pub(super) async fn data_request(
        &self,
        mut stream: TcpStream,
        path: &str,
        query: &str,
    ) -> RuntimeResult<()> {
        let (base, operation) = path.rsplit_once('/').unwrap_or(("", ""));
        let route = {
            let routes = self.data_routes.lock().unwrap();
            routes
                .get(base)
                .inspect(|route| {
                    route.active.fetch_add(1, Ordering::SeqCst);
                })
                .cloned()
        };
        let Some(route) = route else {
            return response(&mut stream, 404, &serde_json::json!({})).await;
        };
        struct Active(Arc<Route>);
        impl Drop for Active {
            fn drop(&mut self) {
                self.0.active.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _active = Active(route.clone());
        let request = match parse(operation, query) {
            Ok(request) => request,
            Err(error) => {
                return response(&mut stream, 400, &serde_json::json!({
                    "outcome":"rejected","request_id":null,"duplicate":false,"error":error.to_string()
                })).await;
            }
        };
        if let Request::Write(mutation) = request {
            let mut result = route.write(mutation).await;
            route.fence_result(&mut result);
            let status = match result.outcome {
                Outcome::Committed => 200,
                Outcome::Ambiguous => 202,
                Outcome::Rejected => 503,
            };
            return response(&mut stream, status, &result.json()).await;
        }
        let authoritative_read = matches!(request, Request::Read);
        let status = if authoritative_read {
            route.authority(false)
        } else {
            route.lifetime()
        };
        let Ok(status) = status else {
            return response(&mut stream, 503, &serde_json::json!({})).await;
        };
        if let Request::WireProbe(kind) = request {
            let snapshot = route.engine.snapshot();
            if snapshot.epoch != Some(status.current_epoch.primary())
                || snapshot.role != status.service_role
                || !matches!(snapshot.role, ReplicaRole::Primary | ReplicaRole::Secondary)
                || (snapshot.role == ReplicaRole::Primary && route.authority(true).is_err())
            {
                return response(&mut stream, 503, &serde_json::json!({})).await;
            }
            let result = route.engine.qualification_probe(&kind, None).await?;
            if route.lifetime().is_err() {
                return response(&mut stream, 503, &serde_json::json!({})).await;
            }
            return response(&mut stream, 200, &result).await;
        }
        if let Request::DataLoss(boundary) = request {
            route.provider.request_data_loss(boundary)?;
            return response(
                &mut stream,
                200,
                &serde_json::json!({"qualification_only":true,
                "data_loss_boundary":boundary,"armed_only":true,
                "note":"Only a later native OnDataLoss callback may change provider state."}),
            )
            .await;
        }
        let hook = match request {
            Request::Test(delay) => {
                route.delay.store(delay, Ordering::Release);
                Some(("ack_delay_ms", delay))
            }
            Request::CopyTest(delay) => {
                route.copy_delay.store(delay, Ordering::Release);
                Some(("copy_delay_ms", delay))
            }
            _ => None,
        };
        if let Some((name, delay)) = hook {
            if route.lifetime().is_err() {
                return response(&mut stream, 503, &serde_json::json!({})).await;
            }
            return response(
                &mut stream,
                200,
                &serde_json::json!({
                    "profile":PROFILE,"qualification_only":true,(name):delay
                }),
            )
            .await;
        }
        let (read, write) = status.access();
        let counters = self.metrics.snapshot();
        let id = &route.identity;
        let body = serde_json::json!({
            "profile":PROFILE,"engine_kind":"rust","partition":id.partition_id.to_string(),
            "replica":id.replica_id,"incarnation":id.incarnation,
            "engine_incarnation":route.engine.endpoint().actor.incarnation.to_string(),
            "node":self.node,"host":self.host,"pid":std::process::id(),
            "service_address":route.service_address,
            "replication_address":route.engine.endpoint().to_string(),
            "native_epoch":{"data_loss":status.current_epoch.data_loss,"configuration":status.current_epoch.configuration},
            "read_status":format!("{read:?}"),"write_status":format!("{write:?}"),
            "state":route.provider.snapshot(),"engine":route.engine.snapshot(),
            "observed_primary":route.engine.snapshot().observed_primary,
            "role":format!("{:?}",status.service_role),"current_role":format!("{:?}",status.current_role),
            "replicator_role":format!("{:?}",status.replicator_role),
            "creation_generation":route.generation,"packages":self.package_evidence()?,
            "ack_delay_ms":route.delay.load(Ordering::Acquire),
            "copy_delay_ms":route.copy_delay.load(Ordering::Acquire),"pump_fault":route.fault.lock().unwrap().clone(),
            "native_actions":{"open":counters.open_commands,"close":counters.close_commands,
                "abort":counters.abort_commands,"query":counters.queries,"update":counters.service_updates,
                "open_failures":counters.open_failures,"close_failures":counters.close_failures,
                "abort_callbacks":counters.abort_callbacks}
        });
        if (if authoritative_read {
            route.authority(false)
        } else {
            route.lifetime()
        })
        .is_err()
        {
            return response(&mut stream, 503, &serde_json::json!({})).await;
        }
        response(&mut stream, 200, &body).await
    }
}

async fn response(
    stream: &mut TcpStream,
    status: u16,
    body: &serde_json::Value,
) -> RuntimeResult<()> {
    let body = body.to_string();
    let frame = format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(frame.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}
