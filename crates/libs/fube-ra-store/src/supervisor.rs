use crate::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    thread::JoinHandle,
    time::Instant,
};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, watch, Notify};

pub(crate) struct Config {
    backend: Box<dyn Backend>,
    #[cfg(test)]
    pub(crate) hooks: Arc<crate::tests::Hooks>,
}

impl Config {
    pub(crate) fn new(backend: impl Backend + 'static) -> Self {
        Self {
            backend: Box::new(backend),
            #[cfg(test)]
            hooks: Arc::default(),
        }
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct Pending {
    status: watch::Sender<Status>,
    bytes: usize,
    deadline: Instant,
}

struct Envelope {
    id: u64,
    request: Request,
}

struct State {
    lifecycle: Lifecycle,
    bytes: usize,
    next_id: u64,
    pending: BTreeMap<u64, Arc<Pending>>,
    report: CloseReport,
}

pub(crate) struct Shared {
    state: Mutex<State>,
    wake: Notify,
    boot: watch::Sender<Option<Result<()>>>,
    done: watch::Sender<Option<Result<CloseReport>>>,
    join: Mutex<Option<JoinHandle<()>>>,
    joined: watch::Sender<Option<Result<()>>>,
    join_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Kept independently of the connection task until a positive disk-close witness.
    pub(crate) resource: Mutex<Option<Box<dyn Send>>>,
    pub(crate) pressure: AtomicBool,
    pub(crate) worker_lost: AtomicBool,
    #[cfg(test)]
    pub(crate) hooks: Arc<crate::tests::Hooks>,
}

struct Client {
    shared: Arc<Shared>,
    sender: mpsc::Sender<Envelope>,
    settings: Settings,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shared.request_close();
    }
}

/// Clones share one admission boundary, owned supervisor and backend.
#[derive(Clone)]
pub struct Store {
    client: Arc<Client>,
}

impl Shared {
    pub(crate) fn failure(&self, error: Error) {
        lock(&self.state).report.first_failure.get_or_insert(error);
    }

    pub(crate) fn mark_worker_lost(&self) {
        self.worker_lost.store(true, Ordering::Release);
        let mut state = lock(&self.state);
        state.lifecycle = Lifecycle::Fenced;
        state.report.first_failure.get_or_insert_with(|| {
            Error::new(
                ErrorKind::Unknown,
                "backend worker lost; ownership quarantined",
            )
        });
        drop(state);
        self.wake.notify_one();
    }

    fn request_close(&self) {
        let mut state = lock(&self.state);
        if matches!(state.lifecycle, Lifecycle::Starting | Lifecycle::Open) {
            state.lifecycle = Lifecycle::Closing;
        }
        drop(state);
        self.wake.notify_one();
    }

    fn finish(&self, id: u64, result: Result<Output>) {
        let mut state = lock(&self.state);
        let pending = state
            .pending
            .remove(&id)
            .expect("accepted operation registered");
        state.bytes -= pending.bytes;
        match &result {
            Ok(_) => state.report.succeeded += 1,
            Err(error) => {
                if error.kind == ErrorKind::NotExecuted {
                    state.report.not_executed += 1;
                } else {
                    state.report.failed += 1;
                }
                state
                    .report
                    .first_failure
                    .get_or_insert_with(|| error.clone());
            }
        }
        pending
            .status
            .send_replace(Status::Complete(result.map(Arc::new)));
    }

    fn fence(&self, id: u64) {
        let error = Error::new(
            ErrorKind::Unknown,
            "operation supervision expired; worker still owned",
        );
        let mut state = lock(&self.state);
        state.lifecycle = Lifecycle::Fenced;
        state
            .report
            .first_failure
            .get_or_insert_with(|| error.clone());
        if let Some(p) = state.pending.get(&id) {
            p.status.send_replace(Status::Unknown(error));
        }
    }
}

impl Store {
    /// Initialize and exclusively own a caller-selected backend under supervision.
    /// Accepts concrete engines or `Box<dyn Backend>`; initialization outlives
    /// cancellation of this constructor, just like the convenience constructors.
    pub async fn new(backend: impl Backend + 'static, settings: Settings) -> Result<Self> {
        Self::start(settings, Config::new(backend)).await
    }

    pub async fn memory(settings: Settings) -> Result<Self> {
        Self::new(MemoryBackend::new(), settings).await
    }

    /// Atomically reserve a new database path. Never opens an existing file.
    pub async fn create_new(
        path: impl AsRef<Path>,
        identity: Identity,
        settings: Settings,
    ) -> Result<Self> {
        Self::new(SqliteBackend::create_new(path, identity), settings).await
    }

    /// Require a compatible initialized store; never creates/reinitializes one.
    pub async fn open_existing(
        path: impl AsRef<Path>,
        identity: Identity,
        settings: Settings,
    ) -> Result<Self> {
        Self::new(SqliteBackend::existing(path, identity), settings).await
    }

    pub(crate) async fn start(settings: Settings, config: Config) -> Result<Self> {
        settings.validate()?;
        let (sender, receiver) = mpsc::channel(settings.max_outstanding);
        let (boot, _) = watch::channel(None);
        let (done, _) = watch::channel(None);
        let (joined, _) = watch::channel(None);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                lifecycle: Lifecycle::Starting,
                bytes: 0,
                next_id: 1,
                pending: BTreeMap::new(),
                report: CloseReport::default(),
            }),
            wake: Notify::new(),
            boot,
            done,
            join: Mutex::new(None),
            joined,
            join_task: Mutex::new(None),
            resource: Mutex::new(None),
            pressure: AtomicBool::new(false),
            worker_lost: AtomicBool::new(false),
            #[cfg(test)]
            hooks: config.hooks.clone(),
        });
        let store = Self {
            client: Arc::new(Client {
                shared: shared.clone(),
                sender,
                settings: settings.clone(),
            }),
        };
        let owner = shared.clone();
        let handle = std::thread::Builder::new()
            .name("ra-store-supervisor".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                        .map_err(|e| Error::new(ErrorKind::Io, e.to_string()))?;
                    runtime.block_on(run(owner.clone(), receiver, settings, config))
                }));
                let error = match result {
                    Ok(Ok(report)) => {
                        #[cfg(test)]
                        owner.hooks.cleanup_blocking();
                        owner.done.send_replace(Some(Ok(report)));
                        None
                    }
                    Ok(Err(e)) => Some(e),
                    Err(_) => Some(Error::new(
                        ErrorKind::Unknown,
                        "supervisor panic; ownership quarantined",
                    )),
                };
                if let Some(error) = error {
                    let mut state = lock(&owner.state);
                    state.lifecycle = Lifecycle::Fenced;
                    for pending in state.pending.values() {
                        pending.status.send_replace(Status::Unknown(error.clone()));
                    }
                    owner.boot.send_replace(Some(Err(error.clone())));
                    owner.done.send_replace(Some(Err(error)));
                }
                if lock(&owner.resource).is_some() || owner.worker_lost.load(Ordering::Acquire) {
                    // Unjoined/unwitnessed engine loss must not release the cooperative lease.
                    // This bounded failed-store quarantine lasts until process exit.
                    std::mem::forget(owner);
                }
            })
            .map_err(|e| Error::new(ErrorKind::Io, e.to_string()))?;
        *lock(&shared.join) = Some(handle);
        let mut boot = shared.boot.subscribe();
        let wait = async {
            loop {
                if let Some(result) = boot.borrow().clone() {
                    return result;
                }
                boot.changed()
                    .await
                    .map_err(|_| Error::new(ErrorKind::Unknown, "bootstrap owner lost"))?;
            }
        };
        tokio::time::timeout(store.client.settings.bootstrap_timeout, wait)
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unknown,
                    "bootstrap pending; cleanup remains owned",
                )
            })??;
        Ok(store)
    }

    /// Fail-fast bounded admission. Once accepted, work outlives this call/ticket.
    pub fn submit(&self, request: Request) -> Result<Operation> {
        let bytes = request.charge(&self.client.settings)?;
        let mut state = lock(&self.client.shared.state);
        match state.lifecycle {
            Lifecycle::Open => {}
            Lifecycle::Fenced => return Err(Error::new(ErrorKind::Fenced, "store fenced")),
            _ => return Err(Error::new(ErrorKind::Closed, "admission closed")),
        }
        if matches!(
            request,
            Request::Insert(_) | Request::Update(_) | Request::Delete(_)
        ) && self.client.shared.pressure.load(Ordering::Acquire)
        {
            return Err(Error::new(
                ErrorKind::Capacity,
                "storage pressure; Maintain required",
            ));
        }
        if state.pending.len() >= self.client.settings.max_outstanding
            || bytes
                > self
                    .client
                    .settings
                    .max_queued_bytes
                    .saturating_sub(state.bytes)
        {
            return Err(Error::new(
                ErrorKind::Capacity,
                "outstanding operation/byte budget",
            ));
        }
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorKind::Capacity, "operation IDs exhausted"))?;
        let (status, rx) = watch::channel(Status::Pending);
        let pending = Arc::new(Pending {
            status,
            bytes,
            deadline: Instant::now() + self.client.settings.operation_timeout,
        });
        self.client
            .sender
            .try_send(Envelope { id, request })
            .map_err(|_| Error::new(ErrorKind::Closed, "supervisor queue unavailable"))?;
        state.pending.insert(id, pending);
        state.bytes += bytes;
        state.report.accepted += 1;
        Ok(Operation { id, rx })
    }

    pub fn stats(&self) -> Stats {
        let state = lock(&self.client.shared.state);
        Stats {
            lifecycle: state.lifecycle,
            outstanding: state.pending.len(),
            queued_bytes: state.bytes,
        }
    }

    /// Drain and observe cleanup finitely, without cancelling the owned cleanup.
    /// Inspect the report's first failure even when this method returns Ok.
    pub async fn close(&self) -> Result<CloseReport> {
        self.client.shared.request_close();
        let mut done = self.client.shared.done.subscribe();
        let wait = async {
            let report = loop {
                if let Some(result) = done.borrow().clone() {
                    break result;
                }
                done.changed()
                    .await
                    .map_err(|_| Error::new(ErrorKind::Unknown, "close owner lost"))?;
            };
            let shared = &self.client.shared;
            let mut joined = shared.joined.subscribe();
            {
                let mut joining = lock(&shared.join_task);
                if joining.is_none() {
                    if let Some(handle) = lock(&shared.join).take() {
                        let owner = shared.clone();
                        *joining = Some(tokio::task::spawn_blocking(move || {
                            let result = handle.join().map_err(|_| {
                                Error::new(ErrorKind::Unknown, "supervisor join failed")
                            });
                            owner.joined.send_replace(Some(result));
                        }));
                    }
                }
            }
            loop {
                if let Some(result) = joined.borrow().clone() {
                    result?;
                    break;
                }
                joined
                    .changed()
                    .await
                    .map_err(|_| Error::new(ErrorKind::Unknown, "join owner lost"))?;
            }
            report
        };
        tokio::time::timeout(self.client.settings.close_timeout, wait)
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unknown,
                    "close pending; worker cannot be killed safely",
                )
            })?
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.client.shared
    }
}

impl KeyValueStore for Store {
    fn submit(&self, request: Request) -> Result<Operation> {
        self.submit(request)
    }
    fn stats(&self) -> Stats {
        self.stats()
    }
    fn close(&self) -> StoreFuture<'_, CloseReport> {
        Box::pin(self.close())
    }
}

fn verify_cleanup(shared: &Shared) -> Result<()> {
    if shared.worker_lost.load(Ordering::Acquire) || lock(&shared.resource).is_some() {
        return Err(Error::new(
            ErrorKind::Unknown,
            "backend close did not witness release of owned resources",
        ));
    }
    Ok(())
}

async fn run(
    shared: Arc<Shared>,
    mut receiver: mpsc::Receiver<Envelope>,
    settings: Settings,
    config: Config,
) -> Result<CloseReport> {
    let mut backend = config.backend;
    let context = BackendContext {
        shared: shared.clone(),
    };
    if let Err(error) = backend.initialize(settings, context).await {
        if let Err(cleanup) = backend.close().await.and_then(|()| verify_cleanup(&shared)) {
            return Err(Error::new(
                cleanup.kind,
                format!("backend initialization failed: {error}; cleanup failed: {cleanup}"),
            ));
        }
        return Err(error);
    }
    {
        let mut state = lock(&shared.state);
        if state.lifecycle == Lifecycle::Starting {
            state.lifecycle = Lifecycle::Open;
        }
    }
    shared.boot.send_replace(Some(Ok(())));
    loop {
        let envelope = {
            let finished = {
                let state = lock(&shared.state);
                state.pending.is_empty() && state.lifecycle != Lifecycle::Open
            };
            if finished {
                break;
            }
            tokio::select! {
                value = receiver.recv() => match value { Some(e) => e, None => break },
                _ = shared.wake.notified() => continue,
            }
        };
        if lock(&shared.state).lifecycle == Lifecycle::Fenced {
            shared.finish(
                envelope.id,
                Err(Error::new(ErrorKind::NotExecuted, "fenced before dispatch")),
            );
            continue;
        }
        let deadline = lock(&shared.state).pending[&envelope.id].deadline;
        if Instant::now() >= deadline {
            shared.fence(envelope.id);
            shared.finish(
                envelope.id,
                Err(Error::new(
                    ErrorKind::NotExecuted,
                    "deadline before dispatch",
                )),
            );
            continue;
        }
        #[cfg(test)]
        if shared.hooks.supervisor_panic.swap(false, Ordering::SeqCst) {
            panic!("injected supervisor loss");
        }
        let execution = backend.execute(envelope.request);
        tokio::pin!(execution);
        let result = tokio::select! {
            result = &mut execution => result,
            _ = tokio::time::sleep_until(deadline.into()) => {
                shared.fence(envelope.id);
                while let Ok(queued) = receiver.try_recv() {
                    shared.finish(queued.id, Err(Error::new(ErrorKind::NotExecuted, "fenced before dispatch")));
                }
                // Keep polling the SAME owned work. No abort or racing replacement.
                execution.await
            }
        };
        if result.as_ref().is_err_and(|e| e.kind == ErrorKind::Unknown) {
            shared.fence(envelope.id);
        }
        if shared.worker_lost.load(Ordering::Acquire) {
            // Transport loss does not prove disposal/quiescence. Keep this charge.
            while let Ok(queued) = receiver.try_recv() {
                shared.finish(
                    queued.id,
                    Err(Error::new(
                        ErrorKind::NotExecuted,
                        "worker lost before dispatch",
                    )),
                );
            }
            return Err(Error::new(
                ErrorKind::Unknown,
                "worker lost; lease and outstanding ownership quarantined",
            ));
        }
        shared.finish(envelope.id, result);
    }
    backend.close().await?;
    verify_cleanup(&shared)?;
    lock(&shared.state).lifecycle = Lifecycle::Closed;
    let report = lock(&shared.state).report.clone();
    Ok(report)
}
