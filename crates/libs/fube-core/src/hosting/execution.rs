use super::{ExecutionAuthority, ExecutionDeadline, ExecutionThread, StandaloneAuthority};
use crate::{Error, Result};
use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore},
    task::{AbortHandle, Id, JoinSet},
};

const WORKERS: usize = 32;
const RECORDS: usize = 64;
// 32 admitted units * (64 retained copy streams + service/control/provider)
// plus 32 retained factories and 64 host subscriptions. Per-object quotas remain required.
const DORMANT_TICKETS: usize = 32 * (64 + 3) + 32 + 64;
#[path = "execution_owned.rs"]
mod owned;
pub use owned::PublicationBatch;
pub use owned::{
    CleanupPermit, CommandCleanup, ConstructionReservation, DeferredCleanup, DisposalBatch,
    DisposalCapture, DisposalReceipt, DisposalTracker, ManagedObject, ObjectPublication,
    ObjectQuota, ObjectReservation, TerminalPermit, TerminalRecord,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallLane {
    Native,
    Provider,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CallOrigin {
    pub engine: u64,
    pub session: u64,
    pub generation: u64,
}

#[derive(Clone)]
struct HostAssociation {
    value: Arc<dyn std::any::Any + Send + Sync>,
    capture: bool,
}
#[derive(Clone)]
struct EntryContext {
    origin: CallOrigin,
    active: Arc<Active>,
    scope: Option<Arc<owned::Scope>>,
    cleanup: bool,
    release_budget: Option<Duration>,
    invocation: Option<HostAssociation>,
}
impl EntryContext {
    fn contains_engine(&self, engine: u64) -> bool {
        engine != 0
            && (self.origin.engine == engine
                || self.active.contains_engine(engine)
                || self
                    .scope
                    .as_ref()
                    .is_some_and(|scope| scope.contains_engine(engine)))
    }
}

// Reentry identity is immutable, bounded and independent of host authorization.
fn reentry_ancestry(origin: CallOrigin) -> Result<Arc<[u64]>> {
    reentry_ancestry_with(origin, None, None, &[])
}
fn reentry_ancestry_with(
    origin: CallOrigin,
    active: Option<&Active>,
    scope: Option<&owned::Scope>,
    retained: &[u64],
) -> Result<Arc<[u64]>> {
    let mut engines = Vec::new();
    let mut add = |engine| {
        if engine != 0 && !engines.contains(&engine) {
            if engines.len() == RECORDS {
                return Err(Error::ResourceLimit("engine callback ancestry"));
            }
            engines.push(engine);
        }
        Ok(())
    };
    for engine in retained.iter().copied() {
        add(engine)?;
    }
    if let Some(active) = active {
        active.visit_engines(&mut add)?;
    }
    if let Some(scope) = scope {
        scope.visit_engines(&mut add)?;
    }
    ENTRY.with(|entry| -> Result<()> {
        if let Some(context) = entry.borrow().as_ref() {
            context.active.visit_engines(&mut add)?;
            add(context.origin.engine)?;
            if let Some(scope) = &context.scope {
                scope.visit_engines(&mut add)?;
            }
        }
        Ok(())
    })?;
    ENGINE_CALLBACKS.with(|callbacks| -> Result<()> {
        for engine in callbacks.borrow().iter().copied() {
            add(engine)?;
        }
        Ok(())
    })?;
    add(origin.engine)?;
    Ok(engines.into())
}
thread_local! {
    static ENTRY: RefCell<Option<EntryContext>> = const { RefCell::new(None) };
    // Reentry identity only. This grants no execution/native authority and must
    // retain nested identities rather than allow an inner tag to hide its parent.
    static ENGINE_CALLBACKS: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

struct EngineCallback;
impl Drop for EngineCallback {
    fn drop(&mut self) {
        ENGINE_CALLBACKS.with(|engines| {
            engines.borrow_mut().pop().expect("engine callback scope");
        });
    }
}
struct Entry {
    previous: Option<EntryContext>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Entry {
    fn enter(origin: CallOrigin, active: Arc<Active>, cleanup: bool) -> Self {
        Self {
            previous: ENTRY.with(|slot| {
                slot.replace(Some(EntryContext {
                    origin,
                    active: active.clone(),
                    scope: None,
                    cleanup,
                    release_budget: None,
                    invocation: active.invocation.clone(),
                }))
            }),
            _thread: std::marker::PhantomData,
        }
    }
    fn scoped(context: EntryContext) -> Self {
        Self {
            previous: ENTRY.with(|slot| slot.replace(Some(context))),
            _thread: std::marker::PhantomData,
        }
    }
}
impl Drop for Entry {
    fn drop(&mut self) {
        ENTRY.with(|slot| slot.replace(self.previous.take()));
    }
}

struct Active {
    origin: CallOrigin,
    ancestry: Arc<[u64]>,
    scheduled_ancestry: std::sync::OnceLock<Result<Arc<[u64]>>>,
    generation: std::sync::atomic::AtomicU64,
    allow_cleanup: bool,
    _deadline: Option<Box<dyn ExecutionDeadline>>,
    until: std::sync::OnceLock<Instant>,
    budget: Duration,
    authority: Arc<dyn ExecutionAuthority>,
    _record: Option<OwnedSemaphorePermit>,
    _admission: Option<OwnedSemaphorePermit>,
    children: AtomicUsize,
    changed: Arc<Notify>,
    lane: Option<CallLane>,
    cleanup_owner: Option<Arc<AtomicUsize>>,
    scopes: Mutex<Vec<std::sync::Weak<owned::Scope>>>,
    cleanup_ready: Arc<Semaphore>,
    invocation: Option<HostAssociation>,
}
impl Drop for Active {
    fn drop(&mut self) {
        drop(self._deadline.take());
        drop(self._record.take());
        drop(self._admission.take());
        if let Some(cleanup) = self.cleanup_owner.take() {
            cleanup.fetch_sub(1, Ordering::AcqRel);
        }
        self.changed.notify_waiters();
    }
}

impl Active {
    fn contains_engine(&self, engine: u64) -> bool {
        self.ancestry.contains(&engine)
            || self.scheduled_ancestry.get().is_some_and(|ancestry| {
                ancestry
                    .as_ref()
                    .map_or(true, |engines| engines.contains(&engine))
            })
    }
    fn visit_engines(&self, add: &mut impl FnMut(u64) -> Result<()>) -> Result<()> {
        for engine in self.ancestry.iter().copied() {
            add(engine)?;
        }
        if let Some(ancestry) = self.scheduled_ancestry.get() {
            for engine in ancestry.as_ref().map_err(Clone::clone)?.iter().copied() {
                add(engine)?;
            }
        }
        Ok(())
    }
    fn until(&self) -> Instant {
        *self.until.get_or_init(|| {
            let until = Instant::now() + self.budget;
            self._deadline.as_ref().unwrap().arm_until(until);
            until
        })
    }
}
struct Tasks {
    name: &'static str,
    state: Mutex<TaskSet>,
}
#[derive(Default)]
struct TaskSet {
    runtime: Option<tokio::runtime::Runtime>,
    joins: JoinSet<()>,
    tickets: HashMap<Id, OwnedSemaphorePermit>,
    panicked: bool,
}
impl TaskSet {
    fn collect(
        &mut self,
        result: std::result::Result<(Id, ()), tokio::task::JoinError>,
        authority: &Arc<dyn ExecutionAuthority>,
    ) {
        let id = match result {
            Ok((id, ())) => id,
            Err(error) => {
                if !error.is_cancelled() {
                    authority.retire();
                    self.panicked = true;
                }
                error.id()
            }
        };
        self.tickets.remove(&id);
    }
    fn drain(&mut self, authority: &Arc<dyn ExecutionAuthority>) {
        while let Some(result) = self.joins.try_join_next_with_id() {
            self.collect(result, authority);
        }
    }
}
impl Tasks {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            state: Mutex::new(TaskSet::default()),
        }
    }
    fn spawn(
        &self,
        authority: &Arc<dyn ExecutionAuthority>,
        future: impl Future<Output = ()> + Send + 'static,
        ticket: Option<OwnedSemaphorePermit>,
    ) -> AbortHandle {
        let mut state = self.state.lock().unwrap();
        state.drain(authority);
        if state.runtime.is_none() {
            state.runtime = Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(WORKERS - 1)
                    .thread_name(self.name)
                    .build()
                    .expect("execution runtime creation failed"),
            );
        }
        let handle = state.runtime.as_ref().unwrap().handle().clone();
        let task = state.joins.spawn_on(future, &handle);
        if let Some(ticket) = ticket {
            state.tickets.insert(task.id(), ticket);
        }
        task
    }
    async fn finish(&self, authority: &Arc<dyn ExecutionAuthority>) -> Result<()> {
        std::future::poll_fn(|cx| {
            let mut state = self.state.lock().unwrap();
            loop {
                match state.joins.poll_join_next_with_id(cx) {
                    Poll::Ready(Some(result)) => state.collect(result, authority),
                    Poll::Ready(None) => {
                        return Poll::Ready(if state.panicked {
                            Err(Error::Protocol("execution task panicked"))
                        } else {
                            Ok(())
                        })
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
        .await
    }
}

type TerminalBody = Box<dyn FnOnce() + Send>;
type TerminalWorker = (
    std::sync::mpsc::SyncSender<TerminalBody>,
    Arc<dyn ExecutionThread>,
);
#[derive(Default)]
struct TerminalQueue(Mutex<Option<TerminalWorker>>);
impl TerminalQueue {
    fn enqueue(
        &self,
        authority: &Arc<dyn ExecutionAuthority>,
        body: impl FnOnce() + Send + 'static,
    ) {
        let mut worker = self.0.lock().unwrap();
        let (sender, _) = worker.get_or_insert_with(|| {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<TerminalBody>(32);
            let thread = std::thread::Builder::new()
                .name("fube-terminal-join".into())
                .spawn(move || {
                    while let Ok(body) = receiver.recv() {
                        body();
                    }
                })
                .expect("terminal worker creation failed");
            (sender, authority.own_thread(thread))
        });
        assert!(sender.try_send(Box::new(body)).is_ok());
    }
    fn stop(&self) -> Vec<Arc<dyn ExecutionThread>> {
        self.0
            .lock()
            .unwrap()
            .take()
            .map(|(_, thread)| vec![thread])
            .unwrap_or_default()
    }
}

struct Inner {
    admission: Mutex<()>,
    callbacks_running: AtomicUsize,
    native_host: bool,
    authority: Arc<dyn ExecutionAuthority>,
    application: tokio::runtime::Handle,
    native: Tasks,
    provider: Tasks,
    cleanup: Tasks,
    terminal: TerminalQueue,
    terminal_tickets: Arc<Semaphore>,
    factory_quota: ObjectQuota,
    service_quota: ObjectQuota,
    control_quota: ObjectQuota,
    native_records: Arc<Semaphore>,
    provider_records: Arc<Semaphore>,
    outstanding: Arc<Semaphore>,
    scheduled: Arc<Semaphore>,
    dormant: Arc<Semaphore>,
    cleanup_owners: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    stopping: AtomicBool,
    joins: tokio::sync::Mutex<ShutdownJoins>,
    #[cfg(test)]
    cleanup_pause: Mutex<Option<Arc<CleanupPause>>>,
}
enum ShutdownJoins {
    Open,
    Pools(DisposalReceipt),
    Terminal(Vec<Arc<dyn ExecutionThread>>),
    Complete,
}
#[cfg(test)]
struct CleanupPause {
    entered: Notify,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let runtimes: Vec<_> = [&self.native, &self.provider, &self.cleanup]
            .into_iter()
            .filter_map(|tasks| tasks.state.lock().unwrap().runtime.take())
            .collect();
        let threads = self.terminal.stop();
        if !runtimes.is_empty() || !threads.is_empty() {
            self.authority.retire();
            let retained = Arc::new(RetiredExecution {
                _authority: self.authority.clone(),
                _runtimes: runtimes,
                _threads: threads,
                keep: Mutex::new(None),
            });
            *retained.keep.lock().unwrap() = Some(retained.clone());
        }
    }
}
struct RetiredExecution {
    _authority: Arc<dyn ExecutionAuthority>,
    _runtimes: Vec<tokio::runtime::Runtime>,
    _threads: Vec<Arc<dyn ExecutionThread>>,
    keep: Mutex<Option<Arc<RetiredExecution>>>,
}

/// Execution profile for runtime-accepted roots, not an application executor.
/// The application supplies and owns the Tokio scheduling/driver context.
/// Implicit application child tasks are not adopted by this boundary.
/// Scheduling uses ordinary Tokio tasks/JoinSets with thin fube deadline,
/// context and disposal wrappers, not a custom task executor or ported SDK API.
/// A stalled supplied/captured application handle has no progress guarantee.
/// Native/provider/cleanup ownership is isolated so core IPC and lease work
/// need not wait for application synchronous polls/destructors.
#[derive(Clone)]
pub struct ExecutionRuntime {
    inner: Arc<Inner>,
}

impl ExecutionRuntime {
    /// Transport host-owned context without interpreting its value.
    pub fn current_host_context<T: Send + Sync + 'static>() -> Option<Arc<T>> {
        Self::current_association()?.value.downcast().ok()
    }
    fn current_association() -> Option<HostAssociation> {
        ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .and_then(|entry| entry.invocation.clone())
        })
    }
    /// Cleanup retains ancestry, but cannot supply context for fresh admission.
    pub fn capturable_host_context<T: Send + Sync + 'static>() -> Option<Arc<T>> {
        ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .filter(|entry| !entry.cleanup)
                .and_then(|entry| entry.invocation.as_ref())
                .filter(|association| association.capture)
                .and_then(|association| association.value.clone().downcast().ok())
        })
    }
    /// Only the host retaining this exact backend may install context. The host
    /// must keep its envelope free of application-owned destruction obligations.
    /// A borrowed execution handle cannot install context or reauthorize cleanup.
    pub fn host_context_scope<T: Send + Sync + 'static>(
        &self,
        authority: &Arc<dyn ExecutionAuthority>,
        value: Arc<T>,
    ) -> Result<impl Drop> {
        if !Arc::ptr_eq(authority, &self.inner.authority) {
            return Err(Error::InvalidInput("host context authority mismatch"));
        }
        if !self.is_current_entry() {
            return Err(Error::InvalidInput("host context execution owner mismatch"));
        }
        let mut context = ENTRY
            .with(|entry| entry.borrow().clone())
            .ok_or(Error::InvalidInput("host context requires owned execution"))?;
        if context.cleanup
            || context
                .invocation
                .as_ref()
                .is_some_and(|association| !association.capture)
        {
            return Err(Error::InvalidInput("cleanup cannot establish host context"));
        }
        context.invocation = Some(HostAssociation {
            value,
            capture: true,
        });
        Ok(Entry::scoped(context))
    }
    /// Standalone fixtures have no native process authority/watchdog. Their
    /// application remains responsible for a terminal blocking-code fence.
    pub fn standalone(application: tokio::runtime::Handle) -> Result<Self> {
        Ok(Self::with_mode(
            Arc::new(StandaloneAuthority::default()),
            application,
            false,
        ))
    }
    /// Bind finite execution to a host's real authority, deadline and thread
    /// accounting. The backend, not this scheduler, owns terminal fencing.
    pub fn with_authority(
        authority: Arc<dyn ExecutionAuthority>,
        application: tokio::runtime::Handle,
    ) -> Self {
        Self::with_mode(authority, application, true)
    }
    fn with_mode(
        authority: Arc<dyn ExecutionAuthority>,
        application: tokio::runtime::Handle,
        native_host: bool,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                admission: Mutex::new(()),
                callbacks_running: AtomicUsize::new(0),
                native_host,
                application,
                native: Tasks::new("fube-native-call"),
                provider: Tasks::new("fube-provider-call"),
                cleanup: Tasks::new("fube-owned-cleanup"),
                terminal: TerminalQueue::default(),
                terminal_tickets: Arc::new(Semaphore::new(32)),
                factory_quota: ObjectQuota::new(32).unwrap(),
                service_quota: ObjectQuota::new(32).unwrap(),
                control_quota: ObjectQuota::new(32).unwrap(),
                authority,
                native_records: Arc::new(Semaphore::new(32)),
                provider_records: Arc::new(Semaphore::new(64)),
                outstanding: Arc::new(Semaphore::new(RECORDS)),
                scheduled: Arc::new(Semaphore::new(RECORDS)),
                dormant: Arc::new(Semaphore::new(DORMANT_TICKETS)),
                cleanup_owners: Arc::new(AtomicUsize::new(0)),
                changed: Arc::new(Notify::new()),
                stopping: AtomicBool::new(false),
                joins: tokio::sync::Mutex::new(ShutdownJoins::Open),
                #[cfg(test)]
                cleanup_pause: Mutex::new(None),
            }),
        }
    }
    /// Reserve without accepting any application capture.
    pub fn try_reserve(&self) -> Result<OwnedCallPermit> {
        self.reserve(
            CallLane::Native,
            CallOrigin::default(),
            Duration::from_secs(30),
            false,
        )
    }
    pub fn reserve(
        &self,
        lane: CallLane,
        origin: CallOrigin,
        timeout: Duration,
        cleanup: bool,
    ) -> Result<OwnedCallPermit> {
        self.reserve_inner(lane, origin, timeout, cleanup, false)
    }
    /// Schedule a callback to a distinct engine on this same execution owner.
    /// Keep its explicit reentry identity, original deadline and host ancestry;
    /// this consumes the existing provider/shared budgets, not another lane.
    pub fn reserve_engine_callback(
        &self,
        origin: CallOrigin,
        deadline: Instant,
    ) -> Result<OwnedCallPermit> {
        if origin.engine == 0 || origin.generation == 0 {
            return Err(Error::InvalidInput("invalid callback engine identity"));
        }
        Self::reject_engine_reentry(origin.engine)?;
        if Self::in_owned_entry() && !self.is_current_entry() {
            return Err(Error::InvalidInput("callback execution owner mismatch"));
        }
        let deadline = if self.is_current_entry() {
            deadline.min(self.current_deadline()?)
        } else {
            deadline
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        self.reserve_inner(CallLane::Provider, origin, timeout, false, true)?
            .with_deadline(deadline)
    }
    fn reserve_inner(
        &self,
        lane: CallLane,
        mut origin: CallOrigin,
        timeout: Duration,
        cleanup: bool,
        engine_callback: bool,
    ) -> Result<OwnedCallPermit> {
        let _admission = self.inner.admission.lock().unwrap();
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        for tasks in [&self.inner.native, &self.inner.provider] {
            tasks.state.lock().unwrap().drain(&self.inner.authority);
        }
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(Error::InvalidInput(
                "execution deadline must be >0 and <=30s",
            ));
        }
        let recursive = ENTRY.with(|entry| {
            let entry = entry.borrow();
            if let Some(parent) = entry.as_ref() {
                if parent.origin.engine != 0 && !engine_callback {
                    origin = parent.origin;
                } else if origin.generation == 0 {
                    origin.generation = parent.origin.generation;
                }
                Arc::ptr_eq(&parent.active.authority, &self.inner.authority)
                    && parent.active.lane == Some(lane)
                    && !engine_callback
            } else {
                false
            }
        });
        if recursive {
            return Err(Error::InvalidInput("recursive execution reservation"));
        }
        let ancestry = reentry_ancestry(origin)?;
        let admission = match lane {
            CallLane::Native => &self.inner.native_records,
            CallLane::Provider => &self.inner.provider_records,
        }
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::ResourceLimit("execution lane records"))?;
        let record = self
            .inner
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("execution records"))?;
        let deadline = self
            .inner
            .authority
            .clone()
            .reserve_callback_deadline(cleanup)?;
        let scheduled = self
            .inner
            .scheduled
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("execution scheduled tasks"))?;
        Ok(OwnedCallPermit {
            runtime: self.clone(),
            lane,
            origin,
            scheduled,
            active: Arc::new(Active {
                origin,
                ancestry,
                scheduled_ancestry: std::sync::OnceLock::new(),
                generation: std::sync::atomic::AtomicU64::new(origin.generation),
                allow_cleanup: cleanup,
                _deadline: Some(deadline),
                until: std::sync::OnceLock::new(),
                budget: timeout,
                authority: self.inner.authority.clone(),
                _record: Some(record),
                _admission: Some(admission),
                children: AtomicUsize::new(0),
                changed: self.inner.changed.clone(),
                lane: Some(lane),
                cleanup_owner: None,
                scopes: Mutex::new(Vec::new()),
                cleanup_ready: Arc::new(Semaphore::new(1)),
                invocation: if self.is_current_entry() {
                    Self::current_association().map(|mut association| {
                        association.capture &= !Self::is_cleanup_entry();
                        association
                    })
                } else {
                    None
                },
            }),
        })
    }
    /// Waiting APIs call this before any mutation or command submission.
    pub fn check_entry(engine: Option<u64>) -> Result<()> {
        ENTRY.with(|entry| {
            let entry = entry.borrow();
            match entry.as_ref() {
                None => Err(Error::InvalidInput("invalid execution context")),
                Some(context)
                    if engine.is_some_and(|id| {
                        context.contains_engine(id)
                            || ENGINE_CALLBACKS.with(|engines| engines.borrow().contains(&id))
                    }) =>
                {
                    Err(Error::InvalidInput("same-engine execution reentry"))
                }
                Some(_) => Ok(()),
            }
        })
    }
    pub fn reject_engine_reentry(engine: u64) -> Result<()> {
        if ENGINE_CALLBACKS.with(|engines| engines.borrow().contains(&engine)) {
            return Err(Error::InvalidInput("same-engine publication reentry"));
        }
        ENTRY.with(|entry| {
            if entry
                .borrow()
                .as_ref()
                .is_some_and(|context| context.contains_engine(engine))
            {
                Err(Error::InvalidInput("same-engine execution reentry"))
            } else {
                Ok(())
            }
        })
    }
    /// Tag a synchronous trusted log-publication prefix for reentry rejection.
    /// No provider invocation or awaiting is permitted in this prefix. The tag is
    /// not native control authority and does not create an execution entry.
    pub fn engine_publication<T>(engine: u64, publish: impl FnOnce() -> Result<T>) -> Result<T> {
        if engine == 0 {
            return Err(Error::InvalidInput("invalid publication engine identity"));
        }
        Self::reject_engine_reentry(engine)?;
        ENGINE_CALLBACKS.with(|engines| {
            if engines.borrow().len() >= 32 {
                return Err(Error::ResourceLimit("engine publication nesting"));
            }
            engines.borrow_mut().push(engine);
            Ok(())
        })?;
        let _scope = EngineCallback;
        publish()
    }
    pub fn cleanup_quiescent(&self) -> bool {
        self.inner.cleanup_owners.load(Ordering::Acquire) == 0
    }
    pub async fn ownership_changed(&self) {
        self.inner.changed.notified().await;
    }
    /// Finish an application-owned standalone owner, never a borrowed native host.
    pub async fn finish_standalone(&self) -> Result<()> {
        if self.inner.native_host || self.is_current_entry() {
            return Err(Error::InvalidInput(
                "execution owner or calling context mismatch",
            ));
        }
        self.shutdown().await
    }
    /// Only the host retaining this exact backend may shut down borrowed
    /// execution. Ordinary consumers use `finish_standalone` for their own
    /// standalone owner; a runtime-owned handle alone is not shutdown authority.
    pub async fn shutdown_with_authority(
        &self,
        authority: &Arc<dyn ExecutionAuthority>,
    ) -> Result<()> {
        if !Arc::ptr_eq(authority, &self.inner.authority) {
            return Err(Error::InvalidInput("execution shutdown authority mismatch"));
        }
        self.shutdown().await
    }
    async fn shutdown(&self) -> Result<()> {
        {
            let _admission = self.inner.admission.lock().unwrap();
            self.inner.stopping.store(true, Ordering::Release);
        }
        loop {
            let changed = self.inner.changed.notified();
            if self.inner.outstanding.available_permits() == RECORDS
                && self.inner.dormant.available_permits() == DORMANT_TICKETS
                && self.inner.terminal_tickets.available_permits() == 32
            {
                break;
            }
            changed.await;
        }
        let mut joins = self.inner.joins.lock().await;
        if matches!(*joins, ShutdownJoins::Open) {
            for tasks in [
                &self.inner.native,
                &self.inner.provider,
                &self.inner.cleanup,
            ] {
                tasks.finish(&self.inner.authority).await?;
            }
            let terminal = self.reserve_terminal_for_shutdown(CallOrigin::default())?;
            let runtimes: Vec<_> = [
                &self.inner.native,
                &self.inner.provider,
                &self.inner.cleanup,
            ]
            .into_iter()
            .filter_map(|tasks| tasks.state.lock().unwrap().runtime.take())
            .collect();
            let receipt = terminal.run(move || {
                drop(runtimes);
                Ok(())
            })?;
            *joins = ShutdownJoins::Pools(receipt);
        }
        if let ShutdownJoins::Pools(receipt) = &*joins {
            receipt.wait().await?;
            *joins = ShutdownJoins::Terminal(self.inner.terminal.stop());
        }
        if let ShutdownJoins::Terminal(handles) = &*joins {
            for handle in handles {
                handle.join_finished().await?;
            }
            *joins = ShutdownJoins::Complete;
        }
        Ok(())
    }
}

/// Contains only internal reservations until eager `run_owned`/`run_future`.
pub struct OwnedCallPermit {
    runtime: ExecutionRuntime,
    lane: CallLane,
    origin: CallOrigin,
    active: Arc<Active>,
    scheduled: OwnedSemaphorePermit,
}
impl OwnedCallPermit {
    /// Clamp this reservation to an original absolute deadline before transferring
    /// captures. Queueing or a second callback must not restart its timeout.
    pub fn with_deadline(self, deadline: Instant) -> Result<Self> {
        let until = deadline.min(Instant::now() + self.active.budget);
        if Instant::now() >= until {
            return Err(Error::Timeout);
        }
        self.active
            .until
            .set(until)
            .map_err(|_| Error::InvalidInput("execution reservation deadline already frozen"))?;
        self.active._deadline.as_ref().unwrap().arm_until(until);
        Ok(self)
    }

    /// Eagerly transfer a synchronous call and its captures to owned execution.
    /// Await the returned observation, not the SDK method's synchronous Result.
    /// Reserve before captures; dropping the observer cancels, not rollback or
    /// proof that physical disposal has finished.
    pub fn run_owned<T: Send + 'static>(
        self,
        call: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> OwnedCall<T> {
        self.run_future(async move { call() })
    }
    pub fn run_future<T: Send + 'static>(
        self,
        future: impl Future<Output = Result<T>> + Send + 'static,
    ) -> OwnedCall<T> {
        // A permit can be reserved before entering a publication/callback.
        // Freeze that live scheduling ancestry too, before accepting captures.
        let ancestry = reentry_ancestry(self.origin).and_then(|current| {
            let mut engines = current.to_vec();
            self.active.visit_engines(&mut |engine| {
                if !engines.contains(&engine) {
                    if engines.len() == RECORDS {
                        return Err(Error::ResourceLimit("engine callback ancestry"));
                    }
                    engines.push(engine);
                }
                Ok(())
            })?;
            Ok(engines.into())
        });
        self.active.scheduled_ancestry.set(ancestry).unwrap();
        let until = self.active.until();
        let (cleanup, cleanup_rx) = oneshot::channel();
        let (decision, decision_rx) = oneshot::channel();
        let (ready, ready_rx) = oneshot::channel();
        let (blocking, blocking_rx) = std::sync::mpsc::sync_channel(1);
        let runtime = self.runtime.clone();
        let lane = self.lane;
        let task = Arc::new(Task {
            activity: Arc::downgrade(&self.active),
            runtime: runtime.clone(),
            abort: std::sync::OnceLock::new(),
            state: Mutex::new(TaskState {
                future: Some(Box::pin(future)),
                result: None,
                active: Some(self.active),
                cleanup: Some(cleanup),
                decision: Some(decision),
                canceled: false,
                finished: false,
            }),
        });
        let cleanup_task = task.clone();
        runtime.inner.cleanup.spawn(
            &runtime.inner.authority,
            async move {
                cleanup_task
                    .cleanup(cleanup_rx, decision_rx, ready, blocking)
                    .await;
            },
            None,
        );
        let driver = Root(task.clone());
        let tasks = match lane {
            CallLane::Native => &runtime.inner.native,
            CallLane::Provider => &runtime.inner.provider,
        };
        let abort = tasks.spawn(&runtime.inner.authority, driver, Some(self.scheduled));
        task.abort.set(abort).unwrap();
        OwnedCall {
            task,
            ready: ready_rx,
            blocking: blocking_rx,
            until,
            consumed: false,
        }
    }
}

type OwnedFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
struct TaskState<T> {
    future: Option<OwnedFuture<T>>,
    result: Option<Result<T>>,
    active: Option<Arc<Active>>,
    cleanup: Option<oneshot::Sender<OwnedFuture<T>>>,
    decision: Option<oneshot::Sender<()>>,
    canceled: bool,
    finished: bool,
}
struct Task<T> {
    activity: std::sync::Weak<Active>,
    runtime: ExecutionRuntime,
    abort: std::sync::OnceLock<AbortHandle>,
    state: Mutex<TaskState<T>>,
}
impl<T: Send + 'static> Task<T> {
    fn release_future(&self) {
        let transfer = {
            let mut state = self.state.lock().unwrap();
            state
                .future
                .take()
                .map(|future| (state.cleanup.take().unwrap(), future))
        };
        if let Some((sender, future)) = transfer {
            // The reserved cleanup task lives until this transfer and is never
            // aborted. Failure would otherwise drop arbitrary captures here.
            if let Err(future) = sender.send(future) {
                std::mem::forget(future);
                self.runtime.retire();
            }
        }
    }
    fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        if state.finished {
            return;
        }
        state.canceled = true;
        if let Some(decision) = state.decision.take() {
            let _ = decision.send(());
        }
        drop(state);
        self.abort.get().unwrap().abort();
        self.release_future();
    }
    fn entered<R>(&self, active: &Arc<Active>, cleanup: bool, body: impl FnOnce() -> R) -> R {
        let _entered = self.runtime.inner.application.enter();
        let mut origin = active.origin;
        origin.generation = active.generation.load(Ordering::Acquire);
        let _entry = Entry::enter(origin, active.clone(), cleanup);
        body()
    }
    async fn children(&self, active: &Active) {
        loop {
            let changed = active.changed.notified();
            if active.children.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
    async fn cleanup(
        &self,
        future: oneshot::Receiver<OwnedFuture<T>>,
        mut decision: oneshot::Receiver<()>,
        ready: oneshot::Sender<()>,
        blocking: std::sync::mpsc::SyncSender<()>,
    ) {
        let future = future.await.expect("root cleanup transfer lost");
        let active = self.state.lock().unwrap().active.as_ref().unwrap().clone();
        tokio::task::block_in_place(|| {
            self.entered(&active, true, || {
                if catch_unwind(AssertUnwindSafe(|| drop(future))).is_err() {
                    self.runtime.retire();
                    let old = self
                        .state
                        .lock()
                        .unwrap()
                        .result
                        .replace(Err(Error::Protocol("owned callback destruction panicked")));
                    if catch_unwind(AssertUnwindSafe(|| drop(old))).is_err() {
                        self.runtime.retire();
                    }
                }
                #[cfg(test)]
                if let Some(pause) = self.runtime.inner.cleanup_pause.lock().unwrap().take() {
                    pause.entered.notify_one();
                    pause
                        .resume
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap();
                }
            })
        });
        let children_done = tokio::select! {
            _ = self.children(&active) => true,
            _ = &mut decision => false,
        };
        drop(active);
        if children_done {
            let _ = ready.send(());
            let _ = blocking.try_send(());
            let _ = decision.await;
        }
        let (result, active) = {
            let mut state = self.state.lock().unwrap();
            if !state.canceled {
                return;
            }
            (state.result.take(), state.active.as_ref().unwrap().clone())
        };
        tokio::task::block_in_place(|| {
            self.entered(&active, true, || {
                if catch_unwind(AssertUnwindSafe(|| drop(result))).is_err() {
                    self.runtime.retire();
                }
            })
        });
        self.children(&active).await;
        drop(active);
        let mut state = self.state.lock().unwrap();
        drop(state.active.take());
        state.finished = true;
        drop(state);
        self.runtime.inner.changed.notify_waiters();
    }
}
struct Root<T: Send + 'static>(Arc<Task<T>>);
impl<T: Send + 'static> Future for Root<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        tokio::task::block_in_place(|| {
            let (mut future, active) = {
                let mut state = self.0.state.lock().unwrap();
                let Some(future) = state.future.take() else {
                    return Poll::Ready(());
                };
                (future, state.active.as_ref().unwrap().clone())
            };
            let polled = self.0.entered(&active, false, || {
                if let Some(Err(error)) = active.scheduled_ancestry.get() {
                    return Poll::Ready(Err(error.clone()));
                }
                if !active.allow_cleanup && active.authority.check().is_err() {
                    return Poll::Ready(Err(Error::Closed));
                }
                match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                    Ok(polled) => polled
                        .map(|result| result.map_err(|error| error.supervised(&self.0.runtime))),
                    Err(_) => {
                        self.0.runtime.retire();
                        Poll::Ready(Err(Error::Protocol("owned callback panicked")))
                    }
                }
            });
            let mut state = self.0.state.lock().unwrap();
            state.future = Some(future);
            match polled {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    state.result = Some(result);
                    Poll::Ready(())
                }
            }
        })
    }
}
impl<T: Send + 'static> Drop for Root<T> {
    fn drop(&mut self) {
        self.0.release_future();
    }
}

/// Dropping an observation requests cleanup, never drops accepted captures on
/// the observer. Blocking timeout is uncertain and leaves the task accounted.
pub struct OwnedCall<T: Send + 'static> {
    task: Arc<Task<T>>,
    ready: oneshot::Receiver<()>,
    blocking: std::sync::mpsc::Receiver<()>,
    until: Instant,
    consumed: bool,
}
trait Disposition: Send + Sync {
    fn finished(&self) -> bool;
    fn cancel(self: Arc<Self>);
    fn runtime(&self) -> ExecutionRuntime;
}
impl<T: Send + 'static> Disposition for Task<T> {
    fn finished(&self) -> bool {
        self.state.lock().unwrap().finished && self.activity.upgrade().is_none()
    }
    fn cancel(self: Arc<Self>) {
        Task::cancel(&self);
    }
    fn runtime(&self) -> ExecutionRuntime {
        self.runtime.clone()
    }
}
#[derive(Clone)]
pub struct OwnedDisposition(Arc<dyn Disposition>);
impl OwnedDisposition {
    pub fn is_finished(&self) -> bool {
        self.0.finished()
    }
    pub fn cancel(&self) {
        self.0.clone().cancel();
    }
    pub async fn wait(&self) {
        let runtime = self.0.runtime();
        loop {
            let changed = runtime.inner.changed.notified();
            if self.is_finished() {
                return;
            }
            changed.await;
        }
    }
}
impl<T: Send + 'static> OwnedCall<T> {
    pub fn disposition(&self) -> OwnedDisposition {
        OwnedDisposition(self.task.clone())
    }
    pub async fn cancel_and_wait(mut self) {
        Task::cancel(&self.task);
        loop {
            let changed = self.task.runtime.inner.changed.notified();
            if self.task.state.lock().unwrap().finished {
                self.consumed = true;
                return;
            }
            changed.await;
        }
    }
    fn take_result(&mut self) -> Result<T> {
        if Instant::now() >= self.until {
            Task::cancel(&self.task);
            return Err(Error::Timeout);
        }
        let (result, active) = {
            let mut state = self.task.state.lock().unwrap();
            if state.canceled {
                return Err(Error::Closed);
            }
            assert!(!state.finished);
            state.finished = true;
            if let Some(decision) = state.decision.take() {
                let _ = decision.send(());
            }
            (state.result.take().unwrap(), state.active.take())
        };
        self.consumed = true;
        drop(active);
        self.task.runtime.inner.changed.notify_waiters();
        result
    }
    pub fn wait(mut self) -> Result<T> {
        ExecutionRuntime::check_entry(None)?;
        self.blocking
            .recv_timeout(self.until.saturating_duration_since(Instant::now()))
            .map_err(|error| match error {
                std::sync::mpsc::RecvTimeoutError::Timeout => Error::Timeout,
                std::sync::mpsc::RecvTimeoutError::Disconnected => Error::Closed,
            })?;
        self.take_result()
    }
}
impl<T: Send + 'static> Future for OwnedCall<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.ready).poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(this.take_result()),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Closed)),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<T: Send + 'static> Drop for OwnedCall<T> {
    fn drop(&mut self) {
        if !self.consumed {
            Task::cancel(&self.task);
        }
    }
}

#[cfg(test)]
#[path = "execution_ownership_tests.rs"]
mod ownership_tests;
#[cfg(test)]
#[path = "execution_tests.rs"]
mod tests;
