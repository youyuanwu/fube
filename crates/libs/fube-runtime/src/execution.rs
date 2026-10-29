use crate::lease::{Authority, CallbackDeadline, OwnedThread};
use fube_transport::{Error, Result};
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const WORKERS: usize = 32;
const RECORDS: usize = 64;
// 32 admitted units * (64 retained copy streams + service/control/provider)
// plus the existing 32 retained factories. Per-object quotas remain required.
const DORMANT_TICKETS: usize = 32 * (64 + 3) + 32 + crate::MAX_PACKAGE_SUBSCRIPTIONS;
#[path = "execution_owned.rs"]
mod owned;
pub(crate) use owned::PublicationBatch;
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
struct EntryContext {
    origin: CallOrigin,
    active: Arc<Active>,
    scope: Option<Arc<owned::Scope>>,
    cleanup: bool,
    release_budget: Option<Duration>,
}
thread_local! {
    static ENTRY: RefCell<Option<EntryContext>> = const { RefCell::new(None) };
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
                    active,
                    scope: None,
                    cleanup,
                    release_budget: None,
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
    generation: std::sync::atomic::AtomicU64,
    allow_cleanup: bool,
    _deadline: Option<CallbackDeadline>,
    until: std::sync::OnceLock<Instant>,
    budget: Duration,
    authority: Arc<Authority>,
    _record: Option<OwnedSemaphorePermit>,
    _admission: Option<OwnedSemaphorePermit>,
    children: AtomicUsize,
    waiter: Mutex<Option<Waker>>,
    changed: Arc<Notify>,
    lane: Option<CallLane>,
    cleanup_owner: Option<Arc<AtomicUsize>>,
    scopes: Mutex<Vec<std::sync::Weak<owned::Scope>>>,
    cleanup_ready: Arc<Semaphore>,
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
    fn until(&self) -> Instant {
        *self.until.get_or_init(|| {
            let until = Instant::now() + self.budget;
            self._deadline.as_ref().unwrap().arm_until(until);
            until
        })
    }
}
trait Work: Send + Sync {
    fn execute(self: Arc<Self>, cleanup_lane: bool);
}

struct PoolState {
    ready: VecDeque<Arc<dyn Work>>,
    idle: usize,
    stop: bool,
    threads: Vec<Arc<OwnedThread>>,
}
struct Pool {
    name: &'static str,
    state: Mutex<PoolState>,
    changed: Condvar,
    authority: std::sync::Weak<Authority>,
    maximum: usize,
}
impl Pool {
    fn new(name: &'static str, authority: &Arc<Authority>) -> Arc<Self> {
        Self::with_maximum(name, authority, WORKERS)
    }
    fn with_maximum(name: &'static str, authority: &Arc<Authority>, maximum: usize) -> Arc<Self> {
        Arc::new(Self {
            name,
            state: Mutex::new(PoolState {
                ready: VecDeque::new(),
                idle: 0,
                stop: false,
                threads: Vec::new(),
            }),
            changed: Condvar::new(),
            authority: Arc::downgrade(authority),
            maximum,
        })
    }
    fn enqueue(self: &Arc<Self>, work: Arc<dyn Work>) {
        let mut state = self.state.lock().unwrap();
        // Every entry is backed by an admitted record or dormant release ticket.
        // Queue storage is not a second source of admission.
        assert!(!state.stop);
        state.ready.push_back(work);
        if state.idle == 0 && state.threads.len() < self.maximum {
            let pool = self.clone();
            let thread = std::thread::Builder::new()
                .name(self.name.into())
                .spawn(move || loop {
                    let work = {
                        let mut state = pool.state.lock().unwrap();
                        loop {
                            if let Some(work) = state.ready.pop_front() {
                                break Some(work);
                            }
                            if state.stop {
                                break None;
                            }
                            state.idle += 1;
                            state = pool.changed.wait(state).unwrap();
                            state.idle -= 1;
                        }
                    };
                    let Some(work) = work else { return };
                    work.execute(pool.name == "fube-owned-cleanup");
                })
                .expect("execution worker creation failed");
            state
                .threads
                .push(self.authority.upgrade().unwrap().own_thread(thread));
        }
        self.changed.notify_one();
    }
    fn stop(&self) -> Vec<Arc<OwnedThread>> {
        let mut state = self.state.lock().unwrap();
        assert!(state.ready.is_empty());
        state.stop = true;
        self.changed.notify_all();
        std::mem::take(&mut state.threads)
    }
    fn remove(&self, work: &Arc<dyn Work>) {
        self.state
            .lock()
            .unwrap()
            .ready
            .retain(|queued| !Arc::ptr_eq(queued, work));
    }
}

struct Inner {
    admission: Mutex<()>,
    callbacks_running: AtomicUsize,
    native_host: bool,
    authority: Arc<Authority>,
    application: tokio::runtime::Handle,
    native: Arc<Pool>,
    provider: Arc<Pool>,
    cleanup: Arc<Pool>,
    terminal: Arc<Pool>,
    terminal_tickets: Arc<Semaphore>,
    factory_quota: ObjectQuota,
    service_quota: ObjectQuota,
    control_quota: ObjectQuota,
    native_records: Arc<Semaphore>,
    provider_records: Arc<Semaphore>,
    outstanding: Arc<Semaphore>,
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
    Terminal(Vec<Arc<OwnedThread>>),
    Complete,
}
#[cfg(test)]
struct CleanupPause {
    entered: Notify,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let mut retain = false;
        for pool in [&self.native, &self.provider, &self.cleanup, &self.terminal] {
            let mut state = pool.state.lock().unwrap();
            if !state.threads.is_empty() {
                // Failed/abandoned lifecycle is terminal, never joined cleanup.
                // Native Authority retains the OS handles until its process
                // fence; stopping idle workers cannot release user captures.
                self.authority.retire();
                retain = true;
                state.stop = true;
                pool.changed.notify_all();
            }
        }
        if retain {
            // Abandoned owners are not successful shutdown. Native fencing
            // retains every OS handle; standalone callers supply that fence.
            let retained = Arc::new(RetiredExecution {
                _authority: self.authority.clone(),
                _pools: vec![
                    self.native.clone(),
                    self.provider.clone(),
                    self.cleanup.clone(),
                    self.terminal.clone(),
                ],
                keep: Mutex::new(None),
            });
            *retained.keep.lock().unwrap() = Some(retained.clone());
        }
    }
}
struct RetiredExecution {
    _authority: Arc<Authority>,
    _pools: Vec<Arc<Pool>>,
    keep: Mutex<Option<Arc<RetiredExecution>>>,
}

/// Execution profile for runtime-accepted roots, not an application executor.
/// The application supplies and owns the Tokio scheduling/driver context.
/// Implicit application child tasks are not adopted by this boundary.
#[derive(Clone)]
pub struct ExecutionRuntime {
    inner: Arc<Inner>,
}

impl ExecutionRuntime {
    /// Standalone fixtures have no native process authority/watchdog. Their
    /// application remains responsible for a terminal blocking-code fence.
    pub fn standalone(application: tokio::runtime::Handle) -> Result<Self> {
        Ok(Self::with_mode(
            Authority::new(Instant::now(), i64::MAX / 100)?,
            application,
            false,
        ))
    }
    pub fn from_partition(partition: &crate::StatefulPartition) -> Result<Self> {
        partition.ensure_valid()?;
        partition.execution.clone().ok_or(Error::InvalidInput(
            "partition has no execution integration",
        ))
    }
    pub(crate) fn new(authority: Arc<Authority>, application: tokio::runtime::Handle) -> Self {
        Self::with_mode(authority, application, true)
    }
    fn with_mode(
        authority: Arc<Authority>,
        application: tokio::runtime::Handle,
        native_host: bool,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                admission: Mutex::new(()),
                callbacks_running: AtomicUsize::new(0),
                native_host,
                application,
                native: Pool::new("fube-native-call", &authority),
                provider: Pool::new("fube-provider-call", &authority),
                cleanup: Pool::new("fube-owned-cleanup", &authority),
                terminal: Pool::with_maximum("fube-terminal-join", &authority, 1),
                terminal_tickets: Arc::new(Semaphore::new(32)),
                factory_quota: ObjectQuota::new(32).unwrap(),
                service_quota: ObjectQuota::new(32).unwrap(),
                control_quota: ObjectQuota::new(32).unwrap(),
                authority,
                native_records: Arc::new(Semaphore::new(32)),
                provider_records: Arc::new(Semaphore::new(64)),
                outstanding: Arc::new(Semaphore::new(RECORDS)),
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
        mut origin: CallOrigin,
        timeout: Duration,
        cleanup: bool,
    ) -> Result<OwnedCallPermit> {
        let _admission = self.inner.admission.lock().unwrap();
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(Error::InvalidInput(
                "execution deadline must be >0 and <=30s",
            ));
        }
        let recursive = ENTRY.with(|entry| {
            let entry = entry.borrow();
            if let Some(parent) = entry.as_ref() {
                if parent.origin.engine != 0 {
                    origin = parent.origin;
                } else if origin.generation == 0 {
                    origin.generation = parent.origin.generation;
                }
                Arc::ptr_eq(&parent.active.authority, &self.inner.authority)
                    && parent.active.lane == Some(lane)
            } else {
                false
            }
        });
        if recursive {
            return Err(Error::InvalidInput("recursive execution reservation"));
        }
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
        let deadline = self.inner.authority.reserve_callback_deadline(cleanup)?;
        Ok(OwnedCallPermit {
            runtime: self.clone(),
            lane,
            origin,
            active: Arc::new(Active {
                origin,
                generation: std::sync::atomic::AtomicU64::new(origin.generation),
                allow_cleanup: cleanup,
                _deadline: Some(deadline),
                until: std::sync::OnceLock::new(),
                budget: timeout,
                authority: self.inner.authority.clone(),
                _record: Some(record),
                _admission: Some(admission),
                children: AtomicUsize::new(0),
                waiter: Mutex::new(None),
                changed: self.inner.changed.clone(),
                lane: Some(lane),
                cleanup_owner: None,
                scopes: Mutex::new(Vec::new()),
                cleanup_ready: Arc::new(Semaphore::new(1)),
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
                        id != 0
                            && (id == context.origin.engine
                                || context
                                    .scope
                                    .as_ref()
                                    .is_some_and(|scope| scope.contains_engine(id)))
                    }) =>
                {
                    Err(Error::InvalidInput("same-engine execution reentry"))
                }
                Some(_) => Ok(()),
            }
        })
    }
    pub fn reject_engine_reentry(engine: u64) -> Result<()> {
        ENTRY.with(|entry| {
            if entry.borrow().as_ref().is_some_and(|context| {
                context.origin.engine == engine
                    || context
                        .scope
                        .as_ref()
                        .is_some_and(|scope| scope.contains_engine(engine))
            }) {
                Err(Error::InvalidInput("same-engine execution reentry"))
            } else {
                Ok(())
            }
        })
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
    pub(crate) async fn shutdown(&self) -> Result<()> {
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
            let handles = self
                .inner
                .native
                .stop()
                .into_iter()
                .chain(self.inner.provider.stop())
                .chain(self.inner.cleanup.stop())
                .collect::<Vec<_>>();
            let terminal = self.reserve_terminal_for_shutdown(CallOrigin::default())?;
            let receipt = terminal.run(move || {
                for handle in handles {
                    handle.join()?;
                }
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
}
impl OwnedCallPermit {
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
        let cell = Arc::new(Cell {
            value: Mutex::new(CellState {
                result: None,
                waker: None,
            }),
            ready: Condvar::new(),
        });
        let until = self.active.until();
        let activity = Arc::downgrade(&self.active);
        let task = Arc::new(Task {
            activity,
            runtime: self.runtime.clone(),
            state: Mutex::new(TaskState {
                future: Some(Box::pin(future)),
                result: None,
                permit: Some(self),
                running: false,
                queued: true,
                wake: false,
                canceled: false,
                cleanup: false,
                completed: false,
                finished: false,
                cell: Some(cell.clone()),
            }),
        });
        let pool = task.pool();
        pool.enqueue(task.clone());
        OwnedCall {
            task,
            cell,
            until,
            consumed: false,
        }
    }
}

struct CellState<T> {
    result: Option<Result<T>>,
    waker: Option<Waker>,
}
struct Cell<T> {
    value: Mutex<CellState<T>>,
    ready: Condvar,
}
impl<T> Cell<T> {
    fn complete(&self, value: Result<T>) {
        let waker = {
            let mut state = self.value.lock().unwrap();
            assert!(state.result.is_none());
            state.result = Some(value);
            state.waker.take()
        };
        self.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

type OwnedFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
struct TaskState<T> {
    future: Option<OwnedFuture<T>>,
    result: Option<Result<T>>,
    permit: Option<OwnedCallPermit>,
    running: bool,
    queued: bool,
    wake: bool,
    canceled: bool,
    cleanup: bool,
    completed: bool,
    finished: bool,
    cell: Option<Arc<Cell<()>>>,
}
struct Task<T> {
    activity: std::sync::Weak<Active>,
    runtime: ExecutionRuntime,
    state: Mutex<TaskState<T>>,
}
impl<T: Send + 'static> Task<T> {
    fn pool(&self) -> Arc<Pool> {
        let state = self.state.lock().unwrap();
        if state.cleanup {
            return self.runtime.inner.cleanup.clone();
        }
        match state.permit.as_ref().unwrap().lane {
            CallLane::Native => self.runtime.inner.native.clone(),
            CallLane::Provider => self.runtime.inner.provider.clone(),
        }
    }
    fn schedule(self: &Arc<Self>, cancel: bool) {
        let (pool, previous) = {
            let mut state = self.state.lock().unwrap();
            if state.finished {
                return;
            }
            state.canceled |= cancel;
            if state.running {
                state.wake = true;
                return;
            }
            if state.queued {
                if !state.canceled || state.cleanup {
                    return;
                }
                let previous = match state.permit.as_ref().unwrap().lane {
                    CallLane::Native => self.runtime.inner.native.clone(),
                    CallLane::Provider => self.runtime.inner.provider.clone(),
                };
                state.cleanup = true;
                (self.runtime.inner.cleanup.clone(), Some(previous))
            } else {
                state.queued = true;
                if state.cleanup || state.canceled {
                    state.cleanup = true;
                    (self.runtime.inner.cleanup.clone(), None)
                } else {
                    (
                        match state.permit.as_ref().unwrap().lane {
                            CallLane::Native => self.runtime.inner.native.clone(),
                            CallLane::Provider => self.runtime.inner.provider.clone(),
                        },
                        None,
                    )
                }
            }
        };
        if let Some(previous) = previous {
            let erased: Arc<dyn Work> = self.clone();
            previous.remove(&erased);
        }
        pool.enqueue(self.clone());
    }
}
impl<T: Send + 'static> Wake for Task<T> {
    fn wake(self: Arc<Self>) {
        self.schedule(false);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule(false);
    }
}
impl<T: Send + 'static> Work for Task<T> {
    fn execute(self: Arc<Self>, cleanup_lane: bool) {
        let (mut future, cleanup, canceled, origin, active) = {
            let mut state = self.state.lock().unwrap();
            if state.finished || state.running || !state.queued || (state.cleanup && !cleanup_lane)
            {
                return;
            }
            if !state.cleanup
                && !state.permit.as_ref().unwrap().active.allow_cleanup
                && self.runtime.inner.authority.check().is_err()
            {
                state.canceled = true;
                state.cleanup = true;
                state.queued = false;
                drop(state);
                self.schedule(true);
                return;
            }
            state.running = true;
            state.queued = false;
            let permit = state.permit.as_ref().unwrap();
            let mut origin = permit.origin;
            origin.generation = permit.active.generation.load(Ordering::Acquire);
            let active = permit.active.clone();
            let cleanup = state.cleanup || state.canceled;
            let canceled = state.canceled;
            (state.future.take(), cleanup, canceled, origin, active)
        };
        let _entered = self.runtime.inner.application.enter();
        let _entry = Entry::enter(origin, active.clone(), cleanup);
        if cleanup {
            let disposed = catch_unwind(AssertUnwindSafe(|| {
                drop(future.take());
            }));
            if disposed.is_err() {
                self.runtime.inner.authority.retire();
            }
            if disposed.is_err() {
                let old = {
                    let mut state = self.state.lock().unwrap();
                    state
                        .result
                        .replace(Err(Error::Protocol("owned callback destruction panicked")))
                };
                if catch_unwind(AssertUnwindSafe(|| drop(old))).is_err() {
                    self.runtime.inner.authority.retire();
                }
            }
            // Cancellation is monotonic. At most one extra pass is needed
            // when it races the result transfer; this never polls a future.
            loop {
                let abandoned = {
                    let mut state = self.state.lock().unwrap();
                    if state.canceled || canceled {
                        state.result.take()
                    } else {
                        None
                    }
                };
                if catch_unwind(AssertUnwindSafe(|| drop(abandoned))).is_err() {
                    self.runtime.inner.authority.retire();
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
                let mut waiter = active.waiter.lock().unwrap();
                *waiter = Some(Waker::from(self.clone()));
                let mut state = self.state.lock().unwrap();
                if state.canceled && state.result.is_some() {
                    // Cancellation won after the first extraction. Keep the
                    // record running/guarded, transfer the value next pass.
                    drop(state);
                    waiter.take();
                    drop(waiter);
                    continue;
                }
                state.running = false;
                if active.children.load(Ordering::Acquire) != 0 {
                    return;
                }
                let canceled = state.canceled || canceled;
                let notify = (!state.completed).then(|| state.cell.as_ref().unwrap().clone());
                state.completed = true;
                let permit = if canceled {
                    state.finished = true;
                    state.permit.take()
                } else {
                    None
                };
                drop(state);
                waiter.take();
                drop(waiter);
                drop(permit);
                drop(_entry);
                drop(active);
                if let Some(cell) = notify {
                    cell.complete(if canceled { Err(Error::Closed) } else { Ok(()) });
                }
                self.runtime.inner.changed.notify_waiters();
                return;
            }
        }
        let waker = Waker::from(self.clone());
        let polled = catch_unwind(AssertUnwindSafe(|| {
            future
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
        }));
        let enqueue = {
            let mut state = self.state.lock().unwrap();
            state.future = future;
            state.running = false;
            match polled {
                Ok(Poll::Pending) => {}
                Ok(Poll::Ready(result)) => {
                    state.result = Some(result);
                    state.cleanup = true;
                }
                Err(_) => {
                    self.runtime.inner.authority.retire();
                    state.result = Some(Err(Error::Protocol("owned callback panicked")));
                    state.cleanup = true;
                }
            }
            let enqueue = state.cleanup || state.canceled || state.wake;
            state.wake = false;
            enqueue
        };
        if enqueue {
            self.schedule(false);
        }
    }
}

/// Dropping an observation requests cleanup, never drops accepted captures on
/// the observer. Blocking timeout is uncertain and leaves the task accounted.
pub struct OwnedCall<T: Send + 'static> {
    task: Arc<Task<T>>,
    cell: Arc<Cell<()>>,
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
        self.schedule(true);
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
        self.task.schedule(true);
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
            self.task.schedule(true);
            return Err(Error::Timeout);
        }
        let (result, permit) = {
            let mut state = self.task.state.lock().unwrap();
            assert!(state.completed && !state.finished);
            state.finished = true;
            (state.result.take().unwrap(), state.permit.take())
        };
        self.consumed = true;
        drop(permit);
        self.task.runtime.inner.changed.notify_waiters();
        result
    }
    pub fn wait(mut self) -> Result<T> {
        ExecutionRuntime::check_entry(None)?;
        let result = {
            let mut state = self.cell.value.lock().unwrap();
            loop {
                if let Some(result) = state.result.take() {
                    break result;
                }
                let remaining = self.until.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(Error::Timeout);
                }
                let (next, _) = self.cell.ready.wait_timeout(state, remaining).unwrap();
                state = next;
            }
        };
        result?;
        self.take_result()
    }
}
impl<T: Send + 'static> Future for OwnedCall<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = {
            let mut state = this.cell.value.lock().unwrap();
            match state.result.take() {
                Some(result) => Some(result),
                None => {
                    state.waker = Some(cx.waker().clone());
                    None
                }
            }
        };
        match result {
            Some(Ok(())) => Poll::Ready(this.take_result()),
            Some(Err(error)) => {
                this.consumed = true;
                Poll::Ready(Err(error))
            }
            None => Poll::Pending,
        }
    }
}
impl<T: Send + 'static> Drop for OwnedCall<T> {
    fn drop(&mut self) {
        if !self.consumed {
            self.task.schedule(true);
        }
    }
}

#[cfg(test)]
#[path = "execution_ownership_tests.rs"]
mod ownership_tests;
#[cfg(test)]
#[path = "execution_tests.rs"]
mod tests;
