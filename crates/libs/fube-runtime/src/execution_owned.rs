//! Finite disposal scopes and dormant release entries used by runtime and data.
use super::*;
#[cfg(test)]
#[path = "execution_owned_tests.rs"]
mod tests;

thread_local! {
    // Accounting capture only: never native invocation authority.
    static CAPTURES: RefCell<Vec<CaptureEntry>> = const { RefCell::new(Vec::new()) };
}
type CaptureEntry = (Arc<AtomicUsize>, Arc<Mutex<Vec<DisposalReceipt>>>);

#[derive(Clone, Default)]
pub struct DisposalTracker {
    pub(super) pending: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
impl DisposalTracker {
    pub fn current_batch(&self) -> DisposalBatch {
        CAPTURES.with(|stack| {
            stack
                .borrow()
                .iter()
                .rev()
                .find(|(id, _)| Arc::ptr_eq(id, &self.pending))
                .map(|(_, receipts)| DisposalBatch(receipts.lock().unwrap().clone()))
                .unwrap_or_default()
        })
    }
    pub fn is_drained(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
    }
    pub async fn drained(&self) {
        loop {
            let changed = self.changed.notified();
            if self.is_drained() {
                return;
            }
            changed.await;
        }
    }
    pub async fn changed(&self) {
        self.changed.notified().await;
    }
    /// Capture just the releases performed by this synchronous owner operation.
    /// Other threads/engines cannot append to this operation's receipt batch.
    pub fn capture(&self) -> DisposalCapture {
        let receipts = Arc::new(Mutex::new(Vec::new()));
        CAPTURES.with(|stack| {
            stack
                .borrow_mut()
                .push((self.pending.clone(), receipts.clone()))
        });
        DisposalCapture {
            tracker: self.clone(),
            receipts,
            finished: false,
            _thread: std::marker::PhantomData,
        }
    }
    fn enroll(&self, receipt: DisposalReceipt, scope: &Arc<Scope>) {
        self.pending.fetch_add(1, Ordering::AcqRel);
        scope.trackers.lock().unwrap().push(self.clone());
        CAPTURES.with(|stack| {
            if let Some((_, receipts)) = stack
                .borrow()
                .iter()
                .rev()
                .find(|(id, _)| Arc::ptr_eq(id, &self.pending))
            {
                receipts.lock().unwrap().push(receipt);
            }
        });
    }
}
pub struct DisposalCapture {
    tracker: DisposalTracker,
    receipts: Arc<Mutex<Vec<DisposalReceipt>>>,
    finished: bool,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl DisposalCapture {
    pub fn finish(mut self) -> DisposalBatch {
        self.leave();
        DisposalBatch(std::mem::take(&mut *self.receipts.lock().unwrap()))
    }
    fn leave(&mut self) {
        if self.finished {
            return;
        }
        CAPTURES.with(|stack| {
            let (id, receipts) = stack.borrow_mut().pop().unwrap();
            assert!(Arc::ptr_eq(&id, &self.tracker.pending));
            assert!(Arc::ptr_eq(&receipts, &self.receipts));
        });
        self.finished = true;
    }
}
impl Drop for DisposalCapture {
    fn drop(&mut self) {
        self.leave();
    }
}
#[derive(Default)]
pub struct DisposalBatch(Vec<DisposalReceipt>);
impl DisposalBatch {
    pub fn is_complete(&self) -> bool {
        self.0.iter().all(DisposalReceipt::is_complete)
    }
    pub fn append(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }
    pub fn result(&self) -> Result<()> {
        for receipt in &self.0 {
            if !receipt.is_complete() {
                return Err(Error::ResourceLimit("disposal still pending"));
            }
            receipt
                .0
                .scope
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .outcome()?;
        }
        Ok(())
    }
    pub async fn wait(&self) -> Result<()> {
        let mut outcome = Ok(());
        for receipt in &self.0 {
            let result = receipt.wait().await;
            if outcome.is_ok() {
                outcome = result;
            }
        }
        outcome
    }
}

#[derive(Clone)]
pub struct DisposalReceipt(Arc<ReceiptState>);
struct ReceiptState {
    scope: Mutex<Option<Arc<Scope>>>,
    changed: Notify,
}
impl DisposalReceipt {
    fn new() -> Self {
        Self(Arc::new(ReceiptState {
            scope: Mutex::new(None),
            changed: Notify::new(),
        }))
    }
    fn bind(&self, scope: Arc<Scope>) {
        *self.0.scope.lock().unwrap() = Some(scope);
        self.0.changed.notify_waiters();
    }
    pub fn is_complete(&self) -> bool {
        self.0
            .scope
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|scope| *scope.completion.lock().unwrap())
    }
    pub async fn wait(&self) -> Result<()> {
        let scope = loop {
            let changed = self.0.changed.notified();
            if let Some(scope) = self.0.scope.lock().unwrap().clone() {
                break scope;
            }
            changed.await;
        };
        if ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .and_then(|entry| entry.scope.as_ref())
                .is_some_and(|current| Arc::ptr_eq(current, &scope))
        }) {
            return Err(Error::InvalidInput("waiting for own disposal"));
        }
        scope.wait().await
    }
    pub fn wait_blocking(&self) -> Result<()> {
        ExecutionRuntime::check_entry(None)?;
        let scope = self
            .0
            .scope
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::InvalidInput("release has not been submitted"))?;
        if ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .and_then(|entry| entry.scope.as_ref())
                .is_some_and(|current| Arc::ptr_eq(current, &scope))
        }) {
            return Err(Error::InvalidInput("waiting for own disposal"));
        }
        let mut completion = scope.completion.lock().unwrap();
        while !*completion {
            let remaining = scope.until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            completion = scope
                .completed
                .wait_timeout(completion, remaining)
                .unwrap()
                .0;
        }
        scope.outcome()
    }
}

pub(super) struct Scope {
    cleanup: bool,
    active: Mutex<Option<Arc<Active>>>,
    pub(super) remaining: AtomicUsize,
    until: Instant,
    origin: CallOrigin,
    origins: Mutex<Vec<CallOrigin>>,
    parent: Option<Arc<Scope>>,
    error: Mutex<Option<Error>>,
    trackers: Mutex<Vec<DisposalTracker>>,
    changed: Notify,
    completion: Mutex<bool>,
    completed: Condvar,
    pending_results: AtomicUsize,
    publications: Mutex<Vec<std::sync::Weak<dyn Publication>>>,
}
impl Scope {
    fn new(
        active: Arc<Active>,
        parent: Option<Arc<Scope>>,
        origin: CallOrigin,
        until: Instant,
    ) -> Arc<Self> {
        Self::new_reserved(active, parent, origin, until, false)
    }
    fn callback(
        active: Arc<Active>,
        parent: Option<Arc<Scope>>,
        origin: CallOrigin,
        until: Instant,
    ) -> Arc<Self> {
        Self::new_kind(active, parent, origin, until, false, false)
    }
    fn new_reserved(
        active: Arc<Active>,
        parent: Option<Arc<Scope>>,
        origin: CallOrigin,
        until: Instant,
        parent_reserved: bool,
    ) -> Arc<Self> {
        Self::new_kind(active, parent, origin, until, parent_reserved, true)
    }
    fn new_kind(
        active: Arc<Active>,
        parent: Option<Arc<Scope>>,
        origin: CallOrigin,
        until: Instant,
        parent_reserved: bool,
        cleanup: bool,
    ) -> Arc<Self> {
        if !parent_reserved {
            if let Some(parent) = &parent {
                parent.add();
            }
        }
        active.children.fetch_add(1, Ordering::AcqRel);
        let scope = Arc::new(Self {
            cleanup,
            until: until.min(active.until()),
            active: Mutex::new(Some(active.clone())),
            remaining: AtomicUsize::new(1),
            origin,
            origins: Mutex::new(vec![origin]),
            parent,
            error: Mutex::new(None),
            trackers: Mutex::new(Vec::new()),
            changed: Notify::new(),
            completion: Mutex::new(false),
            completed: Condvar::new(),
            pending_results: AtomicUsize::new(0),
            publications: Mutex::new(Vec::new()),
        });
        active.scopes.lock().unwrap().push(Arc::downgrade(&scope));
        active.refresh();
        scope
    }
    fn add(&self) {
        assert!(self.remaining.fetch_add(1, Ordering::AcqRel) > 0);
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .children
            .fetch_add(1, Ordering::AcqRel);
    }
    fn add_origin(&self, origin: CallOrigin) {
        let mut origins = self.origins.lock().unwrap();
        if !origins.contains(&origin) {
            origins.push(origin);
        }
    }
    pub(super) fn contains_engine(&self, engine: u64) -> bool {
        engine != 0
            && self
                .origins
                .lock()
                .unwrap()
                .iter()
                .any(|origin| origin.engine == engine)
    }
    fn enter(self: &Arc<Self>, origin: CallOrigin, cleanup: bool) -> Entry {
        self.add_origin(origin);
        Entry::scoped(EntryContext {
            origin,
            active: self.active.lock().unwrap().as_ref().unwrap().clone(),
            scope: Some(self.clone()),
            cleanup,
            release_budget: None,
        })
    }
    fn fail(&self, error: Error) {
        *self.error.lock().unwrap() = Some(error);
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .authority
            .retire();
    }
    fn finish(&self) {
        let active = self.active.lock().unwrap().as_ref().unwrap().clone();
        if Instant::now() >= self.until {
            *self.error.lock().unwrap() = Some(Error::Timeout);
            active.authority.retire_at(self.until);
        }
        let last = self.remaining.fetch_sub(1, Ordering::AcqRel) == 1;
        let trackers = if last {
            if self.pending_results.load(Ordering::Acquire) == 0 {
                self.active.lock().unwrap().take();
            }
            active.refresh();
            if let Some(parent) = &self.parent {
                if let Some(error) = self.error.lock().unwrap().clone() {
                    parent.fail(error);
                }
                parent.finish();
            }
            std::mem::take(&mut *self.trackers.lock().unwrap())
        } else {
            Vec::new()
        };
        let changed = active.changed.clone();
        if last {
            *self.completion.lock().unwrap() = true;
            self.completed.notify_all();
            self.changed.notify_waiters();
            for tracker in trackers {
                tracker.pending.fetch_sub(1, Ordering::AcqRel);
                tracker.changed.notify_waiters();
            }
        }
        active.children.fetch_sub(1, Ordering::AcqRel);
        drop(active);
        changed.notify_waiters();
    }
    fn outcome(&self) -> Result<()> {
        self.error.lock().unwrap().clone().map_or(Ok(()), Err)
    }
    fn release_result(&self) {
        if Instant::now() >= self.until {
            if let Some(active) = self.active.lock().unwrap().as_ref() {
                active.authority.retire_at(self.until);
            }
        }
        if self.pending_results.fetch_sub(1, Ordering::AcqRel) == 1
            && self.remaining.load(Ordering::Acquire) == 0
        {
            if let Some(active) = self.active.lock().unwrap().take() {
                if Instant::now() >= self.until {
                    active.authority.retire_at(self.until);
                }
                active.refresh();
            }
        }
    }
    async fn wait(&self) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            if *self.completion.lock().unwrap() {
                return self.outcome();
            }
            changed.await;
        }
    }
}
impl Active {
    fn refresh(&self) {
        let mut scopes = self.scopes.lock().unwrap();
        let mut until = self.until();
        scopes.retain(|scope| {
            if let Some(scope) = scope.upgrade() {
                if scope.remaining.load(Ordering::Acquire) != 0
                    || scope.pending_results.load(Ordering::Acquire) != 0
                {
                    until = until.min(scope.until);
                    return true;
                }
                // A concurrent final result release/finish can make a scope
                // removable just as it expires. Latch before replacing its slot.
                if Instant::now() >= scope.until {
                    self.authority.retire_at(scope.until);
                }
            }
            false
        });
        if let Some(deadline) = &self._deadline {
            deadline.set_effective_until(until);
        }
    }
}

#[derive(Clone)]
pub struct ObjectQuota(Arc<Semaphore>);
impl ObjectQuota {
    pub fn new(limit: usize) -> Result<Self> {
        if limit == 0 || limit > 64 {
            return Err(Error::InvalidInput("object quota must be 1..=64"));
        }
        Ok(Self(Arc::new(Semaphore::new(limit))))
    }
}
pub struct ObjectReservation<T: Send + 'static> {
    slot: Arc<ReleaseSlot<T>>,
}
pub struct ConstructionReservation<T: Send + 'static> {
    slot: Arc<ReleaseSlot<T>>,
}
pub struct ManagedObject<T: Send + 'static> {
    value: Option<T>,
    slot: Arc<ReleaseSlot<T>>,
}
impl<T: Send + 'static> ManagedObject<T> {
    pub fn get(&self) -> &T {
        self.value.as_ref().unwrap()
    }
    pub fn receipt(&self) -> DisposalReceipt {
        self.slot.receipt.clone()
    }
    pub fn publication(&self) -> ObjectPublication {
        ObjectPublication(self.slot.clone())
    }
}
impl<T: Send + 'static> ObjectReservation<T> {
    pub fn publish(self, value: T) -> ManagedObject<T> {
        ManagedObject {
            value: Some(value),
            slot: self.slot,
        }
    }
}
impl<T: Send + 'static> ConstructionReservation<T> {
    /// Retain the construction budget until explicit receiving-owner acceptance
    /// or actual rejected-result destruction.
    pub fn construct(self, value: T) -> ManagedObject<T> {
        ManagedObject {
            value: Some(value),
            slot: self.slot,
        }
    }
}
trait Publication: Send + Sync {
    fn accept(&self) -> Result<()>;
    fn releasing(&self) -> bool;
    fn pending(&self) -> bool;
}
#[derive(Clone)]
pub struct ObjectPublication(Arc<dyn Publication>);
impl ObjectPublication {
    pub fn accept(&self) -> Result<()> {
        self.0.accept()
    }
}
pub(crate) struct PublicationBatch(Arc<Scope>);
impl PublicationBatch {
    pub(crate) fn accept(self) -> Result<()> {
        for publication in self.0.publications.lock().unwrap().drain(..) {
            if let Some(publication) = publication.upgrade() {
                if !publication.releasing() {
                    publication.accept()?;
                }
            }
        }
        Ok(())
    }
}
struct PendingPublication {
    active: Arc<Active>,
    scope: Option<Arc<Scope>>,
    until: Instant,
}
impl Drop for PendingPublication {
    fn drop(&mut self) {
        if let Some(scope) = &self.scope {
            scope.release_result();
        }
    }
}
struct ReleaseSlot<T: Send + 'static> {
    runtime: ExecutionRuntime,
    _quota: OwnedSemaphorePermit,
    dormant: Mutex<Option<OwnedSemaphorePermit>>,
    origin: CallOrigin,
    payload: Mutex<Option<T>>,
    tracker: DisposalTracker,
    receipt: DisposalReceipt,
    fenced: Mutex<Option<Arc<ReleaseSlot<T>>>>,
    pending: Mutex<Option<PendingPublication>>,
    releasing: AtomicBool,
}
impl<T: Send + 'static> ReleaseSlot<T> {
    fn set_pending(self: &Arc<Self>, active: Arc<Active>, scope: Option<Arc<Scope>>) {
        let until = scope
            .as_ref()
            .map_or_else(|| active.until(), |scope| scope.until.min(active.until()));
        if let Some(scope) = &scope {
            scope.pending_results.fetch_add(1, Ordering::AcqRel);
            let publication: Arc<dyn Publication> = self.clone();
            let mut publications = scope.publications.lock().unwrap();
            publications.retain(|publication| {
                publication
                    .upgrade()
                    .is_some_and(|publication| publication.pending())
            });
            publications.push(Arc::downgrade(&publication));
        }
        *self.pending.lock().unwrap() = Some(PendingPublication {
            active,
            scope,
            until,
        });
    }
}
impl<T: Send + 'static> Publication for ReleaseSlot<T> {
    fn pending(&self) -> bool {
        self.pending.lock().unwrap().is_some() && !self.releasing()
    }
    fn releasing(&self) -> bool {
        self.releasing.load(Ordering::Acquire)
    }
    fn accept(&self) -> Result<()> {
        let mut pending = self.pending.lock().unwrap();
        if self.releasing.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if pending
            .as_ref()
            .is_some_and(|pending| Instant::now() >= pending.until)
        {
            self.runtime.retire();
            return Err(Error::Timeout);
        }
        drop(pending.take());
        Ok(())
    }
}
impl<T: Send + 'static> Drop for ReleaseSlot<T> {
    fn drop(&mut self) {
        self.dormant.get_mut().unwrap().take();
        self.runtime.inner.changed.notify_waiters();
    }
}
impl<T: Send + 'static> Drop for ManagedObject<T> {
    fn drop(&mut self) {
        let slot = self.slot.clone();
        slot.releasing.store(true, Ordering::Release);
        *slot.payload.lock().unwrap() = self.value.take();
        let pending = slot.pending.lock().unwrap();
        let scope = if let Some(pending) = pending.as_ref() {
            let parent = ENTRY.with(|entry| {
                entry
                    .borrow()
                    .as_ref()
                    .filter(|entry| {
                        Arc::ptr_eq(&entry.active.authority, &slot.runtime.inner.authority)
                    })
                    .and_then(|entry| entry.scope.clone())
                    .filter(|scope| scope.remaining.load(Ordering::Acquire) != 0)
            });
            let until = parent
                .as_ref()
                .map_or(pending.until, |parent| pending.until.min(parent.until));
            Ok(Scope::new(
                pending.active.clone(),
                parent,
                slot.origin,
                until,
            ))
        } else {
            slot.runtime.disposal_scope(slot.origin, None)
        };
        drop(pending);
        match scope {
            Ok(scope) => {
                slot.tracker.enroll(slot.receipt.clone(), &scope);
                slot.receipt.bind(scope);
                let runtime = slot.runtime.clone();
                runtime.inner.cleanup.spawn(
                    &runtime.inner.authority,
                    async move {
                        tokio::task::block_in_place(|| slot.release());
                    },
                    None,
                );
            }
            Err(_) => {
                slot.tracker.pending.fetch_add(1, Ordering::AcqRel);
                *slot.fenced.lock().unwrap() = Some(slot.clone());
                slot.runtime.inner.authority.retire();
            }
        }
    }
}
impl<T: Send + 'static> ReleaseSlot<T> {
    fn release(self: Arc<Self>) {
        let scope = self
            .receipt
            .0
            .scope
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        let _entered = self.runtime.inner.application.enter();
        let entry = scope.enter(self.origin, true);
        let value = self.payload.lock().unwrap().take();
        if catch_unwind(AssertUnwindSafe(|| drop(value))).is_err() {
            scope.fail(Error::Protocol("managed destructor panicked"));
        }
        drop(entry);
        self.pending.lock().unwrap().take();
        self.dormant.lock().unwrap().take();
        scope.finish();
    }
}

pub struct CleanupPermit {
    runtime: ExecutionRuntime,
    scope: Option<Arc<Scope>>,
    ready: Option<OwnedSemaphorePermit>,
}
impl CleanupPermit {
    pub fn dispose<T: Send + 'static>(self, value: T) -> DisposalReceipt {
        self.run(move || {
            drop(value);
            Ok(())
        })
    }
    pub fn run(mut self, body: impl FnOnce() -> Result<()> + Send + 'static) -> DisposalReceipt {
        let scope = self.scope.take().unwrap();
        let receipt = DisposalReceipt::new();
        receipt.bind(scope.clone());
        let runtime = self.runtime.clone();
        let quota = self.ready.take();
        self.runtime.inner.cleanup.spawn(
            &self.runtime.inner.authority,
            async move {
                tokio::task::block_in_place(|| {
                    run_body(runtime, scope, Box::new(body), quota, true)
                });
            },
            None,
        );
        receipt
    }
}
impl Drop for CleanupPermit {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.finish();
        }
    }
}
type CleanupBody = Box<dyn FnOnce() -> Result<()> + Send>;
fn run_body(
    runtime: ExecutionRuntime,
    scope: Arc<Scope>,
    body: CleanupBody,
    quota: Option<OwnedSemaphorePermit>,
    cleanup_lane: bool,
) {
    // Terminal bodies are internal joins only; never install an implicit
    // application scheduler on the library-only coordinator thread.
    let _entered = cleanup_lane.then(|| runtime.inner.application.enter());
    let entry = scope.enter(scope.origin, true);
    let result = catch_unwind(AssertUnwindSafe(body))
        .unwrap_or(Err(Error::Protocol("owned disposal/join panicked")));
    if let Err(error) = result {
        if !cleanup_lane {
            // The coordinator owns joins, not arbitrary cause formatting or
            // destruction. Transfer to the existing cleanup lane with the SAME
            // scope, deadline and quota; never reserve another active record.
            drop(entry);
            let owner = runtime.clone();
            runtime.inner.cleanup.spawn(
                &runtime.inner.authority,
                async move {
                    tokio::task::block_in_place(|| {
                        run_body(owner, scope, Box::new(|| Err(error)), quota, true);
                    });
                },
                None,
            );
            return;
        }
        let error = error.supervised(&runtime);
        scope.fail(error);
    }
    drop(entry);
    drop(quota);
    scope.finish();
}
pub struct TerminalPermit {
    runtime: ExecutionRuntime,
    quota: Option<OwnedSemaphorePermit>,
    origin: CallOrigin,
}
pub struct TerminalRecord {
    runtime: ExecutionRuntime,
    quota: Option<OwnedSemaphorePermit>,
    scope: Option<Arc<Scope>>,
    cleanup_reserved: AtomicBool,
}
pub struct DeferredCleanup {
    runtime: ExecutionRuntime,
    parent: Option<Arc<Scope>>,
    budget: Duration,
}
/// An already-admitted command's ownership token. Owner-side enrollment moves
/// no user code onto the owner and does not provide invocation authority.
pub struct CommandCleanup {
    scope: Arc<Scope>,
}
impl CommandCleanup {
    pub fn enter(&self) -> impl Drop {
        let mut context = EntryContext {
            origin: self.scope.origin,
            active: self.scope.active.lock().unwrap().as_ref().unwrap().clone(),
            scope: Some(self.scope.clone()),
            cleanup: true,
            release_budget: Some(Duration::from_millis(250)),
        };
        context.cleanup = true;
        Entry::scoped(context)
    }
}
impl Drop for CommandCleanup {
    fn drop(&mut self) {
        self.scope.finish();
    }
}
impl DeferredCleanup {
    pub fn dispose<T: Send + 'static>(mut self, value: T) -> DisposalReceipt {
        let parent = self.parent.take().unwrap();
        let active = parent.active.lock().unwrap().as_ref().unwrap().clone();
        let scope = Scope::new_reserved(
            active,
            Some(parent.clone()),
            parent.origin,
            (Instant::now() + self.budget).min(parent.until),
            true,
        );
        CleanupPermit {
            runtime: self.runtime.clone(),
            scope: Some(scope),
            ready: None,
        }
        .dispose(value)
    }
}
impl Drop for DeferredCleanup {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.take() {
            parent.finish();
        }
    }
}
impl TerminalRecord {
    pub fn cleanup_after_callbacks(&self, budget: Duration) -> Result<DeferredCleanup> {
        if budget.is_zero() || budget > Duration::from_millis(250) {
            return Err(Error::InvalidInput("invalid terminal cleanup budget"));
        }
        self.cleanup_reserved
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::ResourceLimit("terminal cleanup already reserved"))?;
        let parent = self.scope.as_ref().unwrap().clone();
        parent.add();
        Ok(DeferredCleanup {
            runtime: self.runtime.clone(),
            parent: Some(parent),
            budget,
        })
    }
    pub fn run(mut self, body: impl FnOnce() -> Result<()> + Send + 'static) -> DisposalReceipt {
        let scope = self.scope.take().unwrap();
        let receipt = DisposalReceipt::new();
        receipt.bind(scope.clone());
        let runtime = self.runtime.clone();
        let quota = self.quota.take();
        self.runtime
            .inner
            .terminal
            .enqueue(&self.runtime.inner.authority, move || {
                run_body(runtime, scope, Box::new(body), quota, false);
            });
        receipt
    }
}
impl Drop for TerminalRecord {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.finish();
        }
    }
}
impl TerminalPermit {
    pub fn begin(&mut self) -> Result<TerminalRecord> {
        if self.quota.is_none() {
            return Err(Error::InvalidInput("terminal ticket already consumed"));
        }
        let scope = self
            .runtime
            .disposal_scope(self.origin, Some(Duration::from_secs(30)))?;
        Ok(TerminalRecord {
            runtime: self.runtime.clone(),
            quota: self.quota.take(),
            scope: Some(scope),
            cleanup_reserved: AtomicBool::new(false),
        })
    }
    pub fn run(
        mut self,
        body: impl FnOnce() -> Result<()> + Send + 'static,
    ) -> Result<DisposalReceipt> {
        let scope = match self
            .runtime
            .disposal_scope(self.origin, Some(Duration::from_secs(30)))
        {
            Ok(scope) => scope,
            Err(error) => {
                // Pre-owned terminal input cannot be discarded on overload.
                self.fence(body);
                return Err(error);
            }
        };
        let receipt = DisposalReceipt::new();
        receipt.bind(scope.clone());
        let runtime = self.runtime.clone();
        let quota = self.quota.take();
        self.runtime
            .inner
            .terminal
            .enqueue(&self.runtime.inner.authority, move || {
                run_body(runtime, scope, Box::new(body), quota, false);
            });
        Ok(receipt)
    }
    pub fn fence(mut self, body: impl FnOnce() -> Result<()> + Send + 'static) {
        self.runtime.inner.authority.retire();
        ENTRY.with(|entry| {
            if let Some(scope) = entry
                .borrow()
                .as_ref()
                .and_then(|entry| entry.scope.as_ref())
            {
                scope.fail(Error::InvalidInput("terminal final-destruction dependency"));
            }
        });
        // A terminally retained entry has no fresh deadline/ordinary admission.
        // Its captures, terminal quota and thread handles survive until fencing.
        let work = Arc::new(FencedTerminal {
            _body: Mutex::new(Some(Box::new(body) as CleanupBody)),
            _quota: self.quota.take(),
            keep: Mutex::new(None),
        });
        *work.keep.lock().unwrap() = Some(work.clone());
    }
}
struct FencedTerminal {
    _body: Mutex<Option<CleanupBody>>,
    _quota: Option<OwnedSemaphorePermit>,
    keep: Mutex<Option<Arc<FencedTerminal>>>,
}

struct ScopedFuture<T> {
    inner: Option<OwnedFuture<T>>,
    scope: Arc<Scope>,
}
impl<T> Future for ScopedFuture<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let entry = self.scope.enter(self.scope.origin, false);
        let result = self.inner.as_mut().unwrap().as_mut().poll(cx);
        drop(entry);
        result
    }
}
impl<T> Drop for ScopedFuture<T> {
    fn drop(&mut self) {
        struct Finish(Arc<Scope>);
        impl Drop for Finish {
            fn drop(&mut self) {
                self.0.finish();
            }
        }
        let _finish = Finish(self.scope.clone());
        let _entry = self.scope.enter(self.scope.origin, true);
        drop(self.inner.take());
    }
}
impl ExecutionRuntime {
    pub(crate) fn has_running_callbacks(&self) -> bool {
        self.inner.callbacks_running.load(Ordering::Acquire) != 0
    }
    pub(crate) fn factory_quota(&self) -> ObjectQuota {
        self.inner.factory_quota.clone()
    }
    pub(crate) fn service_quota(&self) -> ObjectQuota {
        self.inner.service_quota.clone()
    }
    pub(crate) fn control_quota(&self) -> ObjectQuota {
        self.inner.control_quota.clone()
    }
    pub fn root_origin() -> Option<CallOrigin> {
        ENTRY.with(|entry| {
            entry.borrow().as_ref().map(|entry| {
                let mut origin = entry.active.origin;
                origin.generation = entry.active.generation.load(Ordering::Acquire);
                origin
            })
        })
    }
    pub fn current_origin() -> Option<CallOrigin> {
        ENTRY.with(|entry| entry.borrow().as_ref().map(|entry| entry.origin))
    }
    pub fn is_cleanup_entry() -> bool {
        ENTRY.with(|entry| entry.borrow().as_ref().is_some_and(|entry| entry.cleanup))
    }
    pub fn in_owned_entry() -> bool {
        ENTRY.with(|entry| entry.borrow().is_some())
    }
    pub fn is_current_entry(&self) -> bool {
        ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .is_some_and(|entry| Arc::ptr_eq(&entry.active.authority, &self.inner.authority))
        })
    }
    /// Absolute deadline for a canonical synchronous waiting operation. This
    /// accepts only this owner's supervised entry, never an arbitrary async
    /// worker. It neither reserves recursively nor extends the enclosing budget.
    pub fn blocking_deadline(&self) -> Result<Instant> {
        Self::blocking_deadline_for(&self.inner.authority)
    }
    pub(crate) fn blocking_deadline_for(authority: &Arc<Authority>) -> Result<Instant> {
        ENTRY.with(|entry| match entry.borrow().as_ref() {
            Some(entry) if Arc::ptr_eq(&entry.active.authority, authority) && !entry.cleanup => {
                authority.check()?;
                Ok(entry
                    .scope
                    .as_ref()
                    .map_or_else(|| entry.active.until(), |scope| scope.until))
            }
            _ => Err(Error::new(
                crate::ErrorKind::InvalidExecutionContext,
                "canonical synchronous wait requires a reserved runtime execution entry",
            )),
        })
    }
    pub(crate) fn set_current_generation(&self, generation: u64) {
        ENTRY.with(|entry| {
            if let Some(entry) = entry.borrow_mut().as_mut() {
                if Arc::ptr_eq(&entry.active.authority, &self.inner.authority) {
                    entry.active.generation.store(generation, Ordering::Release);
                    entry.origin.generation = generation;
                }
            }
        });
    }
    pub fn check_constructor(&self) -> Result<()> {
        ENTRY.with(|entry| match entry.borrow().as_ref() {
            Some(entry)
                if Arc::ptr_eq(&entry.active.authority, &self.inner.authority)
                    && entry.active.lane == Some(CallLane::Native)
                    && !entry.cleanup =>
            {
                Ok(())
            }
            _ => Err(Error::InvalidInput(
                "invalid construction execution context",
            )),
        })
    }
    pub fn retire(&self) {
        self.inner.authority.retire();
    }
    pub fn reserve_terminal(&self, origin: CallOrigin) -> Result<TerminalPermit> {
        let _admission = self.inner.admission.lock().unwrap();
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.reserve_terminal_for_shutdown(origin)
    }
    pub(super) fn reserve_terminal_for_shutdown(
        &self,
        origin: CallOrigin,
    ) -> Result<TerminalPermit> {
        let quota = self
            .inner
            .terminal_tickets
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("terminal join tickets"))?;
        Ok(TerminalPermit {
            runtime: self.clone(),
            quota: Some(quota),
            origin,
        })
    }
    fn disposal_scope(&self, origin: CallOrigin, budget: Option<Duration>) -> Result<Arc<Scope>> {
        let parent = ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .filter(|entry| Arc::ptr_eq(&entry.active.authority, &self.inner.authority))
                .cloned()
        });
        if let Some(parent) = parent {
            let budget = budget.or(parent.release_budget);
            if budget.is_none() {
                if let Some(scope) = &parent.scope {
                    if scope.cleanup {
                        scope.add();
                        scope.add_origin(origin);
                        return Ok(scope.clone());
                    }
                }
            }
            let until = parent
                .scope
                .as_ref()
                .map_or_else(|| parent.active.until(), |scope| scope.until)
                .min(
                    budget.map_or_else(|| parent.active.until(), |budget| Instant::now() + budget),
                );
            return Ok(Scope::new(parent.active, parent.scope, origin, until));
        }
        let budget = budget
            .unwrap_or(Duration::from_millis(250))
            .min(Duration::from_millis(250));
        let record = self
            .inner
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("final release records"))?;
        let deadline = self.inner.authority.reserve_callback_deadline(true)?;
        self.inner.cleanup_owners.fetch_add(1, Ordering::AcqRel);
        let active = Arc::new(Active {
            origin,
            generation: std::sync::atomic::AtomicU64::new(origin.generation),
            allow_cleanup: true,
            _deadline: Some(deadline),
            until: std::sync::OnceLock::new(),
            budget,
            authority: self.inner.authority.clone(),
            _record: Some(record),
            _admission: None,
            children: AtomicUsize::new(0),
            changed: self.inner.changed.clone(),
            lane: None,
            cleanup_owner: Some(self.inner.cleanup_owners.clone()),
            scopes: Mutex::new(Vec::new()),
            cleanup_ready: Arc::new(Semaphore::new(1)),
        });
        Ok(Scope::new(active, None, origin, Instant::now() + budget))
    }
    pub fn command_cleanup(&self) -> Option<CommandCleanup> {
        let parent = ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .filter(|entry| Arc::ptr_eq(&entry.active.authority, &self.inner.authority))
                .cloned()
        })?;
        let until = parent
            .scope
            .as_ref()
            .map_or_else(|| parent.active.until(), |scope| scope.until);
        Some(CommandCleanup {
            scope: Scope::callback(parent.active, parent.scope, parent.origin, until),
        })
    }
    pub fn reserve_cleanup(&self, budget: Duration) -> Result<CleanupPermit> {
        let _admission = self.inner.admission.lock().unwrap();
        if self.inner.stopping.load(Ordering::Acquire) && !self.is_current_entry() {
            return Err(Error::Closed);
        }
        if budget.is_zero() || budget > Duration::from_secs(30) {
            return Err(Error::InvalidInput("invalid cleanup budget"));
        }
        let scope =
            self.disposal_scope(Self::current_origin().unwrap_or_default(), Some(budget))?;
        let ready = scope
            .active
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cleanup_ready
            .clone()
            .try_acquire_owned();
        match ready {
            Ok(ready) => Ok(CleanupPermit {
                runtime: self.clone(),
                scope: Some(scope),
                ready: Some(ready),
            }),
            Err(_) => {
                scope.finish();
                Err(Error::ResourceLimit("root cleanup route already reserved"))
            }
        }
    }
    pub fn reserve_object<T: Send + 'static>(
        &self,
        quota: &ObjectQuota,
        tracker: &DisposalTracker,
    ) -> Result<ObjectReservation<T>> {
        self.reserve_object_with_origin(quota, tracker, Self::current_origin().unwrap_or_default())
    }
    pub fn reserve_construction<T: Send + 'static>(
        &self,
        quota: &ObjectQuota,
        tracker: &DisposalTracker,
    ) -> Result<ConstructionReservation<T>> {
        self.reserve_construction_with_origin(
            quota,
            tracker,
            Self::current_origin().unwrap_or_default(),
        )
    }
    pub fn reserve_construction_with_origin<T: Send + 'static>(
        &self,
        quota: &ObjectQuota,
        tracker: &DisposalTracker,
        origin: CallOrigin,
    ) -> Result<ConstructionReservation<T>> {
        let entry = ENTRY
            .with(|entry| {
                entry
                    .borrow()
                    .as_ref()
                    .filter(|entry| Arc::ptr_eq(&entry.active.authority, &self.inner.authority))
                    .cloned()
            })
            .ok_or(Error::InvalidInput(
                "construction requires an owned callback",
            ))?;
        let ticket = self.reserve_object_with_origin(quota, tracker, origin)?;
        ticket.slot.set_pending(entry.active, entry.scope);
        Ok(ConstructionReservation { slot: ticket.slot })
    }
    pub fn reserve_object_with_origin<T: Send + 'static>(
        &self,
        quota: &ObjectQuota,
        tracker: &DisposalTracker,
        origin: CallOrigin,
    ) -> Result<ObjectReservation<T>> {
        let _admission = self.inner.admission.lock().unwrap();
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let charge = quota
            .0
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("published object quota"))?;
        let dormant = self
            .inner
            .dormant
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("dormant release tickets"))?;
        Ok(ObjectReservation {
            slot: Arc::new(ReleaseSlot {
                runtime: self.clone(),
                _quota: charge,
                dormant: Mutex::new(Some(dormant)),
                origin,
                payload: Mutex::new(None),
                tracker: tracker.clone(),
                receipt: DisposalReceipt::new(),
                fenced: Mutex::new(None),
                pending: Mutex::new(None),
                releasing: AtomicBool::new(false),
            }),
        })
    }
    pub(crate) async fn invoke<T: Send + 'static>(
        &self,
        timeout: Duration,
        operation: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        self.inner.authority.check()?;
        let (value, publication) = self.invoke_creation(timeout, operation).await?;
        publication.accept()?;
        Ok(value)
    }
    pub(crate) async fn invoke_creation<T: Send + 'static>(
        &self,
        timeout: Duration,
        operation: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Result<(T, PublicationBatch)> {
        self.inner.authority.check()?;
        if !Self::in_owned_entry() {
            let permit = self.reserve(CallLane::Native, CallOrigin::default(), timeout, true)?;
            let runtime = self.clone();
            return permit
                .run_future(async move { runtime.invoke_creation_inline(timeout, operation).await })
                .await;
        }
        self.invoke_creation_inline(timeout, operation).await
    }
    pub(crate) async fn invoke_cleanup<T: Send + 'static>(
        &self,
        timeout: Duration,
        operation: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        let output = if !Self::in_owned_entry() {
            let permit = self.reserve(CallLane::Native, CallOrigin::default(), timeout, true)?;
            let runtime = self.clone();
            permit
                .run_future(async move { runtime.invoke_creation_inline(timeout, operation).await })
                .await?
        } else {
            self.invoke_creation_inline(timeout, operation).await?
        };
        output.1.accept()?;
        Ok(output.0)
    }
    async fn invoke_creation_inline<T: Send + 'static>(
        &self,
        timeout: Duration,
        operation: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Result<(T, PublicationBatch)> {
        struct Running(Arc<Inner>);
        impl Drop for Running {
            fn drop(&mut self) {
                self.0.callbacks_running.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.inner.callbacks_running.fetch_add(1, Ordering::AcqRel);
        let _running = Running(self.inner.clone());
        let context = ENTRY.with(|entry| entry.borrow().as_ref().unwrap().clone());
        let until = (Instant::now() + timeout).min(context.active.until());
        let scope = Scope::callback(context.active, context.scope, context.origin, until);
        let mut future = Box::pin(ScopedFuture {
            inner: Some(Box::pin(operation)),
            scope: scope.clone(),
        });
        let result = match tokio::time::timeout(timeout, &mut future).await {
            Ok(result) => result,
            Err(_) => {
                self.inner.authority.retire();
                Err(Error::Timeout)
            }
        };
        drop(future);
        scope.wait().await?;
        result.map(|value| (value, PublicationBatch(scope)))
    }
}
impl OwnedCallPermit {
    pub fn object_reservation<T: Send + 'static>(
        &self,
        quota: &ObjectQuota,
        tracker: &DisposalTracker,
    ) -> Result<ConstructionReservation<T>> {
        let ticket = self
            .runtime
            .reserve_object_with_origin(quota, tracker, self.origin)?;
        let scope = ENTRY.with(|entry| {
            entry
                .borrow()
                .as_ref()
                .filter(|entry| Arc::ptr_eq(&entry.active.authority, &self.active.authority))
                .and_then(|entry| entry.scope.clone())
        });
        ticket.slot.set_pending(self.active.clone(), scope);
        Ok(ConstructionReservation { slot: ticket.slot })
    }
}
