use crate::{supervisor::lock, *};
use std::sync::{atomic::Ordering, Arc};

/// Caller-supplied storage engine, exclusively owned by a [`Store`].
///
/// Construct configuration only; create workers/resources in `initialize`.
/// Store calls initialization once on its independent Tokio supervisor, then
/// serializes `execute` calls and calls `close`, including after initialization
/// failure. Futures must yield: move blocking IO/copies onto an owned worker.
///
/// Implementations must honor the logical record/snapshot and configured
/// storage budgets. Return success only after actual commit. An operation's
/// future is never cancelled on deadline or caller loss. Retain external
/// ownership through the context until positively witnessed cleanup, and mark
/// worker loss when a returned result cannot prove the worker stopped.
pub trait Backend: Send {
    fn initialize(&mut self, settings: Settings, context: BackendContext) -> StoreFuture<'_, ()>;
    fn execute(&mut self, request: Request) -> StoreFuture<'_, Output>;
    fn close(&mut self) -> StoreFuture<'_, ()>;
}

impl<B: Backend + ?Sized> Backend for Box<B> {
    fn initialize(&mut self, settings: Settings, context: BackendContext) -> StoreFuture<'_, ()> {
        (**self).initialize(settings, context)
    }

    fn execute(&mut self, request: Request) -> StoreFuture<'_, Output> {
        (**self).execute(request)
    }

    fn close(&mut self) -> StoreFuture<'_, ()> {
        (**self).close()
    }
}

/// Opaque lifecycle integration for trusted [`Backend`] implementations.
///
/// This does not expose operation admission or the supervisor's internal state.
#[derive(Clone)]
pub struct BackendContext {
    pub(crate) shared: Arc<supervisor::Shared>,
}

impl BackendContext {
    /// Retain an exclusion/resource guard independently of engine/task lifetime.
    /// At most one guard is allowed; group multiple resources into one owner.
    pub fn retain_resource(&self, resource: impl Send + 'static) -> Result<()> {
        let mut slot = lock(&self.shared.resource);
        if slot.is_some() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "backend ownership resource already retained",
            ));
        }
        *slot = Some(Box::new(resource));
        Ok(())
    }

    /// Release the guard only after positively witnessing engine quiescence.
    /// A timeout, cancelled wait or lost worker channel is not such a witness.
    pub fn release_resource(&self) {
        let resource = lock(&self.shared.resource).take();
        drop(resource);
    }

    /// Terminally report worker loss, retaining unresolved operation ownership.
    /// Return an Unknown error from the operation/initialization/close as well.
    pub fn mark_worker_lost(&self) {
        self.shared.mark_worker_lost();
    }

    /// Gate mutation admission while reads and maintenance remain available.
    pub fn set_write_pressure(&self, pressured: bool) {
        self.shared.pressure.store(pressured, Ordering::Release);
    }

    /// Surface a maintenance failure without rewriting an already committed result.
    pub fn record_failure(&self, error: Error) {
        self.shared.failure(error);
    }
}
