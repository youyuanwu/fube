// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Ordinary Transaction.cpp:238–485 and TransactionBase.cpp:140–190 at SF
//! 51a7703. Observation lifetime is separate from shared-self activity lifetime.

use super::{
    identity,
    state::{CommitFailure, RetryDelay, StateMachine},
    status::{retryable, Status},
    version::Completion,
};
use crate::{
    ApiFuture, AtomicOperation, AtomicRedoOperation, Cancellation, Error, ITransaction,
    ITransactionManager, LockContext, OperationContext, OperationData, Result, SequenceNumber,
    StateProviderId, Transaction, TransactionBase, TransactionId, TransactionState,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex, Weak,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

/// Run the native no-suspension prefix now. Only pending work enters the executor.
/// The spawned future, not the returned observer, owns every captured activity.
pub(crate) fn own<T: Clone + Send + Sync + 'static>(
    mut work: ApiFuture<'static, T>,
) -> ApiFuture<'static, T> {
    match work.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => Box::pin(async move { result }),
        Poll::Pending => {
            let completion = Completion::new();
            let observer = completion.clone();
            let runtime = tokio::runtime::Handle::try_current()
                .expect("bounded A0 async activities require a Tokio executor");
            drop(runtime.spawn(async move {
                completion.complete(work.await);
            }));
            Box::pin(async move { observer.wait().await.as_ref().clone() })
        }
    }
}

/// Clock/timer side-effect seam; the native classifier and retry loop remain real.
pub(crate) struct RetryRuntime {
    pub(crate) now: Arc<dyn Fn() -> Duration + Send + Sync>,
    pub(crate) delay: Arc<dyn Fn(Duration) -> ApiFuture<'static, ()> + Send + Sync>,
}

impl Default for RetryRuntime {
    fn default() -> Self {
        let origin = Instant::now();
        Self {
            now: Arc::new(move || origin.elapsed()),
            delay: Arc::new(|delay| {
                Box::pin(async move {
                    tokio::time::sleep(delay).await;
                    Ok(())
                })
            }),
        }
    }
}

#[derive(Clone)]
struct FirstOperation {
    metadata: Option<OperationData>,
    undo: Option<OperationData>,
    redo: Option<OperationData>,
    provider: StateProviderId,
    context: Option<Arc<dyn OperationContext>>,
}

#[derive(Default)]
struct Writes {
    write: bool,
    first: Option<FirstOperation>,
}

pub(crate) struct KernelTransaction {
    self_weak: Weak<Self>,
    manager: Option<Weak<dyn ITransactionManager>>,
    id: TransactionId,
    primary: bool,
    state: StateMachine,
    commit_lsn: AtomicI64,
    visibility: AtomicI64,
    writes: Mutex<Writes>,
    locks: Mutex<Vec<Arc<dyn LockContext>>>,
    disposed: AtomicBool,
    delay: RetryDelay,
    retry: Arc<RetryRuntime>,
}

impl KernelTransaction {
    pub(crate) fn create(
        manager: Weak<dyn ITransactionManager>,
        primary: bool,
        retry: Arc<RetryRuntime>,
    ) -> Result<Arc<Self>> {
        Ok(Self::allocate(
            Some(manager),
            identity::generate()?,
            primary,
            retry,
        ))
    }

    /// Each decoded record receives its own Active transaction object.
    pub(crate) fn replay(id: TransactionId) -> Result<Arc<Self>> {
        identity::replay(id)?;
        Ok(Self::allocate(
            None,
            id,
            false,
            Arc::new(RetryRuntime::default()),
        ))
    }

    fn allocate(
        manager: Option<Weak<dyn ITransactionManager>>,
        id: TransactionId,
        primary: bool,
        retry: Arc<RetryRuntime>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            manager,
            id,
            primary,
            state: StateMachine::new(),
            commit_lsn: AtomicI64::new(-1),
            visibility: AtomicI64::new(-1),
            writes: Mutex::new(Writes::default()),
            locks: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
            delay: RetryDelay::new(),
            retry,
        })
    }

    fn shared(&self) -> Arc<Self> {
        self.self_weak
            .upgrade()
            .expect("activity has a shared transaction")
    }

    fn manager(&self) -> Result<Arc<dyn ITransactionManager>> {
        self.manager
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Status::ObjectClosed.error())
    }

    pub(crate) fn has_cached_first(&self) -> bool {
        self.writes.lock().expect("writes poisoned").first.is_some()
    }

    pub(crate) fn is_write(&self) -> bool {
        self.writes.lock().expect("writes poisoned").write
    }

    pub(crate) fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }

    pub(crate) fn system_abort(&self) -> Result<()> {
        self.state.system_begin_abort()?;
        self.start_abort();
        Ok(())
    }

    fn start_abort(&self) {
        let transaction = self.shared();
        drop(own(Box::pin(
            async move { transaction.abort_worker().await },
        )));
    }

    async fn abort_worker(self: Arc<Self>) -> Result<()> {
        if !self.is_write() {
            self.state.abort_successful();
            return Ok(());
        }
        // Manager expiry deliberately leaves Aborting, before completion hooks.
        let manager = self.manager()?;
        let first = self.writes.lock().expect("writes poisoned").first.clone();
        if let Some(first) = first {
            if let Some(context) = first.context {
                manager
                    .single_operation_transaction_abort_unlock(first.provider, context.as_ref())
                    .expect("native cached abort wrapper must succeed after its activity gate");
            }
            self.writes.lock().expect("writes poisoned").first = None;
            self.state.abort_successful();
            return Ok(());
        }
        loop {
            match manager.abort_transaction(self.clone()).await {
                Ok(_) => {
                    self.state.abort_successful();
                    return Ok(());
                }
                Err(error) if retryable(&error) => {
                    (self.retry.delay)(self.delay.get())
                        .await
                        .expect("native abort retry timer succeeds");
                    self.delay.double();
                }
                Err(error) => {
                    self.state.abort_faulted();
                    return Err(error);
                }
            }
        }
    }

    async fn commit_worker(
        self: Arc<Self>,
        manager: Arc<dyn ITransactionManager>,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> Result<()> {
        if !self.is_write() {
            self.state.commit_successful();
            return Ok(());
        }
        let started = (self.retry.now)();
        loop {
            let first = self.writes.lock().expect("writes poisoned").first.clone();
            let attempted = if let Some(first) = first {
                manager
                    .begin_transaction_async(
                        self.clone(),
                        first.metadata,
                        first.undo,
                        first.redo,
                        first.provider,
                        first.context,
                    )
                    .await
            } else {
                manager.commit_transaction(self.clone()).await
            };
            match attempted {
                Ok(_) => {
                    // Native successful single commit retains firstOperation_.
                    self.state.commit_successful();
                    return Ok(());
                }
                Err(error) => {
                    // The watch/token are consulted only for classified errors.
                    let decision = if retryable(&error) {
                        let cancelled = cancellation.is_cancelled();
                        let elapsed = if cancelled {
                            Duration::ZERO
                        } else {
                            (self.retry.now)().saturating_sub(started)
                        };
                        self.state
                            .commit_failure(&error, cancelled, elapsed, timeout)
                    } else {
                        self.state.commit_faulted();
                        CommitFailure::Faulted
                    };
                    match decision {
                        CommitFailure::Cancelled => return Err(Status::Cancelled.error()),
                        CommitFailure::TimedOut => return Err(Status::Timeout.error()),
                        CommitFailure::Faulted => return Err(error),
                        CommitFailure::Retry => {
                            (self.retry.delay)(self.delay.get())
                                .await
                                .expect("native commit retry timer succeeds");
                            self.delay.double();
                        }
                    }
                }
            }
        }
    }
}

impl TransactionBase for KernelTransaction {
    fn get_is_primary_transaction(&self) -> bool {
        self.primary
    }
    fn get_transaction_id(&self) -> TransactionId {
        self.id
    }
    fn get_is_atomic_operation(&self) -> bool {
        self.id < 0
    }
    fn get_is_valid_transaction(&self) -> bool {
        self.id != 0
    }
    fn get_state(&self) -> TransactionState {
        self.state.get_state()
    }
    fn get_commit_sequence_number(&self) -> SequenceNumber {
        self.commit_lsn.load(Ordering::Acquire)
    }
    fn set_commit_sequence_number(&self, value: SequenceNumber) -> Result<()> {
        if value <= 0 {
            return Err(Error::InvalidInput("nonpositive native commit LSN"));
        }
        self.commit_lsn
            .compare_exchange(-1, value, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| Error::InvalidInput("native commit LSN already assigned"))
    }
    fn add_lock_context(&self, context: Arc<dyn LockContext>) -> Result<()> {
        if !self.primary {
            return Err(Status::TransactionNotActive.error());
        }
        self.locks.lock().expect("locks poisoned").push(context);
        Ok(())
    }
    fn dispose(&self) -> Result<()> {
        if self.is_disposed() {
            return Ok(());
        }
        let abort = self.state.dispose_begin_abort(|| {
            let version = self.visibility.load(Ordering::Acquire);
            if version == -1 {
                Ok(())
            } else {
                self.manager()?.unregister(version)
            }
        });
        if abort.is_ok() {
            self.start_abort();
        }
        for context in self.locks.lock().expect("locks poisoned").iter() {
            if !context.is_tracking_context() {
                context.unlock()?;
            }
        }
        self.disposed.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for KernelTransaction {
    fn drop(&mut self) {
        if !self.disposed.load(Ordering::Acquire) {
            for context in self.locks.get_mut().expect("locks poisoned").iter() {
                if !context.is_tracking_context() {
                    let _ = context.unlock();
                }
            }
        }
    }
}

impl ITransaction for KernelTransaction {
    fn commit(&self) -> ApiFuture<'_, ()> {
        self.commit_with_options(None, Cancellation::default())
    }
    fn commit_with_options(
        &self,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        if let Err(error) = self.state.begin_commit() {
            return Box::pin(async move { Err(error) });
        }
        let manager = match self.manager() {
            Ok(manager) => manager,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let transaction = self.shared();
        own(Box::pin(async move {
            transaction
                .commit_worker(manager, timeout, cancellation)
                .await
        }))
    }
    fn abort(&self) -> Result<()> {
        self.state.user_begin_abort()?;
        self.start_abort();
        Ok(())
    }
}

impl Transaction for KernelTransaction {
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        provider: StateProviderId,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()> {
        self.state.add_operation(|| {
            if !self.primary {
                return Err(Status::TransactionNotActive.error());
            }
            let manager = self.manager()?;
            let mut writes = self.writes.lock().expect("writes poisoned");
            if !writes.write {
                manager.error_if_state_provider_is_not_registered(provider, self.id)?;
                writes.write = true;
                writes.first = Some(FirstOperation {
                    metadata,
                    undo,
                    redo,
                    provider,
                    context,
                });
                return Ok(());
            }

            if let Some(first) = &writes.first {
                manager.begin_transaction(
                    self,
                    first.metadata.clone(),
                    first.undo.clone(),
                    first.redo.clone(),
                    first.provider,
                    first.context.clone(),
                )?;
                writes.first = None;
            }
            manager.add_operation(self, metadata, undo, redo, provider, context)
        })
    }
    fn get_visibility_sequence_number(&self) -> ApiFuture<'_, SequenceNumber> {
        if let Err(error) = self.state.begin_read() {
            return Box::pin(async move { Err(error) });
        }
        let transaction = self.shared();
        own(Box::pin(async move {
            let result = async {
                let manager = transaction.manager()?;
                let cached = transaction.visibility.load(Ordering::Acquire);
                if cached != -1 {
                    return Ok(cached);
                }
                let version = manager.register().await?;
                transaction.visibility.store(version, Ordering::Release);
                Ok(version)
            }
            .await;
            transaction.state.end_read();
            result
        }))
    }
}

impl AtomicOperation for KernelTransaction {
    fn add_operation(
        &self,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: StateProviderId,
        _: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}

impl AtomicRedoOperation for KernelTransaction {
    fn add_operation(
        &self,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: StateProviderId,
        _: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}
