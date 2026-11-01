// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Published COM contexts with bounded, joined tasks and validated once-only End.
use std::{
    cell::RefCell,
    future::{poll_fn, Future},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};

use fube_data::Cancellation;
use mssf_com::FabricCommon::{
    IFabricAsyncOperationCallback, IFabricAsyncOperationContext, IFabricAsyncOperationContext_Impl,
};
use tokio::{runtime::Handle, sync::Notify, task::JoinHandle};
use windows_core::{implement, Interface, Ref, Result};

use crate::com;

pub(crate) const E_ABORT: windows_core::HRESULT = windows_core::HRESULT(0x80004004u32 as i32);
pub(crate) const E_PENDING: windows_core::HRESULT = windows_core::HRESULT(0x8000000au32 as i32);

pub(crate) struct Workers {
    pub(crate) stop: Cancellation,
    pub(crate) rt: Handle,
    handles: Mutex<Vec<Task>>,
    draining: tokio::sync::Mutex<()>,
}

struct Task {
    handle: JoinHandle<()>,
    callback: Arc<CallbackFrame>,
}

struct TaskDrain<'a> {
    owner: &'a Mutex<Vec<Task>>,
    tasks: Vec<Task>,
}

impl Drop for TaskDrain<'_> {
    fn drop(&mut self) {
        self.owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .append(&mut self.tasks);
    }
}

#[derive(Default)]
struct CallbackFrame {
    aborts: AtomicUsize,
    changed: Notify,
}

thread_local! {
    static CALLBACKS: RefCell<Vec<Arc<CallbackFrame>>> = const { RefCell::new(Vec::new()) };
}

/// Only external Invoke frames may outlive a reentrant synchronous Abort.
/// Their application futures have already been destroyed; their task handles
/// remain owned and are joined by the next non-reentrant drain.
pub(crate) struct ReentrantAbort(Vec<Arc<CallbackFrame>>);

impl ReentrantAbort {
    pub(crate) fn enter() -> Self {
        let frames = CALLBACKS.with(|frames| frames.borrow().clone());
        for frame in &frames {
            frame.aborts.fetch_add(1, Ordering::AcqRel);
            frame.changed.notify_waiters();
        }
        Self(frames)
    }
}

impl Drop for ReentrantAbort {
    fn drop(&mut self) {
        for frame in &self.0 {
            frame.aborts.fetch_sub(1, Ordering::AcqRel);
            frame.changed.notify_waiters();
        }
    }
}

impl CallbackFrame {
    fn invoke(self: &Arc<Self>, callback: impl FnOnce()) {
        struct Pop;
        impl Drop for Pop {
            fn drop(&mut self) {
                CALLBACKS.with(|frames| {
                    frames.borrow_mut().pop();
                });
            }
        }
        CALLBACKS.with(|frames| frames.borrow_mut().push(self.clone()));
        let _pop = Pop;
        callback();
    }
}

impl Workers {
    pub(crate) fn new(rt: Handle) -> Arc<Self> {
        Arc::new(Self {
            stop: Cancellation::default(),
            rt,
            handles: Mutex::new(Vec::new()),
            draining: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn spawn(&self, work: impl Future<Output = ()> + Send + 'static) -> Result<()> {
        self.spawn_inner(work, Arc::default())
    }

    fn spawn_inner(
        &self,
        work: impl Future<Output = ()> + Send + 'static,
        callback: Arc<CallbackFrame>,
    ) -> Result<()> {
        let mut handles = self.handles.lock().map_err(|_| com::E_UNEXPECTED)?;
        handles.retain(|task| !task.handle.is_finished());
        if self.stop.is_cancelled() || handles.len() >= 128 {
            return Err(E_ABORT.into());
        }
        handles.push(Task {
            handle: self.rt.spawn(work),
            callback,
        });
        Ok(())
    }

    pub(crate) fn can_release(&self) -> bool {
        let Ok(_draining) = self.draining.try_lock() else {
            return false;
        };
        self.stop.is_cancelled()
            && self
                .handles
                .lock()
                .is_ok_and(|tasks| tasks.iter().all(|task| task.handle.is_finished()))
    }

    pub(crate) async fn join(&self) -> Result<()> {
        self.stop.cancel();
        let _draining = self.draining.lock().await;
        let mut drain = TaskDrain {
            owner: &self.handles,
            tasks: std::mem::take(&mut *self.handles.lock().map_err(|_| com::E_UNEXPECTED)?),
        };
        let mut result = Ok(());
        let mut index = 0;
        while index < drain.tasks.len() {
            let callback = drain.tasks[index].callback.clone();
            loop {
                let changed = callback.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if callback.aborts.load(Ordering::Acquire) != 0 {
                    index += 1;
                    break;
                }
                tokio::select! {
                    outcome = &mut drain.tasks[index].handle => {
                        if outcome.is_err() {
                            result = Err(com::E_UNEXPECTED.into());
                        }
                        drain.tasks.swap_remove(index);
                        break;
                    }
                    _ = changed => {}
                }
            }
        }
        result
    }

    pub(crate) async fn join_fully(&self) -> Result<()> {
        let mut result = Ok(());
        loop {
            if let Err(error) = self.join().await {
                result = Err(error);
            }
            let frame = self
                .handles
                .lock()
                .map_err(|_| com::E_UNEXPECTED)?
                .iter()
                .find(|task| !task.handle.is_finished())
                .map(|task| task.callback.clone());
            let Some(frame) = frame else {
                if let Err(error) = self.join().await {
                    result = Err(error);
                }
                return result;
            };
            let changed = frame.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // Wait outside the drain lock: a concurrent callback's Abort must
            // finish its own drain before its external Invoke can return.
            if frame.aborts.load(Ordering::Acquire) != 0 {
                changed.await;
            }
        }
    }
}

struct Completion<T> {
    value: Mutex<Option<Result<T>>>,
    cancelled: Cancellation,
    callback: Option<IFabricAsyncOperationCallback>,
    completed: AtomicBool,
    abandoned: AtomicBool,
}

#[implement(IFabricAsyncOperationContext)]
struct Context<T: Send + 'static>(Arc<Completion<T>>);

impl<T: Send + 'static> IFabricAsyncOperationContext_Impl for Context_Impl<T> {
    fn IsCompleted(&self) -> bool {
        self.0.completed.load(Ordering::Acquire)
    }
    fn CompletedSynchronously(&self) -> bool {
        false
    }
    fn get_Callback(&self) -> Result<IFabricAsyncOperationCallback> {
        self.0.callback.clone().ok_or_else(|| com::E_POINTER.into())
    }
    fn Cancel(&self) -> Result<()> {
        self.0.cancelled.cancel();
        Ok(())
    }
}

struct Entry<T> {
    context: IFabricAsyncOperationContext,
    completion: Arc<Completion<T>>,
}

pub(crate) struct Slot<T>(Mutex<Option<Entry<T>>>);
impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self(Mutex::new(None))
    }
}

impl<T: Send + 'static> Slot<T> {
    pub(crate) fn begin<F, Fut>(
        &self,
        workers: &Workers,
        callback: Ref<IFabricAsyncOperationCallback>,
        work: F,
    ) -> Result<IFabricAsyncOperationContext>
    where
        F: FnOnce(Cancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.start(workers, callback, work, true, None)
    }

    pub(crate) fn begin_counted<F, Fut>(
        &self,
        workers: &Workers,
        callback: Ref<IFabricAsyncOperationCallback>,
        callbacks: Option<Arc<AtomicUsize>>,
        work: F,
    ) -> Result<IFabricAsyncOperationContext>
    where
        F: FnOnce(Cancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.start(workers, callback, work, true, callbacks)
    }

    pub(crate) fn begin_cleanup<F, Fut>(
        &self,
        workers: &Workers,
        callback: Ref<IFabricAsyncOperationCallback>,
        work: F,
    ) -> Result<IFabricAsyncOperationContext>
    where
        F: FnOnce(Cancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.start(workers, callback, work, false, None)
    }

    fn start<F, Fut>(
        &self,
        workers: &Workers,
        callback: Ref<IFabricAsyncOperationCallback>,
        work: F,
        interruptible: bool,
        callbacks: Option<Arc<AtomicUsize>>,
    ) -> Result<IFabricAsyncOperationContext>
    where
        F: FnOnce(Cancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let mut entry = self.0.lock().map_err(|_| com::E_UNEXPECTED)?;
        if entry.is_some() {
            return Err(com::E_INVALIDARG.into());
        }
        let completion = Arc::new(Completion {
            value: Mutex::new(None),
            cancelled: Cancellation::default(),
            callback: callback.as_ref().cloned(),
            completed: AtomicBool::new(false),
            abandoned: AtomicBool::new(false),
        });
        let context: IFabricAsyncOperationContext = Context(completion.clone()).into();
        let result_context = context.clone();
        let stop = workers.stop.clone();
        let output = completion.clone();
        let frame = Arc::<CallbackFrame>::default();
        let invocation = frame.clone();
        // Catch both construction and polling panics, without an unowned inner task.
        workers.spawn_inner(async move {
            let result = match catch_unwind(AssertUnwindSafe(|| work(output.cancelled.clone()))) {
                Err(_) => Err(com::E_UNEXPECTED.into()),
                Ok(future) => {
                    let mut future = Box::pin(future);
                    tokio::select! {
                        biased;
                        _ = stop.cancelled(), if interruptible => Err(E_ABORT.into()),
                        _ = output.cancelled.cancelled(), if interruptible => Err(E_ABORT.into()),
                        value = poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                            Ok(poll) => poll,
                            Err(_) => Poll::Ready(Err(com::E_UNEXPECTED.into())),
                        }) => value,
                    }
                }
            };
            if let Ok(mut value) = output.value.lock() {
                *value = Some(if output.abandoned.load(Ordering::Acquire) { Err(E_ABORT.into()) } else { result });
            }
            output.completed.store(true, Ordering::Release);
            if let Some(callback) = &output.callback {
                if let Some(count) = callbacks {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                // Common/ComAsyncOperationContext.cpp:159-174 at native pin
                // 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d invokes synchronously.
                // https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ComAsyncOperationContext.cpp#L159-L174
                // External Invoke may block/reenter. Hand off the multithread
                // worker core (including its non-stealable LIFO slot), without
                // detaching the callback from this bounded owned task/frame.
                tokio::task::block_in_place(|| {
                    invocation.invoke(|| unsafe { callback.Invoke(&result_context) });
                });
            }
        }, frame)?;
        *entry = Some(Entry {
            context: context.clone(),
            completion,
        });
        Ok(context)
    }

    pub(crate) fn end(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<T> {
        self.end_counted(context, None)
    }

    pub(crate) fn end_counted(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        ends: Option<&AtomicUsize>,
    ) -> Result<T> {
        self.end_with(context, || {
            if let Some(ends) = ends {
                ends.fetch_add(1, Ordering::Relaxed);
            }
        })
    }

    pub(crate) fn end_with(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        consumed: impl FnOnce(),
    ) -> Result<T> {
        let mut entry = self.0.lock().map_err(|_| com::E_UNEXPECTED)?;
        let current = entry.as_ref().ok_or(com::E_INVALIDARG)?;
        if !context
            .as_ref()
            .is_some_and(|c| c.as_raw() == current.context.as_raw())
        {
            return Err(com::E_INVALIDARG.into());
        }
        let result = current
            .completion
            .value
            .lock()
            .map_err(|_| com::E_UNEXPECTED)?
            .take();
        match result {
            Some(result) => {
                let finished = entry.take();
                drop(entry);
                drop(finished);
                consumed();
                result
            }
            None => Err(E_PENDING.into()),
        }
    }

    pub(crate) fn clear(&self) {
        let entry = self.0.lock().ok().and_then(|mut slot| slot.take());
        if let Some(entry) = entry {
            entry.completion.abandoned.store(true, Ordering::Release);
            entry.completion.cancelled.cancel();
            let value = entry
                .completion
                .value
                .lock()
                .ok()
                .and_then(|mut value| value.take());
            drop(value);
        }
    }
}

impl<T> Drop for Slot<T> {
    fn drop(&mut self) {
        if let Ok(entry) = self.0.get_mut() {
            if let Some(entry) = entry.take() {
                entry.completion.abandoned.store(true, Ordering::Release);
                entry.completion.cancelled.cancel();
                let value = entry
                    .completion
                    .value
                    .lock()
                    .ok()
                    .and_then(|mut value| value.take());
                drop(value);
            }
        }
    }
}

pub(crate) async fn guard_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let mut future = Box::pin(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(_) => Poll::Ready(Err(com::E_UNEXPECTED.into())),
        },
    )
    .await
}
