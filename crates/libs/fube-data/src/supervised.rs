// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Enroll native-host provider ownership in the existing runtime process fence.
//! Accounting is not application authorization; engine/session/native fences
//! still decide whether callback results may be used.
//! Returned streams are Engine-owned results, not running callbacks. Only their
//! construction, individual `next` calls/captures, and final disposal enroll;
//! Runtime's outer participant guard still owns Close/Abort disposal.

use crate::{
    Cancellation, Epoch, OperationData, OperationDataStream, ReplayOperation, Result, StateProvider,
};
use fube_runtime::{ServiceFuture, StatefulCallbackGuard, StatefulPartition};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

// Kept private so tests can exercise ownership/unwind ordering without a
// native node, a runtime dependency cycle, or manufacturing partition authority.
pub(crate) trait Supervision: Clone + Send + Sync + 'static {
    type Guard: Send + Sync + Unpin;
    fn arm(&self) -> Result<Self::Guard>;

    fn cleanup(&self) -> Option<Self::Guard> {
        loop {
            match self.arm() {
                Ok(guard) => return Some(guard),
                Err(crate::Error::ResourceLimit(_)) => {
                    // Rejected callback admission never invokes user code.
                    // Final capture disposal cannot simply run unaccounted:
                    // wait for one of the existing finite owners to release a
                    // slot, or for its unchanged OS deadline to fence the host.
                    std::thread::sleep(Duration::from_millis(1));
                }
                // Disarming is terminal and forbids new enrollment. Native
                // ownership is joined before Runtime disarms; its enclosing
                // callback/final-release guard owns this failure path. After a
                // failed disarm, the existing watchdog is already terminal.
                Err(_) => return None,
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct PartitionSupervision {
    partition: StatefulPartition,
    timeout: Duration,
}
impl Supervision for PartitionSupervision {
    type Guard = StatefulCallbackGuard;
    fn arm(&self) -> Result<Self::Guard> {
        self.partition.supervise_owned_callback(self.timeout)
    }
}

pub(crate) struct SupervisedProvider<S: Supervision = PartitionSupervision> {
    inner: Option<Arc<dyn StateProvider>>,
    supervision: S,
}
impl SupervisedProvider {
    // Engine validates settings before constructing this owned adapter.
    pub(crate) fn new(
        partition: StatefulPartition,
        timeout: Duration,
        inner: Arc<dyn StateProvider>,
    ) -> Self {
        Self {
            inner: Some(inner),
            supervision: PartitionSupervision { partition, timeout },
        }
    }
}
impl<S: Supervision> SupervisedProvider<S> {
    fn inner(&self) -> &dyn StateProvider {
        self.inner.as_deref().unwrap()
    }

    fn stream(&self, inner: Arc<dyn OperationDataStream>) -> Arc<dyn OperationDataStream> {
        Arc::new(SupervisedStream {
            inner: Some(inner),
            supervision: self.supervision.clone(),
        })
    }
}
impl<S: Supervision> Drop for SupervisedProvider<S> {
    fn drop(&mut self) {
        let _guard = self.supervision.cleanup();
        // Take explicitly: automatic field drop after this body would release
        // the cleanup guard before the final application Arc destructor.
        drop(self.inner.take());
    }
}

impl<S: Supervision> StateProvider for SupervisedProvider<S> {
    fn last_committed(&self) -> Result<i64> {
        let _guard = self.supervision.arm()?;
        self.inner().last_committed()
    }

    fn replication_checkpoint(
        &self,
        sequence: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Option<ReplayOperation>> {
        let guard = match self.supervision.arm() {
            Ok(guard) => guard,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let inner = self.inner().replication_checkpoint(sequence, cancellation);
        Box::pin(SupervisedFuture {
            inner,
            _guard: guard,
        })
    }

    fn update_epoch(
        &self,
        epoch: Epoch,
        previous_epoch_last: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        let guard = match self.supervision.arm() {
            Ok(guard) => guard,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        // Enrollment precedes even synchronous user future construction.
        let inner = self
            .inner()
            .update_epoch(epoch, previous_epoch_last, cancellation);
        Box::pin(SupervisedFuture {
            inner,
            _guard: guard,
        })
    }

    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        let guard = match self.supervision.arm() {
            Ok(guard) => guard,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let inner = self.inner().on_data_loss(cancellation);
        Box::pin(SupervisedFuture {
            inner,
            _guard: guard,
        })
    }

    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>> {
        let _guard = self.supervision.arm()?;
        Ok(self.stream(self.inner().copy_context()?))
    }

    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>> {
        let _guard = match self.supervision.arm() {
            Ok(guard) => guard,
            Err(error) => {
                if context.is_some() {
                    let _cleanup = self.supervision.cleanup();
                    drop(context);
                }
                return Err(error);
            }
        };
        Ok(self.stream(self.inner().copy_state(up_to, context)?))
    }
}

struct SupervisedStream<S: Supervision> {
    inner: Option<Arc<dyn OperationDataStream>>,
    supervision: S,
}
impl<S: Supervision> OperationDataStream for SupervisedStream<S> {
    fn next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        let guard = match self.supervision.arm() {
            Ok(guard) => guard,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let inner = self.inner.as_ref().unwrap().next(cancellation);
        Box::pin(SupervisedFuture {
            inner,
            _guard: guard,
        })
    }
}
impl<S: Supervision> Drop for SupervisedStream<S> {
    fn drop(&mut self) {
        let _guard = self.supervision.cleanup();
        // As with the provider, release the final application Arc before the
        // cleanup guard, including destructor unwind.
        drop(self.inner.take());
    }
}

struct SupervisedFuture<'a, T, G> {
    // Rust drops fields in declaration order, including unpolled/canceled
    // futures and unwind. Never release the guard on Poll::Ready alone.
    inner: ServiceFuture<'a, T>,
    _guard: G,
}
impl<T, G: Unpin> Future for SupervisedFuture<'_, T, G> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
#[path = "supervised_tests.rs"]
mod tests;
