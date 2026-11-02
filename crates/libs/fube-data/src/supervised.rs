// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Enroll native-host provider ownership in the existing runtime process fence.
//! Accounting is not application authorization; engine/session/native fences
//! still decide whether callback results may be used.
//! Returned streams are Engine-owned results, not running callbacks. Only their
//! construction, individual `next` calls/captures, and final disposal enroll;
//! Runtime's outer participant guard still owns Close/Abort disposal.

use crate::extensions::{ReplayCheckpointProvider, ReplayOperation};
use crate::{Cancellation, Epoch, IOperationDataStream, IStateProvider, OperationData, Result};
use fube_core::hosting::{
    DisposalTracker, ExecutionRuntime, ManagedObject, ObjectQuota, ObjectReservation,
};
use fube_core::ApiFuture as ServiceFuture;
use std::sync::Arc;

#[cfg(test)]
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

// Historical logical guard-order/unwind probes are test-only. Real accepted
// callbacks and captures below use ExecutionRuntime, not this probe adapter.
#[cfg(test)]
mod accounting {
    use super::*;
    pub(super) trait Supervision: Clone + Send + Sync + 'static {
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

    pub(super) struct SupervisedProvider<S: Supervision> {
        pub(super) inner: Option<Arc<dyn IStateProvider>>,
        pub(super) replay: Option<Arc<dyn ReplayCheckpointProvider>>,
        pub(super) supervision: S,
    }
    impl<S: Supervision> SupervisedProvider<S> {
        fn inner(&self) -> &dyn IStateProvider {
            self.inner.as_deref().unwrap()
        }

        fn stream(&self, inner: Arc<dyn IOperationDataStream>) -> Arc<dyn IOperationDataStream> {
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
            drop(self.replay.take());
        }
    }

    impl<S: Supervision> IStateProvider for SupervisedProvider<S> {
        fn get_last_committed_sequence_number(&self) -> Result<i64> {
            let _guard = self.supervision.arm()?;
            self.inner().get_last_committed_sequence_number()
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

        fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
            let _guard = self.supervision.arm()?;
            Ok(self.stream(self.inner().get_copy_context()?))
        }

        fn get_copy_state(
            &self,
            up_to: i64,
            context: Option<Arc<dyn IOperationDataStream>>,
        ) -> Result<Arc<dyn IOperationDataStream>> {
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
            Ok(self.stream(self.inner().get_copy_state(up_to, context)?))
        }
    }
    impl<S: Supervision> ReplayCheckpointProvider for SupervisedProvider<S> {
        fn replication_checkpoint(
            &self,
            sequence: i64,
            cancellation: Cancellation,
        ) -> ServiceFuture<'_, Option<ReplayOperation>> {
            let guard = match self.supervision.arm() {
                Ok(guard) => guard,
                Err(error) => return Box::pin(async move { Err(error) }),
            };
            let Some(replay) = self.replay.as_ref() else {
                return Box::pin(async { Ok(None) });
            };
            let inner = replay.replication_checkpoint(sequence, cancellation);
            Box::pin(SupervisedFuture {
                inner,
                _guard: guard,
            })
        }
    }

    struct SupervisedStream<S: Supervision> {
        inner: Option<Arc<dyn IOperationDataStream>>,
        supervision: S,
    }
    impl<S: Supervision> IOperationDataStream for SupervisedStream<S> {
        fn get_next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
            let guard = match self.supervision.arm() {
                Ok(guard) => guard,
                Err(error) => return Box::pin(async move { Err(error) }),
            };
            let inner = self.inner.as_ref().unwrap().get_next(cancellation);
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
}

#[cfg(test)]
use accounting::*;

#[cfg(test)]
#[path = "supervised_tests.rs"]
mod tests;

/// Engine-owned provider and stream captures. Callback roots are scheduled by
/// the engine; these proxies reserve dormant disposal before publication.
pub(crate) struct OwnedProvider {
    // One ownership group covers both capabilities, including when both Arcs
    // point at the same application object. Final release is supervised once.
    inner: ManagedObject<ProviderCapabilities>,
    runtime: ExecutionRuntime,
    streams: ObjectQuota,
    tracker: DisposalTracker,
}

pub(crate) struct OwnedPartition(ManagedObject<Arc<dyn fube_core::IStatefulServicePartition>>);
impl OwnedPartition {
    pub(crate) fn new(
        runtime: &ExecutionRuntime,
        partition: Arc<dyn fube_core::IStatefulServicePartition>,
        tracker: &DisposalTracker,
        origin: fube_core::hosting::CallOrigin,
        construction: Option<&fube_core::hosting::OwnedCallPermit>,
    ) -> Result<Self> {
        let quota = ObjectQuota::new(1)?;
        let reservation = if let Some(construction) = construction {
            construction.object_reservation(&quota, tracker)?
        } else {
            runtime.reserve_construction_with_origin(&quota, tracker, origin)?
        };
        Ok(Self(reservation.construct(partition)))
    }
    pub(crate) fn publication(&self) -> fube_core::hosting::ObjectPublication {
        self.0.publication()
    }
    pub(crate) fn disposal_receipt(&self) -> fube_core::hosting::DisposalReceipt {
        self.0.receipt()
    }
    pub(crate) fn check_write_access(&self) -> Result<()> {
        use fube_core::{ErrorKind, ServicePartitionAccessStatus as Access};
        // Both the getter and release of its typed error run in the owning
        // finite callback, never in the owner/admission critical section.
        let access = match self.0.get().get_write_status() {
            Ok(access) => access,
            Err(error) => {
                drop(error);
                Access::NotPrimary
            }
        };
        let kind = match access {
            Access::Granted => return Ok(()),
            Access::ReconfigurationPending => ErrorKind::ReconfigurationPending,
            Access::NoWriteQuorum => ErrorKind::NoWriteQuorum,
            Access::NotPrimary | Access::Invalid => ErrorKind::NotPrimary,
        };
        Err(crate::Error::new(kind, "partition write access denied"))
    }
}
struct ProviderCapabilities {
    provider: Arc<dyn IStateProvider>,
    replay: Option<Arc<dyn ReplayCheckpointProvider>>,
}
fn provider_error(error: crate::Error) -> crate::Error {
    if matches!(
        error.kind(),
        fube_core::ErrorKind::Canceled | fube_core::ErrorKind::Timeout
    ) {
        return error;
    }
    crate::Error::with_cause(
        fube_core::ErrorKind::Provider,
        format!("provider callback failed: {error}"),
        error,
    )
}
impl OwnedProvider {
    pub(crate) fn new(
        runtime: ExecutionRuntime,
        provider: Arc<dyn IStateProvider>,
        replay: Option<Arc<dyn ReplayCheckpointProvider>>,
        streams: usize,
        tracker: DisposalTracker,
        origin: fube_core::hosting::CallOrigin,
        construction: Option<&fube_core::hosting::OwnedCallPermit>,
    ) -> Result<Self> {
        let quota = ObjectQuota::new(1)?;
        let reservation = if let Some(construction) = construction {
            construction.object_reservation(&quota, &tracker)?
        } else {
            runtime.reserve_construction_with_origin(&quota, &tracker, origin)?
        };
        Ok(Self {
            inner: reservation.construct(ProviderCapabilities { provider, replay }),
            runtime,
            streams: ObjectQuota::new(streams)?,
            tracker,
        })
    }
    pub(crate) fn publication(&self) -> fube_core::hosting::ObjectPublication {
        self.inner.publication()
    }
    pub(crate) fn disposal_receipt(&self) -> fube_core::hosting::DisposalReceipt {
        self.inner.receipt()
    }
    pub(crate) fn copy_state_constructed(
        &self,
        up_to: i64,
    ) -> Result<(
        Arc<dyn IOperationDataStream>,
        fube_core::hosting::ObjectPublication,
    )> {
        let reserved = self
            .runtime
            .reserve_construction(&self.streams, &self.tracker)?;
        let stream = self
            .inner
            .get()
            .provider
            .get_copy_state(up_to, None)
            .map_err(provider_error)?;
        let stream = reserved.construct(stream);
        let publication = stream.publication();
        Ok((Arc::new(OwnedStream(stream)), publication))
    }
    fn reserve_stream(&self) -> Result<ObjectReservation<Arc<dyn IOperationDataStream>>> {
        self.runtime.reserve_object(&self.streams, &self.tracker)
    }
    fn publish_stream(
        &self,
        reservation: ObjectReservation<Arc<dyn IOperationDataStream>>,
        stream: Arc<dyn IOperationDataStream>,
    ) -> Arc<dyn IOperationDataStream> {
        Arc::new(OwnedStream(reservation.publish(stream)))
    }
}
impl OwnedProvider {
    pub(crate) fn has_replay(&self) -> bool {
        self.inner.get().replay.is_some()
    }
    pub(crate) fn replication_checkpoint(
        &self,
        n: i64,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, Option<ReplayOperation>> {
        Box::pin(async move {
            match self.inner.get().replay.as_ref() {
                Some(replay) => replay
                    .replication_checkpoint(n, cancel)
                    .await
                    .map_err(provider_error),
                None => Ok(None),
            }
        })
    }
}
impl IStateProvider for OwnedProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.inner
            .get()
            .provider
            .get_last_committed_sequence_number()
            .map_err(provider_error)
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.inner
                .get()
                .provider
                .update_epoch(epoch, previous, cancel)
                .await
                .map_err(provider_error)
        })
    }
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            self.inner
                .get()
                .provider
                .on_data_loss(cancel)
                .await
                .map_err(provider_error)
        })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        let reserved = self.reserve_stream()?;
        Ok(self.publish_stream(
            reserved,
            self.inner
                .get()
                .provider
                .get_copy_context()
                .map_err(provider_error)?,
        ))
    }
    fn get_copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        let reserved = self.reserve_stream()?;
        Ok(self.publish_stream(
            reserved,
            self.inner
                .get()
                .provider
                .get_copy_state(up_to, context)
                .map_err(provider_error)?,
        ))
    }
}
struct OwnedStream(ManagedObject<Arc<dyn IOperationDataStream>>);
impl IOperationDataStream for OwnedStream {
    fn get_next(&self, cancel: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move { self.0.get().get_next(cancel).await.map_err(provider_error) })
    }
}
