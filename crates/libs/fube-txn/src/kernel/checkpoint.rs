// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! SF 51a7703 CheckpointManager.cpp:250–452,1100–1142,1393–1552.
//! Physical records, flushing and rename are finite side-effect seams, not disk
//! recovery or durability. In particular native FlushAsync returns void;
//! only the begin record's flush status and rename have checked status branches.

use super::{
    processor::{FaultState, ProcessingRole},
    status::Status,
    transaction::own,
    version::Completion,
};
use crate::{internal::IStateProviderManager, ApiFuture, Cancellation, Result, SequenceNumber};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

pub(crate) type VoidFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhysicalRecord {
    EndCheckpoint,
    CompleteCheckpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FlushReason {
    EndCheckpoint,
    CompleteCheckpoint,
    CopyDrain,
    BeforeRename,
}

/// Established log/dispatcher observations, not a substitute log authority.
pub(crate) trait CheckpointIo: Send + Sync + 'static {
    fn dispatcher_fence(&self) -> VoidFuture<'_>;
    fn prepared(&self) {}
    fn applied(&self, _: Result<()>) {}
    fn processed(&self) {}
    fn begin_record_flush(&self) -> ApiFuture<'_, ()>;
    fn stability_permission(&self, checkpoint_lsn: SequenceNumber) -> VoidFuture<'_>;
    fn role(&self) -> ProcessingRole;
    fn tail(&self) -> SequenceNumber;
    fn append(&self, record: PhysicalRecord) -> Result<()>;
    /// Observe actual owned record/writer failure separately from native void
    /// FlushAsync completion. Opaque A0 fixtures default to no supplied error.
    fn status(&self) -> Result<()> {
        Ok(())
    }
    fn flush(&self, reason: FlushReason) -> VoidFuture<'_>;
    fn rename_copy_log(&self) -> ApiFuture<'_, ()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointPhase {
    Invalid,
    Ready,
    Applied,
    Completed,
    Aborted,
    Faulted,
}

pub(crate) struct CheckpointActivity {
    pub(crate) lsn: SequenceNumber,
    pub(crate) first_copy: bool,
    pub(crate) phase1: Completion<()>,
    pub(crate) finished: Completion<()>,
    phase: Mutex<CheckpointPhase>,
    drain_claimed: AtomicBool,
}

impl CheckpointActivity {
    pub(crate) fn abort_if_ready(&self) -> bool {
        let mut phase = self.phase.lock().unwrap();
        if matches!(*phase, CheckpointPhase::Invalid | CheckpointPhase::Ready) {
            *phase = CheckpointPhase::Aborted;
            true
        } else {
            false
        }
    }
    pub(crate) fn phase(&self) -> CheckpointPhase {
        *self.phase.lock().expect("checkpoint phase poisoned")
    }
    fn set_phase(&self, phase: CheckpointPhase) {
        *self.phase.lock().expect("checkpoint phase poisoned") = phase;
    }
}

pub(crate) struct CheckpointCoordinator {
    state_manager: Arc<dyn IStateProviderManager>,
    io: Arc<dyn CheckpointIo>,
    faults: Arc<FaultState>,
    api_lock: tokio::sync::Mutex<()>,
    consistency_lock: Arc<tokio::sync::Mutex<()>>,
    pending: Mutex<Option<Arc<CheckpointActivity>>>,
}

impl CheckpointCoordinator {
    pub(crate) fn new(
        state_manager: Arc<dyn IStateProviderManager>,
        io: Arc<dyn CheckpointIo>,
        faults: Arc<FaultState>,
    ) -> Arc<Self> {
        Self::with_consistency(
            state_manager,
            io,
            faults,
            Arc::new(tokio::sync::Mutex::new(())),
        )
    }
    pub(crate) fn with_consistency(
        state_manager: Arc<dyn IStateProviderManager>,
        io: Arc<dyn CheckpointIo>,
        faults: Arc<FaultState>,
        consistency_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state_manager,
            io,
            faults,
            api_lock: tokio::sync::Mutex::new(()),
            consistency_lock,
            pending: Mutex::new(None),
        })
    }

    /// Eager owned checkpoint; dropping its observations does not cancel callbacks.
    /// Locks protect only checkpoint APIs/backup consistency, never txn admission.
    pub(crate) fn start(
        self: &Arc<Self>,
        lsn: SequenceNumber,
        first_copy: bool,
    ) -> Result<Arc<CheckpointActivity>> {
        if lsn < 0 {
            return Err(Status::InvalidParameter.error());
        }
        if first_copy && self.io.role() == ProcessingRole::Primary {
            return Err(Status::InvalidOperation.error());
        }
        let activity = Arc::new(CheckpointActivity {
            lsn,
            first_copy,
            phase1: Completion::new(),
            finished: Completion::new(),
            phase: Mutex::new(CheckpointPhase::Invalid),
            drain_claimed: AtomicBool::new(false),
        });
        {
            let mut pending = self.pending.lock().expect("checkpoint pending poisoned");
            if pending.is_some() {
                return Err(Status::InvalidOperation.error());
            }
            *pending = Some(activity.clone());
        }
        let coordinator = self.clone();
        let work = activity.clone();
        drop(own(Box::pin(async move {
            let result = coordinator.prepare_perform(&work).await;
            work.phase1.complete(result.clone());
            if !work.first_copy {
                let result = match result {
                    Ok(()) => coordinator.complete(&work, false).await,
                    error => error,
                };
                coordinator.finish(&work, result);
            }
            Ok(())
        })));
        Ok(activity)
    }

    async fn prepare_perform(&self, activity: &CheckpointActivity) -> Result<()> {
        self.io.dispatcher_fence().await;
        // OperationProcessor::ProcessImmediately substitutes the service latch
        // only after successful BeginCheckpoint flush, after the dispatcher fence.
        let effective = match self.io.begin_record_flush().await {
            Ok(()) => self.faults.service_error().map_or(Ok(()), Err),
            failure => failure,
        };
        let prepare = match effective {
            Ok(()) => {
                let _api = self.api_lock.lock().await;
                self.state_manager.prepare_checkpoint(activity.lsn)
            }
            error => error,
        };
        self.io.prepared();
        if let Err(error) = prepare {
            self.io.applied(Err(error.clone()));
            activity.set_phase(CheckpointPhase::Faulted);
            self.faults.report_checkpoint_fault(error.clone());
            return Err(error);
        }
        {
            let mut phase = activity.phase.lock().unwrap();
            if *phase == CheckpointPhase::Invalid {
                *phase = CheckpointPhase::Ready;
            }
        }
        if activity.phase() == CheckpointPhase::Aborted {
            self.io.applied(Ok(()));
            return Err(Status::ObjectClosed.error());
        }
        self.io.stability_permission(activity.lsn).await;
        {
            let mut phase = activity.phase.lock().unwrap();
            if *phase == CheckpointPhase::Aborted {
                self.io.applied(Ok(()));
                return Err(Status::ObjectClosed.error());
            }
            if let Err(error) = self.io.status() {
                self.io.applied(Err(error.clone()));
                *phase = CheckpointPhase::Faulted;
                return Err(error);
            }
            *phase = CheckpointPhase::Applied;
            self.io.applied(Ok(()));
        }
        let result = {
            let _api = self.api_lock.lock().await;
            self.state_manager
                .perform_checkpoint(Cancellation::default())
                .await
        };
        match result {
            Ok(()) => {
                activity.set_phase(CheckpointPhase::Completed);
                Ok(())
            }
            Err(error) => {
                activity.set_phase(CheckpointPhase::Faulted);
                self.faults.report_checkpoint_fault(error.clone());
                Err(error)
            }
        }
    }

    async fn complete(&self, _: &CheckpointActivity, rename: bool) -> Result<()> {
        let _consistency = self.consistency_lock.lock().await;
        let result: Result<()> = async {
            self.io.status()?;
            self.io.append(PhysicalRecord::EndCheckpoint)?;
            self.io.flush(FlushReason::EndCheckpoint).await;
            self.io.status()?;
            if rename {
                self.io.flush(FlushReason::BeforeRename).await;
                self.io.rename_copy_log().await?;
            }
            {
                let _api = self.api_lock.lock().await;
                self.state_manager
                    .complete_checkpoint(Cancellation::default())
                    .await?;
            }
            self.io.append(PhysicalRecord::CompleteCheckpoint)?;
            self.io.flush(FlushReason::CompleteCheckpoint).await;
            self.io.status()?;
            Ok(())
        }
        .await;
        drop(_consistency);
        if let Err(error) = &result {
            // Native Complete errors report fault without changing Completed.
            self.faults.report_checkpoint_fault(error.clone());
        }
        result
    }

    fn claim(
        &self,
        activity: &Arc<CheckpointActivity>,
        upto: SequenceNumber,
        cancel: bool,
    ) -> Result<()> {
        let pending = self.pending.lock().expect("checkpoint pending poisoned");
        if !activity.first_copy
            || !pending
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, activity))
            || (!cancel && self.io.tail() != upto)
            || (cancel && self.io.tail() >= upto)
            || activity.drain_claimed.swap(true, Ordering::AcqRel)
        {
            return Err(Status::InvalidOperation.error());
        }
        Ok(())
    }

    pub(crate) fn finish_first_copy(
        self: &Arc<Self>,
        activity: Arc<CheckpointActivity>,
        copied_upto_lsn: SequenceNumber,
    ) -> ApiFuture<'static, ()> {
        if let Err(error) = self.claim(&activity, copied_upto_lsn, false) {
            return Box::pin(async move { Err(error) });
        }
        let coordinator = self.clone();
        own(Box::pin(async move {
            coordinator.io.flush(FlushReason::CopyDrain).await;
            let result = match activity.phase1.wait().await.as_ref().clone() {
                Ok(()) => coordinator.complete(&activity, true).await,
                Err(error) => {
                    coordinator.faults.report_checkpoint_fault(error.clone());
                    Err(error)
                }
            };
            coordinator.finish(&activity, result.clone());
            result
        }))
    }

    pub(crate) fn cancel_first_copy(
        self: &Arc<Self>,
        activity: Arc<CheckpointActivity>,
        copied_upto_lsn: SequenceNumber,
    ) -> ApiFuture<'static, ()> {
        if let Err(error) = self.claim(&activity, copied_upto_lsn, true) {
            return Box::pin(async move { Err(error) });
        }
        let coordinator = self.clone();
        own(Box::pin(async move {
            // Drain must wait even when Perform is currently suspended or failed.
            let _ = activity.phase1.wait().await;
            let error = Status::InvalidOperation.error();
            coordinator.faults.report_checkpoint_fault(error.clone());
            coordinator.finish(&activity, Err(error.clone()));
            Err(error)
        }))
    }

    pub(crate) async fn abort_first_copy(&self, activity: Arc<CheckpointActivity>) {
        let claimed = {
            let pending = self.pending.lock().expect("checkpoint pending poisoned");
            activity.first_copy
                && pending.as_ref().is_some_and(|a| Arc::ptr_eq(a, &activity))
                && !activity.drain_claimed.swap(true, Ordering::AcqRel)
        };
        if claimed {
            let _ = activity.phase1.wait().await;
            self.finish(&activity, Err(Status::ObjectClosed.error()));
        } else {
            let _ = activity.finished.wait().await;
        }
    }

    fn finish(&self, activity: &Arc<CheckpointActivity>, result: Result<()>) {
        let mut pending = self.pending.lock().expect("checkpoint pending poisoned");
        if pending
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, activity))
        {
            *pending = None;
        }
        self.io.processed();
        activity.finished.complete(result);
    }
}

/// Native ApiDispatcher eagerly starts every provider awaitable and then drains
/// all, retaining the first error in array order, not first completion order.
pub(crate) async fn await_all(work: Vec<ApiFuture<'static, ()>>) -> Result<()> {
    let work: Vec<_> = work.into_iter().map(own).collect();
    let mut first_error = None;
    for future in work {
        if let Err(error) = future.await {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}
