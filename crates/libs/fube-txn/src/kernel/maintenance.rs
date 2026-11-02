// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native LogTruncationManager/CheckpointManager runtime policy and physical
//! ownership. Ordinary allocation only; no guaranteed hard-limit progress.
use super::{
    checkpoint::{
        CheckpointActivity, CheckpointCoordinator, CheckpointIo, FlushReason, PhysicalRecord,
        VoidFuture,
    },
    map::RecordNode,
    processor::{Activity, KernelManager, ProcessingRole},
    record::{Record, RecordKind},
    runtime::VolatileRuntime,
    status::Status,
    transaction::own,
    version::Completion,
};
use crate::{ApiFuture, Cancellation, Error, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
#[derive(Default)]
struct Live {
    count: AtomicUsize,
    changed: tokio::sync::Notify,
}
struct Work(Arc<Live>);
impl Drop for Work {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
#[derive(Clone)]
pub(crate) struct LogSettings {
    pub(crate) checkpoint_mb: u64,
    pub(crate) min_log_mb: u64,
    pub(crate) truncate_factor: u64,
    pub(crate) throttle_factor: u64,
}
impl Default for LogSettings {
    fn default() -> Self {
        Self {
            checkpoint_mb: 50,
            min_log_mb: 50,
            truncate_factor: 2,
            throttle_factor: 4,
        }
    }
}
impl LogSettings {
    pub(crate) fn index_interval(&self) -> u64 {
        self.checkpoint() / 50
    }
    fn checkpoint(&self) -> u64 {
        self.checkpoint_mb * 1024 * 1024
    }
    fn min_log(&self) -> u64 {
        self.min_log_mb * 1024 * 1024
    }
    fn throttle(&self) -> u64 {
        self.checkpoint().max(self.min_log()) * self.throttle_factor
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.checkpoint_mb == 0
            || self.min_log_mb == 0
            || self.checkpoint_mb > 1024
            || self.min_log_mb > 1024
            || self.truncate_factor < 2
            || self.throttle_factor <= self.truncate_factor
            || self
                .throttle_factor
                .checked_mul(self.checkpoint().max(self.min_log()))
                .is_none()
            || self.truncate_factor.checked_mul(self.min_log()).is_none()
        {
            return Err(Status::InvalidParameter.error());
        }
        Ok(())
    }
}
pub(crate) struct Truncation {
    phase: Mutex<TruncationPhase>,
    pub(crate) lsn: i64,
    pub(crate) position: u64,
    pub(crate) ready: Completion<()>,
    pub(crate) applied: Completion<()>,
    pub(crate) done: Completion<()>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum TruncationPhase {
    Invalid,
    Ready,
    Applied,
    Completed,
    Faulted,
    Aborted,
}
impl Truncation {
    fn abort_if_ready(&self) {
        let mut phase = self.phase.lock().unwrap();
        if matches!(*phase, TruncationPhase::Invalid | TruncationPhase::Ready) {
            *phase = TruncationPhase::Aborted;
        }
    }
}
struct Pending {
    checkpoint: Option<(Arc<CheckpointCoordinator>, Arc<CheckpointActivity>)>,
    truncate: Option<Arc<Truncation>>,
}
pub(crate) struct Maintenance {
    pub(crate) consistency: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    pub(crate) copy_selection_entered: Completion<()>,
    #[cfg(test)]
    pub(crate) secondary_blocked: Completion<()>,
    live: Arc<Live>,
    weak: Weak<Self>,
    runtime: Weak<VolatileRuntime>,
    settings: LogSettings,
    pending: Mutex<Pending>,
    cancel: Mutex<Cancellation>,
    #[cfg(test)]
    pub(crate) flush_gate: Mutex<Option<Completion<()>>>,
}
struct Io {
    lifecycle_applied: std::sync::atomic::AtomicBool,
    runtime: Weak<VolatileRuntime>,
    begin: Arc<Activity>,
    cancel: Cancellation,
    error: Mutex<Option<Error>>,
}
impl Io {
    fn runtime(&self) -> Arc<VolatileRuntime> {
        self.runtime.upgrade().unwrap()
    }
    fn status_value(&self) -> Result<()> {
        if self.cancel.is_cancelled() && !self.lifecycle_applied.load(Ordering::Acquire) {
            return Err(Status::ObjectClosed.error());
        }
        self.error
            .lock()
            .unwrap()
            .clone()
            .or_else(|| self.runtime().owner.manager.faults.log_error())
            .or_else(|| self.runtime().owner.manager.faults.service_error())
            .or_else(|| {
                self.runtime()
                    .owner
                    .manager
                    .memory()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .writer_error()
            })
            .map_or(Ok(()), Err)
    }
}
impl CheckpointIo for Io {
    fn dispatcher_fence(&self) -> VoidFuture<'_> {
        Box::pin(async move {
            let _ = self.begin.checkpoint_entered.wait().await;
        })
    }
    fn prepared(&self) {
        if let Some(fence) = self.begin.checkpoint_prepared.lock().unwrap().as_ref() {
            fence.complete(Ok(()));
        }
    }
    fn applied(&self, result: Result<()>) {
        if result.is_ok() {
            self.lifecycle_applied.store(true, Ordering::Release);
        }
        self.begin.apply.complete(result.clone());
        self.begin
            .result
            .complete(result.map(|_| self.begin.node.lsn()));
    }
    fn processed(&self) {
        self.begin.processing.complete(Ok(()));
    }
    fn begin_record_flush(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.begin.flush.wait().await.as_ref().clone() })
    }
    fn stability_permission(&self, lsn: i64) -> VoidFuture<'_> {
        Box::pin(async move {
            let runtime = self.runtime();
            let manager = &runtime.owner.manager;
            tokio::select! {
                _ = manager.stable_permission(lsn) => {},
                _ = self.cancel.cancelled() => {},
            }
            if !self.cancel.is_cancelled() && manager.stable() >= lsn {
                if let Some(checkpoint) = manager.memory().unwrap().lock().unwrap().pending.as_mut()
                {
                    checkpoint.stable = manager.stable();
                }
            }
        })
    }
    fn role(&self) -> ProcessingRole {
        self.runtime().owner.manager.role()
    }
    fn tail(&self) -> i64 {
        self.runtime().owner.manager.tail()
    }
    fn append(&self, record: PhysicalRecord) -> Result<()> {
        let runtime = self.runtime();
        let memory = runtime.owner.manager.memory().unwrap();
        let mut memory = memory.lock().unwrap();
        let result = match record {
            PhysicalRecord::EndCheckpoint => memory.end_checkpoint(),
            PhysicalRecord::CompleteCheckpoint => memory.complete_checkpoint(),
        };
        if let Err(error) = &result {
            self.error.lock().unwrap().get_or_insert(error.clone());
        }
        result
    }
    fn status(&self) -> Result<()> {
        self.status_value()
    }
    fn flush(&self, reason: FlushReason) -> VoidFuture<'_> {
        Box::pin(async move {
            let runtime = self.runtime();
            let status = self.status_value();
            if let Some(memory) = runtime.owner.manager.memory() {
                let mut memory = memory.lock().unwrap();
                memory.flush();
                if reason == FlushReason::EndCheckpoint
                    && status.is_ok()
                    && memory.writer_error().is_none()
                {
                    memory.publish_ended_checkpoint();
                }
            }
        })
    }
    fn rename_copy_log(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.status_value() })
    }
}
impl Maintenance {
    pub(crate) fn new(runtime: Weak<VolatileRuntime>, settings: LogSettings) -> Result<Arc<Self>> {
        settings.validate()?;
        Ok(Arc::new_cyclic(|weak| Self {
            consistency: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            copy_selection_entered: Completion::new(),
            #[cfg(test)]
            secondary_blocked: Completion::new(),
            live: Arc::new(Live::default()),
            weak: weak.clone(),
            runtime,
            settings,
            pending: Mutex::new(Pending {
                checkpoint: None,
                truncate: None,
            }),
            cancel: Mutex::new(Cancellation::default()),
            #[cfg(test)]
            flush_gate: Mutex::new(None),
        }))
    }
    fn runtime(&self) -> Arc<VolatileRuntime> {
        self.runtime.upgrade().unwrap()
    }
    fn manager(&self) -> Arc<KernelManager> {
        self.runtime().owner.manager.clone()
    }
    fn work(&self) -> Work {
        self.live.count.fetch_add(1, Ordering::AcqRel);
        Work(self.live.clone())
    }
    pub(crate) fn resume(&self) {
        *self.cancel.lock().unwrap() = Cancellation::default();
    }
    #[cfg(test)]
    pub(crate) fn pending_truncation(&self) -> Option<Arc<Truncation>> {
        self.pending.lock().unwrap().truncate.clone()
    }
    #[cfg(test)]
    pub(crate) fn pending_checkpoint(&self) -> Option<Arc<CheckpointActivity>> {
        self.pending
            .lock()
            .unwrap()
            .checkpoint
            .as_ref()
            .map(|(_, a)| a.clone())
    }
    pub(crate) async fn stop(&self) {
        let (cp, trunc) = {
            let pending = self.pending.lock().unwrap();
            (pending.checkpoint.clone(), pending.truncate.clone())
        };
        if let Some((_, cp)) = &cp {
            cp.abort_if_ready();
        }
        if let Some(trunc) = &trunc {
            trunc.abort_if_ready();
        }
        self.cancel.lock().unwrap().cancel();
        if let Some((coordinator, cp)) = cp {
            let _ = cp.phase1.wait().await;
            if cp.first_copy {
                coordinator.abort_first_copy(cp).await;
                self.pending.lock().unwrap().checkpoint = None;
                self.manager()
                    .memory()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .abort_checkpoint();
            } else {
                let _ = cp.finished.wait().await;
            }
        }
        if let Some(trunc) = trunc {
            let _ = trunc.done.wait().await;
        }
        loop {
            let changed = self.live.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.live.count.load(Ordering::Acquire) == 0 {
                break;
            }
            changed.await;
        }
    }
    pub(crate) fn start_checkpoint(
        &self,
        first_copy: bool,
    ) -> Result<Option<Arc<CheckpointActivity>>> {
        let result = self
            .manager()
            .runtime_checkpoint(|lsn| self.begin_checkpoint_at(lsn, first_copy))?;
        if result.is_some() && !first_copy {
            self.runtime().request_group_commit();
        }
        Ok(result)
    }
    fn begin_checkpoint_at(
        &self,
        lsn: i64,
        first_copy: bool,
    ) -> Result<Option<(Arc<Activity>, Arc<CheckpointActivity>)>> {
        if self.cancel.lock().unwrap().is_cancelled() {
            return Err(Status::ObjectClosed.error());
        }
        let manager = self.manager();
        if manager.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        let mut pending = self.pending.lock().unwrap();
        if pending
            .checkpoint
            .as_ref()
            .is_some_and(|(_, a)| a.finished.is_completed())
        {
            pending.checkpoint = None;
        }
        if pending.checkpoint.is_some() {
            return Err(Status::InvalidOperation.error());
        }
        let earliest = manager.map.earliest_pending(lsn);
        if earliest.failed_barrier_check {
            if manager.role() == ProcessingRole::Primary {
                return Ok(None);
            }
            return Err(Status::InvalidOperation.error());
        }
        let descriptor = manager
            .memory()
            .unwrap()
            .lock()
            .unwrap()
            .begin_checkpoint(lsn, earliest.record.as_ref())?;
        let node = RecordNode::new(
            Record {
                lsn,
                kind: RecordKind::Physical {
                    native_type: 7,
                    event: 0,
                },
            },
            None,
            None,
        )?;
        let begin = Activity::new(node, manager.role() == ProcessingRole::Primary);
        begin
            .physical_position
            .store(descriptor.begin_position, Ordering::Release);
        *begin.checkpoint_prepared.lock().unwrap() = Some(Completion::new());
        let token = self.cancel.lock().unwrap().clone();
        let coordinator = CheckpointCoordinator::with_consistency(
            manager.directory.clone(),
            Arc::new(Io {
                lifecycle_applied: std::sync::atomic::AtomicBool::new(false),
                runtime: self.runtime.clone(),
                begin: begin.clone(),
                cancel: token.clone(),
                error: Mutex::new(None),
            }),
            manager.faults.clone(),
            self.consistency.clone(),
        );
        let activity = coordinator.start(lsn, first_copy)?;
        let guard = (!first_copy).then(|| self.work());
        pending.checkpoint = Some((coordinator, activity.clone()));
        drop(pending);
        if !first_copy {
            let owner = self.weak.upgrade().unwrap();
            let work = activity.clone();
            let guard = guard.unwrap();
            drop(own(Box::pin(async move {
                let _guard = guard;
                let result = work.finished.wait().await.as_ref().clone();
                let retired = {
                    let mut pending = owner.pending.lock().unwrap();
                    let current = pending
                        .checkpoint
                        .as_ref()
                        .is_some_and(|(_, a)| Arc::ptr_eq(a, &work));
                    if current {
                        pending.checkpoint = None;
                    }
                    current
                };
                if result.is_err() && retired {
                    owner
                        .manager()
                        .memory()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .abort_checkpoint();
                }
                Ok(())
            })));
        }
        Ok(Some((begin, activity)))
    }
    pub(crate) fn finish_copy(&self, upto: i64) -> ApiFuture<'static, ()> {
        let guard = self.work();
        let pair = self.pending.lock().unwrap().checkpoint.clone();
        let owner = self.weak.upgrade().unwrap();
        own(Box::pin(async move {
            let _guard = guard;
            let (coordinator, activity) = pair.ok_or_else(|| Status::InvalidOperation.error())?;
            let result = if owner.manager().tail() == upto {
                coordinator.finish_first_copy(activity.clone(), upto).await
            } else {
                coordinator.cancel_first_copy(activity.clone(), upto).await
            };
            let retired = {
                let mut pending = owner.pending.lock().unwrap();
                let current = pending
                    .checkpoint
                    .as_ref()
                    .is_some_and(|(_, a)| Arc::ptr_eq(a, &activity));
                if current {
                    pending.checkpoint = None;
                }
                current
            };
            if result.is_err() && retired {
                owner
                    .manager()
                    .memory()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .abort_checkpoint();
            }
            result
        }))
    }
    pub(crate) async fn settle_failed_copy(&self, upto: i64) {
        let pair = self.pending.lock().unwrap().checkpoint.clone();
        let Some((coordinator, activity)) = pair.filter(|(_, a)| a.first_copy) else {
            return;
        };
        if self.manager().tail() < upto {
            let _ = self.finish_copy(upto).await;
            return;
        }
        coordinator.abort_first_copy(activity.clone()).await;
        let retired = {
            let mut pending = self.pending.lock().unwrap();
            let current = pending
                .checkpoint
                .as_ref()
                .is_some_and(|(_, a)| Arc::ptr_eq(a, &activity));
            if current {
                pending.checkpoint = None;
            }
            current
        };
        if retired {
            self.manager()
                .memory()
                .unwrap()
                .lock()
                .unwrap()
                .abort_checkpoint();
        }
    }
    fn old_transactions(&self) -> Vec<i64> {
        let manager = self.manager();
        let memory = manager.memory().unwrap();
        let memory = memory.lock().unwrap();
        let tail = memory.tail_position;
        let age = self.settings.checkpoint() / 2;
        manager
            .map
            .pending()
            .iter()
            .filter(|node| {
                memory
                    .position_of(node)
                    .is_some_and(|pos| tail > age && pos < tail - age)
            })
            .filter_map(|n| n.transaction_id())
            .collect()
    }
    pub(crate) fn throttle(&self) -> Result<()> {
        let manager = self.manager();
        let memory = manager.memory().unwrap();
        let memory = memory.lock().unwrap();
        let used = memory.tail_position - memory.head_identity().0;
        drop(memory);
        if used > self.settings.throttle() {
            let pending = self.pending.lock().unwrap();
            let blocked = pending.checkpoint.is_some() || pending.truncate.is_some();
            drop(pending);
            if blocked {
                return Err(Status::ServiceTooBusy.error());
            }
            manager.abort_old(&self.old_transactions());
        }
        Ok(())
    }
    pub(crate) fn after_record(&self, record: &super::record::Record) -> Result<bool> {
        if self.cancel.lock().unwrap().is_cancelled() {
            return Ok(false);
        }
        if matches!(record.kind, super::record::RecordKind::UpdateEpoch { .. }) {
            return Ok(false);
        }
        let manager = self.manager();
        let barrier = matches!(record.kind, super::record::RecordKind::Barrier { .. });
        let mut requested = false;
        if !barrier || manager.role() != ProcessingRole::Primary {
            manager.index_if_needed(self.settings.checkpoint() / 50)?;
            requested |= self.check_truncation()?;
        }
        Ok(requested)
    }
    pub(crate) fn checkpoint_on_barrier(&self, lsn: i64) -> Result<Option<Arc<Activity>>> {
        if self.cancel.lock().unwrap().is_cancelled() {
            return Ok(None);
        }
        if self.pending.lock().unwrap().checkpoint.is_some() {
            return Ok(None);
        }
        let manager = self.manager();
        let Some(memory) = manager.memory() else {
            return Ok(None);
        };
        let requested = {
            let memory = memory.lock().unwrap();
            memory.completed.as_ref().is_some_and(|cp| {
                memory.tail_position - cp.begin_position > self.settings.checkpoint()
            })
        };
        if !requested {
            return Ok(None);
        }
        let old = self.old_transactions();
        if !old.is_empty() {
            if manager.role() == ProcessingRole::Primary {
                manager.abort_old(&old);
            }
            return Ok(None);
        }
        self.begin_checkpoint_at(lsn, false)
            .map(|pair| pair.map(|(begin, _)| begin))
    }
    pub(crate) fn check_truncation(&self) -> Result<bool> {
        let mut pending = self.pending.lock().unwrap();
        if pending.truncate.is_some() || self.cancel.lock().unwrap().is_cancelled() {
            return Ok(false);
        }
        let manager = self.manager();
        let memory = manager.memory().unwrap();
        let candidate = {
            let memory = memory.lock().unwrap();
            let Some(cp) = &memory.completed else {
                return Ok(false);
            };
            let head = memory.head_identity().0;
            let tail = memory.tail_position;
            if tail - head < self.settings.min_log() * self.settings.truncate_factor {
                return Ok(false);
            }
            memory
                .candidates()
                .into_iter()
                .rev()
                .find(|(p, _)| {
                    *p < cp.reader_position
                        && p.saturating_sub(head) >= self.settings.checkpoint() / 2
                        && tail - *p >= self.settings.min_log()
                })
                .map(|(p, _)| p)
        };
        let Some(position) = candidate else {
            return Ok(false);
        };
        let lsn = memory.lock().unwrap().select_runtime_head(position)?;
        let trunc = Arc::new(Truncation {
            phase: Mutex::new(TruncationPhase::Invalid),
            lsn,
            position,
            ready: Completion::new(),
            applied: Completion::new(),
            done: Completion::new(),
        });
        pending.truncate = Some(trunc.clone());
        drop(pending);
        let owner = self.weak.upgrade().unwrap();
        let cancel = self.cancel.lock().unwrap().clone();
        let guard = self.work();
        drop(own(Box::pin(async move {
            let _guard = guard;
            #[cfg(test)]
            {
                let gate = owner.flush_gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    let _ = gate.wait().await;
                }
            }
            let result: Result<()> = async {
                let manager = owner.manager();
                if cancel.is_cancelled() {
                    return Err(Status::ObjectClosed.error());
                }
                if let Some(error) = manager
                    .faults
                    .log_error()
                    .or_else(|| manager.faults.service_error())
                {
                    return Err(error);
                }
                manager.memory().unwrap().lock().unwrap().flush();
                {
                    let mut phase = trunc.phase.lock().unwrap();
                    if *phase == TruncationPhase::Aborted {
                        return Err(Status::ObjectClosed.error());
                    }
                    *phase = TruncationPhase::Ready;
                }
                trunc.ready.complete(Ok(()));
                tokio::select! {
                    _ = manager.stable_permission(trunc.lsn) => {},
                    _ = cancel.cancelled() => return Err(Status::ObjectClosed.error()),
                }
                {
                    let mut phase = trunc.phase.lock().unwrap();
                    if *phase == TruncationPhase::Aborted || manager.faults.is_closing() {
                        return Err(Status::ObjectClosed.error());
                    }
                    *phase = TruncationPhase::Applied;
                    trunc.applied.complete(Ok(()));
                }
                loop {
                    if manager.physical_truncate(position)? {
                        break;
                    }
                    let changed = manager.memory().unwrap().lock().unwrap().changed.clone();
                    let wait = changed.notified();
                    tokio::pin!(wait);
                    wait.as_mut().enable();
                    if manager.physical_truncate(position)? {
                        break;
                    }
                    wait.await;
                }
                Ok(())
            }
            .await;
            if let Err(error) = &result {
                trunc.ready.complete(Err(error.clone()));
                trunc.applied.complete(Err(error.clone()));
            }
            {
                let mut phase = trunc.phase.lock().unwrap();
                if result.is_ok() {
                    *phase = TruncationPhase::Completed;
                } else if *phase != TruncationPhase::Aborted {
                    *phase = TruncationPhase::Faulted;
                }
            }
            trunc.done.complete(result.clone());
            owner.pending.lock().unwrap().truncate = None;
            if let Err(error) = result {
                if !cancel.is_cancelled() {
                    owner.manager().faults.report_checkpoint_fault(error);
                }
            }
            Ok(())
        })));
        self.runtime().request_group_commit();
        Ok(true)
    }
    pub(crate) fn safe_lsn(&self) -> i64 {
        let manager = self.manager();
        let memory = manager.memory().unwrap();
        let memory = memory.lock().unwrap();
        memory.safe_provider_lsn()
    }
    pub(crate) async fn block_secondary(&self, copy: bool) {
        if copy {
            return;
        }
        let manager = self.manager();
        {
            let memory = manager.memory().unwrap();
            let memory = memory.lock().unwrap();
            if memory.completed.is_none()
                || memory.tail_position - memory.head_identity().0 <= self.settings.throttle()
            {
                return;
            }
        }
        let (checkpoint, trunc) = {
            let pending = self.pending.lock().unwrap();
            let checkpoint = pending.checkpoint.as_ref().map(|(_, a)| a.clone());
            let trunc = if checkpoint.is_none() {
                pending.truncate.clone()
            } else {
                None
            };
            (checkpoint, trunc)
        };
        if let Some(cp) = checkpoint {
            if cp.lsn > manager.stable() {
                return;
            }
            #[cfg(test)]
            self.secondary_blocked.complete(Ok(()));
            let _ = cp.finished.wait().await;
        }
        if let Some(trunc) = trunc {
            if trunc.lsn > manager.stable() {
                return;
            }
            #[cfg(test)]
            self.secondary_blocked.complete(Ok(()));
            let _ = trunc.done.wait().await;
        }
    }
}
