// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Admitted-record processing from SF 51a7703 TransactionManager.cpp:236–527,
//! ReplicatedLogManager.cpp:638–845,915–978 and OperationProcessor.cpp:366–1027.
//! ParallelLogRecordsDispatcher.cpp:111–320 retains successful flush prefixes
//! until a boundary, publishes its Apply fence before the preceding parallel
//! transaction group, then identifies the boundary after group processing.
//! RoleContextDrainState.cpp:54–68 closes lower writes before one-shot reporting.
//! Lower sequence assignment/completions and an established role are explicit
//! offline seams, not a transport, persistence or role-authority implementation.

use super::{
    map::{RecordNode, TransactionMap},
    provider::ProviderDirectory,
    record::{Record, RecordKind},
    runtime_lower::RuntimeLower,
    status::{self, Status},
    transaction::{own, KernelTransaction, RetryRuntime},
    version::{Completion, VersionManager},
};
use crate::{
    internal::ITransactionStateManager, ApiFuture, ApplyContext, AtomicOperation,
    AtomicRedoOperation, Epoch, Error, ITransactionManager, ITransactionVersionManager,
    IVersionManager, OperationContext, OperationData, ReplicaRole, Result, SequenceNumber,
    StateProviderId, Transaction, TransactionBase, TransactionId, TryRemoveVersionResult,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessingRole {
    Primary,
    Secondary,
    Recovery,
}
impl KernelManager {
    pub(crate) fn set_maintenance_hooks(
        &self,
        admitted: AdmissionHook,
        throttle: WriteAccessCheck,
    ) {
        *self.admission_hook.lock().unwrap() = Some(admitted);
        *self.throttle_hook.lock().unwrap() = Some(throttle);
    }
    pub(crate) fn on_admitted(&self, record: &Record) -> Result<bool> {
        if let Some(hook) = self.admission_hook.lock().unwrap().clone() {
            hook(record)
        } else {
            Ok(false)
        }
    }
    pub(crate) fn abort_old(&self, ids: &[i64]) {
        let manager = self.shared();
        let ids = ids.to_vec();
        drop(own(Box::pin(async move {
            // Native CheckpointManager::AbortOldTransactions queues a work item;
            // do not first-poll an End under the writer lock/engine reentry scope.
            tokio::task::yield_now().await;
            for id in ids {
                if let Some(owner) = manager
                    .owners
                    .lock()
                    .unwrap()
                    .get(&id)
                    .and_then(Weak::upgrade)
                {
                    let _ = owner.system_abort();
                }
            }
            Ok(())
        })));
    }
    pub(crate) async fn stable_permission(&self, lsn: i64) {
        loop {
            let changed = self.log_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.stable() >= lsn || self.faults.is_closing() {
                return;
            }
            changed.await;
        }
    }
    pub(crate) fn index_if_needed(&self, interval: u64) -> Result<()> {
        let log = self.log.lock().unwrap();
        let memory = self.memory().unwrap();
        let mut memory = memory.lock().unwrap();
        if memory
            .latest_index()
            .is_none_or(|p| memory.tail_position - p >= interval)
        {
            memory.indexing(log.tail, log.epoch)?;
            memory.flush();
        }
        Ok(())
    }
    pub(crate) fn runtime_checkpoint<T>(
        &self,
        create: impl FnOnce(i64) -> Result<Option<(Arc<Activity>, T)>>,
    ) -> Result<Option<T>> {
        let mut log = self.log.lock().unwrap();
        let Some((activity, result)) = create(log.tail)? else {
            return Ok(None);
        };
        self.map.admit(activity.node.clone())?;
        log.ordered.push(activity.clone());
        drop(log);
        #[cfg(test)]
        self.pause_physical_insertion(7);
        activity.replication.complete(Ok(()));
        self.start_completion(activity.clone());
        self.flush(Ok(()));
        Ok(Some(result))
    }
    pub(crate) fn set_checkpoint_hook(&self, hook: OrderedCheckpointHook) {
        *self.checkpoint_hook.lock().unwrap() = Some(hook);
    }
    #[cfg(test)]
    fn pause_physical_insertion(&self, kind: i32) {
        let pause = {
            let mut slot = self.physical_insertion_pause.lock().unwrap();
            if slot.as_ref().is_some_and(|(k, _, _)| *k == kind) {
                slot.take()
            } else {
                None
            }
        };
        if let Some((_, entered, release)) = pause {
            let _ = entered.send(());
            release
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
    }
    fn buffered_checkpoint(
        &self,
        log: &mut LogState,
        activity: &Activity,
        errors: &mut Vec<Error>,
    ) {
        if !matches!(activity.node.record().kind, RecordKind::Barrier { .. }) {
            return;
        }
        if activity.io_error.lock().unwrap().is_some() || self.faults.is_closing() {
            return;
        }
        if let Some(hook) = self.checkpoint_hook.lock().unwrap().clone() {
            match hook(log.tail) {
                Ok(Some(begin)) => {
                    if let Err(error) = self.map.admit(begin.node.clone()) {
                        errors.push(error);
                        return;
                    }
                    log.ordered.push(begin.clone());
                    begin.replication.complete(Ok(()));
                    self.start_completion(begin);
                }
                Ok(None) => {}
                Err(error) => errors.push(error),
            }
        }
    }
    pub(crate) fn reset_copy_log(
        &self,
        metadata: &super::copy_codec::CopyMetadata,
        capacity: usize,
    ) -> Result<()> {
        if self.role() != ProcessingRole::Secondary {
            return Err(Status::InvalidOperation.error());
        }
        let mut memory = super::memory_log::MemoryLog::new(capacity)?;
        memory.periodic_times = self
            .memory()
            .ok_or_else(|| Status::ObjectClosed.error())?
            .lock()
            .unwrap()
            .periodic_times;
        memory.indexing(metadata.starting, metadata.starting_epoch)?;
        memory.tail_lsn = metadata.starting;
        memory.vector = metadata.vector.clone();
        memory.flush();
        self.map.reuse();
        self.map.set_covered(metadata.checkpoint);
        self.covered_prefix
            .store(metadata.checkpoint, Ordering::Release);
        let ordering = Completion::new();
        ordering.complete(Ok(()));
        *self.log.lock().unwrap() = LogState {
            tail: metadata.starting,
            ordered: Vec::new(),
            queue: BTreeMap::new(),
            flushed: 0,
            epoch: metadata.starting_epoch,
            epoch_lsn: metadata.starting,
            ordering,
        };
        *self.dispatch.lock().unwrap() = DispatchState::default();
        self.last_barrier
            .store(metadata.starting, Ordering::Release);
        self.stable.store(metadata.checkpoint, Ordering::Release);
        *self.memory.lock().unwrap() = Some(Arc::new(Mutex::new(memory)));
        Ok(())
    }
}
impl ProcessingRole {
    fn redo(self) -> ApplyContext {
        match self {
            Self::Primary => ApplyContext::PrimaryRedo,
            Self::Secondary => ApplyContext::SecondaryRedo,
            Self::Recovery => ApplyContext::RecoveryRedo,
        }
    }
}

type ProcessorFaultHook = Arc<dyn Fn(Error) + Send + Sync>;
type WriteAccessCheck = Arc<dyn Fn() -> Result<()> + Send + Sync>;
type AdmissionHook = Arc<dyn Fn(&Record) -> Result<bool> + Send + Sync>;
type OrderedCheckpointHook = Arc<dyn Fn(i64) -> Result<Option<Arc<Activity>>> + Send + Sync>;

#[derive(Default)]
pub(crate) struct FaultState {
    hook: Mutex<Option<ProcessorFaultHook>>,
    provider_hook: Mutex<Option<ProcessorFaultHook>>,
    service: Mutex<Option<Error>>,
    log: Mutex<Option<Error>>,
    reported: Mutex<Vec<Error>>,
    /// Per-record ProcessError/OnReplicationError, not partition notification.
    observed: Mutex<Vec<Error>>,
    closing: AtomicBool,
}

impl FaultState {
    pub(crate) fn set_provider_hook(&self, hook: ProcessorFaultHook) {
        *self.provider_hook.lock().unwrap() = Some(hook);
    }
    pub(crate) fn set_hook(&self, hook: Arc<dyn Fn(Error) + Send + Sync>) {
        *self.hook.lock().unwrap() = Some(hook);
    }
    pub(crate) fn service_error(&self) -> Option<Error> {
        self.service.lock().expect("fault state poisoned").clone()
    }
    pub(crate) fn log_error(&self) -> Option<Error> {
        self.log.lock().expect("fault state poisoned").clone()
    }
    pub(crate) fn reported(&self) -> Vec<Error> {
        self.reported.lock().expect("fault state poisoned").clone()
    }
    pub(crate) fn observed(&self) -> Vec<Error> {
        self.observed.lock().expect("fault state poisoned").clone()
    }
    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }
    fn report_processor_fault(&self, error: Error) {
        // RoleContextDrainState::ReportFault closes lower admission before the
        // one-shot partition notification, independently of the public gate.
        if self
            .closing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.reported
                .lock()
                .expect("fault state poisoned")
                .push(error.clone());
            let hook = self.hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook(error);
            }
        }
    }
    pub(crate) fn process_service_error(&self, role: ProcessingRole, error: Error) -> Error {
        self.observed
            .lock()
            .expect("fault state poisoned")
            .push(error.clone());
        if role != ProcessingRole::Recovery {
            self.report_processor_fault(error.clone());
        }
        self.service
            .lock()
            .expect("fault state poisoned")
            .get_or_insert(error)
            .clone()
    }
    pub(crate) fn report_provider_fault(&self, error: Error) {
        self.reported
            .lock()
            .expect("fault state poisoned")
            .push(error.clone());
        // StateManager.cpp:4055–4059 direct partition notification is NOT the
        // processor service-error latch or RoleContextDrainState one-shot.
        if let Some(hook) = self.provider_hook.lock().unwrap().clone() {
            hook(error);
        }
    }
    pub(crate) fn report_checkpoint_fault(&self, error: Error) {
        self.observed
            .lock()
            .expect("fault state poisoned")
            .push(error.clone());
        self.report_processor_fault(error);
    }
    fn process_log_error(&self, role: ProcessingRole, error: Error) {
        self.observed
            .lock()
            .expect("fault state poisoned")
            .push(error.clone());
        if role != ProcessingRole::Recovery && !Status::ObjectClosed.matches(&error) {
            self.report_processor_fault(error.clone());
        }
        self.log
            .lock()
            .expect("fault state poisoned")
            .get_or_insert(error);
    }
    pub(crate) fn replication_error(&self, error: &Error) {
        self.observed
            .lock()
            .expect("fault state poisoned")
            .push(error.clone());
        // ReplicatedLogManager.cpp:473–516: expected transport failures do not
        // become service faults. This set is intentionally not the retry set.
        let expected = [
            Status::NotPrimary,
            Status::ReconfigurationPending,
            Status::QueueFull,
            Status::NoWriteQuorum,
            Status::ObjectClosed,
            Status::OperationTooLarge,
            Status::Cancelled,
        ];
        if !expected.iter().any(|status| status.matches(error)) {
            self.report_processor_fault(error.clone());
        }
    }
}

#[derive(Clone)]
pub(crate) struct LowerAttempt {
    pub(crate) wire: OperationData,
    pub(crate) assigned: Result<SequenceNumber>,
}

struct LowerState {
    next: SequenceNumber,
    scripted: VecDeque<Result<SequenceNumber>>,
    attempts: Vec<LowerAttempt>,
}

/// Controlled lower *admission* supplies the authoritative LSN, independently
/// of flush/apply/quorum. Default assignment is the lower's sequence, not a
/// transaction-local sequence or derived commit timestamp.
pub(crate) struct ControlledLower {
    state: Mutex<LowerState>,
}

impl ControlledLower {
    pub(crate) fn new(next_lsn: SequenceNumber) -> Self {
        Self {
            state: Mutex::new(LowerState {
                next: next_lsn,
                scripted: VecDeque::new(),
                attempts: Vec::new(),
            }),
        }
    }
    pub(crate) fn script(&self, outcomes: impl IntoIterator<Item = Result<SequenceNumber>>) {
        self.state
            .lock()
            .expect("lower poisoned")
            .scripted
            .extend(outcomes);
    }
    pub(crate) fn attempts(&self) -> Vec<LowerAttempt> {
        self.state.lock().expect("lower poisoned").attempts.clone()
    }
    fn admit(&self, wire: OperationData) -> Result<SequenceNumber> {
        let mut lower = self.state.lock().expect("lower poisoned");
        let assigned = match lower.scripted.pop_front() {
            Some(outcome) => outcome,
            None => Ok(lower.next),
        }
        .and_then(|lsn| {
            if lsn <= 0 {
                Err(Error::InvalidInput("lower assigned nonpositive LSN"))
            } else {
                Ok(lsn)
            }
        });
        lower.attempts.push(LowerAttempt {
            wire,
            assigned: assigned.clone(),
        });
        if let Ok(lsn) = assigned {
            lower.next = lower.next.max(
                lsn.checked_add(1)
                    .ok_or(Error::ResourceLimit("lower LSN exhausted"))?,
            );
        }
        assigned
    }
}

pub(crate) struct Activity {
    pub(crate) physical_position: AtomicU64,
    pub(crate) checkpoint_entered: Completion<()>,
    pub(crate) checkpoint_prepared: Mutex<Option<Completion<()>>>,
    io_error: Mutex<Option<Error>>,
    pub(crate) previous_stable: AtomicI64,
    pub(crate) node: Arc<RecordNode>,
    pub(crate) flush: Completion<()>,
    pub(crate) apply: Completion<()>,
    pub(crate) replication: Completion<()>,
    pub(crate) processing: Completion<()>,
    pub(crate) result: Completion<SequenceNumber>,
    primary: bool,
}

impl Activity {
    pub(crate) fn new(node: Arc<RecordNode>, primary: bool) -> Arc<Self> {
        Arc::new(Self {
            physical_position: AtomicU64::new(u64::MAX),
            checkpoint_entered: Completion::new(),
            checkpoint_prepared: Mutex::new(None),
            io_error: Mutex::new(None),
            previous_stable: AtomicI64::new(-1),
            node,
            flush: Completion::new(),
            apply: Completion::new(),
            replication: Completion::new(),
            processing: Completion::new(),
            result: Completion::new(),
            primary,
        })
    }
}

struct LogState {
    tail: SequenceNumber,
    ordered: Vec<Arc<Activity>>,
    queue: BTreeMap<SequenceNumber, Arc<Activity>>,
    flushed: usize,
    epoch: Epoch,
    epoch_lsn: SequenceNumber,
    ordering: Completion<()>,
}

#[derive(Default)]
struct DispatchState {
    pending: Vec<Arc<Activity>>,
    flushed: VecDeque<(Arc<Activity>, Result<()>)>,
    running: bool,
}

enum DispatchAction {
    Group(Vec<Arc<Activity>>),
    Failed(Arc<Activity>, Error),
}

type CommitCallback = Arc<dyn Fn(Arc<dyn TransactionBase>) + Send + Sync>;
struct AccessState {
    application: bool,
    terminal: bool,
    faulted: bool,
}
struct CleanupState {
    state: Mutex<(bool, usize)>,
    changed: tokio::sync::Notify,
}
pub(crate) struct CleanupEntry(Arc<CleanupState>);
impl Drop for CleanupEntry {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().1 -= 1;
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
type PhysicalInsertionPause = (
    i32,
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);
pub(crate) struct KernelManager {
    #[cfg(test)]
    pub(crate) physical_insertion_pause: Mutex<Option<PhysicalInsertionPause>>,
    checkpoint_hook: Mutex<Option<OrderedCheckpointHook>>,
    #[cfg(test)]
    pub(crate) flush_extracted: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    self_weak: Weak<Self>,
    pub(crate) lower: Arc<ControlledLower>,
    runtime: Mutex<Option<Arc<RuntimeLower>>>,
    memory: Mutex<Option<Arc<Mutex<super::memory_log::MemoryLog>>>>,
    log_changed: tokio::sync::Notify,
    limits: Mutex<Option<Arc<super::limits::Limits>>>,
    pub(crate) directory: Arc<ProviderDirectory>,
    pub(crate) map: TransactionMap,
    pub(crate) versions: VersionManager,
    pub(crate) faults: Arc<FaultState>,
    role: Mutex<ProcessingRole>,
    replica_role: Mutex<ReplicaRole>,
    covered_prefix: AtomicI64,
    log: Mutex<LogState>,
    dispatch: Mutex<DispatchState>,
    last_barrier: Arc<AtomicI64>,
    stable: AtomicI64,
    /// Weak capture bridge for borrowed SDK begin/add, not a request ledger or
    /// successful-outcome index. Record nodes retain their own shared owners.
    owners: Mutex<BTreeMap<TransactionId, Weak<KernelTransaction>>>,
    admission_hook: Mutex<Option<AdmissionHook>>,
    throttle_hook: Mutex<Option<WriteAccessCheck>>,
    retry: Arc<RetryRuntime>,
    notifications: Mutex<Vec<(TransactionId, SequenceNumber)>>,
    commit_callback: Mutex<Option<CommitCallback>>,
    access: Mutex<AccessState>,
    cleanup: Arc<CleanupState>,
    dispatch_gate: Arc<tokio::sync::RwLock<()>>,
    write_access: Mutex<Option<WriteAccessCheck>>,
    unregister_error: Mutex<Option<Error>>,
    throttle: Mutex<VecDeque<Result<()>>>,
}

impl KernelManager {
    /// Already-established bootstrap has logical tail/version1 (LogManager.cpp:
    /// 286–336). Physical bootstrap and primary election are not synthesized here.
    pub(crate) fn established(
        directory: Arc<ProviderDirectory>,
        lower: Arc<ControlledLower>,
        role: ProcessingRole,
        covered_prefix: SequenceNumber,
        retry: Arc<RetryRuntime>,
    ) -> Arc<Self> {
        let last_barrier = Arc::new(AtomicI64::new(1));
        let version_sample = last_barrier.clone();
        let faults = directory.faults.clone();
        let ordering = Completion::new();
        ordering.complete(Ok(()));
        let manager = Arc::new_cyclic(|self_weak| Self {
            #[cfg(test)]
            flush_extracted: Mutex::new(None),
            self_weak: self_weak.clone(),
            lower,
            runtime: Mutex::new(None),
            memory: Mutex::new(None),
            log_changed: tokio::sync::Notify::new(),
            limits: Mutex::new(None),
            directory: directory.clone(),
            map: TransactionMap::new(covered_prefix),
            versions: VersionManager::new(Arc::new(move || {
                Ok(version_sample.load(Ordering::Acquire))
            })),
            faults,
            role: Mutex::new(role),
            replica_role: Mutex::new(if role == ProcessingRole::Primary {
                ReplicaRole::Primary
            } else {
                ReplicaRole::IdleSecondary
            }),
            covered_prefix: AtomicI64::new(covered_prefix),
            checkpoint_hook: Mutex::new(None),
            #[cfg(test)]
            physical_insertion_pause: Mutex::new(None),
            log: Mutex::new(LogState {
                tail: 1,
                ordered: Vec::new(),
                queue: BTreeMap::new(),
                flushed: 0,
                epoch: Epoch {
                    data_loss_number: 0,
                    configuration_number: 0,
                },
                epoch_lsn: 0,
                ordering,
            }),
            dispatch: Mutex::new(DispatchState::default()),
            last_barrier,
            stable: AtomicI64::new(0),
            owners: Mutex::new(BTreeMap::new()),
            admission_hook: Mutex::new(None),
            throttle_hook: Mutex::new(None),
            retry,
            notifications: Mutex::new(Vec::new()),
            access: Mutex::new(AccessState {
                application: true,
                terminal: false,
                faulted: false,
            }),
            cleanup: Arc::new(CleanupState {
                state: Mutex::new((true, 0)),
                changed: tokio::sync::Notify::new(),
            }),
            dispatch_gate: Arc::new(tokio::sync::RwLock::new(())),
            write_access: Mutex::new(None),
            commit_callback: Mutex::new(None),
            unregister_error: Mutex::new(None),
            throttle: Mutex::new(VecDeque::new()),
        });
        directory.bind(Arc::downgrade(&manager), role);
        manager
    }

    fn shared(&self) -> Arc<Self> {
        self.self_weak
            .upgrade()
            .expect("manager activity has shared owner")
    }
    pub(crate) fn bind_runtime(&self, lower: Arc<RuntimeLower>) {
        if self.limits.lock().unwrap().is_none() {
            *self.limits.lock().unwrap() = Some(super::limits::Limits::new());
            self.versions.set_limits(64, 512);
        }
        *self.runtime.lock().expect("runtime binding poisoned") = Some(lower);
    }
    fn runtime(&self) -> Option<Arc<RuntimeLower>> {
        self.runtime
            .lock()
            .expect("runtime binding poisoned")
            .clone()
    }
    pub(crate) fn role(&self) -> ProcessingRole {
        *self.role.lock().unwrap()
    }
    pub(crate) fn set_role(&self, role: ReplicaRole) {
        let processing = if role == ReplicaRole::Primary {
            ProcessingRole::Primary
        } else if matches!(
            role,
            ReplicaRole::ActiveSecondary | ReplicaRole::IdleSecondary
        ) {
            ProcessingRole::Secondary
        } else {
            ProcessingRole::Recovery
        };
        *self.role.lock().unwrap() = processing;
        *self.replica_role.lock().unwrap() = role;
        self.directory
            .bind(Arc::downgrade(&self.shared()), processing);
    }
    pub(crate) fn memory(&self) -> Option<Arc<Mutex<super::memory_log::MemoryLog>>> {
        self.memory.lock().unwrap().clone()
    }
    pub(crate) fn reserve_memory(
        &self,
        record: &Record,
    ) -> Result<Option<Arc<super::memory_log::Retention>>> {
        self.memory()
            .map(|memory| memory.lock().unwrap().reserve(record))
            .transpose()
    }
    pub(crate) fn reclaim_memory(&self, position: u64) -> Result<bool> {
        let mut log = self.log.lock().unwrap();
        let Some(memory) = self.memory() else {
            return Ok(false);
        };
        let mut memory = memory.lock().unwrap();
        if !memory.reclaim(position)? {
            return Ok(false);
        }
        let retained: std::collections::BTreeSet<_> = memory
            .entries
            .iter()
            .filter_map(|entry| entry.logical.as_ref())
            .map(|node| Arc::as_ptr(node) as usize)
            .collect();
        let mut removed_flushed = 0;
        let old_flushed = log.flushed;
        let mut index = 0;
        log.ordered.retain(|activity| {
            let keep = index >= old_flushed
                || !activity.processing.is_completed()
                || matches!(activity.node.record().kind, RecordKind::Physical { .. })
                || retained.contains(&(Arc::as_ptr(&activity.node) as usize));
            if !keep && index < old_flushed {
                removed_flushed += 1;
            }
            index += 1;
            keep
        });
        log.flushed -= removed_flushed;
        Ok(true)
    }
    pub(crate) fn physical_truncate(&self, position: u64) -> Result<bool> {
        let mut log = self.log.lock().unwrap();
        let memory = self.memory().ok_or_else(|| Status::ObjectClosed.error())?;
        let mut memory = memory.lock().unwrap();
        if !memory.physical_truncate_selected(position)? {
            return Ok(false);
        }
        let retained: std::collections::BTreeSet<_> = memory
            .entries
            .iter()
            .filter_map(|entry| entry.logical.as_ref())
            .map(|node| Arc::as_ptr(node) as usize)
            .collect();
        let old = log.flushed;
        let mut index = 0;
        let mut removed = 0;
        log.ordered.retain(|activity| {
            let keep = index >= old
                || !activity.flush.is_completed()
                || !activity.processing.is_completed()
                || (matches!(activity.node.record().kind, RecordKind::Physical { .. })
                    && activity.physical_position.load(Ordering::Acquire) >= position)
                || retained.contains(&(Arc::as_ptr(&activity.node) as usize));
            if !keep && index < old {
                removed += 1;
            }
            index += 1;
            keep
        });
        log.flushed -= removed;
        Ok(true)
    }
    pub(crate) fn fresh(directory: Arc<ProviderDirectory>, retry: Arc<RetryRuntime>) -> Arc<Self> {
        let manager = Self::established(
            directory,
            Arc::new(ControlledLower::new(2)),
            ProcessingRole::Recovery,
            0,
            retry,
        );
        manager.log.lock().unwrap().tail = 0;
        manager.last_barrier.store(0, Ordering::Release);
        manager.set_role(ReplicaRole::Unknown);
        *manager.limits.lock().unwrap() = Some(super::limits::Limits::new());
        manager.versions.set_limits(64, 512);
        manager
    }
    pub(crate) fn open_memory(&self, capacity: usize) -> Result<()> {
        if self.tail() != 0 || self.memory().is_some() {
            return Err(Error::InvalidInput("fresh memory open repeated"));
        }
        let mut memory = super::memory_log::MemoryLog::new(capacity)?;
        for record in memory.bootstrap()? {
            let activity = self.replay_record(record.encode()?, None)?;
            activity
                .node
                .retain_memory(memory.reserve(activity.node.record())?);
            for entry in &mut memory.entries {
                if entry
                    .logical
                    .as_ref()
                    .is_some_and(|node| node.record() == activity.node.record())
                {
                    entry.logical = Some(activity.node.clone());
                }
            }
        }
        self.flush(Ok(()));
        *self.memory.lock().unwrap() = Some(Arc::new(Mutex::new(memory)));
        Ok(())
    }
    pub(crate) fn gate(&self) -> Result<()> {
        let access = self.access.lock().unwrap();
        if access.application && !access.terminal && !access.faulted {
            Ok(())
        } else {
            Err(Status::ObjectClosed.error())
        }
    }
    pub(crate) fn close_activity_gate(&self) {
        self.fence_terminal();
        self.cleanup.state.lock().unwrap().0 = false;
    }
    pub(crate) fn fence_application(&self) {
        self.access.lock().unwrap().application = false;
    }
    pub(crate) fn fence_fault(&self) {
        let mut access = self.access.lock().unwrap();
        access.faulted = true;
        access.application = false;
    }
    pub(crate) fn fence_terminal(&self) {
        let mut access = self.access.lock().unwrap();
        access.terminal = true;
        access.application = false;
    }
    pub(crate) fn open_activity_gate(&self) -> Result<()> {
        let mut access = self.access.lock().unwrap();
        if access.terminal || access.faulted || self.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        access.application = true;
        Ok(())
    }
    pub(crate) fn open_cleanup_gate(&self) {
        self.cleanup.state.lock().unwrap().0 = true;
    }
    pub(crate) fn cleanup_entry(&self) -> Result<CleanupEntry> {
        let mut state = self.cleanup.state.lock().unwrap();
        if !state.0 {
            return Err(Status::ObjectClosed.error());
        }
        state.1 += 1;
        Ok(CleanupEntry(self.cleanup.clone()))
    }
    pub(crate) async fn close_cleanup_gate(&self) {
        self.cleanup.state.lock().unwrap().0 = false;
        loop {
            let changed = self.cleanup.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.cleanup.state.lock().unwrap().1 == 0 {
                return;
            }
            changed.await;
        }
    }
    pub(crate) async fn processing_drained(&self) {
        // OperationProcessor.cpp:1085–1133 owns this independently of lower
        // stream/capture and primary admission-driver completion.
        let records = {
            let log = self.log.lock().unwrap();
            log.ordered
                .iter()
                .chain(log.queue.values())
                .cloned()
                .collect::<Vec<_>>()
        };
        for activity in records {
            if !matches!(activity.node.record().kind, RecordKind::Physical { .. }) {
                let _ = activity.processing.wait().await;
            }
        }
    }
    pub(crate) async fn wait_lsn_ordering(&self, previous: i64) {
        loop {
            let changed = self.log_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let log = self.log.lock().unwrap();
                if log.tail >= previous && log.queue.is_empty() {
                    return;
                }
            }
            changed.await;
        }
    }
    pub(crate) fn set_write_access(&self, access: Arc<dyn Fn() -> Result<()> + Send + Sync>) {
        *self.write_access.lock().unwrap() = Some(access);
    }
    fn writable(&self) -> Result<()> {
        self.gate()?;
        if let Some(check) = self.write_access.lock().unwrap().clone() {
            check()?;
        }
        Ok(())
    }
    pub(crate) async fn pause_dispatch(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        // Native DrainAndPauseDispatch waits the prior group and blocks only
        // dispatcher/provider-role overlap, not independent transaction groups.
        self.dispatch_gate.clone().write_owned().await
    }
    pub(crate) fn epoch(&self) -> Epoch {
        self.log.lock().unwrap().epoch
    }
    pub(crate) async fn wait_flushed(&self, upto: i64) -> Result<()> {
        loop {
            let changed = self.log_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(error) = self.faults.log_error() {
                return Err(error);
            }
            if self
                .memory()
                .is_some_and(|memory| memory.lock().unwrap().flushed_lsn >= upto)
            {
                return Ok(());
            }
            changed.await;
        }
    }
    pub(crate) fn inject_unregister_error(&self, error: Error) {
        *self
            .unregister_error
            .lock()
            .expect("unregister seam poisoned") = Some(error);
    }
    /// TransactionManager::InitiateLogicalRecordProcessingOnPrimary checks
    /// CheckpointManager::ErrorIfThrottled before ReplicatedLogManager admission.
    pub(crate) fn script_throttle(&self, outcomes: impl IntoIterator<Item = Result<()>>) {
        let outcomes: Vec<_> = outcomes.into_iter().collect();
        assert!(outcomes.iter().all(|outcome| outcome
            .as_ref()
            .err()
            .is_none_or(|error| Status::ServiceTooBusy.matches(error))));
        self.throttle
            .lock()
            .expect("throttle seam poisoned")
            .extend(outcomes);
    }
    pub(crate) fn new_transaction(&self) -> Result<Arc<KernelTransaction>> {
        self.gate()?;
        let manager: Arc<dyn ITransactionManager> = self.shared();
        let transaction = KernelTransaction::create(
            Arc::downgrade(&manager),
            self.role() == ProcessingRole::Primary,
            self.retry.clone(),
        )?;
        if let Some(limits) = self.limits.lock().unwrap().clone() {
            transaction.bind_limits(limits)?;
        }
        let mut owners = self.owners.lock().expect("transaction owners poisoned");
        owners.retain(|_, owner| owner.strong_count() > 0);
        if owners.len() >= 1024 {
            return Err(Status::InsufficientResources.error());
        }
        owners.insert(
            transaction.get_transaction_id(),
            Arc::downgrade(&transaction),
        );
        Ok(transaction)
    }
    fn retain_borrowed(&self, transaction: &dyn Transaction) -> Result<Arc<dyn Transaction>> {
        self.owners
            .lock()
            .expect("transaction owners poisoned")
            .get(&transaction.get_transaction_id())
            .and_then(Weak::upgrade)
            .map(|owner| owner as Arc<dyn Transaction>)
            .ok_or(Error::InvalidInput(
                "borrowed transaction has no bounded shared owner",
            ))
    }
    pub(crate) fn tail(&self) -> SequenceNumber {
        self.log.lock().expect("log poisoned").tail
    }
    pub(crate) fn stable(&self) -> SequenceNumber {
        self.stable.load(Ordering::Acquire)
    }
    pub(crate) fn version(&self) -> SequenceNumber {
        self.last_barrier.load(Ordering::Acquire)
    }
    pub(crate) fn records(&self) -> Vec<Arc<Activity>> {
        self.log.lock().expect("log poisoned").ordered.clone()
    }
    pub(crate) fn queued_lsns(&self) -> Vec<SequenceNumber> {
        self.log
            .lock()
            .expect("log poisoned")
            .queue
            .keys()
            .copied()
            .collect()
    }
    pub(crate) fn notifications(&self) -> Vec<(TransactionId, SequenceNumber)> {
        self.notifications
            .lock()
            .expect("notifications poisoned")
            .clone()
    }
    /// Nonthrowing notification observation seam. It does not manufacture the
    /// full ITransactionalReplicator source needed by the public SDK handler.
    pub(crate) fn set_commit_callback(&self, callback: Option<CommitCallback>) {
        *self
            .commit_callback
            .lock()
            .expect("commit callback poisoned") = callback;
    }
    /// Recovery-role probes surface the original latched callback/log status;
    /// they do not simulate persisted Open or ReportFault-driven recovery.
    pub(crate) fn recovery_result(&self) -> Result<()> {
        if self.role() != ProcessingRole::Recovery {
            return Err(Status::NotImplemented.error());
        }
        self.faults
            .service_error()
            .or_else(|| self.faults.log_error())
            .map_or(Ok(()), Err)
    }
    pub(crate) fn activity(&self, lsn: SequenceNumber) -> Arc<Activity> {
        self.records()
            .into_iter()
            .find(|record| record.node.lsn() == lsn)
            .expect("admitted LSN exists")
    }

    fn preflight(&self, record: &Record) -> Result<()> {
        self.gate()?;
        if let RecordKind::Begin {
            metadata: Some(metadata),
            redo,
            ..
        }
        | RecordKind::Operation {
            metadata: Some(metadata),
            redo,
            ..
        } = &record.kind
        {
            self.directory.preflight_record(metadata, redo.as_ref())?;
        }
        if record.transaction_id().is_some_and(|id| id <= 0)
            || matches!(
                record.kind,
                RecordKind::Operation {
                    redo_only: true,
                    ..
                }
            )
        {
            return Err(Status::NotImplemented.error());
        }
        if matches!(
            record.kind,
            RecordKind::Begin { .. } | RecordKind::Operation { .. }
        ) {
            if let Some(throttle) = self.throttle_hook.lock().unwrap().clone() {
                throttle()?;
            }
            self.throttle
                .lock()
                .expect("throttle seam poisoned")
                .pop_front()
                .unwrap_or(Ok(()))?;
        }
        if self.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        if self.role() != ProcessingRole::Primary {
            return Err(Status::NotPrimary.error());
        }
        if let RecordKind::End { transaction_id, .. } = record.kind {
            if self.runtime().is_some()
                && !self
                    .map
                    .latest_records()
                    .iter()
                    .any(|r| r.transaction_id() == Some(transaction_id))
            {
                return Err(Error::InvalidInput(
                    "end requires an admitted pending begin",
                ));
            }
        }
        Ok(())
    }

    fn admit(
        &self,
        mut record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<Arc<Activity>> {
        self.preflight(&record)?;
        record.lsn = -1;
        if let Some(runtime) = self.runtime() {
            return runtime.admit(self.shared(), record, transaction, context);
        }
        let wire = record.encode()?;
        let lsn = self
            .lower
            .admit(wire)
            .inspect_err(|error| self.faults.replication_error(error))?;
        record.lsn = lsn;
        let node = RecordNode::new(record, transaction, context)?;
        let activity = Activity::new(node, true);
        self.append(activity.clone())?;
        self.start_completion(activity.clone());
        Ok(activity)
    }

    pub(crate) fn append(&self, activity: Arc<Activity>) -> Result<()> {
        let mut log = self.log.lock().expect("log poisoned");
        let mut checkpoint_errors = Vec::new();
        let epoch = matches!(activity.node.record().kind, RecordKind::UpdateEpoch { .. });
        let lsn = activity.node.lsn();
        activity.node.set_epoch(log.epoch);
        if (epoch && lsn != log.tail) || (!epoch && lsn <= log.tail) || log.queue.contains_key(&lsn)
        {
            return Err(Error::InvalidInput("invalid native ordered append LSN"));
        }
        // Native map mutation precedes ordered append, including gap admission.
        self.map.admit(activity.node.clone())?;
        if epoch {
            if let Some(memory) = self.memory() {
                let mut memory = memory.lock().unwrap();
                if let Err(error) = memory.append_logical(activity.node.clone()) {
                    memory.fail_writer(error.clone());
                    *activity.io_error.lock().unwrap() = Some(error);
                }
            }
            log.ordered.push(activity);
        } else if lsn == log.tail + 1 {
            log.tail = lsn;
            if let Some(memory) = self.memory() {
                let mut memory = memory.lock().unwrap();
                if let Err(error) = memory.append_logical(activity.node.clone()) {
                    // Actual lower admission/map/LSN are already owned. Local
                    // write failure is failed flush, never an admission rollback.
                    memory.fail_writer(error.clone());
                    *activity.io_error.lock().unwrap() = Some(error);
                }
            }
            log.ordered.push(activity.clone());
            self.buffered_checkpoint(&mut log, &activity, &mut checkpoint_errors);
            loop {
                let next = log.tail + 1;
                let Some(queued) = log.queue.remove(&next) else {
                    break;
                };
                log.tail = next;
                if let Some(memory) = self.memory() {
                    let mut memory = memory.lock().unwrap();
                    if let Err(error) = memory.append_logical(queued.node.clone()) {
                        memory.fail_writer(error.clone());
                        *queued.io_error.lock().unwrap() = Some(error);
                    }
                }
                log.ordered.push(queued.clone());
                self.buffered_checkpoint(&mut log, &queued, &mut checkpoint_errors);
            }
            if log.queue.is_empty() {
                log.ordering.complete(Ok(()));
            }
        } else {
            if log.queue.is_empty() {
                log.ordering = Completion::new();
            }
            log.queue.insert(lsn, activity);
        }
        drop(log);
        for error in checkpoint_errors {
            self.faults.report_checkpoint_fault(error);
        }
        self.log_changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn admit_operation(
        &self,
        transaction: Arc<dyn Transaction>,
        metadata: OperationData,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        context: Option<Arc<dyn OperationContext>>,
        begin: Option<bool>,
    ) -> Result<Arc<Activity>> {
        let id = transaction.get_transaction_id();
        let kind = match begin {
            Some(single) => RecordKind::Begin {
                transaction_id: id,
                single,
                metadata: Some(metadata),
                undo,
                redo,
            },
            None => RecordKind::Operation {
                transaction_id: id,
                redo_only: false,
                metadata: Some(metadata),
                undo,
                redo,
            },
        };
        self.admit(Record { lsn: -1, kind }, Some(transaction), context)
    }

    pub(crate) fn admit_borrowed(
        &self,
        transaction: &dyn Transaction,
        metadata: OperationData,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        context: Option<Arc<dyn OperationContext>>,
        begin: bool,
    ) -> Result<()> {
        self.admit_operation(
            self.retain_borrowed(transaction)?,
            metadata,
            undo,
            redo,
            context,
            if begin { Some(false) } else { None },
        )
        .map(|_| ())
    }

    fn end(
        &self,
        transaction: Arc<dyn Transaction>,
        committed: bool,
    ) -> ApiFuture<'static, SequenceNumber> {
        let id = transaction.get_transaction_id();
        let record = Record {
            lsn: -1,
            kind: RecordKind::End {
                transaction_id: id,
                committed,
            },
        };
        self.observe_async(record, Some(transaction), None)
    }
    pub(crate) fn observe_admission(
        admitted: Result<Arc<Activity>>,
    ) -> ApiFuture<'static, SequenceNumber> {
        match admitted {
            Err(error) => Box::pin(async move { Err(error) }),
            Ok(activity) => Box::pin(async move { activity.result.wait().await.as_ref().clone() }),
        }
    }
    pub(crate) fn barrier(&self, advertised: SequenceNumber) -> Result<Arc<Activity>> {
        self.admit(
            Record {
                lsn: -1,
                kind: RecordKind::Barrier {
                    last_stable_lsn: advertised,
                },
            },
            None,
            None,
        )
    }

    /// Native local Information/BeginCheckpoint dispatch boundary; no lower LSN
    /// is assigned and no physical record enters replication/copy logical data.
    pub(crate) fn local_boundary(&self, native_type: i32, event: i32) -> Result<Arc<Activity>> {
        if (native_type != 7 && native_type != 12)
            || (native_type == 12 && !(1..=7).contains(&event))
        {
            return Err(Error::InvalidInput("unsupported native local boundary"));
        }
        let mut log = self.log.lock().unwrap();
        let mut physical_position = u64::MAX;
        if native_type == 12 {
            if let Some(memory) = self.memory() {
                let mut memory = memory.lock().unwrap();
                physical_position = memory.tail_position;
                memory.information(event)?;
                memory.flush();
            }
        }
        let node = RecordNode::new(
            Record {
                lsn: log.tail,
                kind: RecordKind::Physical { native_type, event },
            },
            None,
            None,
        )?;
        self.map.admit(node.clone())?;
        let activity = Activity::new(node, self.role() == ProcessingRole::Primary);
        activity
            .physical_position
            .store(physical_position, Ordering::Release);
        log.ordered.push(activity.clone());
        drop(log);
        #[cfg(test)]
        self.pause_physical_insertion(native_type);
        activity.replication.complete(Ok(()));
        self.start_completion(activity.clone());
        self.flush(Ok(()));
        Ok(activity)
    }

    pub(crate) fn observe_async(
        &self,
        mut record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'static, SequenceNumber> {
        if let Some(runtime) = self.runtime() {
            if let Err(error) = self.preflight(&record) {
                return Box::pin(async move { Err(error) });
            }
            record.lsn = -1;
            let manager = self.shared();
            let work = runtime.admit_async(manager.clone(), record, transaction, context);
            return own(Box::pin(async move {
                match work.await {
                    Ok(activity) => activity.result.wait().await.as_ref().clone(),
                    Err(error) => Err(error),
                }
            }));
        }
        Self::observe_admission(self.admit(record, transaction, context))
    }

    /// Progress-vector monotonicity is a narrow input seam. Same-epoch success
    /// precedes native tail equality; a local marker retains the existing tail.
    pub(crate) fn update_epoch(
        &self,
        epoch: Epoch,
        previous_lsn: SequenceNumber,
        replica_id: i64,
        ticks: i64,
    ) -> Result<Option<Arc<Activity>>> {
        self.gate()?;
        self.update_epoch_provider(epoch, previous_lsn, replica_id, ticks)
    }
    /// Internal lower callback/copy admission while application access is fenced.
    /// This grants no control authority; only the bound runtime's provider uses it.
    pub(crate) fn update_epoch_provider(
        &self,
        epoch: Epoch,
        previous_lsn: i64,
        replica_id: i64,
        ticks: i64,
    ) -> Result<Option<Arc<Activity>>> {
        let mut log = self.log.lock().expect("log poisoned");
        if self.role() == ProcessingRole::Primary && !log.queue.is_empty() {
            return Err(Error::InvalidInput(
                "use owned UpdateEpoch ordering wait for admitted gaps",
            ));
        }
        let key = |epoch: Epoch| (epoch.data_loss_number, epoch.configuration_number);
        if key(epoch) < key(log.epoch) || previous_lsn < log.epoch_lsn {
            return Err(Error::InvalidInput("nonmonotonic progress-vector seam"));
        }
        if epoch == log.epoch {
            return Ok(None);
        }
        if previous_lsn != log.tail {
            return Err(if self.role() == ProcessingRole::Secondary {
                Status::InvalidOperation.error()
            } else {
                Error::InvalidInput("native epoch/tail invariant")
            });
        }
        let node = RecordNode::new(
            Record {
                lsn: previous_lsn,
                kind: RecordKind::UpdateEpoch {
                    epoch,
                    primary_replica_id: replica_id,
                    timestamp_ticks: ticks,
                },
            },
            None,
            None,
        )?;
        let activity = Activity::new(node, self.role() == ProcessingRole::Primary);
        // Uniform allocation/PV admission is preflighted before any
        // map/log/epoch mutation. Holding log→memory prevents a stale preview.
        let memory_owner = self.memory();
        let mut memory = memory_owner.as_ref().map(|memory| memory.lock().unwrap());
        if let Some(memory) = memory.as_mut() {
            if memory.vector.len() >= 128 {
                return Err(Status::InsufficientResources.error());
            }
            let retention = memory.reserve(activity.node.record())?;
            activity.node.retain_memory(retention);
            memory.preflight_logical(&activity.node)?;
            memory
                .vector
                .try_reserve(1)
                .map_err(|_| Status::InsufficientResources.error())?;
        }
        self.map.admit(activity.node.clone())?;
        if let Some(memory) = memory.as_mut() {
            // preflight and append use the same locked state; no checked failure
            // remains after the write except an invariant violation/fatal OOM.
            memory.append_logical(activity.node.clone())?;
            memory.vector.push(super::memory_log::ProgressEntry {
                epoch,
                lsn: previous_lsn,
                primary: replica_id,
                ticks,
            });
            memory.flush();
        }
        log.ordered.push(activity.clone());
        log.epoch = epoch;
        log.epoch_lsn = previous_lsn;
        drop(memory);
        drop(log);
        activity.replication.complete(Ok(()));
        self.start_completion(activity.clone());
        Ok(Some(activity))
    }

    /// Native control entry waits already-admitted primary ordering before its
    /// no-suspension epoch prefix. Flush remains an independent lower completion
    /// seam; AwaitProcessing does not invent a new returned flush-status branch.
    pub(crate) fn update_epoch_async(
        &self,
        epoch: Epoch,
        previous_lsn: SequenceNumber,
        replica_id: i64,
        ticks: i64,
    ) -> ApiFuture<'static, ()> {
        let ordering = self.log.lock().expect("log poisoned").ordering.clone();
        let manager = self.shared();
        own(Box::pin(async move {
            if manager.role() == ProcessingRole::Primary {
                let _ = ordering.wait().await;
                loop {
                    let changed = manager.log_changed.notified();
                    if manager.tail() >= previous_lsn {
                        break;
                    }
                    changed.await;
                }
            }
            if let Some(activity) = manager.update_epoch(epoch, previous_lsn, replica_id, ticks)? {
                let _ = activity.processing.wait().await;
            }
            Ok(())
        }))
    }

    /// Secondary metadata overwrites embedded LSN; copy leaves embedded LSN.
    /// This is a bounded replay probe, not persisted recovery/open.
    pub(crate) fn replay_record(
        &self,
        wire: OperationData,
        sequence: Option<SequenceNumber>,
    ) -> Result<Arc<Activity>> {
        if self.role() == ProcessingRole::Primary {
            return Err(Error::InvalidInput("replay requires a nonprimary role"));
        }
        let mut record = Record::decode(&wire)?;
        if let Some(lsn) = sequence {
            record.lsn = lsn;
        }
        Self::preflight_replay(&record)?;
        self.preflight_replay_routing(&record)?;
        self.replay_decoded(record)
    }
    pub(crate) fn preflight_replay_routing(&self, record: &Record) -> Result<()> {
        if let RecordKind::Begin {
            metadata: Some(metadata),
            redo,
            ..
        }
        | RecordKind::Operation {
            metadata: Some(metadata),
            redo,
            ..
        } = &record.kind
        {
            self.directory.preflight_record(metadata, redo.as_ref())?;
        }
        Ok(())
    }
    pub(crate) fn preflight_replay(record: &Record) -> Result<()> {
        if record.transaction_id().is_some_and(|id| id < 0)
            || matches!(
                record.kind,
                RecordKind::Operation {
                    redo_only: true,
                    ..
                }
            )
        {
            return Err(Status::NotImplemented.error());
        }
        // Logical payloads stay opaque in the codec. Execution, including
        // covered-prefix replay, must preflight A0's native-only routing format
        // before append/map mutation. Decode-time ID advancement is separate.
        if let RecordKind::Begin { metadata, .. } | RecordKind::Operation { metadata, .. } =
            &record.kind
        {
            let metadata = metadata
                .as_ref()
                .ok_or(Error::InvalidInput("missing native named metadata"))?;
            let (provider, _) = super::named::decode(metadata)?;
            if provider == 0 {
                return Err(Error::InvalidInput("empty native provider identity"));
            }
        }
        Ok(())
    }
    pub(crate) fn replay_decoded(&self, record: Record) -> Result<Arc<Activity>> {
        Self::preflight_replay(&record)?;
        self.preflight_replay_routing(&record)?;
        let transaction = record
            .transaction_id()
            .map(KernelTransaction::replay)
            .transpose()?
            .map(|transaction| transaction as Arc<dyn TransactionBase>);
        let retention = self.reserve_memory(&record)?;
        let node = RecordNode::new(record, transaction, None)?;
        if let Some(retention) = retention {
            node.retain_memory(retention);
        }
        let activity = Activity::new(node, false);
        self.append(activity.clone())?;
        activity.replication.complete(Ok(()));
        self.start_completion(activity.clone());
        Ok(activity)
    }
    /// SecondaryDrainManager::CopiedUpdateEpoch preserves supported local epoch
    /// record ownership through the writer's failed-flush path, unlike a new
    /// primary control's preflight. No new lower LSN or counterfeit success.
    pub(crate) fn replay_copied_epoch(&self, record: Record) -> Result<Option<Arc<Activity>>> {
        let RecordKind::UpdateEpoch {
            epoch,
            primary_replica_id,
            timestamp_ticks,
        } = record.kind
        else {
            return Err(Error::InvalidInput("copied epoch requires UpdateEpoch"));
        };
        let mut log = self.log.lock().unwrap();
        if self.role() == ProcessingRole::Primary || record.lsn != log.tail {
            return Err(Error::InvalidInput(
                "copied epoch must be at secondary tail",
            ));
        }
        let memory_owner = self.memory();
        let mut memory = memory_owner.as_ref().map(|m| m.lock().unwrap());
        let key = super::copy_codec::epoch_key;
        let last_epoch = memory
            .as_ref()
            .and_then(|m| m.vector.last())
            .map_or(log.epoch, |v| v.epoch);
        let full_prefix = key(log.epoch) < key(last_epoch) && key(epoch) <= key(last_epoch);
        if full_prefix && record.lsn > self.covered_prefix.load(Ordering::Acquire) {
            return Err(Error::InvalidInput("copied PV prefix exceeds checkpoint"));
        }
        let insert = if full_prefix {
            None
        } else if let Some(memory) = &memory {
            if let Some(existing) = memory.vector.iter().find(|v| v.epoch == epoch) {
                if existing.lsn != record.lsn {
                    return Err(Error::InvalidInput("copied PV epoch has different LSN"));
                }
                return Ok(None);
            }
            let position = memory.vector.partition_point(|v| key(v.epoch) < key(epoch));
            if memory.vector[position..]
                .iter()
                .any(|v| v.lsn != record.lsn)
            {
                return Err(Error::InvalidInput("copied retry PV LSN differs"));
            }
            Some(position)
        } else {
            if epoch == log.epoch {
                return Ok(None);
            }
            Some(0)
        };
        if let Some(memory) = &mut memory {
            if insert.is_some() && memory.vector.len() >= 128 {
                return Err(Status::InsufficientResources.error());
            }
            if insert.is_some() {
                memory
                    .vector
                    .try_reserve(1)
                    .map_err(|_| Status::InsufficientResources.error())?;
            }
        }
        let node = RecordNode::new(record, None, None)?;
        if let Some(memory) = &memory {
            node.retain_memory(memory.reserve(node.record())?);
        }
        node.set_epoch(log.epoch);
        let activity = Activity::new(node, false);
        self.map.admit(activity.node.clone())?;
        if let Some(memory) = &mut memory {
            if let Err(error) = memory.append_logical(activity.node.clone()) {
                memory.fail_writer(error.clone());
                *activity.io_error.lock().unwrap() = Some(error);
            }
            if let Some(position) = insert {
                memory.vector.insert(
                    position,
                    super::memory_log::ProgressEntry {
                        epoch,
                        lsn: activity.node.lsn(),
                        primary: primary_replica_id,
                        ticks: timestamp_ticks,
                    },
                );
            }
        }
        if key(epoch) > key(log.epoch) {
            log.epoch = epoch;
        }
        log.epoch_lsn = activity.node.lsn();
        log.ordered.push(activity.clone());
        activity.replication.complete(Ok(()));
        self.log_changed.notify_waiters();
        Ok(Some(activity))
    }

    pub(crate) fn start_completion(&self, activity: Arc<Activity>) {
        if !activity.primary || activity.checkpoint_prepared.lock().unwrap().is_some() {
            return;
        }
        let manager = self.shared();
        drop(own(Box::pin(async move {
            let apply = activity.apply.wait().await;
            let replication = if activity.primary {
                activity.replication.wait().await
            } else {
                Arc::new(Ok(()))
            };
            let result = if activity.primary {
                status::accepted_result(apply.as_ref(), replication.as_ref())
            } else {
                apply.as_ref().clone()
            };
            if let RecordKind::Barrier { last_stable_lsn } = activity.node.record().kind {
                if result.is_ok() {
                    let stable = if activity.primary {
                        activity.node.lsn()
                    } else {
                        last_stable_lsn
                    };
                    let previous = manager.stable.fetch_max(stable, Ordering::AcqRel);
                    activity.previous_stable.store(previous, Ordering::Release);
                    manager.map.remove_stable(stable);
                    manager.log_changed.notify_waiters();
                }
                activity.processing.complete(Ok(()));
            } else if matches!(activity.node.record().kind, RecordKind::UpdateEpoch { .. }) {
                activity.processing.complete(Ok(()));
            } else {
                let _ = manager.unlock(&activity.node);
                activity.processing.complete(Ok(()));
            }
            activity
                .result
                .complete(result.map(|_| activity.node.lsn()));
            Ok(())
        })));
    }

    /// A successful flush buffers records until an explicit native boundary.
    /// Flush neither admits group-commit barriers nor completes quorum.
    pub(crate) fn flush(&self, result: Result<()>) {
        let launch = {
            let mut log = self.log.lock().expect("log poisoned");
            let batch = log.ordered[log.flushed..].to_vec();
            // Native terminal writer error affects this/later batches; earlier
            // already-completed successful flush/Apply is not retroactively failed.
            let failed = self
                .faults
                .log_error()
                .or_else(|| result.as_ref().err().cloned())
                .or_else(|| {
                    batch
                        .iter()
                        .find_map(|a| a.io_error.lock().unwrap().clone())
                });
            if let Some(error) = &failed {
                if let Some(memory) = self.memory() {
                    memory.lock().unwrap().fail_writer(error.clone());
                }
            } else {
                if let Some(memory) = self.memory() {
                    memory.lock().unwrap().flush();
                }
            }
            log.flushed = log.ordered.len();
            #[cfg(test)]
            if let Some((entered, release)) = self.flush_extracted.lock().unwrap().take() {
                let _ = entered.send(());
                release
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("flush probe released");
            }
            // Native flushed-batch order is one publication boundary. Do not
            // release log before enqueue: a later Barrier could otherwise
            // overtake this extracted prefix. No Apply/provider runs here.
            let mut dispatch = self.dispatch.lock().expect("dispatch poisoned");
            dispatch.flushed.extend(batch.into_iter().map(|activity| {
                let flushed = failed.clone().map_or(Ok(()), Err);
                // Native completes each actual owned record with its failed
                // writer status, then dispatches it (LoggingReplicatorImpl.cpp:
                // 1306–1337). First global flush result must not hide local I/O.
                if let Err(error) = &flushed {
                    self.faults.process_log_error(self.role(), error.clone());
                }
                activity.flush.complete(flushed.clone());
                (activity, flushed)
            }));
            let launch = !dispatch.running;
            dispatch.running = true;
            launch
        };
        self.log_changed.notify_waiters();
        if !launch {
            return;
        }
        let manager = self.shared();
        drop(own(Box::pin(async move {
            manager.run_dispatcher().await;
            Ok(())
        })));
    }

    fn next_dispatch(&self) -> Option<DispatchAction> {
        let mut dispatch = self.dispatch.lock().expect("dispatch poisoned");
        loop {
            let Some((activity, flush)) = dispatch.flushed.front().cloned() else {
                dispatch.running = false;
                return None;
            };
            if let Err(error) = flush {
                if !dispatch.pending.is_empty() {
                    // A failed batch releases the successfully flushed prefix,
                    // but does not publish a fabricated barrier/fence.
                    return Some(DispatchAction::Group(std::mem::take(&mut dispatch.pending)));
                }
                dispatch.flushed.pop_front();
                return Some(DispatchAction::Failed(activity, error));
            }
            if Self::is_boundary(&activity) {
                if !dispatch.pending.is_empty() {
                    self.versions
                        .update_dispatching_barrier(activity.apply.clone());
                    // Leave the boundary queued until its preceding group has
                    // finished ProcessLoggedRecord (Apply only on primary).
                    return Some(DispatchAction::Group(std::mem::take(&mut dispatch.pending)));
                }
                dispatch.flushed.pop_front();
                return Some(DispatchAction::Group(vec![activity]));
            }
            dispatch.flushed.pop_front();
            dispatch.pending.push(activity);
        }
    }

    fn is_boundary(activity: &Activity) -> bool {
        matches!(
            activity.node.record().kind,
            RecordKind::Barrier { .. }
                | RecordKind::UpdateEpoch { .. }
                | RecordKind::Physical {
                    native_type: 7 | 12,
                    ..
                }
        )
    }

    async fn run_dispatcher(&self) {
        loop {
            let _dispatch = self.dispatch_gate.read().await;
            let Some(action) = self.next_dispatch() else {
                break;
            };
            match action {
                DispatchAction::Failed(activity, error) => {
                    if activity.checkpoint_prepared.lock().unwrap().is_some() {
                        activity.checkpoint_entered.complete(Err(error));
                    } else {
                        self.process_immediately(&activity, Err(error), true);
                    }
                }
                DispatchAction::Group(group) => {
                    let mut transactions = BTreeMap::new();
                    let mut completions = Vec::new();
                    for activity in group {
                        if Self::is_boundary(&activity) {
                            // IdentifyProcessingMode, not fence publication,
                            // advances GetVersion through a completed group.
                            self.last_barrier
                                .store(activity.node.lsn(), Ordering::Release);
                            let prepared = activity.checkpoint_prepared.lock().unwrap().clone();
                            if let Some(prepared) = prepared {
                                activity
                                    .checkpoint_entered
                                    .complete(self.faults.service_error().map_or(Ok(()), Err));
                                let _ = prepared.wait().await;
                            } else {
                                self.process_immediately(
                                    &activity,
                                    self.faults.service_error().map_or(Ok(()), Err),
                                    false,
                                );
                            }
                        } else {
                            let id = activity
                                .node
                                .transaction_id()
                                .expect("ordinary transaction");
                            let previous = transactions.get(&id).cloned();
                            let completion = self.dispatch(activity, previous);
                            transactions.insert(id, completion.clone());
                            completions.push(completion);
                        }
                    }
                    for completion in completions {
                        let _ = completion.wait().await;
                    }
                }
            }
        }
    }

    fn process_immediately(&self, activity: &Activity, result: Result<()>, failed_flush: bool) {
        activity.apply.complete(result.clone());
        if !activity.primary {
            if result.is_ok() {
                if let RecordKind::Barrier { last_stable_lsn } = activity.node.record().kind {
                    self.stable.fetch_max(last_stable_lsn, Ordering::AcqRel);
                    self.map.remove_stable(last_stable_lsn);
                    self.log_changed.notify_waiters();
                }
            }
            activity
                .result
                .complete(result.map(|_| activity.node.lsn()));
        }
        // ProcessImmediately failed flush/epoch has no replay Unlock callback.
        if !activity.primary
            || failed_flush
            || matches!(activity.node.record().kind, RecordKind::UpdateEpoch { .. })
        {
            activity.processing.complete(Ok(()));
        }
    }

    fn dispatch(
        &self,
        activity: Arc<Activity>,
        previous: Option<Completion<()>>,
    ) -> Completion<()> {
        let completion = Completion::new();
        let dispatched = completion.clone();
        let manager = self.shared();
        drop(own(Box::pin(async move {
            if let Some(previous) = previous {
                let _ = previous.wait().await;
            }
            let result = if let Some(error) = manager.faults.service_error() {
                Err(error)
            } else {
                match manager.apply(&activity.node).await {
                    Err(error) => Err(manager.faults.process_service_error(manager.role(), error)),
                    success => success,
                }
            };
            activity.apply.complete(result.clone());
            if !activity.primary {
                let _ = manager.unlock(&activity.node);
                activity.processing.complete(Ok(()));
                activity
                    .result
                    .complete(result.map(|_| activity.node.lsn()));
            }
            dispatched.complete(Ok(()));
            Ok(())
        })));
        completion
    }

    async fn apply(&self, node: &Arc<RecordNode>) -> Result<()> {
        if let Some(error) = self.faults.service_error() {
            return Err(error);
        }
        let single = matches!(node.record().kind, RecordKind::Begin { single: true, .. });
        let committed = matches!(
            node.record().kind,
            RecordKind::End {
                committed: true,
                ..
            }
        );
        if (!single && !committed) || node.lsn() <= self.covered_prefix.load(Ordering::Acquire) {
            return Ok(());
        }
        let transaction = node.transaction().expect("operation has transaction");
        transaction.set_commit_sequence_number(node.lsn())?;
        let constituents = if single {
            vec![node.clone()]
        } else {
            node.constituents()?
        };
        for constituent in constituents {
            let transaction = constituent
                .transaction()
                .expect("constituent has transaction");
            if self.role() != ProcessingRole::Primary && !single {
                transaction.set_commit_sequence_number(node.lsn())?;
            } else if transaction.get_commit_sequence_number() != node.lsn() {
                return Err(Error::InvalidInput(
                    "primary transaction owner/commit LSN mismatch",
                ));
            }
            let (metadata, redo) = match &constituent.record().kind {
                RecordKind::Begin { metadata, redo, .. }
                | RecordKind::Operation { metadata, redo, .. } => (metadata.clone(), redo.clone()),
                _ => unreachable!("constituents exclude end"),
            };
            let context = crate::internal::IStateProviderManager::apply(
                self.directory.as_ref(),
                constituent.lsn(),
                transaction,
                self.role().redo(),
                metadata,
                redo,
            )
            .await?;
            constituent.replace_context(context);
        }
        Ok(())
    }

    fn unlock(&self, node: &Arc<RecordNode>) -> Result<()> {
        let single = matches!(node.record().kind, RecordKind::Begin { single: true, .. });
        let end = matches!(node.record().kind, RecordKind::End { .. });
        if !single && !end {
            return Ok(());
        }
        let result: Result<()> = (|| {
            if node.lsn() > self.covered_prefix.load(Ordering::Acquire) {
                let constituents = if single {
                    vec![node.clone()]
                } else {
                    node.constituents()?
                };
                for constituent in constituents {
                    if let Some(context) = constituent.take_context() {
                        crate::internal::IStateProviderManager::unlock(
                            self.directory.as_ref(),
                            context.as_ref(),
                        )?;
                    }
                }
            }
            if single
                || matches!(
                    node.record().kind,
                    RecordKind::End {
                        committed: true,
                        ..
                    }
                )
            {
                self.notifications
                    .lock()
                    .expect("notifications poisoned")
                    .push((
                        node.transaction_id().expect("transaction notification"),
                        node.lsn(),
                    ));
                let callback = self
                    .commit_callback
                    .lock()
                    .expect("commit callback poisoned")
                    .clone();
                if let Some(callback) = callback {
                    callback(node.transaction().expect("notification owns transaction"));
                }
            }
            Ok(())
        })();
        if let Err(error) = &result {
            self.faults
                .process_service_error(self.role(), error.clone());
        }
        result
    }

    pub(crate) fn replication(&self, lsn: SequenceNumber, result: Result<()>) {
        self.complete_replication(&self.activity(lsn), result);
    }

    pub(crate) fn complete_replication(&self, activity: &Arc<Activity>, result: Result<()>) {
        if let Err(error) = &result {
            self.faults.replication_error(error);
        }
        activity.replication.complete(result);
    }

    /// Demotion cleanup is operations-only, latest-backwards, status ignored.
    /// Original begins and all five map structures are deliberately retained.
    pub(crate) fn cleanup_pending_operations(&self) {
        for latest in self.map.latest_records() {
            let mut cursor = Some(latest);
            while let Some(node) = cursor {
                if matches!(node.record().kind, RecordKind::Operation { .. }) {
                    if let Some(context) = node.take_context() {
                        let _ = crate::internal::IStateProviderManager::unlock(
                            self.directory.as_ref(),
                            context.as_ref(),
                        );
                    }
                }
                cursor = node.parent();
            }
        }
    }
}

impl ITransactionVersionManager for KernelManager {
    fn register(&self) -> ApiFuture<'_, SequenceNumber> {
        if let Err(error) = self.gate() {
            return Box::pin(async move { Err(error) });
        }
        self.versions.register()
    }
    fn unregister(&self, version: SequenceNumber) -> Result<()> {
        self.gate()?;
        if let Some(error) = self
            .unregister_error
            .lock()
            .expect("unregister seam poisoned")
            .take()
        {
            return Err(error);
        }
        self.versions.unregister(version)
    }
}

impl IVersionManager for KernelManager {
    fn try_remove_checkpoint(
        &self,
        old: SequenceNumber,
        next: SequenceNumber,
    ) -> ApiFuture<'_, ()> {
        self.versions.try_remove_checkpoint(old, next)
    }
    fn try_remove_version(
        &self,
        provider: StateProviderId,
        commit: SequenceNumber,
        next: SequenceNumber,
    ) -> Result<TryRemoveVersionResult> {
        self.versions.try_remove_version(provider, commit, next)
    }
}

impl ITransactionManager for KernelManager {
    fn get_role(&self) -> ReplicaRole {
        *self.replica_role.lock().unwrap()
    }
    fn create_transaction(&self) -> Result<Arc<dyn Transaction>> {
        self.new_transaction()
            .map(|transaction| transaction as Arc<dyn Transaction>)
    }
    fn create_atomic_operation(&self) -> Result<Arc<dyn AtomicOperation>> {
        Err(Status::NotImplemented.error())
    }
    fn create_atomic_redo_operation(&self) -> Result<Arc<dyn AtomicRedoOperation>> {
        Err(Status::NotImplemented.error())
    }
    fn single_operation_transaction_abort_unlock(
        &self,
        provider: StateProviderId,
        context: &dyn OperationContext,
    ) -> Result<()> {
        let _activity = self.cleanup_entry()?;
        // TransactionalReplicator.cpp:626–635 masks inner cleanup failure.
        let _ = self
            .directory
            .single_operation_transaction_abort_unlock(provider, context);
        Ok(())
    }
    fn error_if_state_provider_is_not_registered(
        &self,
        provider: StateProviderId,
        id: TransactionId,
    ) -> Result<()> {
        self.writable()?;
        self.directory
            .error_if_state_provider_is_not_registered(provider, id)
    }
    fn begin_transaction(
        &self,
        transaction: &dyn Transaction,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        provider: StateProviderId,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()> {
        self.writable()?;
        self.directory
            .begin_transaction(transaction, provider, metadata, undo, redo, context)
    }
    fn begin_transaction_async(
        &self,
        transaction: Arc<dyn Transaction>,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        provider: StateProviderId,
        context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber> {
        if let Err(error) = self.writable() {
            return Box::pin(async move { Err(error) });
        }
        self.directory
            .begin_transaction_async(transaction, provider, metadata, undo, redo, context)
    }
    fn add_operation(
        &self,
        transaction: &dyn Transaction,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        provider: StateProviderId,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()> {
        self.writable()?;
        self.directory
            .add_operation(transaction, provider, metadata, undo, redo, context)
    }
    fn add_atomic_operation(
        &self,
        _: Arc<dyn AtomicOperation>,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: StateProviderId,
        _: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn add_atomic_redo_operation(
        &self,
        _: Arc<dyn AtomicRedoOperation>,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: StateProviderId,
        _: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn commit_transaction(
        &self,
        transaction: Arc<dyn Transaction>,
    ) -> ApiFuture<'_, SequenceNumber> {
        self.end(transaction, true)
    }
    fn abort_transaction(
        &self,
        transaction: Arc<dyn Transaction>,
    ) -> ApiFuture<'_, SequenceNumber> {
        self.end(transaction, false)
    }
}
