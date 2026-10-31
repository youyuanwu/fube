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
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex, Weak,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessingRole {
    Primary,
    Secondary,
    Recovery,
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

#[derive(Default)]
pub(crate) struct FaultState {
    service: Mutex<Option<Error>>,
    log: Mutex<Option<Error>>,
    reported: Mutex<Vec<Error>>,
    /// Per-record ProcessError/OnReplicationError, not partition notification.
    observed: Mutex<Vec<Error>>,
    closing: AtomicBool,
}

impl FaultState {
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
                .push(error);
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
            .push(error);
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
    fn replication_error(&self, error: &Error) {
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
    pub(crate) node: Arc<RecordNode>,
    pub(crate) flush: Completion<()>,
    pub(crate) apply: Completion<()>,
    pub(crate) replication: Completion<()>,
    pub(crate) processing: Completion<()>,
    pub(crate) result: Completion<SequenceNumber>,
    primary: bool,
}

impl Activity {
    fn new(node: Arc<RecordNode>, primary: bool) -> Arc<Self> {
        Arc::new(Self {
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

pub(crate) struct KernelManager {
    self_weak: Weak<Self>,
    pub(crate) lower: Arc<ControlledLower>,
    pub(crate) directory: Arc<ProviderDirectory>,
    pub(crate) map: TransactionMap,
    pub(crate) versions: VersionManager,
    pub(crate) faults: Arc<FaultState>,
    role: ProcessingRole,
    covered_prefix: SequenceNumber,
    log: Mutex<LogState>,
    dispatch: Mutex<DispatchState>,
    last_barrier: Arc<AtomicI64>,
    stable: AtomicI64,
    /// Weak capture bridge for borrowed SDK begin/add, not a request ledger or
    /// successful-outcome index. Record nodes retain their own shared owners.
    owners: Mutex<BTreeMap<TransactionId, Weak<dyn Transaction>>>,
    retry: Arc<RetryRuntime>,
    notifications: Mutex<Vec<(TransactionId, SequenceNumber)>>,
    commit_callback: Mutex<Option<CommitCallback>>,
    activity_open: AtomicBool,
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
            self_weak: self_weak.clone(),
            lower,
            directory: directory.clone(),
            map: TransactionMap::new(covered_prefix),
            versions: VersionManager::new(Arc::new(move || {
                Ok(version_sample.load(Ordering::Acquire))
            })),
            faults,
            role,
            covered_prefix,
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
            retry,
            notifications: Mutex::new(Vec::new()),
            activity_open: AtomicBool::new(true),
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
    pub(crate) fn gate(&self) -> Result<()> {
        if self.activity_open.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::ObjectClosed.error())
        }
    }
    pub(crate) fn close_activity_gate(&self) {
        self.activity_open.store(false, Ordering::Release);
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
            self.role == ProcessingRole::Primary,
            self.retry.clone(),
        )?;
        let owner: Arc<dyn Transaction> = transaction.clone();
        let mut owners = self.owners.lock().expect("transaction owners poisoned");
        owners.retain(|_, owner| owner.strong_count() > 0);
        if owners.len() >= 1024 {
            return Err(Status::InsufficientResources.error());
        }
        owners.insert(transaction.get_transaction_id(), Arc::downgrade(&owner));
        Ok(transaction)
    }
    fn retain_borrowed(&self, transaction: &dyn Transaction) -> Result<Arc<dyn Transaction>> {
        self.owners
            .lock()
            .expect("transaction owners poisoned")
            .get(&transaction.get_transaction_id())
            .and_then(Weak::upgrade)
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
        if self.role != ProcessingRole::Recovery {
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

    fn admit(
        &self,
        mut record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<Arc<Activity>> {
        self.gate()?;
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
            self.throttle
                .lock()
                .expect("throttle seam poisoned")
                .pop_front()
                .unwrap_or(Ok(()))?;
        }
        if self.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        if self.role != ProcessingRole::Primary {
            return Err(Status::NotPrimary.error());
        }
        record.lsn = -1;
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

    fn append(&self, activity: Arc<Activity>) -> Result<()> {
        let mut log = self.log.lock().expect("log poisoned");
        let epoch = matches!(activity.node.record().kind, RecordKind::UpdateEpoch { .. });
        let lsn = activity.node.lsn();
        if (epoch && lsn != log.tail) || (!epoch && lsn <= log.tail) || log.queue.contains_key(&lsn)
        {
            return Err(Error::InvalidInput("invalid native ordered append LSN"));
        }
        // Native map mutation precedes ordered append, including gap admission.
        self.map.admit(activity.node.clone())?;
        if epoch {
            log.ordered.push(activity);
        } else if lsn == log.tail + 1 {
            log.tail = lsn;
            log.ordered.push(activity);
            loop {
                let next = log.tail + 1;
                let Some(queued) = log.queue.remove(&next) else {
                    break;
                };
                log.tail = next;
                log.ordered.push(queued);
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
        Self::observe_admission(self.admit(
            Record {
                lsn: -1,
                kind: RecordKind::End {
                    transaction_id: id,
                    committed,
                },
            },
            Some(transaction),
            None,
        ))
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
        let mut log = self.log.lock().expect("log poisoned");
        if self.role == ProcessingRole::Primary && !log.queue.is_empty() {
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
            return Err(if self.role == ProcessingRole::Secondary {
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
        let activity = Activity::new(node, self.role == ProcessingRole::Primary);
        self.map.admit(activity.node.clone())?;
        log.ordered.push(activity.clone());
        log.epoch = epoch;
        log.epoch_lsn = previous_lsn;
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
            if manager.role == ProcessingRole::Primary {
                let _ = ordering.wait().await;
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
        if self.role == ProcessingRole::Primary {
            return Err(Error::InvalidInput("replay requires a nonprimary role"));
        }
        let mut record = Record::decode(&wire)?;
        if let Some(lsn) = sequence {
            record.lsn = lsn;
        }
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
        let transaction = record
            .transaction_id()
            .map(KernelTransaction::replay)
            .transpose()?
            .map(|transaction| transaction as Arc<dyn TransactionBase>);
        let node = RecordNode::new(record, transaction, None)?;
        let activity = Activity::new(node, false);
        self.append(activity.clone())?;
        activity.replication.complete(Ok(()));
        self.start_completion(activity.clone());
        Ok(activity)
    }

    fn start_completion(&self, activity: Arc<Activity>) {
        if !activity.primary {
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
                    manager.stable.fetch_max(stable, Ordering::AcqRel);
                    manager.map.remove_stable(stable);
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
        let batch = {
            let mut log = self.log.lock().expect("log poisoned");
            let batch = log.ordered[log.flushed..].to_vec();
            log.flushed = log.ordered.len();
            batch
        };
        for activity in &batch {
            activity.flush.complete(result.clone());
            if let Err(error) = &result {
                self.faults.process_log_error(self.role, error.clone());
            }
        }
        let mut dispatch = self.dispatch.lock().expect("dispatch poisoned");
        dispatch
            .flushed
            .extend(batch.into_iter().map(|activity| (activity, result.clone())));
        if dispatch.running {
            return;
        }
        dispatch.running = true;
        drop(dispatch);
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
            RecordKind::Barrier { .. } | RecordKind::UpdateEpoch { .. }
        )
    }

    async fn run_dispatcher(&self) {
        while let Some(action) = self.next_dispatch() {
            match action {
                DispatchAction::Failed(activity, error) => {
                    self.process_immediately(&activity, Err(error), true);
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
                            self.process_immediately(
                                &activity,
                                self.faults.service_error().map_or(Ok(()), Err),
                                false,
                            );
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
                    Err(error) => Err(manager.faults.process_service_error(manager.role, error)),
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
        if (!single && !committed) || node.lsn() <= self.covered_prefix {
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
            if self.role != ProcessingRole::Primary && !single {
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
                self.role.redo(),
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
            if node.lsn() > self.covered_prefix {
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
            self.faults.process_service_error(self.role, error.clone());
        }
        result
    }

    pub(crate) fn replication(&self, lsn: SequenceNumber, result: Result<()>) {
        if let Err(error) = &result {
            self.faults.replication_error(error);
        }
        self.activity(lsn).replication.complete(result);
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
        if self.role == ProcessingRole::Primary {
            ReplicaRole::Primary
        } else {
            ReplicaRole::IdleSecondary
        }
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
        self.gate()?;
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
        self.gate()?;
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
        self.gate()?;
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
        if let Err(error) = self.gate() {
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
        self.gate()?;
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
