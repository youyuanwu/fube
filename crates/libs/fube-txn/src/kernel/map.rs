// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native append-time transaction chains and pending/stable bookkeeping.
//!
//! SF 51a7703 `loggingreplicator/TransactionMap.cpp:60–258,388–421` mutates
//! indexes at admission, not Apply/quorum or caller completion.
//! `logrecordlib/TransactionLogRecord.cpp:34–74` retains strong parents and
//! weak children. `TransactionManager.cpp:143–214` bounds nonenlisted chains
//! by the copied/recovered checkpoint seam.
//!
//! A0 rejects invalid identities, repeated admission and impossible chains before
//! mutation; these are checked-input exclusions, not new native rollback paths.
//! Stable cleanup deliberately retains the last stable end: native
//! `RemoveRange(0,i)` takes a count, not an inclusive endpoint.

use super::{
    record::{Record, RecordKind},
    status::Status,
};
use crate::{Error, OperationContext, Result, SequenceNumber, TransactionBase, TransactionId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};

/// Native record links plus admission-time enlisted classification.
#[derive(Default)]
struct Links {
    /// Strong ownership from later record to its predecessor.
    parent: Option<Arc<RecordNode>>,
    /// Nonowning forward traversal; end/log/map anchors retain the chain.
    child: Weak<RecordNode>,
    /// True only for a chain whose original begin was admitted.
    enlisted: bool,
}

/// Shared admitted-record node used by transaction map and operation processor.
///
/// The immutable record has the authoritative assigned/replayed LSN. Context
/// ownership is independent of map state and never silently erased on failure.
pub(crate) struct RecordNode {
    epoch: Mutex<crate::Epoch>,
    retention: Mutex<Option<Arc<super::memory_log::Retention>>>,
    /// Native logical fields; links and captures are not wire metadata.
    record: Record,
    /// Optional shared primary/replay transaction passed to provider Apply.
    transaction: Option<Arc<dyn TransactionBase>>,
    /// Strong-parent/weak-child native links.
    links: Mutex<Links>,
    /// Original or nonnull replacement capture, explicitly taken before Unlock.
    context: Mutex<Option<Arc<dyn OperationContext>>>,
    /// Checked-input guard against putting one node in two maps or twice.
    admitted: AtomicBool,
}

impl RecordNode {
    /// Create a shared native record with its optional transaction/capture owners.
    pub(crate) fn new(
        record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<Arc<Self>> {
        if let Some(transaction) = &transaction {
            if record.transaction_id() != Some(transaction.get_transaction_id()) {
                return Err(Error::InvalidInput("record/transaction identity mismatch"));
            }
        }
        if context.is_some()
            && !matches!(
                record.kind,
                RecordKind::Begin { .. } | RecordKind::Operation { .. }
            )
        {
            return Err(Error::InvalidInput("capture on nonoperation record"));
        }
        Ok(Arc::new(Self {
            epoch: Mutex::new(crate::Epoch {
                data_loss_number: 0,
                configuration_number: 0,
            }),
            retention: Mutex::new(None),
            record,
            transaction,
            links: Mutex::new(Links::default()),
            context: Mutex::new(context),
            admitted: AtomicBool::new(false),
        }))
    }

    /// Borrow immutable native fields.
    pub(crate) fn record(&self) -> &Record {
        &self.record
    }
    pub(crate) fn epoch(&self) -> crate::Epoch {
        *self.epoch.lock().unwrap()
    }
    pub(crate) fn set_epoch(&self, epoch: crate::Epoch) {
        *self.epoch.lock().unwrap() = epoch;
    }
    pub(crate) fn retain_memory(&self, retention: Arc<super::memory_log::Retention>) {
        *self.retention.lock().unwrap() = Some(retention);
    }

    /// Observe authoritative assigned/replayed operation LSN.
    pub(crate) fn lsn(&self) -> SequenceNumber {
        self.record.lsn
    }

    /// Observe native identity for transaction record kinds.
    pub(crate) fn transaction_id(&self) -> Option<TransactionId> {
        self.record.transaction_id()
    }

    /// Clone the retained transaction for actual provider Apply.
    pub(crate) fn transaction(&self) -> Option<Arc<dyn TransactionBase>> {
        self.transaction.clone()
    }

    /// Clone the strong parent anchor.
    pub(crate) fn parent(&self) -> Option<Arc<Self>> {
        self.links
            .lock()
            .expect("record links poisoned")
            .parent
            .clone()
    }

    /// Upgrade the weak forward link; it never owns the next record.
    pub(crate) fn child(&self) -> Option<Arc<Self>> {
        self.links
            .lock()
            .expect("record links poisoned")
            .child
            .upgrade()
    }

    /// Observe admission-time chain classification, not commit outcome.
    pub(crate) fn is_enlisted(&self) -> bool {
        self.links.lock().expect("record links poisoned").enlisted
    }

    /// Observe whether a native capture is retained without consuming it.
    pub(crate) fn context(&self) -> Option<Arc<dyn OperationContext>> {
        self.context
            .lock()
            .expect("record context poisoned")
            .clone()
    }

    /// Install only a nonnull successful Apply capture; null retains the original.
    pub(crate) fn replace_context(&self, returned: Option<Arc<dyn OperationContext>>) {
        if let Some(returned) = returned {
            *self.context.lock().expect("record context poisoned") = Some(returned);
        }
    }

    /// Reset/take the slot before invoking Unlock, including when Unlock fails.
    pub(crate) fn take_context(&self) -> Option<Arc<dyn OperationContext>> {
        self.context.lock().expect("record context poisoned").take()
    }

    /// Collect begin and operations in forward native transaction order.
    ///
    /// End is excluded. Covered orphan chains return an input error so processor
    /// covered-prefix paths can handle them explicitly, never invent a begin.
    pub(crate) fn constituents(self: &Arc<Self>) -> Result<Vec<Arc<Self>>> {
        let mut reverse = Vec::new();
        let mut current = if matches!(self.record.kind, RecordKind::End { .. }) {
            self.parent()
                .ok_or(Error::InvalidInput("covered end has no begin chain"))?
        } else {
            Arc::clone(self)
        };
        let id = current.transaction_id();
        let mut seen = BTreeSet::new();
        loop {
            if !seen.insert(Arc::as_ptr(&current) as usize) || current.transaction_id() != id {
                return Err(Error::InvalidInput("invalid native transaction chain"));
            }
            reverse.push(Arc::clone(&current));
            match &current.record.kind {
                RecordKind::Begin { .. } => {
                    if current.parent().is_some() {
                        return Err(Error::InvalidInput("begin has a parent"));
                    }
                    break;
                }
                RecordKind::Operation { .. } => {
                    let parent = current
                        .parent()
                        .ok_or(Error::InvalidInput("covered operation has no begin"))?;
                    if parent.lsn() >= current.lsn() {
                        return Err(Error::InvalidInput("nonincreasing transaction chain"));
                    }
                    current = parent;
                }
                _ => return Err(Error::InvalidInput("nonoperation in transaction chain")),
            }
        }
        reverse.reverse();
        Ok(reverse)
    }

    /// Mark admission once after all checked-input validation succeeds.
    fn mark_admitted(&self) -> Result<()> {
        self.admitted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| Error::InvalidInput("native record admitted twice"))
    }

    /// Link a later record under the map lock without owning the child strongly.
    fn link(self: &Arc<Self>, parent: Option<Arc<Self>>, enlisted: bool) {
        if let Some(parent) = &parent {
            parent.links.lock().expect("record links poisoned").child = Arc::downgrade(self);
        }
        let mut links = self.links.lock().expect("record links poisoned");
        links.parent = parent;
        links.enlisted = enlisted;
    }
}

impl Drop for RecordNode {
    fn drop(&mut self) {
        // Native TransactionLogRecord::FreeParentChainProactively prevents
        // stack overflow while preserving parents retained by another anchor.
        let mut parent = self
            .links
            .get_mut()
            .expect("record links poisoned")
            .parent
            .take();
        while let Some(anchor) = parent {
            match Arc::try_unwrap(anchor) {
                Ok(mut unique) => {
                    parent = unique
                        .links
                        .get_mut()
                        .expect("record links poisoned")
                        .parent
                        .take();
                    // unique's Drop sees no parent; destruction stays stack bounded.
                }
                Err(shared) => {
                    drop(shared);
                    break;
                }
            }
        }
    }
}

/// All five native transaction-map structures share this one lock.
#[derive(Default)]
struct MapState {
    /// Latest unfinished record for each ordinary transaction.
    latest: BTreeMap<TransactionId, Arc<RecordNode>>,
    /// Original multi-operation begin indexed by native identity.
    pending_by_id: BTreeMap<TransactionId, Arc<RecordNode>>,
    /// Original multi-operation begin indexed by assigned LSN.
    pending_by_lsn: BTreeMap<SequenceNumber, Arc<RecordNode>>,
    /// Completed multi begins sorted by begin LSN, for either decision.
    completed: Vec<Arc<RecordNode>>,
    /// Completed multi ends sorted by end LSN, for either decision.
    unstable: Vec<Arc<RecordNode>>,
}

/// Native append-time map, with explicit covered-checkpoint input boundary.
pub(crate) struct TransactionMap {
    /// All transaction indexes, not a partition-wide write lease.
    state: Mutex<MapState>,
    /// Bounded replay/copy seam; does not implement persisted recovery.
    covered_prefix: std::sync::atomic::AtomicI64,
}

/// Native earliest-pending result, including its distinct failed barrier check.
pub(crate) struct EarliestPending {
    /// Earliest begin strictly before the requested barrier, when valid.
    pub(crate) record: Option<Arc<RecordNode>>,
    /// Latest unstable end was above the requested barrier.
    pub(crate) failed_barrier_check: bool,
}

impl TransactionMap {
    pub(crate) fn set_covered(&self, lsn: i64) {
        self.covered_prefix.store(lsn, Ordering::Release);
    }
    /// Construct an empty map with an explicit covered checkpoint LSN.
    pub(crate) fn new(covered_prefix: SequenceNumber) -> Self {
        Self {
            state: Mutex::new(MapState::default()),
            covered_prefix: std::sync::atomic::AtomicI64::new(covered_prefix),
        }
    }

    /// Admit supported ordinary records and mutate map before async processing.
    ///
    /// Failed admission changes neither links nor indexes. No completion/error
    /// API removes a transaction; only stable cleanup and explicit lifecycle Reuse.
    pub(crate) fn admit(&self, node: Arc<RecordNode>) -> Result<()> {
        if node.lsn() < 0
            || (node.lsn() == 0
                && !matches!(
                    node.record.kind,
                    RecordKind::UpdateEpoch { .. } | RecordKind::Physical { .. }
                ))
        {
            return Err(Error::InvalidInput("nonpositive transaction-map LSN"));
        }
        if let Some(id) = node.transaction_id() {
            if id == 0 {
                return Err(Error::InvalidInput("zero transaction-map identity"));
            }
            if id < 0 {
                return Err(Status::NotImplemented.error());
            }
        }
        if matches!(
            node.record.kind,
            RecordKind::Operation {
                redo_only: true,
                ..
            }
        ) {
            return Err(Status::NotImplemented.error());
        }
        let mut map = self.state.lock().expect("transaction map poisoned");
        match &node.record.kind {
            RecordKind::Begin {
                transaction_id: id,
                single,
                ..
            } => {
                if map.latest.contains_key(id)
                    || map.pending_by_id.contains_key(id)
                    || map
                        .completed
                        .iter()
                        .chain(&map.unstable)
                        .any(|record| record.transaction_id() == Some(*id))
                    || (!*single && map.pending_by_lsn.contains_key(&node.lsn()))
                {
                    return Err(Error::InvalidInput("duplicate native begin"));
                }
                node.mark_admitted()?;
                node.link(None, true);
                if !*single {
                    map.latest.insert(*id, Arc::clone(&node));
                    map.pending_by_id.insert(*id, Arc::clone(&node));
                    map.pending_by_lsn.insert(node.lsn(), Arc::clone(&node));
                }
            }
            RecordKind::Operation {
                transaction_id: id, ..
            } => {
                let previous = map.latest.get(id).cloned();
                let enlisted = validate_previous(
                    &node,
                    previous.as_ref(),
                    self.covered_prefix.load(Ordering::Acquire),
                )?;
                node.mark_admitted()?;
                node.link(previous, enlisted);
                map.latest.insert(*id, Arc::clone(&node));
            }
            RecordKind::End {
                transaction_id: id, ..
            } => {
                let previous = map.latest.get(id).cloned();
                let enlisted = validate_previous(
                    &node,
                    previous.as_ref(),
                    self.covered_prefix.load(Ordering::Acquire),
                )?;
                let begin = map.pending_by_id.get(id).cloned();
                if enlisted != begin.is_some()
                    || begin.as_ref().is_some_and(|begin| {
                        !map.pending_by_lsn
                            .get(&begin.lsn())
                            .is_some_and(|indexed| Arc::ptr_eq(indexed, begin))
                    })
                {
                    return Err(Error::InvalidInput("inconsistent native pending chain"));
                }
                node.mark_admitted()?;
                node.link(previous, enlisted);
                map.latest.remove(id);
                if let Some(begin) = begin {
                    map.pending_by_id.remove(id);
                    map.pending_by_lsn.remove(&begin.lsn());
                    insert_sorted(&mut map.completed, begin);
                    insert_sorted(&mut map.unstable, Arc::clone(&node));
                }
            }
            RecordKind::Barrier { .. }
            | RecordKind::UpdateEpoch { .. }
            | RecordKind::Physical { .. } => {
                node.mark_admitted()?;
            }
        }
        Ok(())
    }

    /// Observe the latest pending chain anchor, including covered orphan chains.
    pub(crate) fn latest(&self, id: TransactionId) -> Option<Arc<RecordNode>> {
        self.state
            .lock()
            .expect("transaction map poisoned")
            .latest
            .get(&id)
            .cloned()
    }

    /// Clone latest anchors for native backwards demotion cleanup.
    pub(crate) fn latest_records(&self) -> Vec<Arc<RecordNode>> {
        self.state
            .lock()
            .expect("transaction map poisoned")
            .latest
            .values()
            .cloned()
            .collect()
    }

    /// Clone pending multi begins in ascending begin-LSN order.
    pub(crate) fn pending(&self) -> Vec<Arc<RecordNode>> {
        self.state
            .lock()
            .expect("transaction map poisoned")
            .pending_by_lsn
            .values()
            .cloned()
            .collect()
    }

    /// Clone completed multi begins, independently of caller completion status.
    pub(crate) fn completed(&self) -> Vec<Arc<RecordNode>> {
        self.state
            .lock()
            .expect("transaction map poisoned")
            .completed
            .clone()
    }

    /// Clone unstable multi ends, including the source retained-last-end quirk.
    pub(crate) fn unstable(&self) -> Vec<Arc<RecordNode>> {
        self.state
            .lock()
            .expect("transaction map poisoned")
            .unstable
            .clone()
    }

    /// Require all unstable ends at/below barrier, then select begin strictly below.
    pub(crate) fn earliest_pending(&self, barrier_lsn: SequenceNumber) -> EarliestPending {
        let map = self.state.lock().expect("transaction map poisoned");
        let failed_barrier_check = map
            .unstable
            .last()
            .is_some_and(|end| end.lsn() > barrier_lsn);
        let record = if failed_barrier_check {
            None
        } else {
            map.pending_by_lsn
                .first_key_value()
                .filter(|(lsn, _)| **lsn < barrier_lsn)
                .map(|(_, node)| Arc::clone(node))
        };
        EarliestPending {
            record,
            failed_barrier_check,
        }
    }

    /// Remove matching completed begins but retain the highest stable end.
    pub(crate) fn remove_stable(&self, stable_lsn: SequenceNumber) {
        let mut map = self.state.lock().expect("transaction map poisoned");
        if let Some(index) = map.unstable.iter().rposition(|end| end.lsn() <= stable_lsn) {
            let stable_ends: Vec<_> = map.unstable[..=index].to_vec();
            map.completed
                .retain(|begin| !stable_ends.iter().any(|end| same_transaction(begin, end)));
            if index > 0 {
                drop(map.unstable.drain(..index));
            }
        }
    }

    /// Native lifecycle Reuse only; never called to erase an ordinary failure.
    pub(crate) fn reuse(&self) {
        *self.state.lock().expect("transaction map poisoned") = MapState::default();
    }
}

/// Validate parent ordering and the covered-prefix gate before any mutation.
fn validate_previous(
    node: &RecordNode,
    previous: Option<&Arc<RecordNode>>,
    covered_prefix: SequenceNumber,
) -> Result<bool> {
    if let Some(previous) = previous {
        if previous.lsn() >= node.lsn()
            || previous.child().is_some()
            || !matches!(
                previous.record.kind,
                RecordKind::Begin { single: false, .. } | RecordKind::Operation { .. }
            )
        {
            return Err(Error::InvalidInput("invalid native chain predecessor"));
        }
    }
    let enlisted = previous.is_some_and(|previous| previous.is_enlisted());
    if !enlisted && node.lsn() > covered_prefix {
        return Err(Error::InvalidInput("orphan above covered checkpoint"));
    }
    Ok(enlisted)
}

/// Sorted native arrays admit interleaved begin/end completion orders.
fn insert_sorted(records: &mut Vec<Arc<RecordNode>>, node: Arc<RecordNode>) {
    let index = records.partition_point(|existing| existing.lsn() < node.lsn());
    records.insert(index, node);
}

/// Native equality includes creation-primary when transaction owners are present.
fn same_transaction(left: &RecordNode, right: &RecordNode) -> bool {
    left.transaction_id() == right.transaction_id()
        && match (&left.transaction, &right.transaction) {
            (Some(left), Some(right)) => {
                left.get_is_primary_transaction() == right.get_is_primary_transaction()
            }
            _ => true,
        }
}

#[cfg(test)]
mod tests {
    #[test]
    fn native_ten_thousand_operation_chain_releases_without_recursive_drop() {
        let map = TransactionMap::new(0);
        let begin = RecordNode::new(
            Record {
                lsn: 1,
                kind: RecordKind::Begin {
                    transaction_id: 77,
                    single: false,
                    metadata: None,
                    redo: None,
                    undo: None,
                },
            },
            None,
            None,
        )
        .unwrap();
        let weak_begin = Arc::downgrade(&begin);
        map.admit(begin).unwrap();
        for lsn in 2..=10_001 {
            map.admit(
                RecordNode::new(
                    Record {
                        lsn,
                        kind: RecordKind::Operation {
                            transaction_id: 77,
                            redo_only: false,
                            metadata: None,
                            redo: None,
                            undo: None,
                        },
                    },
                    None,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        }
        let end = RecordNode::new(
            Record {
                lsn: 10_002,
                kind: RecordKind::End {
                    transaction_id: 77,
                    committed: true,
                },
            },
            None,
            None,
        )
        .unwrap();
        map.admit(end.clone()).unwrap();
        map.reuse();
        assert!(weak_begin.upgrade().is_some());
        drop(end);
        assert!(weak_begin.upgrade().is_none());
    }
    use super::*;
    use crate::{LockContext, TransactionState};
    use std::sync::atomic::AtomicI64;

    /// Bookkeeping witness only; it does not claim transaction execution behavior.
    struct MapTransaction {
        id: i64,
        commit_lsn: AtomicI64,
    }

    impl TransactionBase for MapTransaction {
        fn get_is_primary_transaction(&self) -> bool {
            true
        }
        fn get_transaction_id(&self) -> i64 {
            self.id
        }
        fn get_is_atomic_operation(&self) -> bool {
            self.id < 0
        }
        fn get_is_valid_transaction(&self) -> bool {
            self.id != 0
        }
        fn get_state(&self) -> TransactionState {
            TransactionState::Active
        }
        fn get_commit_sequence_number(&self) -> i64 {
            self.commit_lsn.load(Ordering::Relaxed)
        }
        fn set_commit_sequence_number(&self, value: i64) -> Result<()> {
            if value <= 0
                || self
                    .commit_lsn
                    .compare_exchange(-1, value, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                return Err(Error::InvalidInput("invalid witness commit LSN"));
            }
            Ok(())
        }
        fn add_lock_context(&self, _: Arc<dyn LockContext>) -> Result<()> {
            Err(Status::NotImplemented.error())
        }
        fn dispose(&self) -> Result<()> {
            Err(Status::NotImplemented.error())
        }
    }

    /// Opaque retained capture for slot-ownership tests.
    struct Capture;
    impl OperationContext for Capture {}

    fn tx(id: i64) -> Arc<dyn TransactionBase> {
        Arc::new(MapTransaction {
            id,
            commit_lsn: AtomicI64::new(-1),
        })
    }

    fn begin(id: i64, lsn: i64, single: bool) -> Arc<RecordNode> {
        RecordNode::new(
            Record {
                lsn,
                kind: RecordKind::Begin {
                    transaction_id: id,
                    single,
                    metadata: None,
                    redo: None,
                    undo: None,
                },
            },
            Some(tx(id)),
            Some(Arc::new(Capture)),
        )
        .unwrap()
    }

    fn op(id: i64, lsn: i64) -> Arc<RecordNode> {
        RecordNode::new(
            Record {
                lsn,
                kind: RecordKind::Operation {
                    transaction_id: id,
                    redo_only: false,
                    metadata: None,
                    redo: None,
                    undo: None,
                },
            },
            Some(tx(id)),
            Some(Arc::new(Capture)),
        )
        .unwrap()
    }

    fn end(id: i64, lsn: i64, committed: bool) -> Arc<RecordNode> {
        RecordNode::new(
            Record {
                lsn,
                kind: RecordKind::End {
                    transaction_id: id,
                    committed,
                },
            },
            Some(tx(id)),
            None,
        )
        .unwrap()
    }

    fn lsns(nodes: &[Arc<RecordNode>]) -> Vec<i64> {
        nodes.iter().map(|node| node.lsn()).collect()
    }

    #[test]
    fn s03_interleaved_admission_completion_and_retained_last_stable_end() {
        let map = TransactionMap::new(0);
        let a = begin(1, 2, false);
        let b = begin(2, 3, false);
        map.admit(Arc::clone(&a)).unwrap();
        map.admit(Arc::clone(&b)).unwrap();
        let operation = op(1, 4);
        map.admit(Arc::clone(&operation)).unwrap();
        assert_eq!(lsns(&map.pending()), vec![2, 3]);
        assert_eq!(map.latest(1).unwrap().lsn(), 4);
        assert!(!map.earliest_pending(2).failed_barrier_check);
        assert!(map.earliest_pending(2).record.is_none());
        assert_eq!(map.earliest_pending(3).record.unwrap().lsn(), 2);
        let end_a = end(1, 6, true);
        let end_b = end(2, 8, false);
        map.admit(Arc::clone(&end_a)).unwrap();
        map.admit(Arc::clone(&end_b)).unwrap();
        assert!(map.pending().is_empty());
        assert!(map.latest(1).is_none());
        assert_eq!(lsns(&end_a.constituents().unwrap()), vec![2, 4]);
        assert!(Arc::ptr_eq(&a.child().unwrap(), &operation));
        assert!(Arc::ptr_eq(&operation.child().unwrap(), &end_a));
        assert_eq!(lsns(&map.completed()), vec![2, 3]);
        assert_eq!(lsns(&map.unstable()), vec![6, 8]);
        assert!(map.earliest_pending(7).failed_barrier_check);
        map.remove_stable(6);
        assert_eq!(lsns(&map.completed()), vec![3]);
        assert_eq!(lsns(&map.unstable()), vec![6, 8]);
        map.remove_stable(8);
        assert!(map.completed().is_empty());
        assert_eq!(lsns(&map.unstable()), vec![8]);
        map.remove_stable(9);
        assert_eq!(lsns(&map.unstable()), vec![8]);
    }

    #[test]
    fn end_admission_keeps_map_and_contexts_without_a_completion_success_gate() {
        let map = TransactionMap::new(0);
        let first = begin(1, 2, false);
        map.admit(Arc::clone(&first)).unwrap();
        assert!(map.admit(op(1, 2)).is_err());
        assert!(first.child().is_none());
        assert_eq!(map.latest(1).unwrap().lsn(), 2);
        let finish = end(1, 3, true);
        map.admit(finish).unwrap();
        // No Apply/quorum/caller success was requested to complete the map.
        assert!(map.pending().is_empty());
        assert_eq!(lsns(&map.completed()), vec![2]);
        assert_eq!(lsns(&map.unstable()), vec![3]);
        let original = first.context().unwrap();
        first.replace_context(None);
        assert!(Arc::ptr_eq(&first.context().unwrap(), &original));
        let replacement: Arc<dyn OperationContext> = Arc::new(Capture);
        first.replace_context(Some(Arc::clone(&replacement)));
        assert!(Arc::ptr_eq(&first.take_context().unwrap(), &replacement));
        assert!(first.take_context().is_none());
        assert_eq!(lsns(&map.completed()), vec![2]);
    }

    #[test]
    fn covered_orphan_gate_and_chain_edge_exclusions_are_pre_mutation() {
        let map = TransactionMap::new(5);
        let orphan = op(1, 4);
        map.admit(Arc::clone(&orphan)).unwrap();
        assert!(!orphan.is_enlisted());
        assert!(orphan.parent().is_none());
        assert!(map.admit(op(1, 6)).is_err());
        assert!(orphan.child().is_none());
        assert_eq!(map.latest(1).unwrap().lsn(), 4);
        let finish = end(1, 5, true);
        map.admit(Arc::clone(&finish)).unwrap();
        assert!(!finish.is_enlisted());
        assert!(finish.constituents().is_err());
        assert!(map.completed().is_empty());
        assert!(map.admit(end(2, 6, true)).is_err());
        assert!(map.admit(op(-1, 1)).is_err());
        let single = begin(3, 7, true);
        map.admit(Arc::clone(&single)).unwrap();
        assert!(single.is_enlisted());
        assert!(map.pending().is_empty());
        assert!(map.admit(Arc::clone(&single)).is_err());
        assert!(map.admit(end(3, 8, true)).is_err());
    }

    #[test]
    fn end_anchor_owns_parent_chain_but_begin_does_not_own_weak_child() {
        let map = TransactionMap::new(0);
        let first = begin(1, 2, false);
        let second = op(1, 3);
        let weak_second = Arc::downgrade(&second);
        let finish = end(1, 4, false);
        map.admit(Arc::clone(&first)).unwrap();
        map.admit(second).unwrap();
        map.admit(Arc::clone(&finish)).unwrap();
        map.reuse();
        assert_eq!(lsns(&finish.constituents().unwrap()), vec![2, 3]);
        drop(finish);
        assert!(weak_second.upgrade().is_none());
        assert!(first.child().is_none());
        assert!(first.context().is_some());
    }

    #[test]
    fn interleaved_end_order_sorts_completed_begins_independently() {
        let map = TransactionMap::new(0);
        map.admit(begin(1, 2, false)).unwrap();
        map.admit(begin(2, 3, false)).unwrap();
        map.admit(end(2, 6, true)).unwrap();
        assert_eq!(map.earliest_pending(7).record.unwrap().lsn(), 2);
        map.admit(end(1, 8, false)).unwrap();
        assert_eq!(lsns(&map.completed()), vec![2, 3]);
        assert_eq!(lsns(&map.unstable()), vec![6, 8]);
        map.remove_stable(6);
        assert_eq!(lsns(&map.completed()), vec![2]);
        assert_eq!(lsns(&map.unstable()), vec![6, 8]);
    }
}
