//! Direct source fixtures and bounded-unit counterexamples, not Phase3 runtime
//! provider checkpoint/copy qualification.
use super::{
    map::{RecordNode, TransactionMap},
    memory_log::MemoryLog,
    record::{Record, RecordKind},
    version::{Completion, VersionManager},
};
use crate::{
    Epoch, ITransaction, ITransactionVersionManager, LockContext, OperationContext, TransactionBase,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

fn zero() -> Epoch {
    Epoch {
        data_loss_number: 0,
        configuration_number: 0,
    }
}
fn section_sizes(frame: &[u8]) -> Vec<usize> {
    let end = 4 + u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut sizes = Vec::new();
    while offset < end && sizes.len() < 6 {
        let size = u32::from_le_bytes(frame[offset..offset + 4].try_into().unwrap()) as usize;
        sizes.push(size);
        offset += size;
    }
    assert!(offset <= end);
    sizes
}

#[test]
fn native_transaction_physical_parent_sections_and_psns() {
    let mut log = MemoryLog::new(512 * 1024).unwrap();
    log.indexing(0, zero()).unwrap();
    let map = TransactionMap::new(0);
    let begin = RecordNode::new(
        Record {
            lsn: 1,
            kind: RecordKind::Begin {
                transaction_id: 100,
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
    let operation = RecordNode::new(
        Record {
            lsn: 2,
            kind: RecordKind::Operation {
                transaction_id: 100,
                redo_only: false,
                metadata: None,
                redo: None,
                undo: None,
            },
        },
        None,
        None,
    )
    .unwrap();
    let end = RecordNode::new(
        Record {
            lsn: 3,
            kind: RecordKind::End {
                transaction_id: 100,
                committed: true,
            },
        },
        None,
        None,
    )
    .unwrap();
    for node in [begin, operation, end] {
        map.admit(node.clone()).unwrap();
        log.append_logical(node).unwrap();
    }
    let entries: Vec<_> = log.entries.iter().collect();
    assert_eq!(
        entries
            .iter()
            .map(|e| (e.native_type, e.position, e.psn))
            .collect::<Vec<_>>(),
        vec![(9, 0, 1), (1, 76, 2), (2, 165, 3), (3, 254, 4)]
    );
    for (index, entry) in entries.iter().enumerate().skip(1) {
        let frame = log.read(entry.position, 100);
        let sizes = section_sizes(&frame);
        assert_eq!(&sizes[..6], &[16, 20, 4, 12, 12, 5]);
        let parent_offset = u64::from_le_bytes(frame[60..68].try_into().unwrap());
        assert_eq!(parent_offset, if index == 1 { 0 } else { 89 });
        let psn = i64::from_le_bytes(frame[24..32].try_into().unwrap());
        assert_eq!(psn, index as i64 + 1);
    }
}

#[test]
fn local_maintenance_batch_validates_head_identity_anchor_and_pins() {
    let mut log = MemoryLog::new(512 * 1024).unwrap();
    log.bootstrap().unwrap();
    let index = log.indexing(1, zero()).unwrap();
    // Indexing is not head movement: EndCheckpoint still describes original head.
    let reader = log.pin(0).unwrap();
    let checkpoint = log.begin_checkpoint(1, None).unwrap();
    log.end_checkpoint().unwrap();
    log.complete_checkpoint().unwrap();
    assert!(log.reclaim(index + 1).is_err());
    assert!(!log.reclaim(index).unwrap());
    assert_eq!(log.head, 0);
    log.unpin(reader);
    assert!(log.reclaim(index).unwrap());
    assert_eq!(log.head, index);
    assert!(index < checkpoint.reader_position);
    let truncation = log.entries.back().unwrap();
    assert_eq!(truncation.native_type, 10);
    let frame = log.read(truncation.position, 150);
    // Common16 + physical20 + linked12 then head metadata44.
    let head_lsn = i64::from_le_bytes(frame[72..80].try_into().unwrap());
    let head_psn = i64::from_le_bytes(frame[80..88].try_into().unwrap());
    let head_offset = u64::from_le_bytes(frame[88..96].try_into().unwrap());
    assert_eq!(
        (head_lsn, head_psn, head_offset),
        (1, 8, truncation.position - index)
    );
}

#[test]
fn checkpoint_rejects_invalid_anchors_and_stage_order_without_mutation() {
    let mut log = MemoryLog::new(512 * 1024).unwrap();
    log.bootstrap().unwrap();
    let snapshot = |log: &MemoryLog| {
        (
            log.tail_position,
            log.entries.back().unwrap().psn,
            log.events.len(),
            log.pending.as_ref().map(|cp| cp.begin_position),
            log.completed.as_ref().map(|cp| cp.begin_position),
        )
    };
    let initial = snapshot(&log);
    assert!(log.complete_checkpoint().is_err());
    assert!(log.end_checkpoint().is_err());
    assert_eq!(snapshot(&log), initial);
    let begin = RecordNode::new(
        Record {
            lsn: 1,
            kind: RecordKind::Begin {
                transaction_id: 200,
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
    assert!(
        log.begin_checkpoint(1, Some(&begin)).is_err(),
        "unretained Some is not None"
    );
    assert_eq!(snapshot(&log), initial);
    let map = TransactionMap::new(0);
    let retained = RecordNode::new(
        Record {
            lsn: 2,
            ..begin.record().clone()
        },
        None,
        None,
    )
    .unwrap();
    map.admit(retained.clone()).unwrap();
    log.append_logical(retained.clone()).unwrap();
    let operation = RecordNode::new(
        Record {
            lsn: 3,
            kind: RecordKind::Operation {
                transaction_id: 200,
                redo_only: false,
                metadata: None,
                redo: None,
                undo: None,
            },
        },
        None,
        None,
    )
    .unwrap();
    map.admit(operation.clone()).unwrap();
    log.append_logical(operation.clone()).unwrap();
    let index = log.indexing(3, zero()).unwrap();
    let before = snapshot(&log);
    assert!(
        log.begin_checkpoint(3, Some(&operation)).is_err(),
        "Operation is not a typed earliest Begin"
    );
    assert!(
        log.begin_checkpoint(1, Some(&retained)).is_err(),
        "checkpoint must cover current physical logical tail"
    );
    assert_eq!(snapshot(&log), before);
    let cp = log.begin_checkpoint(3, Some(&retained)).unwrap();
    assert!(cp.reader_position < index);
    let begun = snapshot(&log);
    assert!(
        log.complete_checkpoint().is_err(),
        "Complete cannot skip End"
    );
    assert_eq!(snapshot(&log), begun);
    log.end_checkpoint().unwrap();
    let ended = snapshot(&log);
    assert!(log.end_checkpoint().is_err());
    assert_eq!(snapshot(&log), ended);
    log.complete_checkpoint().unwrap();
    assert!(
        !log.reclaim(index).unwrap(),
        "actual retained Begin remains protected"
    );
    let completed = snapshot(&log);
    assert!(log.complete_checkpoint().is_err());
    assert_eq!(
        snapshot(&log),
        completed,
        "rejection preserves completed anchor"
    );
}

#[test]
fn native_complete_checkpoint_advances_previous_and_linked_physical_chains() {
    let mut log = MemoryLog::new(512 * 1024).unwrap();
    log.bootstrap().unwrap();
    assert_eq!(
        log.entries
            .iter()
            .filter(|e| matches!(e.native_type, 8 | 13 | 12))
            .map(|e| (e.native_type, e.position))
            .collect::<Vec<_>>(),
        vec![(8, 412), (13, 532), (12, 636)]
    );
    log.indexing(1, zero()).unwrap();
    log.begin_checkpoint(1, None).unwrap();
    log.end_checkpoint().unwrap();
    log.complete_checkpoint().unwrap();
    log.information(1).unwrap();
    let mut previous = None;
    let mut linked = None;
    for entry in &log.entries {
        if !matches!(entry.native_type, 9 | 7 | 8 | 13 | 12) {
            continue;
        }
        let frame = log.read(entry.position, 150);
        let previous_offset = u64::from_le_bytes(frame[32..40].try_into().unwrap());
        let linked_offset = u64::from_le_bytes(frame[44..52].try_into().unwrap());
        assert_eq!(previous_offset, previous.map_or(0, |p| entry.position - p));
        assert_eq!(linked_offset, linked.map_or(0, |p| entry.position - p));
        previous = Some(entry.position);
        if matches!(entry.native_type, 8 | 13) {
            linked = Some(entry.position);
        }
    }
}

#[tokio::test]
async fn pending_snapshot_quota_is_owned_before_spawn_and_survives_observer_drop() {
    let sampled = Arc::new(AtomicUsize::new(0));
    let sample = sampled.clone();
    let versions = VersionManager::new(Arc::new(move || {
        sample.fetch_add(1, Ordering::AcqRel);
        Ok(10)
    }));
    versions.set_limits(2, 16);
    let gate = Completion::new();
    versions.update_dispatching_barrier(gate.clone());
    drop(versions.register());
    drop(versions.register());
    assert!(versions.register().await.is_err());
    assert_eq!(sampled.load(Ordering::Acquire), 0);
    gate.complete(Ok(()));
    tokio::time::timeout(Duration::from_secs(1), async {
        while versions.registered_versions().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sampled.load(Ordering::Acquire), 2);
    versions.unregister(10).unwrap();
    versions.unregister(10).unwrap();
    assert_eq!(versions.register().await.unwrap(), 10);
    versions.unregister(10).unwrap();
}

#[test]
fn fidelity_memory_allocation_retires_only_after_final_retained_record_release() {
    let mut log = MemoryLog::new(512 * 1024).unwrap();
    log.bootstrap().unwrap();
    let wire = Record {
        lsn: -1,
        kind: RecordKind::Barrier { last_stable_lsn: 0 },
    };
    let node = RecordNode::new(
        Record {
            lsn: 2,
            ..wire.clone()
        },
        None,
        None,
    )
    .unwrap();
    node.retain_memory(log.reserve(&wire).unwrap());
    log.append_logical(node.clone()).unwrap();
    log.flush();
    let pin = log.pin(0).unwrap();
    let index = log.indexing(2, zero()).unwrap();
    log.begin_checkpoint(2, None).unwrap();
    log.end_checkpoint().unwrap();
    log.complete_checkpoint().unwrap();
    assert!(!log.reclaim(index).unwrap());
    log.unpin(pin);
    assert!(log.reclaim(index).unwrap());
    let before = log.allocated_bytes();
    drop(node);
    assert!(
        log.allocated_bytes() < before,
        "external record observer owned its allocation until final Drop"
    );
    let reused = RecordNode::new(Record { lsn: 3, ..wire }, None, None).unwrap();
    reused.retain_memory(log.reserve(reused.record()).unwrap());
    log.append_logical(reused).unwrap();
    log.flush();
    assert_eq!(log.flushed_lsn, 3);
}

struct Lock {
    count: AtomicUsize,
    fail: AtomicBool,
    tracking: bool,
}
impl OperationContext for Lock {}
impl LockContext for Lock {
    fn is_tracking_context(&self) -> bool {
        self.tracking
    }
    fn unlock(&self) -> crate::Result<()> {
        self.count.fetch_add(1, Ordering::AcqRel);
        if self.fail.load(Ordering::Acquire) {
            Err(crate::Error::InvalidInput("lock fixture fault"))
        } else {
            Ok(())
        }
    }
}
fn lock(tracking: bool) -> Arc<Lock> {
    Arc::new(Lock {
        count: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
        tracking,
    })
}

#[tokio::test]
async fn dispose_retains_native_list_and_retries_but_retires_successful_work_permits() {
    use super::{
        processor::{ControlledLower, KernelManager, ProcessingRole},
        provider::ProviderDirectory,
        transaction::RetryRuntime,
    };
    let manager = KernelManager::established(
        ProviderDirectory::preinitialized([], None).unwrap(),
        Arc::new(ControlledLower::new(2)),
        ProcessingRole::Primary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let limits = super::limits::Limits::new();
    let a = manager.new_transaction().unwrap();
    let b = manager.new_transaction().unwrap();
    a.bind_limits(limits.clone()).unwrap();
    b.bind_limits(limits.clone()).unwrap();
    a.commit().await.unwrap();
    b.commit().await.unwrap();
    let first = lock(false);
    let failed = lock(false);
    failed.fail.store(true, Ordering::Release);
    a.add_lock_context(first.clone()).unwrap();
    a.add_lock_context(failed.clone()).unwrap();
    for _ in 2..64 {
        a.add_lock_context(lock(false)).unwrap();
    }
    assert!(a.dispose().is_err());
    assert!(!a.is_disposed());
    b.add_lock_context(lock(false)).unwrap(); // exactly the first retired slot
    assert!(b.add_lock_context(lock(false)).is_err());
    b.dispose().unwrap();
    failed.fail.store(false, Ordering::Release);
    a.dispose().unwrap();
    assert_eq!(
        first.count.load(Ordering::Acquire),
        2,
        "native retained list retries earlier Unlock"
    );
    assert_eq!(failed.count.load(Ordering::Acquire), 2);
    a.dispose().unwrap();
    assert_eq!(
        first.count.load(Ordering::Acquire),
        2,
        "disposed early return is unchanged"
    );
    assert!(
        a.add_lock_context(lock(false)).is_err(),
        "retained list remains bounded and uncleared"
    );
    let c = manager.new_transaction().unwrap();
    c.bind_limits(limits).unwrap();
    c.commit().await.unwrap();
    let tracking = lock(true);
    for _ in 0..64 {
        c.add_lock_context(tracking.clone()).unwrap();
    }
    c.dispose().unwrap();
    assert_eq!(tracking.count.load(Ordering::Acquire), 0);
}

#[test]
fn fidelity_uniform_allocation_has_no_control_preference_or_synthetic_information_on_failure() {
    use super::{processor::KernelManager, provider::ProviderDirectory, transaction::RetryRuntime};
    let execution = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entry = execution.enter();
    let manager = KernelManager::fresh(
        ProviderDirectory::preinitialized([], None).unwrap(),
        Arc::new(RetryRuntime::default()),
    );
    manager.open_memory(512 * 1024).unwrap();
    let memory = manager.memory().unwrap();
    let ordinary = Record {
        lsn: -1,
        kind: RecordKind::Barrier { last_stable_lsn: 0 },
    };
    let mut owners = Vec::new();
    {
        let log = memory.lock().unwrap();
        while let Ok(retained) = log.reserve(&ordinary) {
            owners.push(retained);
        }
    }
    let before = {
        let log = memory.lock().unwrap();
        (
            log.tail_position,
            log.events.clone(),
            manager.records().len(),
        )
    };
    for kind in [
        RecordKind::End {
            transaction_id: 300,
            committed: true,
        },
        RecordKind::Barrier { last_stable_lsn: 0 },
        RecordKind::UpdateEpoch {
            epoch: zero(),
            primary_replica_id: 0,
            timestamp_ticks: 0,
        },
    ] {
        let error = memory
            .lock()
            .unwrap()
            .reserve(&Record { lsn: -1, kind })
            .err()
            .unwrap();
        assert!(super::status::Status::InsufficientResources.matches(&error));
        assert!(!super::status::retryable(&error));
    }
    // Exhaust the REAL allocator remainder without fault injection. A restored
    // preferential physical pool would make this counterexample fail.
    let remainder = memory.lock().unwrap().occupy_remaining_allocation();
    let rejected = manager.local_boundary(12, 4).err().unwrap();
    assert!(super::status::Status::InsufficientResources.matches(&rejected));
    let log = memory.lock().unwrap();
    assert_eq!(
        (
            log.tail_position,
            log.events.clone(),
            manager.records().len()
        ),
        before
    );
    drop(log);
    drop(remainder);
    drop(owners);
    assert!(memory.lock().unwrap().reserve(&ordinary).is_ok());
}

#[test]
fn epoch_pressure_rejects_before_log_map_pv_or_physical_publication() {
    use super::{processor::KernelManager, provider::ProviderDirectory, transaction::RetryRuntime};
    let manager = KernelManager::fresh(
        ProviderDirectory::preinitialized([], None).unwrap(),
        Arc::new(RetryRuntime::default()),
    );
    // open_memory starts owned processing on a Tokio executor; local fixture owns it.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    manager.open_memory(512 * 1024).unwrap();
    for number in 1..=128 {
        let before_records = manager.records().len();
        let memory = manager.memory().unwrap();
        let before = {
            let memory = memory.lock().unwrap();
            (
                memory.tail_position,
                memory.vector.clone(),
                memory.events.clone(),
            )
        };
        let epoch = Epoch {
            data_loss_number: 0,
            configuration_number: number,
        };
        if manager.update_epoch(epoch, 1, 1, 0).is_err() {
            assert_eq!(manager.records().len(), before_records);
            let memory = memory.lock().unwrap();
            assert_eq!(
                (
                    memory.tail_position,
                    memory.vector.clone(),
                    memory.events.clone()
                ),
                before
            );
            drop(memory);
            assert!(
                manager.update_epoch(epoch, 1, 1, 0).is_err(),
                "rejection cannot become duplicate success"
            );
            let memory = manager.memory().unwrap();
            let mut memory = memory.lock().unwrap();
            memory.indexing(1, epoch).unwrap(); // ordinary allocation, no protected pool
            return;
        }
        manager.flush(Ok(()));
    }
    panic!("finite Rust progress-vector bound must reject without partial publication");
}
