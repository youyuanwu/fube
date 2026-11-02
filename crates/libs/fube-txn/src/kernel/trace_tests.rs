// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Source-derived deterministic traces at SF 51a7703, not native executions.
//! T01–T21: Transaction.cpp:238–485, TransactionManager.cpp:459–527,
//! OperationProcessor.cpp:366–616,657–935; native processor test projections
//! OperationProcessor.Test.cpp:226–370,412–510. S02/S04, V01/V03, W02/W03
//! cite their specific branches below. Assertions distinguish caller, log/map,
//! provider/capture and version/fault observations rather than final-map equality.

use super::{
    map::RecordNode,
    named,
    processor::{ControlledLower, KernelManager, ProcessingRole},
    provider::{
        ApplyPlan, CaptureResult, NamedContext, PointContext, PointEvent, PointProvider,
        ProviderDirectory, ProviderMode, RegistryCallbacks, REGISTRY_ID,
    },
    record::{Record, RecordKind},
    status::Status,
    transaction::{KernelTransaction, RetryRuntime},
    version::Completion,
};
use crate::{
    internal::{IStateProviderManager, ITransactionStateManager},
    ApiFuture, ApplyContext, Cancellation, Error, IStateProvider2, ITransaction,
    ITransactionManager, ITransactionVersionManager, IVersionManager, LockContext,
    OperationContext, OperationData, Result, Transaction, TransactionBase, TransactionState,
};
use std::{
    any::Any,
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

type ApplyObservation = (i64, i64, Option<OperationData>, Option<OperationData>);

fn fixture_with(
    role: ProcessingRole,
    covered: i64,
    retry: Arc<RetryRuntime>,
) -> (Arc<KernelManager>, Arc<PointProvider>) {
    let point = PointProvider::preinitialized("fabric:/point");
    let provider: Arc<dyn IStateProvider2> = point.clone();
    let directory = ProviderDirectory::preinitialized([(7, provider)], None).unwrap();
    (
        KernelManager::established(
            directory,
            Arc::new(ControlledLower::new(2)),
            role,
            covered,
            retry,
        ),
        point,
    )
}
fn fixture() -> (Arc<KernelManager>, Arc<PointProvider>) {
    fixture_with(
        ProcessingRole::Primary,
        0,
        Arc::new(RetryRuntime::default()),
    )
}
fn lsns(nodes: Vec<Arc<RecordNode>>) -> Vec<i64> {
    nodes.iter().map(|node| node.lsn()).collect()
}
fn log(manager: &KernelManager) -> Vec<(char, i64, Option<i64>)> {
    manager
        .records()
        .iter()
        .map(|activity| {
            let record = activity.node.record();
            let kind = match record.kind {
                RecordKind::Begin { single: true, .. } => 'S',
                RecordKind::Begin { single: false, .. } => 'B',
                RecordKind::Operation { .. } => 'O',
                RecordKind::End {
                    committed: true, ..
                } => 'C',
                RecordKind::End {
                    committed: false, ..
                } => 'A',
                RecordKind::Barrier { .. } => '|',
                RecordKind::UpdateEpoch { .. } => 'E',
                RecordKind::Physical { .. } => 'P',
            };
            (kind, record.lsn, record.transaction_id())
        })
        .collect()
}
fn applies(point: &PointProvider) -> Vec<(i64, i64, i64)> {
    point
        .events()
        .into_iter()
        .filter_map(|event| match event {
            PointEvent::Apply {
                operation,
                commit,
                transaction,
                ..
            } => Some((operation, commit, transaction)),
            _ => None,
        })
        .collect()
}
fn unlocks(point: &PointProvider) -> Vec<u64> {
    point
        .events()
        .into_iter()
        .filter_map(|event| match event {
            PointEvent::Unlock { capture, .. } => Some(capture),
            _ => None,
        })
        .collect()
}
fn point_capture(context: Arc<dyn OperationContext>) -> u64 {
    let context = (context.as_ref() as &dyn Any)
        .downcast_ref::<NamedContext>()
        .unwrap();
    (context.inner.as_ref() as &dyn Any)
        .downcast_ref::<PointContext>()
        .unwrap()
        .capture
}
fn code<T>(result: Result<T>, status: Status) {
    assert_eq!(
        result.err().expect("expected failure").native_code(),
        Some(status.code())
    );
}
async fn observe<T>(future: ApiFuture<'_, T>) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("bounded source trace did not complete")
}
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
fn group_tick(manager: &KernelManager) {
    if manager.get_role() == crate::ReplicaRole::Primary {
        manager.barrier(0).unwrap();
    } else {
        manager
            .replay_record(
                replay_wire(
                    RecordKind::Barrier { last_stable_lsn: 0 },
                    manager.tail() + 1,
                ),
                None,
            )
            .unwrap();
    }
}
async fn finish(manager: &KernelManager) {
    manager.flush(Ok(()));
    for activity in manager.records() {
        let _ = activity.apply.wait().await;
        activity.replication.complete(Ok(()));
    }
    for activity in manager.records() {
        let _ = activity.processing.wait().await;
    }
    settle().await;
}
fn add(
    point: &PointProvider,
    transaction: &KernelTransaction,
    key: i64,
    value: i64,
) -> Arc<PointContext> {
    point.write(transaction, 7, key, value).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn t01_read_only_commit_has_no_lsn_record_or_operation_unlock() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert_eq!(
        observe(transaction.get_visibility_sequence_number())
            .await
            .unwrap(),
        1
    );
    assert_eq!(manager.versions.registered_versions(), vec![1]);
    let observer = transaction.commit();
    // Native eager no-suspension read-only path has already committed.
    assert_eq!(transaction.get_state(), TransactionState::Committed);
    observe(observer).await.unwrap();
    assert_eq!(transaction.get_commit_sequence_number(), -1);
    assert!(log(&manager).is_empty());
    assert!(manager.map.pending().is_empty());
    assert!(point.events().is_empty());
    assert_eq!(manager.versions.registered_versions(), vec![1]); // Commit is not Dispose.
    transaction.dispose().unwrap();
    assert!(manager.versions.registered_versions().is_empty());
    assert!(transaction.is_disposed());
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t02_first_operation_validates_and_caches_without_admission() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let context = add(&point, &transaction, 10, 100);
    assert!(transaction.is_write() && transaction.has_cached_first());
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert_eq!(transaction.get_commit_sequence_number(), -1);
    assert!(manager.lower.attempts().is_empty());
    assert!(log(&manager).is_empty() && manager.map.pending().is_empty());
    assert!(point.events().is_empty() && point.versions(10).is_empty());
    assert!(point.is_locked(10));
    assert_eq!(
        point.snapshot(transaction.as_ref(), 10, 1).unwrap(),
        Some(100)
    ); // Own write set.
    transaction.abort().unwrap();
    assert_eq!(unlocks(&point), vec![context.capture]);
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t03_single_apply_precedes_quorum_and_snapshot_has_no_release_gate() {
    let (manager, point) = fixture();
    let writer = manager.new_transaction().unwrap();
    let original = add(&point, &writer, 10, 100);
    let old_snapshot = manager.new_transaction().unwrap();
    assert_eq!(
        observe(old_snapshot.get_visibility_sequence_number())
            .await
            .unwrap(),
        1
    );
    let observer = writer.commit();
    let id = writer.get_transaction_id();
    assert_eq!(log(&manager), vec![('S', 2, Some(id))]);
    assert_eq!(writer.get_state(), TransactionState::Committing);
    assert!(writer.has_cached_first());
    assert!(manager.map.pending().is_empty());
    let barrier = manager.barrier(0).unwrap();
    assert_eq!(barrier.node.lsn(), 3);
    manager.flush(Ok(()));
    assert!(manager.activity(2).apply.wait().await.is_ok());
    assert!(barrier.apply.wait().await.is_ok());
    assert_eq!(applies(&point), vec![(2, 2, id)]);
    assert_eq!(writer.get_commit_sequence_number(), 2);
    assert_eq!(point.versions(10)[0].commit, 2);
    let replacement = point_capture(manager.activity(2).node.context().unwrap());
    assert_ne!(replacement, original.capture);
    assert!(unlocks(&point).is_empty() && point.is_locked(10));
    assert!(!manager.activity(2).result.is_completed());
    assert_eq!(manager.stable(), 0);
    assert_eq!(manager.tail(), 3);
    assert_eq!(manager.version(), 3);
    assert_eq!(point.snapshot(old_snapshot.as_ref(), 10, 1).unwrap(), None);
    let snapshot = manager.new_transaction().unwrap();
    let vsn = observe(snapshot.get_visibility_sequence_number())
        .await
        .unwrap();
    assert_eq!(vsn, 3);
    assert_eq!(
        point.snapshot(snapshot.as_ref(), 10, vsn).unwrap(),
        Some(100)
    );
    let repeatable = manager.new_transaction().unwrap();
    code(
        point.try_repeatable(repeatable.as_ref(), 10),
        Status::ServiceTooBusy,
    );
    let wait = point.repeatable(repeatable.clone(), 10);
    settle().await;
    assert!(point.is_locked(10));
    manager.replication(2, Ok(()));
    observe(observer).await.unwrap();
    assert_eq!(writer.get_state(), TransactionState::Committed);
    assert_eq!(unlocks(&point), vec![replacement]);
    assert_eq!(manager.notifications(), vec![(id, 2)]);
    assert_eq!(observe(wait).await.unwrap(), Some(100));
    assert_eq!(manager.stable(), 0); // transaction quorum is not barrier quorum.
    manager.replication(3, Ok(()));
    barrier.processing.wait().await.as_ref().clone().unwrap();
    assert_eq!(manager.stable(), 3);
    for transaction in [writer, snapshot, old_snapshot, repeatable] {
        transaction.dispose().unwrap();
    }
    assert!(!point.is_locked(10));
}

#[tokio::test(flavor = "current_thread")]
async fn t04_multi_incremental_map_mutates_at_end_admission_before_apply() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let a = add(&point, &transaction, 10, 100);
    let b = add(&point, &transaction, 11, 110);
    let id = transaction.get_transaction_id();
    assert_eq!(log(&manager), vec![('B', 2, Some(id)), ('O', 3, Some(id))]);
    assert!(!transaction.has_cached_first());
    assert_eq!(lsns(manager.map.pending()), vec![2]);
    assert_eq!(manager.map.latest(id).unwrap().lsn(), 3);
    manager.flush(Ok(()));
    assert!(!manager.activity(2).apply.is_completed());
    assert!(!manager.activity(3).apply.is_completed());
    assert!(point.events().is_empty());
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        a.capture
    );
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        b.capture
    );
    let observer = transaction.commit();
    assert_eq!(log(&manager)[2], ('C', 4, Some(id)));
    assert!(manager.map.pending().is_empty() && manager.map.latest(id).is_none());
    assert_eq!(lsns(manager.map.completed()), vec![2]);
    assert_eq!(lsns(manager.map.unstable()), vec![4]);
    assert_eq!(transaction.get_commit_sequence_number(), -1);
    let chain = manager.activity(4).node.constituents().unwrap();
    assert_eq!(lsns(chain), vec![2, 3]);
    manager.flush(Ok(()));
    assert!(!manager.activity(4).apply.is_completed());
    assert!(!manager.activity(4).result.is_completed());
    assert!(point.events().is_empty());
    manager.barrier(0).unwrap();
    manager.flush(Ok(()));
    assert!(manager.activity(4).apply.wait().await.is_ok());
    assert_eq!(applies(&point), vec![(2, 4, id), (3, 4, id)]);
    assert_eq!(point.versions(10)[0].commit, 4);
    assert_eq!(point.versions(11)[0].operation, 3);
    assert!(unlocks(&point).is_empty());
    let expected = vec![
        point_capture(manager.activity(2).node.context().unwrap()),
        point_capture(manager.activity(3).node.context().unwrap()),
    ];
    manager.replication(4, Ok(()));
    observe(observer).await.unwrap();
    assert_eq!(unlocks(&point), expected);
    assert_eq!(transaction.get_state(), TransactionState::Committed);
    assert_eq!(transaction.get_commit_sequence_number(), 4);
    assert_eq!(manager.notifications(), vec![(id, 4)]);
    finish(&manager).await;
    assert_eq!(manager.tail(), 5);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t05_interleaved_chains_can_apply_concurrently_without_cross_tx_total_order() {
    let (manager, point) = fixture();
    let a = manager.new_transaction().unwrap();
    let b = manager.new_transaction().unwrap();
    let aid = a.get_transaction_id();
    let bid = b.get_transaction_id();
    let a0 = point.stage(a.as_ref(), 10, 100).unwrap();
    let b0 = point.stage(b.as_ref(), 20, 200).unwrap();
    // Borrowed direct manager entrypoints deliberately separate public add's
    // consecutive begin+operation pair (native TransactionManager/map tests).
    ITransactionManager::begin_transaction(
        manager.as_ref(),
        a.as_ref(),
        None,
        None,
        Some(PointProvider::payload(10, 100)),
        7,
        Some(a0),
    )
    .unwrap();
    ITransactionManager::begin_transaction(
        manager.as_ref(),
        b.as_ref(),
        None,
        None,
        Some(PointProvider::payload(20, 200)),
        7,
        Some(b0),
    )
    .unwrap();
    let a1 = point.stage(a.as_ref(), 11, 110).unwrap();
    let b1 = point.stage(b.as_ref(), 21, 210).unwrap();
    ITransactionManager::add_operation(
        manager.as_ref(),
        a.as_ref(),
        None,
        None,
        Some(PointProvider::payload(11, 110)),
        7,
        Some(a1),
    )
    .unwrap();
    ITransactionManager::add_operation(
        manager.as_ref(),
        b.as_ref(),
        None,
        None,
        Some(PointProvider::payload(21, 210)),
        7,
        Some(b1),
    )
    .unwrap();
    let endb = manager.commit_transaction(b.clone());
    let enda = manager.commit_transaction(a.clone());
    manager.barrier(0).unwrap();
    assert_eq!(
        log(&manager),
        vec![
            ('B', 2, Some(aid)),
            ('B', 3, Some(bid)),
            ('O', 4, Some(aid)),
            ('O', 5, Some(bid)),
            ('C', 6, Some(bid)),
            ('C', 7, Some(aid)),
            ('|', 8, None),
        ]
    );
    assert_eq!(
        lsns(manager.activity(6).node.constituents().unwrap()),
        vec![3, 5]
    );
    assert_eq!(
        lsns(manager.activity(7).node.constituents().unwrap()),
        vec![2, 4]
    );
    let b_gate = Completion::new();
    point.inject_apply(ApplyPlan {
        gate: Some(b_gate.clone()),
        ..ApplyPlan::default()
    });
    manager.flush(Ok(()));
    assert!(manager.activity(7).apply.wait().await.is_ok());
    assert!(!manager.activity(6).apply.is_completed());
    assert_eq!(point.versions(10)[0].commit, 7);
    assert!(point.versions(20).is_empty());
    let per_tx_a: Vec<_> = applies(&point)
        .into_iter()
        .filter(|(_, _, id)| *id == aid)
        .collect();
    assert_eq!(per_tx_a, vec![(2, 7, aid), (4, 7, aid)]);
    b_gate.complete(Ok(()));
    assert!(manager.activity(6).apply.wait().await.is_ok());
    let per_tx_b: Vec<_> = applies(&point)
        .into_iter()
        .filter(|(_, _, id)| *id == bid)
        .collect();
    assert_eq!(per_tx_b, vec![(3, 6, bid), (5, 6, bid)]);
    finish(&manager).await;
    assert_eq!(observe(endb).await.unwrap(), 6);
    assert_eq!(observe(enda).await.unwrap(), 7);
    // Direct manager completion does not run public state-machine hooks.
    assert_eq!(a.get_state(), TransactionState::Active);
    assert_eq!(b.get_state(), TransactionState::Active);
    a.dispose().unwrap();
    b.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t06_cached_begin_admission_failure_retains_original_then_can_retry() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let original = add(&point, &transaction, 10, 100);
    let second = point.stage(transaction.as_ref(), 11, 110).unwrap();
    manager.lower.script([Err(Status::QueueFull.error())]);
    code(
        Transaction::add_operation(
            transaction.as_ref(),
            None,
            None,
            Some(PointProvider::payload(11, 110)),
            7,
            Some(second.clone()),
        ),
        Status::QueueFull,
    );
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert!(transaction.has_cached_first());
    assert!(log(&manager).is_empty() && manager.map.pending().is_empty());
    assert!(point.events().is_empty());
    Transaction::add_operation(
        transaction.as_ref(),
        None,
        None,
        Some(PointProvider::payload(11, 110)),
        7,
        Some(second.clone()),
    )
    .unwrap();
    assert!(!transaction.has_cached_first());
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        original.capture
    );
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        second.capture
    );
    transaction.abort().unwrap();
    group_tick(&manager);
    finish(&manager).await;
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t07_begin_success_second_op_failure_clears_cache_and_abort_logs_end() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let original = add(&point, &transaction, 10, 100);
    let second = point.stage(transaction.as_ref(), 11, 110).unwrap();
    manager
        .lower
        .script([Ok(2), Err(Status::QueueFull.error())]);
    code(
        Transaction::add_operation(
            transaction.as_ref(),
            None,
            None,
            Some(PointProvider::payload(11, 110)),
            7,
            Some(second),
        ),
        Status::QueueFull,
    );
    let id = transaction.get_transaction_id();
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert!(!transaction.has_cached_first());
    assert_eq!(log(&manager), vec![('B', 2, Some(id))]);
    assert_eq!(lsns(manager.map.pending()), vec![2]);
    assert_eq!(manager.map.latest(id).unwrap().lsn(), 2);
    transaction.abort().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Aborting);
    assert_eq!(log(&manager), vec![('B', 2, Some(id)), ('A', 3, Some(id))]);
    assert!(manager.map.pending().is_empty());
    manager.barrier(0).unwrap();
    finish(&manager).await;
    assert!(applies(&point).is_empty());
    assert_eq!(unlocks(&point), vec![original.capture]);
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
    assert_eq!(transaction.get_commit_sequence_number(), -1);
    assert!(manager.notifications().is_empty());
    assert_eq!(manager.tail(), 4);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t08_retry_cancellation_restores_active_but_pre_cancelled_success_wins() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    let token = Cancellation::default();
    token.cancel();
    manager.lower.script([Err(Status::QueueFull.error())]);
    let observer = transaction.commit_with_options(Some(Duration::ZERO), token.clone());
    assert_eq!(transaction.get_state(), TransactionState::Active); // eager failure prefix
    code(observe(observer).await, Status::Cancelled);
    assert!(log(&manager).is_empty() && transaction.has_cached_first());
    assert!(point.events().is_empty() && point.is_locked(10));
    let observer = transaction.commit_with_options(Some(Duration::ZERO), token);
    assert_eq!(transaction.get_state(), TransactionState::Committing);
    assert_eq!(log(&manager).len(), 1);
    group_tick(&manager);
    finish(&manager).await;
    observe(observer).await.unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Committed);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t09_strict_elapsed_timeout_and_native_noop_delay_setter() {
    let times = Arc::new(Mutex::new(VecDeque::from([0, 10, 10, 11])));
    let delays = Arc::new(Mutex::new(Vec::new()));
    let gate = Completion::new();
    let read_times = times.clone();
    let record_delays = delays.clone();
    let delay_gate = gate.clone();
    let retry = Arc::new(RetryRuntime {
        now: Arc::new(move || {
            Duration::from_millis(read_times.lock().unwrap().pop_front().unwrap())
        }),
        delay: Arc::new(move |duration| {
            record_delays.lock().unwrap().push(duration);
            let gate = delay_gate.clone();
            Box::pin(async move { gate.wait().await.as_ref().clone() })
        }),
    });
    let (manager, point) = fixture_with(ProcessingRole::Primary, 0, retry);
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    manager.lower.script([
        Err(Status::QueueFull.error()),
        Err(Status::NoWriteQuorum.error()),
    ]);
    manager.script_throttle([Ok(()), Err(Status::ServiceTooBusy.error()), Ok(())]);
    let observer =
        transaction.commit_with_options(Some(Duration::from_millis(10)), Cancellation::default());
    assert_eq!(transaction.get_state(), TransactionState::Committing);
    assert_eq!(*delays.lock().unwrap(), vec![Duration::from_millis(32)]); // elapsed==timeout retries.
    gate.complete(Ok(()));
    code(observe(observer).await, Status::Timeout);
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert_eq!(*delays.lock().unwrap(), vec![Duration::from_millis(32); 2]);
    assert_eq!(manager.lower.attempts().len(), 2);
    assert!(!manager.faults.is_closing() && manager.faults.reported().is_empty());
    assert!(log(&manager).is_empty() && transaction.has_cached_first());
    // A new watch start is sampled on retrying Commit; timeout did not alter ID.
    times.lock().unwrap().push_back(100);
    let id = transaction.get_transaction_id();
    let observer = transaction.commit();
    assert_eq!(log(&manager)[0].2, Some(id));
    group_tick(&manager);
    finish(&manager).await;
    observe(observer).await.unwrap();
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t10_permanent_admission_error_faults_without_retry_or_operation_unlock() {
    let calls = Arc::new(Mutex::new(0));
    let timer_calls = calls.clone();
    let retry = Arc::new(RetryRuntime {
        now: Arc::new(|| Duration::ZERO),
        delay: Arc::new(move |_| {
            *timer_calls.lock().unwrap() += 1;
            Box::pin(async { Ok(()) })
        }),
    });
    let (manager, point) = fixture_with(ProcessingRole::Primary, 0, retry);
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    manager
        .lower
        .script([Err(Status::OperationTooLarge.error())]);
    code(
        observe(transaction.commit()).await,
        Status::OperationTooLarge,
    );
    assert_eq!(transaction.get_state(), TransactionState::Faulted);
    assert!(transaction.has_cached_first());
    assert!(log(&manager).is_empty() && manager.map.pending().is_empty());
    assert_eq!(*calls.lock().unwrap(), 0);
    assert!(unlocks(&point).is_empty() && point.is_locked(10));
    transaction.dispose().unwrap();
    assert!(!point.is_locked(10)); // nontracking lock cleanup, not operation Unlock.
    assert!(unlocks(&point).is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t11_cached_abort_uses_original_and_masks_provider_failure_after_wrapper_gate() {
    for fail in [false, true] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        let original = add(&point, &transaction, 10, 100);
        if fail {
            point.inject_unlock(Err(Status::InvalidOperation.error()));
        }
        transaction.abort().unwrap();
        assert_eq!(transaction.get_state(), TransactionState::Aborted);
        assert!(!transaction.has_cached_first());
        assert_eq!(unlocks(&point), vec![original.capture]);
        assert!(log(&manager).is_empty() && manager.map.pending().is_empty());
        assert_eq!(manager.faults.reported().len(), usize::from(fail));
        assert!(manager.faults.service_error().is_none()); // StateManager fault, not processor latch.
        assert!(!manager.faults.is_closing());
        assert_eq!(point.is_locked(10), fail);
        transaction.dispose().unwrap();
        assert!(!point.is_locked(10));
    }
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    Transaction::add_operation(
        transaction.as_ref(),
        None,
        None,
        Some(PointProvider::payload(10, 100)),
        7,
        None,
    )
    .unwrap();
    transaction.abort().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
    assert!(point.events().is_empty());
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t12_multi_abort_is_owned_not_drain_and_retries_end_admission() {
    let timer = Completion::new();
    let gate = timer.clone();
    let retry = Arc::new(RetryRuntime {
        now: Arc::new(|| Duration::ZERO),
        delay: Arc::new(move |duration| {
            assert_eq!(duration, Duration::from_millis(32));
            let gate = gate.clone();
            Box::pin(async move { gate.wait().await.as_ref().clone() })
        }),
    });
    let (manager, point) = fixture_with(ProcessingRole::Primary, 0, retry);
    let transaction = manager.new_transaction().unwrap();
    let a = add(&point, &transaction, 10, 100);
    let b = add(&point, &transaction, 11, 110);
    manager.lower.script([Err(Status::QueueFull.error())]);
    transaction.abort().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Aborting);
    assert_eq!(log(&manager).len(), 2);
    assert_eq!(lsns(manager.map.pending()), vec![2]);
    timer.complete(Ok(()));
    settle().await;
    assert_eq!(log(&manager)[2].0, 'A');
    assert!(manager.map.pending().is_empty());
    assert_eq!(lsns(manager.map.completed()), vec![2]);
    assert_eq!(lsns(manager.map.unstable()), vec![4]);
    assert_eq!(transaction.get_state(), TransactionState::Aborting);
    manager.barrier(0).unwrap();
    manager.flush(Ok(()));
    assert!(manager.activity(4).apply.wait().await.is_ok());
    assert!(point.events().is_empty());
    manager.replication(4, Ok(()));
    manager.activity(4).processing.wait().await;
    settle().await;
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
    assert_eq!(unlocks(&point), vec![a.capture, b.capture]);
    assert!(manager.notifications().is_empty());
    finish(&manager).await;
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t13_expired_weak_manager_leaves_committing_or_aborting_and_read_restores_active() {
    let (manager, point) = fixture();
    let committing = manager.new_transaction().unwrap();
    let aborting = manager.new_transaction().unwrap();
    let reader = manager.new_transaction().unwrap();
    add(&point, &aborting, 10, 100);
    drop(manager);
    code(observe(committing.commit()).await, Status::ObjectClosed);
    assert_eq!(committing.get_state(), TransactionState::Committing);
    aborting.abort().unwrap();
    assert_eq!(aborting.get_state(), TransactionState::Aborting);
    assert!(aborting.has_cached_first());
    assert!(point.events().is_empty());
    code(
        observe(reader.get_visibility_sequence_number()).await,
        Status::ObjectClosed,
    );
    assert_eq!(reader.get_state(), TransactionState::Active);
    reader.dispose().unwrap();
    aborting.dispose().unwrap();
    // Dispose cannot be invoked in Committing: native assertion exclusion.
    drop(committing);
}

#[tokio::test(flavor = "current_thread")]
async fn t14_accepted_first_apply_error_assigns_lsn_then_waits_quorum_and_unlocks_original() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let original = add(&point, &transaction, 10, 100);
    point.inject_apply(ApplyPlan {
        result: Err(Status::InvalidOperation.error()),
        ..ApplyPlan::default()
    });
    let observer = transaction.commit();
    manager.barrier(0).unwrap();
    manager.flush(Ok(()));
    code(
        manager.activity(2).apply.wait().await.as_ref().clone(),
        Status::InvalidOperation,
    );
    assert_eq!(transaction.get_commit_sequence_number(), 2);
    assert!(point.versions(10).is_empty() && point.read_faulted());
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        original.capture
    );
    assert!(!manager.activity(2).result.is_completed() && unlocks(&point).is_empty());
    assert_eq!(
        manager.faults.service_error().unwrap().native_code(),
        Some(Status::InvalidOperation.code())
    );
    assert!(!manager.faults.reported().is_empty());
    manager.replication(2, Ok(()));
    code(observe(observer).await, Status::TransactionAborted);
    assert_eq!(transaction.get_state(), TransactionState::Faulted);
    assert_eq!(unlocks(&point), vec![original.capture]);
    assert_eq!(
        manager.notifications(),
        vec![(transaction.get_transaction_id(), 2)]
    );
    assert!(manager.map.pending().is_empty());
    assert_eq!(log(&manager)[0].0, 'S');
    finish(&manager).await;
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t15_middle_apply_error_keeps_partial_versions_and_unlocks_unseen_originals() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let a = add(&point, &transaction, 10, 100);
    let b = add(&point, &transaction, 11, 110);
    let c = add(&point, &transaction, 12, 120);
    point.inject_apply(ApplyPlan::default());
    point.inject_apply(ApplyPlan {
        result: Err(Status::InvalidOperation.error()),
        ..ApplyPlan::default()
    });
    let observer = transaction.commit();
    manager.barrier(0).unwrap();
    assert!(manager.map.pending().is_empty());
    assert_eq!(lsns(manager.map.completed()), vec![2]);
    assert_eq!(lsns(manager.map.unstable()), vec![5]);
    manager.flush(Ok(()));
    code(
        manager.activity(5).apply.wait().await.as_ref().clone(),
        Status::InvalidOperation,
    );
    let id = transaction.get_transaction_id();
    assert_eq!(applies(&point), vec![(2, 5, id), (3, 5, id)]);
    assert_eq!(point.versions(10)[0].value, 100);
    assert!(point.versions(11).is_empty() && point.versions(12).is_empty());
    let returned = point_capture(manager.activity(2).node.context().unwrap());
    assert_ne!(returned, a.capture);
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        b.capture
    );
    assert_eq!(
        point_capture(manager.activity(4).node.context().unwrap()),
        c.capture
    );
    manager.replication(5, Ok(()));
    code(observe(observer).await, Status::TransactionAborted);
    assert_eq!(unlocks(&point), vec![returned, b.capture, c.capture]);
    assert_eq!(transaction.get_state(), TransactionState::Faulted);
    assert_eq!(transaction.get_commit_sequence_number(), 5);
    assert_eq!(log(&manager)[3], ('C', 5, Some(id)));
    assert_eq!(manager.notifications(), vec![(id, 5)]);
    assert!(point.read_faulted());
    let reader = manager.new_transaction().unwrap();
    code(point.snapshot(reader.as_ref(), 10, 6), Status::ObjectClosed);
    assert_eq!(point.versions(10)[0].value, 100); // no compensating Undo.
    finish(&manager).await;
    transaction.dispose().unwrap();
    reader.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t16_replication_failure_after_apply_does_not_rollback_canonical_commit_or_versions() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    add(&point, &transaction, 11, 110);
    let observer = transaction.commit();
    group_tick(&manager);
    manager.flush(Ok(()));
    assert!(manager.activity(4).apply.wait().await.is_ok());
    assert_eq!(point.versions(10)[0].commit, 4);
    assert_eq!(point.versions(11)[0].commit, 4);
    assert!(point.is_locked(10) && point.is_locked(11));
    manager.replication(4, Err(Status::NoWriteQuorum.error()));
    code(observe(observer).await, Status::TransactionAborted);
    assert_eq!(transaction.get_state(), TransactionState::Faulted);
    assert_eq!(transaction.get_commit_sequence_number(), 4);
    assert_eq!(log(&manager)[2].0, 'C');
    assert_eq!(lsns(manager.map.completed()), vec![2]);
    assert_eq!(lsns(manager.map.unstable()), vec![4]);
    assert!(manager.faults.reported().is_empty()); // expected replication error
    assert!(manager.faults.service_error().is_none());
    assert_eq!(unlocks(&point).len(), 2);
    assert!(!point.is_locked(10) && !point.is_locked(11));
    assert_eq!(
        manager.notifications(),
        vec![(transaction.get_transaction_id(), 4)]
    );
    finish(&manager).await;
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t17_failed_flush_skips_apply_but_primary_waits_quorum_and_unlocks_originals() {
    for failure in [Status::InvalidOperation, Status::ObjectClosed] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        let a = add(&point, &transaction, 10, 100);
        let b = add(&point, &transaction, 11, 110);
        let observer = transaction.commit();
        assert!(manager.map.pending().is_empty());
        manager.flush(Err(failure.error()));
        assert!(manager.activity(4).apply.is_completed());
        assert!(manager.activity(4).processing.is_completed()); // immediate failed-flush processing
        assert!(!manager.activity(4).result.is_completed());
        assert!(point.events().is_empty());
        assert_eq!(transaction.get_commit_sequence_number(), -1);
        assert_eq!(
            manager.faults.log_error().unwrap().native_code(),
            Some(failure.code())
        );
        assert_eq!(
            manager.faults.reported().len(),
            usize::from(failure != Status::ObjectClosed)
        );
        assert_eq!(manager.faults.observed().len(), 3); // Per-record log errors, one partition report.
        manager.replication(4, Ok(()));
        code(observe(observer).await, Status::TransactionAborted);
        assert_eq!(transaction.get_state(), TransactionState::Faulted);
        assert_eq!(unlocks(&point), vec![a.capture, b.capture]);
        assert_eq!(lsns(manager.map.completed()), vec![2]);
        assert_eq!(lsns(manager.map.unstable()), vec![4]);
        assert!(point.versions(10).is_empty());
        finish(&manager).await;
        transaction.dispose().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t18_first_or_middle_unlock_failure_takes_failed_slot_stops_and_does_not_change_success() {
    for failed_index in [0usize, 1] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        add(&point, &transaction, 10, 100);
        add(&point, &transaction, 11, 110);
        add(&point, &transaction, 12, 120);
        let observer = transaction.commit();
        group_tick(&manager);
        manager.flush(Ok(()));
        assert!(manager.activity(5).apply.wait().await.is_ok());
        let contexts: Vec<_> = [2, 3, 4]
            .into_iter()
            .map(|lsn| point_capture(manager.activity(lsn).node.context().unwrap()))
            .collect();
        for _ in 0..failed_index {
            point.inject_unlock(Ok(()));
        }
        point.inject_unlock(Err(Status::InvalidOperation.error()));
        manager.replication(5, Ok(()));
        observe(observer).await.unwrap();
        assert_eq!(transaction.get_state(), TransactionState::Committed);
        assert_eq!(unlocks(&point), contexts[..=failed_index]);
        for lsn in 2..=(2 + failed_index as i64) {
            assert!(manager.activity(lsn).node.context().is_none());
        }
        for lsn in (3 + failed_index as i64)..=4 {
            assert!(manager.activity(lsn).node.context().is_some());
        }
        assert!(manager.notifications().is_empty());
        assert_eq!(
            manager.faults.service_error().unwrap().native_code(),
            Some(Status::InvalidOperation.code())
        );
        assert_eq!(manager.faults.reported().len(), 1);
        assert_eq!(lsns(manager.map.unstable()), vec![5]);
        assert_eq!(point.versions(12)[0].value, 120);
        // Native store context is shared/idempotent: first successful release
        // already closed its transaction; a failed first release keeps all locks.
        assert_eq!(point.is_locked(12), failed_index == 0);
        finish(&manager).await;
        transaction.dispose().unwrap();
        assert!(!point.is_locked(12));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t19_prior_service_error_suppresses_next_apply_but_not_required_unlock() {
    let (manager, point) = fixture();
    let first = manager.new_transaction().unwrap();
    let second = manager.new_transaction().unwrap();
    add(&point, &first, 10, 100);
    let second_original = add(&point, &second, 20, 200);
    point.inject_apply(ApplyPlan {
        result: Err(Status::InvalidOperation.error()),
        ..ApplyPlan::default()
    });
    let first_observer = first.commit();
    let second_observer = second.commit();
    group_tick(&manager);
    manager.flush(Ok(()));
    assert!(manager.activity(2).apply.wait().await.is_err());
    code(
        manager.activity(3).apply.wait().await.as_ref().clone(),
        Status::InvalidOperation,
    );
    assert_eq!(applies(&point), vec![(2, 2, first.get_transaction_id())]);
    assert_eq!(second.get_commit_sequence_number(), -1);
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        second_original.capture
    );
    assert!(manager.activity(3).node.context().is_some());
    finish(&manager).await;
    code(observe(first_observer).await, Status::TransactionAborted);
    code(observe(second_observer).await, Status::TransactionAborted);
    assert!(unlocks(&point).contains(&second_original.capture));
    assert_eq!(second.get_state(), TransactionState::Faulted);
    assert_eq!(log(&manager).len(), 3);
    assert!(manager.faults.is_closing());
    let later = manager.new_transaction().unwrap();
    Transaction::add_operation(
        later.as_ref(),
        None,
        None,
        Some(PointProvider::payload(30, 300)),
        7,
        None,
    )
    .unwrap();
    let attempts = manager.lower.attempts().len();
    code(observe(later.commit()).await, Status::ObjectClosed);
    assert_eq!(manager.lower.attempts().len(), attempts);
    assert_eq!(later.get_commit_sequence_number(), -1);
    assert!(later.has_cached_first());
    later.dispose().unwrap();
    first.dispose().unwrap();
    second.dispose().unwrap();
}

fn replay_wire(kind: RecordKind, embedded: i64) -> OperationData {
    Record {
        lsn: embedded,
        kind,
    }
    .encode()
    .unwrap()
}
fn begin_kind(id: i64, single: bool, key: i64, value: i64) -> RecordKind {
    RecordKind::Begin {
        transaction_id: id,
        single,
        metadata: Some(named::encode(7, None).unwrap()),
        undo: None,
        redo: Some(PointProvider::payload(key, value)),
    }
}
fn op_kind(id: i64, key: i64, value: i64) -> RecordKind {
    RecordKind::Operation {
        transaction_id: id,
        redo_only: false,
        metadata: Some(named::encode(7, None).unwrap()),
        undo: None,
        redo: Some(PointProvider::payload(key, value)),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t20_replay_has_per_record_active_objects_commit_assignment_and_covered_prefix_gates() {
    let (manager, point) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let begin = manager
        .replay_record(replay_wire(begin_kind(500, false, 10, 100), -1), Some(2))
        .unwrap();
    let operation = manager
        .replay_record(replay_wire(op_kind(500, 11, 110), -1), Some(3))
        .unwrap();
    let end = manager
        .replay_record(
            replay_wire(
                RecordKind::End {
                    transaction_id: 500,
                    committed: true,
                },
                -1,
            ),
            Some(4),
        )
        .unwrap();
    assert!(!Arc::ptr_eq(
        &begin.node.transaction().unwrap(),
        &operation.node.transaction().unwrap()
    ));
    assert_eq!(lsns(manager.map.completed()), vec![2]);
    group_tick(&manager);
    finish(&manager).await;
    for activity in [begin, operation, end] {
        let transaction = activity.node.transaction().unwrap();
        assert_eq!(transaction.get_commit_sequence_number(), 4);
        assert_eq!(transaction.get_state(), TransactionState::Active);
        assert!(!transaction.get_is_primary_transaction());
    }
    assert_eq!(applies(&point), vec![(2, 4, 500), (3, 4, 500)]);
    assert!(point
        .events()
        .iter()
        .filter_map(|event| match event {
            PointEvent::Apply { role, .. } => Some(role),
            _ => None,
        })
        .all(|role| *role == ApplyContext::SecondaryRedo));
    assert_eq!(unlocks(&point).len(), 2);
    let (covered, point) = fixture_with(
        ProcessingRole::Recovery,
        4,
        Arc::new(RetryRuntime::default()),
    );
    covered
        .replay_record(replay_wire(op_kind(600, 10, 100), -1), Some(2))
        .unwrap();
    let end = covered
        .replay_record(
            replay_wire(
                RecordKind::End {
                    transaction_id: 600,
                    committed: true,
                },
                -1,
            ),
            Some(3),
        )
        .unwrap();
    assert!(!end.node.is_enlisted());
    group_tick(&covered);
    finish(&covered).await;
    assert!(point.events().is_empty());
    assert_eq!(
        end.node.transaction().unwrap().get_commit_sequence_number(),
        -1
    );
    assert_eq!(covered.notifications(), vec![(600, 3)]);
    assert!(covered.map.pending().is_empty());
    // Above covered bound an orphan is a checked native invariant exclusion.
    assert!(covered
        .replay_record(replay_wire(op_kind(601, 20, 200), -1), Some(5))
        .is_err());
    assert_eq!(covered.tail(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn t21_dropped_commit_observation_keeps_eager_accepted_shared_work() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    let weak = Arc::downgrade(&transaction);
    let observer = transaction.commit();
    assert_eq!(log(&manager).len(), 1); // admission precedes return/poll
    drop(observer);
    let id = transaction.get_transaction_id();
    drop(transaction);
    assert!(weak.upgrade().is_some());
    manager.flush(Ok(()));
    manager.replication(2, Ok(()));
    settle().await;
    assert!(!manager.activity(2).apply.is_completed());
    assert!(!manager.activity(2).result.is_completed());
    assert!(point.events().is_empty());
    group_tick(&manager);
    manager.flush(Ok(()));
    assert!(manager.activity(2).apply.wait().await.is_ok());
    manager
        .activity(2)
        .result
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    settle().await;
    let transaction = weak.upgrade().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Committed);
    assert_eq!(transaction.get_commit_sequence_number(), 2);
    assert_eq!(manager.notifications(), vec![(id, 2)]);
    assert_eq!(unlocks(&point).len(), 1);
    assert!(manager.map.pending().is_empty());
    finish(&manager).await;
    transaction.dispose().unwrap();
}

struct InspectLock {
    tracking: bool,
    calls: Arc<Mutex<usize>>,
    inspect: Arc<dyn Fn() + Send + Sync>,
}
impl OperationContext for InspectLock {}
impl LockContext for InspectLock {
    fn is_tracking_context(&self) -> bool {
        self.tracking
    }
    fn unlock(&self) -> Result<()> {
        (self.inspect)();
        *self.calls.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn s02_dispose_unregisters_before_terminal_state_validation_and_unlocks_only_nontracking() {
    // TransactionStateMachine.h:90–120 / Transaction.cpp:162–190.
    for ignore_failure in [false, true] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        observe(transaction.get_visibility_sequence_number())
            .await
            .unwrap();
        observe(transaction.commit()).await.unwrap();
        let calls = Arc::new(Mutex::new(0));
        let weak = Arc::downgrade(&manager);
        let inspect = Arc::new(move || {
            let versions = weak.upgrade().unwrap().versions.registered_versions();
            assert_eq!(versions, if ignore_failure { vec![1] } else { vec![] });
        });
        transaction
            .add_lock_context(Arc::new(InspectLock {
                tracking: false,
                calls: calls.clone(),
                inspect: inspect.clone(),
            }))
            .unwrap();
        transaction
            .add_lock_context(Arc::new(InspectLock {
                tracking: true,
                calls: calls.clone(),
                inspect: Arc::new(|| panic!("tracking lock must not be disposed")),
            }))
            .unwrap();
        if ignore_failure {
            manager.inject_unregister_error(Status::ObjectClosed.error());
        }
        transaction.dispose().unwrap();
        assert_eq!(transaction.get_state(), TransactionState::Committed);
        assert_eq!(*calls.lock().unwrap(), 1);
        assert!(transaction.is_disposed());
        transaction.dispose().unwrap();
        assert_eq!(*calls.lock().unwrap(), 1);
        assert!(log(&manager).is_empty() && point.events().is_empty());
        if ignore_failure {
            manager.unregister(1).unwrap();
        }
    }
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    observe(transaction.get_visibility_sequence_number())
        .await
        .unwrap();
    add(&point, &transaction, 10, 100);
    add(&point, &transaction, 11, 110);
    transaction.dispose().unwrap();
    assert!(transaction.is_disposed());
    assert_eq!(transaction.get_state(), TransactionState::Aborting);
    assert!(manager.versions.registered_versions().is_empty());
    assert_eq!(log(&manager)[2].0, 'A'); // eager end admission, not completion.
    assert!(unlocks(&point).is_empty());
    assert!(!point.is_locked(10)); // base lock disposal independently released.
    assert!(!manager.activity(4).result.is_completed());
    group_tick(&manager);
    finish(&manager).await;
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
}

#[tokio::test(flavor = "current_thread")]
async fn s04_pending_demotion_cleanup_takes_only_operations_latest_backwards_retains_begin_and_map()
{
    // LoggingReplicatorImpl.cpp:1693–1731, not TransactionMap::Reuse.
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let begin = add(&point, &transaction, 10, 100);
    let op1 = add(&point, &transaction, 11, 110);
    let op2 = add(&point, &transaction, 12, 120);
    point.inject_unlock(Err(Status::InvalidOperation.error()));
    manager.cleanup_pending_operations();
    assert_eq!(unlocks(&point), vec![op2.capture, op1.capture]);
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        begin.capture
    );
    assert!(manager.activity(3).node.context().is_none());
    assert!(manager.activity(4).node.context().is_none());
    assert_eq!(lsns(manager.map.pending()), vec![2]);
    assert_eq!(
        manager
            .map
            .latest(transaction.get_transaction_id())
            .unwrap()
            .lsn(),
        4
    );
    assert!(manager.faults.reported().is_empty() && manager.faults.service_error().is_none());
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert_eq!(log(&manager).len(), 3);
    transaction.abort().unwrap();
    group_tick(&manager);
    finish(&manager).await;
    assert_eq!(
        unlocks(&point),
        vec![op2.capture, op1.capture, begin.capture]
    );
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn v01_dispatching_apply_fence_owns_dropped_read_caches_vsn12_before_quorum() {
    // VersionManager.cpp:276–319 / Transaction.cpp:387–404.
    let (manager, point) = fixture();
    for _ in 2..=10 {
        manager.barrier(0).unwrap();
    }
    let writer = manager.new_transaction().unwrap();
    add(&point, &writer, 10, 100);
    let gate = Completion::new();
    point.inject_apply(ApplyPlan {
        gate: Some(gate.clone()),
        ..ApplyPlan::default()
    });
    let commit = writer.commit();
    let barrier = manager.barrier(0).unwrap();
    assert_eq!(barrier.node.lsn(), 12);
    manager.flush(Ok(()));
    assert_eq!(manager.version(), 10); // Fence12 published; barrier12 not identified yet.
    assert!(!barrier.apply.is_completed());
    let reader = manager.new_transaction().unwrap();
    let read = reader.get_visibility_sequence_number();
    assert_eq!(reader.get_state(), TransactionState::Reading);
    code(
        Transaction::add_operation(reader.as_ref(), None, None, None, 7, None),
        Status::MultithreadedTransactions,
    );
    code(
        observe(reader.commit()).await,
        Status::MultithreadedTransactions,
    );
    drop(read); // work is owned independently of observation.
    gate.complete(Ok(()));
    assert!(barrier.apply.wait().await.is_ok());
    settle().await;
    assert_eq!(reader.get_state(), TransactionState::Active);
    assert_eq!(manager.versions.registered_versions(), vec![12]);
    assert_eq!(
        observe(reader.get_visibility_sequence_number())
            .await
            .unwrap(),
        12
    );
    assert_eq!(point.snapshot(reader.as_ref(), 10, 12).unwrap(), Some(100));
    assert!(point.is_locked(10) && !manager.activity(11).replication.is_completed());
    for _ in 13..=20 {
        manager.barrier(0).unwrap();
    }
    manager.flush(Ok(()));
    assert!(manager.activity(20).apply.wait().await.is_ok());
    assert_eq!(manager.version(), 20);
    assert_eq!(
        observe(reader.get_visibility_sequence_number())
            .await
            .unwrap(),
        12
    );
    assert_eq!(manager.versions.registered_versions(), vec![12]); // no duplicate registration for cached read.
    reader.dispose().unwrap();
    assert!(manager.versions.registered_versions().is_empty());
    finish(&manager).await;
    observe(commit).await.unwrap();
    writer.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn v03_primary_stable_uses_own_lsn_after_quorum_secondary_uses_advertisement_after_apply() {
    // CheckpointManager.cpp:1188–1205,1554–1630; physical checkpoint deferred.
    let (primary, _) = fixture();
    for _ in 2..=11 {
        primary.barrier(0).unwrap();
    }
    let barrier = primary.barrier(8).unwrap();
    primary.flush(Ok(()));
    assert!(barrier.apply.wait().await.is_ok());
    primary.replication(5, Ok(()));
    primary.activity(5).processing.wait().await;
    assert_eq!(primary.stable(), 5);
    assert_eq!(primary.version(), 12);
    assert_eq!(primary.tail(), 12);
    assert!(!barrier.processing.is_completed());
    primary.replication(12, Ok(()));
    barrier.processing.wait().await;
    assert_eq!(primary.stable(), 12);
    finish(&primary).await;

    for fail in [false, true] {
        let (secondary, _) = fixture_with(
            ProcessingRole::Secondary,
            0,
            Arc::new(RetryRuntime::default()),
        );
        for lsn in 2..=11 {
            secondary
                .replay_record(
                    replay_wire(RecordKind::Barrier { last_stable_lsn: 5 }, lsn),
                    None,
                )
                .unwrap();
        }
        secondary.flush(Ok(()));
        secondary.activity(11).processing.wait().await;
        assert_eq!(secondary.stable(), 5);
        let barrier = secondary
            .replay_record(
                replay_wire(RecordKind::Barrier { last_stable_lsn: 8 }, 12),
                None,
            )
            .unwrap();
        secondary.flush(if fail {
            Err(Status::ObjectClosed.error())
        } else {
            Ok(())
        });
        barrier.processing.wait().await;
        assert_eq!(secondary.stable(), if fail { 5 } else { 8 });
        if !fail {
            assert_eq!(secondary.version(), 12);
        }
        assert_eq!(secondary.tail(), 12);
        assert!(secondary.faults.reported().is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn w02_replication_embedded_minus_one_is_overwritten_by_lower_metadata_copy_retains_embedded()
{
    // ReplicatedLogManager.cpp:935–978 / SecondaryDrainManager.cpp:559–610,827–835.
    let (primary, point) = fixture();
    let transaction = primary.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    let commit = transaction.commit();
    let lower = primary.lower.attempts();
    assert_eq!(lower.len(), 1);
    assert_eq!(lower[0].assigned.as_ref().unwrap(), &2);
    let decoded = Record::decode(&lower[0].wire).unwrap();
    assert_eq!(decoded.lsn, -1);
    assert_eq!(
        decoded.transaction_id(),
        Some(transaction.get_transaction_id())
    );
    assert_eq!(lower[0].wire.segments()[0].len(), 16); // H, not outer physical length.
    assert_eq!(primary.activity(2).node.lsn(), 2);
    let (secondary, secondary_point) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let replay = secondary
        .replay_record(lower[0].wire.clone(), Some(2))
        .unwrap();
    assert_eq!(replay.node.lsn(), 2);
    let (copy, copy_point) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let copied = copy
        .replay_record(primary.activity(2).node.record().encode().unwrap(), None)
        .unwrap();
    assert_eq!(copied.node.lsn(), 2);
    group_tick(&primary);
    group_tick(&secondary);
    group_tick(&copy);
    finish(&primary).await;
    finish(&secondary).await;
    finish(&copy).await;
    observe(commit).await.unwrap();
    assert_eq!(secondary_point.versions(10)[0].commit, 2);
    assert_eq!(copy_point.versions(10)[0].commit, 2);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn w03_epoch_duplicate_precedes_tail_validation_new_marker_is_local_at_same_tail() {
    // LoggingReplicatorImpl.cpp:1370–1419 / ReplicatedLogManager.cpp:751–775.
    let (manager, _) = fixture();
    for _ in 2..=10 {
        manager.barrier(0).unwrap();
    }
    let e0 = crate::Epoch {
        data_loss_number: 0,
        configuration_number: 0,
    };
    let e1 = crate::Epoch {
        data_loss_number: 0,
        configuration_number: 1,
    };
    assert!(manager.update_epoch(e0, 9, 42, 123).unwrap().is_none()); // monotonic seam0 <=9, same epoch.
    assert_eq!(manager.tail(), 10);
    assert_eq!(manager.lower.attempts().len(), 9);
    let epoch = manager.update_epoch(e1, 10, 42, 123).unwrap().unwrap();
    assert_eq!(epoch.node.lsn(), 10);
    assert_eq!(manager.tail(), 10);
    assert_eq!(manager.lower.attempts().len(), 9);
    assert_eq!(log(&manager).last(), Some(&('E', 10, None)));
    assert!(manager.update_epoch(e1, 10, 42, 124).unwrap().is_none());
    assert!(manager.update_epoch(e1, 9, 42, 124).is_err()); // progress-vector bound precedes duplicate branch.
    finish(&manager).await;
    assert!(epoch.apply.is_completed() && epoch.replication.is_completed());
    let (secondary, _) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    for lsn in 2..=10 {
        secondary
            .replay_record(
                replay_wire(RecordKind::Barrier { last_stable_lsn: 0 }, lsn),
                None,
            )
            .unwrap();
    }
    code(
        secondary.update_epoch(e1, 9, 42, 123),
        Status::InvalidOperation,
    );
    assert_eq!(secondary.tail(), 10);
    assert_eq!(log(&secondary).len(), 9);
    finish(&secondary).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_append_gap_queue_mutates_map_before_gap_closes_and_flushes_only_ordered() {
    // ReplicatedLogManager.cpp:638–845 ordering lock is not a transaction lease.
    let (manager, point) = fixture();
    let a = manager.new_transaction().unwrap();
    let b = manager.new_transaction().unwrap();
    let ca = point.stage(a.as_ref(), 10, 100).unwrap();
    let cb = point.stage(b.as_ref(), 20, 200).unwrap();
    manager.lower.script([Ok(3), Ok(2)]);
    ITransactionManager::begin_transaction(
        manager.as_ref(),
        a.as_ref(),
        None,
        None,
        Some(PointProvider::payload(10, 100)),
        7,
        Some(ca),
    )
    .unwrap();
    assert_eq!(manager.tail(), 1);
    assert!(log(&manager).is_empty());
    assert_eq!(manager.queued_lsns(), vec![3]);
    assert_eq!(lsns(manager.map.pending()), vec![3]);
    manager.flush(Ok(()));
    assert!(point.events().is_empty());
    ITransactionManager::begin_transaction(
        manager.as_ref(),
        b.as_ref(),
        None,
        None,
        Some(PointProvider::payload(20, 200)),
        7,
        Some(cb),
    )
    .unwrap();
    assert_eq!(manager.tail(), 3);
    assert!(manager.queued_lsns().is_empty());
    assert_eq!(
        log(&manager),
        vec![
            ('B', 2, Some(b.get_transaction_id())),
            ('B', 3, Some(a.get_transaction_id()))
        ]
    );
    assert_eq!(lsns(manager.map.pending()), vec![2, 3]);
    let enda = manager.abort_transaction(a.clone());
    let endb = manager.abort_transaction(b.clone());
    group_tick(&manager);
    finish(&manager).await;
    observe(enda).await.unwrap();
    observe(endb).await.unwrap();
    a.dispose().unwrap();
    b.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn registration_failure_and_own_transient_delete_never_set_write_or_log() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    code(
        Transaction::add_operation(transaction.as_ref(), None, None, None, 999, None),
        Status::InvalidParameter,
    );
    assert!(!transaction.is_write() && !transaction.has_cached_first());
    assert!(manager.lower.attempts().is_empty());
    manager.directory.set_mode(
        7,
        ProviderMode::Active,
        Some(transaction.get_transaction_id()),
    );
    code(
        Transaction::add_operation(transaction.as_ref(), None, None, None, 7, None),
        Status::ObjectClosed,
    );
    assert!(!transaction.is_write() && !transaction.has_cached_first());
    let other = manager.new_transaction().unwrap();
    Transaction::add_operation(
        other.as_ref(),
        None,
        None,
        Some(PointProvider::payload(20, 200)),
        7,
        None,
    )
    .unwrap();
    assert!(other.has_cached_first()); // another transaction cannot see transient delete.
    manager.directory.set_mode(7, ProviderMode::Active, None);
    add(&point, &transaction, 10, 100);
    manager.directory.set_mode(
        7,
        ProviderMode::Active,
        Some(transaction.get_transaction_id()),
    );
    let second = point.stage(transaction.as_ref(), 11, 110).unwrap();
    Transaction::add_operation(
        transaction.as_ref(),
        None,
        None,
        Some(PointProvider::payload(11, 110)),
        7,
        Some(second),
    )
    .unwrap(); // logged registration check=false.
    assert_eq!(log(&manager).len(), 2);
    transaction.abort().unwrap();
    other.abort().unwrap();
    group_tick(&manager);
    finish(&manager).await;
    transaction.dispose().unwrap();
    other.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn later_operation_and_end_admission_failures_leave_existing_chain_and_captures() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let a = add(&point, &transaction, 10, 100);
    let b = add(&point, &transaction, 11, 110);
    let c = point.stage(transaction.as_ref(), 12, 120).unwrap();
    manager.lower.script([Err(Status::QueueFull.error())]);
    code(
        Transaction::add_operation(
            transaction.as_ref(),
            None,
            None,
            Some(PointProvider::payload(12, 120)),
            7,
            Some(c),
        ),
        Status::QueueFull,
    );
    assert_eq!(log(&manager).len(), 2);
    assert_eq!(
        manager
            .map
            .latest(transaction.get_transaction_id())
            .unwrap()
            .lsn(),
        3
    );
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        a.capture
    );
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        b.capture
    );
    manager.lower.script([Err(Status::QueueFull.error())]);
    let token = Cancellation::default();
    token.cancel();
    code(
        observe(transaction.commit_with_options(None, token)).await,
        Status::Cancelled,
    );
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert_eq!(log(&manager).len(), 2);
    assert_eq!(lsns(manager.map.pending()), vec![2]);
    assert!(manager.map.completed().is_empty() && manager.map.unstable().is_empty());
    let observer = transaction.commit();
    assert_eq!(log(&manager)[2].0, 'C');
    group_tick(&manager);
    finish(&manager).await;
    observe(observer).await.unwrap();
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn single_success_with_null_capture_retains_original_or_no_capture_without_synthetic_unlock()
{
    for original in [false, true] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        let capture = if original {
            Some(add(&point, &transaction, 10, 100).capture)
        } else {
            Transaction::add_operation(
                transaction.as_ref(),
                None,
                None,
                Some(PointProvider::payload(10, 100)),
                7,
                None,
            )
            .unwrap();
            None
        };
        point.inject_apply(ApplyPlan {
            capture: CaptureResult::RetainOriginal,
            ..ApplyPlan::default()
        });
        let observer = transaction.commit();
        group_tick(&manager);
        manager.flush(Ok(()));
        assert!(manager.activity(2).apply.wait().await.is_ok());
        assert_eq!(
            manager.activity(2).node.context().map(point_capture),
            capture
        );
        assert_eq!(point.versions(10)[0].value, 100);
        manager.replication(2, Ok(()));
        observe(observer).await.unwrap();
        assert_eq!(unlocks(&point), capture.into_iter().collect::<Vec<_>>());
        assert_eq!(
            manager.notifications(),
            vec![(transaction.get_transaction_id(), 2)]
        );
        finish(&manager).await;
        transaction.dispose().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn first_multi_apply_failure_retains_all_originals_and_suppresses_remaining_apply() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let a = add(&point, &transaction, 10, 100);
    let b = add(&point, &transaction, 11, 110);
    point.inject_apply(ApplyPlan {
        result: Err(Status::InvalidOperation.error()),
        ..ApplyPlan::default()
    });
    let observer = transaction.commit();
    group_tick(&manager);
    manager.flush(Ok(()));
    assert!(manager.activity(4).apply.wait().await.is_err());
    assert_eq!(
        applies(&point),
        vec![(2, 4, transaction.get_transaction_id())]
    );
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        a.capture
    );
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        b.capture
    );
    assert!(point.versions(10).is_empty() && point.versions(11).is_empty());
    finish(&manager).await;
    code(observe(observer).await, Status::TransactionAborted);
    assert_eq!(unlocks(&point), vec![a.capture, b.capture]);
    assert_eq!(lsns(manager.map.unstable()), vec![4]);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_service_apply_unlock_and_log_failures_surface_exact_status_without_report_fault()
{
    for boundary in ["apply", "unlock", "flush"] {
        let (manager, point) = fixture_with(
            ProcessingRole::Recovery,
            0,
            Arc::new(RetryRuntime::default()),
        );
        if boundary == "apply" {
            point.inject_apply(ApplyPlan {
                result: Err(Status::InvalidOperation.error()),
                ..ApplyPlan::default()
            });
        } else if boundary == "unlock" {
            point.inject_unlock(Err(Status::InvalidOperation.error()));
        }
        let activity = manager
            .replay_record(replay_wire(begin_kind(800, true, 10, 100), 2), None)
            .unwrap();
        if boundary != "flush" {
            group_tick(&manager);
        }
        manager.flush(if boundary == "flush" {
            Err(Status::InvalidOperation.error())
        } else {
            Ok(())
        });
        activity.result.wait().await;
        settle().await;
        code(manager.recovery_result(), Status::InvalidOperation);
        assert!(manager.faults.reported().is_empty());
        if boundary == "flush" {
            assert!(point.events().is_empty());
            assert!(manager.notifications().is_empty());
        }
        if boundary == "unlock" {
            assert!(activity.node.context().is_none());
            assert_eq!(point.versions(10)[0].value, 100);
            assert!(manager.notifications().is_empty());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn only_delaydelete_apply_skips_and_stale_falseprogress_is_excluded() {
    for mode in [ProviderMode::DelayDelete, ProviderMode::Stale] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        let original = add(&point, &transaction, 10, 100);
        let observer = transaction.commit();
        manager.directory.set_mode(7, mode, None);
        group_tick(&manager);
        manager.flush(Ok(()));
        let result = manager.activity(2).apply.wait().await.as_ref().clone();
        if mode == ProviderMode::DelayDelete {
            result.unwrap();
        } else {
            code(result, Status::NotImplemented);
        }
        assert!(point.events().is_empty() && point.versions(10).is_empty());
        assert_eq!(
            point_capture(manager.activity(2).node.context().unwrap()),
            original.capture
        );
        finish(&manager).await;
        if mode == ProviderMode::DelayDelete {
            observe(observer).await.unwrap();
        } else {
            code(observe(observer).await, Status::TransactionAborted);
        }
        assert_eq!(unlocks(&point), vec![original.capture]);
        transaction.dispose().unwrap();
    }
    let (manager, point) = fixture();
    manager
        .directory
        .set_mode(7, ProviderMode::DelayDelete, None);
    code(
        observe(IStateProviderManager::apply(
            manager.directory.as_ref(),
            2,
            KernelTransaction::replay(901).unwrap(),
            ApplyContext::SecondaryFalseProgress,
            Some(named::encode(7, None).unwrap()),
            Some(PointProvider::payload(10, 100)),
        ))
        .await,
        Status::NotImplemented,
    );
    assert!(point.events().is_empty());
    let (manager, _) = fixture();
    manager
        .directory
        .error_if_state_provider_is_not_registered(REGISTRY_ID, 900)
        .unwrap();
    let transaction = manager.new_transaction().unwrap();
    Transaction::add_operation(transaction.as_ref(), None, None, None, REGISTRY_ID, None).unwrap();
    let observer = transaction.commit();
    code(observe(observer).await, Status::NotImplemented);
    assert!(manager.lower.attempts().is_empty() && log(&manager).is_empty());
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unsupported_atomic_redo_lifecycle_and_unknown_wire_reject_before_admission() {
    let (manager, point) = fixture();
    assert!(manager.create_atomic_operation().is_err());
    assert!(manager.create_atomic_redo_operation().is_err());
    let atomic = KernelTransaction::replay(-900).unwrap();
    assert!(atomic.get_is_atomic_operation() && atomic.get_is_valid_transaction());
    code(
        observe(crate::AtomicOperation::add_operation(
            atomic.as_ref(),
            None,
            None,
            None,
            7,
            None,
        ))
        .await,
        Status::NotImplemented,
    );
    code(
        observe(crate::AtomicRedoOperation::add_operation(
            atomic.as_ref(),
            None,
            None,
            7,
            None,
        ))
        .await,
        Status::NotImplemented,
    );
    code(
        observe(manager.add_atomic_operation(atomic.clone(), None, None, None, 7, None)).await,
        Status::NotImplemented,
    );
    code(
        observe(manager.add_atomic_redo_operation(atomic.clone(), None, None, 7, None)).await,
        Status::NotImplemented,
    );
    code(
        observe(
            manager
                .directory
                .add_atomic_operation(atomic.clone(), 7, None, None, None, None),
        )
        .await,
        Status::NotImplemented,
    );
    code(
        observe(
            manager
                .directory
                .add_atomic_redo_operation(atomic, 7, None, None, None),
        )
        .await,
        Status::NotImplemented,
    );
    point.prepare_checkpoint(1).unwrap();
    code(
        observe(point.open(Cancellation::default())).await,
        Status::NotImplemented,
    );
    code(
        IStateProviderManager::prepare_checkpoint(manager.directory.as_ref(), 1),
        Status::NotImplemented,
    );
    assert!(manager.lower.attempts().is_empty() && log(&manager).is_empty());
    let (secondary, _) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let wire = replay_wire(begin_kind(900, true, 10, 100), 2);
    let mut segments = wire.to_segments();
    segments[0][4..8].copy_from_slice(&6i32.to_le_bytes()); // Backup recognized codec kind, excluded.
    assert!(secondary
        .replay_record(OperationData::new(segments).unwrap(), Some(2))
        .is_err());
    assert!(log(&secondary).is_empty() && secondary.map.pending().is_empty());
    let redo = RecordKind::Operation {
        transaction_id: -901,
        redo_only: true,
        metadata: Some(named::encode(7, None).unwrap()),
        undo: None,
        redo: Some(PointProvider::payload(10, 100)),
    };
    code(
        secondary.replay_record(replay_wire(redo, 2), Some(2)),
        Status::NotImplemented,
    );
    assert!(log(&secondary).is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn cached_abort_activity_gate_failure_and_readonly_expiry_have_distinct_states() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let original = add(&point, &transaction, 10, 100);
    manager.close_activity_gate();
    code(
        ITransactionManager::single_operation_transaction_abort_unlock(
            manager.as_ref(),
            7,
            original.as_ref(),
        ),
        Status::ObjectClosed,
    );
    assert_eq!(transaction.get_state(), TransactionState::Active);
    drop(manager);
    transaction.abort().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Aborting);
    assert!(transaction.has_cached_first());
    assert!(point.events().is_empty());
    transaction.dispose().unwrap();
    assert!(!point.is_locked(10));
    let (manager, _) = fixture();
    let readonly = manager.new_transaction().unwrap();
    drop(manager);
    readonly.abort().unwrap(); // read-only worker does not upgrade manager.
    assert_eq!(readonly.get_state(), TransactionState::Aborted);
    readonly.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn supported_version_traits_retain_and_signal_real_reader_release() {
    let (manager, _) = fixture();
    let transaction = manager.new_transaction().unwrap();
    assert_eq!(
        observe(transaction.get_visibility_sequence_number())
            .await
            .unwrap(),
        1
    );
    let removal = manager.try_remove_version(7, 0, 2).unwrap();
    assert!(!removal.can_be_removed);
    assert_eq!(
        removal.enumeration_set.unwrap(),
        std::collections::BTreeSet::from([1])
    );
    let notifications = removal.enumeration_completion_notifications.unwrap();
    assert_eq!(notifications.len(), 1);
    assert!(!notifications[0].get_is_notification_completed());
    let checkpoint = manager.try_remove_checkpoint(0, 3);
    observe(transaction.commit()).await.unwrap();
    assert!(!notifications[0].get_is_notification_completed());
    transaction.dispose().unwrap();
    observe(checkpoint).await.unwrap();
    assert!(notifications[0].get_is_notification_completed());
    assert_eq!(
        observe(notifications[0].get_notification()).await.unwrap(),
        1
    );
    assert!(manager.try_remove_version(7, 0, 2).unwrap().can_be_removed);
}

#[tokio::test(flavor = "current_thread")]
async fn update_epoch_owned_prefix_waits_primary_gap_then_appends_at_tail_without_lower_submission()
{
    let (manager, point) = fixture();
    let a = manager.new_transaction().unwrap();
    let b = manager.new_transaction().unwrap();
    manager.lower.script([Ok(3), Ok(2)]);
    let ca = point.stage(a.as_ref(), 10, 100).unwrap();
    ITransactionManager::begin_transaction(
        manager.as_ref(),
        a.as_ref(),
        None,
        None,
        Some(PointProvider::payload(10, 100)),
        7,
        Some(ca),
    )
    .unwrap();
    let epoch = crate::Epoch {
        data_loss_number: 0,
        configuration_number: 1,
    };
    let observer = manager.update_epoch_async(epoch, 3, 42, 123);
    assert_eq!(manager.tail(), 1);
    assert!(log(&manager).is_empty());
    let cb = point.stage(b.as_ref(), 20, 200).unwrap();
    ITransactionManager::begin_transaction(
        manager.as_ref(),
        b.as_ref(),
        None,
        None,
        Some(PointProvider::payload(20, 200)),
        7,
        Some(cb),
    )
    .unwrap();
    settle().await;
    assert_eq!(manager.tail(), 3);
    assert_eq!(
        log(&manager)
            .iter()
            .map(|record| record.0)
            .collect::<Vec<_>>(),
        vec!['B', 'B', 'E']
    );
    assert_eq!(manager.lower.attempts().len(), 2);
    manager.flush(Ok(()));
    observe(observer).await.unwrap();
    let enda = manager.abort_transaction(a.clone());
    let endb = manager.abort_transaction(b.clone());
    group_tick(&manager);
    finish(&manager).await;
    observe(enda).await.unwrap();
    observe(endb).await.unwrap();
    a.dispose().unwrap();
    b.dispose().unwrap();
}

struct RegistryCapture(u64);
impl OperationContext for RegistryCapture {}
#[derive(Default)]
struct OpaqueRegistry {
    apply: Mutex<Vec<ApplyObservation>>,
    unlock: Mutex<Vec<u64>>,
}
impl RegistryCallbacks for OpaqueRegistry {
    fn apply(
        &self,
        lsn: i64,
        transaction: Arc<dyn TransactionBase>,
        _: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>> {
        self.apply.lock().unwrap().push((
            lsn,
            transaction.get_commit_sequence_number(),
            metadata,
            data,
        ));
        Box::pin(async {
            Ok(Some(
                Arc::new(RegistryCapture(2)) as Arc<dyn OperationContext>
            ))
        })
    }
    fn unlock(&self, context: &dyn OperationContext) -> Result<()> {
        let capture = (context as &dyn Any)
            .downcast_ref::<RegistryCapture>()
            .unwrap();
        self.unlock.lock().unwrap().push(capture.0);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_registry_apply_unlock_routes_opaque_callbacks_without_metadata_interpretation() {
    let registry = Arc::new(OpaqueRegistry::default());
    let directory = ProviderDirectory::preinitialized([], Some(registry.clone())).unwrap();
    let manager = KernelManager::established(
        directory,
        Arc::new(ControlledLower::new(2)),
        ProcessingRole::Primary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let transaction = manager.new_transaction().unwrap();
    let opaque = OperationData::new(vec![vec![0xff, 0x11], vec![]]).unwrap();
    Transaction::add_operation(
        transaction.as_ref(),
        Some(opaque.clone()),
        None,
        Some(opaque.clone()),
        REGISTRY_ID,
        Some(Arc::new(RegistryCapture(1))),
    )
    .unwrap();
    let observer = transaction.commit();
    group_tick(&manager);
    manager.flush(Ok(()));
    assert!(manager.activity(2).apply.wait().await.is_ok());
    assert_eq!(
        *registry.apply.lock().unwrap(),
        vec![(2, 2, Some(opaque.clone()), Some(opaque))]
    );
    assert!(registry.unlock.lock().unwrap().is_empty());
    finish(&manager).await;
    observe(observer).await.unwrap();
    assert_eq!(*registry.unlock.lock().unwrap(), vec![2]); // nonnull Apply replacement, not original1.
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_abort_owner_and_repeatable_observer_do_not_cancel_scheduled_work() {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    add(&point, &transaction, 11, 110);
    let weak = Arc::downgrade(&transaction);
    transaction.abort().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Aborting);
    drop(transaction);
    assert!(weak.upgrade().is_some());
    let reader = manager.new_transaction().unwrap();
    let read = point.repeatable(reader.clone(), 10);
    drop(read);
    assert!(point.is_locked(10));
    group_tick(&manager);
    finish(&manager).await;
    let transaction = weak.upgrade().unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Aborted);
    assert_eq!(unlocks(&point).len(), 2);
    assert!(manager.notifications().is_empty());
    // Dropped repeatable observer still completed/acquired its retained read lock.
    assert!(point.is_locked(10));
    reader.dispose().unwrap();
    assert!(!point.is_locked(10));
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn nonprimary_origin_rejects_first_add_before_registration_but_readonly_commit_can_succeed() {
    let (manager, point) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let transaction = manager.new_transaction().unwrap();
    assert!(!transaction.get_is_primary_transaction());
    code(
        Transaction::add_operation(transaction.as_ref(), None, None, None, 999, None),
        Status::TransactionNotActive,
    );
    assert!(!transaction.is_write() && !transaction.has_cached_first());
    assert!(manager.lower.attempts().is_empty() && log(&manager).is_empty());
    assert!(point.events().is_empty());
    observe(transaction.commit()).await.unwrap();
    assert_eq!(transaction.get_state(), TransactionState::Committed);
    assert_eq!(transaction.get_commit_sequence_number(), -1);
    transaction.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn second_unregistered_operation_admits_cached_begin_then_later_and_permanent_end_errors_keep_map(
) {
    let (manager, point) = fixture();
    let transaction = manager.new_transaction().unwrap();
    let original = add(&point, &transaction, 10, 100);
    code(
        Transaction::add_operation(transaction.as_ref(), None, None, None, 999, None),
        Status::InvalidParameter,
    );
    assert!(!transaction.has_cached_first());
    assert_eq!(transaction.get_state(), TransactionState::Active);
    assert_eq!(manager.lower.attempts().len(), 1);
    assert_eq!(
        log(&manager),
        vec![('B', 2, Some(transaction.get_transaction_id()))]
    );
    code(
        Transaction::add_operation(transaction.as_ref(), None, None, None, 999, None),
        Status::InvalidParameter,
    );
    assert_eq!(
        manager
            .map
            .latest(transaction.get_transaction_id())
            .unwrap()
            .lsn(),
        2
    );
    assert_eq!(
        point_capture(manager.activity(2).node.context().unwrap()),
        original.capture
    );
    manager
        .lower
        .script([Err(Status::OperationTooLarge.error())]);
    code(
        observe(transaction.commit()).await,
        Status::OperationTooLarge,
    );
    assert_eq!(transaction.get_state(), TransactionState::Faulted);
    assert_eq!(lsns(manager.map.pending()), vec![2]);
    assert!(manager.map.completed().is_empty() && manager.map.unstable().is_empty());
    assert!(point.events().is_empty());
    transaction.dispose().unwrap();
    assert!(!point.is_locked(10));
    group_tick(&manager);
    finish(&manager).await;
    assert_eq!(lsns(manager.map.pending()), vec![2]); // no fake rollback/map erase on Dispose or Faulted.
    assert!(manager.activity(2).node.context().is_some());
    assert!(unlocks(&point).is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn abort_worker_permanent_admission_or_accepted_failure_faults_without_repairing_canonical_state(
) {
    for boundary in ["admission", "flush", "quorum"] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        let a = add(&point, &transaction, 10, 100);
        let b = add(&point, &transaction, 11, 110);
        if boundary == "admission" {
            manager.lower.script([Err(Status::NotPrimary.error())]);
        }
        transaction.abort().unwrap();
        if boundary == "admission" {
            assert_eq!(transaction.get_state(), TransactionState::Faulted);
            assert_eq!(log(&manager).len(), 2);
            assert_eq!(lsns(manager.map.pending()), vec![2]);
            assert!(point.events().is_empty());
        } else {
            assert_eq!(transaction.get_state(), TransactionState::Aborting);
            assert_eq!(log(&manager)[2].0, 'A');
            assert!(manager.map.pending().is_empty());
            if boundary != "flush" {
                group_tick(&manager);
            }
            manager.flush(if boundary == "flush" {
                Err(Status::ObjectClosed.error())
            } else {
                Ok(())
            });
            assert!(manager.activity(4).apply.is_completed());
            manager.replication(
                4,
                if boundary == "quorum" {
                    Err(Status::NoWriteQuorum.error())
                } else {
                    Ok(())
                },
            );
            manager.activity(4).result.wait().await;
            settle().await;
            assert_eq!(transaction.get_state(), TransactionState::Faulted);
            assert_eq!(unlocks(&point), vec![a.capture, b.capture]);
            assert_eq!(lsns(manager.map.completed()), vec![2]);
            assert_eq!(lsns(manager.map.unstable()), vec![4]);
            assert!(applies(&point).is_empty() && manager.notifications().is_empty());
        }
        transaction.dispose().unwrap();
        if boundary == "admission" {
            group_tick(&manager);
        }
        finish(&manager).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_notification_callback_is_after_unlock_traversal_before_public_success_even_on_quorum_error(
) {
    for fail in [false, true] {
        let (manager, point) = fixture();
        let transaction = manager.new_transaction().unwrap();
        add(&point, &transaction, 10, 100);
        add(&point, &transaction, 11, 110);
        let notified = Arc::new(Mutex::new(Vec::new()));
        let capture = notified.clone();
        let callback_point = point.clone();
        manager.set_commit_callback(Some(Arc::new(move |transaction| {
            assert_eq!(unlocks(&callback_point).len(), 2);
            assert!(!callback_point.is_locked(10));
            assert_eq!(transaction.get_state(), TransactionState::Committing);
            capture
                .lock()
                .unwrap()
                .push(transaction.get_commit_sequence_number());
        })));
        let observer = transaction.commit();
        group_tick(&manager);
        manager.flush(Ok(()));
        assert!(manager.activity(4).apply.wait().await.is_ok());
        assert!(notified.lock().unwrap().is_empty());
        manager.replication(
            4,
            if fail {
                Err(Status::NoWriteQuorum.error())
            } else {
                Ok(())
            },
        );
        if fail {
            code(observe(observer).await, Status::TransactionAborted);
        } else {
            observe(observer).await.unwrap();
        }
        assert_eq!(*notified.lock().unwrap(), vec![4]);
        assert_eq!(
            transaction.get_state(),
            if fail {
                TransactionState::Faulted
            } else {
                TransactionState::Committed
            }
        );
        manager.set_commit_callback(None);
        finish(&manager).await;
        transaction.dispose().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retry_cancel_precedes_elapsed_sampling_and_accepted_error_is_not_cancelled() {
    let ticks = Arc::new(Mutex::new(0));
    let clock = ticks.clone();
    let retry = Arc::new(RetryRuntime {
        now: Arc::new(move || {
            let mut count = clock.lock().unwrap();
            *count += 1;
            Duration::ZERO
        }),
        delay: Arc::new(|_| panic!("cancelled classified failure must not start timer")),
    });
    let (manager, point) = fixture_with(ProcessingRole::Primary, 0, retry);
    let transaction = manager.new_transaction().unwrap();
    add(&point, &transaction, 10, 100);
    let token = Cancellation::default();
    token.cancel();
    manager.lower.script([Err(Status::QueueFull.error())]);
    code(
        observe(transaction.commit_with_options(Some(Duration::ZERO), token.clone())).await,
        Status::Cancelled,
    );
    assert_eq!(*ticks.lock().unwrap(), 1); // watch start only; cancellation short-circuits elapsed.
    point.inject_apply(ApplyPlan {
        result: Err(Status::InvalidOperation.error()),
        ..ApplyPlan::default()
    });
    let observer = transaction.commit_with_options(Some(Duration::ZERO), token);
    group_tick(&manager);
    manager.flush(Ok(()));
    assert!(manager.activity(2).apply.wait().await.is_err());
    manager.replication(2, Ok(()));
    code(observe(observer).await, Status::TransactionAborted);
    assert_eq!(*ticks.lock().unwrap(), 2); // success/terminal accepted result does not inspect deadline.
    assert_eq!(transaction.get_state(), TransactionState::Faulted);
    transaction.dispose().unwrap();
}

struct FallibleLock {
    first_failure: Mutex<bool>,
    label: u64,
    calls: Arc<Mutex<Vec<u64>>>,
}
impl OperationContext for FallibleLock {}
impl LockContext for FallibleLock {
    fn is_tracking_context(&self) -> bool {
        false
    }
    fn unlock(&self) -> Result<()> {
        self.calls.lock().unwrap().push(self.label);
        let mut failure = self.first_failure.lock().unwrap();
        if *failure {
            *failure = false;
            Err(Status::InvalidOperation.error())
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn base_nontracking_dispose_failure_stops_without_marking_disposed_or_unlocking_later_locks()
{
    // TransactionBase.cpp:161–179 has no catch/continue around LockContext::Unlock.
    let (manager, _) = fixture();
    let transaction = manager.new_transaction().unwrap();
    observe(transaction.commit()).await.unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    transaction
        .add_lock_context(Arc::new(FallibleLock {
            first_failure: Mutex::new(true),
            label: 1,
            calls: calls.clone(),
        }))
        .unwrap();
    transaction
        .add_lock_context(Arc::new(FallibleLock {
            first_failure: Mutex::new(false),
            label: 2,
            calls: calls.clone(),
        }))
        .unwrap();
    code(transaction.dispose(), Status::InvalidOperation);
    assert_eq!(*calls.lock().unwrap(), vec![1]);
    assert!(!transaction.is_disposed());
    assert!(manager.versions.registered_versions().is_empty());
    assert_eq!(transaction.get_state(), TransactionState::Committed);
    transaction.dispose().unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![1, 1, 2]);
    assert!(transaction.is_disposed());
}

#[tokio::test(flavor = "current_thread")]
async fn overlapping_barrier_generations_do_not_sample_through_unfinished_apply() {
    let (manager, point) = fixture();
    let a = manager.new_transaction().unwrap();
    let b = manager.new_transaction().unwrap();
    add(&point, &a, 10, 100);
    add(&point, &b, 20, 200);
    let agate = Completion::new();
    let bgate = Completion::new();
    let r = manager.new_transaction().unwrap();
    let entering_reader = r.clone();
    point.inject_apply(ApplyPlan {
        gate: Some(agate.clone()),
        on_enter: Some(Arc::new(move || {
            drop(entering_reader.get_visibility_sequence_number());
            assert_eq!(entering_reader.get_state(), TransactionState::Reading);
        })),
        ..ApplyPlan::default()
    });
    point.inject_apply(ApplyPlan {
        gate: Some(bgate.clone()),
        ..ApplyPlan::default()
    });
    let ac = a.commit();
    group_tick(&manager); // single2, barrier3
    manager.flush(Ok(()));
    assert_eq!(applies(&point), vec![(2, 2, a.get_transaction_id())]);
    assert_eq!(manager.version(), 1);
    // The reader was admitted inside first Apply, observing the installed fence.
    assert_eq!(r.get_state(), TransactionState::Reading);
    let bc = b.commit();
    group_tick(&manager); // single4, barrier5
    manager.flush(Ok(()));
    assert_eq!(manager.version(), 1);
    assert_eq!(applies(&point).len(), 1); // Previous group still owns dispatcher.
    agate.complete(Ok(()));
    assert!(manager.activity(3).apply.wait().await.is_ok());
    settle().await;
    assert_eq!(
        observe(r.get_visibility_sequence_number()).await.unwrap(),
        3
    );
    assert_eq!(applies(&point).len(), 2);
    assert_eq!(manager.version(), 3);
    assert!(!manager.activity(5).apply.is_completed());
    assert_eq!(point.snapshot(r.as_ref(), 20, 3).unwrap(), None);
    let next = manager.new_transaction().unwrap();
    let read_next = next.get_visibility_sequence_number();
    assert_eq!(next.get_state(), TransactionState::Reading);
    bgate.complete(Ok(()));
    assert_eq!(observe(read_next).await.unwrap(), 5);
    assert_eq!(point.snapshot(next.as_ref(), 20, 5).unwrap(), Some(200));
    assert_eq!(point.snapshot(r.as_ref(), 20, 3).unwrap(), None);
    assert_eq!(manager.versions.registered_versions(), vec![3, 5]);
    assert!(unlocks(&point).is_empty());
    assert!(!manager.activity(2).replication.is_completed());
    assert!(!manager.activity(4).replication.is_completed());
    assert_eq!(manager.stable(), 0);
    finish(&manager).await;
    observe(ac).await.unwrap();
    observe(bc).await.unwrap();
    for transaction in [a, b, r, next] {
        transaction.dispose().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn multi_partial_apply_keeps_old_snapshot_and_fences_new_read_before_primary_quorum() {
    let (manager, point) = fixture();
    let seed = manager.new_transaction().unwrap();
    add(&point, &seed, 10, 100);
    add(&point, &seed, 11, 110);
    let seed_commit = seed.commit();
    group_tick(&manager);
    finish(&manager).await;
    observe(seed_commit).await.unwrap();
    seed.dispose().unwrap();
    let old = manager.new_transaction().unwrap();
    assert_eq!(
        observe(old.get_visibility_sequence_number()).await.unwrap(),
        5
    );
    let writer = manager.new_transaction().unwrap();
    add(&point, &writer, 10, 101);
    add(&point, &writer, 11, 111);
    let second = Completion::new();
    point.inject_apply(ApplyPlan::default());
    point.inject_apply(ApplyPlan {
        gate: Some(second.clone()),
        ..ApplyPlan::default()
    });
    let commit = writer.commit();
    group_tick(&manager); // begin6, operation7, end8, barrier9
    manager.flush(Ok(()));
    assert_eq!(point.versions(10).last().unwrap().commit, 8);
    assert_eq!(point.versions(11).len(), 1);
    assert!(!manager.activity(8).apply.is_completed());
    assert_eq!(manager.version(), 5);
    assert_eq!(point.snapshot(old.as_ref(), 10, 5).unwrap(), Some(100));
    assert_eq!(point.snapshot(old.as_ref(), 11, 5).unwrap(), Some(110));
    let new = manager.new_transaction().unwrap();
    let visibility = new.get_visibility_sequence_number();
    assert_eq!(new.get_state(), TransactionState::Reading);
    let repeatable = manager.new_transaction().unwrap();
    code(
        point.try_repeatable(repeatable.as_ref(), 10),
        Status::ServiceTooBusy,
    );
    let read = point.repeatable(repeatable.clone(), 10);
    second.complete(Ok(()));
    assert_eq!(observe(visibility).await.unwrap(), 9);
    assert_eq!(point.snapshot(new.as_ref(), 10, 9).unwrap(), Some(101));
    assert_eq!(point.snapshot(new.as_ref(), 11, 9).unwrap(), Some(111));
    assert_eq!(point.snapshot(old.as_ref(), 10, 5).unwrap(), Some(100));
    assert_eq!(point.snapshot(old.as_ref(), 11, 5).unwrap(), Some(110));
    assert!(!manager.activity(8).replication.is_completed());
    assert!(point.is_locked(10) && point.is_locked(11));
    assert_eq!(manager.stable(), 5);
    manager.replication(8, Ok(()));
    observe(commit).await.unwrap();
    assert_eq!(observe(read).await.unwrap(), Some(101));
    finish(&manager).await;
    for transaction in [writer, old, new, repeatable] {
        transaction.dispose().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn failed_batch_dispatches_buffered_success_prefix_before_immediate_failed_records() {
    let (manager, point) = fixture();
    let a = manager.new_transaction().unwrap();
    let b = manager.new_transaction().unwrap();
    add(&point, &a, 10, 100);
    let b_original = add(&point, &b, 20, 200);
    let ac = a.commit();
    manager.flush(Ok(()));
    manager.replication(2, Ok(()));
    settle().await;
    assert!(point.events().is_empty());
    assert!(!manager.activity(2).apply.is_completed());
    assert!(!manager.activity(2).result.is_completed());
    let gate = Completion::new();
    point.inject_apply(ApplyPlan {
        gate: Some(gate.clone()),
        ..ApplyPlan::default()
    });
    let bc = b.commit();
    manager.flush(Err(Status::InvalidOperation.error()));
    assert_eq!(applies(&point), vec![(2, 2, a.get_transaction_id())]);
    assert!(!manager.activity(3).apply.is_completed());
    assert!(manager.faults.is_closing());
    assert_eq!(manager.faults.reported().len(), 1);
    assert_eq!(manager.version(), 1);
    assert_eq!(
        log(&manager).iter().map(|r| r.0).collect::<Vec<_>>(),
        vec!['S', 'S']
    );
    gate.complete(Ok(()));
    observe(ac).await.unwrap();
    code(
        manager.activity(3).apply.wait().await.as_ref().clone(),
        Status::InvalidOperation,
    );
    assert!(manager.activity(3).processing.is_completed());
    assert!(!manager.activity(3).result.is_completed());
    assert_eq!(point.versions(10)[0].value, 100);
    assert!(point.versions(20).is_empty());
    assert_eq!(
        point_capture(manager.activity(3).node.context().unwrap()),
        b_original.capture
    );
    manager.replication(3, Ok(()));
    code(observe(bc).await, Status::TransactionAborted);
    assert_eq!(unlocks(&point).len(), 2);
    assert_eq!(manager.notifications().len(), 2); // Primary failure cleanup still owns Unlock.
    a.dispose().unwrap();
    b.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn secondary_unlock_failure_is_inside_group_and_suppresses_following_barrier_stability() {
    let (manager, point) = fixture_with(
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let single = manager
        .replay_record(replay_wire(begin_kind(1300, true, 10, 100), 2), None)
        .unwrap();
    let barrier = manager
        .replay_record(
            replay_wire(RecordKind::Barrier { last_stable_lsn: 2 }, 3),
            None,
        )
        .unwrap();
    point.inject_unlock(Err(Status::InvalidOperation.error()));
    manager.flush(Ok(()));
    assert!(single.apply.wait().await.is_ok());
    single.processing.wait().await;
    code(
        barrier.apply.wait().await.as_ref().clone(),
        Status::InvalidOperation,
    );
    barrier.processing.wait().await;
    assert_eq!(manager.stable(), 0);
    assert!(single.node.context().is_none());
    assert_eq!(unlocks(&point).len(), 1);
    assert_eq!(point.versions(10)[0].value, 100);
    assert!(manager.notifications().is_empty());
    assert!(manager.faults.is_closing());
    assert_eq!(manager.faults.reported().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn nonprimary_failed_flush_has_no_apply_unlock_or_commit_notification_for_single_or_end() {
    for role in [ProcessingRole::Secondary, ProcessingRole::Recovery] {
        for single in [true, false] {
            let (manager, point) = fixture_with(role, 0, Arc::new(RetryRuntime::default()));
            manager
                .replay_record(replay_wire(begin_kind(1400, single, 10, 100), 2), None)
                .unwrap();
            if !single {
                manager
                    .replay_record(replay_wire(op_kind(1400, 11, 110), 3), None)
                    .unwrap();
                manager
                    .replay_record(
                        replay_wire(
                            RecordKind::End {
                                transaction_id: 1400,
                                committed: true,
                            },
                            4,
                        ),
                        None,
                    )
                    .unwrap();
            }
            manager.flush(Err(Status::InvalidOperation.error()));
            for activity in manager.records() {
                code(
                    activity.result.wait().await.as_ref().clone(),
                    Status::InvalidOperation,
                );
                assert!(activity.processing.is_completed());
                assert_eq!(
                    activity
                        .node
                        .transaction()
                        .unwrap()
                        .get_commit_sequence_number(),
                    -1
                );
            }
            assert!(point.events().is_empty());
            assert!(manager.notifications().is_empty());
            assert_eq!(
                manager.faults.reported().len(),
                usize::from(role != ProcessingRole::Recovery)
            );
            assert_eq!(
                manager.faults.is_closing(),
                role != ProcessingRole::Recovery
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_apply_failure_returns_first_latched_payload_without_overriding_inflight_success(
) {
    for second_fails in [false, true] {
        let (manager, point) = fixture();
        let a = manager.new_transaction().unwrap();
        let b = manager.new_transaction().unwrap();
        add(&point, &a, 10, 100);
        add(&point, &b, 20, 200);
        let agate = Completion::new();
        let bgate = Completion::new();
        let first = Error::Remote {
            code: Status::InvalidOperation.code(),
            message: "first payload".into(),
        };
        let second = Error::Remote {
            code: Status::InvalidParameter.code(),
            message: "second payload".into(),
        };
        point.inject_apply(ApplyPlan {
            gate: Some(agate.clone()),
            result: Err(first.clone()),
            ..ApplyPlan::default()
        });
        point.inject_apply(ApplyPlan {
            gate: Some(bgate.clone()),
            result: if second_fails {
                Err(second.clone())
            } else {
                Ok(())
            },
            ..ApplyPlan::default()
        });
        let ac = a.commit();
        let bc = b.commit();
        group_tick(&manager);
        manager.flush(Ok(()));
        assert_eq!(applies(&point).len(), 2);
        agate.complete(Ok(()));
        let error = manager
            .activity(2)
            .apply
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap_err();
        assert_eq!(error.to_string(), first.to_string());
        assert!(manager.faults.is_closing());
        assert!(!manager.activity(3).apply.is_completed());
        bgate.complete(Ok(()));
        let result = manager.activity(3).apply.wait().await.as_ref().clone();
        if second_fails {
            let error = result.unwrap_err();
            assert_eq!(error.native_code(), first.native_code());
            assert_eq!(error.to_string(), first.to_string());
        } else {
            result.unwrap();
            assert_eq!(point.versions(20)[0].value, 200);
        }
        assert_eq!(
            manager.faults.service_error().unwrap().to_string(),
            first.to_string()
        );
        assert_eq!(manager.faults.reported().len(), 1);
        let observed = manager.faults.observed();
        assert_eq!(observed[0].to_string(), first.to_string());
        assert_eq!(observed.len(), if second_fails { 2 } else { 1 });
        if second_fails {
            assert_eq!(observed[1].to_string(), second.to_string());
        }
        finish(&manager).await;
        code(observe(ac).await, Status::TransactionAborted);
        if second_fails {
            code(observe(bc).await, Status::TransactionAborted);
        } else {
            observe(bc).await.unwrap();
        }
        a.dispose().unwrap();
        b.dispose().unwrap();
    }
}

fn error_representations(status: Status) -> Vec<Error> {
    let hr = status.code() as u32;
    let native = if status == Status::Cancelled {
        0xc0000120
    } else {
        (hr & 0xffff) | 0xe0430000
    };
    let mut errors = vec![status.error()];
    errors.extend(
        [
            hr as i32 as i64,
            i64::from(hr),
            native as i32 as i64,
            i64::from(native),
        ]
        .into_iter()
        .map(|code| Error::Remote {
            code,
            message: format!("original {code}"),
        }),
    );
    let kind = match status {
        Status::NoWriteQuorum => Some(fube_runtime::ErrorKind::NoWriteQuorum),
        Status::NotPrimary => Some(fube_runtime::ErrorKind::NotPrimary),
        Status::ReconfigurationPending => Some(fube_runtime::ErrorKind::ReconfigurationPending),
        Status::QueueFull => Some(fube_runtime::ErrorKind::ReplicationQueueFull),
        Status::ObjectClosed => Some(fube_runtime::ErrorKind::ObjectClosed),
        Status::OperationTooLarge => Some(fube_runtime::ErrorKind::ReplicationOperationTooLarge),
        Status::Cancelled => Some(fube_runtime::ErrorKind::Canceled),
        _ => None,
    };
    if let Some(kind) = kind {
        errors.push(Error::Classified {
            kind,
            message: "typed original".into(),
            cause: None,
        });
    }
    errors
}

#[tokio::test(flavor = "current_thread")]
async fn expected_replication_status_representations_preserve_admission_and_quorum_payload_without_fault(
) {
    for status in [
        Status::NoWriteQuorum,
        Status::NotPrimary,
        Status::ReconfigurationPending,
        Status::QueueFull,
        Status::ObjectClosed,
        Status::OperationTooLarge,
        Status::Cancelled,
    ] {
        for error in error_representations(status) {
            let (manager, point) = fixture();
            let transaction = manager.new_transaction().unwrap();
            manager.lower.script([Err(error.clone())]);
            let returned = observe(ITransactionManager::begin_transaction_async(
                manager.as_ref(),
                transaction.clone(),
                None,
                None,
                Some(PointProvider::payload(10, 100)),
                7,
                None,
            ))
            .await
            .unwrap_err();
            assert_eq!(returned.native_code(), error.native_code());
            assert_eq!(returned.to_string(), error.to_string());
            assert!(!manager.faults.is_closing() && manager.faults.reported().is_empty());
            assert!(log(&manager).is_empty());
            assert_eq!(transaction.get_commit_sequence_number(), -1);
            add(&point, &transaction, 10, 100);
            let observer = transaction.commit();
            group_tick(&manager);
            manager.flush(Ok(()));
            assert!(manager.activity(2).apply.wait().await.is_ok());
            manager.replication(2, Err(error.clone()));
            let recorded = manager
                .activity(2)
                .replication
                .wait()
                .await
                .as_ref()
                .clone()
                .unwrap_err();
            assert_eq!(recorded.to_string(), error.to_string());
            assert_eq!(recorded.native_code(), error.native_code());
            code(observe(observer).await, Status::TransactionAborted);
            assert!(!manager.faults.is_closing() && manager.faults.reported().is_empty());
            assert_eq!(point.versions(10)[0].value, 100);
            assert_eq!(unlocks(&point).len(), 1);
            finish(&manager).await;
            transaction.dispose().unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn objectclosed_flush_representations_keep_payload_without_fault_or_replay_notification() {
    for error in error_representations(Status::ObjectClosed) {
        for role in [
            ProcessingRole::Primary,
            ProcessingRole::Secondary,
            ProcessingRole::Recovery,
        ] {
            let (manager, point) = fixture_with(role, 0, Arc::new(RetryRuntime::default()));
            let transaction = manager.new_transaction().unwrap();
            let observer = if role == ProcessingRole::Primary {
                add(&point, &transaction, 10, 100);
                Some(transaction.commit())
            } else {
                manager
                    .replay_record(replay_wire(begin_kind(1500, true, 10, 100), 2), None)
                    .unwrap();
                None
            };
            manager.flush(Err(error.clone()));
            let activity = manager.activity(2);
            let apply = activity.apply.wait().await.as_ref().clone().unwrap_err();
            assert_eq!(apply.native_code(), error.native_code());
            assert_eq!(apply.to_string(), error.to_string());
            assert_eq!(
                manager.faults.log_error().unwrap().to_string(),
                error.to_string()
            );
            assert!(!manager.faults.is_closing() && manager.faults.reported().is_empty());
            assert!(point.events().is_empty() && manager.notifications().is_empty());
            if let Some(observer) = observer {
                manager.replication(2, Ok(()));
                code(observe(observer).await, Status::TransactionAborted);
                assert_eq!(unlocks(&point).len(), 1);
            } else {
                let recorded = activity.result.wait().await.as_ref().clone().unwrap_err();
                assert_eq!(recorded.to_string(), error.to_string());
                assert!(manager.notifications().is_empty());
            }
            transaction.dispose().unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unexpected_lower_busy_and_resource_exhaustion_close_before_next_admission_and_report_once()
{
    let mut errors = error_representations(Status::ServiceTooBusy);
    errors.push(Error::ResourceLimit("not transient queue pressure"));
    for error in errors {
        let (manager, point) = fixture();
        let a = manager.new_transaction().unwrap();
        let b = manager.new_transaction().unwrap();
        manager.lower.script([Err(error.clone())]);
        let returned = observe(ITransactionManager::begin_transaction_async(
            manager.as_ref(),
            a.clone(),
            None,
            None,
            Some(PointProvider::payload(10, 100)),
            7,
            None,
        ))
        .await
        .unwrap_err();
        assert_eq!(returned.to_string(), error.to_string());
        assert_eq!(
            super::status::retryable(&returned),
            Status::ServiceTooBusy.matches(&error)
        );
        assert!(manager.faults.is_closing());
        assert_eq!(manager.faults.reported().len(), 1);
        let returned = observe(ITransactionManager::begin_transaction_async(
            manager.as_ref(),
            b.clone(),
            None,
            None,
            Some(PointProvider::payload(20, 200)),
            7,
            None,
        ))
        .await;
        code(returned, Status::ObjectClosed);
        assert_eq!(manager.lower.attempts().len(), 1);
        assert!(log(&manager).is_empty() && point.events().is_empty());
        manager
            .faults
            .process_service_error(ProcessingRole::Primary, Status::InvalidOperation.error());
        assert_eq!(manager.faults.reported().len(), 1);
        // Closing lower writes is not closing public activities or cached abort.
        add(&point, &a, 10, 100);
        a.abort().unwrap();
        assert_eq!(a.get_state(), TransactionState::Aborted);
        a.dispose().unwrap();
        b.dispose().unwrap();

        let (manager, point) = fixture();
        let a = manager.new_transaction().unwrap();
        let b = manager.new_transaction().unwrap();
        add(&point, &a, 10, 100);
        add(&point, &b, 20, 200);
        let ac = a.commit();
        let bc = b.commit();
        group_tick(&manager);
        manager.flush(Ok(()));
        assert!(manager.activity(2).apply.wait().await.is_ok());
        assert!(manager.activity(3).apply.wait().await.is_ok());
        manager.replication(2, Err(error.clone()));
        assert!(manager.faults.is_closing());
        assert_eq!(manager.faults.reported().len(), 1);
        assert_eq!(manager.faults.reported()[0].to_string(), error.to_string());
        manager.replication(3, Err(Error::ResourceLimit("second unexpected error")));
        assert_eq!(manager.faults.reported().len(), 1);
        code(observe(ac).await, Status::TransactionAborted);
        code(observe(bc).await, Status::TransactionAborted);
        assert_eq!(point.versions(10)[0].value, 100);
        assert_eq!(point.versions(20)[0].value, 200);
        assert_eq!(unlocks(&point).len(), 2);
        let later = manager.new_transaction().unwrap();
        add(&point, &later, 30, 300);
        let attempts = manager.lower.attempts().len();
        code(observe(later.commit()).await, Status::ObjectClosed);
        assert_eq!(manager.lower.attempts().len(), attempts);
        finish(&manager).await;
        for transaction in [a, b, later] {
            transaction.dispose().unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn log_fault_closes_new_lower_admission_but_retains_already_accepted_primary_work() {
    let (manager, point) = fixture();
    let first = manager.new_transaction().unwrap();
    let later = manager.new_transaction().unwrap();
    let original = add(&point, &first, 10, 100);
    add(&point, &later, 20, 200);
    let first_commit = first.commit();
    manager.flush(Err(Status::InvalidOperation.error()));
    assert!(manager.faults.is_closing());
    let attempts = manager.lower.attempts().len();
    code(observe(later.commit()).await, Status::ObjectClosed);
    assert_eq!(manager.lower.attempts().len(), attempts);
    assert_eq!(log(&manager).len(), 1);
    assert_eq!(later.get_commit_sequence_number(), -1);
    assert!(point.events().is_empty());
    manager.replication(2, Err(Status::ServiceTooBusy.error()));
    code(observe(first_commit).await, Status::TransactionAborted);
    assert_eq!(unlocks(&point), vec![original.capture]);
    assert_eq!(manager.faults.reported().len(), 1);
    first.dispose().unwrap();
    later.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_point_write_never_publishes_rejected_key_or_replaces_accepted_own_value() {
    let (manager, point) = fixture();
    let new = manager.new_transaction().unwrap();
    code(
        point.write(new.as_ref(), 999, 30, 300),
        Status::InvalidParameter,
    );
    assert_eq!(point.snapshot(new.as_ref(), 30, 1).unwrap(), None);
    assert_eq!(point.try_repeatable(new.as_ref(), 30).unwrap(), None);
    assert!(!new.is_write() && log(&manager).is_empty());
    assert!(point.is_locked(30)); // Native lock acquisition is not undone.
    new.dispose().unwrap();
    for boundary in ["cached-begin", "after-begin"] {
        for key in [10, 11] {
            let (manager, point) = fixture();
            let transaction = manager.new_transaction().unwrap();
            let original = add(&point, &transaction, 10, 100);
            if boundary == "cached-begin" {
                manager.lower.script([Err(Status::QueueFull.error())]);
                code(
                    point.write(transaction.as_ref(), 7, key, 200),
                    Status::QueueFull,
                );
                assert!(transaction.has_cached_first() && log(&manager).is_empty());
            } else {
                code(
                    point.write(transaction.as_ref(), 999, key, 200),
                    Status::InvalidParameter,
                );
                assert!(!transaction.has_cached_first());
                assert_eq!(log(&manager).len(), 1);
                assert_eq!(lsns(manager.map.pending()), vec![2]);
                assert_eq!(
                    point_capture(manager.activity(2).node.context().unwrap()),
                    original.capture
                );
            }
            let own = if key == 10 { Some(100) } else { None };
            assert_eq!(point.snapshot(transaction.as_ref(), key, 1).unwrap(), own);
            assert_eq!(
                point.try_repeatable(transaction.as_ref(), key).unwrap(),
                own
            );
            assert_eq!(
                point.snapshot(transaction.as_ref(), 10, 1).unwrap(),
                Some(100)
            );
            let observer = transaction.commit();
            group_tick(&manager);
            finish(&manager).await;
            observe(observer).await.unwrap();
            assert_eq!(point.versions(10)[0].value, 100);
            assert!(point.versions(11).is_empty());
            transaction.dispose().unwrap();
        }
    }
}

// Native IsRegistered uses active metadata (StateManager.cpp:4566–4620;
// MetadataManager.cpp:305–323,928–982), not retained deleted providers.
#[tokio::test(flavor = "current_thread")]
async fn final_deleted_registration_rejects_before_cache_or_logged_admission() {
    for (mode, expected) in [
        (ProviderMode::DelayDelete, Status::InvalidParameter),
        (ProviderMode::Stale, Status::NotImplemented),
    ] {
        let (manager, point) = fixture();
        let tx = manager.new_transaction().unwrap();
        manager.directory.set_mode(7, mode, None);
        code(
            tx.add_operation(None, None, Some(PointProvider::payload(10, 100)), 7, None),
            expected,
        );
        assert!(!tx.is_write() && !tx.has_cached_first());
        assert!(manager.lower.attempts().is_empty());
        assert!(log(&manager).is_empty() && manager.map.pending().is_empty());
        assert!(point.events().is_empty() && manager.notifications().is_empty());
        assert!(manager.faults.reported().is_empty());
        tx.abort().unwrap();
        tx.dispose().unwrap();

        let (manager, point) = fixture();
        let tx = manager.new_transaction().unwrap();
        tx.add_operation(None, None, Some(PointProvider::payload(10, 100)), 7, None)
            .unwrap();
        manager.directory.set_mode(7, mode, None);
        // Logged Begin repeats registration; rejected Begin keeps cached first.
        code(
            tx.add_operation(None, None, Some(PointProvider::payload(11, 110)), 7, None),
            expected,
        );
        assert!(tx.has_cached_first() && log(&manager).is_empty());
        assert!(manager.lower.attempts().is_empty() && manager.map.pending().is_empty());
        // Single commit has the same source registration gate.
        code(observe(tx.commit()).await, expected);
        assert_eq!(tx.get_state(), TransactionState::Faulted);
        assert!(point.events().is_empty() && manager.notifications().is_empty());
        assert!(manager.faults.reported().is_empty());
        tx.dispose().unwrap();

        let (manager, point) = fixture();
        let tx = manager.new_transaction().unwrap();
        tx.add_operation(None, None, Some(PointProvider::payload(10, 100)), 7, None)
            .unwrap();
        tx.add_operation(None, None, Some(PointProvider::payload(11, 110)), 7, None)
            .unwrap();
        let prior = log(&manager);
        let attempts = manager.lower.attempts().len();
        manager.directory.set_mode(7, mode, None);
        code(
            tx.add_operation(None, None, Some(PointProvider::payload(12, 120)), 7, None),
            expected,
        );
        assert_eq!(log(&manager), prior);
        assert_eq!(manager.lower.attempts().len(), attempts);
        assert_eq!(
            manager.map.latest(tx.get_transaction_id()).unwrap().lsn(),
            3
        );
        assert!(point.events().is_empty() && manager.notifications().is_empty());
        assert!(manager.faults.reported().is_empty());
        manager.directory.set_mode(7, ProviderMode::Active, None);
        tx.abort().unwrap();
        group_tick(&manager);
        finish(&manager).await;
        tx.dispose().unwrap();
    }
}

// A0 deliberately excludes native-supported managed named formats. Codec
// recognition may advance ID high water, but execution rejects before append,
// even when covered-prefix processing would otherwise avoid provider Apply.
#[tokio::test(flavor = "current_thread")]
async fn final_replay_native_profile_gate_precedes_append_even_in_covered_prefix() {
    for covered in [0, 10] {
        let (manager, point) = fixture_with(
            ProcessingRole::Secondary,
            covered,
            Arc::new(RetryRuntime::default()),
        );
        let mut header = 1i32.to_le_bytes().to_vec();
        header.extend_from_slice(&7i64.to_le_bytes());
        header.push(1);
        let managed = OperationData::new(vec![header]).unwrap();
        let unsupported = RecordKind::Begin {
            transaction_id: 5001,
            single: true,
            metadata: Some(managed.clone()),
            redo: Some(PointProvider::payload(10, 100)),
            undo: None,
        };
        assert!(manager
            .replay_record(replay_wire(unsupported, 2), None)
            .is_err());
        assert_eq!(manager.tail(), 1);
        assert!(log(&manager).is_empty() && manager.map.pending().is_empty());
        assert!(point.events().is_empty() && manager.notifications().is_empty());
        assert!(manager.faults.reported().is_empty() && manager.faults.service_error().is_none());

        manager
            .replay_record(replay_wire(begin_kind(5002, false, 10, 100), 2), None)
            .unwrap();
        let original = manager.map.latest(5002).unwrap();
        let unsupported = RecordKind::Operation {
            transaction_id: 5002,
            redo_only: false,
            metadata: Some(managed),
            redo: Some(PointProvider::payload(11, 110)),
            undo: None,
        };
        assert!(manager
            .replay_record(replay_wire(unsupported, 3), None)
            .is_err());
        assert_eq!(manager.tail(), 2);
        assert_eq!(log(&manager).len(), 1);
        assert!(Arc::ptr_eq(&original, &manager.map.latest(5002).unwrap()));
        assert!(original.child().is_none());
        assert!(point.events().is_empty() && manager.notifications().is_empty());
        assert!(manager.faults.reported().is_empty() && manager.faults.service_error().is_none());

        // Native metadata equivalent continues to admit and link.
        let native = RecordKind::Operation {
            transaction_id: 5002,
            redo_only: false,
            metadata: Some(named::encode(7, None).unwrap()),
            redo: Some(PointProvider::payload(11, 110)),
            undo: None,
        };
        manager.replay_record(replay_wire(native, 3), None).unwrap();
        assert_eq!(manager.tail(), 3);
        assert_eq!(manager.map.latest(5002).unwrap().lsn(), 3);
        assert_eq!(original.child().unwrap().lsn(), 3);
    }
}
