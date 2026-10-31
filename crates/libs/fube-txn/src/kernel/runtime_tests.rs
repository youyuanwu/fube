//! Corrective admission counterexamples over actual native-issued fube-data.
use super::{
    processor::{ControlledLower, KernelManager, ProcessingRole},
    provider::{PointEvent, PointProvider, ProviderDirectory},
    runtime_lower::RuntimeLower,
    transaction::RetryRuntime,
};
use crate::{Cancellation, Epoch, ITransaction, OperationData, ReplicaRole, Result, Transaction};
use fube_data::{
    extensions::{ReplicationEndpointOptions, ReplicatorDiagnostics},
    qualification as rap, FabricReplicator, IStateProvider,
};
use fube_runtime::{
    extensions::{
        CallLane, CallOrigin, NativeIntegrationProvider, NativeReplicaSetConfiguration,
        NativeRuntimeIntegration,
    },
    ApiFuture, IPrimaryReplicator, IStatefulServiceFactory, IStatefulServicePartition,
    IStatefulServiceReplica, OpenMode,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
type Created =
    Arc<Mutex<Option<oneshot::Sender<(Arc<FabricReplicator>, NativeRuntimeIntegration)>>>>;
struct Factory {
    native: NativeIntegrationProvider,
    created: Created,
    settings: fube_data::Settings,
}
struct Replica {
    native: NativeRuntimeIntegration,
    created: Created,
    settings: fube_data::Settings,
}
struct LogProvider;
impl IStateProvider for LogProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        Ok(1)
    }
    fn update_epoch(&self, _: Epoch, _: i64, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        Err(crate::Error::new(
            fube_runtime::ErrorKind::Unsupported,
            "admission fixture excludes copy",
        ))
    }
    fn get_copy_state(
        &self,
        _: i64,
        _: Option<Arc<dyn fube_data::IOperationDataStream>>,
    ) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        self.get_copy_context()
    }
}
impl IStatefulServiceFactory for Factory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        replica: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        Ok(Box::new(Replica {
            native: self.native.capture(partition, replica)?,
            created: self.created.clone(),
            settings: self.settings.clone(),
        }))
    }
}
impl IStatefulServiceReplica for Replica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
            let lower = FabricReplicator::create(
                partition,
                self.native.clone(),
                Arc::new(LogProvider),
                None,
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                self.settings.clone(),
            )?;
            let _ = self
                .created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send((lower.clone(), self.native.clone()));
            Ok(Box::new((*lower).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://corrective-admission".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Fixture {
    host: Arc<rap::Host>,
    lower: Arc<FabricReplicator>,
    native: NativeRuntimeIntegration,
    manager: Arc<KernelManager>,
    runtime: Arc<RuntimeLower>,
    provider: Arc<PointProvider>,
}
impl Fixture {
    async fn new(configure: bool) -> Self {
        Self::using_settings(configure, fube_data::Settings::default()).await
    }
    async fn using_settings(configure: bool, settings: fube_data::Settings) -> Self {
        let (send, recv) = oneshot::channel();
        let host = rap::Host::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            move |native| {
                Box::new(Factory {
                    native,
                    created: Arc::new(Mutex::new(Some(send))),
                    settings,
                })
            },
        )
        .await;
        let (lower, native) = recv.await.unwrap();
        let provider = PointProvider::preinitialized("fabric:/point");
        let directory = ProviderDirectory::preinitialized(
            [(10, provider.clone() as Arc<dyn crate::IStateProvider2>)],
            None,
        )
        .unwrap();
        let manager = KernelManager::established(
            directory,
            Arc::new(ControlledLower::new(2)),
            ProcessingRole::Primary,
            0,
            Arc::new(RetryRuntime::default()),
        );
        let runtime = RuntimeLower::new(lower.clone()).unwrap();
        manager.bind_runtime(runtime.clone());
        let fixture = Self {
            host,
            lower,
            native,
            manager,
            runtime,
            provider,
        };
        if configure {
            fixture.configure().await;
        }
        fixture
    }
    async fn configure(&self) {
        self.host
            .configure(NativeReplicaSetConfiguration {
                epoch: Epoch {
                    data_loss_number: 1,
                    configuration_number: 1 << 32,
                },
                replicas: vec![],
                write_quorum: 1,
                local_member: true,
            })
            .await
            .unwrap();
    }
    async fn no_quorum(&self) {
        self.host
            .configure(NativeReplicaSetConfiguration {
                epoch: Epoch {
                    data_loss_number: 1,
                    configuration_number: 1 << 32,
                },
                replicas: vec![fube_runtime::extensions::NativeReplicaDescriptor {
                    node_id: [2; 16],
                    node_instance: 1,
                    replica_id: 2,
                    incarnation: 12,
                    role: ReplicaRole::ActiveSecondary,
                    previous_role: ReplicaRole::None,
                    up: false,
                    ready: false,
                    address: fube_data::wire::Endpoint {
                        actor: fube_data::wire::Actor {
                            partition: self.native.identity().partition_id,
                            replica: 2,
                            incarnation: uuid::Uuid::new_v4(),
                        },
                        address: "127.0.0.1:1".into(),
                        transport_id: Some(String::new()),
                    }
                    .to_string(),
                    progress: fube_runtime::extensions::ReplicationProgress {
                        first: -1,
                        last: -1,
                    },
                    must_catch_up: false,
                }],
                write_quorum: 2,
                local_member: true,
            })
            .await
            .unwrap();
    }
    async fn withheld_quorum(&self) {
        let committed = self.lower.snapshot().progress.committed;
        let incarnation = self.lower.snapshot().progress.assigned + 100;
        self.host
            .configure(NativeReplicaSetConfiguration {
                epoch: Epoch {
                    data_loss_number: 1,
                    configuration_number: 1 << 32,
                },
                replicas: vec![fube_runtime::extensions::NativeReplicaDescriptor {
                    node_id: [3; 16],
                    node_instance: 1,
                    replica_id: 3,
                    incarnation,
                    role: ReplicaRole::ActiveSecondary,
                    previous_role: ReplicaRole::None,
                    up: true,
                    ready: true,
                    address: fube_data::wire::Endpoint {
                        actor: fube_data::wire::Actor {
                            partition: self.native.identity().partition_id,
                            replica: 3,
                            incarnation: uuid::Uuid::new_v4(),
                        },
                        address: "127.0.0.1:1".into(),
                        transport_id: Some(String::new()),
                    }
                    .to_string(),
                    progress: fube_runtime::extensions::ReplicationProgress {
                        first: 1,
                        last: committed,
                    },
                    must_catch_up: false,
                }],
                write_quorum: 2,
                local_member: true,
            })
            .await
            .unwrap();
    }
    async fn barrier(&self) {
        let manager = self.manager.clone();
        let permit = self
            .native
            .execution()
            .reserve(
                CallLane::Native,
                CallOrigin::default(),
                Duration::from_secs(2),
                false,
            )
            .unwrap();
        let lsn = permit
            .run_owned(move || manager.barrier(0).map(|a| a.node.lsn()))
            .await
            .unwrap();
        self.manager
            .activity(lsn)
            .result
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
    async fn until(&self, tail: i64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.manager.tail() < tail {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
}
fn payload(key: i64) -> OperationData {
    OperationData::new(vec![[key.to_le_bytes(), (key * 10).to_le_bytes()].concat()]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_async_retry_after_timer_uses_actual_lower_not_lost_entry() {
    let f = Fixture::new(true).await;
    let identity = f.native.identity();
    f.host
        .configure(NativeReplicaSetConfiguration {
            epoch: Epoch {
                data_loss_number: 1,
                configuration_number: 1 << 32,
            },
            replicas: vec![fube_runtime::extensions::NativeReplicaDescriptor {
                node_id: [2; 16],
                node_instance: 1,
                replica_id: 2,
                incarnation: 12,
                role: ReplicaRole::ActiveSecondary,
                previous_role: ReplicaRole::None,
                up: false,
                ready: false,
                address: fube_data::wire::Endpoint {
                    actor: fube_data::wire::Actor {
                        partition: identity.partition_id,
                        replica: 2,
                        incarnation: uuid::Uuid::new_v4(),
                    },
                    address: "127.0.0.1:1".into(),
                    transport_id: Some(String::new()),
                }
                .to_string(),
                progress: fube_runtime::extensions::ReplicationProgress {
                    first: -1,
                    last: -1,
                },
                must_catch_up: false,
            }],
            write_quorum: 2,
            local_member: true,
        })
        .await
        .unwrap();
    let transaction = f.manager.new_transaction().unwrap();
    transaction
        .add_operation(None, None, Some(payload(7)), 10, None)
        .unwrap();
    let observed = transaction.clone();
    let commit = tokio::spawn(async move { observed.commit().await });
    tokio::time::sleep(Duration::from_millis(90)).await;
    let tail_during_no_quorum = f.manager.tail();
    f.configure().await;
    f.until(2).await;
    assert!(!f.manager.activity(2).apply.is_completed());
    f.barrier().await;
    commit.await.unwrap().unwrap();
    assert_eq!(tail_during_no_quorum, 1);
    assert!(f
        .manager
        .faults
        .observed()
        .iter()
        .all(|e| e.kind() != fube_runtime::ErrorKind::InvalidExecutionContext));
    assert!(f.manager.lower.attempts().is_empty());
    assert!(f.provider.events().iter().any(|e| matches!(
        e,
        PointEvent::Apply {
            operation: 2,
            commit: 2,
            key: 7,
            ..
        }
    )));
    f.runtime.drained().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_sync_begin_add_then_entry_free_async_end_reconcile_once() {
    let f = Fixture::new(true).await;
    let transaction = f.manager.new_transaction().unwrap();
    transaction
        .add_operation(None, None, Some(payload(1)), 10, None)
        .unwrap();
    let written = transaction.clone();
    let permit = f
        .native
        .execution()
        .reserve(
            CallLane::Native,
            CallOrigin::default(),
            Duration::from_secs(2),
            false,
        )
        .unwrap();
    permit
        .run_owned(move || written.add_operation(None, None, Some(payload(2)), 10, None))
        .await
        .unwrap();
    assert_eq!(f.manager.tail(), 3);
    let observer = transaction.clone();
    let commit = tokio::spawn(async move { observer.commit().await });
    f.until(4).await;
    f.barrier().await;
    commit.await.unwrap().unwrap();
    assert_eq!(
        f.provider
            .events()
            .iter()
            .filter(|e| matches!(e, PointEvent::Apply { commit: 4, .. }))
            .count(),
        2
    );
    assert!(f.manager.lower.attempts().is_empty());
    f.runtime.drained().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_publication_rejects_recursive_async_lower_before_admission_lock() {
    use fube_data::IStateReplicator;
    use std::task::{Context, Poll, Waker};
    let f = Fixture::new(true).await;
    let result = fube_runtime::extensions::ExecutionRuntime::engine_publication(
        f.lower.execution_identity(),
        || {
            let mut nested = f.lower.replicate(payload(1), Cancellation::default());
            match nested
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(Err(error)) => Ok(error),
                _ => panic!("reentry must reject before attempting an owner lock"),
            }
        },
    )
    .unwrap();
    assert_eq!(result.kind(), fube_runtime::ErrorKind::InvalidArgument);
    assert_eq!(f.manager.tail(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_rejected_capture_disposal_does_not_block_lower_owner() {
    use super::{
        named,
        record::{Record, RecordKind},
    };
    use crate::{OperationContext, TransactionBase};
    use fube_data::IStateReplicator;
    use std::sync::Condvar;
    struct Capture {
        entered: Option<oneshot::Sender<String>>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }
    impl OperationContext for Capture {}
    impl Drop for Capture {
        fn drop(&mut self) {
            let _ = self
                .entered
                .take()
                .unwrap()
                .send(std::thread::current().name().unwrap_or("").to_owned());
            let lock = self.release.0.lock().unwrap();
            let _ = self
                .release
                .1
                .wait_timeout_while(lock, Duration::from_secs(2), |v| !*v)
                .unwrap();
        }
    }
    let f = Fixture::new(true).await;
    f.no_quorum().await;
    let transaction = f.manager.new_transaction().unwrap();
    let (entered, received) = oneshot::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let observed = f.runtime.admit_async(
        f.manager.clone(),
        Record {
            lsn: -1,
            kind: RecordKind::Begin {
                transaction_id: transaction.get_transaction_id(),
                single: true,
                metadata: Some(named::encode(10, None).unwrap()),
                undo: None,
                redo: Some(payload(1)),
            },
        },
        Some(transaction),
        Some(Arc::new(Capture {
            entered: Some(entered),
            release: release.clone(),
        })),
    );
    drop(observed);
    let thread = tokio::time::timeout(Duration::from_secs(1), received)
        .await
        .unwrap()
        .unwrap();
    let progress = tokio::time::timeout(
        Duration::from_millis(100),
        f.lower.replicate(payload(2), Cancellation::default()),
    )
    .await;
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    f.runtime.drained().await;
    assert!(thread.contains("fube-owned-cleanup"), "{thread}");
    assert!(
        progress.unwrap().is_err(),
        "no quorum remains a lower rejection"
    );
    assert_eq!(f.lower.snapshot().progress.assigned, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_first_primary_group_and_idle_barriers_make_real_commits_live() {
    use super::record::RecordKind;
    use crate::TransactionBase;
    let f = Fixture::new(true).await;
    f.runtime.enable_barriers(&f.manager).unwrap();
    let read_only = f.manager.new_transaction().unwrap();
    read_only.commit().await.unwrap();
    assert_eq!(
        f.manager.tail(),
        1,
        "read-only commit does not establish a write barrier"
    );
    let single = f.manager.new_transaction().unwrap();
    single
        .add_operation(None, None, Some(payload(5)), 10, None)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), single.commit())
        .await
        .unwrap()
        .unwrap();
    f.runtime.group_idle().await;
    assert_eq!(single.get_commit_sequence_number(), 3);
    let barriers: Vec<_> = f
        .manager
        .records()
        .iter()
        .filter_map(|a| match a.node.record().kind {
            RecordKind::Barrier { last_stable_lsn } => Some((a.node.lsn(), last_stable_lsn)),
            _ => None,
        })
        .collect();
    assert_eq!(barriers, vec![(2, 0), (4, 2), (5, 4)]);
    assert_eq!(
        (f.manager.tail(), f.manager.stable(), f.manager.version()),
        (5, 5, 5)
    );
    let multi = f.manager.new_transaction().unwrap();
    multi
        .add_operation(None, None, Some(payload(1)), 10, None)
        .unwrap();
    let writes = multi.clone();
    f.native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || writes.add_operation(None, None, Some(payload(2)), 10, None))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), multi.commit())
        .await
        .unwrap()
        .unwrap();
    f.runtime.group_idle().await;
    assert_eq!(multi.get_commit_sequence_number(), 8);
    assert_eq!(
        f.provider
            .events()
            .iter()
            .filter(|e| matches!(
                e,
                PointEvent::Apply {
                    operation: 6 | 7,
                    commit: 8,
                    ..
                }
            ))
            .count(),
        2
    );
    assert_eq!(f.manager.tail(), 10);
    assert!(f.manager.lower.attempts().is_empty());
    f.runtime.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_apply_visibility_precedes_actual_quorum_and_unlock_on_accepted_failure() {
    use crate::{TransactionBase, TransactionState};
    let f = Fixture::new(true).await;
    f.runtime.enable_barriers(&f.manager).unwrap();
    f.withheld_quorum().await;
    let transaction = f.manager.new_transaction().unwrap();
    let writes = transaction.clone();
    let provider = f.provider.clone();
    f.native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            provider.write(writes.as_ref(), 10, 1, 10)?;
            provider.write(writes.as_ref(), 10, 2, 20)?;
            Ok(())
        })
        .await
        .unwrap();
    let commit = transaction.commit();
    drop(commit); // observer Drop is not rollback/cancellation
    f.until(6).await;
    tokio::time::timeout(Duration::from_secs(2), f.manager.activity(5).apply.wait())
        .await
        .unwrap()
        .as_ref()
        .clone()
        .unwrap();
    assert_eq!(transaction.get_commit_sequence_number(), 5);
    assert!(!f.manager.activity(5).replication.is_completed());
    assert!(f
        .provider
        .events()
        .iter()
        .all(|event| !matches!(event, PointEvent::Unlock { .. })));
    let reader = f.manager.new_transaction().unwrap();
    let version = reader.get_visibility_sequence_number().await.unwrap();
    assert_eq!(version, 6);
    assert_eq!(
        f.provider.snapshot(reader.as_ref(), 1, version).unwrap(),
        Some(10)
    );
    assert_eq!(f.manager.stable(), 0);
    // Real lower role transition fails accepted replication; applied canonical
    // versions remain and required Unlock still runs, with TransactionAborted.
    let lower = f.lower.clone();
    f.native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            fube_runtime::IReplicator::abort(lower.as_ref());
            Ok(())
        })
        .await
        .unwrap();
    f.manager.set_role(ReplicaRole::None);
    f.manager.close_activity_gate();
    let result = f.manager.activity(5).result.wait().await.as_ref().clone();
    assert!(result.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while transaction.get_state() == TransactionState::Committing {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        f.provider
            .events()
            .iter()
            .filter(|e| matches!(e, PointEvent::Unlock { .. }))
            .count(),
        2
    );
    assert_eq!(
        f.provider.snapshot(reader.as_ref(), 1, version).unwrap(),
        Some(10)
    );
    f.runtime.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_runtime_bounds_live_cache_and_snapshot_ownership_before_lower_assignment() {
    use crate::{ITransactionVersionManager, IVersionManager};
    let f = Fixture::new(true).await;
    let mut owners = Vec::new();
    for _ in 0..16 {
        owners.push(f.manager.new_transaction().unwrap());
    }
    assert!(f.manager.new_transaction().is_err());
    let large = OperationData::new(vec![vec![0; 900 * 1024]]).unwrap();
    owners[0]
        .add_operation(
            Some(large.clone()),
            Some(large.clone()),
            Some(large.clone()),
            10,
            None,
        )
        .unwrap();
    assert!(owners[1]
        .add_operation(
            Some(large.clone()),
            Some(large.clone()),
            Some(large),
            10,
            None
        )
        .is_err());
    assert!(
        !owners[1].is_write(),
        "rejected first cache must not mark write"
    );
    assert_eq!(f.manager.tail(), 1);
    for _ in 0..64 {
        f.manager.register().await.unwrap();
    }
    assert!(f.manager.register().await.is_err());
    for id in 1..=512 {
        f.manager.try_remove_version(id, 0, 2).unwrap();
    }
    assert!(f.manager.try_remove_version(513, 0, 2).is_err());
    for _ in 0..64 {
        f.manager.unregister(1).unwrap();
    }
    drop(owners);
    assert!(f.manager.new_transaction().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase1d_reserved_dispatch_progresses_at_full_lower_budget_and_loan_renews_after_retirement(
) {
    use crate::TransactionBase;
    let settings = fube_data::Settings {
        queue_operations: 40,
        operation_bytes: 4096,
        operation_segments: 64,
        queue_bytes: 64 * 1024,
        provider_operations: 1,
        ..fube_data::Settings::default()
    };
    let f = Fixture::using_settings(true, settings.clone()).await;
    f.runtime.enable_barriers(&f.manager).unwrap();
    let initial = f.runtime.barrier_loan(0);
    f.withheld_quorum().await;
    let transaction = f.manager.new_transaction().unwrap();
    transaction
        .add_operation(None, None, Some(payload(1)), 10, None)
        .unwrap();
    let writes = transaction.clone();
    f.native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            for key in 2..=5 {
                writes.add_operation(None, None, Some(payload(key)), 10, None)?;
            }
            assert!(writes
                .add_operation(None, None, Some(payload(6)), 10, None)
                .is_err());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(f.manager.tail(), 7);
    assert_eq!(f.lower.snapshot().owned_operations, 40);
    let observed = transaction.clone();
    let commit = tokio::spawn(async move { observed.commit().await });
    f.until(9).await;
    tokio::time::timeout(Duration::from_secs(2), f.manager.activity(8).apply.wait())
        .await
        .unwrap()
        .as_ref()
        .clone()
        .unwrap();
    assert_eq!(transaction.get_commit_sequence_number(), 8);
    assert!(!f.manager.activity(8).replication.is_completed());
    assert_eq!(f.lower.snapshot().owned_operations, 40);
    assert!(f.lower.snapshot().owned_bytes <= settings.queue_bytes);
    f.configure().await;
    commit.await.unwrap().unwrap();
    f.runtime.group_idle().await;
    while f.manager.tail() < 22 {
        f.barrier().await;
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while !initial.is_available() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Same reservation, restored by actual retained-queue charge retirement.
    assert!(Arc::ptr_eq(&initial, &f.runtime.barrier_loan(0)));
    f.withheld_quorum().await;
    let manager = f.manager.clone();
    let sequence = f
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || manager.barrier(0).map(|activity| activity.node.lsn()))
        .await
        .unwrap();
    let unavailable_while_owned = !initial.is_available();
    assert!(!f.manager.activity(sequence).replication.is_completed());
    f.configure().await;
    f.manager
        .activity(sequence)
        .result
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert!(f.lower.snapshot().owned_operations <= 40);
    f.runtime.stop().await;
    assert!(
        unavailable_while_owned,
        "reused loan stays charged while real quorum/queue ownership remains"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase1d_later_group_applies_while_earlier_barrier_quorum_is_pending() {
    let f = Fixture::new(true).await;
    f.runtime.enable_barriers(&f.manager).unwrap();
    let warm = f.manager.new_transaction().unwrap();
    warm.add_operation(None, None, Some(payload(1)), 10, None)
        .unwrap();
    warm.commit().await.unwrap();
    f.runtime.group_idle().await;
    f.withheld_quorum().await;
    let first = f.manager.new_transaction().unwrap();
    first
        .add_operation(None, None, Some(payload(2)), 10, None)
        .unwrap();
    drop(first.commit());
    f.until(7).await;
    f.manager
        .activity(6)
        .apply
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert!(!f.manager.activity(7).replication.is_completed());
    let later = f.manager.new_transaction().unwrap();
    later
        .add_operation(None, None, Some(payload(3)), 10, None)
        .unwrap();
    drop(later.commit());
    f.until(9).await;
    tokio::time::timeout(Duration::from_secs(2), f.manager.activity(8).apply.wait())
        .await
        .unwrap()
        .as_ref()
        .clone()
        .unwrap();
    assert!(!f.manager.activity(7).replication.is_completed());
    f.configure().await;
    f.runtime.group_idle().await;
    assert!(f
        .provider
        .events()
        .iter()
        .any(|event| matches!(event, PointEvent::Apply { operation: 8, .. })));
    f.runtime.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_quorum_completes_retained_activity_while_upper_has_lsn_gap() {
    use super::{
        named,
        record::{Record, RecordKind},
    };
    use crate::TransactionBase;
    let f = Fixture::new(true).await;
    let (entered, received) = std::sync::mpsc::sync_channel(1);
    let (release, resumed) = std::sync::mpsc::sync_channel(1);
    *f.runtime.publication_pause.lock().unwrap() = Some((2, entered, resumed));
    let a = f.manager.new_transaction().unwrap();
    let first = f.runtime.admit_async(
        f.manager.clone(),
        Record {
            lsn: -1,
            kind: RecordKind::Begin {
                transaction_id: a.get_transaction_id(),
                single: true,
                metadata: Some(named::encode(10, None).unwrap()),
                undo: None,
                redo: Some(payload(1)),
            },
        },
        Some(a),
        None,
    );
    received.recv_timeout(Duration::from_secs(1)).unwrap();
    let b = f.manager.new_transaction().unwrap();
    let later = f
        .runtime
        .admit_async(
            f.manager.clone(),
            Record {
                lsn: -1,
                kind: RecordKind::Begin {
                    transaction_id: b.get_transaction_id(),
                    single: true,
                    metadata: Some(named::encode(10, None).unwrap()),
                    undo: None,
                    redo: Some(payload(2)),
                },
            },
            Some(b),
            None,
        )
        .await
        .unwrap();
    let quorum = tokio::time::timeout(Duration::from_millis(100), later.replication.wait()).await;
    let tail_before_gap = f.manager.tail();
    let queued = f.manager.queued_lsns();
    release.send(()).unwrap();
    let first = first.await.unwrap();
    f.until(3).await;
    f.barrier().await;
    first.result.wait().await.as_ref().clone().unwrap();
    later.result.wait().await.as_ref().clone().unwrap();
    f.runtime.drained().await;
    quorum.unwrap().as_ref().clone().unwrap();
    assert_eq!(tail_before_gap, 1);
    assert_eq!(queued, vec![3]);
    assert_eq!(
        f.provider
            .events()
            .iter()
            .filter(|e| matches!(e, PointEvent::Apply { .. }))
            .count(),
        2
    );
    assert_eq!(
        f.provider
            .events()
            .iter()
            .filter(|e| matches!(e, PointEvent::Unlock { .. }))
            .count(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_flush_extraction_and_dispatch_enqueue_cannot_be_overtaken() {
    let provider = PointProvider::preinitialized("fabric:/point");
    let directory = ProviderDirectory::preinitialized(
        [(10, provider.clone() as Arc<dyn crate::IStateProvider2>)],
        None,
    )
    .unwrap();
    let manager = KernelManager::established(
        directory,
        Arc::new(ControlledLower::new(2)),
        ProcessingRole::Primary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let transaction = manager.new_transaction().unwrap();
    manager
        .admit_operation(
            transaction,
            super::named::encode(10, None).unwrap(),
            None,
            Some(payload(1)),
            None,
            Some(true),
        )
        .unwrap();
    let (entered, received) = std::sync::mpsc::sync_channel(1);
    let (release, resumed) = std::sync::mpsc::sync_channel(1);
    *manager.flush_extracted.lock().unwrap() = Some((entered, resumed));
    let first = manager.clone();
    let handle = tokio::runtime::Handle::current();
    let a = std::thread::spawn(move || {
        let _runtime = handle.enter();
        first.flush(Ok(()));
    });
    received.recv_timeout(Duration::from_secs(1)).unwrap();
    let later = manager.clone();
    let handle = tokio::runtime::Handle::current();
    let b = std::thread::spawn(move || {
        let _runtime = handle.enter();
        later.barrier(0).unwrap();
        later.flush(Ok(()));
    });
    while manager.lower.attempts().len() < 2 {
        tokio::task::yield_now().await;
    }
    // B has lower-assigned Barrier3, but cannot publish ahead of extracted Begin2.
    assert_eq!(manager.version(), 1);
    assert!(provider.events().is_empty());
    release.send(()).unwrap();
    a.join().unwrap();
    b.join().unwrap();
    manager.replication(2, Ok(()));
    manager.replication(3, Ok(()));
    manager
        .activity(2)
        .result
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    manager
        .activity(3)
        .result
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert_eq!(manager.version(), 3);
}
