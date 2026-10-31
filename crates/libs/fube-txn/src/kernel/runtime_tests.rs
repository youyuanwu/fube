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
}
struct Replica {
    native: NativeRuntimeIntegration,
    created: Created,
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
                fube_data::Settings::default(),
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
        let (send, recv) = oneshot::channel();
        let host = rap::Host::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            move |native| {
                Box::new(Factory {
                    native,
                    created: Arc::new(Mutex::new(Some(send))),
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
