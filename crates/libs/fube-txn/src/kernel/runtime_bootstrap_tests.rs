//! Actual constructor/Open/role routing, not an established-tail injection.
use super::{
    processor::KernelManager, provider::ProviderDirectory, runtime_lower::RuntimeLower,
    transaction::RetryRuntime,
};
use crate::{ApiFuture, Cancellation, Epoch, Result};
use fube_data::{
    extensions::{
        ReplicationEndpointOptions, ReplicatorDiagnostics, ReplicatorLifecycle,
        StateProviderCapabilities, VolatileLogInitialization,
    },
    qualification as rap, FabricReplicator, IStateProvider,
};
use fube_runtime::{
    extensions::{NativeIntegrationProvider, NativeRuntimeIntegration},
    IPrimaryReplicator, IStatefulServiceFactory, IStatefulServicePartition,
    IStatefulServiceReplica, OpenMode, ReplicaRole,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;
struct Core {
    partition: uuid::Uuid,
    fail_role: AtomicBool,
    manager: Arc<KernelManager>,
    lower: Mutex<Weak<FabricReplicator>>,
    runtime: Mutex<Option<Arc<RuntimeLower>>>,
    events: Mutex<Vec<&'static str>>,
}
struct Bridge(Weak<Core>);
impl Bridge {
    fn core(&self) -> Result<Arc<Core>> {
        self.0.upgrade().ok_or(crate::Error::Closed)
    }
}
impl IStateProvider for Bridge {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        let core = self.core()?;
        let tail = core.manager.tail();
        if tail == 0 {
            core.events.lock().unwrap().push("constructor-tail-zero");
        }
        Ok(tail)
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let core = self.core()?;
            if let Some(marker) = core.manager.update_epoch(
                epoch,
                previous,
                1,
                super::identity::native_system_time()?,
            )? {
                core.manager.flush(Ok(()));
                marker.processing.wait().await.as_ref().clone()?;
            }
            Ok(())
        })
    }
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        Err(crate::Error::new(
            fube_runtime::ErrorKind::Unsupported,
            "bootstrap test has no copy",
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
impl ReplicatorLifecycle for Bridge {
    fn opened(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let core = self.core()?;
            core.events.lock().unwrap().push("lower-open");
            core.manager.open_memory(1024 * 1024)?;
            core.events.lock().unwrap().push("bootstrap-one");
            let lower = core.lower.lock().unwrap().upgrade().unwrap();
            lower.initialize_volatile_log().await?;
            core.events.lock().unwrap().push("measured-rebase-one");
            Ok(())
        })
    }
    fn role_changed(&self, _: Epoch, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let core = self.core()?;
            assert_eq!(
                core.lower
                    .lock()
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .snapshot()
                    .role,
                role
            );
            core.events.lock().unwrap().push("lower-role");
            if core.fail_role.swap(false, Ordering::AcqRel) {
                return Err(crate::Error::InvalidInput("upper role fixture failure"));
            }
            let previous = crate::ITransactionManager::get_role(core.manager.as_ref());
            let runtime = core.runtime.lock().unwrap().clone().unwrap();
            if previous != role
                && matches!(
                    previous,
                    ReplicaRole::Primary
                        | ReplicaRole::ActiveSecondary
                        | ReplicaRole::IdleSecondary
                )
            {
                runtime.deactivate_for_role(&core.manager, role).await?;
            } else {
                core.manager.set_role(role);
                if role == ReplicaRole::Primary {
                    runtime.enable_barriers(&core.manager)?;
                }
            }
            core.events.lock().unwrap().push("upper-role");
            Ok(())
        })
    }
    fn closed(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let core = self.core()?;
            assert!(
                core.lower
                    .lock()
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .snapshot()
                    .closed
            );
            core.events.lock().unwrap().push("lower-close");
            core.manager.close_activity_gate();
            let runtime = core.runtime.lock().unwrap().clone().unwrap();
            runtime.finish_generation(&core.manager, 4).await?;
            core.events.lock().unwrap().push("upper-close");
            Ok(())
        })
    }
    fn aborted(&self) {
        if let Ok(core) = self.core() {
            core.events.lock().unwrap().push("lower-abort");
            core.manager.close_activity_gate();
            drop(tokio::spawn(async move {
                let runtime = core.runtime.lock().unwrap().clone().unwrap();
                let _ = runtime.finish_generation(&core.manager, 4).await;
                core.events.lock().unwrap().push("upper-abort-owned");
            }));
        }
    }
}
type Created = Arc<Mutex<Option<oneshot::Sender<Arc<Core>>>>>;
struct Factory {
    native: NativeIntegrationProvider,
    created: Created,
    root: bool,
}
struct Replica {
    native: NativeRuntimeIntegration,
    created: Created,
    root: bool,
    core: Arc<Core>,
}
impl IStatefulServiceFactory for Factory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        id: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        Ok(Box::new(Replica {
            native: self.native.capture(partition, id)?,
            created: self.created.clone(),
            root: self.root,
            core: Arc::new(Core {
                partition,
                fail_role: AtomicBool::new(false),
                manager: KernelManager::fresh(
                    ProviderDirectory::preinitialized([], None)?,
                    Arc::new(RetryRuntime::default()),
                ),
                lower: Mutex::new(Weak::new()),
                runtime: Mutex::new(None),
                events: Mutex::new(Vec::new()),
            }),
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
            let bridge = Arc::new(Bridge(Arc::downgrade(&self.core)));
            let lower = FabricReplicator::create_with_lifecycle(
                partition,
                self.native.clone(),
                bridge.clone(),
                None,
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                fube_data::Settings::default(),
                StateProviderCapabilities {
                    supports_copy_until_latest_lsn: true,
                },
                bridge,
            )?;
            lower.qualification_use_root_controls(self.root);
            *self.core.lower.lock().unwrap() = Arc::downgrade(&lower);
            let runtime = RuntimeLower::new(lower.clone())?;
            self.core.manager.bind_runtime(runtime.clone());
            *self.core.runtime.lock().unwrap() = Some(runtime);
            let _ = self
                .created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(self.core.clone());
            Ok(Box::new((*lower).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://fresh-volatile".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
async fn initialized_host(root: bool) -> (Arc<rap::Host>, Arc<Core>) {
    let (send, recv) = oneshot::channel();
    let host = rap::Host::start(
        uuid::Uuid::new_v4(),
        1,
        ReplicaRole::Primary,
        move |native| {
            Box::new(Factory {
                native,
                created: Arc::new(Mutex::new(Some(send))),
                root,
            })
        },
    )
    .await;
    let core = recv.await.unwrap();
    host.configure(fube_runtime::extensions::NativeReplicaSetConfiguration {
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
    (host, core)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fidelity_after_lower_lsn_local_write_error_retains_admission_quorum_and_unlock_on_root_and_bound(
) {
    use super::provider::{PointEvent, PointProvider, ProviderEntry, ProviderMode};
    use crate::ITransactionManager;
    for root in [false, true] {
        let (host, core) = initialized_host(root).await;
        let provider = PointProvider::preinitialized("fabric:/write-fault");
        core.manager.directory.entries.lock().unwrap().insert(
            10,
            ProviderEntry {
                provider: provider.clone(),
                mode: ProviderMode::Active,
                deleting_by: None,
            },
        );
        let runtime = core.runtime.lock().unwrap().clone().unwrap();
        let warm = core.manager.create_transaction().unwrap();
        provider.write(warm.as_ref(), 10, 0, 0).unwrap();
        warm.commit().await.unwrap();
        runtime.group_idle().await;
        let lower = core.lower.lock().unwrap().upgrade().unwrap();
        let prefix = lower.snapshot().progress.committed;
        host.configure(fube_runtime::extensions::NativeReplicaSetConfiguration {
            epoch: Epoch {
                data_loss_number: 1,
                configuration_number: 1 << 32,
            },
            replicas: vec![fube_runtime::extensions::NativeReplicaDescriptor {
                node_id: [3; 16],
                node_instance: 1,
                replica_id: 3,
                incarnation: 100,
                role: ReplicaRole::ActiveSecondary,
                previous_role: ReplicaRole::None,
                up: true,
                ready: true,
                must_catch_up: false,
                address: fube_data::wire::Endpoint {
                    actor: fube_data::wire::Actor {
                        partition: core.partition,
                        replica: 3,
                        incarnation: uuid::Uuid::new_v4(),
                    },
                    address: "127.0.0.1:1".into(),
                    transport_id: Some(String::new()),
                }
                .to_string(),
                progress: fube_runtime::extensions::ReplicationProgress {
                    first: 1,
                    last: prefix,
                },
            }],
            write_quorum: 2,
            local_member: true,
        })
        .await
        .unwrap();
        let previous_unlocks = provider
            .events()
            .iter()
            .filter(|e| matches!(e, PointEvent::Unlock { .. }))
            .count();
        let successful_prefix = core.manager.activity(warm.get_commit_sequence_number());
        let flushed_prefix = core.manager.memory().unwrap().lock().unwrap().flushed_lsn;
        let transaction = core.manager.create_transaction().unwrap();
        provider.write(transaction.as_ref(), 10, 1, 10).unwrap();
        let successor = core.manager.create_transaction().unwrap();
        provider.write(successor.as_ref(), 10, 2, 20).unwrap();
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let (release, resume) = std::sync::mpsc::sync_channel(1);
        *runtime.publication_pause.lock().unwrap() = Some((prefix + 1, entered, resume));
        drop(transaction.commit());
        tokio::task::spawn_blocking(move || {
            observed
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
        })
        .await
        .unwrap();
        drop(successor.commit());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !core.manager.queued_lsns().contains(&(prefix + 3)) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        core.manager.memory().unwrap().lock().unwrap().write_error = Some(
            crate::Error::InvalidInput("controlled native memory write failure"),
        );
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while core.manager.tail() < prefix + 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let admitted = core.manager.activity(prefix + 1);
        assert!(admitted.flush.wait().await.is_err());
        assert!(admitted.apply.wait().await.is_err());
        assert!(
            !admitted.result.is_completed(),
            "accepted failure still awaits actual quorum"
        );
        assert!(!admitted.replication.is_completed());
        for sequence in prefix + 2..=prefix + 3 {
            let affected = core.manager.activity(sequence);
            assert!(affected.flush.wait().await.is_err());
            assert!(affected.apply.wait().await.is_err());
            assert!(!affected.result.is_completed());
        }
        assert_eq!(
            core.manager.memory().unwrap().lock().unwrap().flushed_lsn,
            flushed_prefix
        );
        assert!(successful_prefix.flush.wait().await.is_ok());
        assert!(successful_prefix.apply.wait().await.is_ok());
        assert_eq!(
            provider
                .events()
                .iter()
                .filter(|e| matches!(e, PointEvent::Unlock { .. }))
                .count(),
            previous_unlocks
        );
        assert_eq!(transaction.get_commit_sequence_number(), -1);
        host.configure(fube_runtime::extensions::NativeReplicaSetConfiguration {
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
        let failure = admitted.result.wait().await.as_ref().clone().unwrap_err();
        admitted.processing.wait().await.as_ref().clone().unwrap();
        for sequence in prefix + 2..=prefix + 3 {
            let affected = core.manager.activity(sequence);
            assert!(affected.result.wait().await.is_err());
            affected.processing.wait().await.as_ref().clone().unwrap();
        }
        let before_information = core
            .manager
            .memory()
            .unwrap()
            .lock()
            .unwrap()
            .events
            .clone();
        assert!(
            runtime.finish_generation(&core.manager, 4).await.is_err(),
            "terminal processing is not successful generation completion"
        );
        assert_eq!(
            core.manager.memory().unwrap().lock().unwrap().events,
            before_information,
            "no successful Information across missing write"
        );
        runtime.stop().await;
        assert!(super::status::Status::TransactionAborted.matches(&failure));
        assert_eq!(core.manager.tail(), prefix + 3);
        assert_eq!(
            provider
                .events()
                .iter()
                .filter(|e| matches!(e, PointEvent::Unlock { .. }))
                .count(),
            previous_unlocks + 2
        );
        assert_eq!(core.manager.faults.reported().len(), 1);
        drop(host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase1d_owned_upper_role_failure_fences_same_lower_on_root_and_bound() {
    for root in [false, true] {
        let (host, core) = initialized_host(root).await;
        let lower = core.lower.lock().unwrap().upgrade().unwrap();
        core.fail_role.store(true, Ordering::Release);
        let failed = host.change_role(ReplicaRole::ActiveSecondary).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !core.events.lock().unwrap().contains(&"upper-abort-owned") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(failed.is_err());
        assert!(lower.snapshot().closed);
        assert!(core.manager.gate().is_err());
        drop(host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase1d_active_publication_is_fenced_before_final_role_boundary_on_root_and_bound() {
    use super::provider::{PointProvider, ProviderEntry, ProviderMode};
    use crate::ITransactionManager;
    use std::time::Duration;
    for root in [false, true] {
        let (host, core) = initialized_host(root).await;
        let provider = PointProvider::preinitialized("fabric:/active-generation-fixture");
        core.manager.directory.entries.lock().unwrap().insert(
            10,
            ProviderEntry {
                provider,
                mode: ProviderMode::Active,
                deleting_by: None,
            },
        );
        let runtime = core.runtime.lock().unwrap().clone().unwrap();
        let payload = || {
            Some(
                fube_data::OperationData::new(vec![
                    [1i64.to_le_bytes(), 10i64.to_le_bytes()].concat()
                ])
                .unwrap(),
            )
        };
        let warm = core.manager.create_transaction().unwrap();
        warm.add_operation(None, None, payload(), 10, None).unwrap();
        warm.commit().await.unwrap();
        runtime.group_idle().await;
        let lower = core.lower.lock().unwrap().upgrade().unwrap();
        let next = lower.snapshot().progress.assigned + 1;
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let (release, resume) = std::sync::mpsc::sync_channel(1);
        *runtime.publication_pause.lock().unwrap() = Some((next, entered, resume));
        let transaction = core.manager.create_transaction().unwrap();
        transaction
            .add_operation(None, None, payload(), 10, None)
            .unwrap();
        let committed = tokio::spawn(async move { transaction.commit().await });
        tokio::task::spawn_blocking(move || observed.recv_timeout(Duration::from_secs(2)).unwrap())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while lower.snapshot().progress.committed < next {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let information = || {
            core.manager
                .memory()
                .unwrap()
                .lock()
                .unwrap()
                .events
                .iter()
                .filter(|e| e.0 == 12)
                .count()
        };
        let before = information();
        let changing_host = host.clone();
        let changed = tokio::spawn(async move {
            changing_host
                .change_role(ReplicaRole::ActiveSecondary)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !runtime.publications_fenced() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let no_premature_boundary = information() == before;
        let rejected = runtime
            .admit_async(
                core.manager.clone(),
                super::record::Record {
                    lsn: -1,
                    kind: super::record::RecordKind::Barrier { last_stable_lsn: 0 },
                },
                None,
                None,
            )
            .await
            .is_err();
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), changed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), committed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let published_before_boundary = core
            .manager
            .memory()
            .unwrap()
            .lock()
            .unwrap()
            .events
            .iter()
            .rev()
            .take(2)
            .map(|e| (e.0, e.1))
            .collect::<Vec<_>>();
        drop(host);
        assert!(no_premature_boundary);
        assert!(rejected);
        assert_eq!(published_before_boundary, vec![(12, next), (1, next)]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase1d_primary_generation_drains_old_scheduler_and_reestablishes_first_barrier() {
    use super::provider::{PointProvider, ProviderEntry, ProviderMode};
    use crate::ITransactionManager;
    let (host, core) = initialized_host(false).await;
    let provider = PointProvider::preinitialized("fabric:/generation-fixture");
    core.manager.directory.entries.lock().unwrap().insert(
        10,
        ProviderEntry {
            provider,
            mode: ProviderMode::Active,
            deleting_by: None,
        },
    );
    let runtime = core.runtime.lock().unwrap().clone().unwrap();
    let first = core.manager.create_transaction().unwrap();
    first
        .add_operation(
            None,
            None,
            Some(
                fube_data::OperationData::new(vec![
                    [1i64.to_le_bytes(), 10i64.to_le_bytes()].concat()
                ])
                .unwrap(),
            ),
            10,
            None,
        )
        .unwrap();
    first.commit().await.unwrap();
    runtime.group_idle().await;
    host.change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    let lower = core.lower.lock().unwrap().upgrade().unwrap();
    let copy = fube_data::IStateReplicator::get_copy_stream(lower.as_ref()).unwrap();
    let replication = fube_data::IStateReplicator::get_replication_stream(lower.as_ref()).unwrap();
    let pump = tokio::spawn(async move {
        while let Some(operation) = copy.get_operation(Cancellation::default()).await.unwrap() {
            operation.acknowledge().unwrap();
        }
        while let Some(operation) = replication
            .get_operation(Cancellation::default())
            .await
            .unwrap()
        {
            operation.acknowledge().unwrap();
        }
    });
    host.change_role(ReplicaRole::Primary).await.unwrap();
    pump.await.unwrap();
    host.configure(fube_runtime::extensions::NativeReplicaSetConfiguration {
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
    let before = core.manager.tail();
    let second = core.manager.create_transaction().unwrap();
    second
        .add_operation(
            None,
            None,
            Some(
                fube_data::OperationData::new(vec![
                    [2i64.to_le_bytes(), 20i64.to_le_bytes()].concat()
                ])
                .unwrap(),
            ),
            10,
            None,
        )
        .unwrap();
    second.commit().await.unwrap();
    runtime.group_idle().await;
    assert_eq!(
        second.get_commit_sequence_number(),
        before + 2,
        "new generation must insert first-primary barrier before commit"
    );
    drop(host);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_lower_first_open_bootstrap_measured_rebase_on_root_and_bound_controls() {
    for root in [false, true] {
        let (send, recv) = oneshot::channel();
        let host = rap::Host::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            move |native| {
                Box::new(Factory {
                    native,
                    created: Arc::new(Mutex::new(Some(send))),
                    root,
                })
            },
        )
        .await;
        let core = recv.await.unwrap();
        assert_eq!(
            &core.events.lock().unwrap()[..5],
            [
                "constructor-tail-zero",
                "lower-open",
                "bootstrap-one",
                "measured-rebase-one",
                "lower-role"
            ]
        );
        assert_eq!(core.manager.tail(), 1);
        host.configure(fube_runtime::extensions::NativeReplicaSetConfiguration {
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
        let lower = core.lower.lock().unwrap().upgrade().unwrap();
        assert_eq!(lower.snapshot().progress.assigned, 1);
        let runtime = core.runtime.lock().unwrap().clone().unwrap();
        let barrier = runtime
            .admit_async(
                core.manager.clone(),
                super::record::Record {
                    lsn: -1,
                    kind: super::record::RecordKind::Barrier { last_stable_lsn: 0 },
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(barrier.node.lsn(), 2);
        barrier.result.wait().await.as_ref().clone().unwrap();
        assert_eq!(core.manager.memory().unwrap().lock().unwrap().tail_lsn, 2);
        assert_eq!(
            core.manager.memory().unwrap().lock().unwrap().flushed_lsn,
            2
        );
        assert!(core.manager.lower.attempts().is_empty());
        drop(host);
        let events = core.events.lock().unwrap().clone();
        assert!(
            events.ends_with(&["lower-close", "upper-close"])
                || events.ends_with(&["lower-abort", "upper-abort-owned"]),
            "{events:?}"
        );
    }
}
