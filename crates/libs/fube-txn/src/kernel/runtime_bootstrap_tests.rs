//! Actual constructor/Open/role routing, not an established-tail injection.
use super::{
    processor::KernelManager, provider::ProviderDirectory, runtime_lower::RuntimeLower,
    transaction::RetryRuntime,
};
use crate::{ApiFuture, Cancellation, Epoch, Result};
use fube_data::{
    extensions::{ReplicationEndpointOptions, StateProviderCapabilities},
    FabricReplicator, IStateProvider, IStateReplicator,
};
use fube_rap_test_support as rap;
use fube_runtime::{
    extensions::{NativeIntegrationProvider, NativeRuntimeIntegration},
    IPrimaryReplicator, IReplicator, IStatefulServiceFactory, IStatefulServicePartition,
    IStatefulServiceReplica, OpenMode, ReplicaRole,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;
struct Core {
    partition: uuid::Uuid,
    fail_role: AtomicBool,
    manager: Arc<KernelManager>,
    lower: std::sync::OnceLock<Arc<dyn IPrimaryReplicator>>,
    state: std::sync::OnceLock<Arc<dyn IStateReplicator>>,
    native: NativeRuntimeIntegration,
    runtime: Mutex<Option<Arc<RuntimeLower>>>,
    events: Mutex<Vec<&'static str>>,
}
struct Bridge(Weak<Core>);
#[derive(Clone)]
struct CoreComposite(Arc<Core>);
impl IReplicator for CoreComposite {
    fn open(&self, cancellation: Cancellation) -> ApiFuture<'_, String> {
        let core = self.0.clone();
        super::composite::Composite::owned_control(
            &core.native.execution(),
            Box::pin(async move {
                let address = core.lower.get().unwrap().open(cancellation.clone()).await?;
                Bridge(Arc::downgrade(&core)).opened(cancellation).await?;
                Ok(address)
            }),
        )
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        let core = self.0.clone();
        super::composite::Composite::owned_control(
            &core.native.execution(),
            Box::pin(async move {
                core.lower
                    .get()
                    .unwrap()
                    .change_role(epoch, role, cancellation.clone())
                    .await?;
                let result = Bridge(Arc::downgrade(&core))
                    .role_changed(epoch, role, cancellation)
                    .await;
                if result.is_err() {
                    core.manager.close_activity_gate();
                    let _ = core
                        .native
                        .report_fault(fube_runtime::FaultType::Transient)
                        .await;
                    core.events.lock().unwrap().push("upper-role-fault");
                }
                result
            }),
        )
    }
    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        self.0
            .lower
            .get()
            .unwrap()
            .update_epoch(epoch, cancellation)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.0.lower.get().unwrap().get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.0.lower.get().unwrap().get_catch_up_capability()
    }
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        let core = self.0.clone();
        super::composite::Composite::owned_control(
            &core.native.execution(),
            Box::pin(async move {
                core.lower
                    .get()
                    .unwrap()
                    .close(cancellation.clone())
                    .await?;
                Bridge(Arc::downgrade(&core)).closed(cancellation).await
            }),
        )
    }
    fn abort(&self) {
        self.0.lower.get().unwrap().abort();
        Bridge(Arc::downgrade(&self.0)).aborted();
    }
}
impl IPrimaryReplicator for CoreComposite {
    fn on_data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool> {
        self.0.lower.get().unwrap().on_data_loss(cancellation)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: fube_runtime::ReplicaSetConfig,
        previous: fube_runtime::ReplicaSetConfig,
    ) -> Result<()> {
        self.0
            .lower
            .get()
            .unwrap()
            .update_catch_up_replica_set_configuration(current, previous)
    }
    fn update_current_replica_set_configuration(
        &self,
        current: fube_runtime::ReplicaSetConfig,
    ) -> Result<()> {
        self.0
            .lower
            .get()
            .unwrap()
            .update_current_replica_set_configuration(current)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: fube_runtime::ReplicaSetQuorumMode,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0
            .lower
            .get()
            .unwrap()
            .wait_for_catch_up_quorum(mode, cancellation)
    }
    fn build_replica(
        &self,
        replica: fube_runtime::ReplicaInformation,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0
            .lower
            .get()
            .unwrap()
            .build_replica(replica, cancellation)
    }
    fn remove_replica(&self, id: i64) -> Result<()> {
        self.0.lower.get().unwrap().remove_replica(id)
    }
}
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
impl Bridge {
    fn opened(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let core = self.core()?;
            core.events.lock().unwrap().push("lower-open");
            core.manager.open_memory(1024 * 1024)?;
            core.events.lock().unwrap().push("bootstrap-one");
            Ok(())
        })
    }
    fn role_changed(&self, _: Epoch, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let core = self.core()?;
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
            assert!(core.lower.get().unwrap().get_current_progress().is_err());
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
                lower: std::sync::OnceLock::new(),
                state: std::sync::OnceLock::new(),
                native: self.native.capture(partition, id)?,
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
            let lower = FabricReplicator::create_with_capabilities(
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
            )?;
            let _ = self.core.state.set(lower.get_state_replicator());
            let _ = self.core.lower.set(lower.clone());
            let runtime = RuntimeLower::new(lower.get_state_replicator(), self.native.execution())?;
            self.core.manager.bind_runtime(runtime.clone());
            *self.core.runtime.lock().unwrap() = Some(runtime);
            let _ = self
                .created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(self.core.clone());
            let control = CoreComposite(self.core.clone());
            if self.root {
                Ok(Box::new(control) as Box<dyn IPrimaryReplicator>)
            } else {
                Ok(Box::new(rap::DelegatingPrimary(Arc::new(control)))
                    as Box<dyn IPrimaryReplicator>)
            }
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
    initialized_host_at(uuid::Uuid::new_v4(), 1, root).await
}
async fn initialized_host_at(
    partition: uuid::Uuid,
    id: i64,
    root: bool,
) -> (Arc<rap::Host>, Arc<Core>) {
    let (send, recv) = oneshot::channel();
    let host = rap::Host::start(partition, id, ReplicaRole::Primary, move |native| {
        Box::new(Factory {
            native,
            created: Arc::new(Mutex::new(Some(send))),
            root,
        })
    })
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
        let lower = CoreComposite(core.clone());
        let prefix = lower.get_current_progress().unwrap();
        // A real public Open endpoint, but not a built receiver of this primary.
        // It therefore cannot ACK this generation's writes or supply quorum.
        let (peer_host, _) = initialized_host_at(core.partition, 3, root).await;
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
                address: peer_host.replication_address().unwrap(),
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
        let lower = CoreComposite(core.clone());
        core.fail_role.store(true, Ordering::Release);
        let failed = host.change_role(ReplicaRole::ActiveSecondary).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !core.events.lock().unwrap().contains(&"upper-role-fault") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(failed.is_err());
        assert!(
            lower.get_current_progress().is_ok(),
            "upper role failure must not unconditionally abort lower"
        );
        assert!(!core.events.lock().unwrap().contains(&"lower-abort"));
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
        let lower = CoreComposite(core.clone());
        let next = runtime.last_admitted.load(Ordering::Acquire) + 1;
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
            while lower.get_current_progress().unwrap() < next {
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
    let lower = core.state.get().unwrap().clone();
    let copy = lower.get_copy_stream().unwrap();
    let replication = lower.get_replication_stream().unwrap();
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
            &core.events.lock().unwrap()[..4],
            ["lower-open", "bootstrap-one", "lower-role", "upper-role"]
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
        let lower = CoreComposite(core.clone());
        assert_eq!(lower.get_current_progress().unwrap(), 1);
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
