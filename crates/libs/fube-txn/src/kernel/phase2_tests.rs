//! Actual native-RAP issued lower engine/binder, not a fake lower-manager success.
//! Controlled None/Log copy source is ownership evidence ONLY, not Full negotiation.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_initial_state_eos_before_first_data_keeps_native_success_without_begin_or_end() {
    let partition = uuid::Uuid::new_v4();
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, Spy::new()).await;
    let source = Source::start(partition, vec![header(3)], None).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    let generation = target
        .runtime
        .generation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    generation.copy_done.wait().await.as_ref().clone().unwrap();
    assert!(target
        .runtime
        .owner
        .manager
        .directory
        .copy_progress()
        .is_none());
    assert!(!target.spy.point.events().iter().any(|e| matches!(
        e,
        super::provider::PointEvent::Callback {
            kind: super::provider::CallbackKind::BeginCopy | super::provider::CallbackKind::EndCopy,
            ..
        }
    )));
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_interrupted_after_begin_state_eos_is_cancelled_without_replication_end_or_readiness(
) {
    let partition = uuid::Uuid::new_v4();
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, Spy::new()).await;
    let version = OperationData::new(vec![vec![1, 0, 0, 0, 0]]).unwrap();
    let state = super::copy_codec::add_stage(
        super::named::encode_copy(super::provider::REGISTRY_ID, &version).unwrap(),
        super::copy_codec::Stage::State,
    )
    .unwrap();
    let source = Source::start(partition, vec![header(3), state], None).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    let generation = target
        .runtime
        .generation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    let error = generation
        .copy_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap_err();
    assert!(super::status::Status::Cancelled.matches(&error));
    assert!(generation.done.wait().await.is_err());
    assert!(!target.runtime.owner.ready.load(Ordering::Acquire));
    assert!(!target.runtime.owner.get_is_readable());
    assert!(target.runtime.owner.create_transaction().is_err());
    assert!(target
        .runtime
        .owner
        .manager
        .directory
        .copy_progress()
        .is_some());
    assert!(!target.spy.point.events().iter().any(|e| matches!(
        e,
        super::provider::PointEvent::Callback {
            kind: super::provider::CallbackKind::EndCopy,
            ..
        }
    )));
    let lower = target.runtime.replicator();
    target
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            lower.abort();
            Ok(())
        })
        .await
        .unwrap();
    assert!(target.runtime.terminal.wait().await.is_err());
    target
        .runtime
        .cleanup_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    source.host.remove(descriptor(&target)).await.unwrap();
    drop(source.host);
    drop(target.host);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_secondary_successful_prefix_processing_outlives_failed_copy_and_info() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let source = Source::start(partition, Vec::new(), None).await;
        let mut first = single(1000, 1);
        first.lsn = source.replicate(first.clone()).await;
        let mut failing = single(1001, 2);
        failing.lsn = source.replicate(failing.clone()).await;
        let update = Record {
            lsn: 1,
            kind: RecordKind::UpdateEpoch {
                epoch: epoch(),
                primary_replica_id: 1,
                timestamp_ticks: 0,
            },
        };
        let mut buffers = update.encode().unwrap().to_segments();
        buffers.extend(first.encode().unwrap().to_segments());
        buffers.extend(failing.encode().unwrap().to_segments());
        buffers.push(7i32.to_le_bytes().to_vec());
        *source.provider.records.lock().unwrap() =
            vec![header(7), OperationData::new(buffers).unwrap()];
        let apply = Completion::new();
        *target.spy.apply_gate.lock().unwrap() = Some(apply.clone());
        {
            let memory = target.runtime.owner.manager.memory().unwrap();
            let mut memory = memory.lock().unwrap();
            memory.write_error = Some(Error::InvalidInput("failed copied suffix"));
            memory.write_error_after = 2;
        }
        let member = descriptor(&target);
        let sender = source.host.clone();
        let cancel = Cancellation::default();
        let token = cancel.clone();
        let building = tokio::spawn(async move { sender.build(member, token).await });
        until(|| target.spy.has("apply-start")).await;
        target
            .runtime
            .fault_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        let lower = target.runtime.replicator();
        target
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                lower.abort();
                Ok(())
            })
            .await
            .unwrap();
        assert!(target.runtime.terminal.wait().await.is_err());
        assert!(!target.runtime.cleanup_done.is_completed());
        assert!(
            !target.spy.has("close-start"),
            "failed Info construction does not erase secondary Apply ownership"
        );
        apply.complete(Ok(()));
        target
            .runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert!(target.spy.has("unlock") && target.spy.has("close"));
        assert!(
            target
                .runtime
                .owner
                .manager
                .activity(2)
                .result
                .wait()
                .await
                .is_ok(),
            "genuine successful flushed prefix is retained"
        );
        cancel.cancel();
        let _ = building.await.unwrap();
        source.host.remove(descriptor(&target)).await.unwrap();
        drop(source.host);
        drop(target.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_copied_epoch_after_failed_record_retains_failed_flush_and_batch_ack() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let source = Source::start(partition, Vec::new(), None).await;
        let mut copied = single(1100, 1);
        copied.lsn = source.replicate(copied.clone()).await;
        let next = Epoch {
            data_loss_number: 1,
            configuration_number: 2 << 32,
        };
        source.host.change_epoch(next).await.unwrap();
        let before = Record {
            lsn: 1,
            kind: RecordKind::UpdateEpoch {
                epoch: epoch(),
                primary_replica_id: 1,
                timestamp_ticks: 0,
            },
        };
        let after = Record {
            lsn: 2,
            kind: RecordKind::UpdateEpoch {
                epoch: next,
                primary_replica_id: 1,
                timestamp_ticks: 0,
            },
        };
        let mut buffers = before.encode().unwrap().to_segments();
        buffers.extend(copied.encode().unwrap().to_segments());
        buffers.extend(after.encode().unwrap().to_segments());
        buffers.push(7i32.to_le_bytes().to_vec());
        *source.provider.records.lock().unwrap() =
            vec![header(7), OperationData::new(buffers).unwrap()];
        {
            let memory = target.runtime.owner.manager.memory().unwrap();
            let mut memory = memory.lock().unwrap();
            memory.write_error = Some(Error::InvalidInput("failed copy before UE"));
            memory.write_error_after = 1;
        }
        let member = descriptor(&target);
        let sender = source.host.clone();
        let cancel = Cancellation::default();
        let token = cancel.clone();
        let building = tokio::spawn(async move { sender.build(member, token).await });
        target
            .runtime
            .fault_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        until(|| {
            source
                .lower
                .snapshot()
                .peers
                .iter()
                .any(|p| p.copy_applied >= 2 || p.copy_applied == -1)
        })
        .await;
        let records = target.runtime.owner.manager.records();
        let epoch_record=records.iter().find(|a|matches!(a.node.record().kind,RecordKind::UpdateEpoch {epoch,..} if epoch==next)).unwrap();
        assert!(epoch_record.flush.wait().await.is_err());
        assert!(epoch_record.apply.wait().await.is_err());
        assert_eq!(target.runtime.owner.manager.tail(), 2);
        assert!(!target.spy.has("apply-start"));
        let lower = target.runtime.replicator();
        target
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                lower.abort();
                Ok(())
            })
            .await
            .unwrap();
        assert!(target.runtime.terminal.wait().await.is_err());
        target
            .runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        cancel.cancel();
        let _ = building.await.unwrap();
        source.host.remove(descriptor(&target)).await.unwrap();
        drop(source.host);
        drop(target.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_cached_abort_uses_service_cleanup_while_application_role_is_fenced() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let transaction = f.runtime.owner.create_transaction().unwrap();
        f.spy.point.write(transaction.as_ref(), 10, 1, 10).unwrap();
        let gate = Completion::new();
        *f.spy.role_gate.lock().unwrap() = Some((ReplicaRole::ActiveSecondary, gate.clone()));
        let changing = f.host.clone();
        let role =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        until(|| f.spy.has("role-start")).await;
        assert!(f.runtime.owner.create_transaction().is_err());
        let before = f.runtime.owner.manager.tail();
        transaction.abort().unwrap();
        assert_eq!(transaction.get_state(), TransactionState::Aborted);
        assert!(f.spy.has("unlock"));
        assert_eq!(
            f.runtime.owner.manager.tail(),
            before,
            "cached abort has no replicated End"
        );
        gate.complete(Ok(()));
        role.await.unwrap().unwrap();
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_cached_unlock_error_directly_notifies_native_without_processor_latch() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let transaction = f.runtime.owner.create_transaction().unwrap();
        f.spy.point.write(transaction.as_ref(), 10, 1, 10).unwrap();
        *f.spy.fail.lock().unwrap() = Some("unlock".into());
        transaction.abort().unwrap(); // wrapper masks inner callback failure
        f.runtime
            .provider_fault_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert_eq!(transaction.get_state(), TransactionState::Aborted);
        assert_eq!(f.runtime.provider_fault_reports.load(Ordering::Acquire), 1);
        assert_eq!(f.runtime.fault_reports.load(Ordering::Acquire), 0);
        assert!(!f.runtime.owner.manager.faults.is_closing());
        assert!(f.runtime.owner.manager.faults.service_error().is_none());
        assert!(!f.runtime.replicator().snapshot().closed);
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_actual_epoch_waits_accepted_unpublished_primary_prefix() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let warm = f.runtime.owner.create_transaction().unwrap();
        warm.add_operation(None, None, Some(payload(0)), 10, None)
            .unwrap();
        warm.commit().await.unwrap();
        let logging = f.runtime.logging.lock().unwrap().as_ref().unwrap().clone();
        logging.group_idle().await;
        let timer = Completion::new();
        *logging.group_pause.lock().unwrap() = Some(timer.clone());
        let next = f.runtime.replicator().snapshot().progress.assigned + 1;
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let (release, resume) = std::sync::mpsc::sync_channel(1);
        *logging.publication_pause.lock().unwrap() = Some((next, entered, resume));
        let transaction = f.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(None, None, Some(payload(1)), 10, None)
            .unwrap();
        drop(transaction.commit());
        tokio::task::spawn_blocking(move || observed.recv_timeout(Duration::from_secs(3)).unwrap())
            .await
            .unwrap();
        let changing = f.host.clone();
        let updated = tokio::spawn(async move {
            changing
                .change_epoch(Epoch {
                    data_loss_number: 1,
                    configuration_number: 2 << 32,
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!updated.is_finished());
        assert_eq!(f.runtime.owner.manager.tail(), next - 1);
        release.send(()).unwrap();
        updated.await.unwrap().unwrap();
        assert_eq!(
            f.runtime
                .owner
                .get_current_epoch()
                .unwrap()
                .configuration_number,
            2 << 32
        );
        timer.complete(Ok(()));
        logging.group_idle().await;
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_abort_dominates_inflight_provider_role_readiness_publication() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let gate = Completion::new();
        *f.spy.role_gate.lock().unwrap() = Some((ReplicaRole::ActiveSecondary, gate.clone()));
        let changing = f.host.clone();
        let role =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        until(|| f.spy.has("role-start")).await;
        let lower = f.runtime.replicator();
        f.native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                lower.abort();
                Ok(())
            })
            .await
            .unwrap();
        gate.complete(Ok(()));
        assert!(role.await.unwrap().is_err());
        let _ = f.runtime.terminal.wait().await;
        f.runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert!(!f.runtime.owner.get_is_readable());
        assert!(f.runtime.owner.create_transaction().is_err());
        assert!(f.runtime.owner.try_remove_checkpoint(0, 2).await.is_err());
        assert!(f.runtime.owner.try_remove_version(10, 0, 2).is_err());
        drop(f.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_corrective_information_allocation_failure_reports_error_retains_real_cleanup() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let warm = f.runtime.owner.create_transaction().unwrap();
        warm.add_operation(None, None, Some(payload(0)), 10, None)
            .unwrap();
        warm.commit().await.unwrap();
        let logging = f.runtime.logging.lock().unwrap().as_ref().unwrap().clone();
        logging.group_idle().await;
        let timer = Completion::new();
        *logging.group_pause.lock().unwrap() = Some(timer.clone());
        let transaction = f.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(None, None, Some(payload(1)), 10, None)
            .unwrap();
        let before = f.runtime.owner.manager.tail();
        drop(transaction.commit());
        until(|| f.runtime.owner.manager.tail() > before).await;
        let memory = f.runtime.owner.manager.memory().unwrap();
        let pressure = memory.lock().unwrap().occupy_remaining_allocation();
        let lower = f.runtime.replicator();
        let native = f.native.clone();
        let close = native
            .execution()
            .try_reserve()
            .unwrap()
            .run_future(async move { lower.close(Cancellation::default()).await });
        let failure = tokio::time::timeout(Duration::from_secs(3), close)
            .await
            .unwrap()
            .unwrap_err();
        assert!(super::status::Status::InsufficientResources.matches(&failure));
        assert!(!f.runtime.cleanup_done.is_completed());
        assert!(!f.spy.has("close-start"));
        assert_eq!(transaction.get_state(), TransactionState::Committing);
        // Caller-driven native boundary after genuine allocation recovery is
        // private ownership evidence, NOT automatic retry/fairness or successful
        // completion of the already-failed terminal attempt.
        drop(pressure);
        let actual = f.runtime.owner.manager.local_boundary(12, 4).unwrap();
        actual.processing.wait().await.as_ref().clone().unwrap();
        timer.complete(Ok(()));
        f.runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert!(f.runtime.terminal.wait().await.is_err());
        assert!(f.spy.has("close"));
        drop(f.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_provider_roles_start_all_peers_wait_all_and_preserve_first_array_failure() {
    for root in [false, true] {
        let first = Spy::named("fabric:/first");
        let second = Spy::named("fabric:/second");
        let (send, recv) = oneshot::channel();
        let a = first.clone();
        let b = second.clone();
        let host = rap::Host::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            move |native| {
                Box::new(Factory {
                    native,
                    created: Arc::new(Mutex::new(Some(send))),
                    provider: a,
                    extra: Some(b),
                    root,
                    settings: Default::default(),
                })
            },
        )
        .await;
        let (runtime, _) = recv.await.unwrap();
        *first.fail.lock().unwrap() = Some("role:ActiveSecondary".into());
        *second.fail.lock().unwrap() = Some("role:ActiveSecondary".into());
        let gate = Completion::new();
        *second.role_gate.lock().unwrap() = Some((ReplicaRole::ActiveSecondary, gate.clone()));
        let changing = host.clone();
        let changed =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        until(|| second.has("role-start")).await;
        assert!(first.has("role:ActiveSecondary"));
        assert!(
            !changed.is_finished(),
            "array-first failure must await every peer effect"
        );
        gate.complete(Ok(()));
        assert!(changed.await.unwrap().is_err());
        assert!(runtime
            .owner
            .manager
            .faults
            .service_error()
            .unwrap()
            .to_string()
            .contains("fabric:/first"));
        assert!(runtime.terminal.wait().await.is_err());
        runtime.cleanup_done.wait().await.as_ref().clone().unwrap();
        assert!(first.has("close") && second.has("close"));
        drop(host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_native_transient_write_revocation_allows_cache_then_original_lower_retry_status() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                write_quorum: 2,
                local_member: true,
                replicas: vec![fube_runtime::extensions::NativeReplicaDescriptor {
                    node_id: [3; 16],
                    node_instance: 1,
                    replica_id: 3,
                    incarnation: 100,
                    role: ReplicaRole::ActiveSecondary,
                    previous_role: ReplicaRole::None,
                    up: false,
                    ready: false,
                    must_catch_up: false,
                    address: fube_data::wire::Endpoint {
                        actor: fube_data::wire::Actor {
                            partition: f.native.identity().partition_id,
                            replica: 3,
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
                }],
            })
            .await
            .unwrap();
        assert_eq!(
            f.runtime.owner.partition.get_write_status().unwrap(),
            fube_runtime::ServicePartitionAccessStatus::NoWriteQuorum
        );
        let transaction = f.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(None, None, Some(payload(1)), 10, None)
            .unwrap();
        let writes = transaction.clone();
        let failure = f
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || writes.add_operation(None, None, Some(payload(2)), 10, None))
            .await
            .unwrap_err();
        assert!(super::status::Status::NoWriteQuorum.matches(&failure));
        assert_eq!(f.runtime.replicator().snapshot().progress.assigned, 1);
        assert!(!f.runtime.owner.manager.faults.is_closing());
        transaction.abort().unwrap();
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
    }
}
use super::{
    provider::PointProvider,
    record::{Record, RecordKind},
    runtime::{RuntimeConfig, VolatileRuntime},
    version::Completion,
};
use crate::*;
use fube_data::IStateReplicator;
use fube_data::{
    extensions::{ReplicationEndpointOptions, ReplicatorDiagnostics},
    qualification as rap,
};
use fube_runtime::{
    extensions::{
        NativeIntegrationProvider, NativeReplicaSetConfiguration, NativeRuntimeIntegration,
    },
    IPrimaryReplicator, IReplicator, IStatefulServiceFactory, IStatefulServiceReplica, OpenMode,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::oneshot;

fn epoch() -> Epoch {
    Epoch {
        data_loss_number: 1,
        configuration_number: 1 << 32,
    }
}
struct DataLoss;
impl IDataLossHandler for DataLoss {
    fn data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
}
struct Spy {
    point: Arc<PointProvider>,
    owner: Mutex<Option<Weak<dyn ITransactionalReplicator>>>,
    runtime: Mutex<Weak<VolatileRuntime>>,
    events: Mutex<Vec<String>>,
    fail: Mutex<Option<String>>,
    apply_gate: Mutex<Option<Completion<()>>>,
    close_gate: Mutex<Option<Completion<()>>>,
    role_gate: Mutex<Option<(ReplicaRole, Completion<()>)>>,
}
impl Spy {
    fn new() -> Arc<Self> {
        Self::named("fabric:/phase2")
    }
    fn named(name: &str) -> Arc<Self> {
        Arc::new(Self {
            point: PointProvider::preinitialized(name),
            owner: Mutex::new(None),
            runtime: Mutex::new(Weak::new()),
            events: Mutex::new(Vec::new()),
            fail: Mutex::new(None),
            apply_gate: Mutex::new(None),
            close_gate: Mutex::new(None),
            role_gate: Mutex::new(None),
        })
    }
    fn event(&self, event: impl Into<String>) -> Result<()> {
        let event = event.into();
        self.events.lock().unwrap().push(event.clone());
        if self.fail.lock().unwrap().as_ref() == Some(&event) {
            Err(Error::Remote {
                code: 0xc0000001u32 as i32 as i64,
                message: format!("controlled provider failure {}", self.get_name()),
            })
        } else {
            Ok(())
        }
    }
    fn has(&self, event: &str) -> bool {
        self.events.lock().unwrap().iter().any(|e| e == event)
    }
}
macro_rules! point {
    ($name:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty) => { fn $name(&self, $($arg:$ty),*) -> $ret { self.point.$name($($arg),*) } };
}
impl IStateProvider2 for Spy {
    fn get_name(&self) -> &str {
        self.point.get_name()
    }
    fn get_children(&self, _: &str) -> Result<Vec<StateProviderInfo>> {
        Ok(Vec::new())
    }
    fn initialize(
        &self,
        owner: Weak<dyn ITransactionalReplicator>,
        _: &Path,
        children: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()> {
        assert!(children.is_none());
        let callback = owner.upgrade().unwrap();
        assert_eq!(callback.get_last_committed_sequence_number().unwrap(), 1);
        assert!(!callback.get_is_readable());
        assert!(callback.create_transaction().is_err());
        assert!(
            !self
                .runtime
                .lock()
                .unwrap()
                .upgrade()
                .unwrap()
                .replicator()
                .snapshot()
                .closed
        );
        *self.owner.lock().unwrap() = Some(owner);
        self.event("initialize")
    }
    fn open(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.event("open") })
    }
    fn recover_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let owner = self
                .owner
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            owner.try_remove_checkpoint(0, 2).await?; // actual callback-ready forwarding, not stub weak owner
            self.event("recover")
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let runtime = self.runtime.lock().unwrap().upgrade().unwrap();
            let actual = runtime.replicator().snapshot().role;
            assert!(
                actual == role
                    || (role == ReplicaRole::IdleSecondary
                        && actual == ReplicaRole::ActiveSecondary)
            );
            assert!(!runtime.owner.ready.load(Ordering::Acquire));
            let gate = self.role_gate.lock().unwrap().clone();
            if let Some((expected, gate)) = gate {
                if expected == role {
                    self.event("role-start")?;
                    gate.wait().await.as_ref().clone()?;
                }
            }
            self.event(format!("role:{role:?}"))
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("close-start")?;
            let gate = self.close_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await.as_ref().clone()?;
            }
            self.event("close")
        })
    }
    fn abort(&self) {
        let _ = self.event("abort");
    }
    fn apply(
        &self,
        lsn: i64,
        tx: Arc<dyn TransactionBase>,
        context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>> {
        Box::pin(async move {
            self.event("apply-start")?;
            let gate = self.apply_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await.as_ref().clone()?;
            }
            let result = self.point.apply(lsn, tx, context, metadata, data).await?;
            self.event("apply-end")?;
            Ok(result)
        })
    }
    fn unlock(&self, context: &dyn OperationContext) -> Result<()> {
        self.event("unlock")?;
        self.point.unlock(context)
    }
    fn remove_state(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.event("remove") })
    }
    point!(prepare_checkpoint(lsn: i64) -> Result<()>);
    point!(perform_checkpoint(cancellation: Cancellation) -> ApiFuture<'_, ()>);
    point!(complete_checkpoint(cancellation: Cancellation) -> ApiFuture<'_, ()>);
    point!(backup_checkpoint(path: PathBuf, cancellation: Cancellation) -> ApiFuture<'_, ()>);
    point!(restore_checkpoint(path: PathBuf, cancellation: Cancellation) -> ApiFuture<'_, ()>);
    point!(get_current_state() -> Result<Option<Arc<dyn IOperationDataStream>>>);
    point!(begin_setting_current_state() -> ApiFuture<'_, ()>);
    point!(
        set_current_state(
            number: i64,
            data: OperationData,
            cancellation: Cancellation,
        ) -> ApiFuture<'_, ()>
    );
    point!(end_setting_current_state(cancellation: Cancellation) -> ApiFuture<'_, ()>);
    point!(
        prepare_for_remove(
            transaction: Arc<dyn TransactionBase>,
            cancellation: Cancellation,
        ) -> ApiFuture<'_, ()>
    );
}
type Created =
    Arc<Mutex<Option<oneshot::Sender<(Arc<VolatileRuntime>, NativeRuntimeIntegration)>>>>;
struct Factory {
    native: NativeIntegrationProvider,
    created: Created,
    provider: Arc<Spy>,
    extra: Option<Arc<Spy>>,
    root: bool,
    settings: fube_data::Settings,
}
struct Replica {
    native: NativeRuntimeIntegration,
    created: Created,
    provider: Arc<Spy>,
    extra: Option<Arc<Spy>>,
    root: bool,
    settings: fube_data::Settings,
    runtime: Mutex<Option<Arc<VolatileRuntime>>>,
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
            provider: self.provider.clone(),
            extra: self.extra.clone(),
            root: self.root,
            settings: self.settings.clone(),
            runtime: Mutex::new(None),
        }))
    }
}
impl IStatefulServiceReplica for Replica {
    fn open(
        &self,
        _: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            let mut providers: Vec<(i64, Arc<dyn IStateProvider2>)> =
                vec![(10, self.provider.clone())];
            if let Some(extra) = &self.extra {
                providers.push((11, extra.clone()));
            }
            let runtime = VolatileRuntime::create(
                partition,
                self.native.clone(),
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                self.settings.clone(),
                RuntimeConfig {
                    log_settings: Default::default(),
                    registry: None,
                    providers,
                    work_folder: PathBuf::from("/tmp/fube-phase2"),
                    memory_bytes: 1024 * 1024,
                    data_loss: Arc::new(DataLoss),
                },
            )?;
            *self.provider.runtime.lock().unwrap() = Arc::downgrade(&runtime);
            if let Some(extra) = &self.extra {
                *extra.runtime.lock().unwrap() = Arc::downgrade(&runtime);
            }
            runtime
                .replicator()
                .qualification_use_root_controls(self.root);
            *self.runtime.lock().unwrap() = Some(runtime.clone());
            self.created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send((runtime.clone(), self.native.clone()))
                .ok();
            Ok(Box::new((*runtime.replicator()).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://phase2".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {
        if let Some(runtime) = self.runtime.lock().unwrap().as_ref() {
            runtime.owner.manager.close_activity_gate();
        }
    }
}
struct Fixture {
    host: Arc<rap::Host>,
    runtime: Arc<VolatileRuntime>,
    native: NativeRuntimeIntegration,
    spy: Arc<Spy>,
}
impl Fixture {
    async fn start(
        partition: uuid::Uuid,
        id: i64,
        role: ReplicaRole,
        root: bool,
        spy: Arc<Spy>,
    ) -> Self {
        let (send, recv) = oneshot::channel();
        let input = spy.clone();
        let host = rap::Host::start(partition, id, role, move |native| {
            Box::new(Factory {
                native,
                created: Arc::new(Mutex::new(Some(send))),
                provider: input,
                extra: None,
                root,
                settings: fube_data::Settings {
                    provider_operations: 1,
                    ..Default::default()
                },
            })
        })
        .await;
        let (runtime, native) = recv.await.unwrap();
        Self {
            host,
            runtime,
            native,
            spy,
        }
    }
    async fn configure(&self) {
        self.host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                replicas: vec![],
                write_quorum: 1,
                local_member: true,
            })
            .await
            .unwrap();
    }
}
async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn payload(key: i64) -> OperationData {
    OperationData::new(vec![[key.to_le_bytes(), (key * 10).to_le_bytes()].concat()]).unwrap()
}

struct Frames {
    records: Vec<OperationData>,
    next: Mutex<usize>,
    eof: Option<Completion<()>>,
}
impl fube_data::IOperationDataStream for Frames {
    fn get_next(&self, _: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            let next = {
                let mut next = self.next.lock().unwrap();
                let current = *next;
                *next += 1;
                current
            };
            if let Some(record) = self.records.get(next) {
                return Ok(Some(record.clone()));
            }
            if let Some(eof) = &self.eof {
                eof.wait().await.as_ref().clone()?;
            }
            Ok(None)
        })
    }
}
struct CopySource {
    tail: AtomicI64,
    records: Mutex<Vec<OperationData>>,
    eof: Mutex<Option<Completion<()>>>,
}
impl fube_data::IStateProvider for CopySource {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        Ok(self.tail.load(Ordering::Acquire))
    }
    fn update_epoch(&self, _: Epoch, _: i64, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        Err(Error::InvalidInput("volatile fixture has null context"))
    }
    fn get_copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn fube_data::IOperationDataStream>>,
    ) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        assert!(context.is_none());
        assert!(up_to <= self.tail.load(Ordering::Acquire));
        Ok(Arc::new(Frames {
            records: self.records.lock().unwrap().clone(),
            next: Mutex::new(0),
            eof: self.eof.lock().unwrap().clone(),
        }))
    }
}
type SourceCreated = Arc<
    Mutex<Option<oneshot::Sender<(Arc<fube_data::FabricReplicator>, NativeRuntimeIntegration)>>>,
>;
struct SourceFactory {
    native: NativeIntegrationProvider,
    created: SourceCreated,
    provider: Arc<CopySource>,
}
struct SourceReplica {
    native: NativeRuntimeIntegration,
    created: SourceCreated,
    provider: Arc<CopySource>,
}
impl IStatefulServiceFactory for SourceFactory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        id: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        Ok(Box::new(SourceReplica {
            native: self.native.capture(partition, id)?,
            created: self.created.clone(),
            provider: self.provider.clone(),
        }))
    }
}
impl IStatefulServiceReplica for SourceReplica {
    fn open(
        &self,
        _: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            let lower = fube_data::FabricReplicator::create(
                partition,
                self.native.clone(),
                self.provider.clone(),
                None,
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                Default::default(),
            )?;
            self.created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send((lower.clone(), self.native.clone()))
                .ok();
            Ok(Box::new((*lower).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://controlled-copy-source".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Source {
    host: Arc<rap::Host>,
    lower: Arc<fube_data::FabricReplicator>,
    provider: Arc<CopySource>,
}
impl Source {
    async fn start(
        partition: uuid::Uuid,
        records: Vec<OperationData>,
        eof: Option<Completion<()>>,
    ) -> Self {
        let (send, recv) = oneshot::channel();
        let provider = Arc::new(CopySource {
            tail: AtomicI64::new(1),
            records: Mutex::new(records),
            eof: Mutex::new(eof),
        });
        let input = provider.clone();
        let host = rap::Host::start(partition, 1, ReplicaRole::Primary, move |native| {
            Box::new(SourceFactory {
                native,
                created: Arc::new(Mutex::new(Some(send))),
                provider: input,
            })
        })
        .await;
        let (lower, _) = recv.await.unwrap();
        host.configure(NativeReplicaSetConfiguration {
            epoch: epoch(),
            replicas: vec![],
            write_quorum: 1,
            local_member: true,
        })
        .await
        .unwrap();
        Self {
            host,
            lower,
            provider,
        }
    }
    async fn replicate(&self, record: Record) -> i64 {
        use fube_data::IStateReplicator;
        let mut admitted = self
            .lower
            .replicate(record.encode().unwrap(), Cancellation::default())
            .await
            .unwrap();
        self.provider
            .tail
            .store(admitted.sequence_number, Ordering::Release);
        admitted
            .completion
            .wait(Cancellation::default())
            .await
            .unwrap();
        admitted.sequence_number
    }
}
fn header(stage: i32) -> OperationData {
    OperationData::new(vec![[
        1u32.to_le_bytes().as_slice(),
        stage.to_le_bytes().as_slice(),
        1i64.to_le_bytes().as_slice(),
    ]
    .concat()])
    .unwrap()
}
fn single(id: i64, key: i64) -> Record {
    Record {
        lsn: -1,
        kind: RecordKind::Begin {
            transaction_id: id,
            single: true,
            metadata: Some(super::named::encode(10, None).unwrap()),
            redo: Some(payload(key)),
            undo: None,
        },
    }
}
fn descriptor(target: &Fixture) -> fube_runtime::extensions::NativeReplicaDescriptor {
    fube_runtime::extensions::NativeReplicaDescriptor {
        node_id: [2; 16],
        node_instance: 1,
        replica_id: 2,
        incarnation: 12,
        role: ReplicaRole::IdleSecondary,
        previous_role: ReplicaRole::None,
        up: true,
        ready: true,
        must_catch_up: false,
        address: target.runtime.replicator().endpoint().to_string(),
        progress: fube_runtime::extensions::ReplicationProgress { first: 1, last: 1 },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_copy_wait_idle_active_keeps_live_replication_and_single_capture_reaches_barrier() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let eof = Completion::new();
        let source = Source::start(partition, vec![header(2)], Some(eof.clone())).await;
        let node = descriptor(&target);
        let build_host = source.host.clone();
        let building =
            tokio::spawn(async move { build_host.build(node, Cancellation::default()).await });
        until(|| {
            target.runtime.replicator().snapshot().operation_captures == 0
                && target
                    .runtime
                    .replicator()
                    .snapshot()
                    .dispatched_unacknowledged
                    == 0
                && !target.runtime.replicator().snapshot().copy_done
        })
        .await;
        let changing = target.host.clone();
        let active =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!target.spy.has("role:ActiveSecondary"));
        eof.complete(Ok(()));
        building.await.unwrap().unwrap();
        active.await.unwrap().unwrap();
        assert!(
            !target
                .runtime
                .generation
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .done
                .is_completed(),
            "Idle→Active must not join still-live replication EOS"
        );
        assert!(target.runtime.owner.get_is_readable());
        let mut node = descriptor(&target);
        node.role = ReplicaRole::ActiveSecondary;
        source
            .host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                replicas: vec![node],
                write_quorum: 2,
                local_member: true,
            })
            .await
            .unwrap();
        let n = source.replicate(single(200, 1)).await;
        until(|| target.runtime.replicator().snapshot().progress.applied >= n).await;
        assert!(
            !target.spy.has("apply-start"),
            "flush ACK is not transaction Apply"
        );
        let barrier = source
            .replicate(Record {
                lsn: -1,
                kind: RecordKind::Barrier { last_stable_lsn: 1 },
            })
            .await;
        until(|| target.spy.has("apply-end") && target.runtime.owner.manager.version() >= barrier)
            .await;
        assert_eq!(target.runtime.replicator().snapshot().operation_captures, 0);
        drop(source.host);
        drop(target.host);
        target
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_promotion_waits_real_replication_eof_information_apply_before_provider_role() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let source = Source::start(partition, vec![header(2)], None).await;
        source
            .host
            .build(descriptor(&target), Cancellation::default())
            .await
            .unwrap();
        target
            .host
            .change_role(ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        let mut node = descriptor(&target);
        node.role = ReplicaRole::ActiveSecondary;
        source
            .host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                replicas: vec![node],
                write_quorum: 2,
                local_member: true,
            })
            .await
            .unwrap();
        let gate = Completion::new();
        *target.spy.apply_gate.lock().unwrap() = Some(gate.clone());
        let sequence = source.replicate(single(300, 1)).await;
        until(|| target.runtime.replicator().snapshot().progress.applied >= sequence).await;
        assert!(!target.spy.has("apply-start"));
        let changing = target.host.clone();
        let promoted =
            tokio::spawn(async move { changing.change_role(ReplicaRole::Primary).await });
        until(|| target.spy.has("apply-start")).await; // EOF Information, NO final replicated Barrier
        assert!(!target.spy.has("role:Primary"));
        // Native ActiveSecondary readability is the applied-boundary/recovery
        // frontier comparison, not a blanket pending-promotion read prohibition.
        gate.complete(Ok(()));
        promoted.await.unwrap().unwrap();
        let events = target.spy.events.lock().unwrap().clone();
        assert!(
            events.iter().position(|e| e == "unlock").unwrap()
                < events.iter().position(|e| e == "role:Primary").unwrap()
        );
        assert_eq!(target.runtime.owner.manager.tail(), sequence);
        assert!(target
            .runtime
            .owner
            .manager
            .records()
            .iter()
            .any(|a| matches!(
                a.node.record().kind,
                RecordKind::Physical {
                    native_type: 12,
                    event: 3
                }
            ) && a.node.lsn() == sequence));
        drop(source.host);
        drop(target.host);
        target
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_log_batch_copy_preserves_lsns_and_copy_processing_fence_before_active_role() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let source = Source::start(partition, Vec::new(), None).await;
        let mut records = vec![
            Record {
                lsn: -1,
                kind: RecordKind::Begin {
                    transaction_id: 400,
                    single: false,
                    metadata: Some(super::named::encode(10, None).unwrap()),
                    redo: Some(payload(1)),
                    undo: None,
                },
            },
            Record {
                lsn: -1,
                kind: RecordKind::Operation {
                    transaction_id: 400,
                    redo_only: false,
                    metadata: Some(super::named::encode(10, None).unwrap()),
                    redo: Some(payload(2)),
                    undo: None,
                },
            },
            Record {
                lsn: -1,
                kind: RecordKind::End {
                    transaction_id: 400,
                    committed: true,
                },
            },
        ];
        for record in &mut records {
            record.lsn = source.replicate(record.clone()).await;
        }
        let mut segments = Vec::new();
        for record in &records {
            segments.extend(record.encode().unwrap().to_segments());
        }
        segments.push(7i32.to_le_bytes().to_vec());
        *source.provider.records.lock().unwrap() =
            vec![header(7), OperationData::new(segments).unwrap()];
        let gate = Completion::new();
        *target.spy.apply_gate.lock().unwrap() = Some(gate.clone());
        let node = descriptor(&target);
        let build_host = source.host.clone();
        let building =
            tokio::spawn(async move { build_host.build(node, Cancellation::default()).await });
        until(|| target.spy.has("apply-start")).await; // CopyFinished Info at embedded LSN4, no replicated Barrier
        assert_eq!(target.runtime.owner.manager.tail(), 4);
        assert!(!target
            .runtime
            .generation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .copy_done
            .is_completed());
        let changing = target.host.clone();
        let active =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!target.spy.has("role:ActiveSecondary"));
        gate.complete(Ok(()));
        building.await.unwrap().unwrap();
        active.await.unwrap().unwrap();
        let applied = target
            .spy
            .point
            .events()
            .into_iter()
            .filter_map(|event| match event {
                super::provider::PointEvent::Apply {
                    operation, commit, ..
                } => Some((operation, commit)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            applied,
            vec![(2, 4), (3, 4)],
            "copy operation ordinal2 must NOT replace embedded record LSNs"
        );
        drop(source.host);
        drop(target.host);
        target
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_demotion_information_drains_apply_then_fresh_generation_old_eos_never_revives() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let warm = f.runtime.owner.create_transaction().unwrap();
        warm.add_operation(None, None, Some(payload(0)), 10, None)
            .unwrap();
        warm.commit().await.unwrap();
        let logging = f.runtime.logging.lock().unwrap().as_ref().unwrap().clone();
        logging.group_idle().await;
        let timer = Completion::new();
        *logging.group_pause.lock().unwrap() = Some(timer.clone());
        let apply = Completion::new();
        *f.spy.apply_gate.lock().unwrap() = Some(apply.clone());
        let transaction = f.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(None, None, Some(payload(1)), 10, None)
            .unwrap();
        let before = f.runtime.owner.manager.tail();
        drop(transaction.commit());
        until(|| f.runtime.owner.manager.tail() > before).await;
        let assigned = f.runtime.replicator().snapshot().progress.assigned;
        let changing = f.host.clone();
        let demoted =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        until(|| {
            f.spy.has("apply-start")
                && f.runtime.owner.manager.records().iter().any(|a| {
                    matches!(
                        a.node.record().kind,
                        RecordKind::Physical {
                            native_type: 12,
                            event: 5
                        }
                    )
                })
        })
        .await;
        assert!(!f.spy.has("role:ActiveSecondary"));
        assert_eq!(
            f.runtime.replicator().snapshot().progress.assigned,
            assigned,
            "localInfo has no lower LSN"
        );
        apply.complete(Ok(()));
        timer.complete(Ok(()));
        demoted.await.unwrap().unwrap();
        let old = f.runtime.generation.lock().unwrap().clone().unwrap();
        let old_stream = f.runtime.replicator().get_replication_stream().unwrap();
        f.host.change_role(ReplicaRole::Primary).await.unwrap();
        assert!(old.done.is_completed());
        // A genuinely new native-issued reconfiguration generation, not an
        // identical repeated semantic RAP role request that may match an older
        // retry reply. No custom correlation field/protocol is introduced.
        f.host
            .change_epoch(Epoch {
                data_loss_number: 1,
                configuration_number: 2 << 32,
            })
            .await
            .unwrap();
        f.host
            .change_role(ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        let fresh = f.runtime.generation.lock().unwrap().clone();
        assert!(
            fresh.is_some(),
            "role events {:?}; provider {:?}",
            f.runtime.role_events.lock().unwrap(),
            f.spy.events.lock().unwrap()
        );
        let fresh = fresh.unwrap();
        assert!(!Arc::ptr_eq(&old, &fresh));
        assert!(old_stream
            .get_operation(Cancellation::default())
            .await
            .unwrap()
            .is_none());
        assert!(!fresh.done.is_completed());
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_close_drop_and_sync_abort_retain_owned_apply_before_provider_close() {
    for root in [false, true] {
        for abort in [false, true] {
            let f = Fixture::start(
                uuid::Uuid::new_v4(),
                1,
                ReplicaRole::Primary,
                root,
                Spy::new(),
            )
            .await;
            f.configure().await;
            let warm = f.runtime.owner.create_transaction().unwrap();
            warm.add_operation(None, None, Some(payload(0)), 10, None)
                .unwrap();
            warm.commit().await.unwrap();
            let logging = f.runtime.logging.lock().unwrap().as_ref().unwrap().clone();
            logging.group_idle().await;
            let timer = Completion::new();
            *logging.group_pause.lock().unwrap() = Some(timer.clone());
            let apply = Completion::new();
            *f.spy.apply_gate.lock().unwrap() = Some(apply.clone());
            let transaction = f.runtime.owner.create_transaction().unwrap();
            transaction
                .add_operation(None, None, Some(payload(1)), 10, None)
                .unwrap();
            let tail = f.runtime.owner.manager.tail();
            drop(transaction.commit());
            until(|| f.runtime.owner.manager.tail() > tail).await;
            let lower = f.runtime.replicator();
            let permit = f.native.execution().try_reserve().unwrap();
            if abort {
                permit
                    .run_owned(move || {
                        lower.abort();
                        Ok(())
                    })
                    .await
                    .unwrap();
            } else {
                let observed =
                    permit.run_future(async move { lower.close(Cancellation::default()).await });
                until(|| f.runtime.replicator().snapshot().closed).await;
                drop(observed); // observer abandonment is NOT lifecycle cancellation
            }
            until(|| {
                f.runtime.owner.manager.records().iter().any(|a| {
                    matches!(
                        a.node.record().kind,
                        RecordKind::Physical {
                            native_type: 12,
                            event: 4
                        }
                    )
                })
            })
            .await;
            assert!(
                !f.runtime.terminal.is_completed(),
                "syncAbort is not completed upper drain"
            );
            assert!(!f.spy.has("close-start"));
            assert!(!f.runtime.owner.get_is_readable());
            apply.complete(Ok(()));
            timer.complete(Ok(()));
            f.runtime.terminal.wait().await.as_ref().clone().unwrap();
            assert!(f.spy.has("close"));
            assert!(f.spy.has("unlock"));
            drop(f.host);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_real_open_recover_failure_owns_only_opened_cleanup_and_close_abort_fallback() {
    for root in [false, true] {
        for failed in ["initialize", "open", "recover"] {
            let (send, recv) = oneshot::channel();
            let spy = Spy::new();
            *spy.fail.lock().unwrap() = Some(failed.into());
            let input = spy.clone();
            let (host, result) = rap::Host::start_source_outcome(
                uuid::Uuid::new_v4(),
                rap::ReplicaSource {
                    replica_id: 1,
                    native_incarnation: 11,
                    node_id: [1; 16],
                    node_instance: 1,
                },
                ReplicaRole::Primary,
                move |native| {
                    Box::new(Factory {
                        native,
                        created: Arc::new(Mutex::new(Some(send))),
                        provider: input,
                        extra: None,
                        root,
                        settings: Default::default(),
                    })
                },
            )
            .await;
            let (runtime, _) = recv.await.unwrap();
            assert!(result.is_err());
            assert!(!runtime.owner.get_is_readable());
            assert!(!spy.has("role:Primary"));
            assert_eq!(
                spy.has("close"),
                failed == "recover",
                "only successful Open transfers cleanup ownership"
            );
            drop(host);
        }
    }
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        *f.spy.fail.lock().unwrap() = Some("close-start".into());
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
        assert!(
            f.spy.has("abort"),
            "native Close failure action AbortStateProvider succeeds terminally"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_role_failure_fences_same_owner_and_reports_first_processor_fault_once() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        *f.spy.fail.lock().unwrap() = Some("role:ActiveSecondary".into());
        assert!(f
            .host
            .change_role(ReplicaRole::ActiveSecondary)
            .await
            .is_err());
        until(|| f.runtime.replicator().snapshot().closed).await;
        f.runtime.fault_done.wait().await.as_ref().clone().unwrap();
        assert!(
            f.runtime.terminal.wait().await.is_err(),
            "failed provider role is not a successful terminal boundary"
        );
        f.runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert_eq!(f.runtime.fault_reports.load(Ordering::Acquire), 1);
        assert_eq!(f.runtime.owner.manager.faults.reported().len(), 1);
        assert!(!f.runtime.owner.get_is_readable());
        assert!(f.spy.has("close"));
        drop(f.host);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_real_lower_first_provider_callbacks_and_native_read_write_access_on_root_bound() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        assert_eq!(
            &f.spy.events.lock().unwrap()[..4],
            ["initialize", "open", "recover", "role:Primary"]
        );
        f.configure().await;
        assert!(f.runtime.owner.get_is_readable());
        let transaction = f.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(None, None, Some(payload(1)), 10, None)
            .unwrap();
        transaction.commit().await.unwrap();
        assert!(f.spy.has("apply-end"));
        assert!(f.spy.has("unlock"));
        drop(f.host);
        f.runtime.terminal.wait().await.as_ref().clone().unwrap();
        assert!(f.spy.has("close"));
        assert!(!f.runtime.owner.get_is_readable());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_first_apply_fault_reports_once_without_inventing_lower_abort_or_quorum_success() {
    for root in [false, true] {
        let f = Fixture::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            root,
            Spy::new(),
        )
        .await;
        f.configure().await;
        let warm = f.runtime.owner.create_transaction().unwrap();
        warm.add_operation(None, None, Some(payload(0)), 10, None)
            .unwrap();
        warm.commit().await.unwrap();
        let logging = f.runtime.logging.lock().unwrap().as_ref().unwrap().clone();
        logging.group_idle().await;
        let previous = f
            .spy
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.as_str() == "unlock")
            .count();
        *f.spy.fail.lock().unwrap() = Some("apply-start".into());
        f.host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                write_quorum: 2,
                local_member: true,
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
                            partition: f.native.identity().partition_id,
                            replica: 3,
                            incarnation: uuid::Uuid::new_v4(),
                        },
                        address: "127.0.0.1:1".into(),
                        transport_id: Some(String::new()),
                    }
                    .to_string(),
                    progress: fube_runtime::extensions::ReplicationProgress {
                        first: 1,
                        last: f.runtime.replicator().snapshot().progress.committed,
                    },
                }],
            })
            .await
            .unwrap();
        let transaction = f.runtime.owner.create_transaction().unwrap();
        f.spy.point.write(transaction.as_ref(), 10, 1, 10).unwrap();
        let cached = f.runtime.owner.create_transaction().unwrap();
        f.spy.point.write(cached.as_ref(), 10, 3, 30).unwrap();
        drop(transaction.commit());
        f.runtime.fault_done.wait().await.as_ref().clone().unwrap();
        assert!(
            !f.runtime.replicator().snapshot().closed,
            "native ReportFault does NOT issue V1 Abort"
        );
        assert_eq!(transaction.get_state(), TransactionState::Committing);
        assert_eq!(f.runtime.fault_reports.load(Ordering::Acquire), 1);
        assert!(!f.runtime.owner.get_is_readable());
        assert!(f.runtime.owner.create_transaction().is_err());
        assert_eq!(
            f.spy
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.as_str() == "unlock")
                .count(),
            previous,
            "accepted failure retains quorum and original Unlock"
        );
        cached.abort().unwrap(); // live service cleanup survives first-fault app fencing
        assert_eq!(cached.get_state(), TransactionState::Aborted);
        // Actual native control, representing RA's later terminal decision.
        let lower = f.runtime.replicator();
        f.native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                lower.abort();
                Ok(())
            })
            .await
            .unwrap();
        assert!(f.runtime.terminal.wait().await.is_err());
        f.runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        until(|| transaction.get_state() == TransactionState::Faulted).await;
        assert_eq!(
            f.spy
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.as_str() == "unlock")
                .count(),
            previous + 2
        );
        assert!(f.spy.has("close"));
        drop(f.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_provider_role_callback_readiness_fence_and_native_transient_write_access() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let source = Source::start(partition, vec![header(2)], None).await;
        source
            .host
            .build(descriptor(&target), Cancellation::default())
            .await
            .unwrap();
        let role = Completion::new();
        *target.spy.role_gate.lock().unwrap() = Some((ReplicaRole::ActiveSecondary, role.clone()));
        let changing = target.host.clone();
        let active =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        until(|| target.spy.has("role-start")).await;
        assert!(!target.runtime.owner.get_is_readable());
        assert!(target.runtime.owner.create_transaction().is_err());
        let mut member = descriptor(&target);
        member.role = ReplicaRole::ActiveSecondary;
        source
            .host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                replicas: vec![member],
                write_quorum: 2,
                local_member: true,
            })
            .await
            .unwrap();
        source.replicate(single(800, 1)).await;
        source
            .replicate(Record {
                lsn: -1,
                kind: RecordKind::Barrier { last_stable_lsn: 1 },
            })
            .await;
        assert!(
            !target.spy.has("apply-start"),
            "paused dispatch must not race provider role callback despite flush/ACK progress"
        );
        role.complete(Ok(()));
        active.await.unwrap().unwrap();
        until(|| target.spy.has("apply-end")).await;
        assert!(target.runtime.owner.get_is_readable());
        assert!(target.runtime.owner.get("fabric:/phase2").is_ok());
        drop(source.host);
        drop(target.host);
        target
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_failed_replication_flush_never_acks_and_unsupported_copy_batch_preflights_all() {
    for root in [false, true] {
        for unsupported in [false, true] {
            let partition = uuid::Uuid::new_v4();
            let target =
                Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
            let source = Source::start(partition, vec![header(2)], None).await;
            if unsupported {
                let mut valid = single(600, 1);
                valid.lsn = source.replicate(valid.clone()).await;
                let mut segments = valid.encode().unwrap().to_segments();
                let mut invalid = Record {
                    lsn: -1,
                    kind: RecordKind::Operation {
                        transaction_id: -601,
                        redo_only: true,
                        metadata: Some(super::named::encode(10, None).unwrap()),
                        redo: Some(payload(2)),
                        undo: None,
                    },
                };
                invalid.lsn = source.replicate(invalid.clone()).await;
                segments.extend(invalid.encode().unwrap().to_segments());
                segments.push(7i32.to_le_bytes().to_vec());
                *source.provider.records.lock().unwrap() =
                    vec![header(7), OperationData::new(segments).unwrap()];
                let node = descriptor(&target);
                let sender = source.host.clone();
                let cancel = Cancellation::default();
                let token = cancel.clone();
                let building = tokio::spawn(async move { sender.build(node, token).await });
                target
                    .runtime
                    .fault_done
                    .wait()
                    .await
                    .as_ref()
                    .clone()
                    .unwrap();
                assert_eq!(
                    target.runtime.owner.manager.tail(),
                    1,
                    "unsupported later record rejects BEFORE valid earlier batch append"
                );
                assert!(target.runtime.owner.manager.map.latest_records().is_empty());
                assert!(!target.spy.has("apply-start"));
                cancel.cancel();
                let _ = building.await.unwrap();
                source.host.remove(descriptor(&target)).await.unwrap();
            } else {
                source
                    .host
                    .build(descriptor(&target), Cancellation::default())
                    .await
                    .unwrap();
                target
                    .host
                    .change_role(ReplicaRole::ActiveSecondary)
                    .await
                    .unwrap();
                let mut node = descriptor(&target);
                node.role = ReplicaRole::ActiveSecondary;
                source
                    .host
                    .configure(NativeReplicaSetConfiguration {
                        epoch: epoch(),
                        replicas: vec![node],
                        write_quorum: 2,
                        local_member: true,
                    })
                    .await
                    .unwrap();
                target
                    .runtime
                    .owner
                    .manager
                    .memory()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .write_error =
                    Some(Error::InvalidInput("controlled replication flush failure"));
                let admitted = source
                    .lower
                    .replicate(single(700, 1).encode().unwrap(), Cancellation::default())
                    .await
                    .unwrap();
                source
                    .provider
                    .tail
                    .store(admitted.sequence_number, Ordering::Release);
                target
                    .runtime
                    .fault_done
                    .wait()
                    .await
                    .as_ref()
                    .clone()
                    .unwrap();
                assert_eq!(
                    target.runtime.replicator().snapshot().progress.applied,
                    1,
                    "failed flush has NO service ACK"
                );
                assert!(!target.spy.has("apply-start"));
                assert!(target
                    .runtime
                    .owner
                    .manager
                    .activity(2)
                    .flush
                    .wait()
                    .await
                    .is_err());
            }

            let lower = target.runtime.replicator();
            target
                .native
                .execution()
                .try_reserve()
                .unwrap()
                .run_owned(move || {
                    lower.abort();
                    Ok(())
                })
                .await
                .unwrap();
            assert!(target.runtime.terminal.wait().await.is_err());
            target
                .runtime
                .cleanup_done
                .wait()
                .await
                .as_ref()
                .clone()
                .unwrap();
            assert_eq!(target.runtime.fault_reports.load(Ordering::Acquire), 1);
            drop(source.host);
            drop(target.host);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase2_copied_log_flush_failure_settles_then_acks_batch_but_never_ends_state_or_claims_success(
) {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let target =
            Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, Spy::new()).await;
        let source = Source::start(partition, Vec::new(), None).await;
        let mut copied = single(900, 1);
        copied.lsn = source.replicate(copied.clone()).await;
        let update = Record {
            lsn: 1,
            kind: RecordKind::UpdateEpoch {
                epoch: epoch(),
                primary_replica_id: 1,
                timestamp_ticks: 0,
            },
        };
        let mut buffers = update.encode().unwrap().to_segments();
        buffers.extend(copied.encode().unwrap().to_segments());
        buffers.push(7i32.to_le_bytes().to_vec());
        *source.provider.records.lock().unwrap() =
            vec![header(7), OperationData::new(buffers).unwrap()];
        // The copied local UE initializes the real source epoch before the
        // existing-owned logical write fails. Fail N, not bootstrap/metadata.
        target
            .runtime
            .owner
            .manager
            .memory()
            .unwrap()
            .lock()
            .unwrap()
            .write_error = Some(Error::InvalidInput("copied batch flush failure"));
        target
            .runtime
            .owner
            .manager
            .memory()
            .unwrap()
            .lock()
            .unwrap()
            .write_error_after = 1;
        let member = descriptor(&target);
        let sender = source.host.clone();
        let cancel = Cancellation::default();
        let token = cancel.clone();
        let building = tokio::spawn(async move { sender.build(member, token).await });
        target
            .runtime
            .fault_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        // Header ACK only is1. Native copied-log waiter settlement still ACKs the
        // failed batch2, unlike failed replication flush in the preceding test.
        until(|| {
            source
                .lower
                .snapshot()
                .peers
                .iter()
                .any(|p| p.copy_applied >= 2 || p.copy_applied == -1)
        })
        .await;
        let generation = target
            .runtime
            .generation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        assert!(generation.copy_done.wait().await.is_err());
        assert!(!target.spy.has("apply-start"));
        assert!(!target.spy.point.events().iter().any(|e| matches!(
            e,
            super::provider::PointEvent::Callback {
                kind: super::provider::CallbackKind::EndCopy,
                ..
            }
        )));
        cancel.cancel();
        let _ = building.await.unwrap();
        source.host.remove(descriptor(&target)).await.unwrap();
        let lower = target.runtime.replicator();
        target
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                lower.abort();
                Ok(())
            })
            .await
            .unwrap();
        assert!(target.runtime.terminal.wait().await.is_err());
        target
            .runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        drop(source.host);
        drop(target.host);
    }
}
