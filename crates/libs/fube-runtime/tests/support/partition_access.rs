use super::*;
use fube_runtime::{
    HealthInformation, HealthReportSendOption, LoadMetric, MoveCost,
    ServicePartitionAccessStatus as Access, ServicePartitionInformation,
};
use std::{collections::VecDeque, sync::atomic::AtomicUsize};

type SecondReadProbe = Box<dyn FnOnce() -> Option<Error> + Send>;
pub(super) struct AccessControl {
    values: Mutex<VecDeque<Result<Access>>>,
    reads: AtomicUsize,
    first_read: Mutex<Option<SecondReadProbe>>,
    second_read: Mutex<Option<SecondReadProbe>>,
    final_release: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl AccessControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            values: Mutex::new(VecDeque::from([Ok(Access::Granted)])),
            reads: 0.into(),
            first_read: Mutex::new(None),
            second_read: Mutex::new(None),
            final_release: Mutex::new(None),
        })
    }
    fn set(&self, values: impl IntoIterator<Item = Result<Access>>) {
        *self.values.lock().unwrap() = values.into_iter().collect();
        self.reads.store(0, Ordering::SeqCst);
    }
    fn get(&self) -> Result<Access> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst);
        if read <= 1 {
            let probe = if read == 0 {
                self.first_read.lock().unwrap().take()
            } else {
                self.second_read.lock().unwrap().take()
            };
            if let Some(probe) = probe {
                if let Some(error) = probe() {
                    return Err(error);
                }
            }
        }
        let mut values = self.values.lock().unwrap();
        if values.len() > 1 {
            values.pop_front().unwrap()
        } else {
            values.front().unwrap().clone()
        }
    }
}
pub(super) struct AccessPartition {
    pub inner: Arc<dyn IStatefulServicePartition>,
    pub access: Arc<AccessControl>,
}
impl Drop for AccessPartition {
    fn drop(&mut self) {
        let probe = self.access.final_release.lock().unwrap().take();
        if let Some(probe) = probe {
            probe();
        }
    }
}
impl IStatefulServicePartition for AccessPartition {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation> {
        self.inner.get_partition_information()
    }
    fn get_read_status(&self) -> Result<Access> {
        self.inner.get_read_status()
    }
    fn get_write_status(&self) -> Result<Access> {
        self.access.get()
    }
    fn report_load(&self, metrics: &[LoadMetric]) -> Result<()> {
        self.inner.report_load(metrics)
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.inner.report_fault(fault)
    }
    fn report_move_cost(&self, cost: MoveCost) -> Result<()> {
        self.inner.report_move_cost(cost)
    }
    fn report_partition_health(&self, health: &HealthInformation) -> Result<()> {
        self.inner.report_partition_health(health)
    }
    fn report_replica_health(&self, health: &HealthInformation) -> Result<()> {
        self.inner.report_replica_health(health)
    }
    fn report_partition_health2(
        &self,
        health: &HealthInformation,
        options: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.inner.report_partition_health2(health, options)
    }
    fn report_replica_health2(
        &self,
        health: &HealthInformation,
        options: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.inner.report_replica_health2(health, options)
    }
}

async fn fixture() -> (Fixture, Arc<AccessControl>) {
    fixture_with_settings(Settings::default()).await
}

async fn fixture_with_settings(settings: Settings) -> (Fixture, Arc<AccessControl>) {
    let access = AccessControl::new();
    let supplied_access = access.clone();
    let provider = MemoryProvider::new();
    let supplied = provider.clone();
    let (created, received) = oneshot::channel();
    let host = rap::Host::start(
        uuid::Uuid::new_v4(),
        1,
        ReplicaRole::Primary,
        move |native| {
            Box::new(Factory {
                forwarding: false,
                access: Some(supplied_access),
                native,
                provider: supplied,
                settings,
                created: Arc::new(Mutex::new(Some(created))),
                opening_cancel: Arc::default(),
                sample_open_progress: false,
                retry_open_progress: false,
            })
        },
    )
    .await;
    let (engine, native) = received.await.unwrap();
    host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    until(|| engine.snapshot().ready).await;
    (
        Fixture {
            engine,
            provider,
            native,
            host,
        },
        access,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_primary_independent_access_denials_and_getter_errors_reject_without_lsn() {
    let (p, access) = fixture().await;
    for (status, kind, code) in [
        (Ok(Access::NotPrimary), ErrorKind::NotPrimary, 0x80071bc6u32),
        (Ok(Access::Invalid), ErrorKind::NotPrimary, 0x80071bc6),
        (
            Ok(Access::ReconfigurationPending),
            ErrorKind::ReconfigurationPending,
            0x80071bca,
        ),
        (
            Ok(Access::NoWriteQuorum),
            ErrorKind::NoWriteQuorum,
            0x80071bc5,
        ),
        (
            Err(Error::InvalidInput("failed getter")),
            ErrorKind::NotPrimary,
            0x80071bc6,
        ),
    ] {
        for recheck in [false, true] {
            let before = p.engine.snapshot().progress.assigned;
            access.set(if recheck {
                vec![Ok(Access::Granted), status.clone()]
            } else {
                vec![status.clone()]
            });
            let error = p
                .engine
                .control
                .get_state_replicator()
                .replicate(
                    OperationData::new(vec![vec![1]]).unwrap(),
                    Cancellation::default(),
                )
                .await
                .err()
                .expect("access denial");
            assert_eq!(error.kind(), kind);
            assert_eq!(error.native_result_code(), code as i32 as i64);
            assert!(!error.is_uncertain());
            assert_eq!(
                access.reads.load(Ordering::SeqCst),
                if recheck { 2 } else { 1 }
            );
            let after = p.engine.snapshot();
            assert!(
                after.ready,
                "denial must not be simulated by lower readiness"
            );
            assert_eq!(after.role, ReplicaRole::Primary);
            assert_eq!(after.progress.assigned, before);
            assert_eq!(after.owned_operations, 0);
        }
    }
    access.set([Ok(Access::Granted)]);
    p.write("first-allowed").await;
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_partition_revocation_is_rechecked_before_assignment() {
    let (p, access) = fixture().await;
    let (entered, received) = std_mpsc::sync_channel(1);
    let (release, blocked) = std_mpsc::sync_channel(1);
    *p.engine.shared.write_assignment_pause.lock().unwrap() = Some((entered, blocked));
    let state = p.engine.control.get_state_replicator();
    let write = tokio::spawn(async move {
        state
            .replicate(
                OperationData::new(vec![vec![1]]).unwrap(),
                Cancellation::default(),
            )
            .await
    });
    received
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert_eq!(access.reads.load(Ordering::SeqCst), 1);
    access.set([Ok(Access::NoWriteQuorum)]);
    release.send(()).unwrap();
    assert_eq!(
        write.await.unwrap().err().unwrap().kind(),
        ErrorKind::NoWriteQuorum
    );
    assert_eq!(p.engine.snapshot().progress.assigned, 0);
    assert!(p.engine.snapshot().ready);
    access.set([Ok(Access::Granted)]);
    p.write("after-queued-denial").await;
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_lower_continuation_survives_later_partition_access_revocation() {
    let (p, access) = fixture().await;
    let (network, _incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let mut peer = p.descriptor(ReplicaRole::ActiveSecondary);
    peer.replica_id = 2;
    peer.node_id = [2; 16];
    peer.incarnation = 12;
    peer.address = Endpoint {
        actor: Actor {
            partition: p.native.identity().partition_id,
            replica: 2,
            incarnation: uuid::Uuid::new_v4(),
        },
        address: network.address().to_owned(),
        transport_id: None,
    }
    .to_string();
    p.host
        .configure(config(EPOCH, vec![peer.clone()], 2))
        .await
        .unwrap();
    let data = Mutation {
        id: "accepted".into(),
        key: "key".into(),
        value: "value".into(),
    }
    .operation()
    .unwrap();
    let mut accepted = p
        .engine
        .control
        .get_state_replicator()
        .replicate(data.clone(), Cancellation::default())
        .await
        .unwrap();
    p.provider
        .apply_replication(accepted.sequence_number, &data)
        .unwrap();
    // Retained public facade clones, even in this owner's supported execution,
    // cannot replace the genuine quorum-two configuration or its pending vote.
    let raw = p.engine.control.diagnostics();
    let clone = raw.clone();
    let unissued = config(EPOCH, vec![], 1).sdk().unwrap();
    let denied = p
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || clone.update_current_replica_set_configuration(unissued))
        .await
        .unwrap_err();
    assert_eq!(denied.kind(), ErrorKind::Unsupported);
    let target = peer.sdk();
    assert_eq!(
        raw.build_replica(target, Cancellation::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        raw.remove_replica(peer.replica_id).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    let configured = p.engine.snapshot();
    assert_eq!(configured.peers.len(), 1);
    assert_eq!(configured.peers[0].replica, peer.replica_id);
    assert!(configured.peers[0].configured);
    assert_eq!(configured.progress.assigned, accepted.sequence_number);
    assert_eq!(configured.progress.committed, 0);
    // A genuinely issued configuration on another normally constructed owner
    // has no capability to change this retained owner's pending quorum.
    let other = Fixture::new(
        p.native.identity().partition_id,
        source(3),
        ReplicaRole::Primary,
        false,
    )
    .await;
    other
        .host
        .configure(config(EPOCH, vec![], 1))
        .await
        .unwrap();
    other.write("different-owner").await;
    assert_eq!(other.engine.snapshot().progress.committed, 1);
    assert_eq!(p.engine.snapshot().progress.committed, 0);
    access.set([Ok(Access::NotPrimary)]);
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(30),
        accepted.completion.wait(Cancellation::default())
    )
    .await
    .is_err());
    p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    assert_eq!(
        accepted
            .completion
            .wait(Cancellation::default())
            .await
            .unwrap(),
        1
    );
    assert_eq!(p.engine.snapshot().progress.committed, 1);
    let error = p
        .engine
        .control
        .get_state_replicator()
        .replicate(data, Cancellation::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::NotPrimary);
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
    network.shutdown_checked().unwrap();
}

struct AccessCause {
    probe: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl AccessCause {
    fn new(probe: impl FnOnce() + Send + 'static) -> Self {
        Self {
            probe: Mutex::new(Some(Box::new(probe))),
        }
    }
}
impl std::fmt::Debug for AccessCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owned partition cause")
    }
}
impl std::fmt::Display for AccessCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owned partition cause")
    }
}
impl std::error::Error for AccessCause {}
impl Drop for AccessCause {
    fn drop(&mut self) {
        self.probe.get_mut().unwrap().take().unwrap()();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_partition_read_and_typed_error_release_are_owned_reentry_fenced_not_admission() {
    use fube_data::extensions::ReplicatorDiagnostics;
    use fube_runtime::extensions::ExecutionRuntime;
    for error_drop in [false, true] {
        let (p, access) = fixture().await;
        let native = p.native.clone();
        let control = p.engine.control.clone();
        let id = control.diagnostics().execution_identity();
        let (entered, received) = oneshot::channel();
        let (release, blocked) = std_mpsc::sync_channel(1);
        *access.second_read.lock().unwrap() = Some(Box::new(move || {
            assert!(native.execution().is_current_entry());
            assert_eq!(std::thread::current().name(), Some("fube-provider-call"));
            assert!(ExecutionRuntime::reject_engine_reentry(id).is_err());
            let error = control
                .update_current_replica_set_configuration(config(EPOCH, vec![], 1).sdk().unwrap())
                .unwrap_err();
            assert!(error.to_string().contains("same-engine"));
            let probe = move || {
                assert!(native.execution().is_current_entry());
                assert!(ExecutionRuntime::reject_engine_reentry(id).is_err());
                entered.send(()).unwrap();
                blocked
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap();
            };
            if error_drop {
                // A typed user error is returned on the actual queued read.
                Some(Error::with_cause(
                    ErrorKind::Provider,
                    "getter failed",
                    AccessCause::new(probe),
                ))
            } else {
                probe();
                None
            }
        }));
        let state = p.engine.control.get_state_replicator();
        let write = tokio::spawn(async move {
            state
                .replicate(
                    OperationData::new(vec![vec![1]]).unwrap(),
                    Cancellation::default(),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), received)
            .await
            .unwrap()
            .unwrap();
        // The owner must process a genuine runtime command while arbitrary
        // getter/error destruction is physically blocked.
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            p.host.configure(config(EPOCH, vec![], 1)),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(p.engine.shared.admission.try_lock().is_ok());
        assert_eq!(p.engine.snapshot().progress.assigned, 0);
        release.send(()).unwrap();
        let result = write.await.unwrap();
        if error_drop {
            assert_eq!(result.err().unwrap().kind(), ErrorKind::NotPrimary);
            assert_eq!(p.engine.snapshot().progress.assigned, 0);
        } else {
            assert_eq!(result.unwrap().sequence_number, 1);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_waits_physical_partition_release_even_with_delayed_public_views() {
    use fube_runtime::extensions::ExecutionRuntime;
    let (p, access) = fixture().await;
    let state = p.engine.control.get_state_replicator();
    let diagnostics = p.engine.control.diagnostics();
    let native = p.native.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let observed = dropped.clone();
    let (entered, received) = oneshot::channel();
    let (release, blocked) = std_mpsc::sync_channel(1);
    *access.final_release.lock().unwrap() = Some(Box::new(move || {
        assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
        assert!(ExecutionRuntime::is_cleanup_entry());
        assert!(native.execution().is_current_entry());
        entered.send(()).unwrap();
        blocked
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    let control = p.engine.control.clone();
    let mut close = tokio::spawn(async move { control.close(Cancellation::default()).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    assert!(p.engine.shared.admission.try_lock().is_ok());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut close)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    close.await.unwrap().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(state
        .replicate(
            OperationData::new(vec![vec![1]]).unwrap(),
            Cancellation::default()
        )
        .await
        .is_err());
    drop((state, diagnostics));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_callback_identity_hop_rejects_original_lower_close_state_and_control() {
    use fube_core::hosting::CallOrigin;
    use fube_data::extensions::ReplicatorDiagnostics;
    use fube_runtime::extensions::ExecutionRuntime;
    let (p, access) = fixture().await;
    let control = p.engine.control.clone();
    let diagnostics = control.diagnostics();
    let execution = diagnostics.execution_runtime();
    let id = diagnostics.execution_identity();
    *access.first_read.lock().unwrap() = Some(Box::new(move || {
        let deadline = execution.current_deadline().unwrap();
        let origin = CallOrigin {
            engine: id + 1000,
            generation: 1,
            session: 0,
        };
        let owner = execution.clone();
        execution
            .reserve_engine_callback(origin, deadline)
            .unwrap()
            .run_future(async move {
                tokio::task::yield_now().await;
                let before = diagnostics.snapshot();
                assert!(ExecutionRuntime::check_entry(Some(id)).is_err());
                assert!(ExecutionRuntime::reject_engine_reentry(id).is_err());
                assert!(owner
                    .reserve_engine_callback(
                        CallOrigin {
                            engine: id,
                            ..origin
                        },
                        deadline
                    )
                    .is_err());
                let error = control.close(Cancellation::default()).await.unwrap_err();
                assert!(error.to_string().contains("same-engine"));
                let error = control
                    .get_state_replicator()
                    .replicate(
                        OperationData::new(vec![vec![2]]).unwrap(),
                        Cancellation::default(),
                    )
                    .await
                    .err()
                    .unwrap();
                assert!(error.to_string().contains("same-engine"));
                let error = control
                    .update_current_replica_set_configuration(
                        config(EPOCH, vec![], 1).sdk().unwrap(),
                    )
                    .unwrap_err();
                assert!(error.to_string().contains("same-engine"));
                let after = diagnostics.snapshot();
                assert!(!after.closed);
                assert_eq!(after.epoch, before.epoch);
                assert_eq!(after.peers.len(), before.peers.len());
                assert_eq!(after.progress.assigned, before.progress.assigned);
                Ok(())
            })
            .wait()
            .unwrap();
        None
    }));
    let before = p.engine.snapshot();
    let write = p
        .engine
        .control
        .get_state_replicator()
        .replicate(
            OperationData::new(vec![vec![1]]).unwrap(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(write.sequence_number, 1);
    assert!(!p.engine.shared.stopped.load(Ordering::Acquire));
    let after = p.engine.snapshot();
    assert_eq!(after.epoch, before.epoch);
    assert_eq!(after.peers.len(), before.peers.len());
    assert_eq!(after.progress.assigned, before.progress.assigned + 1);
    p.engine
        .control
        .close(Cancellation::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_stale_permit_cleanup_rejects_original_lower_before_close_state_control_mutation() {
    use fube_core::hosting::{DisposalTracker, ObjectQuota};
    use fube_data::extensions::ReplicatorDiagnostics;
    use fube_runtime::extensions::ExecutionRuntime;
    struct Probe {
        lower: Arc<fube_data::FabricReplicator>,
        control: Arc<RuntimeReplicator>,
        execution: ExecutionRuntime,
        engines: Vec<u64>,
        deadline: std::time::Instant,
        dropped: Option<oneshot::Sender<()>>,
    }
    fn immediate_error<T>(mut future: ServiceFuture<'_, T>) -> Error {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(Err(error)) => error,
            _ => panic!("cleanup reentry must reject on its first poll"),
        }
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(ExecutionRuntime::is_cleanup_entry());
            assert!(self.execution.is_current_entry());
            assert_eq!(self.execution.current_deadline().unwrap(), self.deadline);
            for engine in &self.engines {
                assert!(ExecutionRuntime::check_entry(Some(*engine)).is_err());
                assert!(ExecutionRuntime::reject_engine_reentry(*engine).is_err());
                assert!(self
                    .execution
                    .reserve_engine_callback(
                        CallOrigin {
                            engine: *engine,
                            generation: 1,
                            session: 0,
                        },
                        self.deadline,
                    )
                    .is_err());
            }
            let before = self.lower.snapshot();
            // These are the original public lower, not a stopped substitute.
            let error = immediate_error(self.lower.close(Cancellation::default()));
            assert!(error.to_string().contains("same-engine"));
            let error = immediate_error(fube_data::IStateReplicator::replicate(
                &*self.lower,
                OperationData::new(vec![vec![2]]).unwrap(),
                Cancellation::default(),
            ));
            assert!(error.to_string().contains("same-engine"));
            let error = self
                .control
                .update_current_replica_set_configuration(config(EPOCH, vec![], 1).sdk().unwrap())
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidExecutionContext);
            let after = self.lower.snapshot();
            assert!(!after.closed);
            assert_eq!(after.epoch, before.epoch);
            assert_eq!(
                serde_json::to_value(&after.peers).unwrap(),
                serde_json::to_value(&before.peers).unwrap()
            );
            assert_eq!(after.progress.assigned, before.progress.assigned);
            assert_eq!(after.progress.committed, before.progress.committed);
            self.dropped.take().unwrap().send(()).unwrap();
        }
    }
    for depth in [2, 3] {
        for constructed_outside in [false, true] {
            let (p, access) = fixture().await;
            let control = p.engine.control.clone();
            let lower = control.diagnostics();
            let execution = lower.execution_runtime();
            let id = lower.execution_identity();
            let origin = |engine| CallOrigin {
                engine,
                generation: 1,
                session: 0,
            };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            // X predates both the actual A getter and publication scheduling.
            let spare = execution
                .reserve_engine_callback(origin(id + 1000), deadline)
                .unwrap();
            let publication = id + 2000;
            let b = origin(id + 3000);
            let c = origin(id + 4000);
            let (dropped, received) = oneshot::channel();
            let mut probe = Some(Probe {
                lower,
                control,
                execution: execution.clone(),
                engines: [
                    vec![id, id + 1000, publication, b.engine],
                    if depth == 3 { vec![c.engine] } else { vec![] },
                ]
                .concat(),
                deadline,
                dropped: Some(dropped),
            });
            let object = if constructed_outside {
                Some(
                    spare
                        .object_reservation(
                            &ObjectQuota::new(1).unwrap(),
                            &DisposalTracker::default(),
                        )
                        .unwrap()
                        .construct(probe.take().unwrap()),
                )
            } else {
                None
            };
            *access.first_read.lock().unwrap() = Some(Box::new(move || {
                let callback_deadline = execution.current_deadline().unwrap();
                let owner = execution.clone();
                let cleanup = async move {
                    tokio::task::yield_now().await;
                    let object = match object {
                        Some(object) => object,
                        None => spare
                            .object_reservation(&ObjectQuota::new(1)?, &DisposalTracker::default())?
                            .construct(probe.take().unwrap()),
                    };
                    let receipt = object.receipt();
                    drop(object);
                    receipt.wait().await?;
                    received.await.map_err(|_| Error::Closed)?;
                    Ok(())
                };
                ExecutionRuntime::engine_publication(publication, || {
                    Ok(execution
                        .reserve_engine_callback(b, callback_deadline)?
                        .run_future(async move {
                            if depth == 3 {
                                owner
                                    .reserve_engine_callback(c, callback_deadline)?
                                    .run_future(cleanup)
                                    .await
                            } else {
                                cleanup.await
                            }
                        }))
                })
                .unwrap()
                .wait()
                .unwrap();
                None
            }));
            let before = p.engine.snapshot();
            let write = p
                .engine
                .control
                .get_state_replicator()
                .replicate(
                    OperationData::new(vec![vec![1]]).unwrap(),
                    Cancellation::default(),
                )
                .await
                .unwrap();
            assert_eq!(write.sequence_number, 1);
            assert!(!p.engine.shared.stopped.load(Ordering::Acquire));
            let after = p.engine.snapshot();
            assert_eq!(after.epoch, before.epoch);
            assert_eq!(after.peers.len(), before.peers.len());
            assert_eq!(after.progress.assigned, before.progress.assigned + 1);
            p.engine
                .control
                .close(Cancellation::default())
                .await
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_partition_read_close_joins_live_dropped_and_completed_unpolled_observers() {
    use fube_runtime::extensions::ExecutionRuntime;
    struct Wake(AtomicBool);
    impl std::task::Wake for Wake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }
    for scenario in ["live", "drop", "completed-unpolled"] {
        let (p, access) = fixture().await;
        let native = p.native.clone();
        let callback_deadline = Arc::new(Mutex::new(None));
        let deadline = callback_deadline.clone();
        let (entered, received) = oneshot::channel();
        let (release_read, blocked_read) = std_mpsc::sync_channel(1);
        let unpolled = scenario == "completed-unpolled";
        *access.first_read.lock().unwrap() = Some(Box::new(move || {
            assert!(native.execution().is_current_entry());
            let original = native.execution().current_deadline().unwrap();
            *deadline.lock().unwrap() = Some(original);
            entered.send(()).unwrap();
            blocked_read
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            assert_eq!(native.execution().current_deadline().unwrap(), original);
            if unpolled {
                Some(Error::InvalidInput("first read denied"))
            } else {
                None
            }
        }));
        let drops = Arc::new(AtomicUsize::new(0));
        let observed = drops.clone();
        let native = p.native.clone();
        let (drop_entered, drop_received) = oneshot::channel();
        let (release_drop, blocked_drop) = std_mpsc::sync_channel(1);
        *access.final_release.lock().unwrap() = Some(Box::new(move || {
            assert!(native.execution().is_current_entry());
            assert!(ExecutionRuntime::is_cleanup_entry());
            assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
            assert!(
                native.execution().current_deadline().unwrap()
                    <= callback_deadline.lock().unwrap().unwrap()
            );
            drop_entered.send(()).unwrap();
            blocked_drop
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        let state = p.engine.control.get_state_replicator();
        let mut write = state.replicate(
            OperationData::new(vec![vec![1]]).unwrap(),
            Cancellation::default(),
        );
        let wake = Arc::new(Wake(AtomicBool::new(false)));
        let waker = std::task::Waker::from(wake.clone());
        assert!(write
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending());
        received.await.unwrap();
        // Genuine Stop/configuration ingress remains independent of the getter.
        p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
        assert!(p.engine.shared.admission.try_lock().is_ok());
        if unpolled {
            release_read.send(()).unwrap();
            until(|| wake.0.load(Ordering::Acquire)).await;
        }
        let mut write = Some(write);
        if scenario == "drop" {
            drop(write.take());
        }
        let control = p.engine.control.clone();
        let mut close = tokio::spawn(async move { control.close(Cancellation::default()).await });
        until(|| p.engine.shared.stopped.load(Ordering::Acquire)).await;
        assert_eq!(p.engine.snapshot().progress.assigned, 0);
        if !unpolled {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut close)
                    .await
                    .is_err()
            );
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            release_read.send(()).unwrap();
        }
        drop_received.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut close)
                .await
                .is_err()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        release_drop.send(()).unwrap();
        close.await.unwrap().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(p.engine.snapshot().progress.assigned, 0);
        if let Some(observer) = write.take() {
            assert!(observer.await.is_err());
        }
        drop(write);
        drop(state);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn blocked_queued_partition_getter_or_cause_keeps_native_fence_including_observer_drop() {
    const CHILD: &str = "FUBE_QUEUED_PARTITION_FENCE_CHILD";
    if let Ok(scenario) = std::env::var(CHILD) {
        let application = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        application.block_on(async {
            let (p, access) = fixture_with_settings(Settings {
                callback_timeout: std::time::Duration::from_millis(400),
                ..Settings::default()
            })
            .await;
            let (entered, received) = oneshot::channel();
            let error_drop = scenario.contains("cause");
            let initial = scenario.starts_with("first-");
            let probe: SecondReadProbe = Box::new(move || {
                let probe = move || {
                    if !fube_runtime::extensions::ExecutionRuntime::in_owned_entry() {
                        std::process::exit(84);
                    }
                    entered.send(()).unwrap();
                    loop {
                        std::thread::park_timeout(std::time::Duration::from_secs(1));
                    }
                };
                if error_drop {
                    Some(Error::with_cause(
                        ErrorKind::Provider,
                        "blocked cause",
                        AccessCause::new(probe),
                    ))
                } else {
                    probe();
                    unreachable!()
                }
            });
            if initial {
                *access.first_read.lock().unwrap() = Some(probe);
            } else {
                *access.second_read.lock().unwrap() = Some(probe);
            }
            let state = p.engine.control.get_state_replicator();
            let write = tokio::spawn(async move {
                state
                    .replicate(
                        OperationData::new(vec![vec![1]]).unwrap(),
                        Cancellation::default(),
                    )
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(2), received)
                .await
                .unwrap()
                .unwrap();
            if scenario.ends_with("drop") {
                write.abort();
                let _ = write.await;
            }
            assert_eq!(p.engine.snapshot().progress.assigned, 0);
            if initial {
                let control = p.engine.control.clone();
                let close =
                    tokio::spawn(async move { control.close(Cancellation::default()).await });
                until(|| p.engine.shared.stopped.load(Ordering::Acquire)).await;
                assert!(!close.is_finished());
                assert_eq!(p.engine.snapshot().progress.assigned, 0);
            }
            // No teardown while a native terminal fence owns blocked code.
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        unreachable!();
    }
    for scenario in [
        "getter",
        "getter-drop",
        "cause",
        "cause-drop",
        "first-getter",
        "first-getter-drop",
        "first-cause",
        "first-cause-drop",
    ] {
        let started = std::time::Instant::now();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "partition_access::blocked_queued_partition_getter_or_cause_keeps_native_fence_including_observer_drop", "--nocapture", "--test-threads=1"])
            .env(CHILD, scenario).spawn().unwrap();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(70), "{scenario}");
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(3),
                    "{scenario}"
                );
                break;
            }
            if started.elapsed() >= std::time::Duration::from_secs(4) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("{scenario}: blocked partition callback did not hit original native fence");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
