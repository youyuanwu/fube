use super::*;
use fube_data::qualification as rap_model;
fn host(route: &Arc<Route>) -> Arc<rap_model::Host> {
    route.native_host.as_ref().unwrap().clone()
}
async fn set_current(
    route: &Arc<Route>,
    configuration: ReplicaSetConfiguration,
) -> RuntimeResult<()> {
    host(route).configure(configuration).await
}
async fn rap_build(
    route: &Arc<Route>,
    peer: ReplicaInformation,
    cancellation: Cancellation,
) -> RuntimeResult<()> {
    host(route).build(peer, cancellation).await
}
async fn rap_role(route: &Arc<Route>, role: ReplicaRole) -> RuntimeResult<()> {
    host(route).change_role(role).await?;
    if matches!(
        role,
        ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
    ) {
        route.stream_refresh.send_replace(());
    }
    Ok(())
}
async fn shutdown_route(route: &Arc<Route>) {
    if route.engine.snapshot().closed {
        // Closed publication is not proof that physical disposal has finished.
        route.engine.close(Cancellation::default()).await.unwrap();
        route.shutdown();
        return;
    }
    let execution = route.engine.execution_runtime();
    let owner = route.clone();
    execution
        .reserve(
            fube_runtime::extensions::CallLane::Native,
            fube_runtime::extensions::CallOrigin::default(),
            Duration::from_secs(30),
            false,
        )
        .unwrap()
        .run_owned(move || {
            owner.shutdown();
            Ok(())
        })
        .await
        .unwrap();
    route.engine.close(Cancellation::default()).await.unwrap();
}
use fube_data::extensions::ReplayCheckpointProvider;
use fube_data::{
    IOperationDataStream, IStateProvider, OperationData, OperationMetadata, OperationType,
};
use std::sync::atomic::AtomicUsize;

const EPOCH: Epoch = Epoch {
    data_loss_number: 1,
    configuration_number: 1 << 32,
};

struct NativeModel {
    alive: AtomicBool,
    status: Mutex<ReplicaStatus>,
}
impl NativeView for NativeModel {
    fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        Ok(self.status.lock().unwrap().clone())
    }
}
fn status(role: ReplicaRole) -> ReplicaStatus {
    ReplicaStatus {
        lifecycle: ReplicaLifecycle::Open,
        service_role: role,
        replicator_role: role,
        replicator_open: true,
        current_role: role,
        current_epoch: EPOCH,
        previous_epoch: Epoch::INVALID,
        configuration_stage: ConfigurationStage::Current,
        catch_up: CatchUpResult::NotStarted,
        minimum_replica_set_size: 1,
        configuration: vec![],
        service_address: String::new(),
        replication_address: String::new(),
        progress: ReplicationProgress::EMPTY,
    }
}
fn mutation(id: impl ToString) -> Mutation {
    let id = id.to_string();
    Mutation {
        key: format!("key-{id}"),
        value: format!("value-{id}"),
        id,
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("fixture condition");
}
async fn route(partition: Uuid, replica: i64, role: ReplicaRole) -> (Arc<Route>, Arc<NativeModel>) {
    let native = Arc::new(NativeModel {
        alive: AtomicBool::new(true),
        status: Mutex::new(status(role)),
    });
    (
        route_on(native.clone(), partition, replica, role).await,
        native,
    )
}
async fn route_on(
    native: Arc<dyn NativeView>,
    partition: Uuid,
    replica: i64,
    role: ReplicaRole,
) -> Arc<Route> {
    let provider = MemoryProvider::new();
    route_using(native, partition, replica, role, provider.clone(), provider).await
}
async fn route_using(
    native: Arc<dyn NativeView>,
    partition: Uuid,
    replica: i64,
    role: ReplicaRole,
    provider: Arc<MemoryProvider>,
    engine_provider: Arc<dyn IStateProvider>,
) -> Arc<Route> {
    let copy_delay = Arc::new(AtomicU64::new(0));
    let (send, receive) = oneshot::channel();
    let delay = copy_delay.clone();
    let replay = provider.clone();
    let model = rap_model::Host::start(partition, replica, role, move |native| {
        Box::new(ModelFactory {
            native,
            provider: Arc::new(CopyProvider {
                inner: engine_provider,
                delay,
            }),
            replay,
            result: Arc::new(Mutex::new(Some(send))),
        })
    })
    .await;
    let (engine, binding) = receive.await.unwrap();
    let identity = binding.identity().clone();
    let mut route = Route::start(
        native.clone(),
        identity,
        engine.clone(),
        provider,
        format!("http://127.0.0.1/data/fixture-{replica}/service"),
        1,
        copy_delay,
    )
    .unwrap();
    Arc::get_mut(&mut route).unwrap().native_host = Some(model);
    if role == ReplicaRole::Primary {
        set_current(&route, configuration(vec![], 1)).await.unwrap();
        until(|| engine.snapshot().ready).await;
    }
    route
}
type ModelResult =
    Arc<Mutex<Option<oneshot::Sender<(Arc<FabricReplicator>, NativeRuntimeIntegration)>>>>;
struct ModelFactory {
    native: NativeIntegrationProvider,
    provider: Arc<dyn IStateProvider>,
    replay: Arc<MemoryProvider>,
    result: ModelResult,
}
struct ModelReplica {
    native: NativeRuntimeIntegration,
    provider: Arc<dyn IStateProvider>,
    replay: Arc<MemoryProvider>,
    result: ModelResult,
}
impl IStatefulServiceFactory for ModelFactory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: Uuid,
        replica: i64,
    ) -> RuntimeResult<Box<dyn IStatefulServiceReplica>> {
        Ok(Box::new(ModelReplica {
            native: self.native.capture(partition, replica)?,
            provider: self.provider.clone(),
            replay: self.replay.clone(),
            result: self.result.clone(),
        }))
    }
}
impl IStatefulServiceReplica for ModelReplica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ServiceFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
            let engine = FabricReplicator::create(
                partition,
                self.native.clone(),
                self.provider.clone(),
                Some(self.replay.clone()),
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: Uuid::new_v4(),
                },
                Settings::default(),
            )?;
            if let Some(send) = self.result.lock().unwrap().take() {
                let _ = send.send((engine.clone(), self.native.clone()));
            }
            Ok(Box::new((*engine).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            Ok(if role == ReplicaRole::Primary {
                "model://data-service".into()
            } else {
                String::new()
            })
        })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}

struct HeldCopy {
    provider: Arc<MemoryProvider>,
    eos: Arc<Semaphore>,
}
struct HeldStream {
    stream: Arc<dyn IOperationDataStream>,
    eos: Arc<Semaphore>,
}
impl IOperationDataStream for HeldStream {
    fn get_next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            let item = self.stream.get_next(cancellation.clone()).await?;
            if item.is_none() {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(RuntimeError::Closed),
                    permit = self.eos.acquire() => permit.map_err(|_| RuntimeError::Closed)?.forget(),
                }
            }
            Ok(item)
        })
    }
}
impl IStateProvider for HeldCopy {
    fn get_last_committed_sequence_number(&self) -> RuntimeResult<i64> {
        self.provider.get_last_committed_sequence_number()
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.provider.update_epoch(epoch, previous, cancellation)
    }
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        self.provider.on_data_loss(cancellation)
    }
    fn get_copy_context(&self) -> RuntimeResult<Arc<dyn IOperationDataStream>> {
        self.provider.get_copy_context()
    }
    fn get_copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> RuntimeResult<Arc<dyn IOperationDataStream>> {
        Ok(Arc::new(HeldStream {
            stream: self.provider.get_copy_state(up_to, context)?,
            eos: self.eos.clone(),
        }))
    }
}
fn configuration(replicas: Vec<ReplicaInformation>, quorum: usize) -> ReplicaSetConfiguration {
    ReplicaSetConfiguration {
        epoch: EPOCH,
        replicas,
        write_quorum: quorum,
        local_member: true,
    }
}
fn descriptor(route: &Route) -> ReplicaInformation {
    ReplicaInformation {
        node_id: route.identity.node_id,
        node_instance: route.identity.node_instance,
        replica_id: route.identity.replica_id,
        incarnation: route.identity.incarnation,
        role: ReplicaRole::IdleSecondary,
        previous_role: ReplicaRole::None,
        up: true,
        ready: false,
        address: route.engine.endpoint().to_string(),
        progress: ReplicationProgress::EMPTY,
        must_catch_up: false,
    }
}

#[test]
fn profile_selection_is_exact_and_default_stays_empty() {
    assert!(selected(b"fube-data-v1"));
    for data in [
        b"".as_slice(),
        b"empty",
        b"FUBE-DATA-V1",
        b"fube-data-v1\0",
        b"fube-data-v1\n",
    ] {
        assert!(!selected(data));
    }
}

#[test]
fn query_decodes_strict_utf8_and_rejects_malformed_unknown_and_bounded_requests() {
    let Request::Write(value) = parse("write", "id=a%2Bb&key=%E2%98%83&value=x+y%26z%3D").unwrap()
    else {
        panic!("write");
    };
    assert_eq!(
        value,
        Mutation {
            id: "a+b".into(),
            key: "☃".into(),
            value: "x y&z=".into()
        }
    );
    for (operation, query) in [
        ("write", "id=a&key=k&value=%"),
        ("write", "id=a&key=k&value=%0g"),
        ("write", "id=a&key=k&value=%ff"),
        ("write", "id=a&key=k&value=%c0%af"),
        ("write", "id=&key=k&value=x"),
        ("write", "id=a&key=k&value=x&extra=y"),
        ("write", "id=a&%69d=b&key=k"),
        ("write", "id=a&key=k&other=x"),
        ("write", "id=a&key=k"),
        ("write", "id=a&key=k&value=x&"),
        ("service", "op=write"),
        ("diagnostics", "unexpected=1"),
        ("other", ""),
        ("test", "ack_delay_ms=2001"),
        ("test", "ack_delay_ms=-1"),
        ("test", "ack_delay_ms=+1"),
        ("test", "ack_delay_ms=18446744073709551616"),
        ("test", "ack_delay_ms=1&x=2"),
        ("test", "copy_delay_ms=101"),
        ("test", "copy_delay_ms=-1"),
        ("test", "copy_delay_ms=1&ack_delay_ms=1"),
        ("test", "copy_ack_hold_ms=6001"),
        ("test", "copy_ack_hold_ms=-1"),
        ("test", "copy_ack_hold_ms=+1"),
        ("test", "copy_ack_hold_ms="),
        ("test", "copy_ack_hold_ms=1&ack_delay_ms=1"),
        ("test", "wire_probe=arbitrary"),
        ("test", "wire_probe=stale_epoch&ack_delay_ms=0"),
        ("test", "data_loss_boundary=-1"),
    ] {
        assert!(parse(operation, query).is_err(), "{operation}?{query}");
    }
    assert!(matches!(
        parse("test", "ack_delay_ms=0").unwrap(),
        Request::Test(0)
    ));
    assert!(matches!(
        parse("test", "ack_delay_ms=2000").unwrap(),
        Request::Test(2000)
    ));
    assert!(matches!(
        parse("test", "copy_delay_ms=100").unwrap(),
        Request::CopyTest(100)
    ));
    for milliseconds in [0, 6000] {
        assert!(
            matches!(parse("test", &format!("copy_ack_hold_ms={milliseconds}")).unwrap(),
            Request::CopyAckHold(value) if value == milliseconds)
        );
    }
    assert!(matches!(
        parse("test", "wire_probe=stale_epoch").unwrap(),
        Request::WireProbe(_)
    ));
    assert!(matches!(
        parse("test", "data_loss_boundary=5").unwrap(),
        Request::DataLoss(5)
    ));
    assert!(parse("write", &format!("id={}&key=k&value=x", "x".repeat(129))).is_err());
    assert!(parse("write", &format!("id=a&key={}&value=x", "x".repeat(1025))).is_err());
    assert!(parse("write", &format!("id=a&key=k&value={}", "x".repeat(4096))).is_err());
    assert!(matches!(parse("service", "").unwrap(), Request::Read));
    assert!(matches!(
        parse("diagnostics", "").unwrap(),
        Request::Diagnostics
    ));
}

#[tokio::test]
async fn copy_pacing_is_cancelable_and_does_not_consume_or_change_snapshot_data() {
    let provider = MemoryProvider::new();
    let original = mutation("checkpoint").operation_for_epoch(EPOCH).unwrap();
    provider.apply_replication(1, &original).unwrap();
    let delay = Arc::new(AtomicU64::new(100));
    let wrapper = CopyProvider {
        inner: provider.clone(),
        delay: delay.clone(),
    };
    // Replay is a separately supplied authentic capability, never discovered
    // through the standard-only pacing wrapper.
    let checkpoint = provider
        .replication_checkpoint(1, Cancellation::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.epoch, EPOCH);
    assert_eq!(checkpoint.data, original);
    let stream = wrapper.get_copy_state(0, None).unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        stream.get_next(Cancellation::default())
    )
    .await
    .is_err());
    let cancellation = Cancellation::default();
    cancellation.cancel();
    assert!(stream.get_next(cancellation).await.is_err());
    delay.store(0, Ordering::Release);
    let target = MemoryProvider::new();
    while let Some(data) = stream.get_next(Cancellation::default()).await.unwrap() {
        target.apply_copy(&data).unwrap();
    }
    assert!(!target.snapshot().copy_in_progress);
    assert_eq!(target.snapshot().last, 0);
}

#[tokio::test]
async fn authority_requires_native_access_all_roles_epoch_lifetime_and_engine_readiness() {
    let (route, native) = route(Uuid::new_v4(), 1, ReplicaRole::Primary).await;
    assert!(route.authority(true).is_ok());
    let ready = route.engine.snapshot();
    let mut engine = ready.clone();
    engine.ready = false;
    assert!(!authoritative(&status(ReplicaRole::Primary), &engine, true));
    engine = ready.clone();
    engine.role = ReplicaRole::ActiveSecondary;
    assert!(!authoritative(&status(ReplicaRole::Primary), &engine, true));
    engine = ready.clone();
    engine.epoch = Some(Epoch::INVALID);
    assert!(!authoritative(
        &status(ReplicaRole::Primary),
        &engine,
        false
    ));
    engine = ready.clone();
    engine.fault = Some("failed".into());
    assert!(!authoritative(
        &status(ReplicaRole::Primary),
        &engine,
        false
    ));
    for change in [
        |s: &mut ReplicaStatus| s.current_role = ReplicaRole::ActiveSecondary,
        |s: &mut ReplicaStatus| s.service_role = ReplicaRole::ActiveSecondary,
        |s: &mut ReplicaStatus| s.replicator_role = ReplicaRole::ActiveSecondary,
        |s: &mut ReplicaStatus| s.lifecycle = ReplicaLifecycle::Closing,
        |s: &mut ReplicaStatus| s.replicator_open = false,
        |s: &mut ReplicaStatus| s.minimum_replica_set_size = 3,
    ] {
        let mut denied = status(ReplicaRole::Primary);
        change(&mut denied);
        *native.status.lock().unwrap() = denied;
        let result = route.write(mutation("denied")).await;
        assert_eq!(result.outcome, Outcome::Rejected);
        assert_eq!(result.sequence, None);
        assert_eq!(route.engine.snapshot().progress.assigned, 0);
    }
    *native.status.lock().unwrap() = status(ReplicaRole::Primary);
    native.alive.store(false, Ordering::Release);
    assert!(route.lifetime().is_err());
    assert_eq!(
        route.write(mutation("closed")).await.outcome,
        Outcome::Rejected
    );
    native.alive.store(true, Ordering::Release);
    shutdown_route(&route).await;
    assert!(route.lifetime().is_err());
    assert_eq!(
        route.write(mutation("retired")).await.outcome,
        Outcome::Rejected
    );
}

#[tokio::test]
async fn concurrent_writes_apply_before_waiting_and_deduplicate_before_assigning() {
    let (route, _) = route(Uuid::new_v4(), 1, ReplicaRole::Primary).await;
    let mut writes = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let route = route.clone();
        writes.spawn(async move { route.write(mutation("same")).await });
    }
    let mut originals = 0;
    while let Some(result) = writes.join_next().await {
        let result = result.unwrap();
        assert_ne!(result.outcome, Outcome::Rejected);
        assert_eq!(result.sequence, Some(1));
        if !result.duplicate {
            assert_eq!(result.outcome, Outcome::Committed);
        }
        originals += usize::from(!result.duplicate);
    }
    assert_eq!(originals, 1);
    assert_eq!(route.engine.snapshot().progress.assigned, 1);
    until(|| route.engine.snapshot().progress.committed == 1).await;
    assert_eq!(
        route.write(mutation("same")).await.outcome,
        Outcome::Committed
    );
    let mut conflict = mutation("same");
    conflict.value = "conflict".into();
    let result = route.write(conflict).await;
    assert_eq!(result.outcome, Outcome::Rejected);
    assert_eq!(result.sequence, None);
    assert!(result.json().get("sequence").is_none());
    assert_eq!(route.engine.snapshot().progress.assigned, 1);
    assert_eq!(route.provider.snapshot().history_count, 1);
    let checkpoint = route
        .provider
        .replication_checkpoint(1, Cancellation::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.epoch, EPOCH.primary());
    assert_eq!(
        checkpoint.data,
        mutation("same")
            .operation_for_epoch(EPOCH.primary())
            .unwrap()
    );
    shutdown_route(&route).await;
}

#[tokio::test]
async fn native_primary_epoch_change_between_authority_reads_rejects_before_admission() {
    struct ChangedEpoch(AtomicUsize);
    impl NativeView for ChangedEpoch {
        fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
            let mut view = status(ReplicaRole::Primary);
            if self.0.fetch_add(1, Ordering::AcqRel) > 0 {
                view.current_epoch.configuration_number += 1 << 32;
            }
            Ok(view)
        }
    }
    let (route, _) = route(Uuid::new_v4(), 1, ReplicaRole::Primary).await;
    let Admission::Result(result) = admit(
        &ChangedEpoch(AtomicUsize::new(0)),
        &route.engine,
        &route.provider,
        &route.stopped,
        &mutation("changed-epoch"),
    )
    .await
    else {
        panic!("changed native epoch must not be admitted");
    };
    assert_eq!(result.outcome, Outcome::Rejected);
    assert_eq!(route.engine.snapshot().progress.assigned, 0);
    assert_eq!(route.provider.snapshot().last, 0);
    shutdown_route(&route).await;
}

#[tokio::test]
async fn authority_is_rechecked_before_engine_admission_and_before_local_apply() {
    struct Revoked {
        calls: AtomicUsize,
        at: usize,
    }
    impl NativeView for Revoked {
        fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
            if self.calls.fetch_add(1, Ordering::AcqRel) >= self.at {
                Err(RuntimeError::Closed)
            } else {
                Ok(status(ReplicaRole::Primary))
            }
        }
    }
    for at in 0..=2 {
        let (route, _) = route(Uuid::new_v4(), 1, ReplicaRole::Primary).await;
        let native = Revoked {
            calls: AtomicUsize::new(0),
            at,
        };
        let Admission::Result(result) = admit(
            &native,
            &route.engine,
            &route.provider,
            &route.stopped,
            &mutation("revoked"),
        )
        .await
        else {
            panic!("revoked native authority must not publish success");
        };
        assert_eq!(route.provider.snapshot().last, 0);
        if at == 2 {
            assert_eq!(result.outcome, Outcome::Ambiguous);
            assert_eq!(result.sequence, Some(1));
            assert!(route.engine.snapshot().closed);
        } else {
            assert_eq!(result.outcome, Outcome::Rejected);
            assert_eq!(result.sequence, None);
            assert_eq!(route.engine.snapshot().progress.assigned, 0);
        }
        shutdown_route(&route).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnected_http_waiter_cannot_drop_admission_before_local_apply() {
    struct Paused {
        calls: AtomicUsize,
        reached: AtomicBool,
        released: (Mutex<bool>, std::sync::Condvar),
    }
    impl Paused {
        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }
    impl NativeView for Paused {
        fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 3 {
                self.reached.store(true, Ordering::Release);
                let mut released = self.released.0.lock().unwrap();
                while !*released {
                    released = self.released.1.wait(released).unwrap();
                }
            }
            Ok(status(ReplicaRole::Primary))
        }
    }
    struct Release(Arc<Paused>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let native = Arc::new(Paused {
        calls: AtomicUsize::new(0),
        reached: AtomicBool::new(false),
        released: (Mutex::new(false), std::sync::Condvar::new()),
    });
    let route = route_on(native.clone(), Uuid::new_v4(), 1, ReplicaRole::Primary).await;
    let _release = Release(native.clone());
    let mut write = Box::pin(route.write(mutation("disconnected")));
    assert!(tokio::time::timeout(Duration::from_millis(40), &mut write)
        .await
        .is_err());
    until(|| native.reached.load(Ordering::Acquire)).await;
    assert_eq!(route.engine.snapshot().progress.assigned, 1);
    assert_eq!(route.provider.snapshot().last, 0);
    drop(write);
    native.release();
    until(|| route.provider.snapshot().last == 1).await;
    until(|| route.engine.snapshot().progress.committed == 1).await;
    let duplicate = route.write(mutation("disconnected")).await;
    assert_eq!(duplicate.outcome, Outcome::Committed);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.sequence, Some(1));
    assert_eq!(route.engine.snapshot().progress.assigned, 1);
    shutdown_route(&route).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_waits_for_accepted_primary_local_apply_at_the_exact_horizon() {
    struct Paused {
        calls: AtomicUsize,
        reached: AtomicBool,
        released: (Mutex<bool>, std::sync::Condvar),
    }
    impl Paused {
        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }
    impl NativeView for Paused {
        fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 3 {
                self.reached.store(true, Ordering::Release);
                let mut released = self.released.0.lock().unwrap();
                while !*released {
                    released = self.released.1.wait(released).unwrap();
                }
            }
            Ok(status(ReplicaRole::Primary))
        }
    }
    struct Release(Arc<Paused>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let partition = Uuid::new_v4();
    let native = Arc::new(Paused {
        calls: AtomicUsize::new(0),
        reached: AtomicBool::new(false),
        released: (Mutex::new(false), std::sync::Condvar::new()),
    });
    let primary = route_on(native.clone(), partition, 1, ReplicaRole::Primary).await;
    let (secondary, _) = route(partition, 2, ReplicaRole::IdleSecondary).await;
    let _release = Release(native.clone());
    let mut write = Box::pin(primary.write(mutation("copy-race")));
    assert!(tokio::time::timeout(Duration::from_millis(40), &mut write)
        .await
        .is_err());
    until(|| native.reached.load(Ordering::Acquire)).await;
    assert_eq!(primary.engine.snapshot().progress.committed, 1);
    assert_eq!(primary.provider.snapshot().last, 0);
    let mut build = Box::pin(rap_build(
        &primary,
        descriptor(&secondary),
        Cancellation::default(),
    ));
    assert!(tokio::time::timeout(Duration::from_millis(40), &mut build)
        .await
        .is_err());
    assert!(primary.engine.snapshot().fault.is_none());
    native.release();
    assert_eq!(write.await.outcome, Outcome::Committed);
    tokio::time::timeout(Duration::from_secs(3), &mut build)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        primary.provider.snapshot().values,
        secondary.provider.snapshot().values
    );
    assert_eq!(
        primary.provider.snapshot().results,
        secondary.provider.snapshot().results
    );
    assert!(primary.engine.snapshot().fault.is_none());
    drop(build);
    shutdown_route(&primary).await;
    shutdown_route(&secondary).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_copy_pumps_copy_ack_hold_after_receive_eos_then_replication_ack_and_copied_retry() {
    let partition = Uuid::new_v4();
    let provider = MemoryProvider::new();
    let eos = Arc::new(Semaphore::new(1));
    let native = Arc::new(NativeModel {
        alive: AtomicBool::new(true),
        status: Mutex::new(status(ReplicaRole::Primary)),
    });
    let primary = route_using(
        native,
        partition,
        1,
        ReplicaRole::Primary,
        provider.clone(),
        Arc::new(HeldCopy {
            provider,
            eos: eos.clone(),
        }),
    )
    .await;
    for n in 1..=4 {
        assert_eq!(primary.write(mutation(n)).await.outcome, Outcome::Committed);
    }
    let (secondary, secondary_native) = route(partition, 2, ReplicaRole::IdleSecondary).await;
    secondary.copy_ack_hold.set(6000).unwrap();
    let build = rap_build(&primary, descriptor(&secondary), Cancellation::default());
    tokio::pin!(build);
    tokio::select! {
        result = &mut build => panic!("copy completed before release: {result:?}"),
        _ = until(|| secondary.copy_ack_hold.snapshot()["held"] == true
            && primary.engine.snapshot().peers.iter().any(|peer| peer.replica == 2 && peer.copy_received == -1)) => {},
    }
    assert!(!secondary.engine.snapshot().copy_done);
    assert_eq!(secondary.provider.snapshot().last, 4);
    assert!(!secondary.provider.snapshot().copy_in_progress);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), &mut build)
            .await
            .is_err(),
        "actual EOS receipt and CopyEnd application cannot complete build without its ACK"
    );
    secondary.copy_ack_hold.set(0).unwrap();
    build.await.unwrap();
    assert_eq!(secondary.provider.snapshot().last, 4);
    assert_eq!(
        secondary.provider.snapshot().results,
        primary.provider.snapshot().results
    );
    rap_role(&secondary, ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    let mut peer = descriptor(&secondary);
    peer.role = ReplicaRole::ActiveSecondary;
    peer.ready = true;
    set_current(&primary, configuration(vec![peer], 2))
        .await
        .unwrap();
    until(|| primary.engine.snapshot().ready).await;
    secondary.delay.store(180, Ordering::Release);
    let write = primary.write(mutation(5));
    tokio::pin!(write);
    assert!(tokio::time::timeout(Duration::from_millis(80), &mut write)
        .await
        .is_err());
    assert_eq!(
        primary.provider.snapshot().last,
        5,
        "local apply precedes waiting for quorum"
    );
    until(|| secondary.provider.snapshot().last == 5).await;
    assert!(
        primary.engine.snapshot().progress.committed < 5,
        "receipt/apply without ACK cannot commit"
    );
    let result = write.await;
    assert_eq!(result.outcome, Outcome::Committed);
    assert_eq!(
        secondary.provider.snapshot().values,
        primary.provider.snapshot().values
    );
    assert_eq!(
        secondary.provider.snapshot().results,
        primary.provider.snapshot().results
    );
    secondary.delay.store(0, Ordering::Release);
    let (replacement, _) = route(partition, 3, ReplicaRole::IdleSecondary).await;
    let cancellation = Cancellation::default();
    let build = async {
        rap_build(&primary, descriptor(&replacement), cancellation.clone()).await?;
        rap_role(&replacement, ReplicaRole::ActiveSecondary).await?;
        let mut second = descriptor(&secondary);
        second.role = ReplicaRole::ActiveSecondary;
        second.ready = true;
        let mut third = descriptor(&replacement);
        third.role = ReplicaRole::ActiveSecondary;
        third.ready = true;
        set_current(&primary, configuration(vec![second, third], 2)).await
    };
    let writes = async {
        // Use the actual joined admission pump, but coordinate on acceptance
        // and local application, not client completion while deliberately
        // withholding provider EOS. The client timeout must not be extended to
        // accommodate a fixture-imposed copy barrier.
        let mut accepted = Vec::new();
        for n in 6..=105 {
            let (reply, result) = oneshot::channel();
            assert!(primary
                .requests
                .try_send(Submit {
                    mutation: mutation(n),
                    reply
                })
                .is_ok());
            match result.await.unwrap() {
                Admission::Accepted(replication) => accepted.push(replication),
                Admission::Result(result) => panic!("write admission failed: {result:?}"),
            }
            assert_eq!(primary.provider.snapshot().last, n);
            assert!(!replacement.engine.snapshot().copy_done);
        }
        eos.add_permits(1);
        for mut replication in accepted {
            replication
                .completion
                .wait(Cancellation::default())
                .await
                .unwrap();
        }
    };
    let joined = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(build, writes)
    })
    .await;
    if joined.is_err() {
        cancellation.cancel();
        panic!(
            "live copy timeout: primary={:?}; target={:?}; last={}; pump={:?}",
            primary.engine.snapshot(),
            replacement.engine.snapshot(),
            replacement.provider.snapshot().last,
            replacement.fault.lock().unwrap()
        );
    }
    let (built, ()) = joined.unwrap();
    built.unwrap();
    until(|| replacement.provider.snapshot().last == 105).await;
    assert_eq!(
        replacement.provider.snapshot().values,
        primary.provider.snapshot().values
    );
    assert_eq!(
        replacement.provider.snapshot().results,
        primary.provider.snapshot().results
    );
    primary
        .engine
        .wait_for_catch_up_quorum(CatchUpMode::All, Cancellation::default())
        .await
        .unwrap();
    rap_role(&secondary, ReplicaRole::Primary).await.unwrap();
    set_current(&secondary, configuration(vec![], 1))
        .await
        .unwrap();
    until(|| secondary.engine.snapshot().ready).await;
    *secondary_native.status.lock().unwrap() = status(ReplicaRole::Primary);
    let copied = secondary.write(mutation(1)).await;
    assert_eq!(copied.sequence, Some(1));
    assert!(copied.duplicate);
    assert_eq!(copied.outcome, Outcome::Committed);
    assert_eq!(
        secondary
            .write(Mutation {
                value: "changed".into(),
                ..mutation(1)
            })
            .await
            .outcome,
        Outcome::Rejected
    );
    assert_eq!(secondary.engine.snapshot().progress.assigned, 105);
    assert_eq!(secondary.provider.snapshot().history_count, 105);
    shutdown_route(&primary).await;
    shutdown_route(&secondary).await;
    shutdown_route(&replacement).await;
}

#[tokio::test]
async fn cancellation_after_acceptance_is_ambiguous_never_undo_and_terminal_owner_isolated() {
    let partition = Uuid::new_v4();
    let (primary, _) = route(partition, 1, ReplicaRole::Primary).await;
    let (secondary, _) = route(partition, 2, ReplicaRole::IdleSecondary).await;
    rap_build(&primary, descriptor(&secondary), Cancellation::default())
        .await
        .unwrap();
    let mut peer = descriptor(&secondary);
    peer.role = ReplicaRole::ActiveSecondary;
    peer.ready = true;
    set_current(&primary, configuration(vec![peer], 2))
        .await
        .unwrap();
    secondary.delay.store(2000, Ordering::Release);
    let (control, _) = route(Uuid::new_v4(), 3, ReplicaRole::Primary).await;
    let write = primary.write(mutation("ambiguous"));
    tokio::pin!(write);
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut write)
        .await
        .is_err());
    assert_eq!(primary.provider.snapshot().last, 1);
    let duplicate = primary.write(mutation("ambiguous")).await;
    assert_eq!(duplicate.outcome, Outcome::Ambiguous);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.sequence, Some(1));
    assert_eq!(primary.engine.snapshot().progress.assigned, 1);
    shutdown_route(&primary).await;
    let result = write.await;
    assert_eq!(result.outcome, Outcome::Ambiguous);
    assert_eq!(result.sequence, Some(1));
    assert_eq!(result.json()["outcome"], "ambiguous");
    assert_eq!(result.json()["sequence"], 1);
    assert_eq!(primary.provider.snapshot().results["ambiguous"], 1);
    assert_eq!(
        control.write(mutation("unrelated")).await.outcome,
        Outcome::Committed
    );
    shutdown_route(&secondary).await;
    shutdown_route(&control).await;
}

#[tokio::test]
async fn native_authority_revoked_while_waiting_fences_committed_publication() {
    let partition = Uuid::new_v4();
    let (primary, native) = route(partition, 1, ReplicaRole::Primary).await;
    let (secondary, _) = route(partition, 2, ReplicaRole::IdleSecondary).await;
    rap_build(&primary, descriptor(&secondary), Cancellation::default())
        .await
        .unwrap();
    let mut peer = descriptor(&secondary);
    peer.role = ReplicaRole::ActiveSecondary;
    peer.ready = true;
    set_current(&primary, configuration(vec![peer], 2))
        .await
        .unwrap();
    secondary.delay.store(180, Ordering::Release);
    let mut write = Box::pin(primary.write(mutation("revoked-result")));
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut write)
        .await
        .is_err());
    assert_eq!(primary.provider.snapshot().last, 1);
    native.alive.store(false, Ordering::Release);
    let result = write.await;
    until(|| primary.engine.snapshot().progress.committed == 1).await;
    assert_eq!(primary.engine.snapshot().progress.committed, 1);
    assert_eq!(result.outcome, Outcome::Ambiguous);
    assert_eq!(result.sequence, Some(1));
    assert_eq!(
        primary.write(mutation("late")).await.outcome,
        Outcome::Rejected
    );
    assert_eq!(primary.engine.snapshot().progress.assigned, 1);
    shutdown_route(&primary).await;
    shutdown_route(&secondary).await;
}

#[tokio::test]
async fn stale_route_cleanup_cannot_remove_replacement_and_stale_handle_cannot_write() {
    let (old, _) = route(Uuid::new_v4(), 1, ReplicaRole::Primary).await;
    let (new, _) = route(Uuid::new_v4(), 2, ReplicaRole::Primary).await;
    let mut routes = HashMap::from([("/data/owner".to_string(), new.clone())]);
    remove_owned(&mut routes, "/data/owner", &old);
    assert!(Arc::ptr_eq(&routes["/data/owner"], &new));
    shutdown_route(&old).await;
    assert_eq!(
        old.write(mutation("stale")).await.outcome,
        Outcome::Rejected
    );
    assert_eq!(
        new.write(mutation("live")).await.outcome,
        Outcome::Committed
    );
    remove_owned(&mut routes, "/data/owner", &new);
    assert!(routes.is_empty());
    shutdown_route(&new).await;
}

type PumpFixture = (
    Pump,
    mpsc::Sender<TestOperation>,
    mpsc::Sender<TestOperation>,
    Arc<Mutex<Option<String>>>,
    mpsc::Sender<Submit>,
);
fn pump_fixture(provider: Arc<MemoryProvider>, delay: u64) -> PumpFixture {
    pump_fixture_with_hold(provider, delay, Arc::new(CopyAckHold::default()))
}
fn pump_fixture_with_hold(
    provider: Arc<MemoryProvider>,
    delay: u64,
    copy_ack_hold: Arc<CopyAckHold>,
) -> PumpFixture {
    let (copy_tx, copy_rx) = mpsc::channel(8);
    let (replication_tx, replication_rx) = mpsc::channel(8);
    let (request_tx, requests) = mpsc::channel(1);
    // Keep request admission open until the test shuts down its fixture.
    let native = Arc::new(NativeModel {
        alive: AtomicBool::new(true),
        status: Mutex::new(status(ReplicaRole::ActiveSecondary)),
    });
    let fault = Arc::new(Mutex::new(None));
    let pump = Pump::start(
        PumpInput {
            stream_refresh: watch::channel(()).1,
            native,
            engine: Weak::new(),
            provider,
            copy: Arc::new(TestStream(tokio::sync::Mutex::new(copy_rx))),
            replication: Arc::new(TestStream(tokio::sync::Mutex::new(replication_rx))),
            requests,
            delay: Arc::new(AtomicU64::new(delay)),
            copy_ack_hold,
            fault: fault.clone(),
        },
        Cancellation::default(),
    )
    .unwrap();
    (pump, copy_tx, replication_tx, fault, request_tx)
}
#[derive(Debug)]
struct TestOperation {
    sequence: i64,
    data: OperationData,
    acknowledgment: Acknowledgment,
}
impl IOperation for TestOperation {
    fn get_metadata(&self) -> RuntimeResult<OperationMetadata> {
        Ok(OperationMetadata {
            operation_type: OperationType::Normal,
            sequence_number: self.sequence,
            atomic_group_id: -1,
        })
    }
    fn get_data(&self) -> RuntimeResult<OperationData> {
        Ok(self.data.clone())
    }
    fn acknowledge(&self) -> RuntimeResult<()> {
        self.acknowledgment.acknowledge()
    }
}
struct TestStream(tokio::sync::Mutex<mpsc::Receiver<TestOperation>>);
impl IOperationStream for TestStream {
    fn get_operation(
        &self,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, Option<Arc<dyn IOperation>>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(RuntimeError::Closed),
                item = async { self.0.lock().await.recv().await } => Ok(item.map(|op| Arc::new(op) as Arc<dyn IOperation>)),
            }
        })
    }
}
// App-owned canonical doubles, not data engine implementation constructors.
struct Acknowledgment {
    called: AtomicBool,
    action: Box<dyn Fn() -> RuntimeResult<()> + Send + Sync>,
}
impl std::fmt::Debug for Acknowledgment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TestAck")
    }
}
impl Acknowledgment {
    fn new(action: impl Fn() -> RuntimeResult<()> + Send + Sync + 'static) -> Self {
        Self {
            called: AtomicBool::new(false),
            action: Box::new(action),
        }
    }
    fn acknowledge(&self) -> RuntimeResult<()> {
        if self.called.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::InvalidInput("duplicate test ACK"));
        }
        (self.action)()
    }
}
fn operation(sequence: i64, data: OperationData, ack: Acknowledgment) -> TestOperation {
    TestOperation {
        sequence,
        data,
        acknowledgment: ack,
    }
}

#[tokio::test]
async fn pump_copy_ack_hold_final_nonempty_receive_release_cancel_deadline_and_eos() {
    for action in ["release", "cancel", "deadline"] {
        let provider = MemoryProvider::new();
        let hold = Arc::new(CopyAckHold::default());
        hold.set(if action == "deadline" { 100 } else { 6000 })
            .unwrap();
        let (mut pump, copy, replication, fault, _requests) =
            pump_fixture_with_hold(provider.clone(), 0, hold.clone());
        let source = MemoryProvider::new();
        source
            .apply_replication(1, &mutation("copied").operation().unwrap())
            .unwrap();
        let stream = source.get_copy_state(1, None).unwrap();
        let acknowledgments = Arc::new(AtomicUsize::new(0));
        let mut sequence = 0;
        while let Some(data) = stream.get_next(Cancellation::default()).await.unwrap() {
            sequence += 1;
            let acks = acknowledgments.clone();
            copy.send(operation(
                sequence,
                data,
                Acknowledgment::new(move || {
                    acks.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }),
            ))
            .await
            .unwrap();
        }
        // EOS is available, but neither stream disposal nor provider CopyEnd
        // application is permission to acknowledge the held real operation.
        drop(copy);
        until(|| hold.snapshot()["held"] == true).await;
        assert_eq!(acknowledgments.load(Ordering::Acquire), 2);
        assert_eq!(provider.snapshot().last, 1);
        assert!(!provider.snapshot().copy_in_progress);
        assert!(
            hold.set(6000).is_err(),
            "rearming cannot extend an owned hold"
        );
        let acks = acknowledgments.clone();
        replication
            .send(operation(
                2,
                mutation("tail").operation().unwrap(),
                Acknowledgment::new(move || {
                    acks.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }),
            ))
            .await
            .unwrap();
        assert_eq!(provider.snapshot().last, 1);
        match action {
            "release" => {
                hold.set(0).unwrap();
                until(|| acknowledgments.load(Ordering::Acquire) == 4).await;
                assert_eq!(provider.snapshot().last, 2);
                assert_eq!(hold.snapshot()["acknowledged"], true);
                assert_eq!(hold.snapshot()["released"], true);
            }
            "cancel" => {
                let start = Instant::now();
                pump.join();
                assert!(start.elapsed() < Duration::from_millis(150));
                assert_eq!(hold.snapshot()["cancelled"], true);
                assert_eq!(acknowledgments.load(Ordering::Acquire), 2);
            }
            _ => {
                until(|| fault.lock().unwrap().is_some()).await;
                assert_eq!(hold.snapshot()["expired"], true);
                assert_eq!(acknowledgments.load(Ordering::Acquire), 2);
            }
        }
        pump.join();
        assert_eq!(hold.snapshot()["held"], false);
        if action != "deadline" {
            assert!(fault.lock().unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn pump_copy_eos_does_not_end_replication_and_ack_observes_actual_provider_apply() {
    let provider = MemoryProvider::new();
    let (mut pump, copy, replication, fault, _requests) = pump_fixture(provider.clone(), 0);
    let source = MemoryProvider::new();
    let stream = source.get_copy_state(0, None).unwrap();
    let acknowledgments = Arc::new(AtomicUsize::new(0));
    let mut sequence = 0;
    while let Some(data) = stream.get_next(Cancellation::default()).await.unwrap() {
        sequence += 1;
        let acks = acknowledgments.clone();
        let provider = provider.clone();
        let ack = Acknowledgment::new(move || {
            assert_eq!(provider.snapshot().copy_in_progress, sequence == 1);
            acks.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        copy.send(operation(sequence, data, ack)).await.unwrap();
    }
    drop(copy);
    until(|| acknowledgments.load(Ordering::Acquire) == 2).await;
    let observed = provider.clone();
    let acks = acknowledgments.clone();
    replication
        .send(operation(
            1,
            mutation("applied").operation().unwrap(),
            Acknowledgment::new(move || {
                assert_eq!(observed.snapshot().results["applied"], 1);
                acks.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }),
        ))
        .await
        .unwrap();
    until(|| acknowledgments.load(Ordering::Acquire) == 3).await;
    pump.join();
    assert!(fault.lock().unwrap().is_none());
    assert_eq!(
        Arc::strong_count(&provider),
        1,
        "thread dropped every provider capture before join"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sample_pump_refreshes_demoted_role_streams_without_recopy_or_new_engine() {
    let partition = Uuid::new_v4();
    let (a, a_native) = route(partition, 1, ReplicaRole::Primary).await;
    set_current(&a, configuration(vec![], 1)).await.unwrap();
    until(|| a.engine.snapshot().ready).await;
    assert_eq!(a.write(mutation(1)).await.outcome, Outcome::Committed);
    let (b, b_native) = route(partition, 2, ReplicaRole::IdleSecondary).await;
    rap_build(&a, descriptor(&b), Cancellation::default())
        .await
        .unwrap();
    rap_role(&b, ReplicaRole::ActiveSecondary).await.unwrap();
    let mut peer = descriptor(&b);
    peer.role = ReplicaRole::ActiveSecondary;
    peer.ready = true;
    set_current(&a, configuration(vec![peer], 2)).await.unwrap();
    assert_eq!(a.write(mutation(2)).await.outcome, Outcome::Committed);
    rap_role(&b, ReplicaRole::Primary).await.unwrap();
    *b_native.status.lock().unwrap() = status(ReplicaRole::Primary);
    let old = a
        .engine
        .get_state_replicator()
        .get_replication_stream()
        .unwrap();
    rap_role(&a, ReplicaRole::ActiveSecondary).await.unwrap();
    *a_native.status.lock().unwrap() = status(ReplicaRole::ActiveSecondary);
    let fresh = a
        .engine
        .get_state_replicator()
        .get_replication_stream()
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &fresh));
    let mut peer = descriptor(&a);
    peer.role = ReplicaRole::ActiveSecondary;
    peer.ready = true;
    peer.progress = ReplicationProgress { first: 1, last: 2 };
    set_current(&b, configuration(vec![peer], 2)).await.unwrap();
    until(|| b.engine.snapshot().ready).await;
    assert_eq!(b.write(mutation(3)).await.outcome, Outcome::Committed);
    until(|| a.provider.snapshot().last == 3).await;
    assert_eq!(a.provider.snapshot().values, b.provider.snapshot().values);
    assert!(a.fault.lock().unwrap().is_none());
    shutdown_route(&a).await;
    shutdown_route(&b).await;
}

#[tokio::test]
async fn pump_cancel_delay_joins_and_drops_capture_without_ack_and_apply_error_never_acks() {
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let provider = MemoryProvider::new();
    let (mut pump, _copy, replication, _, _requests) = pump_fixture(provider.clone(), 2000);
    let dropped = Arc::new(AtomicUsize::new(0));
    let capture = Dropped(dropped.clone());
    replication
        .send(operation(
            1,
            mutation("cancel").operation().unwrap(),
            Acknowledgment::new(move || {
                let _ = &capture;
                panic!("cancelled delay must not ACK");
            }),
        ))
        .await
        .unwrap();
    until(|| provider.snapshot().last == 1).await;
    let start = Instant::now();
    pump.join();
    assert!(start.elapsed() < Duration::from_millis(150));
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(Arc::strong_count(&provider), 1);

    let provider = MemoryProvider::new();
    let (mut pump, _copy, replication, fault, _requests) = pump_fixture(provider.clone(), 0);
    replication
        .send(operation(
            1,
            OperationData::new(vec![]).unwrap(),
            Acknowledgment::new(|| {
                panic!("invalid provider data must not ACK");
            }),
        ))
        .await
        .unwrap();
    until(|| fault.lock().unwrap().is_some()).await;
    pump.join();
    assert_eq!(provider.snapshot().last, 0);
    assert_eq!(Arc::strong_count(&provider), 1);
}
