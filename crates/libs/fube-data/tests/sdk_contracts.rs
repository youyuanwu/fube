//! Public-only, offline native-RAP model witnesses, not live qualification.
use fube_data as data_contract;
use fube_data::{
    extensions::{
        EpochDomains, OperationOriginStream, ReplayCheckpointProvider, ReplicationEndpointOptions,
        ReplicatorDiagnostics, StateProviderCapabilities,
    },
    Cancellation, Epoch, FabricReplicator, IOperation, IOperationDataStream, IOperationStream,
    IStateProvider, IStateReplicator, MemoryProvider, Mutation, OperationData, OperationType,
    ReplicationAdmission, Result, Settings,
};
use fube_runtime::{
    extensions::{
        NativeIntegrationProvider, NativeInvocation, NativeInvocationKind, NativeReplicaDescriptor,
        NativeReplicaSetConfiguration, NativeRuntimeIntegration, ReplicationProgress,
    },
    ApiFuture, ErrorKind, IPrimaryReplicator, IReplicator, IStatefulServiceFactory,
    IStatefulServicePartition, IStatefulServiceReplica, OpenMode, ReplicaRole,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
#[path = "support/upper_log.rs"]
mod upper_log;

#[path = "support/rap_model.rs"]
mod rap;

const EPOCH: Epoch = Epoch {
    data_loss_number: 1,
    configuration_number: 1 << 32,
};

struct Payloads(Mutex<VecDeque<OperationData>>);
impl IOperationDataStream for Payloads {
    fn get_next(&self, cancel: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(fube_data::Error::Closed);
            }
            Ok(self.0.lock().unwrap().pop_front())
        })
    }
}

/// Intentionally implements ONLY the standard provider trait, not replay.
struct StandardOnly {
    memory: Arc<MemoryProvider>,
    copies: Mutex<Vec<i64>>,
    epochs: Mutex<Vec<(Epoch, i64)>>,
    epoch_gate: Mutex<Option<CallbackGate>>,
}
struct CallbackGate {
    entered: oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
impl StandardOnly {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            memory: MemoryProvider::new(),
            copies: Mutex::new(vec![]),
            epochs: Mutex::new(vec![]),
            epoch_gate: Mutex::new(None),
        })
    }
}
impl IStateProvider for StandardOnly {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.memory.get_last_committed_sequence_number()
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, cancel: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let gate = self.epoch_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.send(()).unwrap();
                gate.release
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .map_err(|_| fube_data::Error::Timeout)?;
            }
            self.epochs.lock().unwrap().push((epoch, previous));
            self.memory.update_epoch(epoch, previous, cancel).await
        })
    }
    fn on_data_loss(&self, cancel: Cancellation) -> ApiFuture<'_, bool> {
        self.memory.on_data_loss(cancel)
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        Ok(Arc::new(Payloads(Mutex::new(VecDeque::from([
            OperationData::new(vec![])?,
        ])))))
    }
    fn get_copy_state(
        &self,
        horizon: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        assert!(context.is_none(), "volatile engine must pass exact None");
        assert_eq!(std::thread::current().name(), Some("fube-provider-call"));
        self.copies.lock().unwrap().push(horizon);
        self.memory.get_copy_state(horizon, context)
    }
}

type Created =
    Arc<Mutex<Option<oneshot::Sender<(Arc<FabricReplicator>, NativeRuntimeIntegration)>>>>;
type PayloadStream = Arc<dyn IOperationDataStream>;
#[derive(Default)]
struct ControlCalls {
    configurations: std::sync::atomic::AtomicUsize,
    builds: std::sync::atomic::AtomicUsize,
    removals: Mutex<Vec<NativeInvocation>>,
    fail_after_build: std::sync::atomic::AtomicBool,
    roles: std::sync::atomic::AtomicUsize,
}
struct Factory {
    native: NativeIntegrationProvider,
    provider: Arc<dyn IStateProvider>,
    replay: Option<Arc<dyn ReplayCheckpointProvider>>,
    created: Created,
    capabilities: Option<StateProviderCapabilities>,
    bootstrap: Option<Arc<upper_log::UpperLog>>,
    control_calls: Arc<ControlCalls>,
}
struct Replica {
    native: NativeRuntimeIntegration,
    provider: Arc<dyn IStateProvider>,
    replay: Option<Arc<dyn ReplayCheckpointProvider>>,
    created: Created,
    capabilities: Option<StateProviderCapabilities>,
    bootstrap: Option<Arc<upper_log::UpperLog>>,
    control_calls: Arc<ControlCalls>,
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
            provider: self.provider.clone(),
            replay: self.replay.clone(),
            created: self.created.clone(),
            capabilities: self.capabilities,
            bootstrap: self.bootstrap.clone(),
            control_calls: self.control_calls.clone(),
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
            let endpoint = ReplicationEndpointOptions {
                listen_address: "127.0.0.1:0".into(),
                actor_incarnation: uuid::Uuid::new_v4(),
            };
            let settings = Settings {
                operation_bytes: 4096,
                ..Settings::default()
            };
            let fabric = if let Some(capabilities) = self.capabilities {
                FabricReplicator::create_with_capabilities(
                    partition,
                    self.native.clone(),
                    self.provider.clone(),
                    self.replay.clone(),
                    endpoint,
                    settings,
                    capabilities,
                )?
            } else {
                FabricReplicator::create(
                    partition,
                    self.native.clone(),
                    self.provider.clone(),
                    self.replay.clone(),
                    endpoint,
                    settings,
                )?
            };
            if let Some(bootstrap) = &self.bootstrap {
                assert_eq!(
                    bootstrap.getters.load(std::sync::atomic::Ordering::Acquire),
                    0,
                    "native-bound construction must not query the unopened provider"
                );
                bootstrap
                    .constructed
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            if let Some(send) = self.created.lock().unwrap().take() {
                let _ = send.send((fabric.clone(), self.native.clone()));
            }
            Ok(Box::new(PublicComposite {
                lower: fabric,
                upper: self.bootstrap.clone(),
                probe_cache: self
                    .capabilities
                    .is_some_and(|c| c.supports_copy_until_latest_lsn),
                controls: self.control_calls.clone(),
                native: self.native.clone(),
            }) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://public-data".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Fixture {
    fabric: Arc<FabricReplicator>,
    native: NativeRuntimeIntegration,
    provider: Arc<StandardOnly>,
    host: Arc<rap::Host>,
    controls: Arc<ControlCalls>,
}

/// Only ordinary public contracts: no data engine/lifecycle/rebase/binder seam.
struct PublicComposite {
    lower: Arc<FabricReplicator>,
    upper: Option<Arc<upper_log::UpperLog>>,
    probe_cache: bool,
    controls: Arc<ControlCalls>,
    native: NativeRuntimeIntegration,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_primary_provider_failure_faults_before_epoch_and_preserves_failed_open_cleanup() {
    use std::sync::atomic::Ordering;
    for failure in 0..3 {
        let upper = upper_log::UpperLog::new(EPOCH, 27);
        match failure {
            0 => upper.fail_next_getter.store(true, Ordering::Release),
            1 => upper.bad_next_getter.store(true, Ordering::Release),
            _ => upper.fail_next_epoch.store(true, Ordering::Release),
        }
        let supplied = upper.clone();
        let marker = upper.clone();
        let controls = Arc::new(ControlCalls::default());
        let control_calls = controls.clone();
        let (send, receive) = oneshot::channel();
        let (host, opened) = rap::Host::start_source_outcome(
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
                    provider: supplied,
                    replay: None,
                    created: Arc::new(Mutex::new(Some(send))),
                    capabilities: Some(StateProviderCapabilities {
                        supports_copy_until_latest_lsn: false,
                    }),
                    bootstrap: Some(marker),
                    control_calls,
                })
            },
        )
        .await;
        let (fabric, _native) = receive.await.unwrap();
        assert!(opened.is_err());
        assert_eq!(upper.getters.load(Ordering::Acquire), 1);
        assert_eq!(
            upper.updates.load(Ordering::Acquire),
            usize::from(failure == 2)
        );
        assert_eq!(
            controls.roles.load(Ordering::Acquire),
            0,
            "lower failure reached upper role continuation"
        );
        assert!(fabric.snapshot().closed);
        assert_eq!(
            fabric.get_current_progress().unwrap_err().kind(),
            ErrorKind::ObjectClosed
        );
        drop(host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_secondary_progress_is_native_not_ready_not_zero_success() {
    let fixture = Fixture::new(uuid::Uuid::new_v4(), 1, ReplicaRole::IdleSecondary).await;
    let error = fixture.host.get_status().await.unwrap_err();
    assert_eq!(error.native_code(), Some(0x80071bc7u32 as i32 as i64));
    let control = fixture.fabric.clone();
    let error = fixture
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || control.get_catch_up_capability())
        .await
        .unwrap_err();
    assert_eq!(error.native_code(), Some(0x80004005u32 as i32 as i64));
    assert!(fixture.provider.copies.lock().unwrap().is_empty());
    assert!(fixture.provider.epochs.lock().unwrap().is_empty());
}
impl IReplicator for PublicComposite {
    fn open(&self, cancel: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async move {
            let address = self.lower.open(cancel).await?;
            if let Some(upper) = &self.upper {
                assert_eq!(
                    upper.getters.load(std::sync::atomic::Ordering::Acquire),
                    0,
                    "lower Open must not query the upper provider"
                );
                if self.probe_cache {
                    upper
                        .fail_next_getter
                        .store(true, std::sync::atomic::Ordering::Release);
                    assert_eq!(
                        self.lower.get_current_progress().unwrap_err().kind(),
                        ErrorKind::Provider
                    );
                    assert_eq!(self.lower.get_catch_up_capability()?, 0);
                    assert_eq!(self.lower.get_current_progress()?, upper.tail());
                    assert_eq!(self.lower.get_current_progress()?, upper.tail());
                    assert_eq!(
                        upper.getters.load(std::sync::atomic::Ordering::Acquire),
                        2,
                        "failure was not cached; success was"
                    );
                }
            }
            Ok(address)
        })
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.lower.close(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        let lower = self.lower.change_role(epoch, role, cancel);
        Box::pin(async move {
            lower.await?;
            self.controls
                .roles
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Ok(())
        })
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.lower.update_epoch(epoch, cancel)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.lower.get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.lower.get_catch_up_capability()
    }
    fn abort(&self) {
        self.lower.abort();
    }
}
impl IPrimaryReplicator for PublicComposite {
    fn on_data_loss(&self, cancel: Cancellation) -> ApiFuture<'_, bool> {
        self.lower.on_data_loss(cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        cc: fube_runtime::ReplicaSetConfig,
        pc: fube_runtime::ReplicaSetConfig,
    ) -> Result<()> {
        self.controls
            .configurations
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.lower.update_catch_up_replica_set_configuration(cc, pc)
    }
    fn update_current_replica_set_configuration(
        &self,
        cc: fube_runtime::ReplicaSetConfig,
    ) -> Result<()> {
        self.controls
            .configurations
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.lower.update_current_replica_set_configuration(cc)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: fube_runtime::ReplicaSetQuorumMode,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.lower.wait_for_catch_up_quorum(mode, cancel)
    }
    fn build_replica(
        &self,
        replica: fube_runtime::ReplicaInformation,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.controls
            .builds
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let lower = self.lower.build_replica(replica, cancel);
        Box::pin(async move {
            lower.await?;
            if self
                .controls
                .fail_after_build
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return Err(fube_data::Error::Remote {
                    code: 0x80004005u32 as i32 as i64,
                    message: "controlled public completion failure after real lower Build".into(),
                });
            }
            Ok(())
        })
    }
    fn remove_replica(&self, replica: i64) -> Result<()> {
        // Framework-only native-proof discriminator. No shared support/txn
        // helper exports this association or controls the lower.
        self.controls
            .removals
            .lock()
            .unwrap()
            .push(self.native.capture_removal(replica)?);
        self.lower.remove_replica(replica)
    }
}
impl Fixture {
    async fn new(partition: uuid::Uuid, replica: i64, role: ReplicaRole) -> Self {
        let provider = StandardOnly::new();
        let supplied = provider.clone();
        let controls = Arc::new(ControlCalls::default());
        let control_calls = controls.clone();
        let (send, receive) = oneshot::channel();
        let host = rap::Host::start_source_at_epoch(
            partition,
            rap::ReplicaSource {
                replica_id: replica,
                native_incarnation: replica + 10,
                node_id: [replica as u8; 16],
                node_instance: 1,
            },
            role,
            EPOCH,
            move |native| {
                Box::new(Factory {
                    native,
                    provider: supplied,
                    replay: None,
                    created: Arc::new(Mutex::new(Some(send))),
                    capabilities: None,
                    bootstrap: None,
                    control_calls,
                })
            },
        )
        .await;
        let (fabric, native) = receive.await.unwrap();
        Self {
            fabric,
            native,
            provider,
            host,
            controls,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_stop_observes_actual_close_before_guarded_shutdown_with_active_epoch() {
    // Local fixture ownership regression. This holds an actually entered
    // managed provider callback, not a timer/sleep or a fake native Close result.
    let fixture = Fixture::new(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary).await;
    let Fixture {
        fabric,
        native,
        provider,
        host,
        controls: _,
    } = fixture;
    let (entered, active) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    *provider.epoch_gate.lock().unwrap() = Some(CallbackGate {
        entered,
        release: blocked,
    });
    let pending = host
        .begin_epoch(Epoch {
            configuration_number: 2 << 32,
            ..EPOCH
        })
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), active)
        .await
        .unwrap()
        .unwrap();
    let mut events = host.subscribe_events();
    let stopped = std::thread::spawn(move || drop(host));
    let retained = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv()).await;
    let waited_for_callback = !stopped.is_finished();
    // Always release the callback, including failed pre-correction probes.
    let _ = release.send(());
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), pending).await;
    let sent = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv()).await;
    let joined = tokio::task::spawn_blocking(move || stopped.join())
        .await
        .unwrap();
    assert_eq!(retained.unwrap().unwrap(), rap::HostEvent::CloseRequested);
    assert!(
        waited_for_callback,
        "model shut down before admitted callback completed"
    );
    assert_eq!(sent.unwrap().unwrap(), rap::HostEvent::CloseFrameSent);
    assert!(
        result.unwrap().unwrap().is_ok(),
        "original callback result was not successful"
    );
    assert!(joined.is_ok(), "guarded model runtime shutdown failed");
    assert!(fabric.snapshot().closed, "actual lower owner did not close");
    assert_eq!(native.status().unwrap_err().kind(), ErrorKind::ObjectClosed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_primary_samples_actual_nonempty_provider_tail_after_construction() {
    use std::sync::atomic::Ordering;
    for (tail, latest) in [(1, false), (27, true), (20, false)] {
        let partition = uuid::Uuid::new_v4();
        let upper = upper_log::UpperLog::new(EPOCH, tail);
        let supplied = upper.clone();
        let marker = upper.clone();
        let controls = Arc::new(ControlCalls::default());
        let control_calls = controls.clone();
        let (send, receive) = oneshot::channel();
        let host = rap::Host::start(partition, 1, ReplicaRole::Primary, move |native| {
            Box::new(Factory {
                native,
                provider: supplied,
                replay: None,
                created: Arc::new(Mutex::new(Some(send))),
                capabilities: Some(StateProviderCapabilities {
                    supports_copy_until_latest_lsn: latest,
                }),
                bootstrap: Some(marker),
                control_calls,
            })
        })
        .await;
        let (fabric, native) = receive.await.unwrap();
        assert!(!upper.queried_before_return.load(Ordering::Acquire));
        assert!(upper.constructed.load(Ordering::Acquire));
        assert_eq!(
            upper.getters.load(Ordering::Acquire),
            if latest { 2 } else { 1 }
        );
        assert_eq!(upper.tail(), tail);
        assert_eq!(fabric.snapshot().progress.assigned, tail);
        assert_eq!(fabric.snapshot().progress.committed, tail);
        host.get_status().await.unwrap();
        assert_eq!(native.status().unwrap().progress.last, tail);
        host.configure(config(vec![], 1)).await.unwrap();
        assert_eq!(
            controls.configurations.load(Ordering::Acquire),
            1,
            "returned composite was bypassed/duplicated"
        );
        let data = OperationData::new(vec![vec![28]]).unwrap();
        let mut admitted = fabric
            .get_state_replicator()
            .replicate(data.clone(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!(admitted.sequence_number, tail + 1);
        upper.append(tail + 1, &data);
        assert_eq!(
            admitted
                .completion
                .wait(Cancellation::default())
                .await
                .unwrap(),
            tail + 1
        );
        // The same initialized standard provider supplies actual bytes, not an
        // inferred transaction visibility/stability tail or fabricated zero.
        let copy = upper.get_copy_state(tail + 1, None).unwrap();
        let image = copy
            .get_next(Cancellation::default())
            .await
            .unwrap()
            .unwrap();
        let imported = upper_log::UpperLog::new(EPOCH, 1);
        imported.apply_snapshot(&image);
        assert_eq!(imported.tail(), tail + 1);
        assert_eq!(upper.horizons.lock().unwrap().as_slice(), [tail + 1]);
        assert_eq!(
            OpenMode::Existing.validate().unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }
}
impl Fixture {
    fn descriptor(&self, role: ReplicaRole) -> NativeReplicaDescriptor {
        let identity = self.native.identity();
        NativeReplicaDescriptor {
            node_id: identity.node_id,
            node_instance: identity.node_instance,
            replica_id: identity.replica_id,
            incarnation: identity.incarnation,
            role,
            previous_role: ReplicaRole::None,
            up: true,
            ready: role == ReplicaRole::ActiveSecondary,
            address: self.fabric.endpoint().to_string(),
            progress: ReplicationProgress {
                first: -1,
                last: -1,
            },
            must_catch_up: false,
        }
    }
}
fn config(replicas: Vec<NativeReplicaDescriptor>, quorum: usize) -> NativeReplicaSetConfiguration {
    NativeReplicaSetConfiguration {
        epoch: EPOCH,
        replicas,
        write_quorum: quorum,
        local_member: true,
    }
}
fn mutation(id: &str) -> OperationData {
    Mutation {
        id: id.into(),
        key: id.into(),
        value: format!("value-{id}"),
    }
    .operation_for_epoch(EPOCH)
    .unwrap()
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("public-contract condition");
}
fn metadata(operation: &dyn IOperation, sequence: i64) {
    let metadata = operation.get_metadata().unwrap();
    assert_eq!(metadata.operation_type, OperationType::Normal);
    assert_eq!(metadata.operation_type as u32, 0x0001);
    assert_eq!(metadata.sequence_number, sequence);
    assert_eq!(metadata.atomic_group_id, -1);
}
async fn next(stream: &dyn IOperationStream) -> Arc<dyn IOperation> {
    tokio::time::timeout(
        Duration::from_secs(3),
        stream.get_operation(Cancellation::default()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}
async fn admit(fixture: &Fixture, id: &str) -> ReplicationAdmission {
    let payload = mutation(id);
    let state: Arc<dyn IStateReplicator> = fixture.fabric.get_state_replicator();
    let accepted: ReplicationAdmission = tokio::time::timeout(
        Duration::from_secs(1),
        state.replicate(payload.clone(), Cancellation::default()),
    )
    .await
    .unwrap()
    .unwrap();
    fixture
        .provider
        .memory
        .apply_replication(accepted.sequence_number, &payload)
        .unwrap();
    accepted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standard_provider_sync_shapes_one_owner_copy_gap_early_lsn_and_explicit_ack() {
    let partition = uuid::Uuid::new_v4();
    let primary = Fixture::new(partition, 1, ReplicaRole::Primary).await;
    primary.host.configure(config(vec![], 1)).await.unwrap();
    let control: Arc<dyn IPrimaryReplicator> = Arc::new((*primary.fabric).clone());
    let root = control.clone();
    let current = config(vec![], 1).sdk().unwrap();
    let outside_invocation = primary
        .fabric
        .execution_runtime()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let result: Result<()> = root.update_current_replica_set_configuration(current);
            result
        })
        .await;
    assert_eq!(
        outside_invocation.unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    primary.host.configure(config(vec![], 1)).await.unwrap();
    let state: Arc<dyn IStateReplicator> = primary.fabric.get_state_replicator();
    let stream_a: Result<Arc<dyn IOperationStream>> = state.get_replication_stream();
    let stream_b: Result<Arc<dyn IOperationStream>> = state.get_replication_stream();
    assert!(
        Arc::ptr_eq(&stream_a.unwrap(), &stream_b.unwrap()),
        "one shared stream, no relay"
    );
    let provider: Arc<dyn IStateProvider> = primary.provider.clone();
    let context: Result<Arc<dyn IOperationDataStream>> = provider.get_copy_context();
    let context = context.unwrap();
    assert_eq!(
        context
            .get_next(Cancellation::default())
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        0
    );
    assert!(context
        .get_next(Cancellation::default())
        .await
        .unwrap()
        .is_none());
    // Sync factories are pinned by the trait signatures, not async adapters.
    let _copy_factory: fn(&StandardOnly, i64, Option<PayloadStream>) -> Result<PayloadStream> =
        StandardOnly::get_copy_state;
    let _last: Result<i64> = provider.get_last_committed_sequence_number();
    let mut first = admit(&primary, "one").await;
    assert_eq!(
        first
            .completion
            .wait(Cancellation::default())
            .await
            .unwrap(),
        1
    );
    let secondary = Fixture::new(partition, 2, ReplicaRole::IdleSecondary).await;
    let secondary_state: Arc<dyn IStateReplicator> = secondary.fabric.get_state_replicator();
    let copy: Result<Arc<dyn IOperationStream>> = secondary_state.get_copy_stream();
    let copy = copy.unwrap();
    let node = primary.host.clone();
    let target = secondary.descriptor(ReplicaRole::IdleSecondary);
    let mut build = tokio::spawn(async move { node.build(target, Cancellation::default()).await });
    let one = next(&*copy).await;
    let two = next(&*copy).await;
    let three = next(&*copy).await;
    for (sequence, operation) in [(1, &one), (2, &two), (3, &three)] {
        metadata(&**operation, sequence);
        secondary
            .provider
            .memory
            .apply_copy(&operation.get_data().unwrap())
            .unwrap();
    }
    two.acknowledge().unwrap();
    three.acknowledge().unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut build)
        .await
        .is_err());
    assert!(
        !secondary.fabric.snapshot().copy_done,
        "ACK gap is not copied completion"
    );
    one.acknowledge().unwrap();
    assert!(one.acknowledge().is_err(), "ACK is once only");
    drop((one, two, three));
    build.await.unwrap().unwrap();
    assert!(
        copy.get_operation(Cancellation::default())
            .await
            .unwrap()
            .is_none(),
        "EOS is None, not another application ACK"
    );
    assert_eq!(*primary.provider.copies.lock().unwrap(), vec![1]);
    assert_eq!(
        secondary.provider.memory.snapshot().results,
        primary.provider.memory.snapshot().results
    );
    secondary
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    let mut current = config(vec![secondary.descriptor(ReplicaRole::ActiveSecondary)], 2);
    current.epoch.configuration_number += 7;
    primary
        .host
        .catch_up(current.clone(), config(vec![], 1))
        .await
        .unwrap();
    primary.host.configure(current).await.unwrap();
    until(|| primary.fabric.snapshot().ready).await;
    // Sample native paired progress while the replica is still serving, not
    // immediately before closing its owner and tearing down model IPC ingress.
    // Receipt of a native reply frame is not proof its dispatcher has released
    // every publication/endpoint-ack obligation.
    primary.host.get_status().await.unwrap();
    assert_eq!(
        control.get_current_progress().unwrap(),
        primary.native.status().unwrap().progress.last
    );
    let replication: Result<Arc<dyn IOperationStream>> = secondary_state.get_replication_stream();
    let replication = replication.unwrap();
    let mut accepted = admit(&primary, "two").await;
    assert_eq!(
        accepted.sequence_number, 2,
        "admitted LSN available before quorum"
    );
    assert_eq!(provider.get_last_committed_sequence_number().unwrap(), 2);
    assert_eq!(primary.fabric.snapshot().progress.committed, 1);
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        accepted.completion.wait(Cancellation::default())
    )
    .await
    .is_err());
    let cancel = Cancellation::default();
    cancel.cancel();
    let error = accepted.completion.wait(cancel).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Canceled);
    assert!(error.is_uncertain());
    let operation = next(&*replication).await;
    metadata(&*operation, 2);
    secondary
        .provider
        .memory
        .apply_replication(2, &operation.get_data().unwrap())
        .unwrap();
    operation.acknowledge().unwrap();
    drop(operation);
    assert_eq!(
        accepted
            .completion
            .wait(Cancellation::default())
            .await
            .unwrap(),
        2
    );
    let before = primary.fabric.snapshot().progress.assigned;
    let oversized = OperationData::new(vec![vec![7; 4097]]).unwrap();
    let error = match state.replicate(oversized, Cancellation::default()).await {
        Ok(_) => panic!("oversized admission"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::ReplicationOperationTooLarge);
    assert_eq!(error.native_code(), Some(0x80071c2fu32 as i32 as i64));
    assert_eq!(
        primary.fabric.snapshot().progress.assigned,
        before,
        "pre-admission rejection consumes no LSN"
    );
    let third = admit(&primary, "three").await;
    assert_eq!(third.sequence_number, 3);
    drop(third.completion);
    let origin: Arc<dyn OperationOriginStream> = secondary
        .fabric
        .get_replication_stream_with_origin()
        .unwrap();
    let original = origin
        .get_operation_with_origin(Cancellation::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.epoch, EPOCH.primary());
    metadata(&*original.operation, 3);
    secondary
        .provider
        .memory
        .apply_replication(3, &original.operation.get_data().unwrap())
        .unwrap();
    original.operation.acknowledge().unwrap();
    drop(original);
    until(|| primary.fabric.snapshot().progress.committed == 3).await;
    assert_eq!(
        primary.provider.memory.snapshot().last,
        3,
        "dropped waiter did not undo local apply/admission"
    );
    assert!(!control.on_data_loss(Cancellation::default()).await.unwrap());
    primary.provider.memory.request_data_loss(3).unwrap();
    assert!(
        control.on_data_loss(Cancellation::default()).await.unwrap(),
        "provider changed bool survives combined control"
    );
    assert!(primary
        .provider
        .epochs
        .lock()
        .unwrap()
        .iter()
        .any(|(epoch, _)| *epoch == EPOCH));
    assert_eq!(control.get_current_progress().unwrap(), 3);
    control.close(Cancellation::default()).await.unwrap();
    assert!(
        state.get_copy_stream().is_err(),
        "retained state shares the closed control owner"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_destination_some_empty_is_not_legacy_eos_and_ack_gap_is_preserved() {
    use fube_data::{
        transport::PeerTransport,
        wire::{Actor, Body, Endpoint, Packet, WireOperation},
    };
    let partition = uuid::Uuid::new_v4();
    let secondary = Fixture::new(partition, 2, ReplicaRole::IdleSecondary).await;
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let sender = Endpoint {
        actor: Actor {
            partition,
            replica: 1,
            incarnation: uuid::Uuid::new_v4(),
        },
        address: network.address().into(),
        transport_id: Some(String::new()),
    };
    network.register_peer(&secondary.fabric.endpoint()).unwrap();
    let send = |body| {
        network
            .send(Packet {
                from: sender.clone(),
                to: secondary.fabric.endpoint().actor,
                body,
            })
            .unwrap()
    };
    send(Body::StartCopy {
        epoch: EPOCH,
        target: 2,
        start: 1,
    });
    tokio::time::timeout(Duration::from_secs(2), incoming.control.recv())
        .await
        .unwrap()
        .unwrap();
    for (sequence, data, last) in [
        (2, vec![vec![]], false),
        (3, vec![], true),
        (1, vec![vec![9]], false),
    ] {
        send(Body::Copy {
            epoch: EPOCH,
            target: 2,
            sequence,
            data,
            last,
        });
    }
    let state: Arc<dyn IStateReplicator> = secondary.fabric.get_state_replicator();
    let copy: Arc<dyn IOperationStream> = state.get_copy_stream().unwrap();
    let one = next(&*copy).await;
    let two = next(&*copy).await;
    metadata(&*one, 1);
    metadata(&*two, 2);
    assert_eq!(
        two.get_data().unwrap().to_segments(),
        vec![Vec::<u8>::new()]
    );
    // This application model stores received payloads before its explicit ACK.
    let applied = [one.get_data().unwrap(), two.get_data().unwrap()];
    assert_eq!(applied[0].to_segments(), vec![vec![9]]);
    two.acknowledge().unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        copy.get_operation(Cancellation::default())
    )
    .await
    .is_err());
    assert!(!secondary.fabric.snapshot().copy_done);
    one.acknowledge().unwrap();
    until(|| secondary.fabric.snapshot().copy_done).await;
    assert!(copy
        .get_operation(Cancellation::default())
        .await
        .unwrap()
        .is_none());
    assert!(secondary.fabric.snapshot().fault.is_none());
    // Empty replication is still a real metadata-bearing operation. Receiving,
    // cancellation and dropping one Arc do not ACK; terminal ownership fences
    // the first actual ACK from the retained application capture.
    send(Body::Replication {
        primary_epoch: EPOCH,
        operation_epoch: EPOCH,
        operations: vec![WireOperation {
            sequence: 1,
            kind: 1,
            atomic_group: -1,
            data: vec![],
        }],
        batch_end: 1,
        completed: 0,
    });
    let replication: Arc<dyn IOperationStream> = state.get_replication_stream().unwrap();
    let canceled = Cancellation::default();
    canceled.cancel();
    assert!(replication.get_operation(canceled).await.is_err());
    let operation = next(&*replication).await;
    metadata(&*operation, 1);
    assert_eq!(operation.get_data().unwrap().bytes(), 0);
    let retained = operation.clone();
    drop(operation);
    assert_eq!(secondary.fabric.snapshot().progress.applied, 0);
    secondary
        .fabric
        .close(Cancellation::default())
        .await
        .unwrap();
    assert!(
        retained.acknowledge().is_err(),
        "late ACK cannot revive a retired owner"
    );
    drop(retained);
    network.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn combined_native_bound_build_remove_and_full_low_bit_epoch_controls_work() {
    let partition = uuid::Uuid::new_v4();
    let primary = Fixture::new(partition, 1, ReplicaRole::Primary).await;
    primary.host.configure(config(vec![], 1)).await.unwrap();
    let idle = Fixture::new(partition, 2, ReplicaRole::IdleSecondary).await;
    let copy: Arc<dyn IOperationStream> = idle
        .fabric
        .get_state_replicator()
        .get_copy_stream()
        .unwrap();
    let target = idle.descriptor(ReplicaRole::IdleSecondary);
    let node = primary.host.clone();
    let build_target = target.clone();
    let build =
        tokio::spawn(async move { node.build(build_target, Cancellation::default()).await });
    for sequence in 1..=2 {
        let operation = next(&*copy).await;
        metadata(&*operation, sequence);
        idle.provider
            .memory
            .apply_copy(&operation.get_data().unwrap())
            .unwrap();
        operation.acknowledge().unwrap();
    }

    build.await.unwrap().unwrap();
    assert!(copy
        .get_operation(Cancellation::default())
        .await
        .unwrap()
        .is_none());
    idle.host.get_status().await.unwrap();
    assert_eq!(
        idle.native.status().unwrap().progress.last,
        0,
        "zero is valid only after the actual empty copy initialized/completed the queue"
    );
    primary.host.remove(target).await.unwrap();
    assert!(primary.fabric.snapshot().peers.is_empty());
    let full = Epoch {
        configuration_number: EPOCH.configuration_number + 7,
        ..EPOCH
    };
    primary.host.change_epoch(full).await.unwrap();
    assert_eq!(primary.native.status().unwrap().current_epoch, full);
    assert_eq!(primary.fabric.snapshot().epoch, Some(EPOCH.primary()));
    primary.fabric.close(Cancellation::default()).await.unwrap();
    idle.fabric.close(Cancellation::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returned_composite_build_idle_remove_and_native_retry_cleanup_are_once_only() {
    use std::sync::atomic::Ordering;
    let partition = uuid::Uuid::new_v4();
    let primary = Fixture::new(partition, 1, ReplicaRole::Primary).await;
    primary.host.configure(config(vec![], 1)).await.unwrap();
    let idle = Fixture::new(partition, 2, ReplicaRole::IdleSecondary).await;
    let copy = idle
        .fabric
        .get_state_replicator()
        .get_copy_stream()
        .unwrap();
    let target = idle.descriptor(ReplicaRole::IdleSecondary);
    primary
        .controls
        .fail_after_build
        .store(true, Ordering::Release);
    let node = primary.host.clone();
    let build_target = target.clone();
    let first =
        tokio::spawn(async move { node.build(build_target, Cancellation::default()).await });
    for _ in 0..2 {
        let operation = next(&*copy).await;
        idle.provider
            .memory
            .apply_copy(&operation.get_data().unwrap())
            .unwrap();
        operation.acknowledge().unwrap();
    }
    assert!(
        first.await.unwrap().is_err(),
        "actual completed lower result was deliberately rejected"
    );
    assert!(copy
        .get_operation(Cancellation::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(primary.controls.builds.load(Ordering::Acquire), 1);
    primary
        .host
        .build(target.clone(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(primary.controls.builds.load(Ordering::Acquire), 2);
    {
        let removals = primary.controls.removals.lock().unwrap();
        assert_eq!(removals.len(), 1);
        assert_eq!(
            removals[0].kind(),
            NativeInvocationKind::Remove(
                fube_runtime::extensions::ReplicaRemoval::BuildRetryCleanup
            )
        );
    }
    primary.host.remove(target).await.unwrap();
    let removals = primary.controls.removals.lock().unwrap();
    assert_eq!(removals.len(), 2);
    assert_eq!(
        removals[1].kind(),
        NativeInvocationKind::Remove(fube_runtime::extensions::ReplicaRemoval::Idle)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secondary_progress_while_copy_is_incomplete_reports_not_ready_after_queue_creation() {
    let partition = uuid::Uuid::new_v4();
    let primary = Fixture::new(partition, 1, ReplicaRole::Primary).await;
    primary.host.configure(config(vec![], 1)).await.unwrap();
    let idle = Fixture::new(partition, 2, ReplicaRole::IdleSecondary).await;
    let stream = idle
        .fabric
        .get_state_replicator()
        .get_copy_stream()
        .unwrap();
    let host = primary.host.clone();
    let target = idle.descriptor(ReplicaRole::IdleSecondary);
    let build = tokio::spawn(async move { host.build(target, Cancellation::default()).await });
    let first = next(&*stream).await;
    idle.provider
        .memory
        .apply_copy(&first.get_data().unwrap())
        .unwrap();
    let status = idle.host.get_status().await;
    first.acknowledge().unwrap();
    let end = next(&*stream).await;
    idle.provider
        .memory
        .apply_copy(&end.get_data().unwrap())
        .unwrap();
    end.acknowledge().unwrap();
    build.await.unwrap().unwrap();
    assert_eq!(
        status.unwrap_err().native_code(),
        Some(0x80071bc7u32 as i32 as i64)
    );
    assert!(stream
        .get_operation(Cancellation::default())
        .await
        .unwrap()
        .is_none());
}
