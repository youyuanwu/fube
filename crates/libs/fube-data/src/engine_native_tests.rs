//! Native-RAP MODEL ingress exercises the actual issued root/bound SDK paths.
use super::*;
use crate::{MemoryProvider, Mutation};
use fube_runtime::extensions::{NativeIntegrationProvider, NativeRuntimeIntegration};
use fube_runtime::{
    IStatefulServiceFactory, IStatefulServicePartition, IStatefulServiceReplica, OpenMode,
};
#[path = "engine_native_disposal_tests.rs"]
mod disposal;
use fube_rap_test_support as rap;

type Created = Arc<Mutex<Option<oneshot::Sender<(Arc<Engine>, NativeRuntimeIntegration)>>>>;
struct Factory {
    native: NativeIntegrationProvider,
    provider: Arc<dyn IStateProvider>,
    settings: Settings,
    created: Created,
    opening_cancel: Arc<Mutex<Option<Cancellation>>>,
    sample_open_progress: bool,
    retry_open_progress: bool,
}
struct Replica {
    native: NativeRuntimeIntegration,
    provider: Arc<dyn IStateProvider>,
    settings: Settings,
    created: Created,
    opening_cancel: Arc<Mutex<Option<Cancellation>>>,
    sample_open_progress: bool,
    retry_open_progress: bool,
}
struct OpenedSampler(Arc<crate::FabricReplicator>, bool);
impl IReplicator for OpenedSampler {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            let address = self.0.open(cancel).await?;
            let progress = self.0.get_current_progress();
            if progress.is_err() && self.1 {
                self.0.get_current_progress()?;
            } else {
                progress?;
            }
            Ok(address)
        })
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.close(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.update_epoch(epoch, cancel)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.0.get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.0.get_catch_up_capability()
    }
    fn abort(&self) {
        self.0.abort();
    }
}
impl IPrimaryReplicator for OpenedSampler {
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.0.on_data_loss(cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        cc: ReplicaSetConfig,
        pc: ReplicaSetConfig,
    ) -> Result<()> {
        self.0.update_catch_up_replica_set_configuration(cc, pc)
    }
    fn update_current_replica_set_configuration(&self, cc: ReplicaSetConfig) -> Result<()> {
        self.0.update_current_replica_set_configuration(cc)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.wait_for_catch_up_quorum(mode, cancel)
    }
    fn build_replica(
        &self,
        replica: SdkReplicaInformation,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.build_replica(replica, cancel)
    }
    fn remove_replica(&self, replica: i64) -> Result<()> {
        self.0.remove_replica(replica)
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
            provider: self.provider.clone(),
            settings: self.settings.clone(),
            created: self.created.clone(),
            opening_cancel: self.opening_cancel.clone(),
            sample_open_progress: self.sample_open_progress,
            retry_open_progress: self.retry_open_progress,
        }))
    }
}
impl IStatefulServiceReplica for Replica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
            *self.opening_cancel.lock().unwrap() = Some(cancellation);
            let fabric = crate::FabricReplicator::create(
                partition,
                self.native.clone(),
                self.provider.clone(),
                None,
                crate::extensions::ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                self.settings.clone(),
            )?;
            if let Some(send) = self.created.lock().unwrap().take() {
                let _ = send.send((fabric.engine.clone(), self.native.clone()));
            }
            if self.sample_open_progress {
                return Ok(Box::new(OpenedSampler(fabric, self.retry_open_progress))
                    as Box<dyn IPrimaryReplicator>);
            }
            Ok(Box::new((*fabric).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async { Ok("model://native-controls".into()) })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Fixture {
    engine: Arc<Engine>,
    provider: Arc<MemoryProvider>,
    native: NativeRuntimeIntegration,
    host: Arc<rap::Host>,
}
const EPOCH: Epoch = Epoch {
    data_loss_number: 1,
    configuration_number: 1 << 32,
};
const NEXT: Epoch = Epoch {
    data_loss_number: 1,
    configuration_number: 2 << 32,
};
impl Fixture {
    async fn new(
        partition: uuid::Uuid,
        source: rap::ReplicaSource,
        role: ReplicaRole,
        root: bool,
    ) -> Self {
        let provider = MemoryProvider::new();
        Self::using(
            partition,
            source,
            role,
            root,
            (provider.clone(), provider, Settings::default()),
        )
        .await
    }
    async fn using(
        partition: uuid::Uuid,
        source: rap::ReplicaSource,
        role: ReplicaRole,
        root: bool,
        storage: (Arc<MemoryProvider>, Arc<dyn IStateProvider>, Settings),
    ) -> Self {
        Self::using_at_epoch(partition, source, role, root, storage, EPOCH).await
    }
    async fn using_at_epoch(
        partition: uuid::Uuid,
        source: rap::ReplicaSource,
        role: ReplicaRole,
        root: bool,
        storage: (Arc<MemoryProvider>, Arc<dyn IStateProvider>, Settings),
        epoch: Epoch,
    ) -> Self {
        let (provider, supplied, settings) = storage;
        let (send, receive) = oneshot::channel();
        let standard = source == self::source(source.replica_id);
        let factory = move |native| -> Box<dyn IStatefulServiceFactory> {
            Box::new(Factory {
                native,
                provider: supplied,
                settings,
                created: Arc::new(Mutex::new(Some(send))),
                opening_cancel: Arc::new(Mutex::new(None)),
                sample_open_progress: false,
                retry_open_progress: false,
            })
        };
        let host = if epoch != EPOCH {
            rap::Host::start_source_at_epoch(partition, source, role, epoch, factory).await
        } else if standard {
            rap::Host::start(partition, source.replica_id, role, factory).await
        } else {
            rap::Host::start_source(partition, source, role, factory).await
        };
        let (engine, native) = receive.await.unwrap();
        engine.native_root_dispatch.store(root, Ordering::Release);
        Self {
            engine,
            provider,
            native,
            host,
        }
    }
    fn descriptor(&self, role: ReplicaRole) -> ReplicaInformation {
        let id = self.native.identity();
        ReplicaInformation {
            node_id: id.node_id,
            node_instance: id.node_instance,
            replica_id: id.replica_id,
            incarnation: id.incarnation,
            role,
            previous_role: ReplicaRole::None,
            up: true,
            ready: role == ReplicaRole::ActiveSecondary,
            address: self.engine.endpoint().to_string(),
            progress: ReplicationProgress {
                first: -1,
                last: -1,
            },
            must_catch_up: false,
        }
    }
    async fn write(&self, id: &str) {
        let data = Mutation {
            id: id.into(),
            key: id.into(),
            value: format!("value-{id}"),
        }
        .operation_for_epoch(self.engine.snapshot().epoch.unwrap())
        .unwrap();
        let mut accepted = self
            .engine
            .submit(data.clone(), Cancellation::default())
            .await
            .unwrap();
        self.provider
            .apply_replication(accepted.sequence, &data)
            .unwrap();
        accepted.wait(Cancellation::default()).await.unwrap();
    }
    fn pump(&self) -> tokio::task::JoinHandle<()> {
        let copy = self.engine.copy_stream();
        let replication = self.engine.replication_stream();
        let provider = self.provider.clone();
        tokio::spawn(async move {
            while let Some(operation) = copy.next(Cancellation::default()).await.unwrap() {
                provider.apply_copy(&operation.data).unwrap();
                operation.acknowledge().unwrap();
            }
            while let Some(operation) = replication.next(Cancellation::default()).await.unwrap() {
                provider
                    .apply_replication(operation.sequence, &operation.data)
                    .unwrap();
                operation.acknowledge().unwrap();
            }
        })
    }
}
fn source(replica: i64) -> rap::ReplicaSource {
    rap::ReplicaSource {
        replica_id: replica,
        native_incarnation: replica + 10,
        node_id: [replica as u8; 16],
        node_instance: 1,
    }
}
fn config(
    epoch: Epoch,
    replicas: Vec<ReplicaInformation>,
    quorum: usize,
) -> ReplicaSetConfiguration {
    ReplicaSetConfiguration {
        epoch,
        replicas,
        write_quorum: quorum,
        local_member: true,
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_reserved_deadline_rechecked_after_contended_write_lock() {
    use std::task::{Context, Poll, Waker};
    for root in [false, true] {
        let p = Fixture::using(
            uuid::Uuid::new_v4(),
            source(1),
            ReplicaRole::Primary,
            root,
            (
                MemoryProvider::new(),
                Arc::new(tests::Provider::with_initial(1)),
                Settings::default(),
            ),
        )
        .await;
        p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
        let (held, holding) = std_mpsc::sync_channel(1);
        let (release, resumed) = std_mpsc::sync_channel(1);
        let shared = p.engine.shared.clone();
        let lock = std::thread::spawn(move || {
            let _guard = shared.admission.lock().unwrap();
            held.send(()).unwrap();
            resumed
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
        });
        holding
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let (entered, prelock) = std_mpsc::sync_channel(1);
        *p.engine.shared.write_prelock.lock().unwrap() = Some(entered);
        // Controlled time boundary avoids expiring native host authority. The
        // same check also uses the real reserved-entry deadline in production.
        *p.engine.shared.write_deadline.lock().unwrap() =
            Some(std::time::Instant::now() + std::time::Duration::from_millis(50));
        let engine = p.engine.clone();
        let submit = std::thread::spawn(move || {
            let mut future = engine.submit(
                OperationData::new(vec![vec![1]]).unwrap(),
                Cancellation::default(),
            );
            match future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(Err(error)) => Some(error),
                _ => None,
            }
        });
        prelock
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        release.send(()).unwrap();
        lock.join().unwrap();
        let error = submit
            .join()
            .unwrap()
            .expect("expired write must not enqueue");
        assert_eq!(error.kind(), fube_runtime::ErrorKind::Timeout);
        assert_eq!(p.engine.snapshot().progress.assigned, 1);
        assert_eq!(p.engine.snapshot().owned_operations, 0);
        *p.engine.shared.write_deadline.lock().unwrap() = None;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrective_real_reserved_entry_deadline_branch_uses_current_authority() {
    let p = Fixture::using(
        uuid::Uuid::new_v4(),
        source(1),
        ReplicaRole::Primary,
        false,
        (
            MemoryProvider::new(),
            Arc::new(tests::Provider::with_initial(1)),
            Settings::default(),
        ),
    )
    .await;
    p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    let (held, holding) = std_mpsc::sync_channel(1);
    let (release, resumed) = std_mpsc::sync_channel(1);
    let shared = p.engine.shared.clone();
    let lock = std::thread::spawn(move || {
        let _guard = shared.admission.lock().unwrap();
        held.send(()).unwrap();
        resumed
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
    });
    holding
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let (entered, prelock) = std_mpsc::sync_channel(1);
    *p.engine.shared.write_prelock.lock().unwrap() = Some(entered);
    let permit = p
        .native
        .execution()
        .reserve(
            CallLane::Native,
            CallOrigin::default(),
            std::time::Duration::from_secs(2),
            false,
        )
        .unwrap();
    let engine = p.engine.clone();
    let runtime = p.native.execution().clone();
    let call = permit.run_future(async move {
        assert!(runtime.is_current_entry());
        assert!(runtime.blocking_deadline()? > std::time::Instant::now());
        engine
            .submit(
                OperationData::new(vec![vec![1]]).unwrap(),
                Cancellation::default(),
            )
            .await
            .map(|a| a.sequence)
    });
    prelock
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    // Exercise the REAL reserved-entry deadline predicate with controlled now,
    // not the synthetic deadline predicate. Hard authority expiry remains
    // separately covered by isolated runtime process-fence regressions.
    *p.engine.shared.write_now.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(10));
    release.send(()).unwrap();
    lock.join().unwrap();
    assert_eq!(
        call.await.unwrap_err().kind(),
        fube_runtime::ErrorKind::Timeout
    );
    *p.engine.shared.write_now.lock().unwrap() = None;
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
    assert_eq!(p.engine.snapshot().owned_operations, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fidelity_ordinary_queue_full_has_no_log_record_category_or_reserved_capacity() {
    use crate::IStateReplicator;
    for root in [false, true] {
        let settings = Settings {
            queue_operations: 4,
            operation_bytes: 4096,
            operation_segments: 64,
            queue_bytes: 32 * 1024,
            ..Settings::default()
        };
        let p = Fixture::using(
            uuid::Uuid::new_v4(),
            source(1),
            ReplicaRole::Primary,
            root,
            (
                MemoryProvider::new(),
                Arc::new(tests::Provider::with_initial(1)),
                settings.clone(),
            ),
        )
        .await;
        let lower = crate::FabricReplicator {
            engine: p.engine.clone(),
        };
        let data = OperationData::with_settings(vec![vec![1]], &settings).unwrap();
        let (network, _incoming) = PeerTransport::bind("127.0.0.1:0", settings.clone()).unwrap();
        let mut peer = p.descriptor(ReplicaRole::ActiveSecondary);
        peer.replica_id = 2;
        peer.incarnation = 12;
        peer.node_id = [2; 16];
        peer.address = Endpoint {
            actor: Actor {
                partition: p.native.identity().partition_id,
                replica: 2,
                incarnation: uuid::Uuid::new_v4(),
            },
            address: network.address().to_owned(),
            transport_id: Some(String::new()),
        }
        .to_string();
        peer.up = false;
        peer.ready = false;
        p.host
            .configure(config(EPOCH, vec![peer.clone()], 2))
            .await
            .unwrap();
        assert!(lower
            .replicate(data.clone(), Cancellation::default())
            .await
            .is_err());
        assert_eq!(p.engine.snapshot().progress.assigned, 1);
        // Native-issued available member permits admission but sends no service
        // ACK here: ordinary entries retain actual lower queue charges.
        peer.up = true;
        peer.ready = true;
        // Native re-created sender, not a reset of the old down sender's live
        // counters. The new issued incarnation samples truthful constructor tail.
        peer.incarnation += 1;
        peer.address = Endpoint {
            actor: Actor {
                partition: p.native.identity().partition_id,
                replica: 2,
                incarnation: uuid::Uuid::new_v4(),
            },
            address: network.address().to_owned(),
            transport_id: Some(String::new()),
        }
        .to_string();
        p.host
            .configure(config(EPOCH, vec![peer], 2))
            .await
            .unwrap();
        let first = lower
            .replicate(data.clone(), Cancellation::default())
            .await
            .unwrap();
        let second = lower
            .replicate(data.clone(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!((first.sequence_number, second.sequence_number), (2, 3));
        let third = lower
            .replicate(data.clone(), Cancellation::default())
            .await
            .unwrap();
        let fourth = lower
            .replicate(data.clone(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!((third.sequence_number, fourth.sequence_number), (4, 5));
        assert_eq!(p.engine.snapshot().owned_operations, 4);
        assert!(p.engine.snapshot().owned_bytes <= settings.queue_bytes);
        // V1 treats data as opaque; even a V2 type discriminator grants no
        // special admission. Valid txn End/Barrier tests live in fube-txn.
        for kind in [1i32, 3, 4] {
            let opaque =
                OperationData::with_settings(vec![kind.to_le_bytes().to_vec()], &settings).unwrap();
            let rejected = lower
                .replicate(opaque, Cancellation::default())
                .await
                .err()
                .unwrap();
            assert_eq!(
                rejected.kind(),
                fube_runtime::ErrorKind::ReplicationQueueFull
            );
            assert_eq!(p.engine.snapshot().progress.assigned, 5);
        }
        p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
        for mut accepted in [first, second, third, fourth] {
            accepted
                .completion
                .wait(Cancellation::default())
                .await
                .unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while p.engine.snapshot().owned_operations != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let recovered = lower
            .replicate(data, Cancellation::default())
            .await
            .unwrap();
        assert_eq!(recovered.sequence_number, 6);
        drop(recovered);
        network.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_lower_identity_survives_stop_before_deferred_reply_poll() {
    use crate::IStateReplicator;
    let p = Fixture::using(
        uuid::Uuid::new_v4(),
        source(1),
        ReplicaRole::Primary,
        false,
        (
            MemoryProvider::new(),
            Arc::new(tests::Provider::with_initial(1)),
            Settings::default(),
        ),
    )
    .await;
    p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    let (entered, published) = std_mpsc::sync_channel(1);
    let (release, resumed) = std_mpsc::sync_channel(1);
    *p.engine.shared.write_reply_pause.lock().unwrap() = Some((entered, resumed));
    let lower = crate::FabricReplicator {
        engine: p.engine.clone(),
    };
    let write = tokio::spawn(async move {
        lower
            .replicate(
                OperationData::new(vec![vec![1]]).unwrap(),
                Cancellation::default(),
            )
            .await
    });
    published
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let engine = p.engine.clone();
    let permit = p.native.execution().try_reserve().unwrap();
    let abort = permit.run_owned(move || {
        IReplicator::abort(&*engine);
        Ok(())
    });
    while !p.engine.shared.stopped.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    release.send(()).unwrap();
    let accepted = write
        .await
        .unwrap()
        .expect("published LSN must not become an admission rejection");
    abort.await.unwrap();
    assert_eq!(accepted.sequence_number, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issued_new_and_recreated_senders_use_current_progress_but_repeats_keep_live_counters() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let log = Arc::new(tests::Provider::with_initial(20));
        let p = Fixture::using(
            partition,
            source(1),
            ReplicaRole::Primary,
            root,
            (MemoryProvider::new(), log.clone(), Settings::default()),
        )
        .await;
        // A genuine runtime-issued descriptor, no replay extension and no
        // provider/remote ACK callback, represents an already applied prefix.
        let (network, _incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
        let remote = Endpoint {
            actor: Actor {
                partition,
                replica: 2,
                incarnation: uuid::Uuid::new_v4(),
            },
            address: network.address().to_string(),
            transport_id: None,
        };
        let mut descriptor = ReplicaInformation {
            node_id: source(2).node_id,
            node_instance: 1,
            replica_id: 2,
            incarnation: source(2).native_incarnation,
            role: ReplicaRole::ActiveSecondary,
            previous_role: ReplicaRole::ActiveSecondary,
            up: true,
            ready: true,
            address: remote.to_string(),
            progress: ReplicationProgress { first: 1, last: 20 },
            must_catch_up: false,
        };
        p.host
            .configure(config(EPOCH, vec![descriptor.clone()], 2))
            .await
            .unwrap();
        p.engine
            .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
            .await
            .unwrap();
        assert_eq!(
            (
                p.engine.snapshot().peers[0].received,
                p.engine.snapshot().peers[0].applied
            ),
            (20, 20)
        );
        descriptor.progress.last = 0;
        descriptor.progress.first = -1;
        p.host
            .configure(config(EPOCH, vec![descriptor.clone()], 2))
            .await
            .unwrap();
        assert_eq!(
            p.engine.snapshot().peers[0].applied,
            20,
            "repeated descriptor must not reset live sender"
        );
        descriptor.incarnation += 1;
        descriptor.address = Endpoint {
            actor: Actor {
                incarnation: uuid::Uuid::new_v4(),
                ..remote.actor
            },
            ..remote
        }
        .to_string();
        descriptor.progress = ReplicationProgress { first: 1, last: 20 };
        p.host
            .configure(config(EPOCH, vec![descriptor], 2))
            .await
            .unwrap();
        p.engine
            .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
            .await
            .unwrap();
        assert_eq!(p.engine.snapshot().peers[0].applied, 20);
        let mut write = p
            .engine
            .submit(
                OperationData::new(vec![vec![21]]).unwrap(),
                Cancellation::default(),
            )
            .await
            .unwrap();
        log.last.store(21, Ordering::Release);
        assert_eq!(write.sequence, 21);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(50),
            write.wait(Cancellation::default())
        )
        .await
        .is_err());
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(50),
            p.engine
                .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        )
        .await
        .is_err());
        assert_eq!(p.engine.snapshot().progress.committed, 20);
        network.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issued_root_and_bound_canonical_controls_copy_ack_memberships_remove_and_pair_progress() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let primary = Fixture::new(partition, source(1), ReplicaRole::Primary, root).await;
        primary
            .host
            .configure(config(EPOCH, vec![], 1))
            .await
            .unwrap();
        until(|| primary.engine.snapshot().ready).await;
        primary.write("first").await;
        let idle = Fixture::new(partition, source(2), ReplicaRole::IdleSecondary, root).await;
        let pump = idle.pump();
        let target = idle.descriptor(ReplicaRole::IdleSecondary);
        primary
            .host
            .build(target.clone(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!(
            idle.provider.snapshot().values,
            primary.provider.snapshot().values
        );
        assert_eq!(idle.provider.snapshot().last, 1);
        primary.host.remove(target).await.unwrap();
        assert!(primary.engine.snapshot().peers.is_empty());
        pump.abort();
        let _ = pump.await;
        drop(idle);
        let secondary = Fixture::new(partition, source(3), ReplicaRole::IdleSecondary, root).await;
        let pump = secondary.pump();
        primary
            .host
            .build(
                secondary.descriptor(ReplicaRole::IdleSecondary),
                Cancellation::default(),
            )
            .await
            .unwrap();
        secondary
            .host
            .change_role(ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        let full = Epoch {
            configuration_number: EPOCH.configuration_number + 7,
            ..EPOCH
        };
        let current = config(
            full,
            vec![secondary.descriptor(ReplicaRole::ActiveSecondary)],
            2,
        );
        let previous = config(EPOCH, vec![], 1);
        primary
            .host
            .catch_up(current.clone(), previous.clone())
            .await
            .unwrap();
        // Observe the exact proof actually consumed by canonical dispatch,
        // never reselect a latest matching ledger record after the call.
        let proof = primary
            .engine
            .native_configuration
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert!(proof.clone().enter().is_err());
        let (cc, pc) = proof.configuration().unwrap();
        assert_eq!((cc.write_quorum, cc.local_member), (2, true));
        assert_eq!(
            (pc.unwrap().write_quorum, pc.unwrap().local_member),
            (1, true)
        );
        assert_eq!(
            proof.full_epochs().0.configuration,
            full.configuration_number
        );
        assert_eq!(cc.replicas[0].last_lsn, -1);
        assert_eq!(
            cc.replicas[0].incarnation,
            secondary.native.identity().incarnation
        );
        primary.host.configure(current).await.unwrap();
        primary.write("second").await;
        until(|| secondary.provider.snapshot().last == 2).await;
        assert_eq!(
            secondary.provider.snapshot().values,
            primary.provider.snapshot().values
        );
        // The native publication used one private sample, while the public
        // interface remains two separate fallible scalar getters.
        let pair = primary.engine.progress().unwrap();
        assert_eq!(primary.engine.get_current_progress().unwrap(), pair.last);
        assert_eq!(
            primary.engine.get_catch_up_capability().unwrap(),
            pair.first
        );
        primary
            .engine
            .scalar_progress_reads
            .store(0, Ordering::SeqCst);
        primary.host.get_status().await.unwrap();
        assert_eq!(
            primary.engine.scalar_progress_reads.load(Ordering::SeqCst),
            0,
            "native status must not tear two public scalar reads"
        );
        assert_eq!(
            primary.native.status().unwrap().progress,
            primary.engine.progress().unwrap()
        );
        pump.abort();
        let _ = pump.await;
    }
}

struct InitialPublicationProvider {
    memory: Arc<MemoryProvider>,
    gate: Mutex<Option<(std_mpsc::SyncSender<()>, std_mpsc::Receiver<()>)>>,
    getters: AtomicU64,
    fail_next_getter: AtomicBool,
}
impl IStateProvider for InitialPublicationProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.getters.fetch_add(1, Ordering::AcqRel);
        let gate = self.gate.lock().unwrap().take();
        if let Some((entered, release)) = gate {
            let _ = entered.send(());
            release
                .recv_timeout(std::time::Duration::from_secs(2))
                .map_err(|_| Error::Timeout)?;
        }
        if self.fail_next_getter.swap(false, Ordering::AcqRel) {
            return Err(Error::InvalidInput("controlled ordinary getter failure"));
        }
        self.memory.get_last_committed_sequence_number()
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.memory.update_epoch(epoch, previous, cancel)
    }
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.memory.on_data_loss(cancel)
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        self.memory.get_copy_context()
    }
    fn get_copy_state(
        &self,
        horizon: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        self.memory.get_copy_state(horizon, context)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_progress_completed_callback_cannot_publish_after_native_cancel_or_deadline() {
    for (cancel, sample_open_progress) in [(false, false), (true, false), (true, true)] {
        let memory = MemoryProvider::new();
        for sequence in 1..=27 {
            memory
                .apply_replication(
                    sequence,
                    &Mutation {
                        id: format!("initial-{sequence}"),
                        key: format!("initial-{sequence}"),
                        value: "value".into(),
                    }
                    .operation()
                    .unwrap(),
                )
                .unwrap();
        }
        let (getter_entered, getter_wait) = std_mpsc::sync_channel(1);
        let (getter_release, getter_blocked) = std_mpsc::sync_channel(1);
        let provider = Arc::new(InitialPublicationProvider {
            memory: memory.clone(),
            gate: Mutex::new(Some((getter_entered, getter_blocked))),
            getters: AtomicU64::new(0),
            fail_next_getter: AtomicBool::new(false),
        });
        let supplied = provider.clone();
        let opening_cancel = Arc::new(Mutex::new(None));
        let captured_cancel = opening_cancel.clone();
        let (created, receive) = oneshot::channel();
        let settings = Settings {
            callback_timeout: if cancel {
                std::time::Duration::from_secs(2)
            } else {
                std::time::Duration::from_millis(300)
            },
            ..Settings::default()
        };
        let started = tokio::spawn(async move {
            rap::Host::start_source_outcome(
                uuid::Uuid::new_v4(),
                source(1),
                ReplicaRole::Primary,
                move |native| {
                    Box::new(Factory {
                        native,
                        provider: supplied,
                        settings,
                        created: Arc::new(Mutex::new(Some(created))),
                        opening_cancel: captured_cancel,
                        sample_open_progress,
                        retry_open_progress: false,
                    })
                },
            )
            .await
        });
        let (engine, _native) = receive.await.unwrap();
        getter_wait
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let (publication_entered, publication_wait) = std_mpsc::sync_channel(1);
        let (publication_release, publication_blocked) = std_mpsc::sync_channel(1);
        *engine.shared.initial_publication_pause.lock().unwrap() =
            Some((publication_entered, publication_blocked));
        getter_release.send(()).unwrap();
        publication_wait
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        if cancel {
            opening_cancel.lock().unwrap().as_ref().unwrap().cancel();
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        }
        publication_release.send(()).unwrap();
        let (host, opened) = started.await.unwrap();
        assert!(opened.is_err());
        assert!(engine.shared.initial_progress.lock().unwrap().is_none());
        assert_eq!(provider.getters.load(Ordering::Acquire), 1);
        drop(host);
        memory
            .apply_replication(
                28,
                &Mutation {
                    id: "new-initial".into(),
                    key: "new-initial".into(),
                    value: "value".into(),
                }
                .operation()
                .unwrap(),
            )
            .unwrap();
        let supplied = provider.clone();
        let (created, receive) = oneshot::channel();
        let host = rap::Host::start(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            move |native| {
                Box::new(Factory {
                    native,
                    provider: supplied,
                    settings: Settings::default(),
                    created: Arc::new(Mutex::new(Some(created))),
                    opening_cancel: Arc::new(Mutex::new(None)),
                    sample_open_progress: false,
                    retry_open_progress: false,
                })
            },
        )
        .await;
        let (fresh, _) = receive.await.unwrap();
        assert_eq!(provider.getters.load(Ordering::Acquire), 2);
        assert_eq!(fresh.snapshot().progress.assigned, 28);
        drop(host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_coalesced_primary_does_not_fault_valid_opened_getter_retry() {
    let memory = MemoryProvider::new();
    memory
        .apply_replication(
            1,
            &Mutation {
                id: "shared".into(),
                key: "shared".into(),
                value: "value".into(),
            }
            .operation()
            .unwrap(),
        )
        .unwrap();
    let (getter_entered, getter_wait) = std_mpsc::sync_channel(1);
    let (getter_release, getter_blocked) = std_mpsc::sync_channel(1);
    let provider = Arc::new(InitialPublicationProvider {
        memory,
        gate: Mutex::new(Some((getter_entered, getter_blocked))),
        getters: AtomicU64::new(0),
        fail_next_getter: AtomicBool::new(true),
    });
    let supplied = provider.clone();
    let (created, receive) = oneshot::channel();
    let started = tokio::spawn(async move {
        rap::Host::start_source_outcome(
            uuid::Uuid::new_v4(),
            source(1),
            ReplicaRole::Primary,
            move |native| {
                Box::new(Factory {
                    native,
                    provider: supplied,
                    settings: Settings::default(),
                    created: Arc::new(Mutex::new(Some(created))),
                    opening_cancel: Arc::new(Mutex::new(None)),
                    sample_open_progress: true,
                    retry_open_progress: true,
                })
            },
        )
        .await
    });
    let (engine, native) = receive.await.unwrap();
    getter_wait
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let (primary_entered, primary_wait) = std_mpsc::sync_channel(1);
    *engine.shared.initial_coalesced_primary.lock().unwrap() = Some(primary_entered);
    let cancelled = Cancellation::default();
    let supplied_cancel = cancelled.clone();
    let lower = engine.clone();
    let primary = native
        .execution()
        .try_reserve()
        .unwrap()
        .run_future(async move {
            IReplicator::change_role(&*lower, EPOCH, ReplicaRole::Primary, supplied_cancel).await
        });
    primary_wait
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    cancelled.cancel();
    getter_release.send(()).unwrap();
    let primary_result = primary.await;
    let (host, opened) = started.await.unwrap();
    assert_eq!(
        primary_result.unwrap_err().kind(),
        fube_runtime::ErrorKind::ObjectClosed
    );
    assert!(
        opened.is_ok(),
        "valid ordinary getter retry was terminally faulted: {opened:?}"
    );
    assert_eq!(provider.getters.load(Ordering::Acquire), 2);
    assert_eq!(engine.snapshot().progress.assigned, 1);
    assert!(engine.snapshot().fault.is_none());
    drop(host);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_native_bound_duplicate_secondary_epoch_enters_lower_while_copy_ack_is_pending() {
    let partition = uuid::Uuid::new_v4();
    let source = tests::upper_log::UpperLog::new(EPOCH, 100);
    let upper = tests::upper_log::UpperLog::new(EPOCH, 1);
    let p = Fixture::using(
        partition,
        self::source(1),
        ReplicaRole::Primary,
        false,
        (MemoryProvider::new(), source.clone(), Settings::default()),
    )
    .await;
    let s = Fixture::using(
        partition,
        self::source(2),
        ReplicaRole::IdleSecondary,
        false,
        (MemoryProvider::new(), upper.clone(), Settings::default()),
    )
    .await;
    p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    let build = p.host.build(
        s.descriptor(ReplicaRole::IdleSecondary),
        Cancellation::default(),
    );
    tokio::pin!(build);
    let stream = s.engine.copy_stream();
    let operation = tokio::select! {
        result = &mut build => panic!("build before actual copy apply: {result:?}"),
        operation = stream.next(Cancellation::default()) => operation.unwrap().unwrap(),
    };
    upper.apply_snapshot(&operation.data);
    let updates = upper.updates.load(Ordering::Acquire);
    let getters = upper.getters.load(Ordering::Acquire);
    assert!(!s.engine.snapshot().copy_done);
    let entries = s.engine.shared.epoch_entries.load(Ordering::Acquire);
    // The bare SDK UpdateEpoch is valid during copy. RAP's combined
    // UpdateEpochAndGetStatus must wait for real copy readiness: its status
    // getter cannot report a fictitious initialized zero during this gate.
    let permit = s
        .native
        .execution()
        .reserve(
            CallLane::Native,
            CallOrigin {
                engine: 0,
                generation: s.native.binding().generation,
                session: 0,
            },
            std::time::Duration::from_secs(2),
            false,
        )
        .unwrap();
    let engine = s.engine.clone();
    permit
        .run_future(async move {
            IReplicator::update_epoch(&*engine, EPOCH, Cancellation::default()).await
        })
        .await
        .unwrap();
    assert_eq!(
        s.engine.shared.epoch_entries.load(Ordering::Acquire),
        entries + 1
    );
    assert_eq!(upper.updates.load(Ordering::Acquire), updates);
    assert_eq!(upper.getters.load(Ordering::Acquire), getters);
    assert_eq!(upper.tail(), 100);
    operation.acknowledge().unwrap();
    drop(operation);
    build.await.unwrap();
    s.host.change_epoch(EPOCH).await.unwrap();
    assert_eq!(
        s.engine.shared.epoch_entries.load(Ordering::Acquire),
        entries + 1,
        "RAP suppressed the duplicate while obtaining actual completed-copy status"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_lower_secondary_controls_use_traffic_admission_epoch_without_provider_slots() {
    for start_newer in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let upper = tests::upper_log::UpperLog::new(EPOCH, 1);
        let s = Fixture::using(
            partition,
            source(2),
            ReplicaRole::IdleSecondary,
            false,
            (MemoryProvider::new(), upper.clone(), Settings::default()),
        )
        .await;
        let (network, _incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
        network.register_peer(&s.engine.endpoint()).unwrap();
        let primary = Endpoint {
            address: network.address().to_owned(),
            actor: Actor {
                partition,
                replica: 1,
                incarnation: uuid::Uuid::new_v4(),
            },
            transport_id: None,
        };
        let send = |body| {
            network
                .send(Packet {
                    from: primary.clone(),
                    to: s.engine.endpoint().actor,
                    body,
                })
                .unwrap();
        };
        send(Body::StartCopy {
            epoch: if start_newer { NEXT } else { EPOCH },
            target: 2,
            start: 1,
        });
        until(|| s.engine.snapshot().observed_primary.is_some()).await;
        if !start_newer {
            send(Body::Replication {
                primary_epoch: NEXT,
                operation_epoch: EPOCH,
                operations: vec![WireOperation {
                    sequence: 1,
                    kind: 1,
                    atomic_group: -1,
                    data: vec![vec![1]],
                }],
                batch_end: 1,
                completed: 0,
            });
            until(|| s.engine.snapshot().progress.received == 1).await;
        }
        assert!(!s.engine.snapshot().copy_done);
        let getters = upper.getters.load(Ordering::Acquire);
        let updates = upper.updates.load(Ordering::Acquire);
        let entries = s.engine.shared.epoch_entries.load(Ordering::Acquire);
        let execution = s.native.execution();
        let call = execution
            .reserve(
                CallLane::Native,
                CallOrigin {
                    engine: 0,
                    generation: s.native.binding().generation,
                    session: 0,
                },
                std::time::Duration::from_secs(2),
                false,
            )
            .unwrap();
        // One native entry and 63 provider reservations exhaust the shared
        // record budget. Neither lower control below invokes application code.
        let slots: Vec<_> = (0..63)
            .map(|_| {
                execution
                    .reserve(
                        CallLane::Provider,
                        CallOrigin::default(),
                        std::time::Duration::from_secs(2),
                        false,
                    )
                    .unwrap()
            })
            .collect();
        assert!(execution
            .reserve(
                CallLane::Provider,
                CallOrigin::default(),
                std::time::Duration::from_secs(2),
                false,
            )
            .is_err());
        let engine = s.engine.clone();
        call.run_future(async move {
            for epoch in [NEXT, EPOCH] {
                let result =
                    IReplicator::update_epoch(&*engine, epoch, Cancellation::default()).await;
                if epoch == NEXT {
                    result.unwrap();
                } else {
                    assert!(matches!(result, Err(Error::InvalidInput("stale epoch"))));
                }
            }
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            s.engine.shared.epoch_entries.load(Ordering::Acquire),
            entries + 2
        );
        assert_eq!(upper.getters.load(Ordering::Acquire), getters);
        assert_eq!(upper.updates.load(Ordering::Acquire), updates);
        assert!(!s.engine.snapshot().copy_done);
        assert!(s.engine.snapshot().fault.is_none());
        drop(slots);
        network.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captured_unpolled_build_is_revoked_never_retargets_replacement_native_node_or_actor() {
    for variant in 0..4 {
        let partition = uuid::Uuid::new_v4();
        let primary = Fixture::new(partition, source(1), ReplicaRole::Primary, true).await;
        primary
            .host
            .configure(config(EPOCH, vec![], 1))
            .await
            .unwrap();
        let old = Fixture::new(partition, source(2), ReplicaRole::IdleSecondary, false).await;
        let original = old.descriptor(ReplicaRole::IdleSecondary);
        let (captured, receive) = oneshot::channel();
        let (release, blocked) = oneshot::channel();
        *primary.engine.native_build_pause.lock().unwrap() = Some((captured, blocked));
        let node = primary.host.clone();
        let input = original.clone();
        let first = tokio::spawn(async move { node.build(input, Cancellation::default()).await });
        let proof = receive.await.unwrap(); // method entry, before owner admission / first inner poll
        assert!(primary.engine.snapshot().peers.is_empty());
        assert_eq!(proof.replica().unwrap().address, original.address);
        let mut epoch = Box::pin(primary.host.change_epoch(NEXT));
        tokio::select! {
            result = &mut epoch => { result.unwrap(); },
            _ = until(|| proof.validate().is_err()) => {},
        }
        assert!(proof.validate().is_err());
        release.send(()).unwrap();
        epoch.await.unwrap();
        first.abort();
        let _ = first.await;
        assert!(
            primary.engine.snapshot().peers.is_empty(),
            "old Build must not reach owner admission"
        );
        assert_eq!(proof.replica().unwrap().address, original.address);
        assert_eq!(
            proof.full_epochs().0.configuration,
            EPOCH.configuration_number
        );
        drop(old);
        let mut replacement = source(2);
        if variant != 0 {
            replacement.native_incarnation += 1;
        }
        if variant == 2 {
            replacement.node_instance += 1;
        }
        if variant == 3 {
            replacement.node_id = [9; 16];
        }
        let memory = MemoryProvider::new();
        let replacement = Fixture::using_at_epoch(
            partition,
            replacement,
            ReplicaRole::IdleSecondary,
            false,
            (memory.clone(), memory, Settings::default()),
            NEXT,
        )
        .await;
        primary
            .host
            .configure(config(NEXT, vec![], 1))
            .await
            .unwrap();
        let pump = replacement.pump();
        let fresh = replacement.descriptor(ReplicaRole::IdleSecondary);
        assert_ne!(fresh.address, original.address);
        primary
            .host
            .build(fresh, Cancellation::default())
            .await
            .unwrap();
        let peers = primary.engine.snapshot().peers;
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0].incarnation,
            replacement.engine.endpoint().actor.incarnation.to_string()
        );
        assert!(proof.validate().is_err());
        pump.abort();
        let _ = pump.await;
    }
}
