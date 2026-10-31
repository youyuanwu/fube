//! Native-RAP MODEL ingress exercises the actual issued root/bound SDK paths.
use super::*;
use crate::{MemoryProvider, Mutation};
use fube_runtime::extensions::{NativeIntegrationProvider, NativeRuntimeIntegration};
use fube_runtime::{
    IStatefulServiceFactory, IStatefulServicePartition, IStatefulServiceReplica, OpenMode,
};
#[path = "engine_native_disposal_tests.rs"]
mod disposal;
use crate::qualification as rap;

type Created = Arc<Mutex<Option<oneshot::Sender<(Arc<Engine>, NativeRuntimeIntegration)>>>>;
struct Factory {
    native: NativeIntegrationProvider,
    provider: Arc<dyn IStateProvider>,
    settings: Settings,
    created: Created,
}
struct Replica {
    native: NativeRuntimeIntegration,
    provider: Arc<dyn IStateProvider>,
    settings: Settings,
    created: Created,
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
        }))
    }
}
impl IStatefulServiceReplica for Replica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ServiceFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
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
        let (provider, supplied, settings) = storage;
        let (send, receive) = oneshot::channel();
        let standard = source == self::source(source.replica_id);
        let factory = move |native| -> Box<dyn IStatefulServiceFactory> {
            Box::new(Factory {
                native,
                provider: supplied,
                settings,
                created: Arc::new(Mutex::new(Some(send))),
            })
        };
        let host = if standard {
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
        let proof = primary
            .native
            .capture_configuration(&current.sdk().unwrap(), Some(&previous.sdk().unwrap()))
            .unwrap();
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
    // RAP legitimately suppresses a duplicate before calling lower. Verify
    // that optimization, then exercise the canonical lower method itself.
    s.host.change_epoch(EPOCH).await.unwrap();
    assert_eq!(
        s.engine.shared.epoch_entries.load(Ordering::Acquire),
        entries
    );
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
        let replacement =
            Fixture::new(partition, replacement, ReplicaRole::IdleSecondary, false).await;
        replacement.host.change_epoch(NEXT).await.unwrap();
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
