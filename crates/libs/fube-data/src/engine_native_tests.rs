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
    provider: Arc<dyn StateProvider>,
    settings: Settings,
    created: Created,
}
struct Replica {
    native: NativeRuntimeIntegration,
    provider: Arc<dyn StateProvider>,
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
            let id = self.native.identity();
            let engine = Engine::bind_partition(
                partition,
                self.native.clone(),
                Actor {
                    partition: id.partition_id,
                    replica: id.replica_id,
                    incarnation: uuid::Uuid::new_v4(),
                },
                "127.0.0.1:0",
                self.provider.clone(),
                self.settings.clone(),
            )?;
            if let Some(send) = self.created.lock().unwrap().take() {
                let _ = send.send((engine.clone(), self.native.clone()));
            }
            Ok(engine.get_primary_replicator())
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
        storage: (Arc<MemoryProvider>, Arc<dyn StateProvider>, Settings),
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
