//! Public-only, offline native-RAP model witnesses, not live qualification.
use fube_data::{
    extensions::{
        EpochDomains, OperationOriginStream, ReplayCheckpointProvider, ReplicationEndpointOptions,
        ReplicatorDiagnostics,
    },
    Cancellation, Epoch, FabricReplicator, IOperation, IOperationDataStream, IOperationStream,
    IStateProvider, IStateReplicator, MemoryProvider, Mutation, OperationData, OperationType,
    ReplicationAdmission, Result, Settings,
};
use fube_runtime::{
    extensions::{
        NativeIntegrationProvider, NativeReplicaDescriptor, NativeReplicaSetConfiguration,
        NativeRuntimeIntegration, ReplicationProgress,
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
}
impl StandardOnly {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            memory: MemoryProvider::new(),
            copies: Mutex::new(vec![]),
            epochs: Mutex::new(vec![]),
        })
    }
}
impl IStateProvider for StandardOnly {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.memory.get_last_committed_sequence_number()
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, cancel: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
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
struct Factory {
    native: NativeIntegrationProvider,
    provider: Arc<dyn IStateProvider>,
    replay: Option<Arc<dyn ReplayCheckpointProvider>>,
    created: Created,
}
struct Replica {
    native: NativeRuntimeIntegration,
    provider: Arc<dyn IStateProvider>,
    replay: Option<Arc<dyn ReplayCheckpointProvider>>,
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
            replay: self.replay.clone(),
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
            let fabric = FabricReplicator::create(
                partition,
                self.native.clone(),
                self.provider.clone(),
                self.replay.clone(),
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                Settings {
                    operation_bytes: 4096,
                    ..Settings::default()
                },
            )?;
            if let Some(send) = self.created.lock().unwrap().take() {
                let _ = send.send((fabric.clone(), self.native.clone()));
            }
            // The service retains data access while returning canonical control.
            Ok(Box::new((*fabric).clone()) as Box<dyn IPrimaryReplicator>)
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
}
impl Fixture {
    async fn new(partition: uuid::Uuid, replica: i64, role: ReplicaRole) -> Self {
        let provider = StandardOnly::new();
        let supplied = provider.clone();
        let (send, receive) = oneshot::channel();
        let host = rap::Host::start(partition, replica, role, move |native| {
            Box::new(Factory {
                native,
                provider: supplied,
                replay: None,
                created: Arc::new(Mutex::new(Some(send))),
            })
        })
        .await;
        let (fabric, native) = receive.await.unwrap();
        Self {
            fabric,
            native,
            provider,
            host,
        }
    }
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
    primary
        .fabric
        .execution_runtime()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let result: Result<()> = root.update_current_replica_set_configuration(current);
            result
        })
        .await
        .unwrap();
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
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
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
