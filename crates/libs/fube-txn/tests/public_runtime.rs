//! External SDK behavioral witness over registered, native-issued in-process
//! lower integration. No private kernels/PointProvider; no native binary claim.
use fube_data::{extensions::ReplicationEndpointOptions, Settings};
use fube_rap_test_support as rap;
use fube_runtime::{
    extensions::{
        NativeIntegrationProvider, NativeReplicaDescriptor, NativeReplicaSetConfiguration,
        NativeRuntimeIntegration, ReplicationProgress,
    },
    IPrimaryReplicator, IStatefulServiceFactory, IStatefulServiceReplica, OpenMode,
};
use fube_txn::*;
use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::oneshot;

type Events = Arc<Mutex<Vec<String>>>;
#[derive(Default)]
struct Providers {
    events: Events,
    objects: Mutex<BTreeMap<String, Arc<Provider>>>,
}
struct Provider {
    id: i64,
    name: String,
    events: Events,
    state: Mutex<Vec<u8>>,
    prepared: Mutex<Vec<u8>>,
    completed: Mutex<Vec<u8>>,
}
impl Provider {
    fn event(&self, event: impl AsRef<str>) {
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.name, event.as_ref()));
    }
}
impl IStateProvider2Factory for Providers {
    fn create(&self, args: &FactoryArguments) -> Result<Arc<dyn IStateProvider2>> {
        assert!(args.state_provider_id > 0);
        assert_eq!(args.type_name, "byte-fixture");
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:factory", args.name));
        let provider = Arc::new(Provider {
            id: args.state_provider_id,
            name: args.name.clone(),
            events: self.events.clone(),
            state: Mutex::new(vec![]),
            prepared: Mutex::new(vec![]),
            completed: Mutex::new(vec![]),
        });
        self.objects
            .lock()
            .unwrap()
            .insert(args.name.clone(), provider.clone());
        Ok(provider)
    }
}
struct Stream {
    queue: Mutex<VecDeque<OperationData>>,
    events: Events,
    name: String,
}
impl IOperationDataStream for Stream {
    fn get_next(&self, _: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            let result = self.queue.lock().unwrap().pop_front();
            self.events.lock().unwrap().push(format!(
                "{}:stream-{}",
                self.name,
                if result.is_some() { "data" } else { "eos" }
            ));
            Ok(result)
        })
    }
    fn dispose(&self) -> Result<()> {
        self.queue.lock().unwrap().clear();
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:stream-dispose", self.name));
        Ok(())
    }
}
struct Capture(i64);
impl OperationContext for Capture {}
macro_rules! callback {
    ($method:ident, $event:expr) => {
        fn $method(&self, _: Cancellation) -> ApiFuture<'_, ()> {
            Box::pin(async move {
                self.event($event);
                Ok(())
            })
        }
    };
}
impl IStateProvider2 for Provider {
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_children(&self, _: &str) -> Result<Vec<StateProviderInfo>> {
        Ok(vec![])
    }
    fn initialize(
        &self,
        owner: Weak<dyn ITransactionalReplicator>,
        _: &Path,
        children: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()> {
        assert!(owner.upgrade().is_some());
        assert!(children.is_none());
        self.event("initialize");
        Ok(())
    }
    callback!(open, "open");
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event(format!("role-{role:?}"));
            Ok(())
        })
    }
    callback!(close, "close");
    fn abort(&self) {
        self.event("abort");
    }
    fn prepare_checkpoint(&self, _: i64) -> Result<()> {
        *self.prepared.lock().unwrap() = self.state.lock().unwrap().clone();
        self.event("prepare");
        Ok(())
    }
    callback!(perform_checkpoint, "perform");
    fn complete_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            *self.completed.lock().unwrap() = self.prepared.lock().unwrap().clone();
            self.event("complete");
            Ok(())
        })
    }
    callback!(recover_checkpoint, "recover");
    fn backup_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::InvalidInput("fixture backup unsupported")) })
    }
    fn restore_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::InvalidInput("fixture restore unsupported")) })
    }
    callback!(remove_state, "remove");
    fn get_current_state(&self) -> Result<Option<Arc<dyn IOperationDataStream>>> {
        self.event("get-copy");
        if self.name.ends_with("/absent") {
            return Ok(None);
        }
        Ok(Some(Arc::new(Stream {
            // Some(empty) is DATA, not None stream or EOS.
            queue: Mutex::new(VecDeque::from([
                OperationData::new(vec![])?,
                OperationData::new(vec![self.completed.lock().unwrap().clone()])?,
            ])),
            events: self.events.clone(),
            name: self.name.clone(),
        })))
    }
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.state.lock().unwrap().clear();
            self.event("begin");
            Ok(())
        })
    }
    fn set_current_state(
        &self,
        ordinal: i64,
        data: OperationData,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event(format!("set-{ordinal}-{}", data.bytes()));
            if data.bytes() > 0 {
                *self.state.lock().unwrap() = data.segments()[0].to_vec();
            }
            Ok(())
        })
    }
    callback!(end_setting_current_state, "end");
    fn prepare_for_remove(
        &self,
        _: Arc<dyn TransactionBase>,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("prepare-remove");
            Ok(())
        })
    }
    fn apply(
        &self,
        lsn: i64,
        tx: Arc<dyn TransactionBase>,
        _: ApplyContext,
        _: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>> {
        Box::pin(async move {
            assert!(lsn > 0);
            assert!(tx.get_commit_sequence_number() >= lsn);
            let data = data.ok_or(Error::InvalidInput("fixture redo required"))?;
            self.state.lock().unwrap().push(data.segments()[0][0]);
            self.event(format!("apply-{lsn}"));
            Ok(Some(Arc::new(Capture(lsn)) as Arc<dyn OperationContext>))
        })
    }
    fn unlock(&self, context: &dyn OperationContext) -> Result<()> {
        let context = (context as &dyn std::any::Any)
            .downcast_ref::<Capture>()
            .unwrap();
        self.event(format!("unlock-{}", context.0));
        Ok(())
    }
}
struct DataLoss;
impl IDataLossHandler for DataLoss {
    fn data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
}
type Created = Arc<
    Mutex<Option<oneshot::Sender<(VolatileTransactionalReplicator, NativeRuntimeIntegration)>>>,
>;
struct Factory {
    native: NativeIntegrationProvider,
    providers: Arc<Providers>,
    created: Created,
}
struct Replica {
    native: NativeRuntimeIntegration,
    providers: Arc<Providers>,
    created: Created,
    owner: Mutex<Option<VolatileTransactionalReplicator>>,
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
            providers: self.providers.clone(),
            created: self.created.clone(),
            owner: Mutex::new(None),
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
            let owner = VolatileTransactionalReplicator::create(
                partition,
                self.native.clone(),
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                Settings {
                    provider_operations: 1,
                    ..Settings::default()
                },
                self.providers.clone(),
                Arc::new(DataLoss),
                VolatileRuntimeSettings {
                    work_folder: PathBuf::from("public-fixture"),
                    log: VolatileLogSettings {
                        checkpoint_mb: 1,
                        min_log_mb: 1,
                        ..VolatileLogSettings::default()
                    },
                    ..VolatileRuntimeSettings::default()
                },
            )?;
            let control = owner.into_service_replicator();
            *self.owner.lock().unwrap() = Some(owner.clone());
            if let Some(send) = self.created.lock().unwrap().take() {
                let _ = send.send((owner, self.native.clone()));
            }
            Ok(control)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        // Logging/provider controls enter through the returned public composite.
        Box::pin(async { Ok("model://public-txn".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Fixture {
    host: Arc<rap::Host>,
    owner: VolatileTransactionalReplicator,
    native: NativeRuntimeIntegration,
    providers: Arc<Providers>,
}
impl Fixture {
    async fn start(partition: uuid::Uuid, id: i64, role: ReplicaRole) -> Self {
        let providers = Arc::new(Providers::default());
        let input = providers.clone();
        let (send, receive) = oneshot::channel();
        let host = rap::Host::start(partition, id, role, move |native| {
            Box::new(Factory {
                native,
                providers: input,
                created: Arc::new(Mutex::new(Some(send))),
            })
        })
        .await;
        let (owner, native) = receive.await.unwrap();
        Self {
            host,
            owner,
            native,
            providers,
        }
    }
    fn provider(&self, name: &str) -> Arc<Provider> {
        self.providers.objects.lock().unwrap()[name].clone()
    }
    async fn configure(&self, replicas: Vec<NativeReplicaDescriptor>) {
        self.host
            .configure(NativeReplicaSetConfiguration {
                epoch: Epoch {
                    data_loss_number: 1,
                    configuration_number: 1 << 32,
                },
                write_quorum: 1 + replicas.len(),
                replicas,
                local_member: true,
            })
            .await
            .unwrap();
    }
    fn descriptor(&self) -> NativeReplicaDescriptor {
        let identity = self.native.identity();
        NativeReplicaDescriptor {
            node_id: identity.node_id,
            node_instance: identity.node_instance,
            replica_id: identity.replica_id,
            incarnation: identity.incarnation,
            role: ReplicaRole::IdleSecondary,
            previous_role: ReplicaRole::None,
            up: true,
            ready: true,
            must_catch_up: false,
            address: self.host.replication_address().unwrap(),
            progress: ReplicationProgress { first: 1, last: 1 },
        }
    }
    async fn add(&self, name: &str) {
        let root = self.owner.transactional_replicator();
        let tx = root.create_transaction().unwrap();
        let before = root.get_last_committed_sequence_number().unwrap();
        root.add(
            tx.clone(),
            name.into(),
            "byte-fixture".into(),
            None,
            Some(Duration::ZERO),
            Cancellation::default(),
        )
        .await
        .unwrap();
        assert_eq!(root.get_last_committed_sequence_number().unwrap(), before);
        assert!(
            root.get(name).is_err(),
            "first-cache is not active metadata"
        );
        tx.commit().await.unwrap();
        tx.dispose().unwrap();
        let committed = tx.get_commit_sequence_number();
        until(|| {
            root.get_last_stable_sequence_number()
                .is_ok_and(|stable| stable >= committed)
        })
        .await;
        assert_eq!(root.get(name).unwrap().get_name(), name);
    }
    async fn write(&self, name: &str, markers: &[u8], bytes: usize) {
        let root = self.owner.transactional_replicator();
        let id = self.provider(name).id;
        let tx = root.create_transaction().unwrap();
        let input = tx.clone();
        let markers = markers.to_vec();
        self.native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                for marker in markers {
                    input.add_operation(
                        None,
                        None,
                        Some(OperationData::new(vec![vec![marker; bytes]])?),
                        id,
                        None,
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        tx.commit().await.unwrap();
        tx.dispose().unwrap();
        let committed = tx.get_commit_sequence_number();
        until(|| {
            root.get_last_stable_sequence_number()
                .is_ok_and(|stable| stable >= committed)
        })
        .await;
    }
}
async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_factory_primary_registry_incremental_null_full_and_stream_lifecycle() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary).await;
    source.configure(vec![]).await;
    let root = source.owner.transactional_replicator();
    assert!(root.get_last_committed_sequence_number().unwrap() >= 1);
    source.add("fabric:/public").await;
    source.add("fabric:/public/absent").await;
    let provider = source.provider("fabric:/public");
    let reader = root.create_transaction().unwrap();
    let existing = root
        .get_or_add(
            reader.clone(),
            "fabric:/public".into(),
            "byte-fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(existing.already_exists);
    reader.commit().await.unwrap();
    reader.dispose().unwrap();
    let enumerator = root.create_enumerator(true).unwrap();
    assert!(enumerator.current().is_err());
    let mut names = vec![];
    while enumerator.move_next().unwrap() {
        names.push(enumerator.current().unwrap().0);
    }
    assert_eq!(names.len(), 2);
    assert!(enumerator.current().is_err());

    // Native threshold, not a private manual checkpoint helper. Additional
    // ordinary transactions/idle Barriers advertise stability for completion.
    for marker in 1..=6 {
        source
            .write("fabric:/public", &[marker, marker + 10], 100_000)
            .await;
    }
    until(|| !provider.completed.lock().unwrap().is_empty()).await;
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary).await;
    source
        .host
        .build(target.descriptor(), Cancellation::default())
        .await
        .unwrap();
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    let copied = target.provider("fabric:/public");
    assert_eq!(
        copied.state.lock().unwrap().as_slice(),
        provider.state.lock().unwrap().as_slice()
    );
    assert_eq!(copied.id, provider.id, "copy preserves signed registry IDs");
    assert!(target.owner.transactional_replicator().get_is_readable());
    let events = target.providers.events.lock().unwrap().clone();
    let index = |event: &str| events.iter().position(|e| e == event).unwrap();
    assert!(index("fabric:/public:initialize") < index("fabric:/public:open"));
    assert!(index("fabric:/public:recover") < index("fabric:/public:begin"));
    assert!(index("fabric:/public:begin") < index("fabric:/public:end"));
    assert!(events
        .iter()
        .any(|e| e.starts_with("fabric:/public:set-") && e.ends_with("-0")));
    // Active metadata still owns Begin/End even when its source stream is None.
    assert!(events.iter().any(|e| e == "fabric:/public/absent:begin"));
    assert!(!events
        .iter()
        .any(|e| e.starts_with("fabric:/public/absent:set-")));
    let sent = source.providers.events.lock().unwrap().clone();
    assert!(sent.iter().any(|e| e == "fabric:/public:stream-eos"));
    assert!(!sent
        .iter()
        .any(|e| e.starts_with("fabric:/public/absent:stream-")));
    assert_eq!(
        sent.iter()
            .filter(|e| *e == "fabric:/public:stream-dispose")
            .count(),
        1
    );

    let mut member = target.descriptor();
    member.role = ReplicaRole::ActiveSecondary;
    source.configure(vec![member]).await;
    source.write("fabric:/public", &[91, 92, 93], 1).await;
    until(|| copied.state.lock().unwrap().ends_with(&[91, 92, 93])).await;
    let progress = source.owner.replicator().get_current_progress().unwrap();
    // provider_operations=1: advancement across fresh transactions/Barriers
    // requires actual operation capture retirement, not a diagnostic zero.
    let continued: Vec<u8> = (100..108).collect();
    source.write("fabric:/public", &continued, 1).await;
    until(|| copied.state.lock().unwrap().ends_with(&continued)).await;
    assert!(source.owner.replicator().get_current_progress().unwrap() > progress);
    assert!(root.create_atomic_operation().is_err());
    assert!(root.create_atomic_redo_operation().is_err());
    assert!(root.restore(PathBuf::from("unsupported")).await.is_err());
    assert!(OpenMode::Existing.validate().is_err());
    let remove = root.create_transaction().unwrap();
    root.remove(
        remove.clone(),
        "fabric:/public".into(),
        None,
        Cancellation::default(),
    )
    .await
    .unwrap();
    assert!(
        root.get("fabric:/public").is_ok(),
        "TransientDelete remains readable"
    );
    remove.commit().await.unwrap();
    remove.dispose().unwrap();
    assert!(root.get("fabric:/public").is_err());
    let events = source.providers.events.clone();
    drop(source.host);
    drop(target.host);
    until(|| {
        events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e == "fabric:/public:close")
    })
    .await;
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "fabric:/public:close")
            .count(),
        1
    );
    assert!(root.get_last_committed_sequence_number().is_err());
}
