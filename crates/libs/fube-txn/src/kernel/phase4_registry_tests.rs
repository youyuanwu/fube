//! Actual lower primary registry traces. Providers are lifecycle fixtures, not collections.
use super::*;
use crate::kernel::{
    maintenance::LogSettings, provider::PointProvider, runtime::RuntimeConfig, version::Completion,
};
use fube_data::extensions::ReplicationEndpointOptions;
use fube_rap_test_support as rap;
use fube_runtime::{
    extensions::{
        NativeIntegrationProvider, NativeReplicaDescriptor, NativeReplicaSetConfiguration,
        NativeRuntimeIntegration, ReplicationProgress,
    },
    IPrimaryReplicator, IStatefulServiceFactory, IStatefulServiceReplica, OpenMode,
};
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;

#[derive(Default)]
struct FactoryState {
    arguments: Vec<FactoryArguments>,
    events: Vec<(String, String)>,
    objects: BTreeMap<String, Arc<Provider>>,
    fail_recover: bool,
    fail_close: bool,
    prepare_gate: Option<Completion<()>>,
    fail_prepare: Option<String>,
    prepared_objects: Vec<usize>,
    fanout: usize,
}
struct Factory {
    weak: Weak<Self>,
    state: Mutex<FactoryState>,
}
impl Factory {
    fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            weak: weak.clone(),
            state: Mutex::new(FactoryState::default()),
        })
    }
}
impl IStateProvider2Factory for Factory {
    fn create(&self, arguments: &FactoryArguments) -> Result<Arc<dyn IStateProvider2>> {
        let provider = Arc::new(Provider {
            name: arguments.name.clone(),
            factory: self.weak.clone(),
            point: PointProvider::preinitialized(&arguments.name),
        });
        let mut state = self.state.lock().unwrap();
        state.arguments.push(arguments.clone());
        state
            .objects
            .insert(arguments.name.clone(), provider.clone());
        Ok(provider)
    }
}
struct Provider {
    name: String,
    factory: Weak<Factory>,
    point: Arc<PointProvider>,
}
impl Provider {
    fn event(&self, event: &str) {
        self.factory
            .upgrade()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .events
            .push((self.name.clone(), event.into()));
    }
}
macro_rules! delegate {
    ($name:ident ($($arg:ident:$ty:ty),* $(,)?) -> $ret:ty) => {
        fn $name(&self,$($arg:$ty),*) -> $ret { self.point.$name($($arg),*) }
    };
}
impl IStateProvider2 for Provider {
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_children(&self, _: &str) -> Result<Vec<StateProviderInfo>> {
        if self.name.ends_with("/wide") {
            let fanout = self.factory.upgrade().unwrap().state.lock().unwrap().fanout;
            return (0..fanout)
                .map(|i| {
                    Ok(StateProviderInfo {
                        name: format!("{}/child{i}", self.name),
                        type_name: "fixture".into(),
                        initialization_parameters: None,
                    })
                })
                .collect();
        }
        let suffix = if self.name.ends_with("/tree") {
            Some("/child")
        } else if self.name.ends_with("/tree/child") {
            Some("/grandchild")
        } else {
            None
        };
        Ok(suffix
            .map(|suffix| StateProviderInfo {
                name: format!("{}{suffix}", self.name),
                type_name: "fixture".into(),
                initialization_parameters: Some(OperationData::new(vec![vec![7]]).unwrap()),
            })
            .into_iter()
            .collect())
    }
    fn initialize(
        &self,
        owner: Weak<dyn ITransactionalReplicator>,
        _: &Path,
        children: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()> {
        let _owner = owner.upgrade().unwrap();
        if let Some(children) = children {
            let factory = self.factory.upgrade().unwrap();
            let state = factory.state.lock().unwrap();
            for child in children {
                assert!(state
                    .events
                    .contains(&(child.get_name().to_owned(), "initialize".into())));
            }
        }
        self.event("initialize");
        Ok(())
    }
    fn open(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("open");
            Ok(())
        })
    }
    fn recover_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("recover");
            if self
                .factory
                .upgrade()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .fail_recover
            {
                Err(Error::InvalidInput("controlled registry Recover failure"))
            } else {
                Ok(())
            }
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event(&format!("role:{role:?}"));
            Ok(())
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("close");
            if self
                .factory
                .upgrade()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .fail_close
            {
                Err(Error::InvalidInput("controlled registry Close failure"))
            } else {
                Ok(())
            }
        })
    }
    fn abort(&self) {
        self.event("abort");
    }
    delegate!(prepare_checkpoint(lsn: i64) -> Result<()>);
    delegate!(perform_checkpoint(cancel: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(complete_checkpoint(cancel: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(backup_checkpoint(path: PathBuf, cancel: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(restore_checkpoint(path: PathBuf, cancel: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(remove_state(cancel: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(get_current_state() -> Result<Option<Arc<dyn IOperationDataStream>>>);
    delegate!(begin_setting_current_state() -> ApiFuture<'_, ()>);
    delegate!(
        set_current_state(
            number: i64,
            data: OperationData,
            cancel: Cancellation,
        ) -> ApiFuture<'_, ()>
    );
    delegate!(end_setting_current_state(cancel: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(
        apply(
            lsn: i64,
            transaction: Arc<dyn TransactionBase>,
            context: ApplyContext,
            metadata: Option<OperationData>,
            data: Option<OperationData>,
        ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>>
    );
    delegate!(unlock(context: &dyn OperationContext) -> Result<()>);
    fn prepare_for_remove(
        &self,
        _: Arc<dyn TransactionBase>,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("prepare-remove");
            let factory = self.factory.upgrade().unwrap();
            let (gate, fail) = {
                let mut state = factory.state.lock().unwrap();
                state.prepared_objects.push(self as *const Self as usize);
                (
                    state.prepare_gate.clone(),
                    state.fail_prepare.as_deref() == Some(&self.name),
                )
            };
            if fail {
                return Err(Error::InvalidInput(
                    "controlled registry PrepareForRemove failure",
                ));
            }
            if let Some(gate) = gate {
                gate.wait().await.as_ref().clone()?;
            }
            Ok(())
        })
    }
}
struct DataLoss;
impl IDataLossHandler for DataLoss {
    fn data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
}
type Created = Arc<Mutex<Option<oneshot::Sender<Arc<VolatileRuntime>>>>>;
struct ServiceFactory {
    native: NativeIntegrationProvider,
    providers: Arc<Factory>,
    created: Created,
}
struct Service {
    native: NativeRuntimeIntegration,
    providers: Arc<Factory>,
    created: Created,
}
impl IStatefulServiceFactory for ServiceFactory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        id: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        Ok(Box::new(Service {
            native: self.native.capture(partition, id)?,
            providers: self.providers.clone(),
            created: self.created.clone(),
        }))
    }
}
impl IStatefulServiceReplica for Service {
    fn open(
        &self,
        _: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            let runtime = VolatileRuntime::create(
                partition,
                self.native.clone(),
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                fube_data::Settings::default(),
                RuntimeConfig {
                    log_settings: LogSettings {
                        checkpoint_mb: 1,
                        min_log_mb: 1,
                        truncate_factor: 2,
                        throttle_factor: 4,
                    },
                    registry: Some(RegistrySettings {
                        factory: self.providers.clone(),
                        metadata: vec![],
                    }),
                    providers: vec![],
                    work_folder: PathBuf::from("target/phase4-registry"),
                    memory_bytes: 8 * 1024 * 1024,
                    data_loss: Arc::new(DataLoss),
                },
            )?;
            let lower = runtime.replicator();
            self.created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(runtime)
                .ok()
                .unwrap();
            Ok(Box::new((*lower).clone()) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://registry".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Fixture {
    host: Arc<rap::Host>,
    runtime: Arc<VolatileRuntime>,
    providers: Arc<Factory>,
}
impl Fixture {
    async fn start() -> Self {
        Self::replica(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary).await
    }
    async fn replica(partition: uuid::Uuid, id: i64, role: ReplicaRole) -> Self {
        let providers = Factory::new();
        let input = providers.clone();
        let (send, receive) = oneshot::channel();
        let host = rap::Host::start(partition, id, role, move |native| {
            Box::new(ServiceFactory {
                native,
                providers: input,
                created: Arc::new(Mutex::new(Some(send))),
            })
        })
        .await;
        let runtime = receive.await.unwrap();
        if role == ReplicaRole::Primary {
            host.configure(NativeReplicaSetConfiguration {
                epoch: Epoch {
                    data_loss_number: 1,
                    configuration_number: 1 << 32,
                },
                write_quorum: 1,
                replicas: vec![],
                local_member: true,
            })
            .await
            .unwrap();
        }
        Self {
            host,
            runtime,
            providers,
        }
    }
    async fn close(self) {
        let runtime = self.runtime.clone();
        drop(self.host);
        runtime.cleanup_done.wait().await.as_ref().clone().unwrap();
    }
}
async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_cached_insert_visibility_and_dispose_lock_lifetime() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let transaction = owner.create_transaction().unwrap();
    let before = fixture.runtime.owner.manager.tail();
    owner
        .add(
            transaction.clone(),
            "fabric:/new".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        owner.manager.tail(),
        before,
        "Add uses synchronous native first cache"
    );
    assert!(Status::NameDoesNotExist.matches(&owner.get("fabric:/new").err().unwrap()));
    assert!(fixture.providers.state.lock().unwrap().events.is_empty());
    transaction.commit().await.unwrap();
    assert_eq!(owner.get("fabric:/new").unwrap().get_name(), "fabric:/new");
    let blocked = owner.create_transaction().unwrap();
    let error = owner
        .remove(
            blocked.clone(),
            "fabric:/new".into(),
            Some(Duration::ZERO),
            Cancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(
        Status::Timeout.matches(&error),
        "commit does not Dispose retained registry locks"
    );
    blocked.dispose().unwrap();
    transaction.dispose().unwrap();
    let read = owner.create_transaction().unwrap();
    let result = owner
        .get_or_add(
            read.clone(),
            "fabric:/new".into(),
            "ignored".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(result.already_exists);
    assert_eq!(
        fixture.providers.state.lock().unwrap().arguments.len(),
        1,
        "existing GetOrAdd does not construct a provider"
    );
    read.commit().await.unwrap();
    let blocked = owner.create_transaction().unwrap();
    assert!(Status::Timeout.matches(
        &owner
            .remove(
                blocked.clone(),
                "fabric:/new".into(),
                Some(Duration::ZERO),
                Cancellation::default()
            )
            .await
            .unwrap_err()
    ));
    blocked.dispose().unwrap();
    read.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_dropped_add_observer_continues_after_transient_abort() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let first = owner.create_transaction().unwrap();
    owner
        .add(
            first.clone(),
            "fabric:/drop".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    let second = owner.manager.new_transaction().unwrap();
    let observer = owner.get_or_add(
        second.clone(),
        "fabric:/drop".into(),
        "fixture".into(),
        None,
        None,
        Cancellation::default(),
    );
    drop(observer);
    first.abort().unwrap();
    first.dispose().unwrap();
    until(|| second.has_cached_first()).await;
    second.commit().await.unwrap();
    second.dispose().unwrap();
    assert!(owner.get("fabric:/drop").is_ok());
    assert_eq!(
        fixture
            .providers
            .state
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|(_, e)| e == "initialize")
            .count(),
        1
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_remove_transient_abort_and_committed_delete() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let create = owner.create_transaction().unwrap();
    owner
        .add(
            create.clone(),
            "fabric:/remove".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let row = fixture.runtime.registry.rows().pop().unwrap();
    let remove = owner.create_transaction().unwrap();
    owner
        .remove(
            remove.clone(),
            row.name.clone(),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(
        owner.get(&row.name).is_ok(),
        "TransientDelete remains readable"
    );
    assert!(Status::ObjectClosed.matches(
        &owner
            .error_if_state_provider_is_not_registered(row.id, remove.get_transaction_id())
            .unwrap_err()
    ));
    remove.abort().unwrap();
    remove.dispose().unwrap();
    owner
        .error_if_state_provider_is_not_registered(row.id, create.get_transaction_id())
        .unwrap();
    assert!(owner.get(&row.name).is_ok());
    let remove = owner.create_transaction().unwrap();
    owner
        .remove(
            remove.clone(),
            row.name.clone(),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    remove.commit().await.unwrap();
    assert!(Status::NameDoesNotExist.matches(&owner.get(&row.name).err().unwrap()));
    assert!(Status::InvalidParameter.matches(
        &owner
            .error_if_state_provider_is_not_registered(row.id, create.get_transaction_id())
            .unwrap_err()
    ));
    remove.dispose().unwrap();
    let recreate = owner.create_transaction().unwrap();
    let result = owner
        .get_or_add(
            recreate.clone(),
            row.name.clone(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(!result.already_exists);
    recreate.commit().await.unwrap();
    recreate.dispose().unwrap();
    assert_eq!(
        fixture.runtime.registry.rows().len(),
        2,
        "deleted object retained independently of new active name"
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_hierarchy_native_flat_parent_and_enumerator() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let transaction = owner.create_transaction().unwrap();
    owner
        .add(
            transaction.clone(),
            "fabric:/tree".into(),
            "fixture".into(),
            Some(OperationData::new(vec![vec![9]]).unwrap()),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    transaction.dispose().unwrap();
    let rows = fixture.runtime.registry.rows();
    let root = rows.iter().find(|r| r.parent == 0).unwrap();
    assert!(rows
        .iter()
        .filter(|r| r.id != root.id)
        .all(|r| r.parent == root.id && r.initialization.is_none()));
    {
        let state = fixture.providers.state.lock().unwrap();
        assert!(state
            .arguments
            .iter()
            .filter(|a| a.name != root.name)
            .all(|a| a.initialization_parameters.as_ref().unwrap().to_segments() == vec![vec![7]]));
        let initialized = state
            .events
            .iter()
            .position(|(_, e)| e == "initialize")
            .unwrap();
        let opened = state.events.iter().position(|(_, e)| e == "open").unwrap();
        assert_eq!(opened - initialized, 3);
        assert_eq!(
            state.events[opened].0, root.name,
            "all Initialize before parent-first Open"
        );
    }
    let parents = owner.create_enumerator(true).unwrap();
    assert!(parents.current().is_err());
    assert!(parents.move_next().unwrap());
    assert_eq!(parents.current().unwrap().0, root.name);
    assert!(!parents.move_next().unwrap());
    assert!(parents.current().is_err());
    let all = owner.create_enumerator(false).unwrap();
    let mut count = 0;
    while all.move_next().unwrap() {
        count += 1;
    }
    assert_eq!(count, 3);
    let remove = owner.create_transaction().unwrap();
    owner
        .remove(
            remove.clone(),
            root.name.clone(),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    remove.commit().await.unwrap();
    remove.dispose().unwrap();
    assert!(!owner.create_enumerator(false).unwrap().move_next().unwrap());
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_corrective_remove_parent_first_and_root_failure_has_no_child_effects() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let create = owner.create_transaction().unwrap();
    owner
        .add(
            create.clone(),
            "fabric:/tree".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    fixture.providers.state.lock().unwrap().fail_prepare = Some("fabric:/tree".into());
    let remove = owner.manager.new_transaction().unwrap();
    let before = owner.manager.tail();
    assert!(owner
        .remove(
            remove.clone(),
            "fabric:/tree".into(),
            None,
            Cancellation::default()
        )
        .await
        .is_err());
    assert!(
        !remove.has_cached_first(),
        "failed root Prepare precedes first cache"
    );
    assert_eq!(owner.manager.tail(), before);
    assert_eq!(
        fixture
            .providers
            .state
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|(_, event)| event == "prepare-remove")
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
        ["fabric:/tree"]
    );
    let rows = fixture.runtime.registry.rows();
    {
        let entries = owner.manager.directory.entries.lock().unwrap();
        for row in rows {
            assert_eq!(
                entries[&row.id].deleting_by.is_some(),
                row.name == "fabric:/tree"
            );
        }
    }
    remove.abort().unwrap();
    remove.dispose().unwrap();
    fixture.providers.state.lock().unwrap().fail_prepare = None;
    let remove = owner.create_transaction().unwrap();
    owner
        .remove(
            remove.clone(),
            "fabric:/tree".into(),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    let prepared = fixture
        .providers
        .state
        .lock()
        .unwrap()
        .events
        .iter()
        .filter(|(_, event)| event == "prepare-remove")
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        &prepared[1..],
        [
            "fabric:/tree",
            "fabric:/tree/child",
            "fabric:/tree/child/grandchild"
        ]
    );
    remove.commit().await.unwrap();
    remove.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_corrective_add_adopts_absent_name_lock_id_and_disposes_transient_tree() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let registry = &fixture.runtime.registry;
    let transaction = owner.create_transaction().unwrap();
    let id = registry.next_id().unwrap();
    let held = registry
        .acquire("fabric:/cancel-id", id, 1001, LockKind::Read, None)
        .await
        .unwrap();
    let cancel = Cancellation::default();
    cancel.cancel();
    let before = owner.manager.tail();
    let adding = owner.add(
        transaction.clone(),
        "fabric:/cancel-id".into(),
        "fixture".into(),
        None,
        None,
        cancel,
    );
    until(|| !held.lock.grants.lock().unwrap().writers.is_empty()).await;
    registry.release(&held, false).unwrap(); // native read→write window retains lock ID
    assert!(Status::Cancelled.matches(&adding.await.unwrap_err()));
    assert!(registry.state.lock().unwrap().pending.contains_key(&id));
    transaction.dispose().unwrap();
    assert!(registry.state.lock().unwrap().pending.is_empty());
    assert!(!registry
        .state
        .lock()
        .unwrap()
        .locks
        .contains_key("fabric:/cancel-id"));
    assert_eq!(owner.manager.tail(), before);
    let transaction = owner.create_transaction().unwrap();
    owner
        .add(
            transaction.clone(),
            "fabric:/cancel-id".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    transaction.dispose().unwrap();

    let id = registry.next_id().unwrap();
    let held = registry
        .acquire("fabric:/tree", id, 1002, LockKind::Read, None)
        .await
        .unwrap();
    let transaction = owner.create_transaction().unwrap();
    let adding = owner.add(
        transaction.clone(),
        "fabric:/tree".into(),
        "fixture".into(),
        None,
        None,
        Cancellation::default(),
    );
    until(|| !held.lock.grants.lock().unwrap().writers.is_empty()).await;
    registry.release(&held, false).unwrap();
    adding.await.unwrap();
    transaction.commit().await.unwrap();
    transaction.dispose().unwrap();
    let rows = registry.rows();
    assert_eq!(
        rows.iter().find(|r| r.name == "fabric:/tree").unwrap().id,
        id
    );
    assert!(rows
        .iter()
        .filter(|r| r.name.starts_with("fabric:/tree/"))
        .all(|r| r.parent == id));
    // Factory objects are preserved, not recreated to rewrite their constructor IDs.
    {
        let state = fixture.providers.state.lock().unwrap();
        let root_args = state
            .arguments
            .iter()
            .find(|a| a.name == "fabric:/tree")
            .unwrap();
        assert_ne!(root_args.state_provider_id, id);
        assert_eq!(
            state
                .arguments
                .iter()
                .filter(|a| a.name == "fabric:/tree")
                .count(),
            1
        );
    }
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_corrective_waiting_remove_refreshes_replacement_metadata_and_provider() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let registry = &fixture.runtime.registry;
    let create = owner.create_transaction().unwrap();
    owner
        .add(
            create.clone(),
            "fabric:/replace".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let old_id = registry.rows()[0].id;
    let deleting = owner.create_transaction().unwrap();
    owner
        .remove(
            deleting.clone(),
            "fabric:/replace".into(),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap(); // retains old write lock
    let entered = Completion::new();
    let release = Completion::new();
    *registry.disposed_lock_pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let waiting = owner.create_transaction().unwrap();
    let removing = owner.remove(
        waiting.clone(),
        "fabric:/replace".into(),
        None,
        Cancellation::default(),
    );
    until(|| {
        !registry.state.lock().unwrap().locks["fabric:/replace"]
            .grants
            .lock()
            .unwrap()
            .writers
            .is_empty()
    })
    .await;
    deleting.commit().await.unwrap();
    deleting.dispose().unwrap(); // disposed-lock retry pauses while real replacement wins
    entered.wait().await.as_ref().clone().unwrap();
    let replacing = owner.create_transaction().unwrap();
    owner
        .add(
            replacing.clone(),
            "fabric:/replace".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    replacing.commit().await.unwrap();
    replacing.dispose().unwrap();
    let (new_row, current) = registry.lookup_metadata("fabric:/replace").unwrap();
    assert_ne!(new_row.id, old_id);
    let current_pointer = Arc::as_ptr(&current) as *const () as usize;
    release.complete(Ok(()));
    removing.await.unwrap();
    assert_eq!(
        fixture
            .providers
            .state
            .lock()
            .unwrap()
            .prepared_objects
            .last()
            .copied(),
        Some(current_pointer)
    );
    {
        let entries = owner.manager.directory.entries.lock().unwrap();
        assert_eq!(
            entries[&new_row.id].deleting_by,
            Some(waiting.get_transaction_id())
        );
        assert_eq!(entries[&old_id].deleting_by, None);
    }
    waiting.commit().await.unwrap();
    waiting.dispose().unwrap();
    assert!(Status::NameDoesNotExist.matches(&owner.get("fabric:/replace").err().unwrap()));
    assert_eq!(
        registry
            .rows()
            .iter()
            .filter(|r| r.mode == ProviderMode::DelayDelete)
            .count(),
        2
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_corrective_initial_remove_metadata_disappearance_returns_not_found_without_panic() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let create = owner.create_transaction().unwrap();
    owner
        .add(
            create.clone(),
            "fabric:/vanish".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let entered = Completion::new();
    let release = Completion::new();
    *fixture
        .runtime
        .registry
        .remove_discovery_pause
        .lock()
        .unwrap() = Some((entered.clone(), release.clone()));
    let waiting = owner.create_transaction().unwrap();
    let removing = owner.remove(
        waiting.clone(),
        "fabric:/vanish".into(),
        None,
        Cancellation::default(),
    );
    entered.wait().await.as_ref().clone().unwrap();
    let deleting = owner.create_transaction().unwrap();
    owner
        .remove(
            deleting.clone(),
            "fabric:/vanish".into(),
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    deleting.commit().await.unwrap();
    deleting.dispose().unwrap();
    release.complete(Ok(()));
    assert!(Status::NameDoesNotExist.matches(&removing.await.unwrap_err()));
    waiting.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_corrective_wide_discovery_bounds_factory_captures_before_transient_or_lsn() {
    let fixture = Fixture::start().await;
    fixture.providers.state.lock().unwrap().fanout = 10_000;
    let owner = &fixture.runtime.owner;
    let transaction = owner.create_transaction().unwrap();
    let before = owner.manager.tail();
    assert!(Status::InsufficientResources.matches(
        &owner
            .add(
                transaction.clone(),
                "fabric:/wide".into(),
                "fixture".into(),
                None,
                None,
                Cancellation::default()
            )
            .await
            .unwrap_err()
    ));
    assert_eq!(
        fixture.providers.state.lock().unwrap().arguments.len(),
        64,
        "root plus children are bounded before next factory capture"
    );
    assert!(fixture
        .runtime
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .is_empty());
    assert!(fixture
        .runtime
        .registry
        .state
        .lock()
        .unwrap()
        .locks
        .is_empty());
    assert_eq!(owner.manager.tail(), before);
    transaction.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_post_open_failure_closes_once_and_abort_fallback() {
    let fixture = Fixture::start().await;
    fixture.providers.state.lock().unwrap().fail_recover = true;
    fixture.providers.state.lock().unwrap().fail_close = true;
    let transaction = fixture.runtime.owner.create_transaction().unwrap();
    fixture
        .runtime
        .owner
        .add(
            transaction.clone(),
            "fabric:/fail".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(Status::TransactionAborted.matches(&transaction.commit().await.unwrap_err()));
    transaction.dispose().unwrap();
    assert!(fixture.runtime.registry.rows().is_empty());
    assert!(fixture
        .runtime
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .is_empty());
    let events = fixture.providers.state.lock().unwrap().events.clone();
    assert_eq!(
        events.iter().map(|(_, e)| e.as_str()).collect::<Vec<_>>(),
        ["initialize", "open", "recover", "close", "abort"]
    );
    let providers = fixture.providers.clone();
    fixture.close().await;
    assert_eq!(
        providers
            .state
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|(_, e)| e == "close")
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_cancelled_add_retains_transient_until_dispose_and_invalid_no_mutation(
) {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let transaction = owner.create_transaction().unwrap();
    let before = owner.manager.tail();
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(Status::Cancelled.matches(
        &owner
            .add(
                transaction.clone(),
                "fabric:/cancel".into(),
                "fixture".into(),
                None,
                None,
                cancel
            )
            .await
            .unwrap_err()
    ));
    assert_eq!(owner.manager.tail(), before);
    assert_eq!(
        fixture.runtime.registry.state.lock().unwrap().pending.len(),
        1,
        "native cancellation happens at ReplicateAsync after transient mutation"
    );
    transaction.dispose().unwrap();
    assert!(fixture
        .runtime
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .is_empty());
    let invalid = owner.create_transaction().unwrap();
    assert!(owner
        .add(
            invalid.clone(),
            "fabric:/StateManager".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default()
        )
        .await
        .is_err());
    assert_eq!(owner.manager.tail(), before);
    assert!(fixture
        .runtime
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .is_empty());
    let metadata = RegistryOperation {
        id: 999,
        name: "fabric:/raw".into(),
        kind: ApplyType::Insert,
    }
    .encode()
    .unwrap();
    let redo = InsertRedo {
        type_name: "fixture".into(),
        parent: 0,
        initialization: None,
    }
    .encode()
    .unwrap();
    assert!(Status::InvalidParameter.matches(
        &fixture
            .runtime
            .registry
            .preflight(Some(&metadata), Some(&redo))
            .unwrap_err()
    ));
    invalid.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_native_retry_timeout_and_existing_get_ignores_cancelled_lock_token()
{
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let transaction = owner.create_transaction().unwrap();
    let before = owner.manager.tail();
    owner
        .manager
        .script_throttle([Err(Status::ServiceTooBusy.error())]);
    let error = owner
        .add(
            transaction.clone(),
            "fabric:/retry/tree".into(),
            "fixture".into(),
            None,
            Some(Duration::ZERO),
            Cancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(Status::Timeout.matches(&error), "timeout is checked after classified AddOperation failure, not before successful first cache");
    assert_eq!(owner.manager.tail(), before);
    assert_eq!(
        fixture.runtime.registry.state.lock().unwrap().pending.len(),
        2
    );
    transaction.dispose().unwrap();
    assert!(fixture
        .runtime
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .is_empty());
    let create = owner.create_transaction().unwrap();
    owner
        .add(
            create.clone(),
            "fabric:/existing".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    let reader = owner.create_transaction().unwrap();
    let cancelled = Cancellation::default();
    cancelled.cancel();
    let lookup = owner.get_or_add(
        reader.clone(),
        "fabric:/existing".into(),
        "ignored".into(),
        None,
        None,
        cancelled,
    );
    create.dispose().unwrap();
    assert!(lookup.await.unwrap().already_exists, "native metadata lock ignores token and existing GetOrAdd has no replicate cancellation check");
    reader.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_close_waits_owned_prepare_for_remove_callback() {
    let fixture = Fixture::start().await;
    let runtime = fixture.runtime.clone();
    let create = runtime.owner.create_transaction().unwrap();
    runtime
        .owner
        .add(
            create.clone(),
            "fabric:/closing".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let gate = Completion::new();
    fixture.providers.state.lock().unwrap().prepare_gate = Some(gate.clone());
    let remove = runtime.owner.create_transaction().unwrap();
    let observer = runtime.owner.remove(
        remove.clone(),
        "fabric:/closing".into(),
        None,
        Cancellation::default(),
    );
    assert!(fixture
        .providers
        .state
        .lock()
        .unwrap()
        .events
        .iter()
        .any(|(_, e)| e == "prepare-remove"));
    let shutdown = tokio::task::spawn_blocking(move || drop(fixture.host));
    until(|| runtime.owner.manager.gate().is_err()).await;
    assert!(!runtime.cleanup_done.is_completed());
    assert!(!fixture
        .providers
        .state
        .lock()
        .unwrap()
        .events
        .iter()
        .any(|(_, e)| e == "close"));
    gate.complete(Ok(()));
    assert!(
        observer.await.is_err(),
        "terminal authority rejects later admission, not an invented successful Remove"
    );
    remove.dispose().unwrap();
    runtime.cleanup_done.wait().await.as_ref().clone().unwrap();
    shutdown.await.unwrap();
    assert_eq!(
        fixture
            .providers
            .state
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|(_, e)| e == "close")
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_waiting_writer_precedes_later_readers() {
    let fixture = Fixture::start().await;
    let owner = &fixture.runtime.owner;
    let create = owner.create_transaction().unwrap();
    owner
        .add(
            create.clone(),
            "fabric:/queue".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let read = owner.create_transaction().unwrap();
    owner
        .get_or_add(
            read.clone(),
            "fabric:/queue".into(),
            "ignored".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    let writer = owner.create_transaction().unwrap();
    let removing = owner.remove(
        writer.clone(),
        "fabric:/queue".into(),
        None,
        Cancellation::default(),
    );
    let later = owner.create_transaction().unwrap();
    let reading = owner.get_or_add(
        later.clone(),
        "fabric:/queue".into(),
        "ignored".into(),
        None,
        None,
        Cancellation::default(),
    );
    let lock = fixture.runtime.registry.state.lock().unwrap().locks["fabric:/queue"].clone();
    assert_eq!(lock.grants.lock().unwrap().writers.len(), 1);
    assert_eq!(lock.grants.lock().unwrap().waiting_readers.len(), 1);
    read.dispose().unwrap();
    removing.await.unwrap();
    assert!(lock
        .grants
        .lock()
        .unwrap()
        .writer
        .is_some_and(|(tx, _)| tx == writer.get_transaction_id()));
    assert!(!lock
        .grants
        .lock()
        .unwrap()
        .readers
        .contains_key(&later.get_transaction_id()));
    writer.abort().unwrap();
    writer.dispose().unwrap();
    assert!(reading.await.unwrap().already_exists);
    later.dispose().unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase4_registry_actual_primary_created_hierarchy_full_copies_then_promoted_id_advances() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::replica(partition, 1, ReplicaRole::Primary).await;
    let create = source.runtime.owner.create_transaction().unwrap();
    source
        .runtime
        .owner
        .add(
            create.clone(),
            "fabric:/copied/tree".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let checkpoint = source
        .runtime
        .maintenance()
        .start_checkpoint(false)
        .unwrap()
        .unwrap();
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    let target = Fixture::replica(partition, 2, ReplicaRole::IdleSecondary).await;
    source
        .host
        .build(
            NativeReplicaDescriptor {
                node_id: [2; 16],
                node_instance: 1,
                replica_id: 2,
                incarnation: 12,
                role: ReplicaRole::IdleSecondary,
                previous_role: ReplicaRole::None,
                up: true,
                ready: true,
                must_catch_up: false,
                address: target.host.replication_address().unwrap(),
                progress: ReplicationProgress { first: 1, last: 1 },
            },
            Cancellation::default(),
        )
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
    let original = source.runtime.registry.rows();
    let copied = target.runtime.registry.rows();
    assert_eq!(original.len(), 3);
    assert_eq!(
        copied
            .iter()
            .map(|r| (r.id, r.parent, &r.name))
            .collect::<Vec<_>>(),
        original
            .iter()
            .map(|r| (r.id, r.parent, &r.name))
            .collect::<Vec<_>>()
    );
    let copied_high = copied.iter().map(|r| r.id).max().unwrap();
    target.host.change_role(ReplicaRole::Primary).await.unwrap();
    target
        .host
        .configure(NativeReplicaSetConfiguration {
            epoch: Epoch {
                data_loss_number: 1,
                configuration_number: 1 << 32,
            },
            write_quorum: 1,
            replicas: vec![],
            local_member: true,
        })
        .await
        .unwrap();
    let create = target.runtime.owner.create_transaction().unwrap();
    target
        .runtime
        .owner
        .add(
            create.clone(),
            "fabric:/promoted".into(),
            "fixture".into(),
            None,
            None,
            Cancellation::default(),
        )
        .await
        .unwrap();
    create.commit().await.unwrap();
    create.dispose().unwrap();
    let next = target
        .runtime
        .registry
        .rows()
        .into_iter()
        .find(|r| r.name == "fabric:/promoted")
        .unwrap();
    assert!(
        next.id > copied_high,
        "native signed 100ns/high-water identity survives Full and promotion"
    );
    source.close().await;
    target.close().await;
}
