//! Explicitly minimal native-free local Hosting package fixtures.
use fube_data::{
    extensions::ReplicationEndpointOptions, IOperationDataStream, IStateProvider, MemoryProvider,
    Mutation,
};
use fube_runtime::{
    extensions::{NativeIntegrationProvider, NativeRuntimeIntegration},
    ApiFuture, Cancellation, Epoch, Error, IPrimaryReplicator, IReplicator,
    IStatefulServiceFactory, IStatefulServicePartition, IStatefulServiceReplica,
    IStatelessServiceFactory, IStatelessServiceInstance, IStatelessServicePartition, OpenMode,
    ReplicaInformation, ReplicaRole, ReplicaSetConfig, ReplicaSetQuorumMode, Result, Runtime,
    RuntimeReplicator, ServicePartitionAccessStatus,
};
use std::{
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Default)]
struct Mode {
    block_dispose: bool,
    block_provider_dispose: bool,
    close_error: bool,
    open_delay: u64,
}
struct Factory(Mode);
struct StatefulFactory {
    lifecycle: bool,
    native: Option<NativeIntegrationProvider>,
    mode: Mode,
}
struct StatefulReplica {
    resource: Instance,
    partition: Mutex<Option<Arc<dyn IStatefulServicePartition>>>,
    native: Option<NativeRuntimeIntegration>,
    data: Mutex<Option<Arc<RuntimeReplicator>>>,
}
struct ObservedProvider {
    memory: Arc<MemoryProvider>,
    resource: PathBuf,
    block_dispose: bool,
}
struct LocalControl(Mutex<Option<TcpListener>>);
struct Instance {
    mode: Mode,
    resource: PathBuf,
}
fn marker(value: &str) {
    println!("{value}");
    std::io::stdout()
        .flush()
        .expect("flush lifecycle observation");
}
fn wait_for_disposal_release(release: &Path) {
    let end = std::time::Instant::now() + Duration::from_secs(5);
    while !release.exists() {
        assert!(
            std::time::Instant::now() < end,
            "bounded fixture disposal barrier timed out"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}
impl IStatefulServiceFactory for StatefulFactory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        replica: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        marker("LOCAL_CALLBACK create_replica");
        if !self.lifecycle {
            return Err(Error::Protocol("registration-only stateful fixture"));
        }
        let native = self
            .native
            .as_ref()
            .map(|native| native.capture(partition, replica))
            .transpose()?;
        let resource =
            PathBuf::from(std::env::var_os("Fabric_Folder_App_Work").expect("owned work folder"))
                .join("local-instance.resource");
        std::fs::write(&resource, b"actual replica-owned resource")
            .expect("acquire stateful fixture resource");
        marker("LOCAL_RESOURCE acquired");
        Ok(Box::new(StatefulReplica {
            resource: Instance {
                mode: self.mode,
                resource,
            },
            partition: Mutex::new(None),
            native,
            data: Mutex::new(None),
        }))
    }
}
impl IStatefulServiceReplica for StatefulReplica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            assert_eq!(mode, OpenMode::New);
            assert_ne!(
                partition.get_write_status()?,
                ServicePartitionAccessStatus::Granted
            );
            *self.partition.lock().unwrap() = Some(partition.clone());
            marker("LOCAL_CALLBACK open");
            let control: Box<dyn IPrimaryReplicator> = if let Some(native) = &self.native {
                let resource = self
                    .resource
                    .resource
                    .with_file_name("local-provider.resource");
                std::fs::write(&resource, b"actual data-provider-owned resource")
                    .expect("acquire data provider resource");
                let provider = Arc::new(ObservedProvider {
                    memory: MemoryProvider::new(),
                    resource,
                    block_dispose: self.resource.mode.block_provider_dispose,
                });
                marker("LOCAL_DATA_PROVIDER acquired");
                let control = native.create_replicator(
                    partition,
                    provider,
                    None,
                    ReplicationEndpointOptions {
                        listen_address: "127.0.0.1:0".into(),
                        actor_incarnation: uuid::Uuid::new_v4(),
                    },
                    fube_data::Settings::default(),
                )?;
                marker(&format!(
                    "LOCAL_DATA constructed endpoint={}",
                    control.endpoint()
                ));
                *self.data.lock().unwrap() = Some(control.clone());
                Box::new((*control).clone())
            } else {
                Box::new(LocalControl(Mutex::new(None)))
            };
            tokio::time::sleep(Duration::from_millis(self.resource.mode.open_delay)).await;
            Ok(control)
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async move {
            if !matches!(role, ReplicaRole::IdleSecondary | ReplicaRole::None) {
                return Err(Error::Protocol(
                    "fixture prohibits Primary/secondary promotion",
                ));
            }
            marker(&format!("LOCAL_CALLBACK role {role:?}"));
            if role == ReplicaRole::IdleSecondary {
                let partition = self
                    .partition
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("opened partition");
                assert_ne!(
                    partition.get_read_status()?,
                    ServicePartitionAccessStatus::Granted
                );
                assert_ne!(
                    partition.get_write_status()?,
                    ServicePartitionAccessStatus::Granted
                );
                marker("LOCAL_ACCESS denied");
                let data = self.data.lock().unwrap().clone();
                if let Some(data) = data {
                    let before = data.snapshot();
                    assert_eq!(before.role, ReplicaRole::IdleSecondary);
                    assert!(!before.ready && !before.closed);
                    assert!(before.fault.is_none() && before.peers.is_empty());
                    marker(&format!(
                        "LOCAL_DATA idle role={:?} ready={} assigned={}",
                        before.role, before.ready, before.progress.assigned
                    ));
                    let state = data.get_state_replicator();
                    for attempt in 0..2 {
                        let operation = Mutation {
                            id: format!("idle-write-{attempt}"),
                            key: "value".into(),
                            value: "must not be admitted".into(),
                        }
                        .operation()?;
                        let error = state
                            .replicate(operation, Cancellation::default())
                            .await
                            .err()
                            .expect("Idle write must reject");
                        assert_eq!(error.kind(), fube_runtime::ErrorKind::NotPrimary);
                        let after = data.snapshot();
                        assert_eq!(after.progress, before.progress);
                        assert_eq!((after.owned_operations, after.owned_bytes), (0, 0));
                        marker(&format!(
                            "LOCAL_DATA write_denied {:?} assigned={}",
                            error.kind(),
                            after.progress.assigned
                        ));
                    }
                }
            }
            Ok(String::new())
        })
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.release_data();
        IStatelessServiceInstance::close(&self.resource, cancel)
    }
    fn abort(&self) {
        self.release_data();
        marker("LOCAL_CALLBACK abort");
    }
}
impl StatefulReplica {
    fn release_data(&self) {
        if let Some(data) = self.data.lock().unwrap().take() {
            let snapshot = data.snapshot();
            assert!(
                snapshot.closed,
                "RAP must retire the lower before the service"
            );
            assert_eq!(
                (
                    snapshot.provider_callbacks,
                    snapshot.owned_operations,
                    snapshot.owned_bytes
                ),
                (0, 0, 0)
            );
            let error = data
                .get_state_replicator()
                .get_replication_stream()
                .err()
                .expect("retired data owner must reject stream access");
            assert_eq!(error.kind(), fube_runtime::ErrorKind::ObjectClosed);
            marker("LOCAL_DATA retired");
        }
    }
}
impl IStateProvider for ObservedProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.memory.get_last_committed_sequence_number()
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.memory.update_epoch(epoch, previous, cancel)
    }
    fn on_data_loss(&self, cancel: Cancellation) -> ApiFuture<'_, bool> {
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
impl Drop for ObservedProvider {
    fn drop(&mut self) {
        marker("LOCAL_DATA_PROVIDER dispose begin");
        if self.block_dispose {
            wait_for_disposal_release(
                &self
                    .resource
                    .with_file_name("local-provider-dispose.release"),
            );
        }
        let state = self.memory.snapshot();
        assert_eq!(state.last, 0);
        assert!(state.values.is_empty() && state.results.is_empty());
        std::fs::remove_file(&self.resource).expect("dispose actual data provider resource");
        marker("LOCAL_DATA_PROVIDER disposed empty");
    }
}
impl LocalControl {
    fn release(&self) {
        if let Some(listener) = self.0.lock().unwrap().take() {
            drop(listener);
            marker("LOCAL_CONTROL released");
        }
    }
}
impl IReplicator for LocalControl {
    fn open(&self, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("bind actual fixture control listener");
            let address = listener
                .local_addr()
                .expect("fixture control address")
                .to_string();
            *self.0.lock().unwrap() = Some(listener);
            marker("LOCAL_CONTROL acquired");
            Ok(address)
        })
    }
    fn change_role(&self, _: Epoch, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if !matches!(role, ReplicaRole::IdleSecondary | ReplicaRole::None) {
                return Err(Error::Protocol("fixture control prohibits promotion"));
            }
            marker(&format!("LOCAL_CONTROL role {role:?}"));
            Ok(())
        })
    }
    fn update_epoch(&self, _: Epoch, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("fixture has no epoch/data capability")) })
    }
    fn get_current_progress(&self) -> Result<i64> {
        Err(Error::Protocol("fixture has no data progress"))
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        Err(Error::Protocol("fixture has no retained history"))
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async {
            self.release();
            Ok(())
        })
    }
    fn abort(&self) {
        marker("LOCAL_CONTROL abort");
        self.release();
    }
}
impl IPrimaryReplicator for LocalControl {
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Err(Error::Protocol("fixture has no data-loss capability")) })
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        _: ReplicaSetConfig,
        _: ReplicaSetConfig,
    ) -> Result<()> {
        Err(Error::Protocol("fixture has no configuration capability"))
    }
    fn update_current_replica_set_configuration(&self, _: ReplicaSetConfig) -> Result<()> {
        Err(Error::Protocol("fixture has no configuration capability"))
    }
    fn wait_for_catch_up_quorum(
        &self,
        _: ReplicaSetQuorumMode,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("fixture has no quorum capability")) })
    }
    fn build_replica(&self, _: ReplicaInformation, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("fixture has no build capability")) })
    }
    fn remove_replica(&self, _: i64) -> Result<()> {
        Err(Error::Protocol("fixture has no peer-removal capability"))
    }
}
impl Drop for LocalControl {
    fn drop(&mut self) {
        assert!(
            self.0.lock().unwrap().is_none(),
            "control listener was not disposed"
        );
        marker("LOCAL_CONTROL disposed");
    }
}
impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        instance: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>> {
        marker(&format!(
            "LOCAL_CALLBACK create partition={partition} instance={instance}"
        ));
        let resource =
            PathBuf::from(std::env::var_os("Fabric_Folder_App_Work").expect("owned work folder"))
                .join("local-instance.resource");
        std::fs::write(&resource, b"actual instance-owned resource")
            .expect("acquire fixture resource");
        marker("LOCAL_RESOURCE acquired");
        Ok(Box::new(Instance {
            mode: self.0,
            resource,
        }))
    }
}
impl IStatelessServiceInstance for Instance {
    fn open(
        &self,
        _: Arc<dyn IStatelessServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, String> {
        Box::pin(async {
            marker("LOCAL_CALLBACK open");
            tokio::time::sleep(Duration::from_millis(self.mode.open_delay)).await;
            Ok(String::new())
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async {
            marker("LOCAL_CALLBACK close");
            if self.mode.close_error {
                Err(Error::Protocol("fixture graceful close failure"))
            } else {
                Ok(())
            }
        })
    }
    fn abort(&self) {
        marker("LOCAL_CALLBACK abort");
    }
}
impl Drop for Instance {
    fn drop(&mut self) {
        marker("LOCAL_DISPOSE begin");
        if self.mode.block_dispose {
            wait_for_disposal_release(&self.resource.with_file_name("local-dispose.release"));
        }
        std::fs::remove_file(&self.resource).expect("dispose actual fixture resource");
        marker("LOCAL_RESOURCE released");
        marker("LOCAL_DISPOSE complete");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // These explicit fault modes are for owned-lifecycle qualification only.
    let args: Vec<_> = std::env::args().skip(1).collect();
    let stateful_lifecycle = args.first().is_some_and(|s| s == "--stateful-lifecycle");
    let args = if stateful_lifecycle {
        &args[1..]
    } else {
        &args[..]
    };
    let data_replicator =
        stateful_lifecycle && args.first().is_some_and(|s| s == "--data-replicator");
    let args = if data_replicator { &args[1..] } else { args };
    let delay = match args {
        [flag, value]
            if flag == "--delay-ms"
                || flag == "--crash-after-ms"
                || flag == "--shutdown-after-ms" =>
        {
            Some((
                flag.as_str(),
                value.parse::<u64>().expect("integer milliseconds"),
            ))
        }
        [] => None,
        [flag] if flag == "--no-register" || flag == "--exit-before-attach" => None,
        [flag] if flag == "--stateful-register" => None,
        [flag] if flag == "--block-dispose" || flag == "--close-error-block-dispose" => None,
        [flag] if flag == "--block-provider-dispose" && data_replicator => None,
        [flag, _] if flag == "--open-delay-ms" => None,
        _ => panic!("unsupported fixture arguments"),
    };
    if args.first().is_some_and(|s| s == "--exit-before-attach") {
        std::process::exit(17);
    }
    if let Some(("--delay-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    let runtime = Runtime::attach().await.expect("owned Hosting attach");
    let mode = Mode {
        block_dispose: args
            .first()
            .is_some_and(|s| s == "--block-dispose" || s == "--close-error-block-dispose"),
        block_provider_dispose: args
            .first()
            .is_some_and(|s| s == "--block-provider-dispose"),
        close_error: args
            .first()
            .is_some_and(|s| s == "--close-error-block-dispose"),
        open_delay: if args.first().is_some_and(|s| s == "--open-delay-ms") {
            args[1].parse().expect("integer callback milliseconds")
        } else {
            0
        },
    };
    let stateful_registration = args.first().is_some_and(|s| s == "--stateful-register");
    let native = data_replicator.then(|| runtime.native_integration_provider());
    let _runtime = if args.first().is_some_and(|s| s == "--no-register") {
        runtime
    } else {
        runtime
            .execution_runtime()
            .try_reserve()
            .expect("execution reservation")
            .run_owned(move || {
                if stateful_registration || stateful_lifecycle {
                    runtime.register_stateful_service_factory(
                        "LocalStatefulType",
                        Box::new(StatefulFactory {
                            lifecycle: stateful_lifecycle,
                            native,
                            mode,
                        }),
                    )?;
                    marker("LOCAL_STATEFUL_REGISTERED");
                } else {
                    runtime.register_stateless_service_factory(
                        "LocalStatelessType",
                        Box::new(Factory(mode)),
                    )?;
                }
                Ok(runtime)
            })
            .await
            .expect("declared factory registration")
    };
    println!("LOCAL_REGISTERED");
    std::io::stdout()
        .flush()
        .expect("flush registration marker");
    if let Some(("--crash-after-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        std::process::exit(23);
    }
    if let Some(("--shutdown-after-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        _runtime
            .shutdown()
            .await
            .expect("orderly Runtime/session shutdown");
    }
    std::future::pending::<()>().await;
}
