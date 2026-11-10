//! Explicitly minimal native-free local Hosting package fixtures.
use clap::Parser;
use fube_data::{
    extensions::ReplicationEndpointOptions, IOperation, IOperationDataStream, IStateProvider,
    MemoryProvider, Mutation,
};
use fube_runtime::{
    extensions::{
        EpochDomains, NativeIntegrationProvider, NativeRuntimeIntegration, ReplicaLifecycle,
    },
    ApiFuture, Cancellation, Epoch, Error, IPrimaryReplicator, IReplicator,
    IStatefulServiceFactory, IStatefulServicePartition, IStatefulServiceReplica,
    IStatelessServiceFactory, IStatelessServiceInstance, IStatelessServicePartition, LoadMetric,
    OpenMode, ReplicaInformation, ReplicaRole, ReplicaSetConfig, ReplicaSetQuorumMode, Result,
    Runtime, RuntimeReplicator, ServicePartitionAccessStatus,
};
use std::{
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(version, about = "Native-free local Hosting lifecycle fixture",
    group(clap::ArgGroup::new("fault").args([
        "delay_ms", "crash_after_ms", "shutdown_after_ms", "no_register",
        "exit_before_attach", "stateful_register", "block_dispose",
        "close_error_block_dispose", "block_provider_dispose", "open_delay_ms",
        "block_registration",
    ])),
    group(clap::ArgGroup::new("data").args(["stateful_primary", "data_replicator"]).multiple(true)))]
struct Arguments {
    #[arg(long, conflicts_with_all = ["stateful_lifecycle", "data_replicator"])]
    stateful_primary: bool,
    #[arg(long)]
    stateful_lifecycle: bool,
    #[arg(long, requires = "stateful_lifecycle")]
    data_replicator: bool,
    #[arg(long)]
    delay_ms: Option<u64>,
    #[arg(long)]
    crash_after_ms: Option<u64>,
    #[arg(long)]
    shutdown_after_ms: Option<u64>,
    #[arg(long)]
    no_register: bool,
    #[arg(long)]
    exit_before_attach: bool,
    #[arg(long)]
    stateful_register: bool,
    #[arg(long)]
    block_dispose: bool,
    #[arg(long)]
    close_error_block_dispose: bool,
    #[arg(long, requires = "data")]
    block_provider_dispose: bool,
    #[arg(long)]
    open_delay_ms: Option<u64>,
    #[arg(
        long,
        requires = "stateful_lifecycle",
        conflicts_with_all = ["open_delay_ms", "stateful_primary"]
    )]
    block_open: bool,
    #[arg(long, requires = "stateful_lifecycle", conflicts_with_all = ["stateful_primary", "block_open"])]
    block_registration: bool,
}

#[derive(Clone, Copy, Default)]
struct Mode {
    primary: bool,
    block_dispose: bool,
    block_provider_dispose: bool,
    close_error: bool,
    open_delay: u64,
    block_open: bool,
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
    memory: Mutex<Option<Arc<MemoryProvider>>>,
    secondary: Mutex<Option<SecondaryPump>>,
    primary: Mutex<Option<PrimaryService>>,
}
struct ObservedProvider {
    memory: Arc<MemoryProvider>,
    resource: PathBuf,
    block_dispose: bool,
    primary: bool,
}
struct PrimaryService {
    address: String,
    stopped: Cancellation,
    thread: Option<JoinHandle<()>>,
}
struct SecondaryPump {
    stopped: Cancellation,
    thread: Option<JoinHandle<()>>,
}
struct PrimaryState {
    native: NativeRuntimeIntegration,
    partition: Arc<dyn IStatefulServicePartition>,
    data: Arc<RuntimeReplicator>,
    memory: Arc<MemoryProvider>,
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
            memory: Mutex::new(None),
            secondary: Mutex::new(None),
            primary: Mutex::new(None),
        }))
    }
}
impl IStatefulServiceReplica for StatefulReplica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        cancel: Cancellation,
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
                    primary: self.resource.mode.primary,
                });
                let memory = provider.memory.clone();
                *self.memory.lock().unwrap() = Some(memory.clone());
                marker("LOCAL_DATA_PROVIDER acquired");
                let control = native.create_replicator(
                    partition.clone(),
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
                if self.resource.mode.primary {
                    let service = PrimaryService::start(PrimaryState {
                        native: native.clone(),
                        partition,
                        data: control.clone(),
                        memory,
                    })?;
                    marker(&format!(
                        "LOCAL_PRIMARY listening endpoint={}",
                        service.address
                    ));
                    *self.primary.lock().unwrap() = Some(service);
                }
                Box::new((*control).clone())
            } else {
                Box::new(LocalControl(Mutex::new(None)))
            };
            tokio::time::sleep(Duration::from_millis(self.resource.mode.open_delay)).await;
            if self.resource.mode.block_open {
                marker("LOCAL_OPEN held");
                let release = self.resource.resource.with_file_name("local-open.release");
                std::fs::write(release.with_file_name("local-open.held"), b"original Open")
                    .expect("publish fixture Open barrier");
                tokio::time::timeout(Duration::from_secs(5), async {
                    let mut cancelled = false;
                    while !release.exists() {
                        if cancel.is_cancelled() && !cancelled {
                            marker("LOCAL_OPEN cancelled");
                            std::fs::write(
                                release.with_file_name("local-open.cancelled"),
                                b"original Close",
                            )
                            .expect("publish fixture Open cancellation");
                            cancelled = true;
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .expect("bounded fixture Open barrier timed out");
                marker("LOCAL_OPEN released");
            }
            Ok(control)
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async move {
            if !matches!(role, ReplicaRole::IdleSecondary | ReplicaRole::None)
                && !(role == ReplicaRole::Primary && self.resource.mode.primary)
            {
                return Err(Error::Protocol(
                    "fixture prohibits Primary/secondary promotion",
                ));
            }
            marker(&format!("LOCAL_CALLBACK role {role:?}"));
            if role == ReplicaRole::Primary {
                let data = self.data.lock().unwrap().clone().expect("real data owner");
                let before = data.snapshot();
                assert_eq!(before.role, ReplicaRole::Primary);
                assert!(!before.ready);
                let operation = Mutation {
                    id: "before-ready".into(),
                    key: "value".into(),
                    value: "must not be admitted".into(),
                }
                .operation()?;
                let error = data
                    .get_state_replicator()
                    .replicate(operation, Cancellation::default())
                    .await
                    .err()
                    .expect("write before service role completion must reject");
                assert_eq!(
                    error.kind(),
                    fube_runtime::ErrorKind::ReconfigurationPending
                );
                assert_eq!(data.snapshot().progress, before.progress);
                marker("LOCAL_PRIMARY write_denied before_ready assigned=0");
                return Ok(self
                    .primary
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("owned primary listener")
                    .address
                    .clone());
            }
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
                    let mut secondary = self.secondary.lock().unwrap();
                    if secondary.is_some() {
                        return Err(Error::Protocol("Idle pump already owns the streams"));
                    }
                    *secondary = Some(SecondaryPump::start(
                        self.native.as_ref().expect("real data integration").clone(),
                        partition,
                        data,
                        self.memory.lock().unwrap().clone().expect("real provider"),
                    )?);
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
        if let Some(mut secondary) = self.secondary.lock().unwrap().take() {
            secondary.join();
        }
        if let Some(mut primary) = self.primary.lock().unwrap().take() {
            primary.join();
        }
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
        if state.copy_in_progress {
            assert!(!self.primary, "Primary cannot own partial destination copy");
        }
        if state.last != 0 {
            assert!((1..=2).contains(&state.last));
            assert_eq!(
                state.values.get("value").map(String::as_str),
                Some(if state.last == 1 {
                    "acknowledged"
                } else {
                    "continued"
                })
            );
            assert_eq!(state.results.get("primary-write"), Some(&1));
            if state.last == 2 {
                assert_eq!(state.results.get("primary-write-2"), Some(&2));
            }
            assert_eq!(
                (state.values.len(), state.results.len()),
                (1, state.last as usize)
            );
        } else {
            assert_eq!(state.last, 0);
            assert!(state.values.is_empty() && state.results.is_empty());
        }
        std::fs::remove_file(&self.resource).expect("dispose actual data provider resource");
        marker(if state.last == 0 {
            "LOCAL_DATA_PROVIDER disposed empty"
        } else {
            "LOCAL_DATA_PROVIDER disposed written"
        });
    }
}
impl PrimaryState {
    fn authority(&self, write: bool) -> Result<()> {
        let native = self.native.status()?;
        let engine = self.data.snapshot();
        let access = if write {
            self.partition.get_write_status()?
        } else {
            self.partition.get_read_status()?
        };
        if native.lifecycle != ReplicaLifecycle::Open
            || !native.replicator_open
            || native.current_role != ReplicaRole::Primary
            || native.service_role != ReplicaRole::Primary
            || native.replicator_role != ReplicaRole::Primary
            || engine.role != ReplicaRole::Primary
            || !engine.ready
            || engine.closed
            || engine.fault.is_some()
            || engine.epoch != Some(native.current_epoch.primary())
            || access != ServicePartitionAccessStatus::Granted
        {
            return Err(Error::new(
                fube_runtime::ErrorKind::NotPrimary,
                "fixture is not serving",
            ));
        }
        Ok(())
    }
    async fn request(&self, request: &[u8], stopped: Cancellation) -> Result<String> {
        match request {
            b"write\n" | b"write-more\n" => {
                self.authority(true)?;
                let more = request == b"write-more\n";
                if more && !self.memory.snapshot().results.contains_key("primary-write") {
                    return Err(Error::InvalidInput(
                        "continuing write requires the original write",
                    ));
                }
                let mutation = Mutation {
                    id: if more {
                        "primary-write-2"
                    } else {
                        "primary-write"
                    }
                    .into(),
                    key: "value".into(),
                    value: if more { "continued" } else { "acknowledged" }.into(),
                };
                if self.memory.admission(&mutation)?.is_some() {
                    return Err(Error::InvalidInput("fixture request already acknowledged"));
                }
                let operation =
                    mutation.operation_for_epoch(self.native.status()?.current_epoch.primary())?;
                let mut admission = self
                    .data
                    .get_state_replicator()
                    .replicate(operation.clone(), stopped.clone())
                    .await?;
                let sequence = admission.sequence_number;
                self.memory.apply_replication(sequence, &operation)?;
                assert_eq!(admission.completion.wait(stopped).await?, sequence);
                self.authority(true)?;
                marker(&format!("LOCAL_PRIMARY acknowledged sequence={sequence}"));
            }
            b"read\n" => self.authority(false)?,
            b"state\n" => {
                self.authority(false)?;
                return Ok(format!(
                    "{}\n",
                    serde_json::json!({
                        "provider":self.memory.snapshot(), "peers":self.data.snapshot().peers,
                    })
                ));
            }
            b"load\n" => {
                self.authority(false)?;
                let partition = self.partition.clone();
                self.native
                    .execution()
                    .try_reserve()?
                    .run_owned(move || {
                        partition.report_load(&[LoadMetric {
                            name: "LocalWork".into(),
                            value: 7,
                        }])
                    })
                    .await?;
                return Ok("OK load=7\n".into());
            }
            _ => {
                return Err(Error::InvalidInput(
                    "fixture accepts only read/write/write-more/state/load",
                ))
            }
        }
        let native = self.native.status()?;
        let engine = self.data.snapshot();
        let provider = self.memory.snapshot();
        Ok(format!(
            "OK role=Primary epoch={}:{} read={:?} write={:?} ready={} assigned={} applied={} committed={} value={}\n",
            native.current_epoch.data_loss_number,
            native.current_epoch.configuration_number,
            self.partition.get_read_status()?,
            self.partition.get_write_status()?,
            engine.ready,
            engine.progress.assigned,
            provider.last,
            engine.progress.committed,
            provider.values.get("value").map_or("", String::as_str),
        ))
    }
}
impl PrimaryService {
    fn start(state: PrimaryState) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?.to_string();
        listener.set_nonblocking(true)?;
        let stopped = Cancellation::default();
        let stop = stopped.clone();
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("local-primary".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("primary fixture runtime");
                runtime.block_on(async {
                    let listener = match tokio::net::TcpListener::from_std(listener) {
                        Ok(listener) => listener,
                        Err(error) => {
                            ready.send(Err(error)).expect("report listener failure");
                            return;
                        }
                    };
                    ready.send(Ok(())).expect("report owned listener startup");
                    loop {
                        let accepted = tokio::select! {
                            biased;
                            _ = stop.cancelled() => break,
                            accepted = listener.accept() => accepted,
                        };
                        let (mut stream, _) = accepted.expect("primary listener accept");
                        let mut bytes = Vec::new();
                        let input = tokio::time::timeout(Duration::from_secs(2), async {
                            for _ in 0..16 {
                                let byte = stream.read_u8().await?;
                                bytes.push(byte);
                                if byte == b'\n' {
                                    break;
                                }
                            }
                            Ok::<_, std::io::Error>(())
                        })
                        .await;
                        let result = match input {
                            Ok(Ok(())) => state.request(&bytes, stop.clone()).await,
                            Ok(Err(error)) => Err(error.into()),
                            Err(_) => Err(Error::Timeout),
                        };
                        let response = match result {
                            Ok(response) => response,
                            Err(error) => {
                                marker(&format!("LOCAL_PRIMARY request_error {error}"));
                                format!("ERR {:?}\n", error.kind())
                            }
                        };
                        if let Err(error) = tokio::time::timeout(
                            Duration::from_secs(2),
                            stream.write_all(response.as_bytes()),
                        )
                        .await
                        .map_err(|_| Error::Timeout)
                        .and_then(|result| result.map_err(Error::from))
                        {
                            marker(&format!("LOCAL_PRIMARY response_error {error}"));
                        }
                    }
                });
                marker("LOCAL_PRIMARY released");
            })?;
        if let Err(error) = readiness.recv().expect("primary listener startup receipt") {
            thread.join().expect("join failed listener startup");
            return Err(error.into());
        }
        Ok(Self {
            address,
            stopped,
            thread: Some(thread),
        })
    }
    fn join(&mut self) {
        self.stopped.cancel();
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join actual primary listener");
        }
    }
}
impl Drop for PrimaryService {
    fn drop(&mut self) {
        self.join();
    }
}

fn apply_secondary(
    provider: &MemoryProvider,
    operation: &dyn IOperation,
    copy: bool,
) -> Result<()> {
    let data = operation.get_data()?;
    if copy {
        provider.apply_copy(&data)?;
    } else {
        provider.apply_replication(operation.get_metadata()?.sequence_number, &data)?;
    }
    operation.acknowledge()
}

impl SecondaryPump {
    fn start(
        native: NativeRuntimeIntegration,
        partition: Arc<dyn IStatefulServicePartition>,
        data: Arc<RuntimeReplicator>,
        memory: Arc<MemoryProvider>,
    ) -> Result<Self> {
        let state = data.get_state_replicator();
        let copy = state.get_copy_stream()?;
        let replication = state.get_replication_stream()?;
        let data = Arc::downgrade(&data);
        let stopped = Cancellation::default();
        let stop = stopped.clone();
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let thread =
            std::thread::Builder::new()
                .name("local-idle-copy".into())
                .spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            ready.send(Err(error)).expect("report pump startup failure");
                            return;
                        }
                    };
                    ready.send(Ok(())).expect("report owned pump startup");
                    runtime.block_on(async {
                        let result = async {
                        // Copy EOS and replication EOS are independent. Consuming copy
                        // first prevents application replication into a partial snapshot.
                        for (stream, copying) in [(copy, true), (replication, false)] {
                            loop {
                                let operation = tokio::select! {
                                    biased;
                                    _ = stop.cancelled() => return Ok(()),
                                    item = stream.get_operation(stop.clone()) => item?,
                                };
                                let Some(operation) = operation else { break };
                                native.status()?;
                                apply_secondary(&memory, operation.as_ref(), copying)?;
                                if !memory.snapshot().copy_in_progress {
                                    let owner = data.upgrade().ok_or(Error::Closed)?;
                                    marker(&format!("LOCAL_IDLE_DATA {}", serde_json::json!({
                                        "provider":memory.snapshot(),
                                        "role":format!("{:?}",owner.snapshot().role),
                                        "ready":owner.snapshot().ready,
                                        "read":format!("{:?}",partition.get_read_status()?),
                                        "write":format!("{:?}",partition.get_write_status()?),
                                        "application_acknowledged":true,
                                    })));
                                }
                            }
                        }
                        Ok::<(), Error>(())
                    }.await;
                        if let Err(error) = result {
                            if !stop.is_cancelled() {
                                if let Some(data) =
                                    data.upgrade().filter(|data| !data.snapshot().closed)
                                {
                                    marker(&format!("LOCAL_IDLE_DATA_ERROR {error}"));
                                    data.close(Cancellation::default())
                                        .await
                                        .expect("fence failed destination pump");
                                }
                            }
                        }
                    });
                })?;
        if let Err(error) = readiness.recv().expect("pump startup receipt") {
            thread.join().expect("join failed pump startup");
            return Err(error.into());
        }
        Ok(Self {
            stopped,
            thread: Some(thread),
        })
    }
    fn join(&mut self) {
        self.stopped.cancel();
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .expect("join actual destination application pump");
        }
    }
}
impl Drop for SecondaryPump {
    fn drop(&mut self) {
        self.join();
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
    let args = Arguments::parse();
    let primary = args.stateful_primary;
    let stateful_lifecycle = primary || args.stateful_lifecycle;
    let data_replicator = primary || args.data_replicator;
    if args.exit_before_attach {
        std::process::exit(17);
    }
    if let Some(ms) = args.delay_ms {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    let runtime = Runtime::attach().await.expect("owned Hosting attach");
    if args.block_registration {
        let work =
            PathBuf::from(std::env::var_os("Fabric_Folder_App_Work").expect("owned work folder"));
        std::fs::write(
            work.join("local-registration.held"),
            b"original registration",
        )
        .expect("publish fixture registration barrier");
        marker("LOCAL_REGISTRATION held");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !work.join("local-registration.release").exists() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("bounded fixture registration barrier timed out");
        marker("LOCAL_REGISTRATION released");
    }
    let mode = Mode {
        primary,
        block_dispose: args.block_dispose || args.close_error_block_dispose,
        block_provider_dispose: args.block_provider_dispose,
        close_error: args.close_error_block_dispose,
        open_delay: args.open_delay_ms.unwrap_or(0),
        block_open: args.block_open,
    };
    let stateful_registration = args.stateful_register;
    let native = data_replicator.then(|| runtime.native_integration_provider());
    let _runtime = if args.no_register {
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
    if args.block_registration {
        std::fs::write(
            PathBuf::from(std::env::var_os("Fabric_Folder_App_Work").expect("owned work folder"))
                .join("local-registration.complete"),
            b"original factory registered",
        )
        .expect("publish fixture registration completion");
    }
    if let Some(ms) = args.crash_after_ms {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        std::process::exit(23);
    }
    if let Some(ms) = args.shutdown_after_ms {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        _runtime
            .shutdown()
            .await
            .expect("orderly Runtime/session shutdown");
    }
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod argument_tests {
    use super::*;

    struct Operation {
        sequence: i64,
        data: fube_data::OperationData,
        acknowledgement: Box<dyn Fn() -> Result<()> + Send + Sync>,
    }
    impl IOperation for Operation {
        fn get_metadata(&self) -> Result<fube_data::OperationMetadata> {
            Ok(fube_data::OperationMetadata {
                operation_type: fube_data::OperationType::Normal,
                sequence_number: self.sequence,
                atomic_group_id: -1,
            })
        }
        fn get_data(&self) -> Result<fube_data::OperationData> {
            Ok(self.data.clone())
        }
        fn acknowledge(&self) -> Result<()> {
            (self.acknowledgement)()
        }
    }

    #[tokio::test]
    async fn destination_applies_copy_and_replication_before_ack_and_never_acks_failed_apply() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let source = MemoryProvider::new();
        let target = MemoryProvider::new();
        let first = Mutation {
            id: "original".into(),
            key: "value".into(),
            value: "copied".into(),
        }
        .operation()
        .unwrap();
        source.apply_replication(1, &first).unwrap();
        let stream = source.get_copy_state(1, None).unwrap();
        let mut copied = Vec::new();
        while let Some(data) = stream.get_next(Cancellation::default()).await.unwrap() {
            copied.push(data);
        }
        let count = copied.len();
        let acknowledgements = Arc::new(AtomicUsize::new(0));
        for (index, data) in copied.into_iter().enumerate() {
            let target_at_ack = target.clone();
            let acknowledgements = acknowledgements.clone();
            let operation = Operation {
                sequence: index as i64 + 1,
                data,
                acknowledgement: Box::new(move || {
                    if index + 1 == count {
                        let state = target_at_ack.snapshot();
                        assert!(!state.copy_in_progress);
                        assert_eq!(state.last, 1);
                        assert_eq!(state.values["value"], "copied");
                        assert_eq!(state.results["original"], 1);
                    }
                    acknowledgements.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            };
            apply_secondary(&target, &operation, true).unwrap();
        }
        let at_ack = target.clone();
        let acks = acknowledgements.clone();
        let second = Mutation {
            id: "tail".into(),
            key: "value".into(),
            value: "replicated".into(),
        }
        .operation()
        .unwrap();
        let operation = Operation {
            sequence: 2,
            data: second.clone(),
            acknowledgement: Box::new(move || {
                let state = at_ack.snapshot();
                assert_eq!(state.last, 2);
                assert_eq!(state.values["value"], "replicated");
                assert_eq!(state.results["tail"], 2);
                acks.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        };
        apply_secondary(&target, &operation, false).unwrap();
        assert_eq!(acknowledgements.load(Ordering::SeqCst), count + 1);
        let invalid = Operation {
            sequence: 4,
            data: Mutation {
                id: "gap".into(),
                key: "value".into(),
                value: "invalid".into(),
            }
            .operation()
            .unwrap(),
            acknowledgement: Box::new(|| panic!("failed apply cannot acknowledge")),
        };
        assert!(apply_secondary(&target, &invalid, false).is_err());
        assert_eq!(target.snapshot().last, 2);
    }

    fn parse(suffix: &str) -> std::result::Result<Arguments, clap::Error> {
        let input = format!("hosting-local-app {suffix}");
        Arguments::try_parse_from(input.split_whitespace())
    }

    #[test]
    fn preserves_stateful_data_and_exclusive_fault_modes() {
        assert!(!parse("").unwrap().stateful_lifecycle);
        let args = parse("--stateful-lifecycle --data-replicator --open-delay-ms 4000").unwrap();
        assert!(args.stateful_lifecycle && args.data_replicator);
        assert_eq!(args.open_delay_ms, Some(4000));
        assert!(
            parse("--stateful-primary --block-provider-dispose")
                .unwrap()
                .stateful_primary
        );
        assert!(parse("--stateful-register").unwrap().stateful_register);
        assert!(parse("--delay-ms 0").is_ok());
        assert!(
            parse("--stateful-lifecycle --data-replicator --block-registration")
                .unwrap()
                .block_registration
        );
        assert!(
            parse("--stateful-lifecycle --data-replicator --block-open --block-dispose")
                .unwrap()
                .block_open
        );
    }

    #[test]
    fn rejects_conflicting_profiles_faults_and_invalid_delays() {
        for suffix in [
            "--stateful-primary --stateful-lifecycle",
            "--stateful-primary --data-replicator",
            "--data-replicator",
            "--block-provider-dispose",
            "--stateful-lifecycle --block-provider-dispose",
            "--no-register --delay-ms 1",
            "--block-dispose --close-error-block-dispose",
            "--delay-ms",
            "--open-delay-ms invalid",
            "--block-open",
            "--stateful-primary --block-open",
            "--stateful-lifecycle --block-open --open-delay-ms 1",
            "--block-registration",
            "--stateful-primary --block-registration",
            "--stateful-lifecycle --block-registration --block-open",
            "--delay-ms -1",
            "--crash-after-ms 18446744073709551616",
        ] {
            assert!(parse(suffix).is_err(), "{suffix}");
        }
    }
}
