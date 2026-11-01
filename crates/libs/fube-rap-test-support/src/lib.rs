//! Offline native-RAP MODEL host. Registration, lease, lifecycle and control
//! traverse real Runtime IPC/issuer/binder paths; this is NOT deployed SF proof.
//! Shared support depends only on public framework/runtime/transport contracts;
//! it exposes no data engine, diagnostics, lifecycle hook or control setter.
use fube_runtime::{
    extensions::{
        attach_runtime, NativeIntegrationProvider, NativeReplicaDescriptor,
        NativeReplicaSetConfiguration,
    },
    ActivationContext, Cancellation, Error, ErrorKind, IStatefulServiceFactory, ReplicaRole,
    Result,
};
use fube_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId},
    serialization::serde::{
        from_slice, to_vec, ByteArray, EmptyObjectArray, Guid, ObjectArray, Pointer,
    },
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, thread::JoinHandle};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, oneshot},
};

mod primary;
pub use primary::DelegatingPrimary;

// Field order mirrors Hosting2/ServiceModel FABRIC_FIELDS (3988b451); these
// private model records are neither public SDK descriptors nor authority APIs.
macro_rules! record {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        struct $name { $($field: $ty),* }
    };
}
record!(HostContext {
    id: String,
    kind: i64,
    pid: u32,
    container: bool,
    activator: bool
});
record!(ApplicationId {
    type_name: String,
    number: u32
});
record!(PackageId {
    application: ApplicationId,
    name: String
});
record!(Activation {
    guid: Guid,
    mode: i64
});
record!(PackageInstance {
    package: PackageId,
    activation: Activation,
    public_id: String
});
record!(CodeId {
    package: PackageInstance,
    name: String
});
record!(Rollout {
    major: u32,
    minor: u32
});
record!(ApplicationVersion { value: Rollout });
record!(PackageVersion {
    application: ApplicationVersion,
    value: Rollout
});
record!(VersionInstance {
    version: PackageVersion,
    instance: u64
});
record!(CodeContext {
    id: CodeId,
    code_sequence: i64,
    package_sequence: i64,
    version: VersionInstance,
    application_name: String
});
record!(RuntimeContext {
    id: String,
    host: HostContext,
    code: CodeContext
});
record!(RuntimeRequest {
    context: RuntimeContext
});
record!(TypeId {
    package: PackageInstance,
    name: String
});
record!(TypeRequest {
    context: RuntimeContext,
    id: TypeId
});
record!(Pair {
    key: String,
    value: String
});
record!(StartReply {
    node_id: String, node_pid: u32, lease_handle: u64, lease_duration: i64, error: i64,
    node_instance: u64, node_name: String, node_type: String, client_address: String,
    deployment: String, ip: String, work: String, initial_ttl: i64,
    application_directories: ObjectArray<Vec<Pair>>, node_directories: ObjectArray<Vec<Pair>>,
    application_log: Pointer<()>, system_log: Pointer<()>,
});
record!(ErrorReply { error: i64 });
record!(LeaseReply { ttl: i64 });
record!(PartitionId { id: Guid });
record!(ConsistencyUnit {
    id: PartitionId,
    low: i64,
    high: i64,
    name: String,
    kind: i64
});
record!(Epoch {
    configuration: i64,
    data_loss: i64
});
record!(FailoverUnit {
    id: PartitionId,
    consistency: ConsistencyUnit,
    current: Epoch,
    previous: Epoch,
    target: i32,
    minimum: i32
});
record!(Node {
    id: ByteArray,
    instance: u64
});
record!(Replica {
    node: Node,
    id: i64,
    instance: i64,
    current_role: i64,
    previous_role: i64,
    up: bool,
    last_lsn: i64,
    first_lsn: i64,
    state: i64,
    version: VersionInstance,
    service_endpoint: String,
    replication_endpoint: String,
});
record!(ServiceType {
    package: PackageId,
    name: String
});
record!(Correlation {
    service: String,
    scheme: i64
});
record!(Metric {
    name: String,
    weight: i64,
    primary: u32,
    secondary: u32
});
record!(Placement {
    domain: String,
    kind: i64
});
record!(Service {
    name: String, instance: u64, service_type: ServiceType, application: String,
    version: VersionInstance, partitions: i32, target: i32, minimum: i32,
    stateful: bool, persisted: bool, initialization: ByteArray, group: bool,
    correlations: ObjectArray<Vec<Correlation>>, constraints: String, scaleout: i32,
    metrics: ObjectArray<Vec<Metric>>, move_cost: u32, restart_wait: i64, quorum_wait: i64,
    update_version: u64, placement: ObjectArray<Vec<Placement>>, standby_keep: i64,
    activation_mode: i64, dns: String, scaling: EmptyObjectArray,
});
record!(ProxyRequest { runtime: String, unit: FailoverUnit, local: Replica, remote: ObjectArray<Vec<Replica>>, service: Service, flags: i64 });
record!(ApiName {
    api: i64,
    interface: i64,
    metadata: String
});
record!(ProxyError {
    error: i64,
    message: String,
    api: ApiName
});
record!(ProxyReply { unit: FailoverUnit, local: Replica, remote: ObjectArray<Vec<Replica>>, error: ProxyError, flags: i64 });
record!(ReplicaMessage {
    unit: FailoverUnit,
    local: Replica,
    service: Service
});

enum Action {
    Open,
    Close,
    Configure(NativeReplicaSetConfiguration),
    CatchUp(NativeReplicaSetConfiguration, NativeReplicaSetConfiguration),
    Build(NativeReplicaDescriptor),
    Remove(NativeReplicaDescriptor),
    Role(ReplicaRole),
    Epoch(fube_runtime::Epoch),
    Status,
}
struct Command {
    action: Action,
    result: oneshot::Sender<Result<()>>,
}
struct Pending {
    action: &'static str,
    request: ProxyRequest,
    result: oneshot::Sender<Result<()>>,
}

pub struct Host {
    opened_address: Arc<std::sync::Mutex<Option<String>>>,
    commands: mpsc::Sender<Command>,
    stop: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    thread: std::sync::Mutex<Option<JoinHandle<()>>>,
    events: broadcast::Sender<HostEvent>,
}

/// Observational model milestones, never execution barriers or cleanup receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostEvent {
    /// Cleanup intent selected once; outstanding command results may still be pending.
    CloseRequested,
    /// Actual ReplicaClose frame written, not its reply or successful disposal.
    CloseFrameSent,
}
/// Explicit model-node input, never a native invocation/proof constructor.
/// Runtime derives the actual binding only after these RAP bytes are admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSource {
    pub replica_id: i64,
    pub native_incarnation: i64,
    pub node_id: [u8; 16],
    pub node_instance: u64,
}
impl Host {
    pub async fn start(
        partition: uuid::Uuid,
        replica: i64,
        role: ReplicaRole,
        factory: impl FnOnce(NativeIntegrationProvider) -> Box<dyn IStatefulServiceFactory>
            + Send
            + 'static,
    ) -> Arc<Self> {
        Self::start_source(
            partition,
            ReplicaSource {
                replica_id: replica,
                native_incarnation: replica + 10,
                node_id: [replica as u8; 16],
                node_instance: 1,
            },
            role,
            factory,
        )
        .await
    }
    pub async fn start_source(
        partition: uuid::Uuid,
        source: ReplicaSource,
        role: ReplicaRole,
        factory: impl FnOnce(NativeIntegrationProvider) -> Box<dyn IStatefulServiceFactory>
            + Send
            + 'static,
    ) -> Arc<Self> {
        let (host, opened) = Self::start_source_outcome(partition, source, role, factory).await;
        opened.unwrap();
        host
    }
    /// Qualification-only failure fixture: preserve the actual native Open
    /// result instead of panicking before callers can inspect retained cleanup.
    pub async fn start_source_outcome(
        partition: uuid::Uuid,
        source: ReplicaSource,
        role: ReplicaRole,
        factory: impl FnOnce(NativeIntegrationProvider) -> Box<dyn IStatefulServiceFactory>
            + Send
            + 'static,
    ) -> (Arc<Self>, Result<()>) {
        Self::start_source_outcome_at_epoch(
            partition,
            source,
            role,
            fube_runtime::Epoch {
                data_loss_number: 1,
                configuration_number: 1 << 32,
            },
            factory,
        )
        .await
    }
    /// Configure the actual initial native ReplicaOpen request, not a lower
    /// epoch setter or a progress query against an unbuilt secondary.
    pub async fn start_source_at_epoch(
        partition: uuid::Uuid,
        source: ReplicaSource,
        role: ReplicaRole,
        epoch: fube_runtime::Epoch,
        factory: impl FnOnce(NativeIntegrationProvider) -> Box<dyn IStatefulServiceFactory>
            + Send
            + 'static,
    ) -> Arc<Self> {
        let (host, opened) =
            Self::start_source_outcome_at_epoch(partition, source, role, epoch, factory).await;
        opened.unwrap();
        host
    }
    pub async fn start_source_outcome_at_epoch(
        partition: uuid::Uuid,
        source: ReplicaSource,
        role: ReplicaRole,
        epoch: fube_runtime::Epoch,
        factory: impl FnOnce(NativeIntegrationProvider) -> Box<dyn IStatefulServiceFactory>
            + Send
            + 'static,
    ) -> (Arc<Self>, Result<()>) {
        let (ready, readiness) = oneshot::channel();
        let (commands, receive) = mpsc::channel(32);
        let shutdown_commands = commands.clone();
        let (stop, stopped) = oneshot::channel();
        let events = broadcast::channel(8).0;
        let peer_events = events.clone();
        let opened_address = Arc::new(std::sync::Mutex::new(None));
        let peer_address = opened_address.clone();
        let thread = std::thread::Builder::new()
            .name("rap-model-application".into())
            .spawn(move || {
                let application = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap();
                application.block_on(async move {
                    let artifacts = Artifacts::new();
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let context = artifacts.activation(listener.local_addr().unwrap());
                    let peer = tokio::spawn(serve(
                        listener,
                        receive,
                        artifacts.root.clone(),
                        partition,
                        source,
                        (role, epoch),
                        (peer_events, peer_address),
                    ));
                    let runtime = attach_runtime(context).await.unwrap();
                    let native = runtime.native_integration_provider();
                    let permit = runtime.execution_runtime().try_reserve().unwrap();
                    let runtime = permit
                        .run_owned(move || {
                            runtime.register_stateful_service_factory(
                                "ModelStatefulType",
                                factory(native),
                            )?;
                            Ok(runtime)
                        })
                        .await
                        .unwrap();
                    if ready.send(()).is_ok() {
                        let _ = stopped.await;
                        // This model owns its node commands. Observe pending
                        // results and actual ReplicaClose before host shutdown;
                        // a prior reply/observer Drop is not callback disposal.
                        observed_request(&shutdown_commands, Action::Close)
                            .await
                            .expect("model semantic ReplicaClose");
                    }
                    runtime.shutdown().await.unwrap();
                    peer.await.unwrap();
                    drop(artifacts);
                });
            })
            .unwrap();
        let host = Arc::new(Self {
            commands,
            stop: std::sync::Mutex::new(Some(stop)),
            thread: std::sync::Mutex::new(Some(thread)),
            events,
            opened_address,
        });
        readiness.await.expect("RAP model bootstrap");
        let mut opened = host.request(Action::Open).await;
        if opened.is_ok() && role == ReplicaRole::ActiveSecondary {
            opened = host.change_role(role).await;
        }
        (host, opened)
    }
    async fn request(&self, action: Action) -> Result<()> {
        observed_request(&self.commands, action).await
    }
    /// Owned test-profile observation; retains the original pending result even
    /// when the last Host observer is dropped. No command cancellation is inferred.
    pub async fn begin_epoch(&self, epoch: fube_runtime::Epoch) -> oneshot::Receiver<Result<()>> {
        let (result, receive) = oneshot::channel();
        self.commands
            .send(Command {
                action: Action::Epoch(epoch),
                result,
            })
            .await
            .expect("model epoch admitted");
        receive
    }
    /// Subscribe before triggering work. Bounded best-effort events have no replay;
    /// lag and closure are explicit receiver errors, not shutdown completion.
    pub fn subscribe_events(&self) -> broadcast::Receiver<HostEvent> {
        self.events.subscribe()
    }
    /// The address actually returned by public replicator Open and transported
    /// in the successful native ReplicaOpenReply, not lower diagnostics.
    pub fn replication_address(&self) -> Result<String> {
        self.opened_address
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::InvalidInput(
                "native Open has not returned an address",
            ))
    }
    pub async fn configure(&self, configuration: NativeReplicaSetConfiguration) -> Result<()> {
        self.request(Action::Configure(configuration)).await
    }
    pub async fn catch_up(
        &self,
        current: NativeReplicaSetConfiguration,
        previous: NativeReplicaSetConfiguration,
    ) -> Result<()> {
        self.request(Action::CatchUp(current, previous)).await
    }
    pub async fn change_role(&self, role: ReplicaRole) -> Result<()> {
        self.request(Action::Role(role)).await
    }
    pub async fn change_epoch(&self, epoch: fube_runtime::Epoch) -> Result<()> {
        self.request(Action::Epoch(epoch)).await
    }
    pub async fn get_status(&self) -> Result<()> {
        self.request(Action::Status).await
    }
    pub async fn build(
        &self,
        peer: NativeReplicaDescriptor,
        cancellation: Cancellation,
    ) -> Result<()> {
        tokio::select! {
            biased;
            result = self.request(Action::Build(peer.clone())) => result,
            _ = cancellation.cancelled() => {
                // Native Remove while Build is busy signals its advisory
                // cancellation. No completed-disposal or rollback is claimed.
                let (result, receive) = oneshot::channel();
                drop(receive);
                self.commands.send(Command { action: Action::Remove(peer), result }).await.map_err(|_| Error::Closed)?;
                Err(Error::new(ErrorKind::Canceled, "model native build canceled").uncertain())
            }
        }
    }
    pub async fn remove(&self, peer: NativeReplicaDescriptor) -> Result<()> {
        self.request(Action::Remove(peer)).await
    }
}
async fn observed_request(commands: &mpsc::Sender<Command>, action: Action) -> Result<()> {
    let (result, receive) = oneshot::channel();
    commands
        .send(Command { action, result })
        .await
        .map_err(|_| Error::Closed)?;
    tokio::time::timeout(std::time::Duration::from_secs(30), receive)
        .await
        .map_err(|_| Error::Timeout.uncertain())?
        .map_err(|_| Error::Closed)?
}
impl Drop for Host {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.get_mut().unwrap().take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.get_mut().unwrap().take() {
            thread.join().expect("RAP model host shutdown");
        }
    }
}
fn native_role(role: ReplicaRole) -> i64 {
    match role {
        ReplicaRole::Unknown => 0,
        ReplicaRole::None => 1,
        ReplicaRole::Primary => 4,
        ReplicaRole::IdleSecondary => 2,
        ReplicaRole::ActiveSecondary => 3,
    }
}
fn peer(replica: NativeReplicaDescriptor, version: &VersionInstance) -> Replica {
    Replica {
        node: Node {
            id: ByteArray(replica.node_id.to_vec()),
            instance: replica.node_instance,
        },
        id: replica.replica_id,
        instance: replica.incarnation,
        current_role: native_role(replica.role),
        previous_role: native_role(replica.previous_role),
        up: replica.up,
        last_lsn: replica.progress.last,
        first_lsn: replica.progress.first,
        state: if replica.ready { 2 } else { 1 },
        version: version.clone(),
        service_endpoint: String::new(),
        replication_endpoint: replica.address,
    }
}
fn base(
    context: &RuntimeContext,
    partition: uuid::Uuid,
    source: &ReplicaSource,
    role: ReplicaRole,
    epoch: fube_runtime::Epoch,
) -> ProxyRequest {
    let version = context.code.version.clone();
    ProxyRequest {
        runtime: context.id.clone(),
        unit: FailoverUnit {
            id: PartitionId {
                id: Guid(partition.to_bytes_le()),
            },
            consistency: ConsistencyUnit {
                id: PartitionId {
                    id: Guid(partition.to_bytes_le()),
                },
                low: 0,
                high: 0,
                name: String::new(),
                kind: 1,
            },
            current: Epoch {
                configuration: epoch.configuration_number,
                data_loss: epoch.data_loss_number,
            },
            previous: Epoch {
                configuration: 0,
                data_loss: 0,
            },
            target: 1,
            minimum: 1,
        },
        local: Replica {
            node: Node {
                id: ByteArray(source.node_id.to_vec()),
                instance: source.node_instance,
            },
            id: source.replica_id,
            instance: source.native_incarnation,
            current_role: if role == ReplicaRole::ActiveSecondary {
                2
            } else {
                native_role(role)
            },
            previous_role: 0,
            up: true,
            last_lsn: -1,
            first_lsn: -1,
            state: 2,
            version: version.clone(),
            service_endpoint: String::new(),
            replication_endpoint: String::new(),
        },
        remote: ObjectArray(vec![]),
        service: Service {
            name: "fabric:/RustApp/Data".into(),
            instance: 1,
            service_type: ServiceType {
                package: context.code.id.package.package.clone(),
                name: "ModelStatefulType".into(),
            },
            application: context.code.application_name.clone(),
            version,
            partitions: 1,
            target: 1,
            minimum: 1,
            stateful: true,
            persisted: false,
            initialization: ByteArray(vec![]),
            group: false,
            correlations: ObjectArray(vec![]),
            constraints: String::new(),
            scaleout: 0,
            metrics: ObjectArray(vec![]),
            move_cost: 0,
            restart_wait: 0,
            quorum_wait: 0,
            update_version: 1,
            placement: ObjectArray(vec![]),
            standby_keep: 0,
            activation_mode: 0,
            dns: String::new(),
            scaling: EmptyObjectArray,
        },
        flags: 0,
    }
}
fn control(base: &ProxyRequest) -> ProxyRequest {
    let mut request = base.clone();
    request.runtime.clear();
    request.remote.0.clear();
    request.service.name.clear();
    request.service.application.clear();
    request.service.service_type.name.clear();
    request.service.stateful = false;
    request.flags = 0;
    request
}
async fn send(
    stream: &mut tokio::net::tcp::OwnedWriteHalf,
    action: &str,
    request: &ProxyRequest,
    index: &mut u32,
) -> Result<()> {
    *index += 1;
    write_message(
        stream,
        &Message {
            headers: vec![
                Header::actor(12)?,
                Header::action(action)?,
                Header::message_id(MessageId {
                    guid: [0x73; 16],
                    index: *index,
                })?,
            ],
            body: to_vec(request)?,
        },
    )
    .await?;
    Ok(())
}
async fn serve(
    listener: TcpListener,
    mut commands: mpsc::Receiver<Command>,
    deployment: PathBuf,
    partition: uuid::Uuid,
    source: ReplicaSource,
    initial: (ReplicaRole, fube_runtime::Epoch),
    observation: (
        broadcast::Sender<HostEvent>,
        Arc<std::sync::Mutex<Option<String>>>,
    ),
) {
    let (role, epoch) = initial;
    let (events, opened_address) = observation;
    let (mut socket, _) = listener.accept().await.unwrap();
    let hello = read_message(&mut socket).await.unwrap();
    assert_eq!(hello.properties().unwrap().actor, 1);
    write_message(
        &mut socket,
        &Message::listen_instance(
            MessageId {
                guid: [0x74; 16],
                index: 1,
            },
            &ListenInstance::default(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let (mut reader, mut writer) = socket.into_split();
    let (read, mut messages) = mpsc::channel(32);
    let reader = tokio::spawn(async move {
        while let Ok(message) = read_message(&mut reader).await {
            if read.send(message).await.is_err() {
                break;
            }
        }
    });
    let mut context = None;
    let mut local = None::<ProxyRequest>;
    let mut pending = Vec::<Pending>::new();
    // One separately retained cleanup owner. Pending ordinary observations
    // complete first, preserving their original identities/results and the
    // newest acknowledged unit/epoch for the actual Close.
    let mut close_after_pending = None::<Command>;
    let mut sequence = 100;
    let mut retry = tokio::time::interval(std::time::Duration::from_millis(10));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            message = messages.recv() => {
                let Some(message) = message else { break; };
                let properties = message.properties().unwrap();
                let action = properties.action.as_str();
                let body = match action {
                    "StartRegisterApplicationHostRequest" => to_vec(&StartReply {
                        node_id: format!("{:032x}", u128::from_le_bytes(source.node_id)), node_pid: std::process::id(), lease_handle: 1, lease_duration: 300_000_000, error: 0,
                        node_instance: source.node_instance, node_name: "RAP_MODEL".into(), node_type: "MODEL".into(),
                        client_address: "127.0.0.1:19000".into(), deployment: deployment.to_string_lossy().into(), ip: "127.0.0.1".into(),
                        work: deployment.to_string_lossy().into(), initial_ttl: 0, application_directories: ObjectArray(vec![]), node_directories: ObjectArray(vec![]),
                        application_log: Pointer(None), system_log: Pointer(None),
                    }).unwrap(),
                    "RegisterFabricRuntimeRequest" => {
                        let registration: RuntimeRequest = from_slice(&message.body).unwrap();
                        context = Some(registration.context);
                        to_vec(&ErrorReply { error: 0 }).unwrap()
                    },
                    "RegisterServiceTypeRequest" => {
                        let registration: TypeRequest = from_slice(&message.body).unwrap();
                        assert_eq!(registration.id.name, "ModelStatefulType");
                        assert_eq!(registration.context.id, context.as_ref().unwrap().id);
                        local = Some(base(context.as_ref().unwrap(), partition, &source, role, epoch));
                        to_vec(&ErrorReply { error: 0 }).unwrap()
                    },
                    "FinishRegisterApplicationHostRequest" | "UnregisterFabricRuntimeRequest" | "UnregisterApplicationHostRequest" => to_vec(&ErrorReply { error: 0 }).unwrap(),
                    "" if properties.actor == 15 => to_vec(&LeaseReply { ttl: 300_000_000 }).unwrap(),
                    "ProxyReplicaEndpointUpdated" => {
                        let endpoint: ReplicaMessage = from_slice(&message.body).unwrap();
                        let mut ack = control(local.as_ref().unwrap());
                        ack.runtime = context.as_ref().unwrap().id.clone();
                        ack.unit = endpoint.unit; ack.local = endpoint.local;
                        send(&mut writer, "ProxyReplicaEndpointUpdatedReply", &ack, &mut sequence).await.unwrap();
                        continue;
                    },
                    action if action.ends_with("Reply") && properties.actor == 12 => {
                        let reply: ProxyReply = from_slice(&message.body).unwrap();
                        if let Some(index) = pending.iter().position(|p|
                            action == format!("{}Reply", p.action)
                            && reply.unit.id.id == p.request.unit.id.id
                            && reply.unit.current.configuration == p.request.unit.current.configuration
                            && reply.unit.current.data_loss == p.request.unit.current.data_loss
                            && reply.unit.previous.configuration == p.request.unit.previous.configuration
                            && reply.unit.previous.data_loss == p.request.unit.previous.data_loss
                            && reply.local.id == p.request.local.id
                            && reply.local.instance == p.request.local.instance
                            && reply.local.current_role == p.request.local.current_role
                            && to_vec(&reply.remote).unwrap() == to_vec(&p.request.remote).unwrap())
                        {
                            let pending = pending.remove(index);
                            if reply.error.error == 0 {
                                if pending.action == "ReplicaOpen" {
                                    *opened_address.lock().unwrap() = Some(reply.local.replication_endpoint.clone());
                                }
                                let state = local.as_mut().unwrap();
                                state.unit = reply.unit;
                                state.local = reply.local;
                            }
                            let result = if reply.error.error == 0 { Ok(()) } else { Err(Error::Remote { code: reply.error.error, message: reply.error.message }) };
                            let _ = pending.result.send(result);
                        }
                        continue;
                    },
                    "RAReportFault" | "RAPReportLoad" => continue,
                    other => panic!("unexpected MODEL host action {other}"),
                };
                write_message(&mut writer, &Message { headers: vec![Header::relates_to(properties.id).unwrap()], body }).await.unwrap();
            }
            command = async {
                if pending.is_empty() && close_after_pending.is_some() {
                    close_after_pending.take().map(|command| (command, false))
                } else {
                    commands.recv().await.map(|command| (command, true))
                }
            } => {
                let Some((command, fresh)) = command else { break; };
                if matches!(command.action, Action::Close) {
                    if fresh { let _ = events.send(HostEvent::CloseRequested); }
                    if !pending.is_empty() {
                        assert!(close_after_pending.is_none(), "single model cleanup owner");
                        close_after_pending = Some(command);
                        continue;
                    }
                }
                assert!(pending.len() < 32, "model native command bound");
                let closing = matches!(command.action, Action::Close);
                let mut request = match command.action { Action::Open | Action::Close => local.as_ref().unwrap().clone(), _ => control(local.as_ref().unwrap()) };
                let action = match command.action {
                    Action::Open => "ReplicaOpen",
                    Action::Close => "ReplicaClose",
                    Action::Configure(configuration) => {
                        request.flags = 1; // native end-reconfiguration/current configuration
                        request.unit.previous = Epoch { data_loss: 0, configuration: 0 };
                        request.unit.current = Epoch { data_loss: configuration.epoch.data_loss_number, configuration: configuration.epoch.configuration_number };
                        let mut me = request.local.clone(); me.state = 2;
                        if configuration.local_member { request.remote.0.push(me); }
                        request.remote.0.extend(configuration.replicas.into_iter().map(|r| peer(r, &request.local.version)));
                        assert_eq!(configuration.write_quorum, request.remote.0.len()/2 + 1);
                        request.unit.target = request.remote.0.len() as i32;
                        "UpdateConfiguration"
                    },
                    Action::CatchUp(current, previous) => {
                        request.flags = 2;
                        request.unit.current = Epoch { data_loss: current.epoch.data_loss_number, configuration: current.epoch.configuration_number };
                        request.unit.previous = if previous.epoch == fube_runtime::Epoch::INVALID { Epoch { data_loss: 0, configuration: 0 } } else { Epoch { data_loss: previous.epoch.data_loss_number, configuration: previous.epoch.configuration_number } };
                        let mut me = request.local.clone();
                        me.state = 2;
                        me.previous_role = if previous.local_member { 4 } else { 0 };
                        if current.local_member || previous.local_member {
                            if !current.local_member { me.current_role = 1; }
                            request.remote.0.push(me);
                        }
                        for item in current.replicas {
                            let mut native = peer(item, &request.local.version);
                            native.previous_role = previous.replicas.iter().find(|p| p.replica_id == native.id && p.node_id.as_slice() == native.node.id.0).map_or(0, |p| native_role(p.role));
                            request.remote.0.push(native);
                        }
                        for item in previous.replicas {
                            if request.remote.0.iter().any(|r| r.id == item.replica_id) { continue; }
                            let mut native = peer(item, &request.local.version);
                            native.previous_role = native.current_role;
                            native.current_role = 1;
                            request.remote.0.push(native);
                        }
                        let cc = request.remote.0.iter().filter(|r| r.current_role >= 3).count();
                        let pc = request.remote.0.iter().filter(|r| r.previous_role >= 3).count();
                        assert_eq!(current.write_quorum, cc / 2 + 1);
                        assert_eq!(previous.write_quorum, if pc == 0 { 0 } else { pc / 2 + 1 });
                        request.unit.target = cc as i32;
                        "UpdateConfiguration"
                    },
                    Action::Build(replica) => { request.remote.0.push(peer(replica, &request.local.version)); "ReplicatorBuildIdleReplica" },
                    Action::Remove(replica) => { request.remote.0.push(peer(replica, &request.local.version)); "ReplicatorRemoveIdleReplica" },
                    Action::Role(role) => {
                        request.local.current_role = native_role(role); request.local.state = 2;
                        request.remote.0.push(request.local.clone());
                        "UpdateConfiguration"
                    },
                    Action::Epoch(epoch) => {
                        request.unit.current = Epoch { data_loss: epoch.data_loss_number, configuration: epoch.configuration_number };
                        if request.local.current_role == 4 {
                            request.flags = 2;
                            request.remote.0.push(request.local.clone());
                            "UpdateConfiguration"
                        } else {
                            request.runtime = context.as_ref().unwrap().id.clone();
                            "ReplicatorUpdateEpochAndGetStatus"
                        }
                    },
                    Action::Status => {
                        request.runtime = context.as_ref().unwrap().id.clone();
                        "ReplicatorGetStatus"
                    },
                };
                send(&mut writer, action, &request, &mut sequence).await.unwrap();
                if closing { let _ = events.send(HostEvent::CloseFrameSent); }
                if !command.result.is_closed() { pending.push(Pending { action, request, result: command.result }); }
            }
            _ = retry.tick(), if !pending.is_empty() => {
                pending.retain(|p| !p.result.is_closed());
                for call in &pending { send(&mut writer, call.action, &call.request, &mut sequence).await.unwrap(); }
            }
        }
    }
    reader.abort();
    let _ = reader.await;
}

struct Artifacts {
    root: PathBuf,
}
impl Artifacts {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("rap-model-{}", uuid::Uuid::new_v4()));
        let base = root.join("RustApp_App0");
        std::fs::create_dir_all(&base).unwrap();
        let ns = "http://schemas.microsoft.com/2011/01/fabric";
        let code = r#"<CodePackage Name="Code" Version="1"><EntryPoint><ExeHost><Program>app</Program></ExeHost></EntryPoint></CodePackage>"#;
        let types = r#"<ServiceTypes><StatefulServiceType ServiceTypeName="ModelStatefulType" HasPersistedState="false"/></ServiceTypes>"#;
        std::fs::write(base.join("App.1.0.xml"), format!(r#"<ApplicationPackage xmlns="{ns}" ApplicationId="RustApp_App0" ApplicationTypeName="RustApp" NameUri="fabric:/RustApp" RolloutVersion="1.0"><DigestedEnvironment RolloutVersion="1.0"><Principals/><Policies/><Diagnostics/></DigestedEnvironment><DigestedCertificates RolloutVersion="1.0"/></ApplicationPackage>"#)).unwrap();
        std::fs::write(base.join("RustService.Manifest.1.xml"), format!(r#"<ServiceManifest xmlns="{ns}" Name="RustService" Version="1">{types}{code}</ServiceManifest>"#)).unwrap();
        let digest = code
            .replace("Version=\"1\"", "Version=\"1\" IsShared=\"false\"")
            .replace("<ExeHost>", "<ExeHost IsExternalExecutable=\"false\">")
            .replace(
                "</ExeHost>",
                "<WorkingFolder>Work</WorkingFolder></ExeHost>",
            );
        std::fs::write(base.join("RustService.Package.Current.xml"), format!(r#"<ServicePackage xmlns="{ns}" Name="RustService" ManifestVersion="1" RolloutVersion="2.0"><DigestedServiceTypes RolloutVersion="1.0">{types}</DigestedServiceTypes><DigestedCodePackage RolloutVersion="1.0" ContentChecksum="code" IsShared="false">{digest}</DigestedCodePackage><DigestedResources RolloutVersion="1.0"><DigestedEndpoints/></DigestedResources><Diagnostics/></ServicePackage>"#)).unwrap();
        std::fs::write(base.join("RustService.Endpoints.txt"), "").unwrap();
        Self { root }
    }
    fn activation(&self, address: std::net::SocketAddr) -> ActivationContext {
        let mut env: BTreeMap<String, String> = [
            ("Fabric_ApplicationHostId", uuid::Uuid::new_v4().to_string()),
            (
                "Fabric_ApplicationHostType",
                "Activated_SingleCodePackage".into(),
            ),
            ("Fabric_ApplicationId", "RustApp_App0".into()),
            ("Fabric_ServicePackageName", "RustService".into()),
            ("Fabric_CodePackageName", "Code".into()),
            ("Fabric_CodePackageInstanceSeqNum", "1".into()),
            ("Fabric_ServicePackageInstanceSeqNum", "2".into()),
            ("Fabric_ServicePackageVersionInstance", "1.0:2.0:3".into()),
            ("Fabric_ApplicationName", "fabric:/RustApp".into()),
            ("Fabric_Folder_App_Work", self.root.to_string_lossy().into()),
            ("Fabric_Folder_App_Log", self.root.to_string_lossy().into()),
            ("Fabric_Folder_App_Temp", self.root.to_string_lossy().into()),
            ("Fabric_NodeIPOrFQDN", "127.0.0.1".into()),
            ("Fabric_RuntimeConnectionUseSsl", "false".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v))
        .collect();
        env.insert(
            "Fabric_RuntimeConnectionAddress".into(),
            address.to_string(),
        );
        ActivationContext::from_node_environment(&env).unwrap()
    }
}
impl Drop for Artifacts {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}
