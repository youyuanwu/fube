//! Real SF-activated sample. It never constructs activation identity itself.
use fube_runtime::extensions::{
    CatchUpResult, ConfigurationStage, EpochDomains, InstanceIntegrationProvider,
    InstanceRuntimeIntegration, NativeIntegrationProvider,
    NativeReplicaDescriptor as ReplicaInformation,
    NativeReplicaSetConfiguration as ReplicaSetConfiguration, NativeRuntimeIntegration,
    ReplicaDiagnostics as ReplicaStatus, ReplicaIdentity, ReplicaLifecycle, ReplicaRemoval,
    ReplicationProgress, ServiceIdentity,
};
use fube_runtime::*;
use fube_runtime::{ApiFuture as ServiceFuture, Error as RuntimeError, Result as RuntimeResult};
use fube_runtime::{
    ReplicaSetQuorumMode as CatchUpMode, ServicePartitionAccessStatus as AccessStatus,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
#[cfg(test)]
use tokio::sync::Semaphore;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::OnceCell,
};
mod data;
mod stateful;
mod task_ownership;

fn config_evidence(package: &ConfigurationPackage) -> RuntimeResult<serde_json::Value> {
    let description = package.description();
    Ok(serde_json::json!({
        "name":description.name(),"version":description.version(),"rollout":description.rollout(),
        "manifest_version":description.manifest_version(),"path":description.path(),
        "path_exists":description.path().is_dir(),
        "value":package.section("Main")?.parameter("Value")?.plaintext()?,
        "override":package.section("Main")?.parameter("Override")?.plaintext()?,
    }))
}

struct Route {
    partition: InstanceRuntimeIntegration,
    creation_generation: u64,
    creation_configuration: String,
    active: AtomicUsize,
    deliberately_noncooperative: bool,
}
struct Rpc {
    current: ActivationContextHandle,
    instances: InstanceIntegrationProvider,
    native: NativeIntegrationProvider,
    initial: Arc<ActivationContext>,
    retained_config: Arc<ConfigurationPackage>,
    package_events: Arc<Mutex<Vec<serde_json::Value>>>,
    unregistered_callbacks: Arc<AtomicUsize>,
    ip: String,
    port: u16,
    node: String,
    node_type: String,
    host: String,
    code_sequence: i64,
    listener: OnceCell<()>,
    listener_tasks: task_ownership::Listener,
    routes: Mutex<HashMap<String, Arc<Route>>>,
    stateful_routes: Mutex<HashMap<String, Arc<stateful::ReplicaRoute>>>,
    data_routes: Mutex<HashMap<String, Arc<data::Route>>>,
    peers: Arc<stateful::PeerServer>,
    retired: Mutex<VecDeque<InstanceRuntimeIntegration>>,
    metrics: Arc<RuntimeMetrics>,
    opening: AtomicUsize,
    maximum_opening: AtomicUsize,
    node_host_pid: u32,
    initial_ttl: i64,
    fresh_poll_ttl: i64,
    fresh_poll_elapsed_micros: u64,
    deliberately_noncooperative: AtomicBool,
}
impl Rpc {
    fn package_evidence(&self) -> RuntimeResult<serde_json::Value> {
        let current = self.current.snapshot();
        let config = current.packages()?.configuration_package("Config")?;
        let data = current.packages()?.data_package("Data")?;
        let initial_config = self.initial.packages()?.configuration_package("Config")?;
        Ok(serde_json::json!({
            "generation":current.generation(), "runtime_id":current.runtime_id(),
            "package_sequence":current.package_sequence,
            "package_version_instance":current.package_version_instance(),
            "activation_instance":current.code_activation_instance(),
            "configuration":config_evidence(&config)?,
            "initial_configuration":config_evidence(&initial_config)?,
            "retained_configuration":config_evidence(&self.retained_config)?,
            "initial_generation":self.initial.generation(),
            "data":{"version":data.description().version(),"rollout":data.description().rollout(),
                "path":data.description().path(),"path_exists":data.description().path().is_dir()},
            "events":self.package_events.lock().unwrap().clone(),
            "unregistered_callbacks":self.unregistered_callbacks.load(Ordering::Acquire),
        }))
    }
    async fn listen(self: &Arc<Self>) -> RuntimeResult<()> {
        self.listener
            .get_or_try_init(|| async {
                let listener = TcpListener::bind((self.ip.as_str(), self.port)).await?;
                let server = self.clone();
                self.listener_tasks
                    .start(listener, Duration::from_secs(35), move |stream| {
                        let server = server.clone();
                        async move {
                            let _ = server.request(stream).await;
                        }
                    })?;
                RuntimeResult::Ok(())
            })
            .await?;
        Ok(())
    }
    // Both listeners are shared application resources. Unit Close/Abort removes
    // only its own route and drains its active requests, not other units' work.
    async fn shutdown_listeners(&self) -> RuntimeResult<()> {
        self.listener_tasks.stop();
        self.peers.stop();
        let (rpc, peers) = tokio::join!(self.listener_tasks.join(), self.peers.join());
        rpc?;
        peers
    }
    async fn request(&self, mut stream: TcpStream) -> RuntimeResult<()> {
        let mut buffer = [0; 4096];
        let mut size = 0;
        loop {
            if size == buffer.len() {
                return Err(RuntimeError::InvalidInput("RPC header limit"));
            }
            let read = stream.read(&mut buffer[size..]).await?;
            if read == 0 {
                return Ok(());
            }
            size += read;
            if buffer[..size].windows(4).any(|v| v == b"\r\n\r\n") {
                break;
            }
        }
        let request = std::str::from_utf8(&buffer[..size])
            .map_err(|_| RuntimeError::InvalidInput("RPC encoding"))?;
        let path = request
            .lines()
            .next()
            .and_then(|v| v.strip_prefix("GET "))
            .and_then(|v| v.split_once(' '))
            .map(|(p, _)| p)
            .unwrap_or("");
        let (path, query) = path.split_once('?').unwrap_or((path, ""));
        if path.starts_with("/stateful/") {
            return self.stateful_request(stream, path, query).await;
        }
        if path.starts_with("/data/") {
            return self.data_request(stream, path, query).await;
        }
        let parameters: HashMap<_, _> =
            query.split('&').filter_map(|s| s.split_once('=')).collect();
        let route = {
            let routes = self.routes.lock().unwrap();
            routes.get(path).map(|route| {
                route.active.fetch_add(1, Ordering::SeqCst);
                route.clone()
            })
        };
        struct Active(Arc<Route>);
        impl Drop for Active {
            fn drop(&mut self) {
                self.0.active.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let (status, body, _active) = if let Some(route) = route {
            let active = Active(route.clone());
            // Explicit acceptance-only failure injection: prove that even an
            // application that ignores invalid partition authority cannot serve
            // beyond the independent process watchdog. Default services check it.
            if route.deliberately_noncooperative || route.partition.validate().is_ok() {
                let id = route.partition.identity();
                let counters = self.metrics.snapshot();
                let report = self.report(&route.partition, &parameters).await;
                (200,serde_json::json!({"pid":std::process::id(),"partition":id.partition_id.to_string(),
                    "instance":id.instance_id,"incarnation":id.incarnation,"service":id.service_name,
                    "node":self.node,"node_type":self.node_type,"host":self.host,"code_sequence":self.code_sequence,
                    "packages":self.package_evidence()?,
                    "creation_generation":route.creation_generation,"creation_configuration":route.creation_configuration,
                    "initial_ttl_ticks":self.initial_ttl,"fresh_poll_ttl_ticks":self.fresh_poll_ttl,
                    "fresh_poll_elapsed_micros":self.fresh_poll_elapsed_micros,
                    "node_host_pid":self.node_host_pid,"maximum_concurrent_opens":self.maximum_opening.load(Ordering::Relaxed),
                    "remaining_authority_millis":route.partition.authority_remaining().map(|v|v.as_millis() as u64).unwrap_or(0),
                    "native_actions":{"open":counters.open_commands,"close":counters.close_commands,
                        "abort":counters.abort_commands,"query":counters.queries,"update":counters.service_updates,
                        "open_failures":counters.open_failures,"close_failures":counters.close_failures,
                        "abort_callbacks":counters.abort_callbacks},
                    "deliberately_noncooperative":route.deliberately_noncooperative, "report":report
                }).to_string(),Some(active))
            } else {
                (503, "{}".into(), Some(active))
            }
        } else {
            (404, "{}".into(), None)
        };
        let response=format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;
        Ok(())
    }
    async fn report(
        &self,
        partition: &InstanceRuntimeIntegration,
        parameters: &HashMap<&str, &str>,
    ) -> serde_json::Value {
        let permit = match partition.execution().try_reserve() {
            Ok(permit) => permit,
            Err(error) => {
                return serde_json::json!({"ok":false,"error":error.to_string(),"native_code":error.native_code()})
            }
        };
        let partition = partition.clone();
        let parameters: HashMap<String, String> = parameters
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let retired = self.retired.lock().unwrap().clone();
        match permit
            .run_owned(move || {
                let parameters = parameters
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                Ok(Self::report_sync(&partition, &parameters, &retired))
            })
            .await
        {
            Ok(result) => result,
            Err(error) => {
                serde_json::json!({"ok":false,"error":error.to_string(),"native_code":error.native_code()})
            }
        }
    }
    fn report_sync(
        partition: &InstanceRuntimeIntegration,
        parameters: &HashMap<&str, &str>,
        retired: &VecDeque<InstanceRuntimeIntegration>,
    ) -> serde_json::Value {
        let Some(operation) = parameters.get("op") else {
            return serde_json::Value::Null;
        };
        let (reports, observation) = partition.observe_report();
        let result: RuntimeResult<serde_json::Value> = (|| match *operation {
            "load" => {
                let value = parameters
                    .get("value")
                    .ok_or(RuntimeError::InvalidInput("missing value"))?
                    .parse()
                    .map_err(|_| RuntimeError::InvalidInput("invalid value"))?;
                reports.report_load(&[LoadMetric {
                    name: "RuntimeWork".into(),
                    value,
                }])?;
                Ok(serde_json::json!({"completion":"local-frame-write"}))
            }
            "move" => {
                reports.report_move_cost(MoveCost::High)?;
                Ok(serde_json::json!({"completion":"local-frame-write","cost":3}))
            }
            "health" => {
                let number = |name| -> RuntimeResult<i64> {
                    parameters
                        .get(name)
                        .ok_or(RuntimeError::InvalidInput("missing health argument"))?
                        .parse()
                        .map_err(|_| RuntimeError::InvalidInput("invalid health argument"))
                };
                let state = match number("state")? {
                    1 => HealthState::Ok,
                    2 => HealthState::Warning,
                    3 => HealthState::Error,
                    _ => return Err(RuntimeError::InvalidInput("invalid health state")),
                };
                let ttl = number("ttl")?;
                if ttl < 0 {
                    return Err(RuntimeError::InvalidInput("invalid TTL"));
                }
                reports.report_instance_health(&HealthInformation {
                    source_id: "RustRuntime".into(),
                    property: parameters
                        .get("property")
                        .ok_or(RuntimeError::InvalidInput("missing property"))?
                        .to_string(),
                    state,
                    description: "native Rust runtime acceptance".into(),
                    time_to_live_seconds: ttl
                        .try_into()
                        .map_err(|_| RuntimeError::InvalidInput("TTL exceeds u32"))?,
                    sequence_number: number("sequence")?,
                    remove_when_expired: parameters.get("remove") == Some(&"true"),
                })?;
                let sequence = observation
                    .result()
                    .ok_or(RuntimeError::Protocol("missing health observation"))??
                    .health_sequence
                    .ok_or(RuntimeError::Protocol("missing health sequence"))?;
                Ok(serde_json::json!({"completion":"hm-report-result","sequence":sequence}))
            }
            "fault" => {
                reports.report_fault(FaultType::Transient)?;
                let status = observation
                    .result()
                    .ok_or(RuntimeError::Protocol("missing fault observation"))??
                    .fault
                    .ok_or(RuntimeError::Protocol("missing fault completion"))?;
                Ok(serde_json::json!({"completion":"local-frame-write","attempts":status.attempts}))
            }
            "stale" => {
                let instance: i64 = parameters
                    .get("instance")
                    .ok_or(RuntimeError::InvalidInput("missing old instance"))?
                    .parse()
                    .map_err(|_| RuntimeError::InvalidInput("invalid old instance"))?;
                let old = retired
                    .iter()
                    .find(|p| {
                        p.identity().partition_id == partition.identity().partition_id
                            && p.identity().instance_id == instance
                    })
                    .cloned()
                    .ok_or(RuntimeError::InvalidInput("retired instance not retained"))?;
                let closed = |r: RuntimeResult<()>| matches!(r, Err(RuntimeError::Closed));
                let old_reports = old.partition();
                let load = closed(old_reports.report_load(&[LoadMetric {
                    name: "RuntimeWork".into(),
                    value: 1,
                }]));
                let health = closed(old_reports.report_instance_health(&HealthInformation {
                    source_id: "RustRuntime".into(),
                    property: "Stale".into(),
                    state: HealthState::Error,
                    description: "must not be sent".into(),
                    time_to_live_seconds: 0,
                    sequence_number: 0,
                    remove_when_expired: false,
                }));
                let fault = closed(old_reports.report_fault(FaultType::Transient));
                let move_cost = closed(old_reports.report_move_cost(MoveCost::Low));
                if !(load && health && fault && move_cost) {
                    return Err(RuntimeError::InvalidInput(
                        "retired reports were not all rejected as closed",
                    ));
                }
                Ok(
                    serde_json::json!({"load_closed":load,"health_closed":health,"fault_closed":fault,
                        "move_closed":move_cost,"old_instance":instance,"old_incarnation":old.identity().incarnation}),
                )
            }
            _ => Err(RuntimeError::InvalidInput("unsupported report command")),
        })();
        match result {
            Ok(value) => serde_json::json!({"ok":true,"result":value}),
            Err(error) => {
                let native_code = if let RuntimeError::Remote { code, .. } = &error {
                    Some(*code)
                } else {
                    None
                };
                serde_json::json!({"ok":false,"error":error.to_string(),"native_code":native_code})
            }
        }
    }
}
struct Factory(Arc<Rpc>);
struct InstanceCreation {
    identity: ServiceIdentity,
    activation: Arc<ActivationContext>,
    binding: InstanceRuntimeIntegration,
}
struct Service {
    rpc: Arc<Rpc>,
    path: String,
    context: InstanceCreation,
    route: Mutex<Option<Arc<Route>>>,
}
impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        service_type: String,
        service_name: String,
        _: &[u8],
        partition: uuid::Uuid,
        instance: i64,
    ) -> RuntimeResult<Box<dyn IStatelessServiceInstance>> {
        let binding = self.0.instances.capture(partition, instance)?;
        if binding.identity().service_type != service_type
            || binding.identity().service_name != service_name
        {
            return Err(RuntimeError::InvalidInput(
                "factory arguments differ from native identity",
            ));
        }
        let context = InstanceCreation {
            identity: binding.identity().clone(),
            activation: self.0.current.snapshot(),
            binding,
        };
        Ok(Box::new(Service {
            rpc: self.0.clone(),
            path: format!("/{}", context.identity.partition_id),
            context,
            route: Mutex::new(None),
        }) as Box<dyn IStatelessServiceInstance>)
    }
}
impl Service {
    fn remove(&self) -> Option<Arc<Route>> {
        self.rpc.routes.lock().unwrap().remove(&self.path);
        let route = self.route.lock().unwrap().take();
        if let Some(route) = &route {
            let mut retired = self.rpc.retired.lock().unwrap();
            if retired.len() == 32 {
                retired.pop_front();
            }
            retired.push_back(route.partition.clone());
        }
        route
    }
}
impl IStatelessServiceInstance for Service {
    fn open(
        &self,
        partition: Arc<dyn IStatelessServicePartition>,
        _cancellation: Cancellation,
    ) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            let _ = partition.get_partition_info()?;
            let partition = self.context.binding.clone();
            struct Opening<'a>(&'a AtomicUsize);
            impl Drop for Opening<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let count = self.rpc.opening.fetch_add(1, Ordering::SeqCst) + 1;
            let _opening = Opening(&self.rpc.opening);
            self.rpc.maximum_opening.fetch_max(count, Ordering::SeqCst);
            if self.context.identity.service_name.ends_with("/Concurrent") {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            self.rpc.listen().await?;
            partition.validate()?;
            let route = Arc::new(Route {
                creation_generation: self.context.activation.generation(),
                creation_configuration: self
                    .context
                    .activation
                    .packages()?
                    .configuration_package("Config")?
                    .section("Main")?
                    .parameter("Value")?
                    .plaintext()?
                    .to_owned(),
                partition,
                active: AtomicUsize::new(0),
                deliberately_noncooperative: self
                    .context
                    .identity
                    .service_name
                    .ends_with("/Uncooperative"),
            });
            if route.deliberately_noncooperative {
                self.rpc
                    .deliberately_noncooperative
                    .store(true, Ordering::Release);
            }
            self.rpc
                .routes
                .lock()
                .unwrap()
                .insert(self.path.clone(), route.clone());
            *self.route.lock().unwrap() = Some(route);
            println!(
                "OPEN partition={} instance={} incarnation={} pid={}",
                self.context.identity.partition_id,
                self.context.identity.instance_id,
                self.context.identity.incarnation,
                std::process::id()
            );
            if self.context.identity.service_name.ends_with("/FailOpen") {
                return Err(RuntimeError::Remote {
                    code: 0x80004005u32 as i32 as i64,
                    message: "intentional native acceptance open failure".into(),
                });
            }
            Ok(format!(
                "http://{}:{}{}",
                self.rpc.ip, self.rpc.port, self.path
            ))
        })
    }
    fn close(&self, _cancellation: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let route = self.remove();
            if let Some(route) = route {
                while route.active.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            println!(
                "CLOSE partition={} pid={}",
                self.context.identity.partition_id,
                std::process::id()
            );
            if self.context.identity.service_name.ends_with("/FailClose") {
                return Err(RuntimeError::Remote {
                    code: 0x80004005u32 as i32 as i64,
                    message: "intentional native acceptance close failure".into(),
                });
            }
            Ok(())
        })
    }
    fn abort(&self) {
        let route = self.remove();
        if let Some(route) = route {
            let deadline = std::time::Instant::now() + Duration::from_millis(150);
            while route.active.load(Ordering::SeqCst) != 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "RPC cleanup requires process fencing"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        println!(
            "ABORT partition={} pid={}",
            self.context.identity.partition_id,
            std::process::id()
        );
    }
}
async fn run() -> RuntimeResult<()> {
    let runtime = Runtime::attach().await?;
    let activation = runtime.activation_context();
    let current = runtime.context_handle();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut subscriptions = Vec::new();
    for registration in 1..=2 {
        let events = events.clone();
        let provider = current.clone();
        subscriptions.push(current.subscribe_configuration(move |event| {
            let record = match event {
                PackageChange::Modified { previous, current } => serde_json::json!({
                    "registration":registration,"kind":"modified",
                    "previous":config_evidence(&previous).expect("old config"),
                    "current":config_evidence(&current).expect("new config"),
                    "published_generation":provider.snapshot().generation(),
                }),
                _ => panic!("unexpected native config fixture event"),
            };
            let mut list = events.lock().unwrap();
            assert!(list.len() < 32, "package event fixture bound");
            list.push(record);
        })?);
    }
    let unregistered_callbacks = Arc::new(AtomicUsize::new(0));
    let calls = unregistered_callbacks.clone();
    let mut unregistered = current.subscribe_configuration(move |_| {
        calls.fetch_add(1, Ordering::AcqRel);
    })?;
    unregistered.unregister();
    unregistered.unregister();
    let rpc = Arc::new(Rpc {
        current,
        instances: runtime.instance_integration_provider(),
        native: runtime.native_integration_provider(),
        initial: activation.clone(),
        retained_config: activation.packages()?.configuration_package("Config")?,
        package_events: events,
        unregistered_callbacks,
        ip: activation.node_ip.clone(),
        port: *activation
            .endpoints
            .get("Rpc")
            .ok_or(RuntimeError::InvalidInput("missing SF Rpc endpoint"))?,
        node: runtime.node_context().name.clone(),
        node_type: runtime.node_context().node_type.clone(),
        host: activation.host_id.clone(),
        code_sequence: activation.code_sequence,
        listener: OnceCell::new(),
        listener_tasks: task_ownership::Listener::default(),
        routes: Mutex::new(HashMap::new()),
        stateful_routes: Mutex::new(HashMap::new()),
        data_routes: Mutex::new(HashMap::new()),
        peers: {
            let port =
                *activation
                    .endpoints
                    .get("Replication")
                    .ok_or(RuntimeError::InvalidInput(
                        "missing SF replication/control endpoint",
                    ))?;
            if port == 0 || activation.endpoints.get("Rpc") == Some(&port) {
                return Err(RuntimeError::InvalidInput(
                    "service/control endpoint collision",
                ));
            }
            stateful::PeerServer::new(activation.node_ip.clone(), port)
        },
        retired: Mutex::new(VecDeque::new()),
        metrics: runtime.metrics(),
        opening: AtomicUsize::new(0),
        maximum_opening: AtomicUsize::new(0),
        node_host_pid: runtime.node_context().host_process_id,
        initial_ttl: runtime.node_context().initial_lease_ttl_ticks,
        fresh_poll_ttl: runtime.node_context().fresh_poll_ttl_ticks,
        fresh_poll_elapsed_micros: runtime.node_context().fresh_poll_elapsed_micros,
        deliberately_noncooperative: AtomicBool::new(false),
    });
    println!(
        "ATTACHED pid={} host={} node={} code_sequence={}",
        std::process::id(),
        rpc.host,
        rpc.node,
        rpc.code_sequence
    );
    let result = async {
        let permit = runtime.execution_runtime().try_reserve()?;
        let factory_rpc = rpc.clone();
        let (runtime, registered) = permit
            .run_owned(move || {
                let registered = runtime
                    .register_stateless_service_factory(
                        "RustRuntimeType",
                        Box::new(Factory(factory_rpc.clone())),
                    )
                    .and_then(|()| {
                        runtime.register_stateful_service_factory(
                            "RustStatefulRuntimeType",
                            Box::new(stateful::Factory(factory_rpc)),
                        )
                    });
                Ok((runtime, registered))
            })
            .await?;
        registered?;
        println!("TYPE_REGISTERED RustRuntimeType");
        println!("TYPE_REGISTERED RustStatefulRuntimeType");
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _=terminate.recv()=>{},
            _=tokio::signal::ctrl_c()=>{},
            error=runtime.closed()=>{
                if rpc.deliberately_noncooperative.load(Ordering::Acquire) {
                    // Explicit adversarial acceptance fixture: neither application
                    // listener nor main cooperates. Only the independent runtime
                    // watchdog may enforce terminal authority.
                    std::future::pending::<()>().await;
                }
                return Err(error)
            },
        }
        runtime.shutdown().await
    }
    .await;
    // Cancelling requests neither rolls back accepted data writes nor takes
    // ownership of their separate data-service pump.
    let cleanup = rpc.shutdown_listeners().await;
    result.and(cleanup)
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("RUST_RUNTIME_FAILURE {error}");
        std::process::exit(1);
    }
}
