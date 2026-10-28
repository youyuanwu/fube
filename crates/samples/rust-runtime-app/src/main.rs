//! Real SF-activated sample. It never constructs activation identity itself.
use mssf_runtime::*;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OnceCell, Semaphore},
};
mod data;
mod stateful;

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
    partition: Partition,
    creation_generation: u64,
    creation_configuration: String,
    active: AtomicUsize,
    deliberately_noncooperative: bool,
}
struct Rpc {
    current: ActivationContextHandle,
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
    routes: Mutex<HashMap<String, Arc<Route>>>,
    stateful_routes: Mutex<HashMap<String, Arc<stateful::ReplicaRoute>>>,
    data_routes: Mutex<HashMap<String, Arc<data::Route>>>,
    peers: Arc<stateful::PeerServer>,
    retired: Mutex<VecDeque<Partition>>,
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
                tokio::spawn(async move {
                    let capacity = Arc::new(Semaphore::new(64));
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            return;
                        };
                        let Ok(permit) = capacity.clone().try_acquire_owned() else {
                            drop(stream);
                            continue;
                        };
                        let server = server.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            let _ = tokio::time::timeout(
                                Duration::from_secs(35),
                                server.request(stream),
                            )
                            .await;
                        });
                    }
                });
                RuntimeResult::Ok(())
            })
            .await?;
        Ok(())
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
            if route.deliberately_noncooperative || route.partition.ensure_valid().is_ok() {
                let id = &route.partition.identity;
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
        partition: &Partition,
        parameters: &HashMap<&str, &str>,
    ) -> serde_json::Value {
        let Some(operation) = parameters.get("op") else {
            return serde_json::Value::Null;
        };
        let result: RuntimeResult<serde_json::Value> = async {
            match *operation {
                "load" => {
                    let value = parameters.get("value").ok_or(RuntimeError::InvalidInput("missing value"))?
                        .parse().map_err(|_| RuntimeError::InvalidInput("invalid value"))?;
                    partition.report_load(&[LoadMetric { name: "RuntimeWork".into(), value }]).await?;
                    Ok(serde_json::json!({"completion":"local-frame-write"}))
                }
                "move" => {
                    partition.report_move_cost(MoveCost::High).await?;
                    Ok(serde_json::json!({"completion":"local-frame-write","cost":3}))
                }
                "health" => {
                    let number = |name| -> RuntimeResult<i64> {
                        parameters.get(name).ok_or(RuntimeError::InvalidInput("missing health argument"))?
                            .parse().map_err(|_| RuntimeError::InvalidInput("invalid health argument"))
                    };
                    let state = match number("state")? {
                        1 => HealthState::Ok, 2 => HealthState::Warning, 3 => HealthState::Error,
                        _ => return Err(RuntimeError::InvalidInput("invalid health state")),
                    };
                    let ttl = number("ttl")?;
                    if ttl < 0 { return Err(RuntimeError::InvalidInput("invalid TTL")); }
                    let receipt = partition.report_health(&HealthReport {
                        source: "RustRuntime".into(),
                        property: parameters.get("property").ok_or(RuntimeError::InvalidInput("missing property"))?.to_string(),
                        state, description: "native Rust runtime acceptance".into(),
                        time_to_live: if ttl == 0 { None } else { Some(Duration::from_secs(ttl as u64)) },
                        sequence: HealthSequence::Explicit(number("sequence")?),
                        remove_when_expired: parameters.get("remove") == Some(&"true"),
                    }).await?;
                    Ok(serde_json::json!({"completion":"hm-report-result","sequence":receipt.sequence}))
                }
                "fault" => {
                    let status = partition.report_fault(FaultType::Transient).await?;
                    Ok(serde_json::json!({"completion":"local-frame-write","attempts":status.attempts}))
                }
                "stale" => {
                    let instance: i64 = parameters.get("instance").ok_or(RuntimeError::InvalidInput("missing old instance"))?
                        .parse().map_err(|_| RuntimeError::InvalidInput("invalid old instance"))?;
                    let old = self.retired.lock().unwrap().iter().find(|p|
                        p.identity.partition_id == partition.identity.partition_id && p.identity.instance_id == instance
                    ).cloned().ok_or(RuntimeError::InvalidInput("retired instance not retained"))?;
                    let closed = |r: RuntimeResult<()>| matches!(r, Err(RuntimeError::Closed));
                    let load = closed(old.report_load(&[LoadMetric { name:"RuntimeWork".into(),value:1 }]).await);
                    let health = closed(old.report_health(&HealthReport {
                        source:"RustRuntime".into(),property:"Stale".into(),state:HealthState::Error,
                        description:"must not be sent".into(),time_to_live:None,sequence:HealthSequence::Auto,
                        remove_when_expired:false,
                    }).await.map(|_| ()));
                    let fault = closed(old.report_fault(FaultType::Transient).await.map(|_| ()));
                    let move_cost = closed(old.report_move_cost(MoveCost::Low).await);
                    if !(load && health && fault && move_cost) {
                        return Err(RuntimeError::InvalidInput("retired reports were not all rejected as closed"));
                    }
                    Ok(serde_json::json!({"load_closed":load,"health_closed":health,"fault_closed":fault,
                        "move_closed":move_cost,"old_instance":instance,"old_incarnation":old.identity.incarnation}))
                }
                _ => Err(RuntimeError::InvalidInput("unsupported report command")),
            }
        }.await;
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
struct Service {
    rpc: Arc<Rpc>,
    path: String,
    context: ServiceContext,
    route: Mutex<Option<Arc<Route>>>,
}
impl StatelessFactory for Factory {
    fn create(&self, context: ServiceContext) -> ServiceFuture<'_, Arc<dyn StatelessService>> {
        Box::pin(async move {
            Ok(Arc::new(Service {
                rpc: self.0.clone(),
                path: format!("/{}", context.identity.partition_id),
                context,
                route: Mutex::new(None),
            }) as Arc<dyn StatelessService>)
        })
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
impl StatelessService for Service {
    fn open(&self, partition: Partition, _cancellation: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
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
            partition.ensure_valid()?;
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
    let mut runtime = Runtime::attach().await?;
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
    runtime
        .register_stateless("RustRuntimeType", Arc::new(Factory(rpc.clone())))
        .await?;
    println!("TYPE_REGISTERED RustRuntimeType");
    runtime
        .register_stateful(
            "RustStatefulRuntimeType",
            Arc::new(stateful::Factory(rpc.clone())),
        )
        .await?;
    println!("TYPE_REGISTERED RustStatefulRuntimeType");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
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
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("RUST_RUNTIME_FAILURE {error}");
        std::process::exit(1);
    }
}
