//! Real SF-activated sample. It never constructs activation identity itself.
use mssf_runtime::*;
use std::{
    collections::HashMap,
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

struct Route {
    partition: Partition,
    active: AtomicUsize,
    deliberately_noncooperative: bool,
}
struct Rpc {
    ip: String,
    port: u16,
    node: String,
    node_type: String,
    host: String,
    code_sequence: i64,
    listener: OnceCell<()>,
    routes: Mutex<HashMap<String, Arc<Route>>>,
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
                                Duration::from_secs(2),
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
                (200,serde_json::json!({"pid":std::process::id(),"partition":id.partition_id.to_string(),
                    "instance":id.instance_id,"incarnation":id.incarnation,"service":id.service_name,
                    "node":self.node,"node_type":self.node_type,"host":self.host,"code_sequence":self.code_sequence,
                    "initial_ttl_ticks":self.initial_ttl,"fresh_poll_ttl_ticks":self.fresh_poll_ttl,
                    "fresh_poll_elapsed_micros":self.fresh_poll_elapsed_micros,
                    "node_host_pid":self.node_host_pid,"maximum_concurrent_opens":self.maximum_opening.load(Ordering::Relaxed),
                    "remaining_authority_millis":route.partition.authority_remaining().map(|v|v.as_millis() as u64).unwrap_or(0),
                    "native_actions":{"open":counters.open_commands,"close":counters.close_commands,
                        "abort":counters.abort_commands,"query":counters.queries,"update":counters.service_updates,
                        "open_failures":counters.open_failures,"close_failures":counters.close_failures,
                        "abort_callbacks":counters.abort_callbacks},
                    "deliberately_noncooperative":route.deliberately_noncooperative
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
        self.route.lock().unwrap().take()
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
    let rpc = Arc::new(Rpc {
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
