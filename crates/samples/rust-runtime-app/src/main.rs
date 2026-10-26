//! Real SF-activated sample. It never constructs activation identity itself.
use mssf_runtime::*;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
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
}
struct Rpc {
    ip: String,
    port: u16,
    node: String,
    host: String,
    code_sequence: i64,
    listener: OnceCell<()>,
    routes: Mutex<HashMap<String, Arc<Route>>>,
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
            if route.partition.ensure_valid().is_ok() {
                let id = &route.partition.identity;
                (200,serde_json::json!({"pid":std::process::id(),"partition":id.partition_id.to_string(),
                    "instance":id.instance_id,"incarnation":id.incarnation,"service":id.service_name,
                    "node":self.node,"host":self.host,"code_sequence":self.code_sequence}).to_string(),Some(active))
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
            self.rpc.listen().await?;
            partition.ensure_valid()?;
            let route = Arc::new(Route {
                partition,
                active: AtomicUsize::new(0),
            });
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
        host: activation.host_id.clone(),
        code_sequence: activation.code_sequence,
        listener: OnceCell::new(),
        routes: Mutex::new(HashMap::new()),
    });
    println!(
        "ATTACHED pid={} host={} node={} code_sequence={}",
        std::process::id(),
        rpc.host,
        rpc.node,
        rpc.code_sequence
    );
    runtime
        .register_stateless("RustRuntimeType", Arc::new(Factory(rpc)))
        .await?;
    println!("TYPE_REGISTERED RustRuntimeType");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _=terminate.recv()=>{},
        _=tokio::signal::ctrl_c()=>{},
        error=runtime.closed()=>return Err(error),
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
