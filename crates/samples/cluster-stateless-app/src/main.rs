//! Fixed native-free cluster profile. Application contact is independent of FM.
use fube_runtime::{
    extensions::InstanceIntegrationProvider, ApiFuture, Cancellation, Error,
    IStatelessServiceFactory, IStatelessServiceInstance, IStatelessServicePartition, Result,
    Runtime,
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

struct Factory {
    node: fube_runtime::NodeContext,
    integration: InstanceIntegrationProvider,
}
struct Instance {
    node: fube_runtime::NodeContext,
    integration: InstanceIntegrationProvider,
    partition: uuid::Uuid,
    replica: i64,
    listener: Mutex<Option<Listener>>,
}
struct Listener {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}
impl Instance {
    fn dispose(&self) {
        if let Some(listener) = self.listener.lock().unwrap().take() {
            listener.stop.store(true, Ordering::Release);
            // The one bounded synchronous listener owns no children; each
            // accepted socket has finite IO timeouts and is closed before join.
            listener
                .thread
                .join()
                .expect("application listener panicked");
        }
    }
}
impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        replica: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>> {
        Ok(Box::new(Instance {
            node: self.node.clone(),
            integration: self.integration.clone(),
            partition,
            replica,
            listener: Mutex::new(None),
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
            let integration = self.integration.capture(self.partition, self.replica)?;
            integration.validate()?;
            let identity = integration.identity();
            let response = serde_json::to_vec(&serde_json::json!({
                "profile": "fube-cluster-stateless-v1",
                "node": self.node.id, "node_instance": self.node.instance,
                "node_pid": self.node.host_process_id, "app_pid": std::process::id(),
                "service": identity.service_name, "service_type": identity.service_type,
                "partition": identity.partition_id.to_string(),
                "replica": identity.instance_id, "incarnation": identity.incarnation,
            }))
            .map_err(|_| Error::Protocol("application identity encoding"))?;
            let listener = TcpListener::bind("127.0.0.1:0")
                .map_err(|_| Error::Protocol("application bind"))?;
            listener
                .set_nonblocking(true)
                .map_err(|_| Error::Protocol("application listener"))?;
            let endpoint = listener
                .local_addr()
                .map_err(|_| Error::Protocol("application address"))?
                .to_string();
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = stop.clone();
            let thread = std::thread::Builder::new()
                .name("cluster-app-contact".into())
                .spawn(move || {
                    while !stopping.load(Ordering::Acquire) {
                        match listener.accept() {
                            Ok((mut socket, peer)) => {
                                if !peer.ip().is_loopback() {
                                    continue;
                                }
                                if socket
                                    .set_read_timeout(Some(Duration::from_millis(100)))
                                    .is_err()
                                    || socket
                                        .set_write_timeout(Some(Duration::from_millis(100)))
                                        .is_err()
                                {
                                    continue;
                                }
                                let mut request = [0; 9];
                                if socket.read_exact(&mut request).is_ok()
                                    && &request == b"identity\n"
                                    && integration.validate().is_ok()
                                {
                                    let _ = socket.write_all(&response);
                                    let _ = socket.write_all(b"\n");
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(2));
                            }
                            Err(_) => break,
                        }
                    }
                })
                .map_err(|_| Error::Protocol("application listener thread"))?;
            let mut slot = self.listener.lock().unwrap();
            if slot.is_some() {
                stop.store(true, Ordering::Release);
                thread
                    .join()
                    .map_err(|_| Error::Protocol("application listener join"))?;
                return Err(Error::Protocol("application already open"));
            }
            *slot = Some(Listener { stop, thread });
            Ok(endpoint)
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async {
            self.dispose();
            Ok(())
        })
    }
    fn abort(&self) {
        self.dispose();
    }
}
impl Drop for Instance {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let runtime = Runtime::attach()
        .await
        .expect("owned cluster Hosting attach");
    let factory = Factory {
        node: runtime.node_context().clone(),
        integration: runtime.instance_integration_provider(),
    };
    let runtime = runtime
        .execution_runtime()
        .try_reserve()
        .expect("execution reservation")
        .run_owned(move || {
            runtime
                .register_stateless_service_factory("ClusterStatelessType", Box::new(factory))?;
            Ok(runtime)
        })
        .await
        .expect("fixed cluster factory registration");
    let _ = runtime.closed().await;
}
