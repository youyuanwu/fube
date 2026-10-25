use std::{
    io::{BufRead, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use mssf_core::{
    runtime::{
        executor::BoxedCancelToken, node_context::NodeContext, CodePackageActivationContext,
        IStatelessServiceFactory, IStatelessServiceInstance, IStatelessServicePartition, Runtime,
    },
    types::{ServicePartitionInformation, Uri},
    WString, GUID,
};
use mssf_util::tokio::TokioExecutor;
use serde_json::json;

fn event(value: serde_json::Value) {
    println!("MSSF_APP {value}");
}

struct Factory;

impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        service_type: WString,
        service_name: Uri,
        initialization: &[u8],
        partition: GUID,
        instance: i64,
    ) -> mssf_core::Result<Box<dyn IStatelessServiceInstance>> {
        event(json!({"event":"create", "type":service_type.to_string(),
            "service":format!("{service_name:?}"), "partition":format!("{partition:?}"),
            "instance":instance, "initialization":initialization}));
        Ok(Box::new(Instance {
            partition,
            listener: Mutex::new(None),
        }))
    }
}

struct Listener {
    stop: mpsc::Sender<()>,
    join: thread::JoinHandle<()>,
}

struct Instance {
    partition: GUID,
    listener: Mutex<Option<Listener>>,
}

impl Instance {
    fn stop(&self, callback: &str) {
        if let Some(listener) = self.listener.lock().unwrap().take() {
            let _ = listener.stop.send(());
            listener.join.join().unwrap();
        }
        event(json!({"event":callback, "partition":format!("{:?}", self.partition)}));
    }
}

fn ping(mut stream: TcpStream) {
    let timeout = Some(Duration::from_millis(250));
    stream.set_read_timeout(timeout).unwrap();
    stream.set_write_timeout(timeout).unwrap();
    let mut input = [0; 5];
    if stream.read_exact(&mut input).is_ok() && &input == b"ping\n" {
        let _ = stream.write_all(b"native-stateless-pong\n");
    }
}

#[async_trait::async_trait]
impl IStatelessServiceInstance for Instance {
    async fn open(
        &self,
        partition: Arc<dyn IStatelessServicePartition>,
        _: BoxedCancelToken,
    ) -> mssf_core::Result<WString> {
        let info = partition.get_partition_info()?;
        match info {
            ServicePartitionInformation::Singleton(info) => assert_eq!(info.id, self.partition),
            other => panic!("unsupported partition: {other:?}"),
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (stop, receiver) = mpsc::channel();
        let join = thread::spawn(move || loop {
            if receiver.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => ping(stream),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("ping listener: {error}"),
            }
        });
        assert!(self
            .listener
            .lock()
            .unwrap()
            .replace(Listener { stop, join })
            .is_none());
        event(
            json!({"event":"open", "endpoint":address, "partition":format!("{:?}", self.partition),
            "partition_kind":"singleton"}),
        );
        Ok(WString::from(address))
    }

    async fn close(&self, _: BoxedCancelToken) -> mssf_core::Result<()> {
        self.stop("close");
        Ok(())
    }

    fn abort(&self) {
        self.stop("abort");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    event(json!({"event":"starting", "pid":std::process::id()}));
    let executor = tokio::runtime::Runtime::new()?;
    let runtime = Runtime::create(TokioExecutor::new(executor.handle().clone()))?;
    event(json!({"event":"runtime_created"}));
    let activation = CodePackageActivationContext::create()?;
    let node = NodeContext::get_sync()?;
    event(
        json!({"event":"context", "work":activation.get_code_package_info().work_directory.to_string(),
        "node":node.ip_address_or_fqdn.to_string()}),
    );
    runtime.register_stateless_service_factory(
        &WString::from("NativeStatelessType"),
        Box::new(Factory),
    )?;
    event(json!({"event":"type_registered"}));
    let mut command = String::new();
    std::io::stdin().lock().read_line(&mut command)?;
    if command != "shutdown\n" {
        return Err("expected owned host shutdown command".into());
    }
    drop(runtime);
    event(json!({"event":"runtime_released"}));
    // Native runtime release initiates asynchronous unregistration; the host
    // observes that protocol separately before closing this owned stdin.
    command.clear();
    std::io::stdin().lock().read_line(&mut command)?;
    event(json!({"event":"exiting"}));
    Ok(())
}
