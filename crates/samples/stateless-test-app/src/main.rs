use std::{
    io::{BufRead, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
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

static MARKERS: AtomicBool = AtomicBool::new(true);

fn event(value: serde_json::Value) {
    if MARKERS.load(Ordering::Relaxed) {
        println!("MSSF_APP {value}");
    }
}

#[derive(Default)]
struct FixtureConfig {
    manifest: bool,
    service_type: String,
    service_name: Option<String>,
    terminal: Option<String>,
    opaque: bool,
    creates: AtomicUsize,
}

impl FixtureConfig {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Self {
            service_type: "NativeStatelessType".into(),
            ..Self::default()
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--manifest-fixture" => config.manifest = true,
                "--type" => config.service_type = args.next().ok_or("missing type")?,
                "--service" => config.service_name = Some(args.next().ok_or("missing service")?),
                "--terminal" => {
                    let value = args.next().ok_or("missing terminal")?;
                    if !["close", "abort"].contains(&value.as_str()) {
                        return Err("invalid terminal".into());
                    }
                    config.terminal = Some(value);
                }
                "--endpoint" => {
                    let value = args.next().ok_or("missing endpoint")?;
                    if !["socket", "opaque"].contains(&value.as_str()) {
                        return Err("invalid endpoint".into());
                    }
                    config.opaque = value == "opaque";
                }
                _ => return Err(format!("unexpected argument {arg}").into()),
            }
        }
        if config.manifest && (config.service_name.is_none() || config.terminal.is_none()) {
            return Err("manifest fixture requires expected service and terminal".into());
        }
        Ok(config)
    }
}

struct Factory(Arc<FixtureConfig>);

impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        service_type: WString,
        service_name: Uri,
        initialization: &[u8],
        partition: GUID,
        instance: i64,
    ) -> mssf_core::Result<Box<dyn IStatelessServiceInstance>> {
        if self.0.manifest {
            assert_eq!(self.0.creates.fetch_add(1, Ordering::SeqCst), 0);
            assert_eq!(service_type.to_string(), self.0.service_type);
            assert_eq!(
                service_name.to_string(),
                *self.0.service_name.as_ref().unwrap()
            );
            assert!(initialization.is_empty());
            assert_eq!(instance, 1);
        }
        event(json!({"event":"create", "type":service_type.to_string(),
            "service":format!("{service_name:?}"), "partition":format!("{partition:?}"),
            "instance":instance, "initialization":initialization}));
        Ok(Box::new(Instance {
            partition,
            listener: Mutex::new(None),
            config: self.0.clone(),
            opened: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
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
    config: Arc<FixtureConfig>,
    opened: AtomicBool,
    stopped: AtomicBool,
}

impl Instance {
    fn stop(&self, callback: &str) {
        if self.config.manifest {
            assert!(self.opened.load(Ordering::SeqCst));
            assert!(!self.stopped.swap(true, Ordering::SeqCst));
            assert_eq!(Some(callback), self.config.terminal.as_deref());
        }
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
        assert!(!self.opened.swap(true, Ordering::SeqCst));
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
        Ok(WString::from(if self.config.opaque {
            "opaque:manifest-fixture".to_owned()
        } else {
            address
        }))
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
    let config = Arc::new(FixtureConfig::parse()?);
    MARKERS.store(!config.manifest, Ordering::Relaxed);
    event(json!({"event":"starting", "pid":std::process::id()}));
    let executor = tokio::runtime::Runtime::new()?;
    let runtime = Runtime::create(TokioExecutor::new(executor.handle().clone()))?;
    event(json!({"event":"runtime_created"}));
    let activation = CodePackageActivationContext::create()?;
    let node = NodeContext::get_sync()?;
    if config.manifest {
        let info = activation.get_code_package_info();
        assert_eq!(
            info.application_type_name.to_string(),
            "ManifestHostApplication"
        );
        // mssf-com 0.10 generates this FABRIC_URI getter as an out-struct return,
        // unlike native's pointer return. Do not qualify that unrelated binding
        // here; factory service URI and host runtime context check the app name.
        assert_eq!(info.code_package_name.to_string(), "Executables");
        assert_eq!(info.code_package_version.to_string(), "4.3");
        let package = activation.get_code_package(&WString::from("Executables"))?;
        assert_eq!(package.service_manifest_name.to_string(), "EchoPackage");
        assert_eq!(package.service_manifest_version.to_string(), "3.2");
        let endpoint = activation.get_endpoint_resource(&WString::from("Control"))?;
        assert_eq!(endpoint.name.to_string(), "Control");
        assert_eq!(endpoint.protocol.to_string(), "tcp");
        assert_eq!(endpoint.r#type.to_string(), "Internal");
        assert_eq!(endpoint.port, 32123);
        assert!(endpoint.certificate_name.to_string().is_empty());
        assert_eq!(
            std::env::current_dir()?.to_string_lossy(),
            info.work_directory.to_string()
        );
    }
    event(
        json!({"event":"context", "work":activation.get_code_package_info().work_directory.to_string(),
        "node":node.ip_address_or_fqdn.to_string()}),
    );
    runtime.register_stateless_service_factory(
        &WString::from(&config.service_type),
        Box::new(Factory(config.clone())),
    )?;
    event(json!({"event":"type_registered"}));
    if config.manifest {
        // General manifest apps have no owned-stdin shutdown contract. The host
        // must report semantic completion separately from forced owned cleanup.
        loop {
            thread::park();
        }
    }
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
