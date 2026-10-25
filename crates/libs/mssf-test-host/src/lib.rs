//! Experimental standalone node-side host for one trusted native stateless app.
//! Native-free model tests do not establish native FabricRuntime compatibility.

mod controller;
mod deployment;
mod ipc;
mod lifecycle_wire;
mod process;
#[cfg(test)]
mod tests;
mod wire;

use mssf_transport::{
    message::{ListenInstance, Message},
    serialization::serde::{from_slice, to_vec, EmptyObjectArray, Pointer},
};
use serde_json::json;
use std::{
    fmt,
    net::TcpListener,
    path::PathBuf,
    sync::mpsc::RecvTimeoutError,
    thread,
    time::{Duration, Instant},
};

pub type Result<T> = std::result::Result<T, HostError>;

#[derive(Debug)]
pub struct HostError(pub String);
impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for HostError {}
impl From<std::io::Error> for HostError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}
impl From<mssf_transport::Error> for HostError {
    fn from(value: mssf_transport::Error) -> Self {
        Self(value.to_string())
    }
}
impl From<std::env::JoinPathsError> for HostError {
    fn from(value: std::env::JoinPathsError) -> Self {
        Self(value.to_string())
    }
}

pub struct Options {
    pub native_directory: PathBuf,
    pub dependency_directory: PathBuf,
    pub executable: PathBuf,
    pub run_parent: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    Bootstrap,
    Close,
    Abort,
}

#[derive(Debug, serde::Serialize)]
pub struct RunReport {
    pub profile: String,
    pub run_root: PathBuf,
    pub host_id: String,
    pub scenario: Scenario,
    pub pid: u32,
    pub runtime_id: String,
    pub endpoint: Option<String>,
    pub partition_id: Option<String>,
    pub lease_polls: usize,
    pub loaded_native_libraries: Vec<PathBuf>,
    pub callbacks: Vec<serde_json::Value>,
    pub cleanup: serde_json::Value,
}

#[derive(Default)]
struct Registration {
    started: bool,
    finished: bool,
    runtime: Option<wire::RuntimeContext>,
    registered_type: bool,
    lease_polls: usize,
    handshake: bool,
}

fn require(condition: bool, description: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(HostError(description.into()))
    }
}

fn validate_ipc(message: &Message, host: &str, pid: u32) -> Result<()> {
    for id in [
        mssf_transport::message::ACTOR,
        mssf_transport::message::ACTION,
        mssf_transport::message::MESSAGE_ID,
        mssf_transport::message::RELATES_TO,
        mssf_transport::message::EXPECTS_REPLY,
    ] {
        require(
            message.headers.iter().filter(|h| h.id == id).count() <= 1,
            "duplicate IPC routing header",
        )?;
    }
    let identities: Vec<_> = message
        .headers
        .iter()
        .filter(|h| h.id == wire::IPC_HEADER)
        .collect();
    require(
        identities.len() == 1,
        "IPC message must have one identity header",
    )?;
    let identity: wire::IpcIdentity = from_slice(&identities[0].value)?;
    require(
        identity.host == host && identity.pid == pid,
        "unowned IPC host/PID",
    )
}

impl Registration {
    fn dispatch(
        &mut self,
        message: Message,
        session: &mut ipc::Session,
        host: &str,
        pid: u32,
        bundle: &deployment::Deployment,
    ) -> Result<()> {
        let properties = message.properties()?;
        println!(
            "MSSF_HOST {}",
            json!({"event":"incoming", "actor":properties.actor,
            "action":properties.action, "body_bytes":message.body.len()})
        );
        if properties.actor == 1 && properties.action.is_empty() {
            require(!self.handshake, "duplicate transport handshake")?;
            let remote = ListenInstance::decode(&message.body)?;
            require(
                remote.address.is_empty() && remote.instance == 0,
                "expected native client-only listen instance",
            )?;
            self.handshake = true;
            return Ok(());
        }
        require(
            self.handshake,
            "application message before transport handshake",
        )?;
        require(
            properties.relates_to == Default::default(),
            "unexpected correlated app reply",
        )?;
        validate_ipc(&message, host, pid)?;
        require(
            properties.expects_reply,
            "unsupported one-way application traffic",
        )?;
        let mut reply_action = None;
        let body = match (properties.actor, properties.action.as_str()) {
            (7, "StartRegisterApplicationHostRequest") => {
                let request: wire::HostStart = from_slice(&message.body)?;
                require(
                    !self.started
                        && request.host == host
                        && request.pid == pid
                        && request.kind == 1
                        && !request.container
                        && !request.activator
                        && request.timeout > 0,
                    "invalid host start context/state",
                )?;
                self.started = true;
                to_vec(&wire::StartReply {
                    node_id: "1".into(),
                    node_pid: std::process::id(),
                    lease_handle: 1,
                    lease_duration: wire::TTL_TICKS,
                    error: 0,
                    node_instance: 1,
                    node_name: "NativeTestNode".into(),
                    node_type: "NativeTestNodeType".into(),
                    // A closed loopback address, never a real cluster gateway.
                    client_address: "127.0.0.1:0".into(),
                    deployment: bundle.deployment.to_string_lossy().into_owned(),
                    ip: "127.0.0.1".into(),
                    work: bundle.work.to_string_lossy().into_owned(),
                    initial_ttl: wire::TTL_TICKS,
                    application_directories: EmptyObjectArray,
                    node_directories: EmptyObjectArray,
                    app_log: Pointer(None),
                    system_log: Pointer(None),
                })?
            }
            (7, "FinishRegisterApplicationHostRequest") => {
                let request: wire::HostId = from_slice(&message.body)?;
                require(
                    self.started && !self.finished && request.host == host,
                    "invalid host finish state",
                )?;
                self.finished = true;
                to_vec(&wire::ErrorReply { error: 0 })?
            }
            (9, "RegisterFabricRuntimeRequest") => {
                let request: wire::RuntimeRegistration = from_slice(&message.body)?;
                let context = &request.context;
                require(
                    self.finished
                        && self.runtime.is_none()
                        && !context.id.is_empty()
                        && context.host
                            == wire::HostContext {
                                host: host.into(),
                                kind: 1,
                                pid,
                                container: false,
                                activator: false,
                            }
                        && context.code == wire::expected_code(),
                    "invalid runtime registration context/state",
                )?;
                println!(
                    "MSSF_HOST {}",
                    json!({"event":"runtime_registered", "runtime":context.id})
                );
                self.runtime = Some(request.context);
                to_vec(&wire::ErrorReply { error: 0 })?
            }
            (9, "RegisterServiceTypeRequest") => {
                let request: wire::TypeRegistration = from_slice(&message.body)?;
                require(
                    self.runtime.as_ref() == Some(&request.context)
                        && !self.registered_type
                        && request.service_type.package == request.context.code.code.package
                        && request.service_type.name == wire::SERVICE_TYPE,
                    "invalid service type context/state",
                )?;
                self.registered_type = true;
                reply_action = Some("RegisterServiceTypeReply");
                to_vec(&wire::ErrorReply { error: 0 })?
            }
            (15, "") => {
                require(
                    self.started && message.body.is_empty(),
                    "invalid lease request/state",
                )?;
                self.lease_polls += 1;
                to_vec(&wire::LeaseReply {
                    ttl: wire::TTL_TICKS,
                })?
            }
            (9, "UnregisterFabricRuntimeRequest") => {
                let request: wire::RuntimeId = from_slice(&message.body)?;
                require(
                    self.runtime
                        .as_ref()
                        .is_some_and(|runtime| runtime.id == request.id),
                    "unknown runtime unregister",
                )?;
                self.runtime = None;
                self.registered_type = false;
                to_vec(&wire::ErrorReply { error: 0 })?
            }
            (7, "UnregisterApplicationHostRequest") => {
                let request: wire::HostUnregister = from_slice(&message.body)?;
                require(
                    self.finished && request.host == host,
                    "unknown host unregister",
                )?;
                self.finished = false;
                self.started = false;
                self.runtime = None;
                self.registered_type = false;
                to_vec(&wire::ErrorReply { error: 0 })?
            }
            _ => {
                return Err(HostError(format!(
                    "unsupported native actor/action {}/{}",
                    properties.actor, properties.action
                )))
            }
        };
        session.reply(properties.id, reply_action, body)?;
        Ok(())
    }
}

fn pump(
    state: &mut Registration,
    session: &mut ipc::Session,
    child: &mut process::OwnedApp,
    host: &str,
    bundle: &deployment::Deployment,
    unit: Option<&mut controller::ServiceUnit>,
    deadline: Instant,
) -> Result<()> {
    child.check_alive()?;
    require(
        Instant::now() < deadline,
        "native operation deadline exceeded; remote outcome uncertain until cleanup",
    )?;
    match session.incoming.recv_timeout(Duration::from_millis(50)) {
        Ok(message) => {
            let message = message?;
            let properties = message.properties()?;
            if properties.actor == 12 {
                validate_ipc(&message, host, child.pid())?;
                require(
                    state.registered_type,
                    "lifecycle reply without active runtime/type",
                )?;
                let unit = unit.ok_or_else(|| HostError("unsolicited lifecycle traffic".into()))?;
                unit.accept(&message)?;
                println!(
                    "MSSF_HOST {}",
                    json!({"event":"semantic_reply", "action":properties.action,
                    "partition":uuid::Uuid::from_bytes_le(unit.request.unit.id.id.0).to_string(),
                    "instance":unit.request.local.instance, "flags":unit.request.flags,
                    "endpoint":unit.request.local.service_endpoint})
                );
                Ok(())
            } else {
                state.dispatch(message, session, host, child.pid(), bundle)
            }
        }
        Err(RecvTimeoutError::Timeout) => Ok(()),
        Err(RecvTimeoutError::Disconnected) => Err(HostError("native IPC reader stopped".into())),
    }
}

/// Execute a real native-runtime scenario; this never substitutes a model peer.
pub fn run(options: Options, scenario: Scenario) -> Result<RunReport> {
    deployment::preflight(&options.native_directory, &options.executable)?;
    let bundle = deployment::Deployment::create(&options.run_parent, &options.executable)?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?.to_string();
    let host = uuid::Uuid::new_v4().to_string();
    let mut child = process::OwnedApp::launch(
        &bundle,
        &options.native_directory,
        &options.dependency_directory,
        &host,
        &address,
    )?;
    println!(
        "MSSF_HOST {}",
        json!({"event":"spawned", "host":host, "pid":child.pid(),
        "profile":"linux-x86_64-11.8.121.4-loopback-clear", "root":bundle.root, "ipc":address})
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut endpoint = None;
    let mut partition_id = None;
    let mut lease_polls = 0;
    let mut loaded_native_libraries = Vec::new();
    let mut runtime_id = String::new();
    let outcome = (|| -> Result<()> {
        let stream = loop {
            child.check_alive()?;
            require(
                Instant::now() < deadline,
                "native connection deadline exceeded",
            )?;
            match listener.accept() {
                Ok((stream, peer)) => {
                    require(peer.ip().is_loopback(), "non-loopback peer")?;
                    break stream;
                }

                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10))
                }
                Err(error) => return Err(error.into()),
            }
        };
        let mut session = ipc::Session::new(stream, address)?;
        let mut state = Registration::default();
        loop {
            pump(
                &mut state,
                &mut session,
                &mut child,
                &host,
                &bundle,
                None,
                deadline,
            )?;
            if state.registered_type
                && state.lease_polls > 0
                && child
                    .callbacks()
                    .iter()
                    .any(|e| e["event"] == "type_registered")
            {
                println!(
                    "MSSF_HOST {}",
                    json!({"event":"bootstrap_passed", "lease_polls":state.lease_polls})
                );
                break;
            }
        }
        let runtime = state.runtime.as_ref().unwrap();
        runtime_id = runtime.id.clone();
        lease_polls = state.lease_polls;
        let maps = std::fs::read_to_string(format!("/proc/{}/maps", child.pid()))?;
        for name in ["libFabricRuntime.so", "libFabricCommon.so"] {
            let path = options.native_directory.join(name).canonicalize()?;
            let escaped = path
                .to_string_lossy()
                .replace('\\', "\\134")
                .replace(' ', "\\040")
                .replace('\t', "\\011")
                .replace('\n', "\\012");
            require(
                maps.lines()
                    .any(|line| line.ends_with(&format!(" {escaped}"))),
                &format!("native app did not map pinned library {}", path.display()),
            )?;
            loaded_native_libraries.push(path);
        }
        if scenario != Scenario::Bootstrap {
            let mut unit = controller::ServiceUnit::new(runtime);
            partition_id = Some(uuid::Uuid::from_bytes_le(unit.request.unit.id.id.0).to_string());
            unit.open(&mut session)?;
            while unit.state != controller::UnitState::Open {
                pump(
                    &mut state,
                    &mut session,
                    &mut child,
                    &host,
                    &bundle,
                    Some(&mut unit),
                    deadline,
                )?;
            }
            let address = unit.request.local.service_endpoint.clone();
            controller::ping(&address)?;
            println!(
                "MSSF_HOST {}",
                json!({"event":"ping_passed","endpoint":address})
            );
            unit.close(&mut session, scenario == Scenario::Abort)?;
            while unit.state != controller::UnitState::Closed {
                pump(
                    &mut state,
                    &mut session,
                    &mut child,
                    &host,
                    &bundle,
                    Some(&mut unit),
                    deadline,
                )?;
            }
            controller::stopped(&address)?;
            child.check_alive()?;
            println!(
                "MSSF_HOST {}",
                json!({"event":"endpoint_stopped","process_still_alive":true,"scenario":scenario})
            );
            endpoint = Some(address);
        }
        if scenario == Scenario::Abort {
            return Ok(());
        }
        child.command(b"shutdown\n")?;
        while state.runtime.is_some() {
            pump(
                &mut state,
                &mut session,
                &mut child,
                &host,
                &bundle,
                None,
                deadline,
            )?;
        }
        child.command(b"exit\n")?;
        child.wait_exit(deadline.min(Instant::now() + Duration::from_secs(5)))?;
        Ok(())
    })();
    let cleanup = child.cleanup();
    for line in child.logs() {
        println!("MSSF_NATIVE {line}");
    }
    println!(
        "MSSF_HOST {}",
        json!({"event":"cleanup", "result":cleanup.as_ref().map_err(ToString::to_string)})
    );
    let cleanup = cleanup?;
    outcome?;
    let callbacks = child.callbacks();
    if scenario != Scenario::Bootstrap {
        for event in [
            "create",
            "open",
            if scenario == Scenario::Abort {
                "abort"
            } else {
                "close"
            },
        ] {
            require(
                callbacks.iter().filter(|e| e["event"] == event).count() == 1,
                &format!("native app must observe exactly one {event} callback"),
            )?;
        }
        let other = if scenario == Scenario::Abort {
            "close"
        } else {
            "abort"
        };
        require(
            !callbacks.iter().any(|e| e["event"] == other),
            "unexpected opposite terminal callback",
        )?;
        let created = callbacks.iter().find(|e| e["event"] == "create").unwrap();
        require(
            created["type"] == wire::SERVICE_TYPE
                && created["instance"] == 1
                && created["initialization"] == json!(b"native-test-host"),
            "native factory arguments mismatch",
        )?;
        let expected_partition = partition_id.as_deref().unwrap();
        for callback in callbacks.iter().filter(|event| {
            ["create", "open", "close", "abort"].contains(&event["event"].as_str().unwrap_or(""))
        }) {
            let actual = callback["partition"]
                .as_str()
                .and_then(|s| uuid::Uuid::parse_str(s).ok());
            require(
                actual.is_some_and(|id| id.to_string() == expected_partition),
                "native callback partition differs from commanded singleton",
            )?;
        }
        require(
            created["service"] == "Uri(\"fabric:/NativeTestHost/Singleton\")",
            "native callback service name mismatch",
        )?;
        require(
            callbacks.iter().any(|e| {
                e["event"] == "open"
                    && e["partition_kind"] == "singleton"
                    && Some(e["endpoint"].as_str().unwrap_or("")) == endpoint.as_deref()
            }),
            "Open callback endpoint/context differs from native semantic reply",
        )?;
    }
    Ok(RunReport {
        profile: "linux-x86_64-11.8.121.4-loopback-clear".into(),
        run_root: bundle.root,
        host_id: host,
        scenario,
        pid: child.pid(),
        runtime_id,
        endpoint,
        partition_id,
        lease_polls,
        loaded_native_libraries,
        callbacks,
        cleanup,
    })
}
