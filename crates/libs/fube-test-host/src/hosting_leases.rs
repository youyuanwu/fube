//! Separately named native application wire qualification against production
//! Hosting. The original factory/lifecycle fixture profiles are unchanged.
use crate::{
    complete, deployment, process, require, HostError, Options, Result, RunReport, Scenario,
};
use fube_hosting::{ipc, wire};
use fube_rap_test_support::transport::relay::Frame;
use fubst::serde::{from_slice, to_vec, ObjectArray, Pointer};
use furpc::MessageId;
use std::{
    collections::HashMap,
    path::Path,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinSet,
};

pub(super) enum Observation {
    Request(Frame),
    Reply(Frame),
}

pub(super) async fn observe_frames(
    reader: &mut (impl AsyncRead + Unpin),
    writer: &mut (impl AsyncWrite + Unpin),
    observed: mpsc::Sender<Observation>,
    observation: fn(Frame) -> Observation,
) -> Result<()> {
    while let Some(frame) = Frame::read(reader).await? {
        observed
            .try_send(observation(frame.clone()))
            .map_err(|_| HostError("native observer frame budget".into()))?;
        match frame.forward(writer).await {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) async fn stop_observer(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    tasks.abort_all();
    let mut outcome = Ok(());
    while let Some(result) = tasks.join_next().await {
        let result = match result {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(HostError(format!("native observer join: {error}"))),
        };
        outcome = complete(outcome, result);
    }
    outcome
}

#[derive(Default)]
pub(super) struct Trace {
    pending: HashMap<MessageId, (i64, String)>,
    pub(super) actions: Vec<String>,
    pub(super) polls: usize,
    pub(super) runtime: Option<String>,
    pub(super) unregistered: bool,
    pub(super) registered: bool,
}
impl Trace {
    pub(super) fn observe(&mut self, observation: Observation) -> Result<()> {
        match observation {
            Observation::Request(message) => {
                let p = message.metadata()?;
                if p.actor == 1 {
                    return Ok(());
                }
                println!(
                    "MSSF_HOST {}",
                    serde_json::json!({"event":"production_hosting_request_frame",
                    "actor":p.actor,"action":p.action,"request_index":p.id.index,
                    "headers":message.headers.records().map(|header| header.id().get()).collect::<Vec<_>>(),
                    "expects_reply":message.headers.effective().expects_reply,"body_bytes":message.body().len()})
                );
                require(self.pending.len() < 32, "native pending observation budget")?;
                require(
                    self.pending
                        .insert(p.id, (p.actor, p.action.clone()))
                        .is_none(),
                    "native duplicate correlation",
                )?;
                if p.actor == 15 {
                    require(
                        p.action.is_empty() && message.body().is_empty(),
                        "native lease request shape",
                    )?;
                } else {
                    require(
                        self.actions.len() < 16,
                        "native operation observation budget",
                    )?;
                    self.actions.push(p.action.clone());
                    if p.action == "RegisterFabricRuntimeRequest" {
                        let value: wire::RuntimeRequest = from_slice(message.body())?;
                        self.runtime = Some(value.context.id);
                    }
                }
            }
            Observation::Reply(message) => {
                let p = message.metadata()?;
                if p.actor == 1 {
                    return Ok(());
                }
                let (actor, action) = self
                    .pending
                    .remove(&p.relates_to)
                    .ok_or_else(|| HostError("uncorrelated native application reply".into()))?;
                require(p.action.is_empty(), "native body-only Hosting reply")?;
                if actor == 15 {
                    let ttl = from_slice::<wire::LeaseReply>(message.body())?.ttl;
                    require(ttl > 0 && ttl <= 300_000_000, "native finite TTL")?;
                    self.polls += 1;
                    println!(
                        "MSSF_HOST {}",
                        serde_json::json!({"event":"production_hosting_lease_frame",
                        "request_guid":uuid::Uuid::from_bytes_le(p.relates_to.guid).to_string(),
                        "request_index":p.relates_to.index,"ttl_ticks":ttl})
                    );
                } else if action == "StartRegisterApplicationHostRequest" {
                    let value: wire::StartReply = from_slice(message.body())?;
                    require(
                        value.error == 0 && value.lease_duration == 300_000_000,
                        "native Start result",
                    )?;
                } else {
                    require(
                        from_slice::<wire::ErrorReply>(message.body())?.error == 0,
                        "native registration result",
                    )?;
                    if action == "UnregisterFabricRuntimeRequest" {
                        self.unregistered = true;
                    }
                    if action == "RegisterServiceTypeRequest" {
                        self.registered = true;
                    }
                }
            }
        }
        Ok(())
    }
}

pub(super) fn node(bundle: &deployment::Deployment) -> wire::StartReply {
    wire::StartReply {
        node_id: "1".into(),
        node_pid: std::process::id(),
        lease_handle: 0,
        lease_duration: 0,
        error: 0,
        node_instance: 1,
        node_name: "hosting-local".into(),
        node_type: "local".into(),
        client_address: "127.0.0.1:19000".into(),
        deployment: bundle.deployment.to_string_lossy().into_owned(),
        ip: "127.0.0.1".into(),
        work: bundle.work.to_string_lossy().into_owned(),
        initial_ttl: 0,
        application_directories: ObjectArray(vec![]),
        node_directories: ObjectArray(vec![]),
        application_log: Pointer(None),
        system_log: Pointer(None),
    }
}
pub(super) fn mapped_libraries(native: &Path, pid: u32) -> Result<Vec<std::path::PathBuf>> {
    let maps = std::fs::read_to_string(format!("/proc/{pid}/maps"))?;
    let mut result = Vec::new();
    for name in ["libFabricRuntime.so", "libFabricCommon.so"] {
        let path = native.join(name).canonicalize()?;
        let escaped = path
            .to_string_lossy()
            .replace('\\', "\\134")
            .replace(' ', "\\040")
            .replace('\t', "\\011")
            .replace('\n', "\\012");
        require(
            maps.lines()
                .any(|line| line.ends_with(&format!(" {escaped}"))),
            &format!("native application did not map {}", path.display()),
        )?;
        result.push(path);
    }
    Ok(result)
}

pub fn run_hosting_leases(options: Options) -> Result<RunReport> {
    deployment::preflight(&options.native_directory, &options.executable)?;
    let (native, dependencies) =
        process::absolute_directories(&options.native_directory, &options.dependency_directory)?;
    let mut bundle = deployment::Deployment::create(&options.run_parent, &options.executable)?;
    bundle
        .arguments
        .push("--hosting-lease-bootstrap-only".into());
    let mut server = ipc::Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        ipc::Config {
            node: node(&bundle),
            core_limits: Default::default(),
            max_admissions: 1,
            scratch_bytes: 8 * 1024 * 1024,
            decode_bytes: 256 * 1024,
            projection_bytes: 256 * 1024,
            reply_bytes: 64 * 1024,
            lease_duration: ipc::LOCAL_LEASE_DURATION,
        },
    )
    .map_err(|e| HostError(format!("production Hosting bind: {e:?}")))?;
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    executor.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = uuid::Uuid::new_v4().to_string();
        let mut child = process::OwnedApp::launch(
            &bundle,
            &native,
            &dependencies,
            &host,
            &listener.local_addr()?.to_string(),
        )?;
        let pid = child.pid();
        let code: wire::CodeContext = from_slice(&to_vec(&bundle.identity)?)?;
        let lease = server
            .admit(ipc::Admission {
                deadlines: ipc::RequestDeadlines::NativeClientLocal,
                host: wire::HostContext {
                    id: host.clone(),
                    pid,
                    kind: 1,
                    container: false,
                    activator: false,
                },
                code,
            })
            .map_err(|e| HostError(format!("production Hosting admission: {e:?}")))?;
        let (observed, mut observations) = mpsc::channel(64);
        let address = server.address();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let (client, _) = listener.accept().await?;
            let hosting = TcpStream::connect(address).await?;
            let (mut cr, mut cw) = client.into_split();
            let (mut hr, mut hw) = hosting.into_split();
            let requests = observe_frames(&mut cr, &mut hw, observed.clone(), Observation::Request);
            let replies = observe_frames(&mut hr, &mut cw, observed, Observation::Reply);
            tokio::select! { result = requests => result, result = replies => result }
        });
        let mut trace = Trace {
            pending: HashMap::new(),
            actions: Vec::new(),
            polls: 0,
            runtime: None,
            unregistered: false,
            registered: false,
        };
        let mut libraries = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(120);
        let outcome = async {
            while trace.polls < 2 {
                child.check_alive()?;
                require(
                    Instant::now() < deadline,
                    "native Hosting bootstrap/poll deadline",
                )?;
                let observation =
                    tokio::time::timeout(Duration::from_secs(10), observations.recv())
                        .await
                        .map_err(|_| HostError("native Hosting frame timeout".into()))?
                        .ok_or_else(|| {
                            HostError("native Hosting IPC closed before qualification".into())
                        })?;
                trace.observe(observation)?;
            }
            require(
                child
                    .callbacks()
                    .iter()
                    .any(|e| e["event"] == "hosting_lease_bootstrap_only"),
                "native bootstrap-only application marker absent",
            )?;
            require(
                !trace
                    .actions
                    .iter()
                    .any(|a| a == "RegisterServiceTypeRequest"),
                "factory traffic in bootstrap-only profile",
            )?;
            libraries = mapped_libraries(&native, pid)?;
            child.command(b"shutdown\n")?;
            while !trace.unregistered {
                let observation = tokio::time::timeout(Duration::from_secs(5), observations.recv())
                    .await
                    .map_err(|_| HostError("native runtime unregister timeout".into()))?
                    .ok_or_else(|| {
                        HostError("native IPC closed before runtime unregister".into())
                    })?;
                trace.observe(observation)?;
            }
            child.command(b"exit\n")?;
            child.wait_exit(deadline.min(Instant::now() + Duration::from_secs(5)))?;
            Ok::<_, HostError>(())
        }
        .await;
        println!(
            "MSSF_HOST {}",
            serde_json::json!({"event":"production_hosting_terminal_status",
            "status":format!("{:?}", server.status(&lease)),
            "outcome":outcome.as_ref().map_err(ToString::to_string)})
        );
        // Qualification observer and process cleanup cannot affect node authority:
        // revoke before joining any proxy/app cleanup and retain owned outcomes.
        lease.revoke();
        let outcome = complete(outcome, stop_observer(&mut tasks).await);
        server
            .stop()
            .map_err(|e| HostError(format!("Hosting stop: {e:?}")))?;
        let outputs = server
            .take_outputs()
            .map_err(|e| HostError(format!("Hosting output transfer: {e:?}")))?;
        let cleanup = child.cleanup();
        for line in child.logs() {
            println!("MSSF_NATIVE {line}");
        }
        let cleanup = complete(outcome, cleanup)?;
        require(
            outputs.operations.iter().all(|value| value.primary.is_ok()),
            "production Hosting operation failure",
        )?;
        require(
            !outputs.events.is_empty(),
            "production Hosting closure ownership absent",
        )?;
        Ok(RunReport {
            profile: "linux-x86_64-11.8.121.4-production-hosting-bootstrap-only".into(),
            run_root: bundle.root.clone(),
            host_id: host,
            scenario: Scenario::Bootstrap,
            pid,
            runtime_id: trace
                .runtime
                .ok_or_else(|| HostError("native runtime identity absent".into()))?,
            endpoint: None,
            partition_id: None,
            lease_polls: trace.polls,
            loaded_native_libraries: libraries,
            callbacks: child.callbacks(),
            cleanup,
            service_name: String::new(),
            service_type: String::new(),
            lifecycle_completed: false,
            runtime_unregistered: trace.unregistered,
        })
    })
}
