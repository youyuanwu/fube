//! Release-native factory producer against the actual owned local node.
//! This is not the five original canned lifecycle fixtures or H4 deployment.
use crate::hosting_leases::{mapped_libraries, node, Observation, Trace};
use crate::{
    complete, deployment, process, require, HostError, Options, Result, RunReport, Scenario,
};
use fube_hosting::{self as hosting, ipc, wire};
use fube_node::{Config, Input, Node, Response};
use fube_ra::{entity as ra, protocol};
use fube_ra_store::{MemoryBackend, Settings as StoreSettings, Store};
use fube_transport::{
    connection::{read_message, write_message},
    serialization::serde::{from_slice, to_vec},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinSet,
};

pub fn run_hosting_registration(options: Options) -> Result<RunReport> {
    deployment::preflight(&options.native_directory, &options.executable)?;
    let (native, dependencies) =
        process::absolute_directories(&options.native_directory, &options.dependency_directory)?;
    let bundle = deployment::Deployment::create(&options.run_parent, &options.executable)?;
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    executor.block_on(async {
        let store = Arc::new(Store::new(Box::new(MemoryBackend::new()), StoreSettings::default()).await
            .map_err(|e| HostError(format!("node store: {e}")))?);
        let ra = Arc::new(ra::Executor::new(store.clone(), ra::Admission {
            local_node: protocol::NodeInstance { id: [1; 16], instance: 1 },
            fm_source: protocol::NodeInstance::default(), generation: protocol::GenerationNumber::default(),
            owner_ready: false, node_open: true, node_activated: false, fabric_upgrading: false,
        }, ra::Settings::default()).map_err(|e| HostError(format!("local RA owner: {e:?}")))?);
        let cfg = Config::registration(ipc::Config {
            node: node(&bundle), core_limits: Default::default(), max_admissions: 1,
            scratch_bytes: 8 * 1024 * 1024, decode_bytes: 256 * 1024,
            projection_bytes: 256 * 1024, reply_bytes: 64 * 1024, lease_duration: ipc::LOCAL_LEASE_DURATION,
        });
        let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone())
            .map_err(|e| HostError(format!("registration node: {e:?}")))?;
        let handle = node.handle();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = uuid::Uuid::new_v4().to_string();
        // This parent owns the qualification launch, not production Hosting.
        let mut child = process::OwnedApp::launch(&bundle, &native, &dependencies, &host, &listener.local_addr()?.to_string())?;
        let pid = child.pid();
        let code: wire::CodeContext = from_slice(&to_vec(&bundle.identity)?)?;
        let Response::Prepared(prepared) = handle.submit(Input::Admit { admission: ipc::Admission {
            deadlines: ipc::RequestDeadlines::NativeClientLocal,
            host: wire::HostContext { id: host.clone(), pid, kind: 1, container: false, activator: false }, code,
        }, types: vec![bundle.service_type.clone()], source: "prepared-native-service-manifest".into() })
            .map_err(|e| HostError(format!("node admission submit: {e:?}")))?
            .wait(Duration::from_secs(3)).map_err(|e| HostError(format!("node admission: {e:?}")))?
            else { return Err(HostError("missing prepared native admission".into())); };
        require(prepared.failure.is_none() && prepared.admitted == 1, "native declaration preparation failure")?;
        let (observed, mut observations) = mpsc::channel(64);
        let address = node.address();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let (client, _) = listener.accept().await?;
            let server = TcpStream::connect(address).await?;
            let (mut cr, mut cw) = client.into_split(); let (mut sr, mut sw) = server.into_split();
            let upstream = observed.clone();
            let requests = async move {
                while let Ok(m) = read_message(&mut cr).await {
                    upstream.try_send(Observation::Request(m.clone())).map_err(|_| HostError("registration observer request budget".into()))?;
                    if write_message(&mut sw, &m).await.is_err() { break; }
                }
                Ok::<_, HostError>(())
            };
            let replies = async move {
                while let Ok(m) = read_message(&mut sr).await {
                    observed.try_send(Observation::Reply(m.clone())).map_err(|_| HostError("registration observer reply budget".into()))?;
                    if write_message(&mut cw, &m).await.is_err() { break; }
                }
                Ok::<_, HostError>(())
            };
            tokio::select! { result = requests => result, result = replies => result }
        });
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut trace = Trace::default();
        let mut type_request = None;
        let mut libraries = Vec::new();
        let mut receipts = Vec::new();
        let outcome = async {
            while !trace.registered || trace.polls < 2 {
                child.check_alive()?;
                require(Instant::now() < deadline, "native factory registration deadline")?;
                let observation = tokio::time::timeout(Duration::from_secs(10), observations.recv()).await
                    .map_err(|_| HostError("native registration frame timeout".into()))?
                    .ok_or_else(|| HostError("native IPC closed before factory registration".into()))?;
                if let Observation::Request(m) = &observation {
                    if m.properties()?.action == "RegisterServiceTypeRequest" {
                        require(type_request.is_none(), "unexpected duplicate native factory attempt")?;
                        type_request = Some(from_slice::<wire::TypeRequest>(&m.body)?);
                    }
                }
                trace.observe(observation)?;
            }
            let request = type_request.as_ref().ok_or_else(|| HostError("native type producer absent".into()))?;
            require(request.id.name == bundle.service_type && request.context.host.pid == pid, "native factory identity")?;
            libraries = mapped_libraries(&native, pid)?;
            let Response::Sources(sources) = handle.submit(Input::HealthSources).map_err(|e| HostError(format!("{e:?}")))?
                .wait(Duration::from_secs(3)).map_err(|e| HostError(format!("{e:?}")))? else { return Err(HostError("health source response".into())); };
            require(sources.len() == 1 && sources[0].last_disabled_report.is_some(), "managed source/report bookkeeping absent")?;
            let Response::HostStatus(status) = handle.submit(Input::HostStatus(prepared.lease.clone())).map_err(|e| HostError(format!("{e:?}")))?
                .wait(Duration::from_secs(3)).map_err(|e| HostError(format!("{e:?}")))? else { return Err(HostError("native session status".into())); };
            require(status.runtimes.len() == 1, "native runtime ownership")?;
            let Response::Lookup(lookup) = handle.submit(Input::Lookup(hosting::LookupRequest {
                service_type: sources[0].id.service_type.clone(), version: status.runtimes[0].version.clone(),
                activation: hosting::Activation::SHARED, service_name: bundle.service_name.clone(),
                application_name: sources[0].application_name.clone(), should_activate: false,
            })).map_err(|e| HostError(format!("{e:?}")))?.wait(Duration::from_secs(3)).map_err(|e| HostError(format!("{e:?}")))?
                else { return Err(HostError("native Hosting lookup response".into())); };
            require(lookup.result.is_ok(), "native factory not available through same-core lookup")?;
            let candidate = lookup.candidate.ok_or_else(|| HostError("native lookup candidate absent".into()))?;
            require(candidate.runtime_id == request.context.id && candidate.host_id == host
                && candidate.process_id == pid && candidate.package_instance_sequence == request.context.code.package_sequence,
                "native lookup operational provenance")?;
            println!("MSSF_HOST {}", serde_json::json!({"event":"production_hosting_factory_lookup",
                "runtime":candidate.runtime_id,"host":candidate.host_id,"pid":candidate.process_id,
                "type":candidate.type_instance.service_type.name,"package_sequence":candidate.package_instance_sequence,
                "code_sequence":request.context.code.code_sequence,"full_version":format!("{:?}", candidate.version),
                "lookup_sequence":lookup.sequence,"health_reporting":"disabled","fm_hm_rap_completion":false}));
            receipts.extend(handle.take_receipts());
            child.command(b"shutdown\n")?;
            while !trace.unregistered {
                let o = tokio::time::timeout(Duration::from_secs(5), observations.recv()).await
                    .map_err(|_| HostError("native registration runtime closure timeout".into()))?
                    .ok_or_else(|| HostError("native IPC closed before runtime unregister".into()))?;
                trace.observe(o)?;
            }
            child.command(b"exit\n")?;
            child.wait_exit(deadline.min(Instant::now() + Duration::from_secs(5)))?;
            // Consumer decision completion is explicit; never substitute drain
            // or an empty FM map for native-ready/deployed RA acknowledgment.
            let decision_due = Instant::now() + Duration::from_secs(3);
            loop {
                receipts.extend(handle.take_receipts());
                let registered = receipts.iter().any(|r| matches!(r.input, ipc::Delivery::Registration(hosting::ClosedRegisteredEvent::Registered { .. })));
                let closed = receipts.iter().any(|r| matches!(r.input, ipc::Delivery::Registration(hosting::ClosedRegisteredEvent::RuntimeClosed { .. })));
                if registered && closed { break; }
                require(Instant::now() < decision_due, "owned native registration/closure decisions absent")?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            require(child.callbacks().iter().any(|e| e["event"] == "type_registered"), "actual native factory marker absent")?;
            Ok::<_, HostError>(())
        }.await;
        prepared.lease.revoke();
        tasks.abort_all(); while tasks.join_next().await.is_some() {}
        node.stop().map_err(|e| HostError(format!("registration node stop: {e:?}")))?;
        let residual = handle.take_shutdown().ok_or_else(|| HostError("node shutdown residual missing".into()))?;
        let operations = residual.owner.take_operations().map_err(|e| HostError(format!("operation transfer: {e:?}")))?;
        receipts.extend(handle.take_receipts());
        let remaining = residual.owner.take_outputs().map_err(|e| HostError(format!("remaining Hosting outputs: {e:?}")))?;
        let cleanup = child.cleanup();
        for line in child.logs() { println!("MSSF_NATIVE {line}"); }
        let cleanup = complete(outcome, cleanup)?;
        require(operations.iter().all(|o| o.primary.is_ok()), "production native registration operation failure")?;
        require(receipts.iter().all(|r| !r.downstream_completion), "unearned native deployment/FM acknowledgment")?;
        println!("MSSF_HOST {}", serde_json::json!({"event":"production_hosting_owned_event_completion",
            "decisions":receipts.iter().map(|r|format!("{:?}:{:?}",r.input,r.root)).collect::<Vec<_>>(),
            "pending_owned_obligations":residual.pending.len()+remaining.obligations.len(),
            "remaining_events":remaining.events.len(),"native_fm_readiness":false,
            "qualified":"actual native factory registration, same-core lookup, owned local registration/runtime closure decisions",
            "not_qualified":"native node handlers, FM/HM/RA-ready/deployed lifecycle, health reporting, package/process cleanup"}));
        ra.close(Duration::from_secs(3)).await.map_err(|e| HostError(format!("local RA close: {e:?}")))?;
        store.close().await.map_err(|e| HostError(format!("local store close: {e}")))?;
        Ok(RunReport {
            profile: "linux-x86_64-11.8.121.4-production-hosting-registration-events-local-health-disabled".into(),
            run_root: bundle.root.clone(), host_id: host, scenario: Scenario::Bootstrap, pid,
            runtime_id: trace.runtime.ok_or_else(|| HostError("native runtime ID absent".into()))?,
            endpoint: None, partition_id: None, lease_polls: trace.polls, loaded_native_libraries: libraries,
            callbacks: child.callbacks(), cleanup, service_name: bundle.service_name.clone(),
            service_type: bundle.service_type.clone(), lifecycle_completed: false, runtime_unregistered: trace.unregistered,
        })
    })
}
