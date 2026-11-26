//! Clusterless native stateless lifecycle through the normal local Node owner.
use crate::{
    complete, deployment, hosting_leases::mapped_libraries, require, HostError, Options, Result,
    RunReport, Scenario,
};
use fube_hosting::{
    self as hosting,
    local::{Activation, NativeRuntime},
};
use fube_node::local::{self, CommandReceipt, EffectReceipt, LocalDisposition, Node};
use fube_ra::protocol::NativeReplicaState;
use fube_ra::{entity as ra, protocol as p};
use fube_ra_store::{Settings, Store};
use fubst::serde::{from_slice, to_vec, ByteArray};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn error(error: local::Error) -> HostError {
    HostError(format!("local Node: {error:?}"))
}

fn package(parent: &Path, executable: &Path) -> Result<PathBuf> {
    let root = parent.join(format!("node-rap-package-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&root)?;
    fs::create_dir_all(root.join("NativeService/Code"))?;
    fs::write(
        root.join("ApplicationManifest.xml"),
        r#"<ApplicationManifest xmlns="http://schemas.microsoft.com/2011/01/fabric" ApplicationTypeName="NativeTestHost" ApplicationTypeVersion="1.0">
<ServiceManifestImport><ServiceManifestRef ServiceManifestName="NativeService" ServiceManifestVersion="1.0" /></ServiceManifestImport>
<DefaultServices><Service Name="Singleton"><StatelessService ServiceTypeName="NativeStatelessType" InstanceCount="1"><SingletonPartition /></StatelessService></Service></DefaultServices>
</ApplicationManifest>"#,
    )?;
    fs::write(
        root.join("NativeService/ServiceManifest.xml"),
        r#"<ServiceManifest xmlns="http://schemas.microsoft.com/2011/01/fabric" Name="NativeService" Version="1.0">
<ServiceTypes><StatelessServiceType ServiceTypeName="NativeStatelessType" /></ServiceTypes>
<CodePackage Name="Code" Version="1.0"><EntryPoint><ExeHost><Program>stateless-test-app</Program><WorkingFolder>CodePackage</WorkingFolder></ExeHost></EntryPoint></CodePackage>
<Resources />
</ServiceManifest>"#,
    )?;
    fs::copy(
        executable,
        root.join("NativeService/Code/stateless-test-app"),
    )?;
    Ok(root)
}

fn admission() -> ra::Admission {
    let mut id = [0; 16];
    id[8] = 1;
    ra::Admission {
        local_node: p::NodeInstance { id, instance: 1 },
        fm_source: p::NodeInstance::default(),
        generation: p::GenerationNumber::default(),
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}

fn body(node: &Node) -> Result<p::AddInstance> {
    let activation = node.activation();
    let premise = activation
        .premise()
        .ok_or_else(|| HostError("local package premise absent".into()))?;
    let identity = activation
        .identity()
        .ok_or_else(|| HostError("local host identity absent".into()))?;
    let service_type = activation
        .declared_type()
        .ok_or_else(|| HostError("local type declaration absent".into()))?;
    let mut body = p::AddInstance::default();
    body.unit.id.guid.0 = uuid::Uuid::new_v4().to_bytes_le();
    body.unit.consistency.id = body.unit.id.clone();
    body.replica.node = admission().local_node;
    body.replica.id = 1;
    body.replica.instance = 1;
    body.replica.state = NativeReplicaState::InBuild;
    body.service.name = premise.service_name.clone();
    body.service.application = premise.application_name.clone();
    body.service.service_type = p::ServiceType {
        package: p::PackageId {
            application: p::ApplicationId {
                type_name: service_type.package.application.type_name,
                number: service_type.package.application.number,
            },
            name: service_type.package.name,
        },
        name: service_type.name,
    };
    body.service.version = from_slice(&to_vec(&identity.code.version)?)?;
    body.replica.version = body.service.version.clone();
    body.service.partitions = 1;
    body.service.target = 1;
    body.service.minimum = 1;
    body.service.initialization = ByteArray(b"native-test-host".to_vec());
    Ok(body)
}

fn progress(
    node: &mut Node,
    effects: &mut Vec<EffectReceipt>,
    commands: &mut Vec<CommandReceipt>,
) -> Result<()> {
    node.drive().map_err(error)?;
    let incoming = node.take_command_receipts();
    for receipt in &incoming {
        receipt
            .result
            .clone()
            .map_err(|e| HostError(format!("native RA command {}: {e:?}", receipt.id)))?;
    }
    require(
        commands.len() + incoming.len() <= 32,
        "native command receipt budget",
    )?;
    commands.extend(incoming);
    let incoming = node.take_effect_receipts();
    require(
        effects.len() + incoming.len() <= 32,
        "native effect receipt budget",
    )?;
    effects.extend(incoming);
    require(
        node.activation()
            .process_status()
            .is_none_or(|s| s.dropped_log_records == 0),
        "native log records dropped",
    )?;
    Ok(())
}

fn callbacks(node: &Node) -> Result<Vec<serde_json::Value>> {
    node.activation()
        .logs()
        .iter()
        .filter_map(|line| {
            line.find("MSSF_APP ").map(|start| {
                serde_json::from_str(&line[start + 9..])
                    .map_err(|e| HostError(format!("native callback marker: {e}")))
            })
        })
        .collect()
}

async fn probe(endpoint: &str, open: bool) -> Result<()> {
    let address: std::net::SocketAddr = endpoint
        .parse()
        .map_err(|e| HostError(format!("native endpoint: {e}")))?;
    require(
        address.ip().is_loopback() && address.port() != 0,
        "native endpoint must be actual loopback listener",
    )?;
    tokio::time::timeout(Duration::from_secs(2), async {
        let connection = tokio::net::TcpStream::connect(address).await;
        if !open {
            return match connection {
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => Ok(()),
                Err(e) => Err(e.into()),
                Ok(_) => Err(HostError("native listener remains live after Close".into())),
            };
        }
        let mut stream = connection?;
        stream.write_all(b"ping\n").await?;
        let mut reply = [0; 22];
        stream.read_exact(&mut reply).await?;
        require(
            &reply == b"native-stateless-pong\n",
            "native application endpoint response",
        )
    })
    .await
    .map_err(|_| HostError("native endpoint probe timeout".into()))?
}

async fn cleanup(
    node: &mut Node,
    unit: Option<&p::PartitionId>,
    effects: &mut Vec<EffectReceipt>,
    commands: &mut Vec<CommandReceipt>,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut dropped = false;
    loop {
        match node.terminate() {
            Ok(true) => break,
            Ok(false) | Err(local::Error::Activation(hosting::local::LocalError::Busy)) => {}
            Err(e) => return Err(error(e)),
        }
        require(Instant::now() < deadline, "native Node cleanup deadline")?;
        progress(node, effects, commands)?;
        if !dropped {
            if let Some(unit) = unit {
                if node.snapshot(unit.guid.0).committed.is_some_and(|s| {
                    s.phase == ra::Phase::Opening && s.runtime.is_none() && s.usage.is_some()
                }) {
                    node.close(unit.clone(), ra::CloseMode::Drop)
                        .map_err(error)?;
                    dropped = true;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    while !node.quiescent() {
        require(
            Instant::now() < deadline,
            "native upstream cleanup drain deadline",
        )?;
        progress(node, effects, commands)?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    node.close_executor(Duration::from_secs(3))
        .await
        .map_err(error)
}

pub fn run_node_rap(options: Options) -> Result<RunReport> {
    deployment::preflight(&options.native_directory, &options.executable)?;
    let inputs = NativeRuntime::new(&options.native_directory, &options.dependency_directory)?;
    let parent = options.run_parent.canonicalize()?;
    let package = package(&parent, &options.executable)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let store = Arc::new(Store::memory(Settings::default()).await.map_err(|e| HostError(format!("native Node store: {e}")))?);
        let mut settings = hosting::local::Options::new(parent);
        settings.native_runtime = Some(inputs.clone());
        let activation = Activation::new(settings).map_err(|e| HostError(format!("native activation: {e}")))?;
        let mut node = Node::new(activation, store.clone(), admission(), ra::Settings::default()).map_err(error)?;
        let mut effects = Vec::new();
        let mut commands = Vec::new();
        let mut selected = None;
        let mut root = PathBuf::new();
        let mut host = String::new();
        let mut pid = 0;
        let mut runtime_id = String::new();
        let mut endpoint = String::new();
        let mut libraries = Vec::new();
        let outcome = async {
            node.start(&package, "fabric:/NativeTestHost", &[]).map_err(error)?;
            root = node.activation().root().ok_or_else(|| HostError("native run root absent".into()))?.to_owned();
            host = node.activation().identity().ok_or_else(|| HostError("native host absent".into()))?.host_id.clone();
            pid = node.activation().process_status().ok_or_else(|| HostError("native process absent".into()))?.pid;
            println!("MSSF_HOST {}", serde_json::json!({"event":"node_native_launch","host":host,"pid":pid,"root":root,"package":package,"launch":"shared-owned-Activation","fm_network":false}));
            let deadline = Instant::now() + Duration::from_secs(30);
            while !node.package_registration().registered {
                require(Instant::now() < deadline, "native Node registration deadline")?;
                progress(&mut node, &mut effects, &mut commands)?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            libraries = mapped_libraries(inputs.directory(), pid)?;
            let body = body(&node)?;
            let unit = body.unit.id.clone();
            selected = Some(unit.clone());
            let add = node.add(body.clone()).map_err(error)?;
            while !node.snapshot(unit.guid.0).committed.is_some_and(|s| s.phase == ra::Phase::Ready) {
                require(Instant::now() < deadline, "native Node Open deadline")?;
                progress(&mut node, &mut effects, &mut commands)?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let state = node.snapshot(unit.guid.0).committed.ok_or_else(|| HostError("native Open state absent".into()))?;
            let registration = state.runtime.as_ref().ok_or_else(|| HostError("native runtime registration absent".into()))?;
            runtime_id = registration.runtime.clone();
            endpoint = state.body.replica.service_endpoint.clone();
            require(registration.host == host && state.usage.is_some() && node.retained_usage() == 1, "native Open original runtime/acquisition")?;
            require(commands.iter().any(|r| r.id == add && r.result == Ok(ra::Disposition::Applied)), "native Add did not commit")?;
            require(effects.iter().filter(|r| matches!(r.dispatch.effect, ra::Effect::RuntimeOpen { .. }) && matches!(r.disposition, LocalDisposition::Written)).count() == 1, "native Open local write receipt absent or duplicated")?;
            probe(&endpoint, true).await?;
            println!("MSSF_HOST {}", serde_json::json!({"event":"node_native_open_applied","partition":uuid::Uuid::from_bytes_le(unit.guid.0).to_string(),"host":host,"runtime":runtime_id,"acquisition":state.usage,"endpoint":endpoint,"semantic_phase":"Ready","write_receipt":"local-only"}));
            let close = node.close(unit.clone(), ra::CloseMode::Close).map_err(error)?;
            while !node.snapshot(unit.guid.0).committed.is_some_and(|s| s.phase == ra::Phase::Closed) || node.retained_usage() != 0 || !node.quiescent() {
                require(Instant::now() < deadline, "native Node Close/release deadline")?;
                progress(&mut node, &mut effects, &mut commands)?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            require(commands.iter().any(|r| r.id == close && r.result == Ok(ra::Disposition::Applied)), "native Close command did not commit")?;
            require(effects.iter().filter(|r| matches!(r.dispatch.effect, ra::Effect::Close { .. }) && matches!(r.disposition, LocalDisposition::Written)).count() == 1, "native Close local write receipt absent or duplicated")?;
            let disposal = node.instance_disposal().ok_or_else(|| HostError("native successful Close exact-disposal lineage absent".into()))?;
            require(disposal.host == host && disposal.runtime == runtime_id && state.usage == Some(disposal.lookup), "native exact acquisition release lineage")?;
            require(effects.iter().filter(|r| r.dispatch.effect == ra::Effect::ReleaseUsage { acquisition: disposal.lookup } && matches!(r.disposition, LocalDisposition::ExactUsageReleased)).count() == 1, "native exact usage release absent or duplicated")?;
            require(node.activation().session_status().map_err(|e| HostError(format!("native session: {e}")))?.is_some_and(|s| s.connected), "service Close incorrectly retired host")?;
            probe(&endpoint, false).await?;
            let marker_due = Instant::now() + Duration::from_secs(2);
            let markers = loop {
                let markers = callbacks(&node)?;
                if markers.iter().any(|m| m["event"] == "close") {
                    break markers;
                }
                require(Instant::now() < marker_due, "native Close marker capture deadline")?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            for name in ["create", "open", "close"] {
                require(markers.iter().filter(|m| m["event"] == name).count() == 1, &format!("native callback {name} absent or duplicated"))?;
            }
            require(!markers.iter().any(|m| m["event"] == "abort"), "unexpected native Abort")?;
            require(markers.iter().any(|m| m["event"] == "create" && m["type"] == body.service.service_type.name && m["instance"] == body.replica.instance), "native factory type/instance provenance")?;
            require(commands.iter().any(|r| r.id == disposal.command && r.result == Ok(ra::Disposition::Applied)), "native semantic Close receipt did not commit")?;
            let partition = uuid::Uuid::from_bytes_le(unit.guid.0).to_string();
            require(markers.iter().filter(|m| matches!(m["event"].as_str(), Some("create" | "open" | "close"))).all(|m| m["partition"].as_str().is_some_and(|p| p.eq_ignore_ascii_case(&partition))), "native callback partition provenance")?;
            println!("MSSF_HOST {}", serde_json::json!({"event":"node_native_close_applied","disposal":disposal,"host_still_connected":true,"endpoint_connection_refused":true,"retained_usage":0,"semantic_phase":"Closed","frame_write_is_semantic_ack":false}));
            Ok::<_, HostError>(())
        }.await;
        let cleanup = cleanup(&mut node, selected.as_ref(), &mut effects, &mut commands).await;
        let closed_store = store.close().await.map_err(|e| HostError(format!("native store close: {e}"))).and_then(|s| require(s.is_clean(), "native store cleanup incomplete"));
        let outcome = complete(outcome, cleanup);
        let outcome = complete(outcome, closed_store);
        for line in node.activation().logs() { println!("MSSF_NATIVE {line}"); }
        outcome?;
        let process = node.activation().process_status().ok_or_else(|| HostError("native final process status absent".into()))?;
        let cleanup = node.activation().cleanup_status();
        require(process.reaped && process.group_drained && cleanup.directory_released && !root.exists(), "native original process/group/root not retired")?;
        let markers = callbacks(&node)?;
        println!("MSSF_HOST {}", serde_json::json!({"event":"node_native_owned_cleanup","pid":pid,"forced":process.cleanup_forced,"reaped":process.reaped,"process_group_drained":process.group_drained,"directory_released":cleanup.directory_released,"host_unregister_observed":false,"runtime_unregister_observed":false,"scope":"local stateless Node/RAP, not deployed FM/Federation or stateful lifecycle"}));
        Ok(RunReport {
            profile: "linux-x86_64-11.8.121.4-local-node-native-stateless-rap-open-close".into(),
            run_root: root, host_id: host, scenario: Scenario::Close, pid, runtime_id,
            endpoint: Some(endpoint),
            partition_id: selected.map(|unit| uuid::Uuid::from_bytes_le(unit.guid.0).to_string()),
            lease_polls: 0, loaded_native_libraries: libraries, callbacks: markers,
            cleanup: serde_json::json!({"pid":pid,"forced":process.cleanup_forced,"reaped":process.reaped,"process_group_drained":process.group_drained,"directory_released":cleanup.directory_released}),
            service_name: "fabric:/NativeTestHost/Singleton".into(),
            service_type: "NativeStatelessType".into(),
            lifecycle_completed: true, runtime_unregistered: false,
        })
    })
}
