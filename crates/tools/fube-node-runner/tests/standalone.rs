//! Real standalone Node processes with a fixture FM, not container qualification.
use fube_local_host::process::OwnedApp;
use fube_node::control::wire::{self, Link};
use fube_node_runner::{Config, Fm, Identity, Placement};
use fube_ra::protocol::*;
use fube_transport::{message::Message, serialization::serde::ObjectArray};
use serde_json::Value;
use std::{
    fs,
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    process::Command,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};
#[path = "../../fube-cluster/tests/support/mod.rs"]
mod support;

struct Fixture {
    root: PathBuf,
    configs: Vec<Config>,
    children: Vec<OwnedApp>,
}

fn reserve(ip: Ipv4Addr) -> SocketAddrV4 {
    let listener = std::net::TcpListener::bind((ip, 0)).unwrap();
    let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    address
}

impl Fixture {
    fn new(fm: SocketAddrV4) -> Self {
        let root = support::parent().join(format!("external-node-{}", uuid::Uuid::new_v4()));
        let package = root.join("package");
        fs::create_dir_all(package.join("LocalService/Code")).unwrap();
        fs::write(
            package.join("ApplicationManifest.xml"),
            include_bytes!(
                "../../../samples/hosting-local-app/pkg-stateful/ApplicationManifest.xml"
            ),
        )
        .unwrap();
        fs::write(
            package.join("LocalService/ServiceManifest.xml"),
            include_bytes!(
                "../../../samples/hosting-local-app/pkg-stateful/LocalService/ServiceManifest.xml"
            ),
        )
        .unwrap();
        fs::copy(
            support::binary("hosting-local-app", "FUBE_NODE_APP"),
            package.join("LocalService/Code/hosting-local-app"),
        )
        .unwrap();
        let placements: Vec<_> = (1..=3)
            .map(|id| Placement {
                identity: Identity {
                    id: format!("{id:x}"),
                    instance: 1,
                },
                address: reserve(Ipv4Addr::new(127, 0, 0, id + 1)),
                replica_id: i64::from(id) * 10,
                replica_instance: 1,
            })
            .collect();
        let deployment = uuid::Uuid::new_v4().to_string();
        let partition = uuid::Uuid::new_v4().to_string();
        let configs = placements.iter().enumerate().map(|(index, placement)| Config {
            schema_version: 1, deployment: deployment.clone(), name: format!("node{}", index + 1),
            node: placement.clone(),
            fm: Fm { identity: Identity { id: "f".into(), instance: 1 }, address: fm, generation: 1 },
            peers: placements.iter().enumerate().filter(|(peer, _)| *peer != index)
                .map(|(_, peer)| peer.clone()).collect(),
            partition: partition.clone(), package: package.clone(), application: "fabric:/External".into(),
            package_version_instance: 1,
            parameters: vec![
                ("Arguments".into(), format!("--stateful-lifecycle --data-replicator --replication-listen {}:0 --service-listen {}:0",
                    placement.address.ip(), placement.address.ip())),
                ("TargetReplicaSetSize".into(), "3".into()),
                ("MinReplicaSetSize".into(), "2".into()),
            ],
            run_directory: root.join(format!("node{}", index + 1)),
            startup_timeout_ms: 10_000, shutdown_timeout_ms: 20_000,
        }).collect();
        Self {
            root,
            configs,
            children: vec![],
        }
    }

    fn start(&mut self) {
        for (index, config) in self.configs.iter().enumerate() {
            config.validate().unwrap();
            let path = self.root.join(format!("node{index}.json"));
            fs::write(&path, serde_json::to_vec(config).unwrap()).unwrap();
            let mut command = Command::new(support::binary("fube-node", "FUBE_NODE_EXE"));
            command.arg("--config").arg(path);
            let mut child = None;
            OwnedApp::start(&mut child, command).unwrap();
            self.children.push(child.unwrap());
        }
    }

    fn status(&self, index: usize) -> Value {
        let path = self.configs[index].run_directory.join("status.json");
        if !path.exists() {
            return Value::Null;
        }
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }

    fn diagnostics(&self) -> String {
        format!(
            "root={:?}, status={:?}, logs={:?}",
            self.root,
            (0..3).map(|index| self.status(index)).collect::<Vec<_>>(),
            self.children.iter().map(OwnedApp::logs).collect::<Vec<_>>()
        )
    }

    fn terminate(&mut self) {
        for child in &self.children {
            assert!(Command::new("kill")
                .args(["-TERM", &child.pid().to_string()])
                .status()
                .unwrap()
                .success());
        }
    }

    fn finish(mut self, expected_clean: [bool; 3]) {
        for (index, child) in self.children.iter_mut().enumerate() {
            let deadline = std::time::Instant::now() + Duration::from_secs(25);
            let status = loop {
                if let Some(status) = child.observed_exit().unwrap() {
                    break status;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "Node exit: {:?}",
                    child.logs()
                );
                std::thread::sleep(Duration::from_millis(5));
            };
            assert_eq!(
                status.code(),
                Some(if expected_clean[index] { 0 } else { 1 })
            );
            let receipt = child.cleanup().unwrap();
            assert_eq!(receipt["forced"], false);
        }
        for (index, expected) in expected_clean.into_iter().enumerate() {
            let status = self.status(index);
            assert_eq!(
                status["cleanup"]["clean"],
                expected,
                "{}",
                self.diagnostics()
            );
            assert_eq!(status["cleanup"]["application_cleaned"], true);
            assert_eq!(status["cleanup"]["executor_closed"], true);
            assert_eq!(status["cleanup"]["store_closed"], true);
            let pid = status["application_pid"].as_u64().unwrap();
            assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
            assert!(
                fs::read_dir(self.configs[index].run_directory.join("hosting"))
                    .unwrap()
                    .next()
                    .is_none()
            );
        }
        fs::remove_dir_all(&self.root).unwrap();
    }
}

struct FmFixture {
    links: Vec<Option<Link>>,
    configurations: Vec<Config>,
    bootstrapped: [bool; 3],
    primary: Option<AddPrimaryReply>,
    builds: Vec<AddReplicaReply>,
    configured: Vec<ConfigurationReply>,
    deleted: [bool; 3],
}

impl FmFixture {
    async fn accept(listener: TcpListener, fixture: &mut Fixture) -> Self {
        let mut links = vec![None; 3];
        let deadline = Instant::now() + Duration::from_secs(10);
        for _ in 0..3 {
            let accepted = tokio::time::timeout_at(deadline, listener.accept())
                .await
                .unwrap_or_else(|_| panic!("FM accept: {}", fixture.diagnostics()))
                .unwrap();
            let index = fixture
                .configs
                .iter()
                .position(|config| accepted.1.ip() == *config.node.address.ip())
                .unwrap();
            let channel = fube_transport::accept_unsecured_tcp(
                accepted.0,
                wire::composition(true).unwrap(),
                wire::limits(),
                deadline,
            )
            .await
            .unwrap();
            let config = &fixture.configs[index];
            links[index] = Some(
                Link::new(
                    channel,
                    config.fm.identity.native().unwrap(),
                    config.node.identity.native().unwrap(),
                    config.generation().unwrap(),
                )
                .unwrap(),
            );
        }
        Self {
            links,
            configurations: fixture.configs.clone(),
            bootstrapped: [false; 3],
            primary: None,
            builds: vec![],
            configured: vec![],
            deleted: [false; 3],
        }
    }

    async fn step(&mut self) {
        for index in 0..3 {
            let Some(link) = &self.links[index] else {
                continue;
            };
            let incoming = match link
                .receive(Instant::now() + Duration::from_millis(2))
                .await
            {
                Ok(incoming) => incoming,
                Err(error) if self.deleted[index] => {
                    eprintln!("original Node Channel closed after Delete: {error}");
                    continue;
                }
                Err(error) => panic!("original Node Channel failed: {error}"),
            };
            let Some(mut incoming) = incoming else {
                continue;
            };
            wire::predecode(&incoming.message).unwrap();
            let generation = self.configurations[index].generation().unwrap();
            match incoming.message.properties().unwrap().action.as_str() {
                "NodeUp" => {
                    let body = decode_node_up(&incoming.message).unwrap();
                    body.validate_fresh_stateless().unwrap();
                    let ack =
                        NodeUpAck::fresh_activated(FabricVersionInstance::native_default(), 0)
                            .unwrap();
                    incoming
                        .reply
                        .take()
                        .unwrap()
                        .reply(
                            link.seal(node_up_ack_message(&ack, &generation).unwrap())
                                .unwrap(),
                            wire::deadline(),
                        )
                        .await
                        .unwrap();
                }
                "ReplicaUp" => {
                    let (body, _) = decode_replica_up(&incoming.message).unwrap();
                    assert!(
                        body.last && body.replicas.0.is_empty() && body.dropped.0.is_empty(),
                        "healthy fixture does not authorize terminal-loss recovery"
                    );
                    link.send(replica_up_reply_message(&body, &generation).unwrap())
                        .await
                        .unwrap();
                    self.bootstrapped[index] = true;
                }
                "ServiceTypeNotification" => {
                    let body = decode_service_type_notification(&incoming.message).unwrap();
                    assert!(!body.infos.0.is_empty());
                    link.send(service_type_notification_reply_message(&body, &generation).unwrap())
                        .await
                        .unwrap();
                }
                "ServiceTypeEnabled" | "ServiceTypeDisabled" => {
                    let enabled =
                        incoming.message.properties().unwrap().action == "ServiceTypeEnabled";
                    let body = if enabled {
                        decode_service_type_enabled(&incoming.message)
                    } else {
                        decode_service_type_disabled(&incoming.message)
                    }
                    .unwrap();
                    let reply = ServiceTypeUpdateReply {
                        sequence: body.sequence,
                    };
                    link.send(
                        if enabled {
                            service_type_enabled_reply_message(&reply, &generation)
                        } else {
                            service_type_disabled_reply_message(&reply, &generation)
                        }
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                }
                "AddPrimaryReply" => {
                    let (body, _) = decode_add_primary_reply(&incoming.message).unwrap();
                    assert_eq!(body.error, 0);
                    self.primary = Some(body);
                }
                "AddReplicaReply" => {
                    let (body, _) = decode_add_replica_reply(&incoming.message).unwrap();
                    assert_eq!(body.error, 0);
                    self.builds.push(body);
                }
                "DoReconfigurationReply" => {
                    let (body, _) = decode_do_reconfiguration_reply(&incoming.message).unwrap();
                    assert_eq!(body.error, 0);
                    self.configured.push(body);
                }
                "DeleteReplicaReply" => {
                    let (body, _) = decode_delete_replica_reply(&incoming.message).unwrap();
                    assert_eq!(body.error, 0);
                    self.deleted[index] = true;
                }
                action => panic!("unexpected fixture FM action: {action}"),
            }
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
        }
    }

    async fn until(&mut self, fixture: &mut Fixture, condition: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(8);
        while !condition(self) {
            assert!(
                Instant::now() < deadline,
                "FM progress: {}",
                fixture.diagnostics()
            );
            for child in &mut fixture.children {
                child
                    .check_alive()
                    .unwrap_or_else(|error| panic!("{error}: {:?}", child.logs()));
            }
            self.step().await;
        }
    }

    async fn send(&self, index: usize, message: Message) {
        self.links[index]
            .as_ref()
            .unwrap()
            .send(message)
            .await
            .unwrap();
    }
}

fn primary(config: &Config) -> AddPrimary {
    let mut body = AddPrimary::default();
    body.unit.id = config.partition_id().unwrap();
    body.unit.consistency.id = body.unit.id.clone();
    body.unit.current = Epoch {
        data_loss: 1,
        configuration: (1 << 32) + 1,
    };
    body.unit.target = 3;
    body.unit.minimum = 2;
    body.replica.node = config.node.identity.native().unwrap();
    body.replica.id = config.node.replica_id;
    body.replica.instance = config.node.replica_instance;
    body.replica.current_role = 4;
    body.replica.state = 1;
    body.replica.version = VersionInstance {
        instance: config.package_version_instance,
        ..VersionInstance::default()
    };
    body.service.name = format!("{}/Singleton", config.application);
    body.service.application = config.application.clone();
    body.service.instance = 1;
    body.service.stateful = true;
    body.service.target = 3;
    body.service.minimum = 2;
    body.service.partitions = 1;
    body.service.version = body.replica.version.clone();
    body.service.service_type = ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: "HostingLocalStatefulApp".into(),
                number: 1,
            },
            name: "LocalService".into(),
        },
        name: "LocalStatefulType".into(),
    };
    body
}

async fn application(address: &str, request: &str) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(request.as_bytes()).await.unwrap();
        socket.write_all(b"\n").await.unwrap();
        let mut bytes = vec![];
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_standalone_nodes_obey_external_fm_initial_membership_replay_and_owned_shutdown() {
    journey(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_fm_channel_preserves_original_writer_until_explicit_owned_shutdown() {
    journey(true).await;
}

async fn journey(disconnect_primary: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let mut fixture = Fixture::new(address);
    fixture.start();
    let mut fm = FmFixture::accept(listener, &mut fixture).await;
    fm.until(&mut fixture, |fm| {
        fm.bootstrapped.iter().all(|ready| *ready)
    })
    .await;
    let selected = 1;
    let body = primary(&fixture.configs[selected]);
    let generation = fixture.configs[selected].generation().unwrap();
    fm.send(selected, add_primary_message(&body, &generation).unwrap())
        .await;
    fm.until(&mut fixture, |fm| fm.primary.is_some()).await;
    let endpoint = fm
        .primary
        .as_ref()
        .unwrap()
        .replica
        .service_endpoint
        .clone();
    assert!(!application(&endpoint, "write").await.starts_with("OK"));
    for destination in [0, 2] {
        let mut build = body.clone();
        let placement = &fixture.configs[destination].node;
        build.replica = Replica {
            node: placement.identity.native().unwrap(),
            id: placement.replica_id,
            instance: placement.replica_instance,
            current_role: 2,
            state: 1,
            version: body.service.version.clone(),
            ..Replica::default()
        };
        let before = fm.builds.len();
        fm.send(selected, add_replica_message(&build, &generation).unwrap())
            .await;
        fm.until(&mut fixture, |fm| fm.builds.len() > before).await;
    }
    let mut replicas = vec![fm.primary.as_ref().unwrap().replica.clone()];
    replicas[0].previous_role = 4;
    for build in &fm.builds {
        let mut replica = build.replica.clone();
        replica.previous_role = 2;
        replica.current_role = 3;
        replicas.push(replica);
    }
    let mut unit = body.unit.clone();
    unit.previous = unit.current;
    unit.current.configuration += 1;
    let configuration = DoReconfiguration {
        configuration: Configuration {
            unit: unit.clone(),
            service: body.service.clone(),
            replicas: ObjectArray(replicas),
        },
        phase0_duration: i64::MAX,
    };
    fm.send(
        selected,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    fm.until(&mut fixture, |fm| fm.configured.len() == 1).await;
    let mut completed = unit.clone();
    completed.previous = Epoch::default();
    assert_eq!(fm.configured[0].unit, completed);
    let pids: Vec<_> = (0..3)
        .map(|index| fixture.status(index)["application_pid"].clone())
        .collect();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !application(&endpoint, "read").await.starts_with("OK") {
        assert!(
            Instant::now() < deadline,
            "public read readiness: {}",
            fixture.diagnostics()
        );
    }
    assert!(application(&endpoint, "write").await.starts_with("OK"));
    assert!(application(&endpoint, "read")
        .await
        .contains("value=acknowledged"));
    fm.send(
        selected,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
    assert_eq!(fm.configured[0], fm.configured[1]);
    if disconnect_primary {
        fm.links[selected].take().unwrap().close().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while fixture.status(selected)["control_unknown"] != true {
            assert!(
                Instant::now() < deadline,
                "FM loss observation: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        for child in &mut fixture.children {
            child.check_alive().unwrap();
        }
        assert!(application(&endpoint, "write-more").await.starts_with("OK"));
        assert!(application(&endpoint, "read")
            .await
            .contains("value=continued"));
    }
    for (index, pid) in pids.iter().enumerate() {
        assert_eq!(&fixture.status(index)["application_pid"], pid);
        if disconnect_primary && index == selected {
            continue;
        }
        let replica = configuration
            .configuration
            .replicas
            .0
            .iter()
            .find(|replica| replica.node == fixture.configs[index].node.identity.native().unwrap())
            .unwrap();
        let delete = DeleteReplica::new(
            AddInstance {
                unit: completed.clone(),
                service: body.service.clone(),
                replica: replica.clone(),
            },
            false,
        );
        fm.send(index, delete_replica_message(&delete, &generation).unwrap())
            .await;
    }
    fm.until(&mut fixture, |fm| {
        fm.deleted
            .iter()
            .enumerate()
            .all(|(index, deleted)| *deleted || (disconnect_primary && index == selected))
    })
    .await;
    fixture.terminate();
    let deadline = Instant::now() + Duration::from_secs(10);
    while (0..3).any(|index| fixture.status(index)["stage"] != "stopped") {
        assert!(
            Instant::now() < deadline,
            "Node shutdown: {}",
            fixture.diagnostics()
        );
        fm.step().await;
    }
    for link in fm.links.iter().flatten() {
        link.close().await.unwrap();
    }
    let mut expected = [true; 3];
    expected[selected] = !disconnect_primary;
    fixture.finish(expected);
}

#[test]
fn invalid_static_topology_rejects_before_acquiring_a_run_directory() {
    let fixture = Fixture::new("127.0.0.1:19001".parse().unwrap());
    let original = fixture.configs[0].clone();
    for case in 0..5 {
        let mut config = original.clone();
        match case {
            0 => config.peers[0].identity = config.node.identity.clone(),
            1 => config.peers[0].address.set_ip(*config.node.address.ip()),
            2 => config.node.address.set_ip(Ipv4Addr::UNSPECIFIED),
            3 => config.fm.generation = 0,
            _ => config.schema_version = 2,
        }
        assert!(config.validate().is_err());
        let path = fixture.root.join(format!("invalid-{case}.json"));
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let output = Command::new(support::binary("fube-node", "FUBE_NODE_EXE"))
            .arg("--config")
            .arg(path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["event"], "error");
        assert!(!config.run_directory.exists());
    }
    fs::remove_dir_all(fixture.root).unwrap();
}
