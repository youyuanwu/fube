//! Real standalone Node binaries with a native fixture FM.
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
            assert_eq!(status["cleanup"]["channels_closed"], true);
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
    uploaded: [Option<LfumUpload>; 3],
    primary: Option<AddPrimaryReply>,
    builds: Vec<AddReplicaReply>,
    configured: Vec<ConfigurationReply>,
    losses: Vec<usize>,
    deleted: [bool; 3],
    hold_build: bool,
    held_build: Option<AddReplicaReply>,
    hold_configuration: bool,
    held_configuration: Option<ConfigurationReply>,
    stopping: bool,
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
            uploaded: [None, None, None],
            primary: None,
            builds: vec![],
            configured: vec![],
            losses: vec![],
            deleted: [false; 3],
            hold_build: false,
            held_build: None,
            hold_configuration: false,
            held_configuration: None,
            stopping: false,
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
                Err(error) if self.deleted[index] || self.stopping => {
                    eprintln!("original Node Channel closed during owned teardown: {error}");
                    self.links[index].take().unwrap().close().await.unwrap();
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
                "LFUMUpload" => {
                    let body = decode_lfum_upload(&incoming.message).unwrap();
                    assert_eq!(body.generation, generation.number);
                    if let Some(configuration) = self.configured.last() {
                        assert!(body.any_replica_found);
                        assert_eq!(body.units.0.len(), 1);
                        let info = &body.units.0[0];
                        assert_eq!(info.unit, configuration.unit);
                        assert_eq!(info.service, primary(&self.configurations[index]).service);
                        assert_eq!(info.intermediate, Epoch::default());
                        assert!(!info.local_deleted);
                        assert_eq!(info.replicas.0.len(), configuration.replicas.0.len());
                        assert_eq!(
                            info.report_from_primary,
                            configuration.replicas.0.iter().any(|r| r.node
                                == self.configurations[index].node.identity.native().unwrap()
                                && r.current_role == 4)
                        );
                        for expected in &configuration.replicas.0 {
                            let mut expected = expected.clone();
                            expected.first_lsn = -1;
                            expected.last_lsn = -1;
                            assert_eq!(
                                info.replicas
                                    .0
                                    .iter()
                                    .filter(
                                        |r| r.description == expected && r.intermediate_role == 1
                                    )
                                    .count(),
                                1
                            );
                        }
                    } else {
                        assert!(body.units.0.is_empty() && !body.any_replica_found);
                    }
                    incoming
                        .reply
                        .take()
                        .unwrap()
                        .reply(
                            link.seal(lfum_upload_reply_message(&generation).unwrap())
                                .unwrap(),
                            wire::deadline(),
                        )
                        .await
                        .unwrap();
                    self.uploaded[index] = Some(body);
                }
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
                    if body.last && body.replicas.0.is_empty() && body.dropped.0.is_empty() {
                        self.bootstrapped[index] = true;
                    } else {
                        assert!(!body.last && !body.from_fmm);
                        assert_eq!(body.replicas.0.len() + body.dropped.0.len(), 1);
                        let dropped = body.replicas.0.first().or(body.dropped.0.first()).unwrap();
                        let config = &self.configurations[index];
                        assert_eq!(dropped.unit.id, config.partition_id().unwrap());
                        assert_eq!(dropped.service, primary(config).service);
                        assert_eq!(dropped.replicas.0.len(), 1);
                        let replica = &dropped.replicas.0[0].description;
                        assert_eq!(replica.node, config.node.identity.native().unwrap());
                        assert_eq!(replica.id, config.node.replica_id);
                        assert_eq!(replica.instance, config.node.replica_instance);
                        assert!(!replica.up);
                        assert_eq!(replica.state, 3);
                        if !self.stopping && !self.deleted[index] && !self.losses.contains(&index) {
                            self.losses.push(index);
                        }
                    }
                    link.send(replica_up_reply_message(&body, &generation).unwrap())
                        .await
                        .unwrap();
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
                    if self.hold_build {
                        self.held_build = Some(body);
                    } else {
                        self.builds.push(body);
                    }
                }
                "DoReconfigurationReply" => {
                    let (body, _) = decode_do_reconfiguration_reply(&incoming.message).unwrap();
                    assert_eq!(body.error, 0);
                    if self.hold_configuration {
                        self.held_configuration = Some(body);
                    } else {
                        self.configured.push(body);
                    }
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
        let mut socket = TcpStream::connect(address)
            .await
            .unwrap_or_else(|error| panic!("application {address:?}: {error}"));
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
    journey(Fault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_fm_channel_preserves_original_writer_until_explicit_owned_shutdown() {
    journey(Fault::LostFm).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_primary_failover_uses_cluster_tested_ra_after_verified_app_host_loss() {
    journey(Fault::Failover).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_generation_handshake_and_empty_upload_resume_shared_control() {
    journey(Fault::GenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_generation_rollover_preserves_live_replicated_membership_and_data() {
    journey(Fault::ReplicatedGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_wrong_generation_refuses_new_authority() {
    journey(Fault::Generation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_resize_refuses_changed_membership() {
    journey(Fault::Resize).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_invalid_role_change_is_rejected_by_shared_ra() {
    journey(Fault::Promotion).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_foreign_instance_refuses_replacement() {
    journey(Fault::ForeignInstance).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    LostFm,
    Generation,
    Resize,
    Promotion,
    ForeignInstance,
    Failover,
    GenerationHandshake,
    ReplicatedGenerationHandshake,
}

async fn rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    fm.uploaded.fill(None);
    for index in 0..3 {
        let mut generation = fm.configurations[index].generation().unwrap();
        generation.number.generation += 1;
        let link = fm.links[index].as_ref().unwrap();
        let reply = link
            .request(generation_proposal_message(&generation).unwrap())
            .await
            .unwrap();
        let reply = decode_generation_proposal_reply(&reply).unwrap();
        assert_eq!(reply.error, 0);
        assert_eq!(reply.proposed, generation.number);
        assert_eq!(reply.current, generation.number);
        fm.configurations[index].fm.generation += 1;
        link.advance_generation(&generation);
        fm.send(
            index,
            generation_update_message(&GenerationUpdate::default(), &generation).unwrap(),
        )
        .await;
    }
    fm.until(fixture, |fm| fm.uploaded.iter().all(Option::is_some))
        .await;
    let deadline = Instant::now() + Duration::from_secs(3);
    while (0..3).any(|index| {
        fixture.status(index)["generations"]["send"] != fm.configurations[index].fm.generation
    }) {
        assert!(
            Instant::now() < deadline,
            "generation completion: {}",
            fixture.diagnostics()
        );
        fm.step().await;
    }
}

async fn journey(fault: Fault) {
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
    if fault == Fault::GenerationHandshake {
        rollover(&mut fm, &mut fixture).await;
    }
    let selected = 1;
    let body = primary(&fixture.configs[selected]);
    let mut generation = fm.configurations[selected].generation().unwrap();
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
    let pids: Vec<_> = (0..3)
        .map(|index| fixture.status(index)["application_pid"].clone())
        .collect();
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
        fm.hold_build = true;
        fm.send(selected, add_replica_message(&build, &generation).unwrap())
            .await;
        fm.until(&mut fixture, |fm| fm.held_build.is_some()).await;
        assert_eq!(
            fm.builds.len(),
            before,
            "lost reply cannot complete FM build"
        );
        let original = fm.held_build.take().unwrap();
        fm.hold_build = false;
        fm.send(selected, add_replica_message(&build, &generation).unwrap())
            .await;
        fm.until(&mut fixture, |fm| fm.builds.len() > before).await;
        assert_eq!(fm.builds.last().unwrap(), &original);
        assert!(
            original.replica.service_endpoint.is_empty(),
            "Idle is not a published writer"
        );
        assert!(!application(&endpoint, "write").await.starts_with("OK"));
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
    fm.hold_configuration = true;
    fm.send(
        selected,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
        .await;
    assert!(
        fm.configured.is_empty(),
        "FM create/publication is still pending"
    );
    let mut completed = unit.clone();
    completed.previous = Epoch::default();
    let original = fm.held_configuration.take().unwrap();
    assert_eq!(original.unit, completed);
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
    fm.hold_configuration = false;
    fm.send(
        selected,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    fm.until(&mut fixture, |fm| fm.configured.len() == 1).await;
    assert_eq!(fm.configured[0], original);
    if fault == Fault::ReplicatedGenerationHandshake {
        rollover(&mut fm, &mut fixture).await;
        generation = fm.configurations[selected].generation().unwrap();
        assert!(application(&endpoint, "read")
            .await
            .contains("value=acknowledged"));
        assert!(application(&endpoint, "write-more")
            .await
            .contains("committed=2 value=continued"));
        fm.send(
            selected,
            do_reconfiguration_message(&configuration, &generation).unwrap(),
        )
        .await;
        fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
        assert_eq!(fm.configured[1], original);
    }
    let mut deletion_bodies: Vec<_> = fixture
        .configs
        .iter()
        .map(|config| AddInstance {
            unit: completed.clone(),
            service: body.service.clone(),
            replica: configuration
                .configuration
                .replicas
                .0
                .iter()
                .find(|replica| replica.node == config.node.identity.native().unwrap())
                .unwrap()
                .clone(),
        })
        .collect();
    for (index, config) in fixture.configs.iter().enumerate() {
        let status = fixture.status(index);
        let address = config.node.address.to_string();
        if status["service_endpoint"].as_str() == Some(&address)
            || status["replication_endpoint"]
                .as_str()
                .and_then(|endpoint| endpoint.split('/').next())
                == Some(&address)
        {
            // An application listener may reuse the released ephemeral RA port.
            continue;
        }
        assert!(
            TcpStream::connect(config.node.address).await.is_err(),
            "an established mesh must not accept duplicate/replacement sessions"
        );
    }
    if fault == Fault::Failover {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let state: Value =
                serde_json::from_str(&application(&endpoint, "state").await).unwrap();
            if state["peers"]
                .as_array()
                .unwrap()
                .iter()
                .all(|peer| peer["applied"].as_i64().unwrap() >= 1)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "all original peers must apply before loss: {state}"
            );
            fm.step().await;
        }
        let lost_pid = pids[selected].as_u64().unwrap();
        assert!(Command::new("kill")
            .args(["-KILL", &lost_pid.to_string()])
            .status()
            .unwrap()
            .success());
        fm.until(&mut fixture, |fm| fm.losses.contains(&selected))
            .await;
        assert!(
            TcpStream::connect(&endpoint).await.is_err(),
            "lost Primary listener cannot survive"
        );

        let candidate = 0;
        let mut failover = configuration.clone();
        failover.configuration.unit = original.unit.clone();
        failover.configuration.unit.previous = original.unit.current;
        failover.configuration.unit.current.configuration += 0x1_0000_0001;
        failover.configuration.replicas = original.replicas.clone();
        for replica in &mut failover.configuration.replicas.0 {
            replica.previous_role = replica.current_role;
            if replica.node == fixture.configs[selected].node.identity.native().unwrap() {
                replica.current_role = 1;
                replica.up = false;
                replica.state = 3;
            } else if replica.node == fixture.configs[candidate].node.identity.native().unwrap() {
                replica.current_role = 4;
            }
            replica.first_lsn = -1;
            replica.last_lsn = -1;
        }
        fm.send(
            candidate,
            do_reconfiguration_message(&failover, &generation).unwrap(),
        )
        .await;
        fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
        let promoted = fm.configured[1].clone();
        let new_primary = promoted
            .replicas
            .0
            .iter()
            .find(|replica| replica.current_role == 4)
            .unwrap();
        assert_eq!(
            new_primary.node,
            fixture.configs[candidate].node.identity.native().unwrap()
        );
        assert!(application(&new_primary.service_endpoint, "read")
            .await
            .contains("value=acknowledged"));
        assert!(application(&new_primary.service_endpoint, "write-more")
            .await
            .starts_with("OK"));
        assert!(application(&new_primary.service_endpoint, "read")
            .await
            .contains("value=continued"));
        assert!(
            TcpStream::connect(&endpoint).await.is_err(),
            "lost Primary listener cannot survive"
        );
        fm.send(
            candidate,
            do_reconfiguration_message(&failover, &generation).unwrap(),
        )
        .await;
        fm.until(&mut fixture, |fm| fm.configured.len() == 3).await;
        assert_eq!(fm.configured[2], promoted);
        for (index, deletion) in deletion_bodies.iter_mut().enumerate() {
            if index != selected {
                deletion.unit = promoted.unit.clone();
                deletion.replica = promoted
                    .replicas
                    .0
                    .iter()
                    .find(|replica| {
                        replica.node == fixture.configs[index].node.identity.native().unwrap()
                    })
                    .unwrap()
                    .clone();
            }
        }
    }
    let refusal = !matches!(
        fault,
        Fault::None
            | Fault::Failover
            | Fault::GenerationHandshake
            | Fault::ReplicatedGenerationHandshake
    );
    if refusal {
        if fault == Fault::LostFm {
            fm.links[selected].take().unwrap().close().await.unwrap();
        } else {
            let mut invalid = configuration.clone();
            let mut header = generation.clone();
            match fault {
                Fault::Generation => header.number.generation += 1,
                Fault::Resize => invalid.configuration.unit.target = 4,
                Fault::Promotion => {
                    invalid.configuration.replicas.0[0].current_role = 3;
                    invalid.configuration.replicas.0[1].current_role = 4;
                }
                Fault::ForeignInstance => invalid.configuration.replicas.0[1].node.instance += 1,
                _ => unreachable!(),
            }
            fm.send(
                selected,
                do_reconfiguration_message(&invalid, &header).unwrap(),
            )
            .await;
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while fixture.status(selected)["control_unknown"] != true {
            assert!(
                Instant::now() < deadline,
                "control refusal observation: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        let expected = match fault {
            Fault::LostFm => "Channel",
            Fault::Generation => "generation",
            Fault::Resize => "immutable for this service",
            Fault::Promotion | Fault::ForeignInstance => "only initial configured admission",
            Fault::None
            | Fault::Failover
            | Fault::GenerationHandshake
            | Fault::ReplicatedGenerationHandshake => unreachable!(),
        };
        assert!(
            fixture.status(selected)["error"]
                .as_str()
                .unwrap()
                .contains(expected),
            "{}",
            fixture.diagnostics()
        );
        if fault == Fault::LostFm {
            assert_eq!(fixture.status(selected)["fm_connected"], false);
        }
        assert_eq!(fm.configured.len(), 1, "invalid intent is never completed");
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
        if refusal && index == selected {
            continue;
        }
        let delete = DeleteReplica::new(deletion_bodies[index].clone(), false);
        fm.send(index, delete_replica_message(&delete, &generation).unwrap())
            .await;
    }
    fm.until(&mut fixture, |fm| {
        fm.deleted
            .iter()
            .enumerate()
            .all(|(index, deleted)| *deleted || (refusal && index == selected))
    })
    .await;
    fm.stopping = true;
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
    expected[selected] = !refusal;
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
