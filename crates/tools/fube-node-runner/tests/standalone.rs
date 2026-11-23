//! Real standalone Node binaries with a native fixture FM.
use fube_local_host::process::OwnedApp;
use fube_node::control::wire::{self, Link};
use fube_node_runner::{Config, Fm, Identity, Placement};
use fube_ra::protocol::*;
use fubst::serde::ObjectArray;
use furpc::Message;
use serde_json::Value;
use std::{
    fs,
    net::{Ipv4Addr, SocketAddrV4},
    os::unix::process::{CommandExt, ExitStatusExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
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

struct GroupHelper(Child);

struct PausedProcess(u32);
impl PausedProcess {
    fn new(pid: u32) -> Self {
        assert!(Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .unwrap()
            .success());
        Self(pid)
    }
}
impl Drop for PausedProcess {
    fn drop(&mut self) {
        assert!(Command::new("kill")
            .args(["-CONT", &self.0.to_string()])
            .status()
            .unwrap()
            .success());
    }
}

impl GroupHelper {
    fn join(pid: u64) -> Self {
        Self(
            Command::new("/bin/sleep")
                .arg("30")
                .process_group(i32::try_from(pid).unwrap())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
}
impl Drop for GroupHelper {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
        }
        self.0.wait().unwrap();
    }
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
        Self::with_sizes(fm, 3, 2)
    }

    fn with_sizes(fm: SocketAddrV4, target: i32, minimum: i32) -> Self {
        Self::with_node_pool(fm, target, minimum, target)
    }

    fn with_node_pool(fm: SocketAddrV4, target: i32, minimum: i32, nodes: i32) -> Self {
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
        let placements: Vec<_> = (1..=u8::try_from(nodes).unwrap())
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
            partition: partition.clone(), target, minimum, package: package.clone(), application: "fabric:/External".into(),
            package_version_instance: 1,
            parameters: vec![
                ("Arguments".into(), format!("--stateful-lifecycle --data-replicator --replication-listen {}:0 --service-listen {}:0",
                    placement.address.ip(), placement.address.ip())),
                ("TargetReplicaSetSize".into(), target.to_string()),
                ("MinReplicaSetSize".into(), minimum.to_string()),
            ],
            run_directory: root.join(format!("node{}", index + 1)),
            startup_timeout_ms: 10_000, shutdown_timeout_ms: 20_000,
            remote_replica_progress_query_wait_ms: fube_ra::entity::Settings::default().remote_replica_progress_query_wait_ms,
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

    fn work(&self, index: usize) -> PathBuf {
        let roots: Vec<_> = fs::read_dir(self.configs[index].run_directory.join("hosting"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(roots.len(), 1);
        roots[0].join("deployment/HostingLocalStatefulApp_App1/work")
    }

    fn diagnostics(&self) -> String {
        format!(
            "root={:?}, status={:?}, logs={:?}",
            self.root,
            (0..self.configs.len())
                .map(|index| self.status(index))
                .collect::<Vec<_>>(),
            self.children.iter().map(OwnedApp::logs).collect::<Vec<_>>()
        )
    }

    async fn stop_node(&mut self, fm: &mut FmFixture, index: usize) {
        assert!(Command::new("kill")
            .args(["-TERM", &self.children[index].pid().to_string()])
            .status()
            .unwrap()
            .success());
        let deadline = Instant::now() + Duration::from_secs(25);
        while self.children[index].observed_exit().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "Node shutdown: {}",
                self.diagnostics()
            );
            fm.step().await;
        }
        assert_eq!(self.status(index)["stage"], "stopped");
    }

    async fn await_control_loss(&mut self, fm: &mut FmFixture, index: usize) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !self.status(index)["control_error"].is_string() {
            assert!(
                Instant::now() < deadline,
                "peer loss observation: {}",
                self.diagnostics()
            );
            self.children[index].check_alive().unwrap();
            fm.step().await;
        }
        assert_eq!(self.status(index)["control_unknown"], true);
    }

    fn finish(mut self, expected_clean: impl AsRef<[bool]>) {
        let expected_clean = expected_clean.as_ref();
        assert_eq!(expected_clean.len(), self.children.len());
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
                Some(if expected_clean[index] { 0 } else { 1 }),
                "node {index}; root={:?}; logs={:?}",
                self.root,
                child.logs()
            );
            let receipt = child.cleanup().unwrap();
            assert_eq!(receipt["forced"], false);
        }
        for (index, expected) in expected_clean.iter().copied().enumerate() {
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
    bootstrapped: Vec<bool>,
    uploaded: Vec<Option<LfumUpload>>,
    primary: Option<AddPrimaryReply>,
    builds: Vec<AddReplicaReply>,
    configured: Vec<ConfigurationReply>,
    losses: Vec<usize>,
    deleted: Vec<bool>,
    hold_build: bool,
    held_build: Option<AddReplicaReply>,
    hold_configuration: bool,
    held_configuration: Option<ConfigurationReply>,
    deactivation_requests: Vec<Option<i64>>,
    deactivation_replies: Vec<Option<i64>>,
    initial_membership: Option<Configuration>,
    initial_builds: Option<Vec<AddReplica>>,
    failover: Option<Configuration>,
    failover_previous: Option<ConfigurationReply>,
    failover_ending: bool,
    transfer: Option<(ConfigurationReply, DoReconfiguration)>,
    removal: Option<RemovalLfum>,
    older_uploads: Vec<(usize, wire::Incoming)>,
    repair_previous: Option<ConfigurationReply>,
    repair_build: Option<AddReplica>,
    repair_membership: Option<Configuration>,
    repair_cancelled: bool,
    repair_deleting: bool,
    removals: Vec<RemoveReplicaReply>,
    hold_removal: bool,
    held_removal: Option<RemoveReplicaReply>,
    hold_deletion: bool,
    held_deletion: Option<DeleteReplicaReply>,
    retired_replicas: Vec<(usize, i64, i64)>,
    hold_uploads: bool,
    held_uploads: Vec<(usize, wire::Incoming)>,
    stopping: bool,
    expected_loss: Option<usize>,
}
struct RemovalLfum {
    previous: ConfigurationReply,
    intent: Configuration,
    supersession: Option<Configuration>,
}

fn completed_removal_view(mut configuration: Configuration) -> Configuration {
    configuration.unit.previous = Epoch::default();
    configuration.replicas.0.retain(|r| r.current_role >= 3);
    for r in &mut configuration.replicas.0 {
        r.previous_role = 1;
    }
    configuration
}

fn assert_removal_lfum(body: &LfumUpload, node: &NodeInstance, lost: bool, removal: &RemovalLfum) {
    assert!(body.any_replica_found && body.units.0.len() <= 1);
    let original = Configuration {
        unit: removal.previous.unit.clone(),
        service: removal.intent.service.clone(),
        replicas: removal.previous.replicas.clone(),
    };
    let stable = completed_removal_view(removal.intent.clone());
    if lost {
        let Some(info) = body.units.0.first() else {
            return;
        };
        let replica = original
            .replicas
            .0
            .iter()
            .find(|r| &r.node == node)
            .unwrap();
        if info.replicas.0
            == vec![ReplicaInfo {
                description: Replica {
                    node: node.clone(),
                    id: replica.id,
                    instance: replica.instance,
                    up: false,
                    state: 3,
                    ..Replica::default()
                },
                intermediate_role: 1,
            }]
        {
            assert!([
                original.unit.clone(),
                removal.intent.unit.clone(),
                stable.unit.clone()
            ]
            .contains(&info.unit));
            assert_eq!(info.service, original.service);
            assert!(!info.report_from_primary && !info.local_deleted);
            assert!(
                info.intermediate == Epoch::default()
                    || info.intermediate == removal.intent.unit.current
            );
            return;
        }
        assert_eq!(
            replica.current_role, 4,
            "deleted removal destination cannot report a live membership"
        );
    }
    let info = &body.units.0[0];
    let mut views = vec![
        (original.clone(), None),
        (removal.intent.clone(), None),
        (removal.intent.clone(), Some(&removal.intent)),
        (stable.clone(), None),
    ];
    let primary = removal
        .supersession
        .as_ref()
        .and_then(|f| f.replicas.0.iter().find(|r| r.current_role == 4));
    if let Some(failover) = removal.supersession.as_ref().filter(|_| !lost) {
        views.extend([
            (failover.clone(), None),
            (failover.clone(), Some(failover)),
            (completed_removal_view(failover.clone()), None),
        ]);
        if !lost && primary.is_some_and(|p| &p.node != node) {
            for (mut query, intermediate) in [
                (original, None),
                (removal.intent.clone(), None),
                (removal.intent.clone(), Some(&removal.intent)),
                (stable, None),
            ] {
                query.unit = failover.unit.clone();
                for r in &mut query.replicas.0 {
                    r.previous_role = removal
                        .intent
                        .replicas
                        .0
                        .iter()
                        .find(|old| old.node == r.node)
                        .unwrap()
                        .previous_role;
                }
                views.push((query, intermediate));
            }
        }
    }
    assert!(
        views.into_iter().any(|(mut view, intermediate)| {
            if info.unit != view.unit
                || info.service != view.service
                || info.local_deleted
                || info.report_from_primary
                    != view
                        .replicas
                        .0
                        .iter()
                        .any(|r| &r.node == node && r.current_role == 4)
                || info.intermediate != intermediate.map_or(Epoch::default(), |c| c.unit.current)
            {
                return false;
            }
            for r in &mut view.replicas.0 {
                r.first_lsn = -1;
                r.last_lsn = -1;
                if primary.is_some_and(|p| p.node == r.node) && r.current_role == 4 {
                    let Some(actual) = info
                        .replicas
                        .0
                        .iter()
                        .find(|a| a.description.node == r.node)
                    else {
                        return false;
                    };
                    r.service_endpoint = actual.description.service_endpoint.clone();
                }
            }
            let expected: Vec<_> = view
                .replicas
                .0
                .into_iter()
                .map(|description| {
                    let role = intermediate
                        .and_then(|c| c.replicas.0.iter().find(|r| r.node == description.node))
                        .map_or(1, |r| r.current_role);
                    ReplicaInfo {
                        description,
                        intermediate_role: role,
                    }
                })
                .collect();
            info.replicas.0 == expected
        }),
        "unexpected removal inventory: {info:?}"
    );
}

impl FmFixture {
    async fn accept(listener: TcpListener, fixture: &mut Fixture) -> Self {
        let count = fixture.configs.len();
        let mut links = vec![None; count];
        let deadline = Instant::now() + Duration::from_secs(10);
        for _ in 0..count {
            let accepted = tokio::time::timeout_at(deadline, listener.accept())
                .await
                .unwrap_or_else(|_| panic!("FM accept: {}", fixture.diagnostics()))
                .unwrap();
            let index = fixture
                .configs
                .iter()
                .position(|config| accepted.1.ip() == *config.node.address.ip())
                .unwrap();
            let channel = wire::accept(accepted.0, wire::Role::Fm, deadline)
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
            bootstrapped: vec![false; count],
            uploaded: vec![None; count],
            primary: None,
            builds: vec![],
            configured: vec![],
            losses: vec![],
            deleted: vec![false; count],
            hold_build: false,
            held_build: None,
            hold_configuration: false,
            held_configuration: None,
            deactivation_requests: vec![None; count],
            deactivation_replies: vec![None; count],
            initial_membership: None,
            initial_builds: None,
            failover: None,
            failover_previous: None,
            failover_ending: false,
            transfer: None,
            removal: None,
            older_uploads: vec![],
            repair_previous: None,
            repair_build: None,
            repair_membership: None,
            repair_cancelled: false,
            repair_deleting: false,
            removals: vec![],
            hold_removal: false,
            held_removal: None,
            hold_deletion: false,
            held_deletion: None,
            retired_replicas: vec![],
            hold_uploads: false,
            held_uploads: vec![],
            stopping: false,
            expected_loss: None,
        }
    }

    async fn step(&mut self) {
        for index in 0..self.configurations.len() {
            let Some(link) = &self.links[index] else {
                continue;
            };
            let incoming = match link
                .receive(Instant::now() + Duration::from_millis(2))
                .await
            {
                Ok(incoming) => incoming,
                Err(error)
                    if self.deleted[index]
                        || self.stopping
                        || self.expected_loss == Some(index) =>
                {
                    eprintln!("original Node Channel closed during owned teardown: {error}");
                    self.links[index].take().unwrap().close().await.unwrap();
                    continue;
                }
                Err(error) => panic!("original Node Channel failed: {error}"),
            };
            let Some(mut incoming) = incoming else {
                continue;
            };
            let action = wire::action(&incoming.message).unwrap();
            if action != "NodeUp" {
                wire::predecode(&incoming.message).unwrap();
            }
            let generation = self.configurations[index].generation().unwrap();
            match action.as_str() {
                "NodeDeactivateReply" => {
                    let (reply, owner) = decode_node_deactivate_reply(&incoming.message).unwrap();
                    assert_eq!(owner, Owner::Fm);
                    assert_eq!(Some(reply.sequence), self.deactivation_requests[index]);
                    self.deactivation_replies[index] = Some(reply.sequence);
                }
                "LFUMUpload" => {
                    let body = decode_lfum_upload(&incoming.message).unwrap();
                    if self.removal.is_some() && body.generation.older_than(&generation.number) {
                        self.older_uploads.push((index, incoming));
                        continue;
                    }
                    assert_eq!(body.generation, generation.number);
                    let node = self.configurations[index].node.identity.native().unwrap();
                    let deleted_repair = self.repair_deleting
                        && self
                            .repair_build
                            .as_ref()
                            .is_some_and(|b| b.replica.node == node)
                        && body.units.0.is_empty();
                    let closed = self.failover.as_ref().is_some_and(|intent| {
                        intent
                            .replicas
                            .0
                            .iter()
                            .any(|r| r.node == node && r.previous_role == 4)
                    });
                    let expected = self
                        .repair_build
                        .as_ref()
                        .filter(|_| !deleted_repair)
                        .map(|build| {
                            let previous = self.repair_previous.as_ref().unwrap();
                            if let Some(request) = &self.repair_membership {
                                let mut complete = request.unit.clone();
                                complete.previous = Epoch::default();
                                if body.units.0[0].unit == request.unit {
                                    return request.clone();
                                }
                                if body.units.0[0].unit == complete {
                                    let mut configuration = request.clone();
                                    configuration.unit = complete;
                                    for r in &mut configuration.replicas.0 {
                                        r.previous_role = 1;
                                    }
                                    return configuration;
                                }
                            }
                            let primary = previous
                                .replicas
                                .0
                                .iter()
                                .any(|r| r.node == node && r.current_role == 4);
                            let fresh = build.replica.node == node;
                            let mut replicas = if fresh {
                                vec![]
                            } else {
                                previous.replicas.0.clone()
                            };
                            if (primary || fresh)
                                && !(primary
                                    && self.repair_cancelled
                                    && !body.units.0[0]
                                        .replicas
                                        .0
                                        .iter()
                                        .any(|r| r.description.node == build.replica.node))
                            {
                                replicas.push(build.replica.clone());
                            }
                            Configuration {
                                unit: previous.unit.clone(),
                                service: build.service.clone(),
                                replicas: ObjectArray(replicas),
                            }
                        })
                        .or_else(|| {
                            self.failover.as_ref().map(|intent| {
                                if intent
                                    .replicas
                                    .0
                                    .iter()
                                    .any(|r| r.node == node && r.current_role == 4)
                                {
                                    intent.clone()
                                } else if self.failover_ending {
                                    let mut body = intent.clone();
                                    body.unit.previous = Epoch::default();
                                    body.replicas.0.retain(|r| r.current_role >= 3);
                                    for r in &mut body.replicas.0 {
                                        r.previous_role = 1;
                                    }
                                    body
                                } else {
                                    let mut replicas =
                                        self.failover_previous.as_ref().unwrap().replicas.clone();
                                    for r in &mut replicas.0 {
                                        r.previous_role = r.current_role;
                                    }
                                    Configuration {
                                        unit: intent.unit.clone(),
                                        service: intent.service.clone(),
                                        replicas,
                                    }
                                }
                            })
                        })
                        .or_else(|| {
                            self.initial_builds.as_ref().map(|builds| {
                                let primary = &self.primary.as_ref().unwrap().replica;
                                let node =
                                    self.configurations[index].node.identity.native().unwrap();
                                let mut replicas = vec![];
                                if primary.node == node {
                                    replicas.push(primary.clone());
                                }
                                replicas.extend(
                                    builds
                                        .iter()
                                        .filter(|build| {
                                            primary.node == node || build.replica.node == node
                                        })
                                        .map(|build| build.replica.clone()),
                                );
                                Configuration {
                                    unit: builds[0].unit.clone(),
                                    service: builds[0].service.clone(),
                                    replicas: ObjectArray(replicas),
                                }
                            })
                        })
                        .or_else(|| self.initial_membership.clone())
                        .or_else(|| {
                            self.configured
                                .last()
                                .filter(|configuration| {
                                    configuration.replicas.0.iter().any(|r| r.node == node)
                                })
                                .map(|configuration| Configuration {
                                    unit: configuration.unit.clone(),
                                    service: primary(&self.configurations[index]).service,
                                    replicas: configuration.replicas.clone(),
                                })
                        });
                    if let Some(removal) = &self.removal {
                        assert_removal_lfum(&body, &node, self.losses.contains(&index), removal);
                    } else if deleted_repair {
                        assert!(body.units.0.is_empty());
                    } else if let Some((previous, request)) = &self.transfer {
                        assert!(body.any_replica_found);
                        assert_eq!(body.units.0.len(), 1);
                        let info = &body.units.0[0];
                        let intent = &request.configuration;
                        let mut complete = intent.unit.clone();
                        complete.previous = Epoch::default();
                        assert!(
                            info.unit == previous.unit
                                || info.unit == intent.unit
                                || info.unit == complete
                        );
                        assert_eq!(info.service, intent.service);
                        assert!(!info.local_deleted);
                        assert_eq!(info.replicas.0.len(), intent.replicas.0.len());
                        for expected in &intent.replicas.0 {
                            let replicas: Vec<_> = info
                                .replicas
                                .0
                                .iter()
                                .filter(|r| r.description.node == expected.node)
                                .collect();
                            assert_eq!(replicas.len(), 1);
                            let actual = &replicas[0].description;
                            assert_eq!(
                                (
                                    actual.id,
                                    actual.instance,
                                    &actual.version,
                                    &actual.replication_endpoint
                                ),
                                (
                                    expected.id,
                                    expected.instance,
                                    &expected.version,
                                    &expected.replication_endpoint
                                )
                            );
                            assert!(actual.up && matches!(actual.state, 1 | 2));
                            assert_eq!((actual.first_lsn, actual.last_lsn), (-1, -1));
                            if info.unit == complete {
                                assert_eq!(
                                    (actual.previous_role, actual.current_role),
                                    (1, expected.current_role)
                                );
                            } else if info.unit == previous.unit {
                                let old = previous
                                    .replicas
                                    .0
                                    .iter()
                                    .find(|r| r.node == expected.node)
                                    .unwrap();
                                assert_eq!(
                                    (actual.previous_role, actual.current_role),
                                    (old.previous_role, old.current_role)
                                );
                            } else {
                                assert_eq!(actual.previous_role, expected.previous_role);
                                assert!(
                                    actual.current_role == expected.current_role
                                        || actual.current_role == expected.previous_role
                                );
                            }
                            assert_eq!(
                                replicas[0].intermediate_role,
                                if info.intermediate == Epoch::default() {
                                    1
                                } else {
                                    actual.current_role
                                }
                            );
                        }
                        if info.intermediate != Epoch::default() {
                            assert_eq!(info.intermediate, intent.unit.current);
                            assert!(intent
                                .replicas
                                .0
                                .iter()
                                .any(|r| r.node == node && r.current_role == 4));
                        }
                    } else if closed {
                        assert!(body.any_replica_found);
                        assert!(body.units.0.len() <= 1);
                        if let Some(info) = body.units.0.first() {
                            let previous = self.failover_previous.as_ref().unwrap();
                            let original =
                                previous.replicas.0.iter().find(|r| r.node == node).unwrap();
                            assert_eq!(info.unit, previous.unit);
                            assert_eq!(info.service, primary(&self.configurations[index]).service);
                            assert!(!info.report_from_primary && !info.local_deleted);
                            assert_eq!(info.intermediate, Epoch::default());
                            assert_eq!(
                                info.replicas.0,
                                vec![ReplicaInfo {
                                    description: Replica {
                                        node,
                                        id: original.id,
                                        instance: original.instance,
                                        up: false,
                                        state: 3,
                                        ..Replica::default()
                                    },
                                    intermediate_role: 1,
                                }]
                            );
                        }
                    } else if let Some(configuration) = expected {
                        assert!(body.any_replica_found);
                        assert_eq!(body.units.0.len(), 1);
                        let info = &body.units.0[0];
                        assert_eq!(info.unit, configuration.unit);
                        assert_eq!(info.service, primary(&self.configurations[index]).service);
                        let primary = configuration.replicas.0.iter().any(|r| {
                            r.node == self.configurations[index].node.identity.native().unwrap()
                                && r.current_role == 4
                        });
                        assert_eq!(
                            info.intermediate,
                            if (self.initial_membership.is_some()
                                || self.failover_ending
                                || self
                                    .repair_membership
                                    .as_ref()
                                    .is_some_and(|request| info.unit == request.unit))
                                && primary
                            {
                                configuration.unit.current
                            } else {
                                Epoch::default()
                            }
                        );
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
                            if expected.current_role == 4
                                && self.failover.as_ref().is_some_and(|intent| {
                                    intent
                                        .replicas
                                        .0
                                        .iter()
                                        .any(|r| r.node == expected.node && r.current_role == 4)
                                })
                            {
                                expected.service_endpoint = info
                                    .replicas
                                    .0
                                    .iter()
                                    .find(|r| r.description.node == expected.node)
                                    .unwrap()
                                    .description
                                    .service_endpoint
                                    .clone();
                            }
                            if self.initial_builds.is_some() && expected.current_role == 2 {
                                let actual = &info
                                    .replicas
                                    .0
                                    .iter()
                                    .find(|r| r.description.node == expected.node)
                                    .unwrap()
                                    .description;
                                assert!(!actual.replication_endpoint.is_empty());
                                expected.replication_endpoint = actual.replication_endpoint.clone();
                            }
                            if self.repair_build.as_ref().is_some_and(|build| {
                                build.replica.node == expected.node && expected.current_role == 2
                            }) {
                                let actual = &info
                                    .replicas
                                    .0
                                    .iter()
                                    .find(|r| r.description.node == expected.node)
                                    .unwrap()
                                    .description;
                                assert!(
                                    matches!(actual.state, 1 | 2)
                                        || self.repair_cancelled && actual.state == 3
                                );
                                if expected.node == node {
                                    assert!(
                                        actual.state == 1
                                            || self.repair_deleting && actual.state == 3
                                    );
                                }
                                if self.repair_deleting
                                    && expected.node == node
                                    && actual.state == 3
                                {
                                    expected = Replica {
                                        node: expected.node,
                                        id: expected.id,
                                        instance: expected.instance,
                                        up: false,
                                        state: 3,
                                        ..Replica::default()
                                    };
                                } else {
                                    assert!(!actual.replication_endpoint.is_empty());
                                    expected.state = actual.state;
                                    expected.replication_endpoint =
                                        actual.replication_endpoint.clone();
                                }
                            }
                            let intermediate_role = if info.intermediate == Epoch::default() {
                                1
                            } else {
                                expected.current_role
                            };
                            assert_eq!(
                                info.replicas
                                    .0
                                    .iter()
                                    .filter(|r| r.description == expected
                                        && r.intermediate_role == intermediate_role)
                                    .count(),
                                1
                            );
                        }
                    } else {
                        assert!(body.units.0.is_empty() && !body.any_replica_found);
                    }
                    if self.hold_uploads {
                        self.held_uploads.push((index, incoming));
                        self.uploaded[index] = Some(body);
                        continue;
                    }
                    incoming
                        .reply
                        .take()
                        .unwrap()
                        .lfum_upload(link, |decoded| {
                            assert_eq!(decoded, body);
                            Ok(generation)
                        })
                        .await
                        .unwrap();
                    self.uploaded[index] = Some(body);
                }
                "NodeUp" => {
                    incoming
                        .reply
                        .take()
                        .unwrap()
                        .node_up(link, |body| {
                            body.validate_fresh_stateless().unwrap();
                            let ack = NodeUpAck::fresh_activated(
                                FabricVersionInstance::native_default(),
                                0,
                            )
                            .unwrap();
                            Ok((ack, generation))
                        })
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
                        let retired =
                            self.retired_replicas
                                .contains(&(index, replica.id, replica.instance));
                        if !retired {
                            assert_eq!(replica.id, config.node.replica_id);
                            assert_eq!(replica.instance, config.node.replica_instance);
                        }
                        assert!(!replica.up);
                        assert_eq!(replica.state, 3);
                        if !retired
                            && !self.stopping
                            && !self.deleted[index]
                            && !self.losses.contains(&index)
                        {
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
                    let enabled = wire::action(&incoming.message).unwrap() == "ServiceTypeEnabled";
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
                    if self.repair_cancelled {
                        let build = self.repair_build.as_ref().unwrap();
                        assert_eq!(body.unit, build.unit);
                        assert_eq!(
                            (body.replica.node, body.replica.id, body.replica.instance),
                            (
                                build.replica.node.clone(),
                                build.replica.id,
                                build.replica.instance
                            )
                        );
                        continue;
                    }
                    assert_eq!(body.error, 0);
                    if self.hold_build {
                        self.held_build = Some(body);
                    } else {
                        self.builds.push(body);
                    }
                }
                "RemoveReplicaReply" => {
                    let (body, owner) = decode_remove_replica_reply(&incoming.message).unwrap();
                    assert_eq!(owner, Owner::Fm);
                    assert!(self.repair_cancelled);
                    let build = self.repair_build.as_ref().unwrap();
                    assert_eq!(body.unit, build.unit);
                    let mut expected = build.replica.clone();
                    expected.state = 3;
                    expected.replication_endpoint = body.replica.replication_endpoint.clone();
                    assert!(!expected.replication_endpoint.is_empty());
                    assert_eq!(body.replica, expected);
                    assert_eq!(body.error, 0);
                    if self.hold_removal {
                        self.held_removal = Some(body);
                    } else {
                        self.removals.push(body);
                    }
                }
                "DoReconfigurationReply" => {
                    let (body, _) = decode_do_reconfiguration_reply(&incoming.message).unwrap();
                    self.configuration_reply(body);
                }
                "ReplicaEndpointUpdated" => {
                    let (body, owner) = decode_replica_endpoint_updated(&incoming.message).unwrap();
                    assert_eq!(owner, Owner::Fm);
                    link.send(
                        replica_endpoint_updated_reply_message(
                            &AddInstanceReply {
                                unit: body.unit,
                                replica: body.replica,
                                error: 0,
                            },
                            &generation,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                }
                "DeleteReplicaReply" => {
                    let (body, _) = decode_delete_replica_reply(&incoming.message).unwrap();
                    assert_eq!(body.error, 0);
                    if self.repair_deleting {
                        let build = self.repair_build.as_ref().unwrap();
                        assert_eq!(body.unit, build.unit);
                        assert_eq!(
                            body.replica,
                            Replica {
                                node: build.replica.node.clone(),
                                id: build.replica.id,
                                instance: build.replica.instance,
                                up: false,
                                state: 3,
                                ..Replica::default()
                            }
                        );
                    }
                    if self.hold_deletion {
                        self.held_deletion = Some(body);
                    } else {
                        self.deleted[index] = true;
                    }
                }
                action => panic!("unexpected fixture FM action: {action}"),
            }
        }
    }

    fn configuration_reply(&mut self, body: ConfigurationReply) {
        assert_eq!(body.error, 0);
        if self.hold_configuration
            || self
                .removal
                .as_ref()
                .is_some_and(|r| body.unit.current == r.intent.unit.current)
        {
            self.held_configuration = Some(body);
        } else {
            self.configured.push(body);
        }
    }

    async fn until(&mut self, fixture: &mut Fixture, condition: impl Fn(&Self) -> bool) {
        self.until_with_budget(fixture, Duration::from_secs(8), condition)
            .await;
    }

    async fn until_with_budget(
        &mut self,
        fixture: &mut Fixture,
        budget: Duration,
        condition: impl Fn(&Self) -> bool,
    ) {
        let deadline = Instant::now() + budget;
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
            // Keep the large FM dispatch future off nested scenario stacks.
            Box::pin(self.step()).await;
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
    body.unit.target = config.target;
    body.unit.minimum = config.minimum;
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
    body.service.target = config.target;
    body.service.minimum = config.minimum;
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

fn idle_request(primary: &AddPrimary, config: &Config) -> AddReplica {
    let mut body = primary.clone();
    body.replica = Replica {
        node: config.node.identity.native().unwrap(),
        id: config.node.replica_id,
        instance: config.node.replica_instance,
        current_role: 2,
        state: 1,
        version: primary.service.version.clone(),
        ..Replica::default()
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
async fn standalone_configured_sizes_preserve_membership_failover_lfum_and_deletion() {
    for (target, minimum) in [(1, 1), (2, 1), (3, 3), (4, 2), (4, 3), (4, 4), (5, 3)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let mut fixture = Fixture::with_sizes(address, target, minimum);
        fixture.start();
        let mut fm = FmFixture::accept(listener, &mut fixture).await;
        fm.until(&mut fixture, |fm| {
            fm.bootstrapped.iter().all(|ready| *ready)
        })
        .await;
        rollover(&mut fm, &mut fixture).await;
        let selected = target as usize / 2;
        let body = primary(&fixture.configs[selected]);
        let mut generation = fm.configurations[selected].generation().unwrap();
        fm.send(selected, add_primary_message(&body, &generation).unwrap())
            .await;
        fm.until(&mut fixture, |fm| fm.primary.is_some()).await;
        let original = fm.primary.clone().unwrap();
        let endpoint = original.replica.service_endpoint.clone();
        if minimum > 1 {
            assert!(!application(&endpoint, "write").await.starts_with("OK"));
        }
        for destination in (0..target as usize).filter(|index| *index != selected) {
            let build = idle_request(&body, &fixture.configs[destination]);
            let before = fm.builds.len();
            fm.hold_build = true;
            fm.send(selected, add_replica_message(&build, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.held_build.is_some()).await;
            let held = fm.held_build.take().unwrap();
            fm.hold_build = false;
            fm.send(selected, add_replica_message(&build, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.builds.len() == before + 1)
                .await;
            assert_eq!(fm.builds.last(), Some(&held));
        }
        let mut replicas = vec![original.replica.clone()];
        replicas[0].previous_role = 4;
        for built in &fm.builds {
            let mut replica = built.replica.clone();
            replica.previous_role = 2;
            replica.current_role = 3;
            replicas.push(replica);
        }
        let mut unit = body.unit.clone();
        let configuration = (target > 1).then(|| {
            unit.previous = unit.current;
            unit.current.configuration += 1;
            DoReconfiguration {
                configuration: Configuration {
                    unit: unit.clone(),
                    service: body.service.clone(),
                    replicas: ObjectArray(replicas),
                },
                phase0_duration: i64::MAX,
            }
        });
        if let Some(request) = &configuration {
            fm.hold_configuration = true;
            fm.send(
                selected,
                do_reconfiguration_message(request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
                .await;
            let held = fm.held_configuration.take().unwrap();
            fm.hold_configuration = false;
            fm.send(
                selected,
                do_reconfiguration_message(request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == 1).await;
            assert_eq!(fm.configured[0], held);
        } else {
            // Singleton AddPrimary itself completes the current membership.
            fm.configured.push(ConfigurationReply {
                unit: original.unit.clone(),
                replicas: ObjectArray(vec![original.replica.clone()]),
                error: 0,
            });
        }
        let deadline = Instant::now() + Duration::from_secs(8);
        while !application(&endpoint, "read").await.starts_with("OK") {
            assert!(
                Instant::now() < deadline,
                "public read readiness: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        let written = application(&endpoint, "write").await;
        assert!(
            written.contains("committed=1 value=acknowledged"),
            "{target}/{minimum}: {written}"
        );
        let owners: Vec<_> = (0..target as usize)
            .map(|index| fixture.status(index))
            .collect();
        for owner in &owners {
            assert_eq!(
                owner["replica_set"],
                serde_json::json!({
                    "target": target, "minimum": minimum, "immutable": true,
                })
            );
            assert_eq!(owner["peers_connected"], target - 1);
        }
        rollover(&mut fm, &mut fixture).await;
        generation = fm.configurations[selected].generation().unwrap();
        if let Some(request) = &configuration {
            fm.send(
                selected,
                do_reconfiguration_message(request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
            assert_eq!(fm.configured[0], fm.configured[1]);
        } else {
            fm.primary = None;
            fm.send(selected, add_primary_message(&body, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.primary.is_some()).await;
            assert_eq!(fm.primary.as_ref(), Some(&original));
        }
        let mut membership = fm.configured.last().unwrap().clone();
        let mut primary_index = selected;
        if target > 1 {
            let pid = owners[selected]["application_pid"].as_u64().unwrap();
            assert!(Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            fm.until(&mut fixture, |fm| fm.losses.contains(&selected))
                .await;
            let delete = DeleteReplica::new(
                AddInstance {
                    unit: membership.unit.clone(),
                    service: body.service.clone(),
                    replica: membership
                        .replicas
                        .0
                        .iter()
                        .find(|r| r.node == body.replica.node)
                        .unwrap()
                        .clone(),
                },
                false,
            );
            fm.send(
                selected,
                delete_replica_message(&delete, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.deleted[selected]).await;
            primary_index = (0..target as usize)
                .find(|index| *index != selected)
                .unwrap();
            let candidate = fixture.configs[primary_index]
                .node
                .identity
                .native()
                .unwrap();
            let mut failover = Configuration {
                unit: membership.unit.clone(),
                service: body.service.clone(),
                replicas: membership.replicas.clone(),
            };
            failover.unit.previous = failover.unit.current;
            failover.unit.current.configuration += 0x1_0000_0001;
            for replica in &mut failover.replicas.0 {
                replica.previous_role = replica.current_role;
                if replica.current_role == 4 {
                    replica.current_role = 3;
                    replica.up = false;
                    replica.state = 3;
                } else if replica.node == candidate {
                    replica.current_role = 4;
                }
                replica.first_lsn = -1;
                replica.last_lsn = -1;
            }
            let mut count = failover.replicas.0.len();
            for replica in &mut failover.replicas.0 {
                if !replica.up && count > minimum as usize {
                    replica.current_role = 1;
                    count -= 1;
                }
            }
            let request = DoReconfiguration {
                configuration: failover,
                phase0_duration: i64::MAX,
            };
            let before = fm.configured.len();
            fm.send(
                primary_index,
                do_reconfiguration_message(&request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == before + 1)
                .await;
            membership = fm.configured.last().unwrap().clone();
            assert!(membership.replicas.0.len() >= minimum as usize);
            fm.send(
                primary_index,
                do_reconfiguration_message(&request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == before + 2)
                .await;
            assert_eq!(fm.configured.last(), Some(&membership));
            assert!(TcpStream::connect(&endpoint).await.is_err());
        }
        let endpoint = &membership
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == 4)
            .unwrap()
            .service_endpoint;
        assert!(application(endpoint, "read")
            .await
            .contains("committed=1 value=acknowledged"));
        assert!(application(endpoint, "write-more")
            .await
            .contains("committed=2 value=continued"));
        for (index, owner) in owners.iter().enumerate() {
            assert_eq!(fixture.status(index)["host_replacement_count"], 0);
            if target == 1 || index != selected {
                assert_eq!(
                    fixture.status(index)["application_pid"],
                    owner["application_pid"]
                );
                assert_eq!(fixture.status(index)["runtime_root"], owner["runtime_root"]);
            }
        }
        fm.stopping = true;
        let mut deletions = vec![];
        for index in (0..target as usize)
            .filter(|index| *index != primary_index)
            .chain(std::iter::once(primary_index))
        {
            if fm.deleted[index] {
                continue;
            }
            if index == primary_index && membership.replicas.0.len() > 1 {
                if let Ok(reply) = tokio::time::timeout(
                    Duration::from_millis(250),
                    application(endpoint, "write-final"),
                )
                .await
                {
                    assert!(
                        !reply.starts_with("OK"),
                        "closed Secondaries supply no votes"
                    );
                }
            }
            let node = fixture.configs[index].node.identity.native().unwrap();
            let delete = DeleteReplica::new(
                AddInstance {
                    unit: membership.unit.clone(),
                    service: body.service.clone(),
                    replica: membership
                        .replicas
                        .0
                        .iter()
                        .find(|r| r.node == node)
                        .unwrap()
                        .clone(),
                },
                false,
            );
            fm.send(index, delete_replica_message(&delete, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.deleted[index]).await;
            until_node(&mut fm, &fixture, index, |status| {
                status["replica_phase"] == "Closed"
            })
            .await;
            deletions.push((index, delete));
        }
        for (index, delete) in deletions {
            fm.deleted[index] = false;
            fm.send(index, delete_replica_message(&delete, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.deleted[index]).await;
        }
        for index in 0..target as usize {
            fixture.stop_node(&mut fm, index).await;
        }
        for link in fm.links.iter().flatten() {
            link.close().await.unwrap();
        }
        fixture.finish(vec![true; target as usize]);
    }
}

async fn planned_handoff(
    fm: &mut FmFixture,
    fixture: &mut Fixture,
    current: &Configuration,
    candidate: usize,
    generation: &GenerationHeader,
    stale: &[DoReconfiguration],
) -> (DoReconfiguration, ConfigurationReply) {
    let old = current
        .replicas
        .0
        .iter()
        .find(|r| r.current_role == 4)
        .unwrap();
    let source = fixture
        .configs
        .iter()
        .position(|c| c.node.identity.native().unwrap() == old.node)
        .unwrap();
    let candidate_node = fixture.configs[candidate].node.identity.native().unwrap();
    let mut intent = current.clone();
    intent.unit.previous = intent.unit.current;
    intent.unit.current.configuration += 0x1_0000_0001;
    for replica in &mut intent.replicas.0 {
        replica.previous_role = replica.current_role;
        replica.current_role = if replica.node == candidate_node { 4 } else { 3 };
    }
    let request = DoReconfiguration {
        configuration: intent,
        phase0_duration: i64::MAX,
    };
    let count = fm.configured.len();
    fm.hold_configuration = true;
    fm.send(
        source,
        do_reconfiguration_message(&request, generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.held_configuration.is_some())
        .await;
    let held = fm.held_configuration.take().unwrap();
    assert_eq!(fm.configured.len(), count);
    assert!(application(&old.service_endpoint, "write")
        .await
        .contains("NotPrimary"));
    for request in stale {
        fm.send(
            source,
            do_reconfiguration_message(request, generation).unwrap(),
        )
        .await;
    }
    fm.hold_configuration = false;
    fm.send(
        source,
        do_reconfiguration_message(&request, generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.configured.len() == count + 1)
        .await;
    assert_eq!(fm.configured.last(), Some(&held));
    assert_eq!(held.unit.current, request.configuration.unit.current);
    assert_eq!(held.replicas.0.len(), current.replicas.0.len());
    (request, held)
}

async fn configured_membership(fm: &mut FmFixture, fixture: &mut Fixture) -> DoReconfiguration {
    fm.until(fixture, |fm| fm.bootstrapped.iter().all(|ready| *ready))
        .await;
    let body = primary(&fixture.configs[0]);
    let generation = fixture.configs[0].generation().unwrap();
    fm.send(0, add_primary_message(&body, &generation).unwrap())
        .await;
    fm.until(fixture, |fm| fm.primary.is_some()).await;
    for destination in 1..fixture.configs[0].target as usize {
        let build = idle_request(&body, &fixture.configs[destination]);
        fm.send(0, add_replica_message(&build, &generation).unwrap())
            .await;
        fm.until(fixture, |fm| fm.builds.len() == destination).await;
    }
    let mut replicas = vec![fm.primary.as_ref().unwrap().replica.clone()];
    replicas[0].previous_role = 4;
    for built in &fm.builds {
        let mut replica = built.replica.clone();
        replica.previous_role = 2;
        replica.current_role = 3;
        replicas.push(replica);
    }
    let mut unit = body.unit.clone();
    unit.previous = unit.current;
    unit.current.configuration += 1;
    let request = DoReconfiguration {
        configuration: Configuration {
            unit,
            service: body.service,
            replicas: ObjectArray(replicas),
        },
        phase0_duration: i64::MAX,
    };
    fm.send(
        0,
        do_reconfiguration_message(&request, &generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.configured.len() == 1).await;
    request
}

async fn capture_removal_rollover(
    fm: &mut FmFixture,
    fixture: &mut Fixture,
    hold: &[usize],
    retained: &mut Vec<(usize, wire::Incoming)>,
) -> Vec<LfumUpload> {
    hold_rollover(fm, fixture).await;
    let frozen: Vec<_> = fm.uploaded.iter().map(|u| u.clone().unwrap()).collect();
    retained.append(&mut fm.older_uploads);
    for (index, mut incoming) in std::mem::take(&mut fm.held_uploads) {
        if hold.contains(&index) {
            retained.push((index, incoming));
        } else {
            incoming
                .reply
                .take()
                .unwrap()
                .reply(
                    fm.links[index]
                        .as_ref()
                        .unwrap()
                        .seal(
                            lfum_upload_reply_message(
                                &fm.configurations[index].generation().unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                    wire::deadline(),
                )
                .await
                .unwrap();
        }
    }
    fm.hold_uploads = false;
    for (index, body) in frozen.iter().enumerate() {
        until_node(fm, fixture, index, |status| {
            status["generations"]["receive"] == body.generation.generation
        })
        .await;
        if !hold.contains(&index) {
            until_node(fm, fixture, index, |status| {
                status["generations"]["send"] == body.generation.generation
            })
            .await;
        }
    }
    frozen
}

async fn reply_older_removal_rollovers(
    fm: &mut FmFixture,
    retained: &mut Vec<(usize, wire::Incoming)>,
) {
    retained.append(&mut fm.older_uploads);
    for (index, mut incoming) in retained.drain(..) {
        let body = decode_lfum_upload(&incoming.message).unwrap();
        let header = GenerationHeader {
            number: body.generation,
            for_fm_replica: false,
        };
        incoming
            .reply
            .take()
            .unwrap()
            .reply(
                fm.links[index]
                    .as_ref()
                    .unwrap()
                    .seal(lfum_upload_reply_message(&header).unwrap())
                    .unwrap(),
                wire::deadline(),
            )
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_lfum_during_removal_supersession_preserves_pc_uploads_quorum_and_native_completion(
) {
    for (target, minimum, checkpoint) in [(5, 3, 0), (5, 3, 1), (5, 3, 2), (3, 2, 2)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let mut fixture = Fixture::with_sizes(address, target, minimum);
        fixture.start();
        let mut fm = FmFixture::accept(listener, &mut fixture).await;
        let initial = configured_membership(&mut fm, &mut fixture).await;
        let original = fm.configured[0].clone();
        let endpoint = original
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == 4)
            .unwrap()
            .service_endpoint
            .clone();
        assert!(application(&endpoint, "write")
            .await
            .contains("committed=1"));
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let state: Value =
                serde_json::from_str(&application(&endpoint, "state").await).unwrap();
            if state["peers"]
                .as_array()
                .unwrap()
                .iter()
                .all(|p| p["applied"].as_i64().unwrap() >= 1)
            {
                break;
            }
            assert!(Instant::now() < deadline, "original copy progress: {state}");
            fm.step().await;
        }
        let owners: Vec<_> = (0..target as usize).map(|i| fixture.status(i)).collect();
        let nodes: Vec<_> = fixture.children.iter().map(OwnedApp::pid).collect();
        let pid = owners[1]["application_pid"].as_u64().unwrap();
        assert!(Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success());
        fm.until(&mut fixture, |fm| fm.losses.contains(&1)).await;
        let secondary = AddInstance {
            unit: original.unit.clone(),
            service: initial.configuration.service.clone(),
            replica: original
                .replicas
                .0
                .iter()
                .find(|r| r.node == fixture.configs[1].node.identity.native().unwrap())
                .unwrap()
                .clone(),
        };
        fm.send(
            1,
            delete_replica_message(
                &DeleteReplica::new(secondary, false),
                &fm.configurations[1].generation().unwrap(),
            )
            .unwrap(),
        )
        .await;
        fm.until(&mut fixture, |fm| fm.deleted[1]).await;
        let mut removal = initial.configuration;
        removal.unit = original.unit.clone();
        removal.unit.previous = original.unit.current;
        removal.unit.current.configuration += 1;
        removal.replicas = original.replicas.clone();
        for r in &mut removal.replicas.0 {
            r.previous_role = r.current_role;
            if r.node == fixture.configs[1].node.identity.native().unwrap() {
                r.current_role = 1;
                r.up = false;
                r.state = 3;
            }
        }
        fm.removal = Some(RemovalLfum {
            previous: original,
            intent: removal.clone(),
            supersession: None,
        });
        let request = DoReconfiguration {
            configuration: removal.clone(),
            phase0_duration: i64::MAX,
        };
        let all: Vec<_> = (0..target as usize).collect();
        let mut retained = vec![];
        let frozen = capture_removal_rollover(&mut fm, &mut fixture, &all, &mut retained).await;
        let held = if checkpoint == 1 {
            vec![target as usize - 1]
        } else {
            vec![]
        };
        capture_removal_rollover(&mut fm, &mut fixture, &held, &mut retained).await;
        let generation = fm.configurations[0].generation().unwrap();
        let late = if checkpoint == 2 {
            fm.hold_configuration = true;
            fm.send(
                0,
                do_reconfiguration_message(&request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
                .await;
            fm.held_configuration.take()
        } else {
            if checkpoint == 1 {
                fm.send(
                    0,
                    do_reconfiguration_message(&request, &generation).unwrap(),
                )
                .await;
                until_node(&mut fm, &fixture, 0, |s| {
                    s["local_configuration_stage"] == "Deactivate"
                })
                .await;
                until_node(&mut fm, &fixture, target as usize - 1, |s| {
                    s["local_configuration_stage"] == "Deactivated"
                })
                .await;
            }
            None
        };
        let removing = capture_removal_rollover(&mut fm, &mut fixture, &held, &mut retained).await;
        if checkpoint == 1 {
            assert_eq!(
                removing[target as usize - 1].units.0[0].intermediate,
                removal.unit.current
            );
        }
        let work = fixture.work(2);
        let mut paused = None;
        if target == 5 {
            for name in ["local-primary-role", "local-current-configuration"] {
                fs::write(
                    work.join(format!("{name}.block")),
                    b"hold actual superseding RAP work",
                )
                .unwrap();
            }
            paused = Some(PausedProcess::new(
                owners[4]["application_pid"]
                    .as_u64()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ));
        }
        let pid = owners[0]["application_pid"].as_u64().unwrap();
        assert!(Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success());
        fm.until(&mut fixture, |fm| fm.losses.contains(&0)).await;
        let quorum = [true, false].into_iter().all(|previous| {
            let members: Vec<_> = removal
                .replicas
                .0
                .iter()
                .filter(|r| {
                    if previous {
                        r.previous_role >= 3
                    } else {
                        r.current_role >= 3
                    }
                })
                .collect();
            members
                .iter()
                .filter(|r| r.up && r.node != fixture.configs[0].node.identity.native().unwrap())
                .count()
                >= members.len().div_ceil(2)
        });
        assert_eq!(quorum, target == 5);
        let mut failover = removal.clone();
        if quorum {
            failover.unit.current.configuration += 0x1_0000_0001;
            for r in &mut failover.replicas.0 {
                if r.node == fixture.configs[0].node.identity.native().unwrap() {
                    r.current_role = 3;
                    r.up = false;
                    r.state = 3;
                } else if r.node == fixture.configs[2].node.identity.native().unwrap() {
                    r.current_role = 4;
                }
                r.first_lsn = -1;
                r.last_lsn = -1;
            }
            fm.removal.as_mut().unwrap().supersession = Some(failover.clone());
            fm.hold_configuration = true;
            fm.send(
                2,
                do_reconfiguration_message(
                    &DoReconfiguration {
                        configuration: failover.clone(),
                        phase0_duration: i64::MAX,
                    },
                    &fm.configurations[2].generation().unwrap(),
                )
                .unwrap(),
            )
            .await;
            until_node(&mut fm, &fixture, 2, |s| {
                s["primary_swap_stage"] == "GetLsn"
            })
            .await;
        }
        hold_rollover(&mut fm, &mut fixture).await;
        for (index, incoming) in retained.iter().chain(&fm.older_uploads) {
            let body = decode_lfum_upload(&incoming.message).unwrap();
            if body.generation == frozen[*index].generation {
                assert_eq!(body, frozen[*index]);
            }
        }
        for index in 0..target as usize {
            let generation = fm.configurations[index].fm.generation;
            until_node(&mut fm, &fixture, index, |s| {
                s["generations"]["receive"] == generation
            })
            .await;
        }
        let send: Vec<_> = (0..target as usize)
            .map(|i| fixture.status(i)["generations"]["send"].clone())
            .collect();
        reply_older_removal_rollovers(&mut fm, &mut retained).await;
        for (index, expected) in send.iter().enumerate() {
            until_node(&mut fm, &fixture, index, |s| {
                s["generations"]["pending_uploads"] == 1
            })
            .await;
            assert_eq!(&fixture.status(index)["generations"]["send"], expected);
        }
        assert_ne!(fm.uploaded[0].as_ref().unwrap().units, frozen[0].units);
        acknowledge_rollover(&mut fm, &mut fixture).await;
        assert_eq!(fm.configured.len(), 1);
        if quorum {
            assert_eq!(
                fixture.status(2)["primary_swap_stage"],
                "GetLsn",
                "LFUM ACK cannot replace actual progress"
            );
            drop(paused.take());
            let intent = DoReconfiguration {
                configuration: failover.clone(),
                phase0_duration: i64::MAX,
            };
            fm.send(
                2,
                do_reconfiguration_message(&intent, &fm.configurations[2].generation().unwrap())
                    .unwrap(),
            )
            .await;
            until_node(&mut fm, &fixture, 2, |s| {
                s["primary_swap_stage"] == "Catchup"
                    && work.join("local-primary-role.held").exists()
            })
            .await;
            hold_rollover(&mut fm, &mut fixture).await;
            assert_eq!(
                fm.uploaded[2].as_ref().unwrap().units.0[0].intermediate,
                Epoch::default()
            );
            reply_older_removal_rollovers(&mut fm, &mut retained).await;
            acknowledge_rollover(&mut fm, &mut fixture).await;
            assert!(!work.join("local-current-configuration.held").exists());
            fs::write(
                work.join("local-primary-role.release"),
                b"complete actual superseding promotion",
            )
            .unwrap();
            until_node(&mut fm, &fixture, 2, |s| {
                s["primary_swap_stage"] == "End"
                    && work.join("local-current-configuration.held").exists()
            })
            .await;
            hold_rollover(&mut fm, &mut fixture).await;
            assert_eq!(
                fm.uploaded[2].as_ref().unwrap().units.0[0].intermediate,
                failover.unit.current
            );
            reply_older_removal_rollovers(&mut fm, &mut retained).await;
            acknowledge_rollover(&mut fm, &mut fixture).await;
            assert_eq!(fm.configured.len(), 1);
            fs::write(
                work.join("local-current-configuration.release"),
                b"complete actual superseding final RAP",
            )
            .unwrap();
            fm.until(&mut fixture, |fm| {
                fm.held_configuration
                    .as_ref()
                    .is_some_and(|r| r.unit.current == failover.unit.current)
            })
            .await;
            let completed = fm.held_configuration.take().unwrap();
            rollover(&mut fm, &mut fixture).await;
            assert_eq!(
                fm.uploaded[2].as_ref().unwrap().units.0[0].unit.previous,
                Epoch::default()
            );
            assert_eq!(
                fm.configured.len(),
                1,
                "LFUM cannot replace final native completion"
            );
            fm.hold_configuration = false;
            fm.send(
                2,
                do_reconfiguration_message(&intent, &fm.configurations[2].generation().unwrap())
                    .unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
            assert_eq!(fm.configured[1], completed);
            let endpoint = completed
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint
                .clone();
            assert!(application(&endpoint, "read")
                .await
                .contains("committed=1 value=acknowledged"));
            assert!(application(&endpoint, "write-more")
                .await
                .contains("committed=2"));
            let pid = owners[4]["application_pid"].as_u64().unwrap();
            assert!(Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            fm.until(&mut fixture, |fm| fm.losses.contains(&4)).await;
            if let Ok(reply) = tokio::time::timeout(
                Duration::from_millis(250),
                application(&endpoint, "write-final"),
            )
            .await
            {
                assert!(
                    !reply.starts_with("OK"),
                    "retained down CC member cannot supply a vote"
                );
            }
        } else {
            assert_eq!(fixture.status(2)["replica_role"], 3);
            assert!(fm.removal.as_ref().unwrap().supersession.is_none());
            assert!(fixture.status(2)["service_endpoint"]
                .as_str()
                .unwrap()
                .is_empty());
            assert!(TcpStream::connect(&endpoint).await.is_err());
        }
        let replay_owner = PausedProcess::new(fixture.children[0].pid());
        fm.send(
            0,
            do_reconfiguration_message(&request, &fm.configurations[0].generation().unwrap())
                .unwrap(),
        )
        .await;
        if let Some(late) = late {
            assert_eq!(late.unit.current, removal.unit.current);
            let completed = fm.configured.len();
            fm.configuration_reply(late);
            assert_eq!(
                fm.configured.len(),
                completed,
                "earlier removal reply cannot complete supersession"
            );
        }
        drop(replay_owner);
        // Fence the late one-way replay's ingress, not reconfiguration completion.
        let generation = fm.configurations[0].generation().unwrap();
        let link = fm.links[0].as_ref().unwrap().clone();
        let proposal = link.generation_proposal(&generation);
        tokio::pin!(proposal);
        let reply = loop {
            tokio::select! {
                biased;
                reply = &mut proposal => break reply.unwrap(),
                _ = std::future::ready(()) => {}
            }
            fm.step().await;
        };
        assert_eq!(reply.error, 0);
        assert_eq!(reply.proposed, generation.number);
        assert_eq!(reply.current, generation.number);
        for (index, owner) in owners.iter().enumerate() {
            let status = fixture.status(index);
            assert_eq!(fixture.children[index].pid(), nodes[index]);
            assert_eq!(status["application_pid"], owner["application_pid"]);
            assert_eq!(status["runtime_root"], owner["runtime_root"]);
            assert_eq!(status["host_replacement_count"], 0);
            assert_eq!(status["control_unknown"], false);
        }
        fm.stopping = true;
        for index in 0..target as usize {
            fixture.stop_node(&mut fm, index).await;
        }
        for link in fm.links.iter().flatten() {
            link.close().await.unwrap();
        }
        fixture.finish(vec![true; target as usize]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_repeated_planned_handoffs_coordinate_all_members_and_replay_native_completion()
{
    for (target, minimum, candidate) in [(2, 1, 1), (3, 2, 1), (3, 2, 2)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let mut fixture = Fixture::with_sizes(address, target, minimum);
        fixture.start();
        let mut fm = FmFixture::accept(listener, &mut fixture).await;
        let initial = configured_membership(&mut fm, &mut fixture).await;
        let body = primary(&fixture.configs[0]);
        let generation = fixture.configs[0].generation().unwrap();
        let old_endpoint = fm
            .primary
            .as_ref()
            .unwrap()
            .replica
            .service_endpoint
            .clone();
        let deadline = Instant::now() + Duration::from_secs(8);
        while !application(&old_endpoint, "read").await.starts_with("OK") {
            assert!(
                Instant::now() < deadline,
                "listener readiness: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        assert!(application(&old_endpoint, "write")
            .await
            .contains("committed=1"));
        let owners: Vec<_> = (0..target as usize).map(|i| fixture.status(i)).collect();
        let completed = fm.configured[0].clone();
        let mut membership = Configuration {
            unit: completed.unit.clone(),
            service: body.service.clone(),
            replicas: completed.replicas,
        };
        let mut stale = vec![initial.clone()];
        let destinations = [
            candidate,
            0,
            if target == 2 {
                candidate
            } else {
                3 - candidate
            },
        ];
        for (round, destination) in destinations.into_iter().enumerate() {
            let (request, reply) = planned_handoff(
                &mut fm,
                &mut fixture,
                &membership,
                destination,
                &generation,
                &stale,
            )
            .await;
            let endpoint = &reply
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint;
            assert!(application(endpoint, "read")
                .await
                .contains(&format!("committed={}", round + 1)));
            assert!(application(endpoint, &format!("write {}", round + 2))
                .await
                .contains(&format!("committed={}", round + 2)));
            membership.unit = reply.unit;
            membership.replicas = reply.replicas;
            stale.push(request);
        }
        let candidate = destinations[2];
        let lost = fm.configured.last().unwrap().clone();
        let endpoint = lost
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == 4)
            .unwrap()
            .service_endpoint
            .clone();
        assert!(application(&endpoint, "read").await.contains("committed=4"));
        assert_eq!((lost.unit.target, lost.unit.minimum), (target, minimum));
        assert_eq!(lost.replicas.0.len(), target as usize);
        for (index, owner) in owners.iter().enumerate() {
            let current = fixture.status(index);
            fixture.children[index].check_alive().unwrap();
            assert_eq!(
                current["application_pid"].as_u64().unwrap(),
                owner["application_pid"].as_u64().unwrap()
            );
            assert_eq!(current["runtime_root"], owner["runtime_root"]);
            assert_eq!(current["host_replacement_count"], 0);
            assert!(
                current["control_error"].is_null(),
                "{}",
                fixture.diagnostics()
            );
        }
        fm.send(
            0,
            do_reconfiguration_message(&initial, &generation).unwrap(),
        )
        .await;
        fm.stopping = true;
        for index in (0..target as usize)
            .filter(|i| *i != candidate)
            .chain(std::iter::once(candidate))
        {
            if target == 3 && index != 0 && index != candidate {
                assert!(application(&endpoint, "write 5")
                    .await
                    .contains("committed=5"));
            }
            if target == 3 && index == candidate {
                if let Ok(reply) = tokio::time::timeout(
                    Duration::from_millis(250),
                    application(&endpoint, "write 6"),
                )
                .await
                {
                    assert!(
                        !reply.starts_with("OK"),
                        "closed members cannot supply a quorum: {reply}"
                    );
                }
            }
            let node = fixture.configs[index].node.identity.native().unwrap();
            let deletion = DeleteReplica::new(
                AddInstance {
                    unit: lost.unit.clone(),
                    service: body.service.clone(),
                    replica: lost
                        .replicas
                        .0
                        .iter()
                        .find(|r| r.node == node)
                        .unwrap()
                        .clone(),
                },
                false,
            );
            fm.send(
                index,
                delete_replica_message(&deletion, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.deleted[index]).await;
        }
        for index in 0..target as usize {
            fixture.stop_node(&mut fm, index).await;
        }
        for link in fm.links.iter().flatten() {
            link.close().await.unwrap();
        }
        fixture.finish(vec![true; target as usize]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_lfum_during_planned_handoff_preserves_sf_phases_upload_owners_and_writer_fencing(
) {
    for candidate in [1, 2] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let mut fixture = Fixture::new(address);
        fixture.start();
        let mut fm = FmFixture::accept(listener, &mut fixture).await;
        let initial = configured_membership(&mut fm, &mut fixture).await;
        let previous = fm.configured.last().unwrap().clone();
        let endpoint = previous
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == 4)
            .unwrap()
            .service_endpoint
            .clone();
        assert!(application(&endpoint, "write")
            .await
            .contains("committed=1"));
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let state: Value =
                serde_json::from_str(&application(&endpoint, "state").await).unwrap();
            if state["peers"]
                .as_array()
                .unwrap()
                .iter()
                .all(|p| p["applied"].as_i64().unwrap() >= 1)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "all members apply before handoff: {state}"
            );
            fm.step().await;
        }
        let owners: Vec<_> = (0..3).map(|i| fixture.status(i)).collect();
        let node_pids: Vec<_> = fixture.children.iter().map(OwnedApp::pid).collect();
        let third = 3 - candidate;
        let mut work = vec![];
        for (index, barrier) in [
            (0, "local-secondary-role"),
            (candidate, "local-primary-role"),
            (candidate, "local-current-configuration"),
        ] {
            let path = fixture.work(index);
            fs::write(
                path.join(format!("{barrier}.block")),
                b"hold actual handoff callback",
            )
            .unwrap();
            work.push((
                path.join(format!("{barrier}.held")),
                path.join(format!("{barrier}.release")),
            ));
        }
        let mut intent = Configuration {
            unit: previous.unit.clone(),
            service: initial.configuration.service.clone(),
            replicas: previous.replicas.clone(),
        };
        intent.unit.previous = intent.unit.current;
        intent.unit.current.configuration += 0x1_0000_0001;
        let candidate_node = fixture.configs[candidate].node.identity.native().unwrap();
        for r in &mut intent.replicas.0 {
            r.previous_role = r.current_role;
            r.current_role = if r.node == candidate_node { 4 } else { 3 };
        }
        let request = DoReconfiguration {
            configuration: intent.clone(),
            phase0_duration: i64::MAX,
        };
        fm.transfer = Some((previous, request.clone()));
        fm.hold_configuration = true;
        fm.send(
            0,
            do_reconfiguration_message(&request, &fm.configurations[0].generation().unwrap())
                .unwrap(),
        )
        .await;
        until_node(&mut fm, &fixture, 0, |status| {
            status["primary_swap_stage"] == "Demote" && work[0].0.exists()
        })
        .await;
        hold_superseding_rollover(&mut fm, &mut fixture).await;
        assert!(fm.uploaded[0].as_ref().unwrap().units.0[0].report_from_primary);
        acknowledge_rollover(&mut fm, &mut fixture).await;
        assert!(fm.held_configuration.is_none());

        let before = fixture.status(candidate)["generations"].clone();
        fm.hold_uploads = true;
        fm.uploaded.fill(None);
        start_rollover_on(&mut fm, 0).await;
        fm.until(&mut fixture, |fm| fm.uploaded[0].is_some()).await;
        let source_snapshot = fm.uploaded[0].clone().unwrap();
        for (index, mut incoming) in std::mem::take(&mut fm.held_uploads) {
            assert_eq!(index, 0);
            let link = fm.links[index].as_ref().unwrap();
            incoming
                .reply
                .take()
                .unwrap()
                .reply(
                    link.seal(
                        lfum_upload_reply_message(&fm.configurations[index].generation().unwrap())
                            .unwrap(),
                    )
                    .unwrap(),
                    wire::deadline(),
                )
                .await
                .unwrap();
        }
        let source_generation = fm.configurations[0].fm.generation;
        until_node(&mut fm, &fixture, 0, |status| {
            status["generations"]["send"] == source_generation
        })
        .await;
        let paused = PausedProcess::new(
            u32::try_from(fixture.status(third)["application_pid"].as_u64().unwrap()).unwrap(),
        );
        fs::write(&work[0].1, b"finish actual demotion").unwrap();
        until_node(&mut fm, &fixture, candidate, |status| {
            status["primary_swap_stage"] == "GetLsn"
        })
        .await;
        until_node(&mut fm, &fixture, third, |status| {
            status["replica_epoch"] == serde_json::json!(intent.unit.current)
        })
        .await;
        assert_eq!(
            fixture.status(candidate)["generations"],
            before,
            "newer peer continuation cannot adopt the sender's generation"
        );
        fm.uploaded[0] = None;
        fm.send(
            0,
            generation_update_message(
                &GenerationUpdate::default(),
                &fm.configurations[0].generation().unwrap(),
            )
            .unwrap(),
        )
        .await;
        for index in 1..3 {
            start_rollover_on(&mut fm, index).await;
        }
        fm.until(&mut fixture, |fm| fm.uploaded.iter().all(Option::is_some))
            .await;
        let old = &fm.uploaded[0].as_ref().unwrap().units.0[0];
        assert_ne!(
            source_snapshot.units,
            fm.uploaded[0].as_ref().unwrap().units
        );
        assert!(!old.report_from_primary);
        let local = old
            .replicas
            .0
            .iter()
            .find(|r| r.description.node == fixture.configs[0].node.identity.native().unwrap())
            .unwrap();
        assert_eq!(local.description.state, 1);
        assert!(local.description.service_endpoint.is_empty());
        let queried = &fm.uploaded[third].as_ref().unwrap().units.0[0];
        assert_eq!(queried.unit, intent.unit);
        assert!(queried
            .replicas
            .0
            .iter()
            .any(|r| r.description.current_role == 4 && r.description.previous_role == 4));
        assert!(!work[1].0.exists());
        acknowledge_rollover(&mut fm, &mut fixture).await;
        assert!(
            !work[1].0.exists(),
            "LFUM ACK cannot replace missing progress"
        );
        drop(paused);
        fm.send(
            0,
            do_reconfiguration_message(&request, &fm.configurations[0].generation().unwrap())
                .unwrap(),
        )
        .await;
        until_node(&mut fm, &fixture, candidate, |status| {
            status["primary_swap_stage"] == "Catchup" && work[1].0.exists()
        })
        .await;
        assert!(application(&endpoint, "write 2")
            .await
            .contains("NotPrimary"));

        hold_rollover(&mut fm, &mut fixture).await;
        let mut delayed = None;
        for (index, mut incoming) in std::mem::take(&mut fm.held_uploads) {
            assert_eq!(
                decode_lfum_upload(&incoming.message).unwrap().units.0[0].intermediate,
                Epoch::default()
            );
            if index == third {
                delayed = Some(incoming);
            } else {
                let link = fm.links[index].as_ref().unwrap();
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .reply(
                        link.seal(
                            lfum_upload_reply_message(
                                &fm.configurations[index].generation().unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                        wire::deadline(),
                    )
                    .await
                    .unwrap();
            }
        }
        let generation = fm.configurations[candidate].fm.generation;
        until_node(&mut fm, &fixture, candidate, |status| {
            status["generations"]["send"] == generation
        })
        .await;
        fs::write(&work[1].1, b"finish Primary role").unwrap();
        until_node(&mut fm, &fixture, candidate, |status| {
            status["primary_swap_stage"] == "Activate"
        })
        .await;
        assert!(
            !work[2].0.exists(),
            "every Secondary's matching ActivateReply is required"
        );
        for index in [0, third] {
            until_node(&mut fm, &fixture, index, |status| {
                status["primary_swap_stage"] == "Complete"
            })
            .await;
        }

        hold_rollover(&mut fm, &mut fixture).await;
        assert_eq!(
            fm.uploaded[candidate].as_ref().unwrap().units.0[0].intermediate,
            intent.unit.current
        );
        for index in [0, third] {
            let info = &fm.uploaded[index].as_ref().unwrap().units.0[0];
            assert_eq!(info.unit.previous, Epoch::default());
            assert!(!info.report_from_primary);
        }
        let mut delayed = delayed.unwrap();
        let old = decode_lfum_upload(&delayed.message).unwrap();
        assert_ne!(old.units, fm.uploaded[third].as_ref().unwrap().units);
        let send = fixture.status(third)["generations"]["send"].clone();
        let link = fm.links[third].as_ref().unwrap();
        delayed
            .reply
            .take()
            .unwrap()
            .reply(
                link.seal(
                    lfum_upload_reply_message(&GenerationHeader {
                        number: old.generation,
                        for_fm_replica: false,
                    })
                    .unwrap(),
                )
                .unwrap(),
                wire::deadline(),
            )
            .await
            .unwrap();
        until_node(&mut fm, &fixture, third, |status| {
            status["generations"]["pending_uploads"] == 1
        })
        .await;
        assert_eq!(fixture.status(third)["generations"]["send"], send);
        assert_eq!(fixture.status(third)["generations"]["upload_pending"], true);
        assert!(!work[2].0.exists());
        acknowledge_rollover(&mut fm, &mut fixture).await;
        fm.send(
            0,
            do_reconfiguration_message(&request, &fm.configurations[0].generation().unwrap())
                .unwrap(),
        )
        .await;
        until_node(&mut fm, &fixture, candidate, |status| {
            status["primary_swap_stage"] == "End" && work[2].0.exists()
        })
        .await;
        rollover(&mut fm, &mut fixture).await;
        assert_eq!(
            fm.uploaded[candidate].as_ref().unwrap().units.0[0].intermediate,
            intent.unit.current
        );
        assert!(fm.held_configuration.is_none());
        assert_eq!(fm.configured.len(), 1);
        fs::write(&work[2].1, b"finish final RAP configuration").unwrap();
        fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
            .await;
        let lost = fm.held_configuration.take().unwrap();
        rollover(&mut fm, &mut fixture).await;
        assert!(fm
            .uploaded
            .iter()
            .all(|body| body.as_ref().unwrap().units.0[0].unit.previous == Epoch::default()));
        assert_eq!(
            fm.configured.len(),
            1,
            "upload ACK cannot complete the FM handoff"
        );
        let generation = fm.configurations[0].generation().unwrap();
        fm.send(
            0,
            do_reconfiguration_message(&initial, &generation).unwrap(),
        )
        .await;
        fm.hold_configuration = false;
        fm.send(
            0,
            do_reconfiguration_message(&request, &generation).unwrap(),
        )
        .await;
        fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
        assert_eq!(fm.configured.last(), Some(&lost));
        let promoted = lost
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == 4)
            .unwrap()
            .service_endpoint
            .clone();
        assert!(application(&promoted, "read").await.contains("committed=1"));
        assert!(application(&promoted, "write 2")
            .await
            .contains("committed=2"));
        for (index, owner) in owners.iter().enumerate() {
            let current = fixture.status(index);
            fixture.children[index].check_alive().unwrap();
            assert_eq!(fixture.children[index].pid(), node_pids[index]);
            for field in [
                "application_pid",
                "runtime_root",
                "replica_id",
                "replica_instance",
            ] {
                assert_eq!(current[field], owner[field]);
            }
            assert_eq!(current["host_replacement_count"], 0);
            for field in [
                "control_error",
                "adapter_error",
                "peer_error",
                "peer_command_error",
            ] {
                assert!(current[field].is_null(), "unexpected {field}: {current}");
            }
        }
        fm.transfer = None;
        fm.stopping = true;
        for index in [0, third, candidate] {
            if index == third {
                assert!(application(&promoted, "write 3")
                    .await
                    .contains("committed=3"));
            } else if index == candidate {
                if let Ok(reply) = tokio::time::timeout(
                    Duration::from_millis(250),
                    application(&promoted, "write 4"),
                )
                .await
                {
                    assert!(
                        !reply.starts_with("OK"),
                        "closed members cannot supply a quorum: {reply}"
                    );
                }
            }
            let node = fixture.configs[index].node.identity.native().unwrap();
            let delete = DeleteReplica::new(
                AddInstance {
                    unit: lost.unit.clone(),
                    service: intent.service.clone(),
                    replica: lost
                        .replicas
                        .0
                        .iter()
                        .find(|r| r.node == node)
                        .unwrap()
                        .clone(),
                },
                false,
            );
            fm.send(index, delete_replica_message(&delete, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.deleted[index]).await;
        }
        for index in 0..3 {
            fixture.stop_node(&mut fm, index).await;
        }
        for link in fm.links.iter().flatten() {
            link.close().await.unwrap();
        }
        fixture.finish([true; 3]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_handoff_standalone_app_crash_repairs_same_node_then_transfers() {
    for candidate in [1, 2] {
        for lost in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
                unreachable!()
            };
            let mut fixture = Fixture::new(address);
            fixture.start();
            let mut fm = FmFixture::accept(listener, &mut fixture).await;
            let initial = configured_membership(&mut fm, &mut fixture).await;
            let service = initial.configuration.service.clone();
            let generation = fixture.configs[0].generation().unwrap();
            let completed = fm.configured.last().unwrap().clone();
            let membership = Configuration {
                unit: completed.unit,
                service: service.clone(),
                replicas: completed.replicas,
            };
            let endpoint = &membership
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint;
            assert!(application(endpoint, "write").await.contains("committed=1"));
            let (planned, completed) = planned_handoff(
                &mut fm,
                &mut fixture,
                &membership,
                candidate,
                &generation,
                &[initial],
            )
            .await;
            let endpoint = completed
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint
                .clone();
            assert!(application(&endpoint, "write 2")
                .await
                .contains("committed=2"));
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let state: Value =
                    serde_json::from_str(&application(&endpoint, "state").await).unwrap();
                if state["peers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|peer| peer["applied"].as_i64().unwrap() >= 2)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "all members apply before crash: {state}"
                );
                fm.step().await;
            }
            let owners = (0..3).map(|i| fixture.status(i)).collect::<Vec<_>>();
            let node_pids = fixture
                .children
                .iter()
                .map(OwnedApp::pid)
                .collect::<Vec<_>>();
            let (predecessor, recovered, primary_index) = if lost == candidate {
                let predecessor = AddInstance {
                    unit: completed.unit.clone(),
                    service: service.clone(),
                    replica: completed
                        .replicas
                        .0
                        .iter()
                        .find(|r| r.node == fixture.configs[lost].node.identity.native().unwrap())
                        .unwrap()
                        .clone(),
                };
                let pid = owners[lost]["application_pid"].as_u64().unwrap();
                assert!(Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .status()
                    .unwrap()
                    .success());
                fm.until(&mut fixture, |fm| fm.losses.contains(&lost)).await;
                let primary_index = (0..3).find(|i| *i != lost).unwrap();
                let node = fixture.configs[primary_index]
                    .node
                    .identity
                    .native()
                    .unwrap();
                let mut configuration = Configuration {
                    unit: completed.unit.clone(),
                    service: service.clone(),
                    replicas: completed.replicas.clone(),
                };
                configuration.unit.previous = configuration.unit.current;
                configuration.unit.current.configuration += 0x1_0000_0001;
                for r in &mut configuration.replicas.0 {
                    r.previous_role = r.current_role;
                    if r.node == predecessor.replica.node {
                        r.current_role = 1;
                        r.up = false;
                        r.state = 3;
                    } else if r.node == node {
                        r.current_role = 4;
                    }
                    r.first_lsn = -1;
                    r.last_lsn = -1;
                }
                let request = DoReconfiguration {
                    configuration,
                    phase0_duration: i64::MAX,
                };
                let replies = fm.configured.len();
                fm.hold_configuration = true;
                fm.send(
                    primary_index,
                    do_reconfiguration_message(&request, &generation).unwrap(),
                )
                .await;
                fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
                    .await;
                let held = fm.held_configuration.take().unwrap();
                assert_eq!(fm.configured.len(), replies);
                fm.send(
                    0,
                    do_reconfiguration_message(&planned, &generation).unwrap(),
                )
                .await;
                fm.hold_configuration = false;
                fm.send(
                    primary_index,
                    do_reconfiguration_message(&request, &generation).unwrap(),
                )
                .await;
                fm.until(&mut fixture, |fm| fm.configured.len() == replies + 1)
                    .await;
                assert_eq!(fm.configured.last(), Some(&held));
                assert_eq!(held.unit.current, request.configuration.unit.current);
                (predecessor, held, primary_index)
            } else {
                let (predecessor, removed) = remove_lost_secondary(
                    &mut fixture,
                    &mut fm,
                    &completed,
                    &service,
                    lost,
                    candidate,
                )
                .await;
                (predecessor, removed, candidate)
            };
            let endpoint = recovered
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint
                .clone();
            assert!(application(&endpoint, "read").await.contains("committed=2"));
            let repaired = restore_original(
                &mut fixture,
                &mut fm,
                &predecessor,
                &recovered,
                lost,
                primary_index,
                false,
            )
            .await;
            assert_eq!(
                (
                    repaired.unit.target,
                    repaired.unit.minimum,
                    repaired.replicas.0.len()
                ),
                (3, 2, 3)
            );
            let fresh = fixture.status(lost);
            assert_ne!(fresh["application_pid"], owners[lost]["application_pid"]);
            assert_ne!(fresh["runtime_root"], owners[lost]["runtime_root"]);
            assert!(fresh["replica_id"].as_i64().unwrap() > predecessor.replica.id);
            assert!(fresh["replica_instance"].as_i64().unwrap() > predecessor.replica.instance);
            for (i, owner) in owners.iter().enumerate().filter(|(i, _)| *i != lost) {
                assert_eq!(
                    fixture.status(i)["application_pid"],
                    owner["application_pid"]
                );
                assert_eq!(fixture.status(i)["runtime_root"], owner["runtime_root"]);
            }
            assert!(application(&endpoint, "write 3")
                .await
                .contains("committed=3"));
            fm.send(
                lost,
                delete_replica_message(&DeleteReplica::new(predecessor, false), &generation)
                    .unwrap(),
            )
            .await;
            let membership = Configuration {
                unit: repaired.unit,
                service: service.clone(),
                replicas: repaired.replicas,
            };
            let (_, final_reply) = planned_handoff(
                &mut fm,
                &mut fixture,
                &membership,
                lost,
                &generation,
                &[planned],
            )
            .await;
            assert!(application(&endpoint, "write 4")
                .await
                .contains("NotPrimary"));
            let endpoint = final_reply
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint
                .clone();
            assert!(application(&endpoint, "read").await.contains("committed=3"));
            assert!(application(&endpoint, "write 4")
                .await
                .contains("committed=4"));
            for (i, pid) in node_pids.iter().enumerate() {
                assert_eq!(fixture.children[i].pid(), *pid);
                fixture.children[i].check_alive().unwrap();
                assert!(
                    fixture.status(i)["control_error"].is_null(),
                    "{}",
                    fixture.diagnostics()
                );
            }
            fm.stopping = true;
            for (round, index) in (0..3)
                .filter(|i| *i != lost)
                .chain(std::iter::once(lost))
                .enumerate()
            {
                let node = fixture.configs[index].node.identity.native().unwrap();
                fm.deleted[index] = false;
                fm.send(
                    index,
                    delete_replica_message(
                        &DeleteReplica::new(
                            AddInstance {
                                unit: final_reply.unit.clone(),
                                service: service.clone(),
                                replica: final_reply
                                    .replicas
                                    .0
                                    .iter()
                                    .find(|r| r.node == node)
                                    .unwrap()
                                    .clone(),
                            },
                            false,
                        ),
                        &generation,
                    )
                    .unwrap(),
                )
                .await;
                fm.until(&mut fixture, |fm| fm.deleted[index]).await;
                if round == 0 {
                    assert!(application(&endpoint, "write 5")
                        .await
                        .contains("committed=5"));
                } else if round == 1 {
                    if let Ok(reply) = tokio::time::timeout(
                        Duration::from_millis(250),
                        application(&endpoint, "write 6"),
                    )
                    .await
                    {
                        assert!(
                            !reply.starts_with("OK"),
                            "missing quorum cannot ACK: {reply}"
                        );
                    }
                }
            }
            for index in 0..3 {
                fixture.stop_node(&mut fm, index).await;
            }
            for link in fm.links.iter().flatten() {
                link.close().await.unwrap();
            }
            fixture.finish([true; 3]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_planned_handoff_drops_missing_progress_then_rebuilds_an_explicit_spare() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let mut fixture = Fixture::with_node_pool(address, 3, 2, 4);
    for config in &mut fixture.configs {
        config.remote_replica_progress_query_wait_ms = 250;
    }
    fixture.start();
    let mut fm = FmFixture::accept(listener, &mut fixture).await;
    let initial = configured_membership(&mut fm, &mut fixture).await;
    let generation = fixture.configs[0].generation().unwrap();
    let mut membership = fm.configured[0].clone();
    let endpoint = membership.replicas.0[0].service_endpoint.clone();
    let deadline = Instant::now() + Duration::from_secs(8);
    while !application(&endpoint, "read").await.starts_with("OK") {
        assert!(Instant::now() < deadline, "{}", fixture.diagnostics());
        fm.step().await;
    }
    assert!(application(&endpoint, "write")
        .await
        .contains("committed=1"));
    let owners: Vec<_> = (0..4).map(|i| fixture.status(i)).collect();
    let paused = PausedProcess::new(fixture.children[2].pid());
    let mut intent = initial.configuration.clone();
    intent.unit.previous = membership.unit.current;
    intent.unit.current = membership.unit.current;
    intent.unit.current.configuration += 0x1_0000_0001;
    intent.replicas = membership.replicas;
    let candidate = fixture.configs[1].node.identity.native().unwrap();
    for r in &mut intent.replicas.0 {
        r.previous_role = r.current_role;
        r.current_role = if r.node == candidate { 4 } else { 3 };
    }
    let request = DoReconfiguration {
        configuration: intent,
        phase0_duration: i64::MAX,
    };
    fm.hold_configuration = true;
    fm.send(
        0,
        do_reconfiguration_message(&request, &generation).unwrap(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(45);
    while fixture.status(1)["primary_swap_stage"] != "Activate" {
        assert!(Instant::now() < deadline, "{}", fixture.diagnostics());
        fm.step().await;
    }
    assert_eq!(fm.configured.len(), 1);
    assert!(fm.held_configuration.is_none());
    assert!(application(&endpoint, "write-more")
        .await
        .contains("NotPrimary"));
    drop(paused);
    fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
        .await;
    let held = fm.held_configuration.take().unwrap();
    for resource in ["local-instance.resource", "local-provider.resource"] {
        assert!(
            !fixture.work(2).join(resource).exists(),
            "native restart Close must dispose {resource}"
        );
    }
    let lost = fixture.configs[2].node.identity.native().unwrap();
    let dropped = held
        .replicas
        .0
        .iter()
        .find(|r| r.node == lost)
        .unwrap()
        .clone();
    assert!(!dropped.up && dropped.state == 3 && dropped.current_role == 3);
    assert_eq!(fm.configured.len(), 1);
    fm.hold_configuration = false;
    fm.send(
        0,
        do_reconfiguration_message(&request, &generation).unwrap(),
    )
    .await;
    fm.until(&mut fixture, |fm| {
        fm.configured.len() == 2 && fm.losses.contains(&2)
    })
    .await;
    assert_eq!(fm.configured.last(), Some(&held));
    membership = held;
    let endpoint = membership
        .replicas
        .0
        .iter()
        .find(|r| r.current_role == 4)
        .unwrap()
        .service_endpoint
        .clone();
    assert!(application(&endpoint, "write-more")
        .await
        .contains("committed=2"));
    let delete = DeleteReplica::new(
        AddInstance {
            unit: request.configuration.unit,
            service: initial.configuration.service.clone(),
            replica: dropped,
        },
        false,
    );
    fm.send(2, delete_replica_message(&delete, &generation).unwrap())
        .await;
    fm.until(&mut fixture, |fm| fm.deleted[2]).await;
    let mut build = idle_request(&primary(&fixture.configs[0]), &fixture.configs[3]);
    build.unit = membership.unit.clone();
    fm.hold_build = true;
    fm.send(1, add_replica_message(&build, &generation).unwrap())
        .await;
    fm.until(&mut fixture, |fm| fm.held_build.is_some()).await;
    let built = fm.held_build.take().unwrap();
    fm.hold_build = false;
    fm.send(1, add_replica_message(&build, &generation).unwrap())
        .await;
    fm.until(&mut fixture, |fm| fm.builds.len() == 3).await;
    assert_eq!(fm.builds.last(), Some(&built));
    let mut repair = Configuration {
        unit: membership.unit.clone(),
        service: initial.configuration.service.clone(),
        replicas: membership.replicas,
    };
    repair.unit.previous = repair.unit.current;
    repair.unit.current.configuration += 1;
    for r in &mut repair.replicas.0 {
        r.previous_role = r.current_role;
        if r.node == lost {
            r.current_role = 1;
        }
    }
    let mut fresh = built.replica;
    fresh.previous_role = 2;
    fresh.current_role = 3;
    repair.replicas.0.push(fresh);
    let repair = DoReconfiguration {
        configuration: repair,
        phase0_duration: i64::MAX,
    };
    fm.hold_configuration = true;
    fm.send(1, do_reconfiguration_message(&repair, &generation).unwrap())
        .await;
    fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
        .await;
    let restored = fm.held_configuration.take().unwrap();
    fm.hold_configuration = false;
    fm.send(1, do_reconfiguration_message(&repair, &generation).unwrap())
        .await;
    fm.until(&mut fixture, |fm| fm.configured.len() == 3).await;
    assert_eq!(fm.configured.last(), Some(&restored));
    assert_eq!(restored.replicas.0.len(), 3);
    assert!(restored.replicas.0.iter().all(|r| r.node != lost && r.up));
    assert!(application(&endpoint, "write-final")
        .await
        .contains("committed=3"));
    for i in [0, 1] {
        assert_eq!(
            fixture.status(i)["application_pid"],
            owners[i]["application_pid"]
        );
        assert_eq!(fixture.status(i)["runtime_root"], owners[i]["runtime_root"]);
        assert_eq!(fixture.status(i)["host_replacement_count"], 0);
    }
    fm.stopping = true;
    for index in [0, 3, 1] {
        if index == 3 {
            assert!(application(&endpoint, "write 4")
                .await
                .contains("committed=4"));
        } else if index == 1 {
            if let Ok(reply) = tokio::time::timeout(
                Duration::from_millis(250),
                application(&endpoint, "write 5"),
            )
            .await
            {
                assert!(
                    !reply.starts_with("OK"),
                    "dropped replicas cannot vote: {reply}"
                );
            }
        }
        let node = fixture.configs[index].node.identity.native().unwrap();
        let delete = DeleteReplica::new(
            AddInstance {
                unit: restored.unit.clone(),
                service: initial.configuration.service.clone(),
                replica: restored
                    .replicas
                    .0
                    .iter()
                    .find(|r| r.node == node)
                    .unwrap()
                    .clone(),
            },
            false,
        );
        fm.send(index, delete_replica_message(&delete, &generation).unwrap())
            .await;
        fm.until(&mut fixture, |fm| fm.deleted[index]).await;
    }
    for index in 0..4 {
        fixture.stop_node(&mut fm, index).await;
    }
    for link in fm.links.iter().flatten() {
        link.close().await.unwrap();
    }
    fixture.finish(vec![true; 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_sequential_spares_restore_latest_membership_without_reopening_survivors() {
    for (target, minimum, lose_primary, second_lost) in [
        (3, 3, true, 1),
        (4, 4, true, 3),
        (3, 3, false, 3),
        (4, 4, false, 0),
        (3, 2, true, 3),
        (4, 2, true, 1),
        (4, 3, true, 3),
        (5, 3, true, 5),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let mut fixture = Fixture::with_node_pool(address, target, minimum, target + 2);
        fixture.start();
        let mut fm = FmFixture::accept(listener, &mut fixture).await;
        fm.until(&mut fixture, |fm| {
            fm.bootstrapped.iter().all(|ready| *ready)
        })
        .await;
        rollover(&mut fm, &mut fixture).await;
        let mut generation = fm.configurations[0].generation().unwrap();
        let body = primary(&fixture.configs[0]);
        fm.send(0, add_primary_message(&body, &generation).unwrap())
            .await;
        fm.until(&mut fixture, |fm| fm.primary.is_some()).await;
        let original = fm.primary.clone().unwrap();
        for destination in 1..target as usize {
            let request = idle_request(&body, &fixture.configs[destination]);
            fm.send(0, add_replica_message(&request, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.builds.len() == destination)
                .await;
        }
        let mut replicas = vec![original.replica.clone()];
        replicas[0].previous_role = 4;
        for built in &fm.builds {
            let mut r = built.replica.clone();
            r.previous_role = 2;
            r.current_role = 3;
            replicas.push(r);
        }
        let mut unit = body.unit.clone();
        unit.previous = unit.current;
        unit.current.configuration += 1;
        let request = DoReconfiguration {
            configuration: Configuration {
                unit,
                service: body.service.clone(),
                replicas: ObjectArray(replicas),
            },
            phase0_duration: i64::MAX,
        };
        fm.send(
            0,
            do_reconfiguration_message(&request, &generation).unwrap(),
        )
        .await;
        fm.until(&mut fixture, |fm| fm.configured.len() == 1).await;
        let mut membership = fm.configured[0].clone();
        let endpoint = original.replica.service_endpoint.clone();
        let deadline = Instant::now() + Duration::from_secs(8);
        while !application(&endpoint, "read").await.starts_with("OK") {
            assert!(Instant::now() < deadline, "{}", fixture.diagnostics());
            fm.step().await;
        }
        assert!(application(&endpoint, "write")
            .await
            .contains("committed=1"));
        let spare = target as usize;
        assert_eq!(fixture.status(spare)["replica_phase"], Value::Null);
        rollover(&mut fm, &mut fixture).await;
        generation = fm.configurations[0].generation().unwrap();
        let owners: Vec<_> = (0..fixture.configs.len())
            .map(|i| fixture.status(i))
            .collect();
        let mut selected = 0;
        let mut endpoint = endpoint;
        let mut previous: Option<(AddReplica, DoReconfiguration)> = None;
        for (cycle, lost) in [
            if lose_primary { 0 } else { target as usize - 1 },
            second_lost,
        ]
        .into_iter()
        .enumerate()
        {
            let spare = target as usize + cycle;
            let was_primary = selected == lost;
            let pid = owners[lost]["application_pid"].as_u64().unwrap();
            assert!(Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            fm.until(&mut fixture, |fm| fm.losses.contains(&lost)).await;
            let old = membership
                .replicas
                .0
                .iter()
                .find(|r| r.node == fixture.configs[lost].node.identity.native().unwrap())
                .unwrap()
                .clone();
            let delete = DeleteReplica::new(
                AddInstance {
                    unit: membership.unit.clone(),
                    service: body.service.clone(),
                    replica: old.clone(),
                },
                false,
            );
            fm.send(lost, delete_replica_message(&delete, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.deleted[lost]).await;
            selected = fixture
                .configs
                .iter()
                .position(|config| {
                    membership.replicas.0.iter().any(|r| {
                        r.node == config.node.identity.native().unwrap()
                            && r.node != old.node
                            && r.current_role == if was_primary { 3 } else { 4 }
                            && r.up
                    })
                })
                .unwrap();
            if was_primary {
                let before = fm.configured.len();
                let mut config = Configuration {
                    unit: membership.unit.clone(),
                    service: body.service.clone(),
                    replicas: membership.replicas.clone(),
                };
                config.unit.previous = config.unit.current;
                config.unit.current.configuration += 0x1_0000_0001;
                for r in &mut config.replicas.0 {
                    r.previous_role = r.current_role;
                    if r.node == old.node {
                        r.current_role = if minimum == target { 3 } else { 1 };
                        r.up = false;
                        r.state = 3;
                    } else if r.node == fixture.configs[selected].node.identity.native().unwrap() {
                        r.current_role = 4;
                    }
                    r.first_lsn = -1;
                    r.last_lsn = -1;
                }
                fm.send(
                    selected,
                    do_reconfiguration_message(
                        &DoReconfiguration {
                            configuration: config,
                            phase0_duration: i64::MAX,
                        },
                        &generation,
                    )
                    .unwrap(),
                )
                .await;
                fm.until(&mut fixture, |fm| fm.configured.len() == before + 1)
                    .await;
                membership = fm.configured.last().unwrap().clone();
            }
            endpoint = membership
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint
                .clone();
            assert!(application(&endpoint, "read").await.contains("committed=1"));

            let mut build = idle_request(&body, &fixture.configs[spare]);
            build.unit = membership.unit.clone();
            let before = fm.builds.len();
            fm.hold_build = true;
            fm.send(selected, add_replica_message(&build, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.held_build.is_some()).await;
            let held = fm.held_build.take().unwrap();
            assert_eq!(held.replica.current_role, 2);
            assert_eq!(fixture.status(spare)["replica_role"], 2);
            fm.hold_build = false;
            fm.send(selected, add_replica_message(&build, &generation).unwrap())
                .await;
            fm.until(&mut fixture, |fm| fm.builds.len() == before + 1)
                .await;
            assert_eq!(fm.builds.last(), Some(&held));

            let mut replacement = Configuration {
                unit: membership.unit.clone(),
                service: body.service.clone(),
                replicas: membership.replicas.clone(),
            };
            replacement.unit.previous = replacement.unit.current;
            replacement.unit.current.configuration += 1;
            for r in &mut replacement.replicas.0 {
                r.previous_role = r.current_role;
                if r.node == old.node {
                    r.current_role = 1;
                    r.up = false;
                    r.state = 3;
                }
            }
            let mut fresh = held.replica;
            fresh.previous_role = 2;
            fresh.current_role = 3;
            replacement.replicas.0.push(fresh);
            assert_eq!(
                replacement
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.previous_role >= 3)
                    .count(),
                target as usize - usize::from(was_primary && minimum < target)
            );
            assert_eq!(
                replacement
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.current_role >= 3)
                    .count(),
                target as usize
            );
            let request = DoReconfiguration {
                configuration: replacement,
                phase0_duration: i64::MAX,
            };
            let before = fm.configured.len();
            fm.hold_configuration = true;
            fm.send(
                selected,
                do_reconfiguration_message(&request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
                .await;
            let held = fm.held_configuration.take().unwrap();
            fm.hold_configuration = false;
            if let Some((old_build, old_request)) = &previous {
                fm.send(
                    selected,
                    add_replica_message(old_build, &generation).unwrap(),
                )
                .await;
                fm.send(
                    selected,
                    do_reconfiguration_message(old_request, &generation).unwrap(),
                )
                .await;
            }
            fm.send(
                selected,
                do_reconfiguration_message(&request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == before + 1)
                .await;
            assert_eq!(fm.configured.last(), Some(&held));
            assert_eq!(held.replicas.0.len(), target as usize);
            assert!(held.replicas.0.iter().all(|r| r.node != old.node && r.up));
            assert_eq!((held.unit.target, held.unit.minimum), (target, minimum));
            assert!(application(&endpoint, "read")
                .await
                .contains("committed=1 value=acknowledged"));
            for (i, owner) in owners.iter().enumerate() {
                if !fm.losses.contains(&i) {
                    assert_eq!(
                        fixture.status(i)["application_pid"],
                        owner["application_pid"]
                    );
                    assert_eq!(fixture.status(i)["runtime_root"], owner["runtime_root"]);
                }
                assert_eq!(fixture.status(i)["host_replacement_count"], 0);
            }
            membership = held;
            previous = Some((build, request));
        }
        if target == 3 && minimum == 2 {
            let original_primary = selected;
            let mut stale = vec![previous.as_ref().unwrap().1.clone()];
            for (round, destination) in [spare + 1, original_primary].into_iter().enumerate() {
                let current = Configuration {
                    unit: membership.unit.clone(),
                    service: body.service.clone(),
                    replicas: membership.replicas.clone(),
                };
                let (request, reply) = planned_handoff(
                    &mut fm,
                    &mut fixture,
                    &current,
                    destination,
                    &generation,
                    &stale,
                )
                .await;
                endpoint = reply
                    .replicas
                    .0
                    .iter()
                    .find(|r| r.current_role == 4)
                    .unwrap()
                    .service_endpoint
                    .clone();
                assert!(application(&endpoint, &format!("write {}", round + 2))
                    .await
                    .contains(&format!("committed={}", round + 2)));
                membership = reply;
                selected = destination;
                stale.push(request);
            }
        }
        // The surviving originals need the spare's vote to form a majority.
        let quorum_losses = (0..=target as usize)
            .filter(|i| {
                *i != selected
                    && membership
                        .replicas
                        .0
                        .iter()
                        .any(|r| r.node == fixture.configs[*i].node.identity.native().unwrap())
            })
            .take(spare - (spare / 2 + 1))
            .collect::<Vec<_>>();
        for second_loss in quorum_losses {
            let pid = owners[second_loss]["application_pid"].as_u64().unwrap();
            assert!(Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            fm.until(&mut fixture, |fm| fm.losses.contains(&second_loss))
                .await;
        }
        let repeated_handoff = target == 3 && minimum == 2;
        assert!(application(
            &endpoint,
            if repeated_handoff {
                "write 4"
            } else {
                "write-more"
            }
        )
        .await
        .contains(if repeated_handoff {
            "committed=4 value=write-4"
        } else {
            "committed=2 value=continued"
        }));
        let pid = owners[spare + 1]["application_pid"].as_u64().unwrap();
        assert!(Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success());
        fm.until(&mut fixture, |fm| fm.losses.contains(&(spare + 1)))
            .await;
        if let Ok(reply) = tokio::time::timeout(
            Duration::from_millis(250),
            application(
                &endpoint,
                if repeated_handoff {
                    "write 5"
                } else {
                    "write-final"
                },
            ),
        )
        .await
        {
            assert!(!reply.starts_with("OK"));
        }
        fm.stopping = true;
        for index in 0..fixture.configs.len() {
            fixture.stop_node(&mut fm, index).await;
        }
        for link in fm.links.iter().flatten() {
            link.close().await.unwrap();
        }
        fixture.finish(vec![true; spare + 2]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_configured_sizes_reject_mismatched_native_descriptions_before_open() {
    for service_mismatch in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let mut fixture = Fixture::with_sizes(address, 3, 3);
        fixture.start();
        let mut fm = FmFixture::accept(listener, &mut fixture).await;
        fm.until(&mut fixture, |fm| {
            fm.bootstrapped.iter().all(|ready| *ready)
        })
        .await;
        let mut body = primary(&fixture.configs[1]);
        if service_mismatch {
            body.service.minimum = 2;
        } else {
            body.unit.minimum = 2;
        }
        let generation = fm.configurations[1].generation().unwrap();
        fm.expected_loss = Some(1);
        fm.send(1, add_primary_message(&body, &generation).unwrap())
            .await;
        until_node(&mut fm, &fixture, 1, |status| status["error"].is_string()).await;
        assert!(fixture.status(1)["error"]
            .as_str()
            .unwrap()
            .contains("replica-set settings are immutable"));
        assert!(fixture.status(1)["replica_phase"].is_null());
        assert!(fm.primary.is_none());
        fm.stopping = true;
        for index in 0..fixture.configs.len() {
            fixture.stop_node(&mut fm, index).await;
        }
        for link in fm.links.iter().flatten() {
            link.close().await.unwrap();
        }
        fixture.finish([true, false, true]);
    }
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
async fn standalone_generation_rollover_during_primary_failover_preserves_sf_inventory() {
    journey(Fault::FailoverGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_repair_lfum_activates_fresh_host_and_preserves_quorum() {
    journey(Fault::RepairGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_repeated_failover_and_repair_lfum_preserves_latest_membership_and_quorum() {
    journey(Fault::RepeatedRepairGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_repair_deactivation_keeps_lfum_close_and_configuration_completion_separate() {
    journey(Fault::DeactivatedRepairGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_cancelled_repair_build_keeps_lfum_removal_deletion_and_uploads_independent() {
    for fault in [
        Fault::CancelledCopyGenerationHandshake,
        Fault::CancelledBuildGenerationHandshake,
    ] {
        journey(fault).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_restores_a_fresh_original_replica_before_repeated_primary_failover() {
    journey(Fault::Restore).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_restores_after_forced_predecessor_group_cleanup() {
    journey(Fault::RestoreForced).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_restoration_outlives_bounded_diagnostic_history() {
    journey(Fault::RestoreRepeated).await;
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
async fn standalone_generation_rollover_during_secondary_admission_preserves_sf_inventory() {
    journey(Fault::AdmittingGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_loss_before_deletion_retains_uncertainty_but_allows_local_node_close() {
    journey(Fault::LostPeer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_generation_rollover_during_idle_build_preserves_sf_inventory() {
    journey(Fault::BuildingGenerationHandshake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_wrong_generation_refuses_new_authority() {
    journey(Fault::Generation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_stale_generation_is_not_mistaken_for_channel_loss() {
    journey(Fault::StaleGeneration).await;
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
    LostPeer,
    Generation,
    StaleGeneration,
    Resize,
    Promotion,
    ForeignInstance,
    Failover,
    Restore,
    RestoreForced,
    RestoreRepeated,
    GenerationHandshake,
    ReplicatedGenerationHandshake,
    AdmittingGenerationHandshake,
    BuildingGenerationHandshake,
    FailoverGenerationHandshake,
    RepairGenerationHandshake,
    RepeatedRepairGenerationHandshake,
    DeactivatedRepairGenerationHandshake,
    CancelledCopyGenerationHandshake,
    CancelledBuildGenerationHandshake,
}

async fn start_rollover(fm: &mut FmFixture) {
    fm.uploaded.fill(None);
    for index in 0..fm.configurations.len() {
        start_rollover_on(fm, index).await;
    }
}

async fn start_rollover_on(fm: &mut FmFixture, index: usize) {
    let mut generation = fm.configurations[index].generation().unwrap();
    generation.number.generation += 1;
    let link = fm.links[index].as_ref().unwrap();
    let reply = link.generation_proposal(&generation).await.unwrap();
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

async fn finish_rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    fm.until(fixture, |fm| fm.uploaded.iter().all(Option::is_some))
        .await;
    let deadline = Instant::now() + Duration::from_secs(3);
    while (0..fixture.configs.len()).any(|index| {
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

async fn rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    start_rollover(fm).await;
    finish_rollover(fm, fixture).await;
}

async fn admitting_rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    hold_superseding_rollover(fm, fixture).await;
    acknowledge_rollover(fm, fixture).await;
}

async fn hold_superseding_rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    hold_superseding_rollover_with_deactivation(fm, fixture, None).await;
}

async fn hold_superseding_rollover_with_deactivation(
    fm: &mut FmFixture,
    fixture: &mut Fixture,
    deactivate: Option<usize>,
) {
    let before: Vec<_> = (0..fixture.configs.len())
        .map(|index| fixture.status(index))
        .collect();
    hold_rollover(fm, fixture).await;
    let frozen = fm.uploaded.clone();
    let old = std::mem::take(&mut fm.held_uploads);
    hold_rollover(fm, fixture).await;
    for (before, after) in frozen.iter().zip(&fm.uploaded) {
        assert_eq!(
            before.as_ref().unwrap().units,
            after.as_ref().unwrap().units
        );
    }
    if let Some(index) = deactivate {
        let request = NodeActivationRequest {
            sequence: 1,
            from_fmm: false,
        };
        fm.deactivation_requests[index] = Some(request.sequence);
        let generation = fm.configurations[index].generation().unwrap();
        fm.send(
            index,
            node_deactivate_request_message(&request, &generation).unwrap(),
        )
        .await;
        until_node(fm, fixture, index, |status| {
            status["replica_phase"] == "Closing"
        })
        .await;
        assert_eq!(fixture.status(index)["generations"]["pending_uploads"], 2);
        assert_eq!(fm.deactivation_replies[index], None);
    }
    for (index, mut incoming) in old {
        let body = decode_lfum_upload(&incoming.message).unwrap();
        let header = GenerationHeader {
            number: body.generation,
            for_fm_replica: false,
        };
        let link = fm.links[index].as_ref().unwrap();
        incoming
            .reply
            .take()
            .unwrap()
            .reply(
                link.seal(lfum_upload_reply_message(&header).unwrap())
                    .unwrap(),
                wire::deadline(),
            )
            .await
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while (0..fixture.configs.len())
        .any(|i| fixture.status(i)["generations"]["pending_uploads"] != 1)
    {
        assert!(
            Instant::now() < deadline,
            "stale upload completion: {}",
            fixture.diagnostics()
        );
        fm.step().await;
    }
    for (index, before) in before.iter().enumerate() {
        assert_eq!(
            fixture.status(index)["generations"]["send"],
            before["generations"]["send"]
        );
        assert_eq!(
            fixture.status(index)["local_configuration_complete"],
            before["local_configuration_complete"]
        );
    }
}

async fn hold_rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    fm.hold_uploads = true;
    start_rollover(fm).await;
    fm.until(fixture, |fm| fm.uploaded.iter().all(Option::is_some))
        .await;
}

async fn acknowledge_rollover(fm: &mut FmFixture, fixture: &mut Fixture) {
    fm.hold_uploads = false;
    for (index, mut incoming) in std::mem::take(&mut fm.held_uploads) {
        let link = fm.links[index].as_ref().unwrap();
        incoming
            .reply
            .take()
            .unwrap()
            .reply(
                link.seal(
                    lfum_upload_reply_message(&fm.configurations[index].generation().unwrap())
                        .unwrap(),
                )
                .unwrap(),
                wire::deadline(),
            )
            .await
            .unwrap();
    }
    finish_rollover(fm, fixture).await;
}

async fn restore_original(
    fixture: &mut Fixture,
    fm: &mut FmFixture,
    predecessor: &AddInstance,
    completed: &ConfigurationReply,
    destination: usize,
    primary_node: usize,
    forced: bool,
) -> ConfigurationReply {
    let generation = fm.configurations[primary_node].generation().unwrap();
    let old_pid = fixture.status(destination)["application_pid"].clone();
    fm.deleted[destination] = false;
    fm.send(
        destination,
        delete_replica_message(&DeleteReplica::new(predecessor.clone(), false), &generation)
            .unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.deleted[destination]).await;
    let next = completed
        .replicas
        .0
        .iter()
        .map(|replica| replica.id.max(replica.instance))
        .chain(std::iter::once(
            predecessor.replica.id.max(predecessor.replica.instance),
        ))
        .max()
        .unwrap()
        + 1;
    let mut replica = predecessor.replica.clone();
    replica.id = next;
    replica.instance = next;
    replica.previous_role = 1;
    replica.current_role = 2;
    replica.state = 1;
    replica.up = true;
    replica.first_lsn = -1;
    replica.last_lsn = -1;
    replica.service_endpoint.clear();
    replica.replication_endpoint.clear();
    let request = AddReplica {
        unit: completed.unit.clone(),
        service: predecessor.service.clone(),
        replica,
    };
    fm.send(
        primary_node,
        add_replica_message(&request, &generation).unwrap(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut retry_at = Instant::now() + Duration::from_millis(250);
    let built = loop {
        if let Some(built) = fm.builds.iter().rev().find(|built| {
            built.unit == request.unit && built.replica.id == next && built.replica.instance == next
        }) {
            break built.clone();
        }
        assert!(
            Instant::now() < deadline,
            "fresh replica build: {}",
            fixture.diagnostics()
        );
        fm.step().await;
        if Instant::now() >= retry_at {
            fm.send(
                primary_node,
                add_replica_message(&request, &generation).unwrap(),
            )
            .await;
            retry_at = Instant::now() + Duration::from_millis(250);
        }
    };
    assert_eq!(built.replica.id, next);
    assert_eq!(built.replica.instance, next);
    until_node(fm, fixture, destination, |status| {
        status["replica_id"] == next
    })
    .await;
    let status = fixture.status(destination);
    let retired = status["retired_applications"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_ne!(status["application_pid"], old_pid);
    assert_eq!(retired["pid"], old_pid);
    for field in ["reaped", "group_drained", "root_removed"] {
        assert_eq!(retired[field], true, "{status}");
    }
    assert_eq!(retired["forced"], forced);
    fm.retired_replicas.push((
        destination,
        predecessor.replica.id,
        predecessor.replica.instance,
    ));
    let mut unit = completed.unit.clone();
    unit.previous = unit.current;
    unit.current.configuration += 1;
    let mut replicas = completed.replicas.clone();
    for replica in &mut replicas.0 {
        replica.previous_role = replica.current_role;
    }
    let mut replica = built.replica;
    replica.previous_role = 2;
    replica.current_role = 3;
    replicas.0.push(replica);
    let request = DoReconfiguration {
        configuration: Configuration {
            unit,
            replicas,
            service: predecessor.service.clone(),
        },
        phase0_duration: i64::MAX,
    };
    let replies = fm.configured.len();
    fm.send(
        primary_node,
        do_reconfiguration_message(&request, &generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.configured.len() == replies + 1)
        .await;
    let completed = fm.configured.last().unwrap().clone();
    assert_eq!(completed.replicas.0.len(), fixture.configs.len());
    fm.configurations[destination].node.replica_id = next;
    fm.configurations[destination].node.replica_instance = next;
    fm.losses.retain(|lost| *lost != destination);
    fm.deleted[destination] = false;
    completed
}

async fn remove_lost_secondary(
    fixture: &mut Fixture,
    fm: &mut FmFixture,
    completed: &ConfigurationReply,
    service: &Service,
    destination: usize,
    primary_node: usize,
) -> (AddInstance, ConfigurationReply) {
    let predecessor = AddInstance {
        unit: completed.unit.clone(),
        service: service.clone(),
        replica: completed
            .replicas
            .0
            .iter()
            .find(|replica| {
                replica.node == fixture.configs[destination].node.identity.native().unwrap()
            })
            .unwrap()
            .clone(),
    };
    let pid = fixture.status(destination)["application_pid"]
        .as_u64()
        .unwrap();
    assert!(Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap()
        .success());
    fm.until(fixture, |fm| fm.losses.contains(&destination))
        .await;
    let mut configuration = Configuration {
        unit: completed.unit.clone(),
        service: service.clone(),
        replicas: completed.replicas.clone(),
    };
    configuration.unit.previous = configuration.unit.current;
    configuration.unit.current.configuration += 1;
    for replica in &mut configuration.replicas.0 {
        replica.previous_role = replica.current_role;
        if replica.node == predecessor.replica.node {
            replica.current_role = 1;
            replica.up = false;
            replica.state = 3;
        }
    }
    let generation = fm.configurations[primary_node].generation().unwrap();
    let replies = fm.configured.len();
    fm.send(
        primary_node,
        do_reconfiguration_message(
            &DoReconfiguration {
                configuration,
                phase0_duration: i64::MAX,
            },
            &generation,
        )
        .unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.configured.len() == replies + 1)
        .await;
    let removed = fm.configured.last().unwrap().clone();
    assert_eq!(removed.replicas.0.len(), 2);
    (predecessor, removed)
}

async fn until_node(
    fm: &mut FmFixture,
    fixture: &Fixture,
    index: usize,
    condition: impl Fn(&Value) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while !condition(&fixture.status(index)) {
        assert!(
            Instant::now() < deadline,
            "Node progress: {}",
            fixture.diagnostics()
        );
        fm.step().await;
    }
}

enum RepairLfumOutcome {
    Completed(ConfigurationReply),
    Interrupted,
}

async fn cancel_repair_with_lfum(
    fm: &mut FmFixture,
    fixture: &mut Fixture,
    destination: usize,
    primary_index: usize,
    owners: &[Value],
    delayed_build: Option<AddReplicaReply>,
) {
    let build = fm.repair_build.as_ref().unwrap().clone();
    let previous = fm.repair_previous.as_ref().unwrap().clone();
    let endpoint = previous
        .replicas
        .0
        .iter()
        .find(|r| r.current_role == 4)
        .unwrap()
        .service_endpoint
        .clone();
    let work = fixture.work(primary_index);
    let fresh = fixture.status(destination);
    let configured = fm.configured.len();
    let builds = fm.builds.len();
    hold_rollover(fm, fixture).await;
    let frozen = fm.uploaded.clone();
    let old_uploads = std::mem::take(&mut fm.held_uploads);
    hold_rollover(fm, fixture).await;
    assert_eq!(
        fm.uploaded
            .iter()
            .map(|u| &u.as_ref().unwrap().units)
            .collect::<Vec<_>>(),
        frozen
            .iter()
            .map(|u| &u.as_ref().unwrap().units)
            .collect::<Vec<_>>()
    );
    acknowledge_rollover(fm, fixture).await;
    fm.repair_cancelled = true;
    fm.hold_removal = true;
    let generation = fm.configurations[primary_index].generation().unwrap();
    fm.send(
        primary_index,
        remove_replica_message(&build, &generation).unwrap(),
    )
    .await;
    if delayed_build.is_none() {
        until_node(fm, fixture, primary_index, |_| {
            work.join("local-copy-state.retired").exists()
        })
        .await;
        assert_eq!(
            fs::read_to_string(work.join("local-copy-state.retired")).unwrap(),
            "cancelled"
        );
        assert!(!work.join("local-copy-state.release").exists());
        fm.send(
            primary_index,
            remove_replica_message(&build, &generation).unwrap(),
        )
        .await;
    }
    fm.until(fixture, |fm| fm.held_removal.is_some()).await;
    assert!(fm.removals.is_empty());
    fm.repair_deleting = true;
    fm.hold_deletion = true;
    fm.send(
        destination,
        delete_replica_message(&DeleteReplica::new(build.clone(), false), &generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.held_deletion.is_some()).await;
    until_node(fm, fixture, destination, |status| {
        status["replica_phase"] == "Closed"
    })
    .await;
    let fresh_work = fixture.work(destination);
    for resource in ["local-instance.resource", "local-provider.resource"] {
        assert!(
            !fresh_work.join(resource).exists(),
            "native Delete must dispose {resource}"
        );
    }
    assert!(
        !fm.deleted[destination],
        "disposal cannot replace lost DeleteReplicaReply"
    );
    fm.hold_deletion = false;
    fm.send(
        destination,
        delete_replica_message(&DeleteReplica::new(build.clone(), false), &generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.deleted[destination]).await;
    assert!(
        fm.removals.is_empty(),
        "Delete/disposal cannot complete Primary removal"
    );
    hold_rollover(fm, fixture).await;
    assert_eq!(
        fm.uploaded[primary_index].as_ref().unwrap().units.0[0]
            .replicas
            .0
            .len(),
        2
    );
    assert_ne!(
        fm.uploaded[destination].as_ref().unwrap().units,
        frozen[destination].as_ref().unwrap().units
    );
    let send: Vec<_> = (0..3)
        .map(|i| fixture.status(i)["generations"]["send"].clone())
        .collect();
    for (index, mut incoming) in old_uploads {
        let body = decode_lfum_upload(&incoming.message).unwrap();
        assert_eq!(Some(&body), frozen[index].as_ref());
        let header = GenerationHeader {
            number: body.generation,
            for_fm_replica: false,
        };
        incoming
            .reply
            .take()
            .unwrap()
            .reply(
                fm.links[index]
                    .as_ref()
                    .unwrap()
                    .seal(lfum_upload_reply_message(&header).unwrap())
                    .unwrap(),
                wire::deadline(),
            )
            .await
            .unwrap();
    }
    for (index, expected) in send.iter().enumerate() {
        until_node(fm, fixture, index, |status| {
            status["generations"]["pending_uploads"] == 1
        })
        .await;
        assert_eq!(&fixture.status(index)["generations"]["send"], expected);
    }
    acknowledge_rollover(fm, fixture).await;
    assert!(
        fm.removals.is_empty(),
        "LFUM ACK cannot replace RemoveReplicaReply"
    );
    let removed = fm.held_removal.take().unwrap();
    fm.removals.push(removed.clone());
    fm.hold_removal = false;
    if let Some(built) = delayed_build {
        assert_eq!(built.replica.id, build.replica.id);
        assert_eq!(built.replica.instance, build.replica.instance);
        assert_eq!(built.error, 0);
    }
    let generation = fm.configurations[primary_index].generation().unwrap();
    fm.send(
        primary_index,
        add_replica_message(&build, &generation).unwrap(),
    )
    .await;
    fm.send(
        primary_index,
        remove_replica_message(&build, &generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.removals.len() == 2).await;
    assert_eq!(fm.removals[1], removed);
    assert_eq!(fm.configured.len(), configured);
    assert_eq!(fm.builds.len(), builds);
    assert_eq!(
        fixture.status(destination)["application_pid"],
        fresh["application_pid"]
    );
    assert_eq!(
        fixture.status(destination)["runtime_root"],
        fresh["runtime_root"]
    );
    assert_eq!(
        fixture.status(destination)["host_replacement_count"],
        fresh["host_replacement_count"]
    );
    assert!(
        PathBuf::from(fresh["runtime_root"].as_str().unwrap()).exists(),
        "replica disposal is not app-host retirement"
    );
    let survivor = (0..3)
        .find(|i| *i != primary_index && *i != destination)
        .unwrap();
    for index in [primary_index, survivor] {
        assert_eq!(
            fixture.status(index)["application_pid"],
            owners[index]["application_pid"]
        );
        assert_eq!(
            fixture.status(index)["runtime_root"],
            owners[index]["runtime_root"]
        );
        assert_eq!(fixture.status(index)["control_unknown"], false);
    }
    assert!(application(&endpoint, "read")
        .await
        .contains("value=acknowledged"));
    assert!(application(&endpoint, "write-more")
        .await
        .contains("committed=2"));
    let pid = fixture.status(survivor)["application_pid"]
        .as_u64()
        .unwrap();
    assert!(Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap()
        .success());
    fm.until(fixture, |fm| fm.losses.contains(&survivor)).await;
    if let Ok(reply) = tokio::time::timeout(
        Duration::from_millis(250),
        application(&endpoint, "write-final"),
    )
    .await
    {
        assert!(
            !reply.starts_with("OK"),
            "cancelled Idle cannot supply a quorum vote"
        );
    }
}

async fn repair_with_lfum(
    fm: &mut FmFixture,
    fixture: &mut Fixture,
    destination: usize,
    primary_index: usize,
    previous: &ConfigurationReply,
    predecessor: &AddInstance,
    fault: Fault,
) -> RepairLfumOutcome {
    let deactivate = fault == Fault::DeactivatedRepairGenerationHandshake;
    let cancel_copy = fault == Fault::CancelledCopyGenerationHandshake;
    let cancel_build = cancel_copy || fault == Fault::CancelledBuildGenerationHandshake;
    let owners: Vec<_> = (0..3).map(|i| fixture.status(i)).collect();
    let mut generation = fm.configurations[primary_index].generation().unwrap();
    fm.send(
        destination,
        delete_replica_message(&DeleteReplica::new(predecessor.clone(), false), &generation)
            .unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.deleted[destination]).await;
    let replacement_count = owners[destination]["host_replacement_count"]
        .as_u64()
        .unwrap();
    assert_eq!(
        fixture.status(destination)["host_replacement_count"],
        replacement_count
    );
    let next = fm
        .configurations
        .iter()
        .map(|config| config.node.replica_id.max(config.node.replica_instance))
        .max()
        .unwrap()
        + 1;
    let mut build = predecessor.clone();
    build.unit = previous.unit.clone();
    build.replica = Replica {
        node: fixture.configs[destination].node.identity.native().unwrap(),
        id: next,
        instance: next,
        current_role: 2,
        state: 1,
        version: build.service.version.clone(),
        ..Replica::default()
    };
    fm.hold_build = true;
    let primary_work = fixture.work(primary_index);
    if cancel_copy {
        fs::write(
            primary_work.join("local-copy-state.block"),
            b"hold public provider stream",
        )
        .unwrap();
    }
    fm.send(
        primary_index,
        add_replica_message(&build, &generation).unwrap(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut retry_at = Instant::now() + Duration::from_millis(250);
    while if cancel_copy {
        !primary_work.join("local-copy-state.held").exists()
            || fixture.status(destination)["replica_id"] != next
    } else {
        fm.held_build.is_none()
    } {
        assert!(
            Instant::now() < deadline,
            "repair build: {}",
            fixture.diagnostics()
        );
        fm.step().await;
        if Instant::now() >= retry_at {
            fm.send(
                primary_index,
                add_replica_message(&build, &generation).unwrap(),
            )
            .await;
            retry_at = Instant::now() + Duration::from_millis(250);
        }
    }
    let built = fm.held_build.take();
    fm.hold_build = false;
    if let Some(built) = &built {
        assert_eq!(built.replica.id, next);
        assert_eq!(built.replica.instance, next);
        assert_eq!(built.replica.state, 2);
    }
    let fresh = fixture.status(destination);
    assert_ne!(
        fresh["application_pid"],
        owners[destination]["application_pid"]
    );
    assert_ne!(fresh["runtime_root"], owners[destination]["runtime_root"]);
    assert!(!PathBuf::from(owners[destination]["runtime_root"].as_str().unwrap()).exists());
    assert!(!PathBuf::from(format!("/proc/{}", owners[destination]["application_pid"])).exists());
    assert_eq!(fresh["host_replacement_count"], replacement_count + 1);
    fm.retired_replicas.push((
        destination,
        predecessor.replica.id,
        predecessor.replica.instance,
    ));
    fm.configurations[destination].node.replica_id = next;
    fm.configurations[destination].node.replica_instance = next;
    fm.deleted[destination] = false;
    fm.losses.retain(|i| *i != destination);
    fm.repair_previous = Some(previous.clone());
    fm.repair_build = Some(build.clone());
    if cancel_build {
        cancel_repair_with_lfum(fm, fixture, destination, primary_index, &owners, built).await;
        return RepairLfumOutcome::Interrupted;
    }
    let built = built.expect("completed repair build");
    let secondary_work = fixture.work(destination);
    let primary_work = fixture.work(primary_index);
    fs::write(
        secondary_work.join("local-secondary-role.block"),
        b"hold repair role",
    )
    .unwrap();
    fs::write(
        primary_work.join("local-current-configuration.block"),
        b"hold final repair RAP",
    )
    .unwrap();
    fm.repair_previous = Some(previous.clone());
    fm.repair_build = Some(build.clone());
    until_node(fm, fixture, destination, |status| {
        status["replica_id"] == next
            && status["replica_phase"] == "Ready"
            && status["local_configuration_complete"] == false
    })
    .await;
    for index in 0..3 {
        if index != destination {
            until_node(fm, fixture, index, |status| {
                status["replica_epoch"]["configuration"] == previous.unit.current.configuration
                    && status["local_configuration_complete"] == true
            })
            .await;
        }
    }
    hold_superseding_rollover(fm, fixture).await;
    let frozen_build = fm.uploaded.clone();
    let survivor = (0..3)
        .find(|i| *i != destination && *i != primary_index)
        .unwrap();
    assert_eq!(
        frozen_build[destination].as_ref().unwrap().units.0[0]
            .replicas
            .0
            .len(),
        1
    );
    assert_eq!(
        frozen_build[primary_index].as_ref().unwrap().units.0[0]
            .replicas
            .0
            .len(),
        3
    );
    assert_eq!(
        frozen_build[survivor].as_ref().unwrap().units.0[0]
            .replicas
            .0
            .len(),
        2
    );
    let mut request = Configuration {
        unit: previous.unit.clone(),
        service: build.service.clone(),
        replicas: previous.replicas.clone(),
    };
    request.unit.previous = previous.unit.current;
    request.unit.current.configuration += 1;
    for r in &mut request.replicas.0 {
        r.previous_role = r.current_role;
    }
    let mut replica = built.replica.clone();
    replica.previous_role = 2;
    replica.current_role = 3;
    request.replicas.0.push(replica);
    let configuration = DoReconfiguration {
        configuration: request.clone(),
        phase0_duration: i64::MAX,
    };
    fm.repair_membership = Some(request.clone());
    generation = fm.configurations[primary_index].generation().unwrap();
    let completed_before = fm.configured.len();
    fm.send(
        primary_index,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    until_node(fm, fixture, primary_index, |status| {
        status["local_configuration_stage"] == "Deactivate"
    })
    .await;
    assert_eq!(
        fixture.status(primary_index)["generations"]["upload_pending"],
        true
    );
    assert_eq!(fm.configured.len(), completed_before);
    assert_eq!(fm.uploaded, frozen_build);
    acknowledge_rollover(fm, fixture).await;
    fm.send(
        primary_index,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    until_node(fm, fixture, primary_index, |status| {
        status["local_configuration_stage"] == "Activate"
            && secondary_work.join("local-secondary-role.held").exists()
    })
    .await;
    until_node(fm, fixture, survivor, |status| {
        status["replica_epoch"]["configuration"] == request.unit.current.configuration
            && status["local_configuration_complete"] == true
    })
    .await;
    hold_superseding_rollover_with_deactivation(fm, fixture, deactivate.then_some(destination))
        .await;
    let frozen = fm.uploaded.clone();
    assert_eq!(
        frozen[primary_index].as_ref().unwrap().units.0[0].intermediate,
        request.unit.current
    );
    assert_eq!(
        frozen[destination].as_ref().unwrap().units.0[0].intermediate,
        Epoch::default()
    );
    if deactivate {
        for resource in ["local-instance.resource", "local-provider.resource"] {
            assert!(
                secondary_work.join(resource).exists(),
                "held role still owns {resource}"
            );
        }
        assert_eq!(fixture.status(destination)["replica_phase"], "Closing");
        assert_eq!(fm.deactivation_replies[destination], None);
        acknowledge_rollover(fm, fixture).await;
        assert_eq!(fixture.status(destination)["replica_phase"], "Closing");
        assert_eq!(fm.deactivation_replies[destination], None);
        assert_eq!(fm.configured.len(), completed_before);
        assert_eq!(fm.uploaded, frozen);
        assert!(!secondary_work.join("local-secondary-role.release").exists());
        for resource in ["local-instance.resource", "local-provider.resource"] {
            assert!(
                secondary_work.join(resource).exists(),
                "LFUM ACK cannot dispose {resource}"
            );
        }
        let endpoint = &previous
            .replicas
            .0
            .iter()
            .find(|r| r.current_role == 4)
            .unwrap()
            .service_endpoint;
        assert!(application(endpoint, "write-more")
            .await
            .contains("committed=2 value=continued"));
        fs::write(
            secondary_work.join("local-secondary-role.release"),
            b"finish original role so native Close can drain",
        )
        .unwrap();
        fm.until_with_budget(fixture, Duration::from_secs(40), |fm| {
            fm.deactivation_replies[destination].is_some()
        })
        .await;
        until_node(fm, fixture, destination, |status| {
            status["replica_phase"] == "Closed"
        })
        .await;
        for resource in ["local-instance.resource", "local-provider.resource"] {
            assert!(
                !secondary_work.join(resource).exists(),
                "native Close must dispose {resource}"
            );
        }
        assert_eq!(fm.configured.len(), completed_before);
        assert!(!primary_work
            .join("local-current-configuration.held")
            .exists());
        assert_eq!(
            fixture.status(destination)["host_replacement_count"],
            replacement_count + 1
        );
        assert!(
            PathBuf::from(fresh["runtime_root"].as_str().unwrap()).exists(),
            "replica Close is not app-host retirement"
        );
        for index in [primary_index, survivor] {
            assert_eq!(
                fixture.status(index)["application_pid"],
                owners[index]["application_pid"]
            );
            assert_eq!(
                fixture.status(index)["runtime_root"],
                owners[index]["runtime_root"]
            );
            assert_eq!(fixture.status(index)["control_unknown"], false);
        }
        let pid = fixture.status(survivor)["application_pid"]
            .as_u64()
            .unwrap();
        assert!(Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success());
        fm.until(fixture, |fm| fm.losses.contains(&survivor)).await;
        if let Ok(reply) = tokio::time::timeout(
            Duration::from_millis(250),
            application(endpoint, "write-final"),
        )
        .await
        {
            assert!(
                !reply.starts_with("OK"),
                "Primary alone cannot use the interrupted replica's vote"
            );
        }
        return RepairLfumOutcome::Interrupted;
    }
    fs::write(
        secondary_work.join("local-secondary-role.release"),
        b"finish Secondary before LFUM ACK",
    )
    .unwrap();
    until_node(fm, fixture, destination, |status| {
        status["local_configuration_complete"] == true
    })
    .await;
    assert_eq!(
        fixture.status(destination)["generations"]["upload_pending"],
        true
    );
    assert_eq!(fm.configured.len(), completed_before);
    assert_eq!(fm.uploaded, frozen);
    acknowledge_rollover(fm, fixture).await;
    generation = fm.configurations[primary_index].generation().unwrap();
    fm.send(
        primary_index,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    until_node(fm, fixture, primary_index, |status| {
        status["local_configuration_stage"] == "End"
            && primary_work
                .join("local-current-configuration.held")
                .exists()
    })
    .await;
    assert_eq!(fm.configured.len(), completed_before);
    fs::write(
        primary_work.join("local-current-configuration.release"),
        b"finish final repair RAP",
    )
    .unwrap();
    fm.until(fixture, |fm| fm.configured.len() == completed_before + 1)
        .await;
    let repaired = fm.configured.last().unwrap().clone();
    assert_eq!(repaired.replicas.0.len(), 3);
    assert_eq!(repaired.unit.previous, Epoch::default());
    fm.send(
        primary_index,
        add_replica_message(&build, &generation).unwrap(),
    )
    .await;
    fm.send(
        primary_index,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    fm.until(fixture, |fm| fm.configured.len() == completed_before + 2)
        .await;
    assert_eq!(fm.configured.last().unwrap(), &repaired);
    let after = fixture.status(destination);
    assert_eq!(after["host_replacement"], fresh["host_replacement"]);
    assert_eq!(after["host_replacement_count"], replacement_count + 1);
    assert_eq!(after["application_pid"], fresh["application_pid"]);
    for (index, owner) in owners.iter().enumerate() {
        if index != destination {
            assert_eq!(
                fixture.status(index)["application_pid"],
                owner["application_pid"]
            );
            assert_eq!(fixture.status(index)["runtime_root"], owner["runtime_root"]);
        }
        assert_eq!(fixture.status(index)["control_unknown"], false);
    }
    let endpoint = &repaired
        .replicas
        .0
        .iter()
        .find(|r| r.current_role == 4)
        .unwrap()
        .service_endpoint;
    assert!(application(endpoint, "read")
        .await
        .contains("committed=2 value=continued"));
    fm.repair_previous = None;
    fm.repair_build = None;
    fm.repair_membership = None;
    RepairLfumOutcome::Completed(repaired)
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
    let mut pids: Vec<_> = (0..3)
        .map(|index| fixture.status(index)["application_pid"].clone())
        .collect();
    if fault == Fault::BuildingGenerationHandshake {
        let mut builds = vec![];
        for destination in [0, 2] {
            fs::write(
                fixture.work(destination).join("local-copy.block"),
                b"hold actual copy",
            )
            .unwrap();
            let build = idle_request(&body, &fixture.configs[destination]);
            fm.send(selected, add_replica_message(&build, &generation).unwrap())
                .await;
            builds.push(build);
        }
        let deadline = Instant::now() + Duration::from_secs(8);
        while [0, 2]
            .iter()
            .any(|index| !fixture.work(*index).join("local-copy.held").exists())
        {
            assert!(
                Instant::now() < deadline,
                "copy barriers: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        assert!(fm.builds.is_empty());
        fm.initial_builds = Some(builds.clone());
        let authority = application(&endpoint, "read").await;
        hold_superseding_rollover(&mut fm, &mut fixture).await;
        assert_eq!(application(&endpoint, "read").await, authority);
        let primary_upload = &fm.uploaded[selected].as_ref().unwrap().units.0[0];
        for index in [0, 2] {
            let idle = &fm.uploaded[index].as_ref().unwrap().units.0[0].replicas.0[0];
            assert_eq!(
                (idle.description.current_role, idle.description.state),
                (2, 1)
            );
            assert!(primary_upload.replicas.0.contains(idle));
        }
        assert!(fm.builds.is_empty() && fm.configured.is_empty());
        fm.initial_builds = None;
        generation = fm.configurations[selected].generation().unwrap();
        for destination in [0, 2] {
            fs::write(
                fixture.work(destination).join("local-copy.release"),
                b"finish actual copy",
            )
            .unwrap();
        }
        fm.until(&mut fixture, |fm| fm.builds.len() == 2).await;
        assert_eq!(fm.builds.len(), 2);
    } else {
        for destination in [0, 2] {
            let build = idle_request(&body, &fixture.configs[destination]);
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
    if fault == Fault::AdmittingGenerationHandshake {
        for index in [0, 2] {
            fs::write(
                fixture.work(index).join("local-secondary-role.block"),
                b"hold role",
            )
            .unwrap();
        }
        fm.initial_membership = Some(configuration.configuration.clone());
    }
    fm.hold_configuration = true;
    fm.send(
        selected,
        do_reconfiguration_message(&configuration, &generation).unwrap(),
    )
    .await;
    if fault == Fault::BuildingGenerationHandshake {
        let deadline = Instant::now() + Duration::from_secs(8);
        while fixture.status(selected)["local_configuration_stage"] != "Activate" {
            assert!(
                Instant::now() < deadline,
                "configuration before LFUM ACK: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        assert_eq!(
            fixture.status(selected)["generations"]["upload_pending"],
            true
        );
        assert!(fm.held_configuration.is_none());
        acknowledge_rollover(&mut fm, &mut fixture).await;
        fm.send(
            selected,
            do_reconfiguration_message(&configuration, &generation).unwrap(),
        )
        .await;
    }
    if fault == Fault::AdmittingGenerationHandshake {
        let deadline = Instant::now() + Duration::from_secs(8);
        while [0, 2].iter().any(|index| {
            !fixture
                .work(*index)
                .join("local-secondary-role.held")
                .exists()
        }) {
            assert!(
                Instant::now() < deadline,
                "Secondary role barriers: {}",
                fixture.diagnostics()
            );
            fm.step().await;
        }
        assert!(fm.held_configuration.is_none());
        let authority = application(&endpoint, "read").await;
        admitting_rollover(&mut fm, &mut fixture).await;
        assert_eq!(application(&endpoint, "read").await, authority);
        generation = fm.configurations[selected].generation().unwrap();
        for index in [0, 2] {
            fs::write(
                fixture.work(index).join("local-secondary-role.release"),
                b"release role",
            )
            .unwrap();
        }
    }
    fm.until(&mut fixture, |fm| fm.held_configuration.is_some())
        .await;
    fm.initial_membership = None;
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
    if matches!(
        fault,
        Fault::Failover
            | Fault::Restore
            | Fault::RestoreForced
            | Fault::RestoreRepeated
            | Fault::FailoverGenerationHandshake
            | Fault::RepairGenerationHandshake
            | Fault::RepeatedRepairGenerationHandshake
            | Fault::DeactivatedRepairGenerationHandshake
            | Fault::CancelledCopyGenerationHandshake
            | Fault::CancelledBuildGenerationHandshake
    ) {
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
        let candidate = 0;
        if fault == Fault::FailoverGenerationHandshake {
            for barrier in ["local-primary-role", "local-current-configuration"] {
                fs::write(
                    fixture.work(candidate).join(format!("{barrier}.block")),
                    b"hold actual failover",
                )
                .unwrap();
            }
        }
        let lost_pid = pids[selected].as_u64().unwrap();
        let mut helper = (fault == Fault::RestoreForced).then(|| GroupHelper::join(lost_pid));
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
        if fault == Fault::FailoverGenerationHandshake {
            fm.failover = Some(failover.configuration.clone());
            fm.failover_previous = Some(original.clone());
        }
        fm.send(
            candidate,
            do_reconfiguration_message(&failover, &generation).unwrap(),
        )
        .await;
        if fault == Fault::FailoverGenerationHandshake {
            let deadline = Instant::now() + Duration::from_secs(8);
            while !fixture
                .work(candidate)
                .join("local-primary-role.held")
                .exists()
            {
                assert!(
                    Instant::now() < deadline,
                    "Primary callback: {}",
                    fixture.diagnostics()
                );
                fm.step().await;
            }
            admitting_rollover(&mut fm, &mut fixture).await;
            assert_eq!(fm.configured.len(), 1);
            fs::write(
                fixture.work(candidate).join("local-primary-role.release"),
                b"finish Primary role",
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(8);
            while !fixture
                .work(candidate)
                .join("local-current-configuration.held")
                .exists()
            {
                assert!(
                    Instant::now() < deadline,
                    "final RAP configuration: {}",
                    fixture.diagnostics()
                );
                fm.step().await;
            }
            fm.failover_ending = true;
            admitting_rollover(&mut fm, &mut fixture).await;
            assert_eq!(fm.configured.len(), 1);
            generation = fm.configurations[candidate].generation().unwrap();
            fs::write(
                fixture
                    .work(candidate)
                    .join("local-current-configuration.release"),
                b"finish final RAP configuration",
            )
            .unwrap();
        }
        fm.until(&mut fixture, |fm| fm.configured.len() == 2).await;
        fm.failover = None;
        fm.failover_previous = None;
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
        if !matches!(
            fault,
            Fault::DeactivatedRepairGenerationHandshake
                | Fault::CancelledCopyGenerationHandshake
                | Fault::CancelledBuildGenerationHandshake
        ) {
            assert!(application(&new_primary.service_endpoint, "write-more")
                .await
                .starts_with("OK"));
            assert!(application(&new_primary.service_endpoint, "read")
                .await
                .contains("value=continued"));
        }
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
        if matches!(
            fault,
            Fault::RepairGenerationHandshake
                | Fault::RepeatedRepairGenerationHandshake
                | Fault::DeactivatedRepairGenerationHandshake
                | Fault::CancelledCopyGenerationHandshake
                | Fault::CancelledBuildGenerationHandshake
        ) {
            let mut repaired = match repair_with_lfum(
                &mut fm,
                &mut fixture,
                selected,
                candidate,
                &promoted,
                &deletion_bodies[selected],
                fault,
            )
            .await
            {
                RepairLfumOutcome::Completed(repaired) => repaired,
                RepairLfumOutcome::Interrupted => {
                    let replies = fm.configured.len();
                    fm.stopping = true;
                    for index in 0..3 {
                        fixture.stop_node(&mut fm, index).await;
                    }
                    assert_eq!(
                        fm.configured.len(),
                        replies,
                        "owned shutdown cannot complete interrupted repair"
                    );
                    if fault == Fault::DeactivatedRepairGenerationHandshake {
                        assert!(
                            fixture.status(candidate)["cleanup"]["pending_work"]
                                .as_u64()
                                .unwrap()
                                > 0
                        );
                    }
                    fixture.finish([true; 3]);
                    return;
                }
            };
            generation = fm.configurations[candidate].generation().unwrap();
            pids[selected] = fixture.status(selected)["application_pid"].clone();
            let mut repaired_primary = candidate;
            if fault == Fault::RepeatedRepairGenerationHandshake {
                for barrier in ["local-primary-role", "local-current-configuration"] {
                    fs::write(
                        fixture.work(selected).join(format!("{barrier}.block")),
                        b"hold repeated failover",
                    )
                    .unwrap();
                }
                let predecessor = AddInstance {
                    unit: repaired.unit.clone(),
                    service: body.service.clone(),
                    replica: repaired
                        .replicas
                        .0
                        .iter()
                        .find(|r| {
                            r.node == fixture.configs[candidate].node.identity.native().unwrap()
                        })
                        .unwrap()
                        .clone(),
                };
                let old = fixture.status(candidate);
                let pid = old["application_pid"].as_u64().unwrap();
                assert!(Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .status()
                    .unwrap()
                    .success());
                fm.until(&mut fixture, |fm| fm.losses.contains(&candidate))
                    .await;
                let mut request = Configuration {
                    unit: repaired.unit.clone(),
                    service: body.service.clone(),
                    replicas: repaired.replicas.clone(),
                };
                request.unit.previous = request.unit.current;
                request.unit.current.configuration += 0x1_0000_0001;
                for replica in &mut request.replicas.0 {
                    replica.previous_role = replica.current_role;
                    if replica.node == predecessor.replica.node {
                        replica.current_role = 1;
                        replica.up = false;
                        replica.state = 3;
                    } else if replica.node
                        == fixture.configs[selected].node.identity.native().unwrap()
                    {
                        replica.current_role = 4;
                    }
                    replica.first_lsn = -1;
                    replica.last_lsn = -1;
                }
                fm.failover = Some(request.clone());
                fm.failover_previous = Some(repaired.clone());
                fm.failover_ending = false;
                let replies = fm.configured.len();
                fm.send(
                    selected,
                    do_reconfiguration_message(
                        &DoReconfiguration {
                            configuration: request.clone(),
                            phase0_duration: i64::MAX,
                        },
                        &generation,
                    )
                    .unwrap(),
                )
                .await;
                until_node(&mut fm, &fixture, selected, |_| {
                    fixture
                        .work(selected)
                        .join("local-primary-role.held")
                        .exists()
                })
                .await;
                admitting_rollover(&mut fm, &mut fixture).await;
                assert_eq!(fm.configured.len(), replies);
                fs::write(
                    fixture.work(selected).join("local-primary-role.release"),
                    b"finish repeated promotion",
                )
                .unwrap();
                until_node(&mut fm, &fixture, selected, |_| {
                    fixture
                        .work(selected)
                        .join("local-current-configuration.held")
                        .exists()
                })
                .await;
                until_node(&mut fm, &fixture, 2, |status| {
                    status["replica_epoch"]["configuration"] == request.unit.current.configuration
                        && status["local_configuration_complete"] == true
                })
                .await;
                fm.failover_ending = true;
                admitting_rollover(&mut fm, &mut fixture).await;
                assert_eq!(fm.configured.len(), replies);
                fs::write(
                    fixture
                        .work(selected)
                        .join("local-current-configuration.release"),
                    b"finish repeated RAP",
                )
                .unwrap();
                fm.until(&mut fixture, |fm| fm.configured.len() == replies + 1)
                    .await;
                fm.failover = None;
                fm.failover_previous = None;
                fm.failover_ending = false;
                let promoted = fm.configured.last().unwrap().clone();
                for suffix in ["block", "held", "release"] {
                    fs::remove_file(
                        fixture
                            .work(selected)
                            .join(format!("local-current-configuration.{suffix}")),
                    )
                    .unwrap();
                }
                let RepairLfumOutcome::Completed(completed) = repair_with_lfum(
                    &mut fm,
                    &mut fixture,
                    candidate,
                    selected,
                    &promoted,
                    &predecessor,
                    Fault::None,
                )
                .await
                else {
                    panic!("repeated repair unexpectedly deactivated");
                };
                repaired = completed;
                pids[candidate] = fixture.status(candidate)["application_pid"].clone();
                repaired_primary = selected;
                generation = fm.configurations[selected].generation().unwrap();
                assert_eq!(fixture.status(selected)["host_replacement_count"], 1);
                assert_eq!(fixture.status(candidate)["host_replacement_count"], 1);
            }
            for (index, deletion) in deletion_bodies.iter_mut().enumerate() {
                deletion.unit = repaired.unit.clone();
                deletion.replica = repaired
                    .replicas
                    .0
                    .iter()
                    .find(|r| r.node == fixture.configs[index].node.identity.native().unwrap())
                    .unwrap()
                    .clone();
            }
            let survivor = (0..3).find(|i| *i != selected && *i != candidate).unwrap();
            let pid = fixture.status(survivor)["application_pid"]
                .as_u64()
                .unwrap();
            assert!(Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            fm.until(&mut fixture, |fm| fm.losses.contains(&survivor))
                .await;
            let endpoint = &repaired
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .unwrap()
                .service_endpoint;
            assert_eq!(
                repaired
                    .replicas
                    .0
                    .iter()
                    .find(|r| r.current_role == 4)
                    .unwrap()
                    .node,
                fixture.configs[repaired_primary]
                    .node
                    .identity
                    .native()
                    .unwrap()
            );
            assert!(application(endpoint, "write-final")
                .await
                .contains("committed=3 value=final"));
        }
        if matches!(
            fault,
            Fault::Restore | Fault::RestoreForced | Fault::RestoreRepeated
        ) {
            let mut retired_pids = vec![pids[selected].clone()];
            let mut restored = restore_original(
                &mut fixture,
                &mut fm,
                &deletion_bodies[selected],
                &promoted,
                selected,
                candidate,
                fault == Fault::RestoreForced,
            )
            .await;
            if let Some(helper) = helper.as_mut() {
                assert_eq!(helper.0.wait().unwrap().signal(), Some(9));
            }
            if fault == Fault::RestoreRepeated {
                for cycle in 1..33 {
                    retired_pids.push(fixture.status(selected)["application_pid"].clone());
                    let (old, removed) = remove_lost_secondary(
                        &mut fixture,
                        &mut fm,
                        &restored,
                        &configuration.configuration.service,
                        selected,
                        candidate,
                    )
                    .await;
                    restored = restore_original(
                        &mut fixture,
                        &mut fm,
                        &old,
                        &removed,
                        selected,
                        candidate,
                        false,
                    )
                    .await;
                    assert_eq!(
                        fixture.status(selected)["host_replacement_count"],
                        cycle + 1
                    );
                    assert_eq!(fixture.status(selected)["control_unknown"], false);
                }
                let status = fixture.status(selected);
                assert_eq!(status["host_replacement_count"], 33);
                let retained: Vec<_> = status["retired_applications"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|application| application["pid"].clone())
                    .collect();
                assert_eq!(retained, retired_pids[1..]);
            }
            let endpoint = restored
                .replicas
                .0
                .iter()
                .find(|replica| replica.current_role == 4)
                .unwrap()
                .service_endpoint
                .clone();
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let state: Value =
                    serde_json::from_str(&application(&endpoint, "state").await).unwrap();
                let last = state["provider"]["last"].as_i64().unwrap();
                if state["peers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|peer| peer["applied"].as_i64().unwrap() >= last)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "restored peers did not apply: {state}"
                );
                fm.step().await;
            }
            let pid = fixture.status(candidate)["application_pid"]
                .as_u64()
                .unwrap();
            assert!(Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            fm.until(&mut fixture, |fm| fm.losses.contains(&candidate))
                .await;
            let mut unit = restored.unit.clone();
            unit.previous = unit.current;
            unit.current.configuration += 0x1_0000_0001;
            let mut replicas = restored.replicas.clone();
            for replica in &mut replicas.0 {
                replica.previous_role = replica.current_role;
                if replica.node == fixture.configs[candidate].node.identity.native().unwrap() {
                    replica.current_role = 1;
                    replica.up = false;
                    replica.state = 3;
                } else if replica.node == fixture.configs[selected].node.identity.native().unwrap()
                {
                    replica.current_role = 4;
                }
                replica.first_lsn = -1;
                replica.last_lsn = -1;
            }
            let request = DoReconfiguration {
                configuration: Configuration {
                    unit,
                    replicas,
                    service: configuration.configuration.service.clone(),
                },
                phase0_duration: i64::MAX,
            };
            let replies = fm.configured.len();
            fm.send(
                selected,
                do_reconfiguration_message(&request, &generation).unwrap(),
            )
            .await;
            fm.until(&mut fixture, |fm| fm.configured.len() == replies + 1)
                .await;
            let completed = fm.configured.last().unwrap();
            let primary = completed
                .replicas
                .0
                .iter()
                .find(|replica| replica.current_role == 4)
                .unwrap();
            assert!(application(&primary.service_endpoint, "read")
                .await
                .contains("value=continued"));
            assert!(application(&primary.service_endpoint, "write-final")
                .await
                .starts_with("OK"));
            assert!(application(&primary.service_endpoint, "read")
                .await
                .contains("value=final"));
            for (index, deletion) in deletion_bodies.iter_mut().enumerate() {
                let membership = if index == candidate {
                    &restored
                } else {
                    completed
                };
                deletion.unit = membership.unit.clone();
                deletion.replica = membership
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
    if fault == Fault::LostPeer {
        fm.stopping = true;
        fixture.stop_node(&mut fm, 0).await;
        fixture.await_control_loss(&mut fm, selected).await;
        fixture.await_control_loss(&mut fm, 2).await;
        for index in [selected, 2] {
            let status = fixture.status(index);
            assert_eq!(status["application_pid"], pids[index]);
            assert_eq!(status["stage"], "running");
            assert!(status["error"].as_str().unwrap().contains("peer"));
            assert!(TcpStream::connect(fixture.configs[index].node.address)
                .await
                .is_err());
        }
        for index in [selected, 2] {
            fixture.stop_node(&mut fm, index).await;
            let status = fixture.status(index);
            assert_eq!(status["control_unknown"], true);
            assert!(status["cleanup"]["control_error"].is_string());
            assert_eq!(status["cleanup"]["error"], Value::Null);
        }
        assert_eq!(fm.deleted, [false; 3], "node Close is not native deletion");
        fixture.finish([true; 3]);
        return;
    }
    let refusal = !matches!(
        fault,
        Fault::None
            | Fault::Failover
            | Fault::Restore
            | Fault::RestoreForced
            | Fault::RestoreRepeated
            | Fault::GenerationHandshake
            | Fault::ReplicatedGenerationHandshake
            | Fault::AdmittingGenerationHandshake
            | Fault::BuildingGenerationHandshake
            | Fault::FailoverGenerationHandshake
            | Fault::RepairGenerationHandshake
            | Fault::RepeatedRepairGenerationHandshake
            | Fault::DeactivatedRepairGenerationHandshake
    );
    if refusal {
        if fault == Fault::LostFm {
            fm.links[selected].take().unwrap().close().await.unwrap();
        } else {
            let mut invalid = configuration.clone();
            let mut header = generation.clone();
            match fault {
                Fault::Generation => header.number.generation += 1,
                Fault::StaleGeneration => {
                    header.number.generation -= 1;
                    fm.expected_loss = Some(selected);
                }
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
            Fault::StaleGeneration => "foreign/stale/duplicate",
            Fault::Resize => "immutable for this service",
            Fault::Promotion | Fault::ForeignInstance => "only initial configured admission",
            Fault::None
            | Fault::LostPeer
            | Fault::Failover
            | Fault::Restore
            | Fault::RestoreForced
            | Fault::RestoreRepeated
            | Fault::GenerationHandshake
            | Fault::ReplicatedGenerationHandshake
            | Fault::AdmittingGenerationHandshake
            | Fault::BuildingGenerationHandshake
            | Fault::FailoverGenerationHandshake
            | Fault::RepairGenerationHandshake
            | Fault::RepeatedRepairGenerationHandshake
            | Fault::DeactivatedRepairGenerationHandshake
            | Fault::CancelledCopyGenerationHandshake
            | Fault::CancelledBuildGenerationHandshake => unreachable!(),
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
        if !matches!(
            fault,
            Fault::Restore | Fault::RestoreForced | Fault::RestoreRepeated
        ) || index != selected
        {
            assert_eq!(&fixture.status(index)["application_pid"], pid);
        }
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
    fixture.stop_node(&mut fm, 0).await;
    fixture.await_control_loss(&mut fm, selected).await;
    fixture.await_control_loss(&mut fm, 2).await;
    for index in [selected, 2] {
        fixture.stop_node(&mut fm, index).await;
        assert_eq!(fixture.status(index)["control_unknown"], true);
        assert!(fixture.status(index)["cleanup"]["control_error"].is_string());
    }
    for link in fm.links.iter().flatten() {
        link.close().await.unwrap();
    }
    let mut expected = [true; 3];
    expected[selected] = !refusal || fault == Fault::LostFm;
    if fault == Fault::LostFm {
        let status = fixture.status(selected);
        assert!(status["cleanup"]["control_error"]
            .as_str()
            .unwrap()
            .contains("FM Channel"));
        assert_eq!(status["cleanup"]["error"], Value::Null);
    }
    fixture.finish(expected);
}

#[test]
fn invalid_static_topology_rejects_before_acquiring_a_run_directory() {
    let fixture = Fixture::new("127.0.0.1:19001".parse().unwrap());
    let original = fixture.configs[0].clone();
    for case in 0..11 {
        let mut config = original.clone();
        match case {
            0 => config.peers[0].identity = config.node.identity.clone(),
            1 => config.peers[0].address.set_ip(*config.node.address.ip()),
            2 => config.node.address.set_ip(Ipv4Addr::UNSPECIFIED),
            3 => config.fm.generation = 0,
            4 => config.schema_version = 2,
            5 => config.target = 0,
            6 => config.minimum = 0,
            7 => config.minimum = config.target + 1,
            8 => config.target = 65,
            9 => {
                config.peers.pop();
            }
            _ => config.peers.push(config.peers[0].clone()),
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

#[test]
fn standalone_requires_explicit_sizes_and_matching_package_before_acquiring_a_run_directory() {
    let fixture = Fixture::new("127.0.0.1:19001".parse().unwrap());
    for field in ["target", "minimum"] {
        let mut json = serde_json::to_value(&fixture.configs[0]).unwrap();
        json.as_object_mut().unwrap().remove(field);
        let path = fixture.root.join(format!("missing-{field}.json"));
        fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();
        let output = Command::new(support::binary("fube-node", "FUBE_NODE_EXE"))
            .arg("--config")
            .arg(path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr).unwrap().contains(field));
        assert!(!fixture.configs[0].run_directory.exists());
    }
    let mut config = fixture.configs[0].clone();
    config.minimum = 3;
    config.validate().unwrap();
    let path = fixture.root.join("package-mismatch.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = Command::new(support::binary("fube-node", "FUBE_NODE_EXE"))
        .arg("--config")
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("package target/minimum must match"));
    assert!(!config.run_directory.exists());
    fs::remove_dir_all(fixture.root).unwrap();
}
