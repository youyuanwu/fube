//! The CLI boundary is exercised with a real foreground executable, two real
//! executable nodes and the real Rust runtime listener. Expected identities are
//! captured from original launch/activation metadata before create, not copied
//! from an endpoint response. Library tests add original-capability fault cases.
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread::JoinHandle,
    time::{Duration, Instant},
};
mod support;

struct Foreground {
    child: Child,
    stdin: ChildStdin,
    messages: Receiver<Value>,
    reader: Option<JoinHandle<()>>,
}
impl Foreground {
    fn start(root: &Path) -> Self {
        Self::start_profile(root, false)
    }
    fn start_profile(root: &Path, stateful: bool) -> Self {
        Self::start_options(root, stateful, None, false)
    }
    fn start_options(
        root: &Path,
        stateful: bool,
        sizes: Option<(i32, i32)>,
        manual_recovery: bool,
    ) -> Self {
        let app = if stateful {
            support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP")
        } else {
            support::binary("cluster-stateless-app", "FUBE_CLUSTER_APP")
        };
        let mut command = Command::new(support::binary("fube-cluster", "FUBE_CLUSTER_NODE_EXE"));
        command
            .arg("--root")
            .arg(root)
            .arg("--app")
            .arg(app)
            .env_clear()
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some((target, minimum)) = sizes {
            command.args([
                "--stateful",
                "--target-replica-set-size",
                &target.to_string(),
                "--min-replica-set-size",
                &minimum.to_string(),
            ]);
        } else if stateful {
            command.arg("--stateful-singleton");
        }
        if manual_recovery {
            command.arg("--manual-recovery");
        }
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, messages) = mpsc::sync_channel(8);
        let reader = std::thread::spawn(move || {
            let mut reader = BufReader::with_capacity(4096, stdout);
            loop {
                let mut bytes = vec![];
                let count = reader
                    .by_ref()
                    .take(65537)
                    .read_until(b'\n', &mut bytes)
                    .unwrap();
                if count == 0 {
                    break;
                }
                assert!(
                    count <= 65536 && bytes.last() == Some(&b'\n'),
                    "unbounded CLI output"
                );
                let message = serde_json::from_slice(&bytes).unwrap();
                if sender.send(message).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            messages,
            reader: Some(reader),
        }
    }
    fn read(&self) -> Value {
        self.messages
            .recv_timeout(Duration::from_secs(40))
            .expect("foreground bounded response")
    }
    fn command(&mut self, value: &Value) -> Value {
        serde_json::to_writer(&mut self.stdin, value).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
        self.read()
    }
    fn finish(&mut self) -> Value {
        let pending = self.command(&json!({"op":"quit","action":"quit"}));
        assert_eq!(pending["state"], "pending");
        let receipt = self.read();
        assert_eq!(receipt["event"], "shutdown");
        assert_eq!(receipt["clean"], true, "{receipt}");
        assert!(self.child.wait().unwrap().success());
        self.reader.take().unwrap().join().unwrap();
        receipt
    }
}
impl Drop for Foreground {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            let _ = self
                .stdin
                .write_all(b"{\"op\":\"guard-quit\",\"action\":\"quit\"}\n");
            let end = Instant::now() + Duration::from_secs(30);
            while self.child.try_wait().unwrap().is_none() && Instant::now() < end {
                std::thread::sleep(Duration::from_millis(10));
            }
            if self.child.try_wait().unwrap().is_none() {
                // This is the original Child capability, not a reconstructed
                // numeric process/group owner. Children retain PDEATHSIG.
                self.child.kill().unwrap();
                self.child.wait().unwrap();
            }
        }
        if let Some(reader) = self.reader.take() {
            // Drop the receiving side before joining a potentially full queue.
            let (_, empty) = mpsc::sync_channel(1);
            self.messages = empty;
            reader.join().unwrap();
        }
    }
}
fn metadata(path: PathBuf) -> Value {
    let bytes = fs::read(path).unwrap();
    assert!(bytes.len() <= 65536);
    serde_json::from_slice(&bytes).unwrap()
}
fn contact(endpoint: &str) -> Value {
    let mut socket =
        TcpStream::connect_timeout(&endpoint.parse().unwrap(), Duration::from_secs(1)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket.write_all(b"identity\n").unwrap();
    let mut bytes = vec![];
    socket.take(4097).read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() <= 4096 && bytes.last() == Some(&b'\n'));
    serde_json::from_slice(&bytes).unwrap()
}
#[test]
fn foreground_configured_service_builds_four_replicas_from_one_create() {
    let root = support::parent().join(format!("configured-foreground-{}", uuid::Uuid::new_v4()));
    let mut foreground = Foreground::start_options(&root, true, Some((4, 3)), false);
    let ready = foreground.read();
    assert_eq!(ready["event"], "ready", "{ready}");
    assert_eq!(ready["status"]["recovery_mode"], "legacy");
    assert_eq!(ready["status"]["nodes"].as_array().unwrap().len(), 4);
    assert_eq!(ready["status"]["fm"]["separate_process"], true);
    let fm_pid = ready["status"]["fm"]["pid"].as_u64().unwrap() as u32;
    assert_ne!(fm_pid, foreground.child.id());
    assert!(ready["status"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|node| node["node_pid"] != fm_pid));
    let expected: Vec<_> = (1..=4).map(|node| Expected::capture(&root, node)).collect();
    assert_eq!(
        foreground.command(&json!({"op":"create","action":"create","node":1}))["state"],
        "pending"
    );
    let status = poll(&mut foreground, &mut 0, |status| {
        status["operations"][0]["state"] == "complete"
    });
    assert_eq!(status["configuration"]["ready"], true);
    assert_eq!(
        status["replica_set"],
        json!({"target":4,"minimum":3,"immutable":true})
    );
    let endpoint = status["nodes"][0]["endpoint"].as_str().unwrap();
    let mut socket = TcpStream::connect(endpoint).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket.write_all(b"write\n").unwrap();
    let mut response = String::new();
    socket.take(1024).read_to_string(&mut response).unwrap();
    assert!(
        response.contains("committed=1 value=acknowledged"),
        "{response}"
    );
    assert_eq!(
        foreground.command(&json!({"op":"create","action":"create","node":1}))["state"],
        "complete"
    );
    let cleanup = foreground.finish();
    assert_eq!(cleanup["fm"]["pid"], fm_pid);
    assert_eq!(cleanup["fm"]["reaped"], true);
    assert_eq!(cleanup["fm"]["group_drained"], true);
    for owner in expected {
        owner.assert_gone();
    }
    let mut next = Foreground::start_options(&root, true, Some((2, 1)), false);
    assert_eq!(next.read()["event"], "ready");
    next.finish();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn manual_foreground_requires_scoped_recovery_and_replays_only_original_owners() {
    let root = support::parent().join(format!("manual-foreground-{}", uuid::Uuid::new_v4()));
    let mut foreground = Foreground::start_options(&root, true, Some((3, 3)), true);
    let ready = foreground.read();
    assert_eq!(ready["event"], "ready", "{ready}");
    assert_eq!(ready["status"]["recovery_mode"], "manual");
    assert_eq!(ready["status"]["fm"]["separate_process"], true);
    let fm_pid = ready["status"]["fm"]["pid"].as_u64().unwrap() as u32;
    let fm_start = process_start(fm_pid);
    let original: Vec<_> = (1..=3).map(|node| Expected::capture(&root, node)).collect();
    let partition = ready["status"]["nodes"][0]["partition"].clone();
    let create = json!({"op":"create","action":"create-partition","partition":partition,"node":1});
    assert_eq!(foreground.command(&create)["state"], "pending");
    let mut next = 0;
    let running = wait_operation(&mut foreground, &mut next, "create");
    let endpoint = running["nodes"][0]["endpoint"].as_str().unwrap().to_owned();
    poll(&mut foreground, &mut next, |_| {
        data_contact(&endpoint, b"read\n").starts_with("OK ")
    });
    assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
    poll(&mut foreground, &mut next, |status| {
        (1..3).all(|node| {
            status["nodes"][node]["retained_work"]["app_observation"]["idle_data"]["provider"]
                ["last"]
                == 1
        })
    });

    let kill = json!({"op":"kill-primary","action":"kill-node","node":1});
    let killed = foreground.command(&kill);
    assert_eq!(killed["state"], "complete", "{killed}");
    assert_eq!(killed["node_down"], "not_reported");
    let stopped = wait_operation(&mut foreground, &mut next, "kill-primary");
    assert!(stopped["node_failures"].as_array().unwrap().is_empty());
    let down = json!({"op":"down-primary","action":"node-down","node":1});
    assert_eq!(foreground.command(&down)["state"], "down");
    let failed = wait_operation(&mut foreground, &mut next, "down-primary");
    original[0].assert_gone();
    assert_eq!(failed["repair"]["state"], "absent");
    assert_eq!(failed["configuration"]["ready"], false);
    assert_eq!(
        failed["configuration"]["request"],
        running["configuration"]["request"]
    );
    let predecessor = failed["node_failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|failure| failure["node"] == 1 && failure["instance"] == killed["node_instance"])
        .unwrap()["predecessor"]
        .clone();
    let rebuild = json!({"op":"rebuild","action":"rebuild-replica","partition":partition,
        "predecessor":predecessor,"node":1});
    let mut premature = rebuild.clone();
    premature["op"] = json!("premature");
    assert_eq!(foreground.command(&premature)["state"], "rejected");
    let reconfigure = json!({"op":"reconfigure","action":"reconfigure-partition",
        "partition":partition,"predecessor":predecessor});
    assert_eq!(foreground.command(&reconfigure)["state"], "pending");
    let configured = wait_operation(&mut foreground, &mut next, "reconfigure");
    assert_eq!(
        configured["replica_set"],
        json!({"target":3,"minimum":3,"immutable":true})
    );
    let replicas = configured["configuration"]["reply"]["replicas"]
        .as_array()
        .unwrap();
    assert_eq!(replicas.len(), 3);
    let retained = replicas
        .iter()
        .find(|replica| {
            replica["node"]["instance"] == predecessor["node_instance"]
                && replica["id"] == predecessor["replica"]
        })
        .unwrap();
    assert_eq!(retained["up"], false);
    assert_eq!(retained["current_role"], 3);
    let promoted = configured["nodes"][1]["endpoint"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(data_contact(&promoted, b"read\n").contains("committed=1"));
    assert!(data_contact(&promoted, b"write-more\n").contains("committed=2"));

    let restart = json!({"op":"restart-primary","action":"restart-node","node":1});
    let restarted = foreground.command(&restart);
    assert_eq!(restarted["state"], "complete", "{restarted}");
    assert_eq!(restarted["replica_recovery"], "not_requested");
    let infrastructure = wait_operation(&mut foreground, &mut next, "restart-primary");
    assert_eq!(infrastructure["repair"]["state"], "absent");
    assert_eq!(infrastructure["nodes"][0]["observed"], "absent");
    assert_eq!(infrastructure["nodes"][0]["created_lifetime"], false);
    let instance = infrastructure["nodes"][0]["node_instance"]
        .as_u64()
        .unwrap();
    assert!(instance > original[0].launch["node"]["instance"].as_u64().unwrap());
    let fresh =
        Expected::capture_directory(&root.join(format!("deployment/node1-instance-{instance}")));
    assert_eq!(fresh.node_id, original[0].node_id);
    assert_ne!(fresh.node_pid, original[0].node_pid);
    assert_ne!(fresh.app_pid, original[0].app_pid);
    assert_ne!(fresh.launch["session"], original[0].launch["session"]);
    assert_eq!(foreground.command(&kill), killed);
    let mut changed = kill.clone();
    changed["node"] = json!(2);
    assert_eq!(foreground.command(&changed)["state"], "rejected");
    let absent = poll(&mut foreground, &mut next, |status| {
        let node = &status["nodes"][0];
        assert_eq!(node["node_pid"], fresh.node_pid);
        assert_eq!(node["observed"], "absent");
        assert_eq!(status["repair"]["state"], "absent");
        node["retained_work"]["app_observation"]["opens"] == 0
    });
    assert_eq!(absent["nodes"][0]["admitted"], true);

    let building = foreground.command(&rebuild);
    assert_eq!(building["state"], "pending", "{building}");
    assert_eq!(building["node_instance"], instance);
    assert!(building["replica"].as_i64().unwrap() > predecessor["replica"].as_i64().unwrap());
    assert!(
        building["incarnation"].as_i64().unwrap() > predecessor["incarnation"].as_i64().unwrap()
    );
    let restored = wait_operation(&mut foreground, &mut next, "rebuild");
    let completed = foreground.command(&rebuild);
    assert_eq!(completed["state"], "complete");
    assert_eq!(completed["phase"], "ready");
    assert_eq!(completed["replica"], building["replica"]);
    assert_eq!(completed["incarnation"], building["incarnation"]);
    assert_eq!(foreground.command(&kill), killed);
    assert_eq!(foreground.command(&restart), restarted);
    assert_eq!(restored["nodes"][0]["node_pid"], fresh.node_pid);
    assert_eq!(process_start(fresh.node_pid), fresh.node_start);
    assert_eq!(process_start(fresh.app_pid), fresh.app_start);
    for (node, owner) in original.iter().enumerate().skip(1) {
        assert_eq!(restored["nodes"][node]["node_pid"], owner.node_pid);
        assert_eq!(restored["nodes"][node]["app_pid"], owner.app_pid);
        assert_eq!(process_start(owner.node_pid), owner.node_start);
        assert_eq!(process_start(owner.app_pid), owner.app_start);
        assert_eq!(
            restored["nodes"][node]["retained_work"]["app_observation"]["opens"],
            1
        );
    }
    assert_eq!(
        restored["replica_set"],
        json!({"target":3,"minimum":3,"immutable":true})
    );
    assert!(data_contact(&promoted, b"read\n").contains("committed=2 value=continued"));
    poll(&mut foreground, &mut next, |status| {
        status["nodes"][0]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2
    });
    assert_eq!(
        foreground.command(&json!({"op":"kill-secondary","action":"kill-node","node":3}))["state"],
        "complete"
    );
    assert_eq!(
        foreground.command(&json!({"op":"down-secondary","action":"node-down","node":3}))["state"],
        "down"
    );
    wait_operation(&mut foreground, &mut next, "down-secondary");
    original[2].assert_gone();
    assert!(data_contact(&promoted, b"write-final\n").contains("committed=3 value=final"));
    assert_eq!(process_start(fm_pid), fm_start);
    let cleanup = foreground.finish();
    assert_eq!(cleanup["fm"]["pid"], fm_pid);
    assert_eq!(cleanup["fm"]["reaped"], true);
    assert_eq!(cleanup["fm"]["group_drained"], true);
    for owner in original {
        owner.assert_gone();
    }
    fresh.assert_gone();
    assert!(!root.join("deployment").exists());
    fs::remove_dir_all(root).unwrap();
}

fn data_contact(endpoint: &str, request: &[u8]) -> String {
    let mut socket =
        TcpStream::connect_timeout(&endpoint.parse().unwrap(), Duration::from_secs(2)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket.write_all(request).unwrap();
    let mut response = String::new();
    socket.take(1025).read_to_string(&mut response).unwrap();
    assert!(response.len() <= 1024 && response.ends_with('\n'));
    response
}

#[test]
fn foreground_stateful_singleton_recreates_fresh_data_after_verified_fm_deletion() {
    let root = support::parent().join(format!("fm-primary-{}", uuid::Uuid::new_v4()));
    let mut foreground = Foreground::start_profile(&root, true);
    assert_eq!(foreground.read()["event"], "ready");
    let pending = foreground.command(&json!({"op":"primary","action":"create","node":1}));
    assert_eq!(pending["state"], "pending");
    let expected = Expected::capture(&root, 1);
    let mut completed = None;
    for attempt in 0..40 {
        let status =
            foreground.command(&json!({"op":format!("status-{attempt}"),"action":"status"}));
        if status["status"]["nodes"][0]["observed"] == "running" {
            completed = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let status = completed.expect("matched FM AddPrimaryReply did not complete");
    let endpoint = status["status"]["nodes"][0]["endpoint"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = |command: &[u8]| {
        let mut socket = TcpStream::connect(&endpoint).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(command).unwrap();
        let mut response = String::new();
        socket.take(512).read_to_string(&mut response).unwrap();
        response
    };
    let mut ready = false;
    for _ in 0..40 {
        let response = request(b"read\n");
        if response.starts_with("OK ") {
            ready = true;
            break;
        }
        assert_eq!(response, "ERR NotPrimary\n");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(ready);
    let written = request(b"write\n");
    assert_eq!(written, "OK role=Primary epoch=1:4294967297 read=Granted write=Granted ready=true assigned=1 applied=1 committed=1 value=acknowledged\n");
    assert_eq!(request(b"read\n"), written);
    let duplicate = foreground.command(&json!({"op":"primary","action":"create","node":1}));
    assert_eq!(duplicate["state"], "complete");
    assert_eq!(duplicate["replica"], pending["replica"]);
    assert_eq!(duplicate["incarnation"], pending["incarnation"]);
    let delete = foreground.command(&json!({"op":"delete","action":"delete","node":1}));
    assert_eq!(delete["state"], "pending");
    assert_eq!(delete["replica"], pending["replica"]);
    assert_eq!(delete["incarnation"], pending["incarnation"]);
    let mut deleted = None;
    for attempt in 0..40 {
        let status =
            foreground.command(&json!({"op":format!("delete-status-{attempt}"),"action":"status"}));
        if status["status"]["nodes"][0]["observed"] == "absent" {
            deleted = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let status = deleted.expect("DeleteReplicaReply plus original-node disposal did not complete");
    let node = &status["status"]["nodes"][0];
    assert_eq!(node["reusable"], true);
    assert_eq!(node["retired_lifetimes"], 1);
    assert_eq!(node["retirement"]["app_pid"], expected.app_pid);
    assert_eq!(node["retirement"]["node_pid"], expected.node_pid);
    assert!(TcpStream::connect(&endpoint).is_err());
    assert_eq!(
        foreground.command(&json!({"op":"delete","action":"delete","node":1}))["state"],
        "complete"
    );
    let replacement = foreground.command(&json!({"op":"replacement","action":"create","node":1}));
    assert_eq!(replacement["state"], "pending");
    assert_eq!(
        replacement["replica"].as_i64().unwrap(),
        pending["replica"].as_i64().unwrap() + 1
    );
    assert_eq!(
        replacement["incarnation"].as_i64().unwrap(),
        pending["incarnation"].as_i64().unwrap() + 1
    );
    let mut fresh = None;
    for attempt in 0..40 {
        let status =
            foreground.command(&json!({"op":format!("fresh-status-{attempt}"),"action":"status"}));
        if status["status"]["nodes"][0]["observed"] == "running" {
            fresh = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let status = fresh.expect("fresh AddPrimary did not complete under original host");
    let fresh_endpoint = status["status"]["nodes"][0]["endpoint"]
        .as_str()
        .unwrap()
        .to_owned();
    let fresh_request = |command: &[u8]| {
        let mut socket = TcpStream::connect(&fresh_endpoint).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(command).unwrap();
        let mut response = String::new();
        socket.take(512).read_to_string(&mut response).unwrap();
        response
    };
    let end = Instant::now() + Duration::from_secs(2);
    while fresh_request(b"read\n") == "ERR NotPrimary\n" {
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(10));
    }
    let empty = fresh_request(b"read\n");
    assert!(
        empty.contains("assigned=0 applied=0 committed=0 value=\n"),
        "{empty}"
    );
    assert_eq!(fresh_request(b"write\n"), written);
    assert_eq!(fresh_request(b"read\n"), written);
    assert_eq!(process_start(expected.node_pid), expected.node_start);
    assert_eq!(process_start(expected.app_pid), expected.app_start);
    foreground.finish();
    assert!(!Path::new(&format!("/proc/{}", expected.app_pid)).exists());
    assert!(TcpStream::connect(&endpoint).is_err());
    assert!(TcpStream::connect(&fresh_endpoint).is_err());
    fs::remove_dir_all(root).unwrap();
}

struct Expected {
    launch: Value,
    node_pid: u32,
    app_pid: u32,
    node_id: String,
    node_start: String,
    app_start: String,
}
fn process_start(pid: u32) -> String {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    stat.rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .to_string()
}
impl Expected {
    fn capture(root: &Path, node: usize) -> Self {
        Self::capture_directory(&root.join(format!("deployment/node{node}")))
    }
    fn capture_directory(directory: &Path) -> Self {
        let launch = metadata(directory.join("launch.json"));
        let actual = metadata(directory.join("profile.json"));
        let id: [u8; 16] = serde_json::from_value(launch["node"]["id"].clone()).unwrap();
        let high = u64::from_le_bytes(id[..8].try_into().unwrap());
        let low = u64::from_le_bytes(id[8..].try_into().unwrap());
        let node_pid = actual["pid"].as_u64().unwrap() as u32;
        let app_pid = actual["profile"]["app_pid"].as_u64().unwrap() as u32;
        Self {
            launch,
            node_pid,
            app_pid,
            node_id: format!("{high:016x}{low:016x}"),
            node_start: process_start(node_pid),
            app_start: process_start(app_pid),
        }
    }
    fn assert_response(&self, actual: &Value) {
        self.assert_lifetime(actual, 0);
    }
    fn assert_lifetime(&self, actual: &Value, ordinal: i64) {
        assert_eq!(actual["profile"], "fube-cluster-stateless-v1");
        assert_eq!(actual["node"], self.node_id);
        assert_eq!(actual["node_instance"], self.launch["node"]["instance"]);
        assert_eq!(actual["node_pid"], self.node_pid);
        assert_eq!(actual["app_pid"], self.app_pid);
        assert_eq!(actual["partition"], self.launch["partition"]);
        assert_eq!(
            actual["replica"],
            self.launch["replica"].as_i64().unwrap() + ordinal
        );
        assert_eq!(
            actual["incarnation"],
            self.launch["incarnation"].as_i64().unwrap() + ordinal
        );
        assert_eq!(actual["service"], "fabric:/Cluster/Service");
        assert_eq!(actual["service_type"], "ClusterStatelessType");
        assert_eq!(process_start(self.node_pid), self.node_start);
        assert_eq!(process_start(self.app_pid), self.app_start);
    }
    fn assert_gone(&self) {
        assert!(!Path::new(&format!("/proc/{}", self.node_pid)).exists());
        assert!(!Path::new(&format!("/proc/{}", self.app_pid)).exists());
    }
}
fn poll(owner: &mut Foreground, next: &mut usize, predicate: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..40 {
        *next += 1;
        assert!(
            *next < 56,
            "polling must not overrun finite operation retention"
        );
        let response = owner.command(&json!({"op":format!("s{next}"),"action":"status"}));
        assert_eq!(response["state"], "complete", "{response}");
        if predicate(&response["status"]) {
            return response["status"].clone();
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("semantic progress deadline");
}

fn wait_operation(owner: &mut Foreground, next: &mut usize, operation: &str) -> Value {
    poll(owner, next, |status| {
        let outcome = status["operations"]
            .as_array()
            .unwrap()
            .iter()
            .chain(status["node_controls"].as_array().unwrap().iter())
            .find(|outcome| outcome["op"] == operation)
            .unwrap_or_else(|| panic!("operation {operation} is not retained: {status}"));
        match outcome["state"].as_str() {
            Some("complete" | "down") => true,
            Some("pending") => false,
            _ => panic!("operation {operation} did not complete: {outcome}"),
        }
    })
}

#[test]
fn actual_foreground_reuses_both_nodes_with_fresh_instances_and_clean_relaunch() {
    let parent = support::parent();
    assert!(parent.is_absolute() && parent.is_dir());
    let root = parent.join(format!("foreground-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let mut owner = Foreground::start(&root);
    let ready = owner.read();
    assert_eq!(ready["event"], "ready", "{ready}");
    let deployment = ready["status"]["deployment"].clone();
    let one = Expected::capture(&root, 1);
    let two = Expected::capture(&root, 2);
    assert_ne!(one.node_pid, two.node_pid);
    assert_ne!(one.app_pid, two.app_pid);
    assert_ne!(one.node_pid, owner.child.id());
    let fm_start = process_start(owner.child.id());
    for pid in [one.node_pid, two.node_pid, one.app_pid, two.app_pid] {
        let maps = fs::read_to_string(format!("/proc/{pid}/maps")).unwrap();
        assert!(!maps.contains("libFabricRuntime") && !maps.contains("libFabricCommon"));
        let environment = fs::read(format!("/proc/{pid}/environ")).unwrap();
        assert!(!environment
            .split(|byte| *byte == 0)
            .any(|entry| entry.starts_with(b"LD_LIBRARY_PATH=")
                || entry.starts_with(b"FabricConfigFileName=")));
    }
    for node in 1..=2 {
        let response =
            owner.command(&json!({"op":format!("c{node}"),"action":"create","node":node}));
        assert_eq!(
            response["state"], "pending",
            "writes cannot claim lifecycle success"
        );
    }
    let mut next = 0;
    let running = poll(&mut owner, &mut next, |status| {
        status["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["observed"] == "running" && node["catalog_notified"] == true)
    });
    let first = running["nodes"][0]["endpoint"]
        .as_str()
        .unwrap()
        .to_string();
    let second = running["nodes"][1]["endpoint"]
        .as_str()
        .unwrap()
        .to_string();
    one.assert_response(&contact(&first));
    two.assert_response(&contact(&second));
    assert_ne!(
        contact(&second)["partition"],
        one.launch["partition"],
        "swapped endpoint oracle"
    );
    let first_result = owner.command(&json!({"op":"c1","action":"create","node":1}));
    assert_eq!(first_result["state"], "complete");
    assert_eq!(
        owner.command(&json!({"op":"c1","action":"create","node":2}))["state"],
        "rejected"
    );
    assert_eq!(
        owner.command(&json!({"op":"bad","action":"create","node":1,"force":true}))["state"],
        "rejected"
    );
    let snapshot = owner.command(&json!({"op":"snapshot","action":"status"}));
    assert_eq!(
        owner.command(&json!({"op":"snapshot","action":"status"})),
        snapshot
    );
    owner.stdin.write_all(&vec![b'x'; 5000]).unwrap();
    owner.stdin.write_all(b"\n").unwrap();
    assert_eq!(owner.read()["state"], "rejected");
    owner.command(&json!({"op":"d1","action":"delete","node":1}));
    poll(&mut owner, &mut next, |status| {
        status["nodes"][0]["observed"] == "absent"
    });
    assert!(
        TcpStream::connect(&first).is_err(),
        "semantic delete must dispose actual listener"
    );
    two.assert_response(&contact(&second));
    assert_eq!(
        owner.command(&json!({"op":"recreate","action":"create","node":1}))["state"],
        "pending"
    );
    let running = poll(&mut owner, &mut next, |status| {
        status["nodes"][0]["observed"] == "running"
    });
    let new_first = running["nodes"][0]["endpoint"]
        .as_str()
        .unwrap()
        .to_string();
    one.assert_lifetime(&contact(&new_first), 1);
    two.assert_response(&contact(&second));
    assert_eq!(
        owner.command(&json!({"op":"c1","action":"create","node":1})),
        first_result
    );
    assert_eq!(
        owner.command(&json!({"op":"d2","action":"delete","node":2}))["state"],
        "pending"
    );
    let retired = poll(&mut owner, &mut next, |status| {
        status["nodes"][1]["observed"] == "absent"
    });
    assert_eq!(retired["nodes"][1]["reusable"], true);
    assert_eq!(retired["nodes"][1]["retired_lifetimes"], 1);
    assert!(TcpStream::connect(&second).is_err());
    one.assert_lifetime(&contact(&new_first), 1);
    assert_eq!(
        owner.command(&json!({"op":"recreate2","action":"create","node":2}))["state"],
        "pending"
    );
    let running = poll(&mut owner, &mut next, |status| {
        status["nodes"][1]["observed"] == "running"
    });
    let new_second = running["nodes"][1]["endpoint"]
        .as_str()
        .unwrap()
        .to_string();
    two.assert_lifetime(&contact(&new_second), 1);
    one.assert_lifetime(&contact(&new_first), 1);
    assert_eq!(process_start(owner.child.id()), fm_start);
    for node in 1..=2 {
        assert_eq!(
            owner.command(
                &json!({"op":format!("final-delete{node}"),"action":"delete","node":node})
            )["state"],
            "pending"
        );
    }
    let retired = poll(&mut owner, &mut next, |status| {
        status["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["observed"] == "absent")
    });
    for node in retired["nodes"].as_array().unwrap() {
        assert_eq!(node["reusable"], true);
        assert_eq!(node["retired_lifetimes"], 2);
    }
    assert!(TcpStream::connect(&new_first).is_err());
    assert!(TcpStream::connect(&new_second).is_err());
    assert_eq!(process_start(owner.child.id()), fm_start);
    owner.finish();
    one.assert_gone();
    two.assert_gone();
    assert!(!root.join("deployment").exists());
    drop(owner);
    let mut successor = Foreground::start(&root);
    let ready = successor.read();
    assert_eq!(ready["event"], "ready", "{ready}");
    assert_ne!(ready["status"]["deployment"], deployment);
    let next_one = Expected::capture(&root, 1);
    let next_two = Expected::capture(&root, 2);
    successor.finish();
    next_one.assert_gone();
    next_two.assert_gone();
    drop(successor);
    assert_eq!(metadata(root.join("handoff.json"))["clean"], true);
    fs::remove_dir_all(root).unwrap();
}
