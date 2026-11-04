use super::*;
use crate::fm::{Desired, Observed};
use fube_transport::serialization::serde::ObjectArray;
use std::{
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::PermissionsExt,
};

fn launches() -> [Launch; 2] {
    [1, 2].map(|placement| Launch {
        deployment: "00000000-0000-4000-8000-000000000001".into(),
        session: format!("00000000-0000-4000-8000-00000000000{}", placement + 1),
        capability: uuid::Uuid::new_v4().to_string(),
        parent_pid: std::process::id(),
        node: NodeInstance {
            id: [placement; 16],
            instance: 100 + u64::from(placement),
        },
        fm: NodeInstance {
            id: [9; 16],
            instance: 99,
        },
        generation: GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        },
        placement,
        directory: PathBuf::from(format!("/owned/node{placement}")),
        package: PathBuf::from("/owned/package"),
        partition: format!("00000000-0000-4000-8000-00000000001{placement}"),
        replica: i64::from(placement),
        incarnation: 800 + i64::from(placement),
    })
}
fn profile(launch: &Launch) -> ActualProfile {
    ActualProfile {
        service: SERVICE.into(),
        service_type: fixed_type(),
        version: VersionInstance {
            instance: 12345,
            ..VersionInstance::default()
        },
        code_package: "Code".into(),
        app_pid: 123,
        runtime_root: launch.directory.join("hosting/actual-activation"),
    }
}
fn ledger() -> Ledger {
    let mut ledger = Ledger::new(launches());
    for placement in &mut ledger.placements {
        placement.bootstrap = Bootstrap::Metadata;
        placement.admit_profile(profile(&placement.launch)).unwrap();
    }
    ledger
}
fn command(op: &str, action: &str, node: Option<u8>) -> Command {
    Command {
        op: op.into(),
        action: action.into(),
        node,
    }
}
fn ready_reply(placement: &fm::Placement) -> AddInstanceReply {
    let body = placement.body().unwrap();
    AddInstanceReply {
        unit: body.unit,
        replica: Replica {
            state: 2,
            service_endpoint: "127.0.0.1:23456".into(),
            ..body.replica
        },
        error: 0,
    }
}
    fn remove_reply(placement: &fm::Placement) -> AddInstanceReply {
        let mut reply = ready_reply(placement);
        reply.replica.state = 3;
        reply.replica.up = false;
        reply.replica.version = VersionInstance::invalid();
        reply.replica.service_endpoint.clear();
        reply
    }
    fn retirement(placement: &fm::Placement) -> RetirementReceipt {
        RetirementReceipt {
            deployment: placement.launch.deployment.clone(),
            session: placement.launch.session.clone(),
            node: placement.launch.node.clone(),
            fm: placement.launch.fm.clone(),
            generation: placement.launch.generation.clone(),
            node_pid: 555,
            app_pid: placement.profile.as_ref().unwrap().app_pid,
            runtime_root: placement.profile.as_ref().unwrap().runtime_root.clone(),
            disposal: fube_node::local::InstanceDisposal {
                body: placement.body().unwrap(),
                host: "original-host".into(),
                runtime: "original-runtime".into(),
                command: 8,
                ra_operation: 19,
                lookup: 3,
                acquisition: 42,
            },
        }
    }

    #[test]
    fn serialized_retirement_join_rejects_missing_old_and_cross_owner_proofs_in_both_orders() {
        let parent = support::parent();
        let root = parent.join(format!("receipt-{}", uuid::Uuid::new_v4()));
        private_dir(&root).unwrap();
        let path = root.join("retirement.json");
        for proof_first in [false, true] {
            let mut ledger = ledger();
            ledger.accept(command("a", "create", Some(1))).unwrap();
            let first = ready_reply(&ledger.placements[0]);
            ledger.reply(0, first.clone(), false).unwrap();
            let first_result = ledger.result("a");
            ledger.accept(command("d", "delete", Some(1))).unwrap();
            let old_proof = retirement(&ledger.placements[0]);
            let removed = remove_reply(&ledger.placements[0]);
            if proof_first {
                write_json(&path, &old_proof).unwrap();
                ledger.read_retirement(0, &path, 555).unwrap();
                assert_eq!(ledger.result("d")["state"], "pending");
                assert!(ledger.accept(command("early", "create", Some(1))).is_err());
                ledger.reply(0, removed, true).unwrap();
            } else {
                let _ = fs::remove_file(&path);
                ledger.reply(0, removed, true).unwrap();
                ledger.read_retirement(0, &path, 555).unwrap();
                assert_eq!(ledger.result("d")["state"], "pending");
                assert!(ledger.accept(command("early", "create", Some(1))).is_err());
                write_json(&path, &old_proof).unwrap();
                ledger.read_retirement(0, &path, 555).unwrap();
            }
            assert_eq!(ledger.result("d")["cleanup"], "verified_original_instance");
            let deleted = ledger.result("d");
            ledger.accept(command("b", "create", Some(1))).unwrap();
            assert_eq!(ledger.placements[0].replica, first.replica.id + 1);
            assert_eq!(ledger.placements[0].incarnation, first.replica.instance + 1);
            ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
            ledger.accept(command("e", "delete", Some(1))).unwrap();
            ledger.reply(0, remove_reply(&ledger.placements[0]), true).unwrap();
            // The serialized, otherwise valid old proof cannot discharge B.
            write_json(&path, &old_proof).unwrap();
            ledger.read_retirement(0, &path, 555).unwrap();
            assert_eq!(ledger.result("e")["state"], "pending");
            let identity = (ledger.placements[0].replica, ledger.placements[0].incarnation);
            let operations = ledger.operations.len();
            assert!(ledger.accept(command("premature", "create", Some(1))).is_err());
            assert_eq!(ledger.operations.len(), operations);
            assert_eq!((ledger.placements[0].replica, ledger.placements[0].incarnation), identity);
            let current = retirement(&ledger.placements[0]);
            let mut sibling = current.clone();
            sibling.session = ledger.placements[1].launch.session.clone();
            write_json(&path, &sibling).unwrap();
            assert!(ledger.read_retirement(0, &path, 555).is_err());
            assert_eq!(ledger.result("e")["state"], "pending");
            write_json(&path, &current).unwrap();
            ledger.read_retirement(0, &path, 555).unwrap();
            assert_eq!(ledger.result("e")["state"], "complete");
            ledger.accept(command("c", "create", Some(1))).unwrap();
            ledger.lost(0, "current link lost".into());
            assert_eq!(ledger.result("a"), first_result, "predecessor snapshot is frozen");
            assert_eq!(ledger.result("d"), deleted);
            assert!(matches!(ledger.accept(command("a", "create", Some(1))).unwrap(),
                Accepted::Duplicate(value) if value == first_result));
            assert!(ledger.accept(command("a", "create", Some(2))).is_err());
            assert!(ledger.accept(command("after-loss", "create", Some(1))).is_err());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_native_frames_do_not_mutate_successor_and_identity_overflow_does_not_consume_history() {
        let mut ledger = ledger();
        ledger.accept(command("a", "create", Some(1))).unwrap();
        let old = ready_reply(&ledger.placements[0]);
        ledger.reply(0, old.clone(), false).unwrap();
        ledger.accept(command("d", "delete", Some(1))).unwrap();
        let removed = remove_reply(&ledger.placements[0]);
        ledger.reply(0, removed.clone(), true).unwrap();
        ledger.retirement(0, retirement(&ledger.placements[0]), 555).unwrap();
        ledger.accept(command("b", "create", Some(1))).unwrap();
        let current = ready_reply(&ledger.placements[0]);
        ledger.reply(0, current.clone(), false).unwrap();
        let status = ledger.status();
        ledger.reply(0, old, false).unwrap();
        ledger.reply(0, removed.clone(), true).unwrap();
        let report = ReplicaUp {
            dropped: ObjectArray(vec![FailoverUnitInfo {
                unit: removed.unit,
                service: ledger.placements[0].body().unwrap().service,
                replicas: ObjectArray(vec![ReplicaInfo {
                    description: removed.replica,
                    intermediate_role: 1,
                }]),
                ..FailoverUnitInfo::default()
            }]),
            ..ReplicaUp::default()
        };
        assert!(ledger.placements[0].report(&report).unwrap().is_none());
        assert_eq!(ledger.status(), status);
        assert!(ledger.placements[0].link_alive);
        ledger.accept(command("e", "delete", Some(1))).unwrap();
        ledger.reply(0, remove_reply(&ledger.placements[0]), true).unwrap();
        ledger.retirement(0, retirement(&ledger.placements[0]), 555).unwrap();
        ledger.placements[0].incarnation = i64::MAX;
        let prior = ledger.operations.len();
        let replica = ledger.placements[0].replica;
        assert!(ledger.accept(command("overflow", "create", Some(1))).is_err());
        assert_eq!(ledger.operations.len(), prior);
        assert_eq!(ledger.placements[0].replica, replica);
    }

#[test]
fn private_launch_metadata_roundtrip_does_not_use_native_byte_array_json_visitors() {
    let launch = launches()[0].clone();
    let bytes = serde_json::to_vec(&launch).unwrap();
    let decoded: Launch = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.node.id, [1; 16]);
    assert_eq!(decoded.node.instance, 101);
    assert_eq!(decoded.fm.id, [9; 16]);
    assert_eq!(decoded.fm.instance, 99);
    assert_eq!(decoded.generation.number.owner, [9; 16]);
    assert_eq!(decoded.generation.number.generation, 7);
    assert!(!decoded.generation.for_fm_replica);
    let metadata = Metadata {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node,
        pid: 555,
        address: "127.0.0.1:23456".parse().unwrap(),
        profile: Some(profile(&decoded)),
    };
    let bytes = serde_json::to_vec(&metadata).unwrap();
    let decoded: Metadata = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.pid, 555);
    assert_eq!(decoded.node.instance, 101);
    assert_eq!(decoded.profile.unwrap().version.instance, 12345);
}

#[test]
fn semantic_completion_is_exact_and_writes_never_complete_an_operation() {
    let mut ledger = ledger();
    assert!(matches!(
        ledger.accept(command("a", "create", Some(1))).unwrap(),
        Accepted::Create(0, _)
    ));
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    assert_eq!(ledger.result("a")["state"], "pending");
    let reply = ready_reply(&ledger.placements[0]);
    let mut stale = reply.clone();
    stale.replica.node.instance -= 1;
    assert!(ledger.reply(0, stale, false).is_err());
    let mut stale = reply.clone();
    stale.unit.current.configuration += 1;
    assert!(ledger.reply(0, stale, false).is_err());
    let mut stale = reply.clone();
    stale.replica.instance -= 1;
    assert!(ledger.reply(0, stale, false).is_err());
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    ledger.reply(0, reply.clone(), false).unwrap();
    assert_eq!(ledger.placements[0].observed, Observed::Running);
    let count = ledger.operations.len();
    assert!(matches!(
        ledger.accept(command("a", "create", Some(1))).unwrap(),
        Accepted::Duplicate(_)
    ));
    assert_eq!(ledger.operations.len(), count);
    assert!(ledger.accept(command("a", "create", Some(2))).is_err());
    ledger.accept(command("d", "delete", Some(1))).unwrap();
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    assert_eq!(ledger.placements[0].desired, Desired::Absent);
    assert!(ledger.reply(0, reply.clone(), true).is_err());
    let mut disposed = reply;
    disposed.replica.state = 3;
    disposed.replica.up = false;
    disposed.replica.version = VersionInstance::invalid();
    disposed.replica.service_endpoint.clear();
    ledger.reply(0, disposed, true).unwrap();
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    assert_eq!(ledger.result("d")["state"], "pending");
    assert!(ledger.accept(command("new", "create", Some(1))).is_err());
}

#[test]
fn notification_catalog_uses_generated_version_and_unsupported_work_is_not_echoed() {
    let mut ledger = ledger();
    let placement = &ledger.placements[0];
    let body = ServiceTypeNotification {
        infos: ObjectArray(vec![placement.profile.as_ref().unwrap().info()]),
    };
    assert_eq!(placement.notification(&body).unwrap(), body);
    let mut wrong = body.clone();
    wrong.infos.0[0].versioned.version.instance += 1;
    assert!(placement.notification(&wrong).is_err());
    wrong = body.clone();
    wrong.infos.0[0].code_package = "Other".into();
    assert!(placement.notification(&wrong).is_err());
    let final_upload = ReplicaUp {
        last: true,
        ..ReplicaUp::default()
    };
    assert!(placement.report(&final_upload).is_err());
    ledger.placements[0].bootstrap = Bootstrap::NodeUp;
    assert!(ledger.placements[0].report(&final_upload).is_err());
    let launch = ledger.placements[0].launch.clone();
    assert!(ledger.placements[0]
        .admit_profile(profile(&launch))
        .is_err());
    ledger.placements[0].bootstrap = Bootstrap::FinalUpload;
    assert_eq!(
        ledger.placements[0].report(&final_upload).unwrap(),
        Some(final_upload.clone())
    );
    let mut unsupported = final_upload;
    unsupported.from_fmm = true;
    assert!(ledger.placements[0].report(&unsupported).is_err());
}

#[test]
fn finite_identities_are_not_evicted_and_uncertainty_retains_authority() {
    let mut ledger = ledger();
    ledger.accept(command("create", "create", Some(1))).unwrap();
    ledger.lost(0, "link lost".into());
    assert_eq!(ledger.placements[0].desired, Desired::Running);
    assert_eq!(ledger.placements[0].observed, Observed::Unknown);
    assert_eq!(ledger.result("create")["state"], "unknown");
    assert!(ledger
        .accept(command("replace", "create", Some(1)))
        .is_err());
    assert!(ledger.accept(command("remove", "delete", Some(1))).is_err());
    for index in 1..64 {
        ledger
            .accept(command(&format!("status{index}"), "status", None))
            .unwrap();
    }
    assert!(ledger.accept(command("overflow", "status", None)).is_err());
    assert!(matches!(
        ledger.accept(command("create", "create", Some(1))).unwrap(),
        Accepted::Duplicate(_)
    ));
    assert_eq!(ledger.operations.len(), 64);
    assert!(serde_json::from_str::<Command>(
        r#"{"op":"x","action":"create","node":1,"force":true}"#
    )
    .is_err());
    assert!(command("x", "create", Some(3)).validate().is_err());

    let mut repeated = self::ledger();
    for cycle in 0..2 {
        repeated.accept(command(&format!("c{cycle}"), "create", Some(1))).unwrap();
        repeated.reply(0, ready_reply(&repeated.placements[0]), false).unwrap();
        repeated.accept(command(&format!("d{cycle}"), "delete", Some(1))).unwrap();
        repeated.reply(0, remove_reply(&repeated.placements[0]), true).unwrap();
        repeated.retirement(0, retirement(&repeated.placements[0]), 555).unwrap();
    }
    let old = repeated.result("c0");
    let replica = repeated.placements[0].replica;
    for index in 4..64 {
        repeated.accept(command(&format!("s{index}"), "status", None)).unwrap();
    }
    assert!(repeated.accept(command("capacity", "create", Some(1))).is_err());
    assert_eq!(repeated.placements[0].replica, replica);
    assert_eq!(repeated.operations.len(), 64);
    assert!(matches!(repeated.accept(command("c0", "create", Some(1))).unwrap(),
        Accepted::Duplicate(value) if value == old));
    assert!(repeated.accept(command("c0", "create", Some(2))).is_err());
}

#[test]
fn semantic_remove_then_disconnect_cannot_be_repaired_by_a_late_local_receipt() {
    let mut ledger = ledger();
    ledger.accept(command("c", "create", Some(1))).unwrap();
    ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
    ledger.accept(command("d", "delete", Some(1))).unwrap();
    let proof = retirement(&ledger.placements[0]);
    ledger.reply(0, remove_reply(&ledger.placements[0]), true).unwrap();
    assert_eq!(ledger.result("d")["state"], "pending");
    ledger.lost(0, "original channel lost before proof".into());
    ledger.retirement(0, proof, 555).unwrap();
    assert_eq!(ledger.result("d")["state"], "unknown");
    assert!(!ledger.placements[0].reusable);
    assert!(ledger.accept(command("unsafe", "create", Some(1))).is_err());
}

#[path = "../tests/support/mod.rs"]
mod support;

struct Fixture {
    root: PathBuf,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let parent = support::parent();
        assert!(parent.is_absolute() && parent.is_dir());
        let root = parent.join(format!("{label}-{}", uuid::Uuid::new_v4()));
        Self { root }
    }
    fn config(&self) -> Config {
        Config {
            root: self.root.clone(),
            app: support::binary("cluster-stateless-app", "FUBE_CLUSTER_APP"),
            executable: support::binary("fube-cluster", "FUBE_CLUSTER_NODE_EXE"),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.root.join("handoff.json").exists()
            && read_json::<Value>(&self.root.join("handoff.json")).unwrap()["clean"] == true
        {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
}
async fn until(cluster: &mut Cluster, predicate: impl Fn(&Cluster) -> bool) {
    until_with_budget(cluster, Duration::from_secs(12), predicate).await;
}
async fn until_with_budget(
    cluster: &mut Cluster,
    budget: Duration,
    predicate: impl Fn(&Cluster) -> bool,
) {
    let end = Instant::now() + budget;
    loop {
        cluster.drive().await;
        if predicate(cluster) {
            break;
        }
        assert!(
            Instant::now() < end,
            "progress deadline: {}",
            cluster.status()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
fn contact(endpoint: &str) -> Value {
    let mut socket =
        TcpStream::connect_timeout(&endpoint.parse().unwrap(), Duration::from_secs(1)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket.write_all(b"identity\n").unwrap();
    let mut bytes = vec![];
    socket.take(4097).read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() <= 4096 && bytes.last() == Some(&b'\n'));
    serde_json::from_slice(&bytes).unwrap()
}
fn assert_identity(actual: &Value, launch: &Launch, node_pid: u32, app_pid: u32) {
    // Expected identities come from original launch authority and actual owned
    // process metadata, captured BEFORE reading any FM endpoint/result.
    assert_eq!(actual["profile"], "fube-cluster-stateless-v1");
    assert_eq!(actual["node"], node_hex(&launch.node));
    assert_eq!(actual["node_instance"], launch.node.instance);
    assert_eq!(actual["node_pid"], node_pid);
    assert_eq!(actual["app_pid"], app_pid);
    assert_eq!(actual["service"], SERVICE);
    assert_eq!(actual["service_type"], TYPE);
    assert_eq!(actual["partition"], launch.partition);
    assert_eq!(actual["replica"], launch.replica);
    assert_eq!(actual["incarnation"], launch.incarnation as u64);
}
fn gone(pid: u32) -> bool {
    !Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
async fn executable_nodes_preserve_uncertainty_sibling_contact_and_clean_handoff() {
    let fixture = Fixture::new("uncertain");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    let expected = cluster
        .children
        .iter()
        .map(|child| {
            (
                child.launch.clone(),
                child.process.as_ref().unwrap().pid(),
                child
                    .metadata
                    .as_ref()
                    .unwrap()
                    .profile
                    .as_ref()
                    .unwrap()
                    .app_pid,
            )
        })
        .collect::<Vec<_>>();
    assert_ne!(expected[0].1, expected[1].1);
    assert_ne!(expected[0].1, std::process::id());
    assert_ne!(expected[0].2, expected[1].2);
    for node in 1..=2 {
        let outcome = cluster
            .execute(command(&format!("c{node}"), "create", Some(node)))
            .await
            .unwrap();
        assert_eq!(outcome["state"], "pending");
    }
    // Native notification eligibility starts at monotonic zero. A fresh runner
    // may need the full type interval plus a retry before its first notification.
    let settings = fube_ra::entity::Settings::default();
    let notification_budget = Duration::from_millis(
        settings.notification_type_ms + settings.notification_retry_ms,
    ) + Duration::from_secs(12);
    until_with_budget(&mut cluster, notification_budget, |cluster| {
        cluster
            .ledger
            .placements
            .iter()
            .all(|p| p.observed == Observed::Running && p.catalog_notified)
    })
    .await;
    let endpoints = cluster
        .ledger
        .placements
        .iter()
        .map(|p| p.endpoint.clone().unwrap())
        .collect::<Vec<_>>();
    for index in 0..2 {
        assert_identity(
            &contact(&endpoints[index]),
            &expected[index].0,
            expected[index].1,
            expected[index].2,
        );
    }
    assert_ne!(
        contact(&endpoints[1])["node"],
        node_hex(&expected[0].0.node),
        "swapped sibling endpoint must not satisfy the placement oracle"
    );
    assert_eq!(
        cluster
            .execute(command("c1", "create", Some(1)))
            .await
            .unwrap()["state"],
        "complete"
    );
    assert!(cluster
        .execute(command("c1", "create", Some(2)))
        .await
        .is_err());
    cluster.children[0].link.as_ref().unwrap().channel.dispose();
    until(&mut cluster, |cluster| {
        cluster.ledger.placements[0].observed == Observed::Unknown
    })
    .await;
    assert_eq!(cluster.ledger.placements[0].desired, Desired::Running);
    assert_identity(
        &contact(&endpoints[0]),
        &expected[0].0,
        expected[0].1,
        expected[0].2,
    );
    assert_identity(
        &contact(&endpoints[1]),
        &expected[1].0,
        expected[1].1,
        expected[1].2,
    );
    assert!(cluster
        .execute(command("replacement", "create", Some(1)))
        .await
        .is_err());
    cluster
        .execute(command("d2", "delete", Some(2)))
        .await
        .unwrap();
    until(&mut cluster, |cluster| {
        cluster.ledger.placements[1].observed == Observed::Absent
    })
    .await;
    assert!(TcpStream::connect(&endpoints[1]).is_err());
    assert_identity(
        &contact(&endpoints[0]),
        &expected[0].0,
        expected[0].1,
        expected[0].2,
    );
    let prior = cluster.interlock.deployment.clone();
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
    for (_, node, app) in expected {
        assert!(gone(node));
        assert!(gone(app));
    }
    assert!(!fixture.root.join("deployment").exists());
    drop(cluster);
    let mut successor = Cluster::new(fixture.config()).unwrap();
    assert_ne!(successor.interlock.deployment, prior);
    successor.start().await.unwrap();
    let result = successor.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
}

#[tokio::test]
async fn real_stale_native_generation_is_not_admitted_and_sibling_remains_usable() {
    let fixture = Fixture::new("stale");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    let launch = cluster.children[1].launch.clone();
    let pid = cluster.children[1].process.as_ref().unwrap().pid();
    let app = cluster.children[1]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    let body = cluster.ledger.placements[0].body().unwrap();
    let mut stale = cluster.children[0].launch.generation.clone();
    stale.number.generation -= 1;
    cluster.children[0]
        .link
        .as_ref()
        .unwrap()
        .send(add_instance_message(&body, &stale).unwrap())
        .await
        .unwrap();
    until(&mut cluster, |c| {
        c.ledger.placements[0].observed == Observed::Unknown
    })
    .await;
    assert!(!cluster.ledger.placements[0].created);
    assert!(cluster.ledger.placements[0].endpoint.is_none());
    cluster
        .execute(command("sibling", "create", Some(2)))
        .await
        .unwrap();
    until(&mut cluster, |c| {
        c.ledger.placements[1].observed == Observed::Running
    })
    .await;
    assert_identity(
        &contact(cluster.ledger.placements[1].endpoint.as_ref().unwrap()),
        &launch,
        pid,
        app,
    );
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
}

#[tokio::test]
async fn partial_startup_rolls_back_every_acquired_original_child() {
    let fixture = Fixture::new("partial");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.prepare().unwrap();
    cluster.spawn(0).await.unwrap();
    let pid = cluster.children[0].process.as_ref().unwrap().pid();
    let app = cluster.children[0]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    // Only this test-owned second launch loses its immutable package source;
    // the first original owner and its rollback are the production code path.
    cluster.ledger.placements[1].launch.package = fixture.root.join("missing-package");
    assert!(cluster.spawn(1).await.is_err());
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
    assert!(gone(pid) && gone(app));
    assert!(!fixture.root.join("deployment").exists());
}

#[tokio::test]
async fn active_original_node_owner_loss_cannot_fabricate_receipt_or_clean_relaunch() {
    let fixture = Fixture::new("lost-owner");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    for node in 1..=2 {
        cluster
            .execute(command(&format!("c{node}"), "create", Some(node)))
            .await
            .unwrap();
    }
    until(&mut cluster, |c| {
        c.ledger
            .placements
            .iter()
            .all(|p| p.observed == Observed::Running)
    })
    .await;
    let sibling_endpoint = cluster.ledger.placements[1].endpoint.clone().unwrap();
    let lost_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
    let lost_app = cluster.children[0]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    let sibling_pid = cluster.children[1].process.as_ref().unwrap().pid();
    let sibling_app = cluster.children[1]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    // Fault through the retained ORIGINAL child/group capability, never a
    // process-name signal or a PID reconstructed into ownership.
    cluster.children[0]
        .process
        .as_mut()
        .unwrap()
        .cleanup()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let terminated = match fs::read_to_string(format!("/proc/{lost_app}/stat")) {
            Ok(stat) => stat
                .rsplit_once(')')
                .is_some_and(|(_, rest)| matches!(rest.split_whitespace().next(), Some("Z" | "X"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => panic!("independent lost-app observation failed: {error}"),
        };
        if terminated {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "parent-death containment stranded the lost app"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(TcpStream::connect(&lost_endpoint).is_err());
    until(&mut cluster, |c| {
        c.ledger.placements[0].observed == Observed::Unknown
    })
    .await;
    assert_eq!(contact(&sibling_endpoint)["app_pid"], sibling_app);
    let receipt_path = cluster.children[0].launch.directory.join("cleanup.json");
    assert!(!receipt_path.exists());
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], false, "{result}");
    assert!(gone(sibling_pid) && gone(sibling_app));
    assert!(TcpStream::connect(&sibling_endpoint).is_err());
    assert_eq!(
        read_json::<Value>(&fixture.root.join("handoff.json")).unwrap()["clean"],
        false
    );
    drop(cluster);
    let error = Cluster::new(fixture.config())
        .err()
        .expect("dirty root must refuse fresh authority");
    assert!(error.contains("dirty predecessor"), "{error}");
    // Preserve this small failed-authority domain and its dirty marker as
    // evidence. No force/reset or claim to the lost app capability is made.
}

#[test]
fn profile_root_lock_and_missing_handoff_fail_closed() {
    let fixture = Fixture::new("interlock");
    private_dir(&fixture.root).unwrap();
    let first = interlock::Interlock::acquire(&fixture.root).unwrap();
    assert!(interlock::Interlock::acquire(&fixture.root).is_err());
    first.publish_clean(&[]).unwrap();
    drop(first);
    let second = interlock::Interlock::acquire(&fixture.root).unwrap();
    second.publish_clean(&[]).unwrap();
    drop(second);
    fs::remove_file(fixture.root.join("handoff.json")).unwrap();
    fs::write(fixture.root.join("unproved-resource"), b"owned").unwrap();
    assert!(interlock::Interlock::acquire(&fixture.root).is_err());
    // This fixture never acquired processes, so exact test-owned input cleanup
    // is safe; this is not a reset path exposed by the product.
    fs::remove_dir_all(&fixture.root).unwrap();
    private_dir(&fixture.root).unwrap();
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(interlock::Interlock::acquire(&fixture.root).is_err());
    fs::remove_dir_all(&fixture.root).unwrap();
}
