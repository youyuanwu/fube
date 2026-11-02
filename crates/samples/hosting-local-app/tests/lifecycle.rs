//! Real owned process + production local RA/effect/H2/RAP bridge.
use fube_hosting::local::{Activation, Options};
use fube_node::local::{EffectReceipt, Node};
use fube_ra::{entity as ra, protocol::*};
use fube_ra_store::{MemoryBackend, Settings, Store};
use std::{fs, path::PathBuf, sync::Arc, time::Duration};

struct Fixture {
    parent: PathBuf,
    package: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let parent = std::env::temp_dir().join(format!("ra-rap-{}", uuid::Uuid::new_v4()));
        let package = parent.join("input");
        fs::create_dir_all(package.join("LocalService/Code")).unwrap();
        fs::write(
            package.join("ApplicationManifest.xml"),
            include_str!("../pkg/ApplicationManifest.xml"),
        )
        .unwrap();
        fs::write(
            package.join("LocalService/ServiceManifest.xml"),
            include_str!("../pkg/LocalService/ServiceManifest.xml"),
        )
        .unwrap();
        fs::copy(
            std::env::var_os("CARGO_BIN_EXE_hosting-local-app").unwrap(),
            package.join("LocalService/Code/hosting-local-app"),
        )
        .unwrap();
        Self { parent, package }
    }
    async fn node(&self, args: &str) -> (Node, Arc<Store>) {
        let store = Arc::new(
            Store::new(Box::new(MemoryBackend::new()), Settings::default())
                .await
                .unwrap(),
        );
        let node = self.with_store(args, store.clone());
        (node, store)
    }
    fn with_store(&self, args: &str, store: Arc<Store>) -> Node {
        let mut options = Options::new(self.parent.clone());
        options.lease_duration = Duration::from_secs(2);
        let mut node = Node::new(
            Activation::new(options).unwrap(),
            store.clone(),
            admission(),
            ra::Settings::default(),
        )
        .unwrap();
        node.start(
            &self.package,
            "fabric:/Local",
            &[("Arguments".into(), args.into())],
        )
        .unwrap();
        node
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.parent).unwrap();
    }
}
fn admission() -> ra::Admission {
    ra::Admission {
        local_node: NodeInstance {
            id: [4; 16],
            instance: 20,
        },
        fm_source: NodeInstance {
            id: [8; 16],
            instance: 30,
        },
        generation: GenerationNumber {
            owner: [8; 16],
            generation: 7,
        },
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}
fn body(node: &Node) -> AddInstance {
    let a = node.activation();
    let premise = a.premise().unwrap();
    let ty = a.declared_type().unwrap();
    let mut b = AddInstance::default();
    b.unit.id.guid.0 = uuid::Uuid::new_v4().to_bytes_le();
    b.unit.consistency.id = b.unit.id.clone();
    b.unit.current = Epoch {
        configuration: 5,
        data_loss: 1,
    };
    b.replica.node = admission().local_node;
    b.replica.id = 40;
    b.replica.instance = 50;
    b.replica.up = true;
    b.replica.state = 1;
    b.service.name = premise.service_name.clone();
    b.service.application = premise.application_name.clone();
    b.service.service_type = ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: ty.package.application.type_name,
                number: ty.package.application.number,
            },
            name: ty.package.name,
        },
        name: ty.name,
    };
    b.service.version = VersionInstance {
        version: PackageVersion {
            application: ApplicationVersion {
                value: Rollout {
                    major: premise.version.application.major,
                    minor: premise.version.application.minor,
                },
            },
            value: Rollout {
                major: premise.version.package.major,
                minor: premise.version.package.minor,
            },
        },
        instance: premise.version.instance,
    };
    b.replica.version = b.service.version.clone();
    b.service.partitions = 1;
    b.service.target = 1;
    b.service.minimum = 1;
    b
}
async fn until(node: &mut Node, mut predicate: impl FnMut(&Node) -> bool) -> Vec<EffectReceipt> {
    let mut effects = vec![];
    let end = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        node.drive()
            .unwrap_or_else(|e| panic!("{e:?}; {:?}", node.activation().logs()));
        for receipt in node.take_command_receipts() {
            receipt.result.unwrap();
        }
        effects.extend(node.take_effect_receipts());
        if predicate(node) {
            return effects;
        }
        assert!(
            std::time::Instant::now() < end,
            "progress timeout; {:?}; effects: {:?}; package: {:?}",
            node.activation().logs(),
            effects,
            node.package_registration()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
async fn cleanup(node: &mut Node, store: &Store) {
    node.terminate().unwrap();
    until(node, |n| {
        n.pending_commands() == 0
            && n.retained_usage() == 0
            && n.package_registration().health_sources_released > 0
    })
    .await;
    node.close_executor(Duration::from_secs(3)).await.unwrap();
    store.close().await.unwrap();
}

struct PausedSecondWrite {
    inner: MemoryBackend,
    writes: Arc<std::sync::atomic::AtomicUsize>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
    pause_on: usize,
}
impl fube_ra_store::Backend for PausedSecondWrite {
    fn initialize(
        &mut self,
        settings: Settings,
        context: fube_ra_store::BackendContext,
    ) -> fube_ra_store::StoreFuture<'_, ()> {
        self.inner.initialize(settings, context)
    }
    fn execute(
        &mut self,
        request: fube_ra_store::Request,
    ) -> fube_ra_store::StoreFuture<'_, fube_ra_store::Output> {
        Box::pin(async move {
            if matches!(
                request,
                fube_ra_store::Request::Insert(_) | fube_ra_store::Request::Update(_)
            ) && self
                .writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1
                == self.pause_on
            {
                self.gate.take().unwrap().await.unwrap();
            }
            self.inner.execute(request).await
        })
    }
    fn close(&mut self) -> fube_ra_store::StoreFuture<'_, ()> {
        self.inner.close()
    }
}

#[tokio::test]
async fn original_pending_open_commit_keeps_exact_use_across_unknown_observation_and_busy_stop() {
    let fixture = Fixture::new();
    let (release, gate) = tokio::sync::oneshot::channel();
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = Arc::new(
        Store::new(
            Box::new(PausedSecondWrite {
                inner: MemoryBackend::new(),
                writes: writes.clone(),
                gate: Some(gate),
                pause_on: 2,
            }),
            Settings {
                // This case abandons an observer while the original work is
                // pending. A real Store timeout terminally fences that store
                // incarnation; it is not recoverable admission (RA/store tests).
                operation_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let mut node = fixture.with_store("", store.clone());
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| {
        writes.load(std::sync::atomic::Ordering::SeqCst) == 2 && n.retained_usage() == 1
    })
    .await;
    let accepted = node.statistics().accepted;
    assert_eq!(node.retained_usage(), 1);
    assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 2);
    // Drop an actual observer future after acceptance; adapter ownership and the
    // original supervisor-owned store operation are not inside this future.
    assert!(tokio::time::timeout(Duration::from_millis(30), async {
        loop {
            node.drive().unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_err());
    assert_eq!(node.statistics().accepted, accepted);
    assert_eq!(node.retained_usage(), 1);
    assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        node.close_executor(Duration::from_millis(20)).await,
        Err(fube_node::local::Error::Busy)
    );
    assert!(node.activation().root().unwrap().exists());
    release.send(()).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 4); // Add/Open/StartClose/FinishClose.
    cleanup(&mut node, &store).await;
}

#[tokio::test]
async fn add_before_registration_retries_native_hosting_error_then_opens_once() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("--delay-ms 300").await;
    let b = body(&node);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.registration_failures > 0)
    })
    .await;
    assert_eq!(
        node.snapshot(b.unit.id.guid.0).committed.unwrap().phase,
        ra::Phase::Opening
    );
    assert!(!node
        .activation()
        .logs()
        .iter()
        .any(|l| l.contains("LOCAL_CALLBACK")));
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    assert_eq!(node.retained_usage(), 1);
    assert_eq!(
        node.activation()
            .logs()
            .iter()
            .filter(|l| l.contains("LOCAL_CALLBACK create"))
            .count(),
        1
    );
    node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    cleanup(&mut node, &store).await;
}

#[tokio::test]
async fn unsupported_and_near_wire_limit_adds_reject_before_selected_or_usage_ownership() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("").await;
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    for variant in 0..6 {
        let mut bad = b.clone();
        match variant {
            0 => bad.replica.current_role = 0,
            1 => bad.replica.up = false,
            2 => bad.replica.state = 0,
            3 => bad.replica.id = -1,
            4 => bad.replica.instance = -1,
            _ => bad.service.constraints = "x".repeat(32_000),
        }
        assert!(node.add(bad).is_err());
        assert_eq!(node.statistics().accepted, 0);
        assert_eq!(node.pending_commands(), 0);
        assert_eq!(node.retained_usage(), 0);
    }
    node.add(b.clone()).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    let accepted = node.statistics().accepted;
    let mut impossible = b.clone();
    impossible.replica.instance += 1;
    assert!(node.add(impossible.clone()).is_err());
    assert_eq!(node.statistics().accepted, accepted);
    let mut stale = b.clone();
    stale.replica.id -= 1;
    stale.replica.instance -= 1;
    node.add(stale).unwrap();
    until(&mut node, |n| n.pending_commands() == 0).await;
    assert_eq!(
        node.snapshot(b.unit.id.guid.0)
            .committed
            .unwrap()
            .body
            .replica
            .id,
        b.replica.id
    );
    let accepted = node.statistics().accepted;
    assert!(
        node.add(impossible).is_err(),
        "ignored stale input cannot authorize a poisoned command"
    );
    assert_eq!(node.statistics().accepted, accepted);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| n.pending_commands() == 0).await;
    node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    cleanup(&mut node, &store).await;
}

#[tokio::test]
async fn real_unknown_final_close_retains_original_ticket_and_exact_use_without_store_recovery() {
    let fixture = Fixture::new();
    let (release, gate) = tokio::sync::oneshot::channel();
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = Arc::new(
        Store::new(
            Box::new(PausedSecondWrite {
                inner: MemoryBackend::new(),
                writes: writes.clone(),
                gate: Some(gate),
                pause_on: 4,
            }),
            Settings {
                operation_timeout: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let mut node = fixture.with_store("", store.clone());
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    let acquisition = node
        .snapshot(b.unit.id.guid.0)
        .committed
        .unwrap()
        .usage
        .unwrap();
    node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
    until(&mut node, |n| n.snapshot(b.unit.id.guid.0).fenced).await;
    assert_eq!(
        node.snapshot(b.unit.id.guid.0).committed.unwrap().phase,
        ra::Phase::Closing
    );
    assert_eq!(node.retained_usage(), 1);
    let accepted = node.statistics().accepted;
    for _ in 0..10 {
        node.drive().unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(node.statistics().accepted, accepted);
    assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(
        node.close_executor(Duration::from_millis(20)).await,
        Err(fube_node::local::Error::Busy)
    );
    release.send(()).unwrap();
    let effects = until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    assert_eq!(
        effects
            .iter()
            .filter(|r| r.dispatch.effect == ra::Effect::ReleaseUsage { acquisition })
            .count(),
        1
    );
    assert_eq!(node.statistics().accepted, accepted);
    assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert!(
        store.submit(fube_ra_store::Request::Maintain).is_err(),
        "terminal store never reopens"
    );
    cleanup(&mut node, &store).await;
}

#[tokio::test]
async fn original_ra_lifetime_gate_retires_owned_session_before_and_after_registration() {
    for registered in [false, true] {
        let fixture = Fixture::new();
        let (mut node, store) = fixture
            .node(if registered { "" } else { "--delay-ms 300" })
            .await;
        if registered {
            until(&mut node, |n| n.package_registration().registered).await;
        }
        node.close_executor(Duration::from_secs(3)).await.unwrap();
        // No adapter drive/event is required for Hosting's independent gate.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let status = node.activation().session_status().unwrap().unwrap();
        assert!(!status.connected);
        assert!(!status.registered_host);
        assert!(!node
            .activation()
            .logs()
            .iter()
            .any(|l| l.contains("LOCAL_CALLBACK")));
        assert_eq!(node.drive(), Err(fube_node::local::Error::OwnerLost));
        assert!(node.terminate().unwrap());
        assert!(node.activation().cleanup_status().directory_released);
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn actual_ra_opens_empty_endpoint_and_close_releases_exact_use_not_host() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("").await;
    until(&mut node, |n| n.package_registration().registered).await;
    assert!(!node
        .activation()
        .logs()
        .iter()
        .any(|l| l.contains("LOCAL_CALLBACK")));
    let b = body(&node);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    let opened = node.snapshot(b.unit.id.guid.0).committed.unwrap();
    assert_eq!(opened.body.replica.service_endpoint, "");
    assert!(opened.usage.is_some());
    assert_eq!(node.retained_usage(), 1);
    let logs = node.activation().logs();
    assert_eq!(
        logs.iter()
            .filter(|l| l.contains("LOCAL_CALLBACK create"))
            .count(),
        1
    );
    assert_eq!(
        logs.iter()
            .filter(|l| l.contains("LOCAL_CALLBACK open"))
            .count(),
        1
    );
    let mut unsupported = b.clone();
    unsupported.service.update_version += 1;
    assert!(node.add(unsupported).is_err());
    node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    assert!(
        node.activation()
            .session_status()
            .unwrap()
            .unwrap()
            .connected
    );
    assert!(!node.activation().process_status().unwrap().exit_observed);
    assert_eq!(
        node.activation()
            .logs()
            .iter()
            .filter(|l| l.contains("LOCAL_CALLBACK close"))
            .count(),
        1
    );
    let pid = node.activation().process_status().unwrap().pid;
    let root = node.activation().root().unwrap().to_path_buf();
    cleanup(&mut node, &store).await;
    let status = node.activation().process_status().unwrap();
    assert!(
        status.termination_requested
            && status.exit_observed
            && status.group_drained
            && status.reaped
    );
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(!root.exists());
    assert_eq!(node.activation().residency().resources().logical_bytes, 0);
}
