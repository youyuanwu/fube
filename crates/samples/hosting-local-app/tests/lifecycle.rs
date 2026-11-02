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
        assert_eq!(
            node.activation()
                .process_status()
                .unwrap()
                .dropped_log_records,
            0
        );
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
    until(node, Node::quiescent).await;
    assert!(node.package_registration().health_sources_released > 0);
    node.close_executor(Duration::from_secs(3)).await.unwrap();
    store.close().await.unwrap();
}
fn resource(node: &Node) -> PathBuf {
    node.activation()
        .root()
        .unwrap()
        .join("deployment/HostingLocalApp_App1/work/local-instance.resource")
}
fn count(node: &Node, marker: &str) -> usize {
    assert_eq!(
        node.activation()
            .process_status()
            .unwrap()
            .dropped_log_records,
        0
    );
    node.activation()
        .logs()
        .iter()
        .filter(|l| l.contains(marker))
        .count()
}
fn native_free(node: &Node) {
    let pid = node.activation().process_status().unwrap().pid;
    let maps = fs::read_to_string(format!("/proc/{pid}/maps")).unwrap();
    assert!(!maps.contains("libFabricRuntime") && !maps.contains("libFabricCommon"));
    let env = fs::read(format!("/proc/{pid}/environ")).unwrap();
    assert!(!env
        .split(|b| *b == 0)
        .any(|s| s.starts_with(b"LD_LIBRARY_PATH=") || s.starts_with(b"FabricConfigFileName=")));
}
async fn advance(node: &mut Node, duration: Duration) {
    let start = std::time::Instant::now();
    until(node, |_| start.elapsed() >= duration).await;
}
async fn replay(node: &mut Node, dispatch: &ra::Dispatched, stale: bool) {
    let (action, registration, mut body, flags) = match &dispatch.effect {
        ra::Effect::RuntimeOpen {
            registration,
            body,
            flags,
            ..
        } => ("ReplicaOpen", registration, (**body).clone(), *flags),
        ra::Effect::Close {
            registration,
            body,
            flags,
            ..
        } => ("ReplicaClose", registration, (**body).clone(), *flags),
        _ => panic!("only an actual captured RA lifecycle effect may be replayed"),
    };
    if stale {
        body.replica.instance -= 1;
    }
    let send = node
        .activation()
        .host_lease()
        .unwrap()
        .send_lifecycle(
            action,
            fube_node::local::rap::request(registration, &body, flags).unwrap(),
        )
        .unwrap();
    until(node, |_| send.result().is_some()).await;
    send.result().unwrap().unwrap();
    drop(send);
    advance(node, Duration::from_millis(50)).await;
}

#[tokio::test]
async fn actual_disposal_barrier_blocks_semantic_retirement_and_error_close_aborts_first() {
    for error in [false, true] {
        let fixture = Fixture::new();
        let (mut node, store) = fixture
            .node(if error {
                "--close-error-block-dispose"
            } else {
                "--block-dispose"
            })
            .await;
        until(&mut node, |n| n.package_registration().registered).await;
        native_free(&node);
        let b = body(&node);
        node.add(b.clone()).unwrap();
        until(&mut node, |n| {
            n.snapshot(b.unit.id.guid.0)
                .committed
                .is_some_and(|s| s.phase == ra::Phase::Ready)
        })
        .await;
        assert!(resource(&node).is_file());
        node.close(b.unit.id.clone(), ra::CloseMode::Close).unwrap();
        until(&mut node, |n| count(n, "LOCAL_DISPOSE begin") == 1).await;
        // Continue actual adapter and channel progress throughout the negative
        // oracle. Merely not driving could conceal an already-sent early reply.
        advance(&mut node, Duration::from_millis(150)).await;
        let closing = node.snapshot(b.unit.id.guid.0).committed.unwrap();
        assert_eq!(closing.phase, ra::Phase::Closing);
        assert!(closing.usage.is_some());
        assert_eq!(node.retained_usage(), 1);
        assert!(resource(&node).is_file());
        assert_eq!(count(&node, "LOCAL_CALLBACK close"), 1);
        assert_eq!(count(&node, "LOCAL_CALLBACK abort"), usize::from(error));
        assert_eq!(count(&node, "LOCAL_RESOURCE released"), 0);
        assert!(
            node.activation()
                .session_status()
                .unwrap()
                .unwrap()
                .connected
        );
        fs::write(
            resource(&node).with_file_name("local-dispose.release"),
            b"release actual destructor",
        )
        .unwrap();
        until(&mut node, |n| {
            n.snapshot(b.unit.id.guid.0)
                .committed
                .is_some_and(|s| s.phase == ra::Phase::Closed)
                && n.retained_usage() == 0
        })
        .await;
        assert!(!resource(&node).exists());
        assert_eq!(count(&node, "LOCAL_RESOURCE released"), 1);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
        assert!(!node.activation().process_status().unwrap().exit_observed);
        cleanup(&mut node, &store).await;
    }
}

#[tokio::test]
async fn actual_duplicate_stale_traffic_and_ra_replacement_require_a_new_authorized_resend() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("").await;
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    node.add(b.clone()).unwrap();
    let effects = until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    let open = effects
        .iter()
        .find(|r| matches!(r.dispatch.effect, ra::Effect::RuntimeOpen { .. }))
        .unwrap()
        .dispatch
        .clone();
    replay(&mut node, &open, false).await;
    replay(&mut node, &open, true).await;
    assert_eq!(count(&node, "LOCAL_CALLBACK create"), 1);
    assert_eq!(count(&node, "LOCAL_CALLBACK open"), 1);
    assert_eq!(node.retained_usage(), 1);
    let mut next = b.clone();
    next.replica.id += 1;
    next.replica.instance += 1;
    // Native replacement closes the old identity but does not remember/replay
    // the incoming replacement Add as an autonomous new incarnation.
    node.add(next.clone()).unwrap();
    let effects = until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    let close = effects
        .iter()
        .find(|r| matches!(r.dispatch.effect, ra::Effect::Close { .. }))
        .unwrap()
        .dispatch
        .clone();
    assert_eq!(count(&node, "LOCAL_CALLBACK create"), 1);
    assert!(!resource(&node).exists());
    replay(&mut node, &close, false).await;
    replay(&mut node, &open, false).await; // closed-proxy failure reply cannot revive RA.
    assert_eq!(
        node.snapshot(b.unit.id.guid.0).committed.unwrap().phase,
        ra::Phase::Closed
    );
    assert_eq!(node.retained_usage(), 0);
    node.add(next.clone()).unwrap(); // actual new authorized resend, not a saved launch.
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready && s.body.replica.id == next.replica.id)
    })
    .await;
    replay(&mut node, &open, false).await;
    replay(&mut node, &close, false).await; // actual old request on this same session.
    assert_eq!(
        node.snapshot(b.unit.id.guid.0)
            .committed
            .unwrap()
            .body
            .replica
            .id,
        next.replica.id
    );
    assert_eq!(count(&node, "LOCAL_CALLBACK create"), 2);
    assert_eq!(count(&node, "LOCAL_CALLBACK open"), 2);
    assert_eq!(count(&node, "LOCAL_CALLBACK close"), 1);
    node.close(b.unit.id.clone(), ra::CloseMode::ForceAbort)
        .unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    assert_eq!(count(&node, "LOCAL_CALLBACK abort"), 1);
    assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 2);
    assert!(!resource(&node).exists());
    cleanup(&mut node, &store).await;
}

#[tokio::test]
async fn actual_crash_and_lease_loss_distinguish_opening_use_logical_retirement_and_reap() {
    for opening in [false, true] {
        for lease_loss in [false, true] {
            let fixture = Fixture::new();
            let (mut node, store) = fixture
                .node(if opening { "--open-delay-ms 4000" } else { "" })
                .await;
            until(&mut node, |n| n.package_registration().registered).await;
            native_free(&node);
            let b = body(&node);
            node.add(b.clone()).unwrap();
            until(&mut node, |n| {
                if opening {
                    count(n, "LOCAL_CALLBACK open") == 1
                } else {
                    n.snapshot(b.unit.id.guid.0)
                        .committed
                        .is_some_and(|s| s.phase == ra::Phase::Ready)
                }
            })
            .await;
            assert!(resource(&node).exists());
            let pid = node.activation().process_status().unwrap().pid;
            if lease_loss {
                node.activation().host_lease().unwrap().stop_renewal();
            } else {
                assert!(std::process::Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .status()
                    .unwrap()
                    .success());
            }
            until(&mut node, |n| {
                n.snapshot(b.unit.id.guid.0).committed.is_some_and(|s| {
                    if opening {
                        s.phase == ra::Phase::Opening && s.runtime.is_none() && s.open_failures > 0
                    } else {
                        s.phase == ra::Phase::Closed && s.usage.is_none()
                    }
                }) && (opening || n.retained_usage() == 0)
            })
            .await;
            assert!(
                !node
                    .activation()
                    .session_status()
                    .unwrap()
                    .unwrap()
                    .connected
            );
            assert_eq!(node.retained_usage(), usize::from(opening));
            if opening {
                node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
                until(&mut node, |n| {
                    n.snapshot(b.unit.id.guid.0)
                        .committed
                        .is_some_and(|s| s.phase == ra::Phase::Closed)
                        && n.retained_usage() == 0
                })
                .await;
            }
            // Logical host loss is not process death/reap. Observe the actual
            // dead waitable leader before any cleanup can send a PGID signal.
            until(&mut node, |_| {
                fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| s.contains(") Z "))
            })
            .await;
            assert!(!node.activation().process_status().unwrap().reaped);
            assert_eq!(count(&node, "LOCAL_CALLBACK close"), 0);
            assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 0);
            assert!(
                resource(&node).exists(),
                "no destructor receipt is fabricated for a hard exit"
            );
            let root = node.activation().root().unwrap().to_path_buf();
            cleanup(&mut node, &store).await;
            assert!(!root.exists());
            let status = node.activation().process_status().unwrap();
            assert!(status.exit_observed && status.group_drained && status.reaped);
            assert_eq!(node.activation().residency().resources().logical_bytes, 0);
        }
    }
}

#[tokio::test]
async fn actual_runtime_unregister_consumes_distinct_events_without_host_exit_claim() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("--shutdown-after-ms 2000").await;
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
            && n.package_registration().unregistered_at.is_some()
    })
    .await;
    assert!(!node.activation().process_status().unwrap().exit_observed);
    assert_eq!(count(&node, "LOCAL_CALLBACK abort"), 1);
    assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
    assert!(!resource(&node).exists());
    cleanup(&mut node, &store).await;
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
    for variant in 0..11 {
        let mut bad = b.clone();
        match variant {
            0 => bad.replica.current_role = 0,
            1 => bad.replica.up = false,
            2 => bad.replica.state = 0,
            3 => bad.replica.id = -1,
            4 => bad.replica.instance = -1,
            5 => bad.service.constraints = "x".repeat(32_000),
            6 => bad.unit.consistency.kind = 0,
            7 => {
                bad.unit.consistency.kind = 3;
                bad.unit.consistency.name = "NotSingleton".into();
            }
            8 => bad.unit.consistency.kind = 2,
            9 => bad.unit.consistency.low = 1,
            _ => bad.unit.consistency.id.guid.0 = [9; 16],
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
        node.qualification_close_owner(Duration::from_secs(3))
            .await
            .unwrap();
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
async fn public_shutdown_drains_upstream_obligations_before_executor_close() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("").await;
    until(&mut node, |n| n.package_registration().registered).await;
    assert_eq!(
        node.close_executor(Duration::from_secs(3)).await,
        Err(fube_node::local::Error::Busy),
        "a momentary empty snapshot is not a stable producer stop"
    );
    assert!(node.terminate().unwrap());
    assert!(
        !node.quiescent(),
        "upstream HostClosed/package/source work is not drained"
    );
    assert_eq!(
        node.close_executor(Duration::from_secs(3)).await,
        Err(fube_node::local::Error::Busy)
    );
    until(&mut node, Node::quiescent).await;
    assert!(!node.package_registration().registered);
    assert!(node.package_registration().unregistered_at.is_some());
    assert_eq!(node.package_registration().health_sources_released, 1);
    node.close_executor(Duration::from_secs(3)).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn physical_terminate_during_open_keeps_exact_drop_cleanup_after_serving_withdrawal() {
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("--open-delay-ms 4000").await;
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| count(n, "LOCAL_CALLBACK open") == 1).await;
    let acquisition = node
        .snapshot(b.unit.id.guid.0)
        .committed
        .unwrap()
        .usage
        .unwrap();
    assert_eq!(
        node.terminate(),
        Err(fube_node::local::Error::Activation(
            fube_hosting::local::LocalError::Busy
        ))
    );
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Opening && s.runtime.is_none())
    })
    .await;
    assert_eq!(node.retained_usage(), 1);
    assert!(node.add(b.clone()).is_err());
    assert!(node.retry(b.unit.id.clone()).is_err());
    assert!(node.activation().root().unwrap().exists());
    node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
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
    cleanup(&mut node, &store).await;
    assert!(node.activation().cleanup_status().directory_released);
}

#[tokio::test]
async fn production_reply_adapter_preserves_stale_failed_and_native_nonfull_equality_admission() {
    use fube_node::local::rap;
    use fube_transport::serialization::serde::{from_slice, to_vec};
    let fixture = Fixture::new();
    let (mut node, store) = fixture.node("--block-dispose").await;
    until(&mut node, |n| n.package_registration().registered).await;
    let b = body(&node);
    node.qualification_hold_replies(true);
    node.add(b.clone()).unwrap();
    until(&mut node, |n| count(n, "LOCAL_CALLBACK open") == 1).await;
    let lease = node.activation().host_lease().unwrap();
    // Take the genuine reply only after the actual callback completed, before
    // the adapter consumes it. This is explicitly reply-admission qualification,
    // not substituted positive callback/disposal evidence.
    let actual = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(reply) = lease.take_lifecycle_reply() {
                break reply;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let original: rap::Reply = from_slice(actual.body()).unwrap();
    let use_id = node
        .snapshot(b.unit.id.guid.0)
        .committed
        .unwrap()
        .usage
        .unwrap();
    let mut stale = original.clone();
    stale.local.instance -= 1;
    node.qualification_ingress(
        lease
            .qualification_reply("ReplicaOpenReply", to_vec(&stale).unwrap())
            .unwrap(),
    )
    .unwrap();
    advance(&mut node, Duration::from_millis(30)).await;
    let state = node.snapshot(b.unit.id.guid.0).committed.unwrap();
    assert_eq!(state.phase, ra::Phase::Opening);
    assert_eq!(state.usage, Some(use_id));
    let mut allowed = original.clone();
    allowed.local.id += 100; // native Open admission deliberately does not require full equality.
    allowed.local.node.id = [9; 16];
    node.qualification_ingress(
        lease
            .qualification_reply("ReplicaOpenReply", to_vec(&allowed).unwrap())
            .unwrap(),
    )
    .unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready)
    })
    .await;
    assert_eq!(
        node.snapshot(b.unit.id.guid.0)
            .committed
            .unwrap()
            .body
            .replica
            .id,
        b.replica.id
    );
    node.qualification_hold_replies(false);
    node.close(b.unit.id.clone(), ra::CloseMode::Close).unwrap();
    until(&mut node, |n| count(n, "LOCAL_DISPOSE begin") == 1).await;
    let mut stale_close = original.clone();
    stale_close.local.id -= 1;
    node.qualification_ingress(
        lease
            .qualification_reply("ReplicaCloseReply", to_vec(&stale_close).unwrap())
            .unwrap(),
    )
    .unwrap();
    advance(&mut node, Duration::from_millis(30)).await;
    assert_eq!(
        node.snapshot(b.unit.id.guid.0)
            .committed
            .unwrap()
            .close_failures,
        0
    );
    let mut failed = original;
    failed.error.error = 0x80004005u32 as i32 as i64;
    node.qualification_ingress(
        lease
            .qualification_reply("ReplicaCloseReply", to_vec(&failed).unwrap())
            .unwrap(),
    )
    .unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.close_failures == 1)
    })
    .await;
    assert_eq!(node.retained_usage(), 1);
    assert!(resource(&node).exists());
    fs::write(
        resource(&node).with_file_name("local-dispose.release"),
        b"actual disposal",
    )
    .unwrap();
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Closed)
            && n.retained_usage() == 0
    })
    .await;
    assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
    cleanup(&mut node, &store).await;
}

#[tokio::test]
async fn production_reply_adapter_rejects_malformed_and_foreign_session_without_losing_cleanup() {
    use fube_node::local::rap;
    use fube_transport::serialization::serde::{to_vec, ObjectArray};
    for foreign in [false, true] {
        let fixture = Fixture::new();
        let other_fixture = Fixture::new();
        let (mut node, store) = fixture.node("").await;
        let (mut other, other_store) = other_fixture.node("").await;
        until(&mut node, |n| n.package_registration().registered).await;
        until(&mut other, |n| n.package_registration().registered).await;
        let b = body(&node);
        node.add(b.clone()).unwrap();
        until(&mut node, |n| {
            n.snapshot(b.unit.id.guid.0)
                .committed
                .is_some_and(|s| s.phase == ra::Phase::Ready)
        })
        .await;
        let before = node.snapshot(b.unit.id.guid.0).committed.unwrap();
        let source = if foreign {
            other.activation().host_lease().unwrap()
        } else {
            node.activation().host_lease().unwrap()
        };
        let bytes = if foreign {
            to_vec(&rap::Reply {
                unit: before.body.unit.clone(),
                local: before.body.replica.clone(),
                remote: ObjectArray(vec![]),
                error: rap::ProxyError {
                    error: 0,
                    message: String::new(),
                    api: Default::default(),
                },
                flags: 0,
            })
            .unwrap()
        } else {
            vec![1, 2, 3]
        };
        node.qualification_ingress(
            source
                .qualification_reply("ReplicaCloseReply", bytes)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(node.drive(), Err(fube_node::local::Error::Protocol));
        assert_eq!(node.retained_usage(), 1);
        assert_eq!(
            node.snapshot(b.unit.id.guid.0)
                .committed
                .unwrap()
                .body
                .replica
                .id,
            before.body.replica.id
        );
        // First rejected opaque input stays diagnostic, not a head that blocks
        // the actual HostClosed/native retirement/exact release after revocation.
        until(&mut node, |n| {
            n.snapshot(b.unit.id.guid.0)
                .committed
                .is_some_and(|s| s.phase == ra::Phase::Closed)
                && n.retained_usage() == 0
        })
        .await;
        cleanup(&mut node, &store).await;
        cleanup(&mut other, &other_store).await;
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
