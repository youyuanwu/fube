//! Real owned process + production local RA/effect/H2/RAP bridge.
use fube_hosting::local::{Activation, Options};
use fube_node::local::{EffectReceipt, Node};
use fube_ra::{entity as ra, protocol::*};
use fube_ra_store::{MemoryBackend, Settings, Store};
use std::{fs, path::PathBuf, sync::Arc, time::Duration};

struct Fixture {
    parent: PathBuf,
    package: PathBuf,
    profile: fube_hosting::local::Profile,
}
impl Fixture {
    fn new() -> Self {
        Self::with_profile(fube_hosting::local::Profile::Stateless)
    }
    fn stateful() -> Self {
        Self::with_profile(fube_hosting::local::Profile::VolatileStatefulLifecycle)
    }
    fn primary() -> Self {
        Self::with_profile(fube_hosting::local::Profile::VolatileStatefulPrimaryLifecycle)
    }
    fn fm_primary() -> Self {
        Self::with_profile(fube_hosting::local::Profile::VolatileStatefulFmPrimary)
    }
    fn with_profile(profile: fube_hosting::local::Profile) -> Self {
        let parent = std::env::temp_dir().join(format!("ra-rap-{}", uuid::Uuid::new_v4()));
        let package = parent.join("input");
        fs::create_dir_all(package.join("LocalService/Code")).unwrap();
        fs::write(
            package.join("ApplicationManifest.xml"),
            if profile == fube_hosting::local::Profile::Stateless {
                include_str!("../pkg/ApplicationManifest.xml")
            } else {
                include_str!("../pkg-stateful/ApplicationManifest.xml")
            },
        )
        .unwrap();
        fs::write(
            package.join("LocalService/ServiceManifest.xml"),
            if profile == fube_hosting::local::Profile::Stateless {
                include_str!("../pkg/LocalService/ServiceManifest.xml")
            } else {
                include_str!("../pkg-stateful/LocalService/ServiceManifest.xml")
            },
        )
        .unwrap();
        fs::copy(
            std::env::var_os("CARGO_BIN_EXE_hosting-local-app").unwrap(),
            package.join("LocalService/Code/hosting-local-app"),
        )
        .unwrap();
        Self {
            parent,
            package,
            profile,
        }
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
        options.profile = self.profile;
        options.lease_duration = Duration::from_secs(2);
        let mut node = Node::new(
            Activation::new(options).unwrap(),
            store.clone(),
            admission(),
            ra::Settings::default(),
        )
        .unwrap();
        let args = match self.profile {
            fube_hosting::local::Profile::VolatileStatefulLifecycle => {
                format!("--stateful-lifecycle {args}")
            }
            fube_hosting::local::Profile::VolatileStatefulPrimaryLifecycle
            | fube_hosting::local::Profile::VolatileStatefulFmPrimary => {
                format!("--stateful-primary {args}")
            }
            _ => args.to_owned(),
        };
        node.start(
            &self.package,
            "fabric:/Local",
            &[("Arguments".into(), args)],
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

#[tokio::test]
async fn stateless_node_rejects_stateful_registration_profile_before_activation() {
    let fixture = Fixture::new();
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), Settings::default())
            .await
            .unwrap(),
    );
    let mut options = Options::new(fixture.parent.clone());
    options.profile = fube_hosting::local::Profile::VolatileStatefulRegistration;
    assert!(matches!(
        Node::new(
            Activation::new(options).unwrap(),
            store.clone(),
            admission(),
            ra::Settings::default()
        ),
        Err(fube_node::local::Error::Unsupported)
    ));
    assert_eq!(fs::read_dir(&fixture.parent).unwrap().count(), 1);
    store.close().await.unwrap();
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
    if matches!(
        a.profile(),
        fube_hosting::local::Profile::VolatileStatefulLifecycle
            | fube_hosting::local::Profile::VolatileStatefulPrimaryLifecycle
            | fube_hosting::local::Profile::VolatileStatefulFmPrimary
    ) {
        b.service.stateful = true;
        b.replica.current_role = if matches!(
            a.profile(),
            fube_hosting::local::Profile::VolatileStatefulPrimaryLifecycle
                | fube_hosting::local::Profile::VolatileStatefulFmPrimary
        ) {
            4
        } else {
            2
        };
    }
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
    let application_type = &node
        .activation()
        .premise()
        .unwrap()
        .package
        .package
        .application
        .type_name;
    node.activation().root().unwrap().join(format!(
        "deployment/{application_type}_App1/work/local-instance.resource"
    ))
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
async fn advance(node: &mut Node, duration: Duration) -> Vec<EffectReceipt> {
    let start = std::time::Instant::now();
    until(node, |_| start.elapsed() >= duration).await
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

fn data_endpoint(node: &Node) -> fube_data::wire::Endpoint {
    assert_eq!(count(node, "LOCAL_DATA constructed endpoint="), 1);
    node.activation()
        .logs()
        .iter()
        .find_map(|line| {
            line.split_once("LOCAL_DATA constructed endpoint=")
                .map(|(_, endpoint)| endpoint.trim().parse().unwrap())
        })
        .unwrap()
}

fn primary_endpoint(node: &Node) -> String {
    node.activation()
        .logs()
        .iter()
        .rev()
        .find_map(|line| {
            line.split_once("LOCAL_PRIMARY listening endpoint=")
                .map(|(_, endpoint)| endpoint.trim().to_owned())
        })
        .expect("actual app listener observation")
}
async fn primary_request(address: &str, command: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(command.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream
            .take(512)
            .read_to_string(&mut response)
            .await
            .unwrap();
        response
    })
    .await
    .expect("bounded independent app client")
}
async fn primary_ready(node: &mut Node, address: &str) {
    let end = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        until(node, |_| true).await;
        let response = primary_request(address, "read\n").await;
        if response.starts_with("OK ") {
            assert!(response.contains("read=Granted write=Granted ready=true"));
            return;
        }
        assert_eq!(response, "ERR NotPrimary\n");
        assert!(std::time::Instant::now() < end, "primary readiness timeout");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test]
async fn stateful_initial_primary_applies_acknowledges_and_reads_one_write_before_joined_close() {
    for args in ["--block-provider-dispose", "--block-dispose"] {
        let fixture = Fixture::primary();
        let (mut node, store) = fixture.node(args).await;
        until(&mut node, |n| n.package_registration().registered).await;
        native_free(&node);
        let mut b = body(&node);
        b.unit.current.configuration = (1 << 32) + 5;
        assert!(node.open_stateful(b.clone()).is_err());
        assert!(node.add(b.clone()).is_err());
        assert!(node
            .fm_work(fube_node::local::FmWork::DeleteReplica(
                Box::new(DeleteReplica::new(b.clone(), false)),
                admission().fm_source,
                GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: false
                },
            ))
            .is_err());
        for invalid in 0..3 {
            let mut rejected = b.clone();
            match invalid {
                0 => rejected.replica.current_role = 2,
                1 => rejected.service.minimum = 2,
                _ => rejected.unit.previous = b.unit.current,
            }
            assert!(node.open_stateful_primary(rejected).is_err());
        }
        assert_eq!(node.retained_usage(), 0);
        node.open_stateful_primary(b.clone()).unwrap();
        let effects = until(&mut node, |n| {
            n.snapshot(b.unit.id.guid.0)
                .committed
                .is_some_and(|s| s.phase == ra::Phase::Ready)
        })
        .await;
        let open = effects
            .iter()
            .find(|receipt| matches!(receipt.dispatch.effect, ra::Effect::RuntimeOpen { .. }))
            .unwrap()
            .dispatch
            .clone();
        assert!(effects.iter().any(|receipt| matches!(
            receipt.dispatch.effect,
            ra::Effect::LocalStatefulOpenReply(_)
        )));
        assert!(!effects
            .iter()
            .any(|receipt| matches!(receipt.dispatch.effect, ra::Effect::Reply(_))));
        let opened = node.snapshot(b.unit.id.guid.0).committed.unwrap();
        assert_eq!(
            (opened.body.replica.current_role, opened.body.replica.state),
            (4, 2)
        );
        let address = primary_endpoint(&node);
        let replication = data_endpoint(&node).address;
        assert_eq!(opened.body.replica.service_endpoint, address);
        assert_eq!(
            opened.body.replica.replication_endpoint,
            data_endpoint(&node).to_string()
        );
        assert!(std::net::TcpStream::connect(&replication).is_ok());
        assert_eq!(
            count(&node, "LOCAL_PRIMARY write_denied before_ready assigned=0"),
            1
        );
        primary_ready(&mut node, &address).await;
        let response = primary_request(&address, "write\n").await;
        assert_eq!(response, "OK role=Primary epoch=1:4294967301 read=Granted write=Granted ready=true assigned=1 applied=1 committed=1 value=acknowledged\n");
        assert_eq!(primary_request(&address, "read\n").await, response);
        node.open_stateful_primary(b.clone()).unwrap();
        replay(&mut node, &open, false).await;
        replay(&mut node, &open, true).await;
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        assert_eq!(count(&node, "LOCAL_PRIMARY acknowledged sequence=1"), 1);
        assert_eq!(node.retained_usage(), 1);
        node.close(b.unit.id.clone(), ra::CloseMode::Close).unwrap();
        let provider_barrier = args == "--block-provider-dispose";
        let effects = until(&mut node, |n| {
            count(
                n,
                if provider_barrier {
                    "LOCAL_DATA_PROVIDER dispose begin"
                } else {
                    "LOCAL_DISPOSE begin"
                },
            ) == 1
        })
        .await;
        let close = effects
            .iter()
            .find(|receipt| matches!(receipt.dispatch.effect, ra::Effect::Close { .. }))
            .unwrap()
            .dispatch
            .clone();
        advance(&mut node, Duration::from_millis(150)).await;
        assert_eq!(
            node.snapshot(b.unit.id.guid.0).committed.unwrap().phase,
            ra::Phase::Closing
        );
        assert_eq!(node.retained_usage(), 1);
        assert!(node.instance_disposal().is_none());
        assert!(
            node.activation()
                .session_status()
                .unwrap()
                .unwrap()
                .connected
        );
        let provider_resource = resource(&node).with_file_name("local-provider.resource");
        if provider_barrier {
            assert!(provider_resource.is_file());
            for command in ["write\n", "read\n"] {
                let response = primary_request(&address, command).await;
                assert!(
                    response == "ERR NotPrimary\n" || response == "ERR ObjectClosed\n",
                    "{response}"
                );
            }
        } else {
            assert!(!provider_resource.exists());
            assert!(std::net::TcpStream::connect(&address).is_err());
            assert_eq!(count(&node, "LOCAL_PRIMARY released"), 1);
        }
        fs::write(
            resource(&node).with_file_name(if provider_barrier {
                "local-provider-dispose.release"
            } else {
                "local-dispose.release"
            }),
            b"release actual disposer",
        )
        .unwrap();
        until(&mut node, |n| n.instance_disposal().is_some()).await;
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed written"), 1);
        assert_eq!(count(&node, "LOCAL_PRIMARY released"), 1);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
        assert!(!resource(&node).exists() && !provider_resource.exists());
        assert!(std::net::TcpStream::connect(&address).is_err());
        assert!(std::net::TcpStream::connect(&replication).is_err());
        assert_eq!(node.retained_usage(), 0);
        assert_eq!(
            node.external_pending()
                .iter()
                .filter(|effect| matches!(effect.effect, ra::Effect::StatefulClosePending { .. }))
                .count(),
            1
        );
        replay(&mut node, &close, false).await;
        replay(&mut node, &open, false).await;
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        assert!(node.open_stateful_primary(b.clone()).is_err());
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
        assert_eq!(node.activation().residency().resources().logical_bytes, 0);
    }
}

#[tokio::test]
async fn stateful_initial_primary_lease_loss_fences_opening_and_serving_without_adoption() {
    for opening in [true, false] {
        let fixture = Fixture::primary();
        let (mut node, store) = fixture
            .node(if opening { "--open-delay-ms 4000" } else { "" })
            .await;
        until(&mut node, |n| n.package_registration().registered).await;
        let mut b = body(&node);
        b.unit.current.configuration = (1 << 32) + 5;
        node.open_stateful_primary(b.clone()).unwrap();
        until(&mut node, |n| {
            if opening {
                count(n, "LOCAL_PRIMARY listening") == 1
            } else {
                n.snapshot(b.unit.id.guid.0)
                    .committed
                    .is_some_and(|s| s.phase == ra::Phase::Ready)
            }
        })
        .await;
        let address = primary_endpoint(&node);
        let replication = data_endpoint(&node).address;
        if opening {
            assert_eq!(
                primary_request(&address, "write\n").await,
                "ERR NotPrimary\n"
            );
            assert_eq!(count(&node, "LOCAL_PRIMARY acknowledged"), 0);
        } else {
            primary_ready(&mut node, &address).await;
            assert!(primary_request(&address, "write\n")
                .await
                .contains("assigned=1 applied=1 committed=1 value=acknowledged"));
        }
        let pid = node.activation().process_status().unwrap().pid;
        node.activation().host_lease().unwrap().stop_renewal();
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
        assert_eq!(node.retained_usage(), usize::from(opening));
        assert!(node.instance_disposal().is_none());
        if opening {
            node.close(b.unit.id.clone(), ra::CloseMode::Drop).unwrap();
            until(&mut node, |n| n.retained_usage() == 0).await;
        }
        until(&mut node, |_| {
            fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| s.contains(") Z "))
                && std::net::TcpStream::connect(&address).is_err()
                && std::net::TcpStream::connect(&replication).is_err()
        })
        .await;
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed"), 0);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 0);
        assert!(resource(&node).is_file());
        assert!(resource(&node)
            .with_file_name("local-provider.resource")
            .is_file());
        assert!(node.open_stateful_primary(b).is_err());
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
    }
}

#[tokio::test]
async fn stateful_fm_delete_waits_for_real_provider_and_service_disposal_before_reply_and_release()
{
    for args in ["--block-provider-dispose", "--block-dispose"] {
        let fixture = Fixture::fm_primary();
        let (mut node, store) = fixture.node(args).await;
        until(&mut node, |n| n.package_registration().registered).await;
        native_free(&node);
        let mut request = body(&node);
        request.unit.current.configuration = (1 << 32) + 5;
        let generation = GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        };
        node.add_primary_from_fm(request.clone(), &admission().fm_source, &generation)
            .unwrap();
        until(&mut node, |n| {
            n.snapshot(request.unit.id.guid.0)
                .committed
                .is_some_and(|s| s.phase == ra::Phase::Ready)
        })
        .await;
        let address = primary_endpoint(&node);
        let replication = data_endpoint(&node).address;
        primary_ready(&mut node, &address).await;
        let written = primary_request(&address, "write\n").await;
        assert!(written.contains("assigned=1 applied=1 committed=1 value=acknowledged"));
        assert_eq!(primary_request(&address, "read\n").await, written);
        for invalid in 0..4 {
            let mut body = DeleteReplica::new(request.clone(), false);
            let mut source = admission().fm_source;
            let mut header = generation.clone();
            match invalid {
                0 => body.replica.0.service.persisted = true,
                1 => source.instance += 1,
                2 => header.for_fm_replica = true,
                _ => header.number.generation -= 1,
            }
            assert!(node
                .fm_work(fube_node::local::FmWork::DeleteReplica(
                    Box::new(body),
                    source,
                    header
                ))
                .is_err());
        }
        assert!(node
            .fm_work(fube_node::local::FmWork::Remove(
                Box::new(RemoveInstance::new(request.clone(), false)),
                admission().fm_source,
                generation.clone()
            ))
            .is_err());
        node.fm_work(fube_node::local::FmWork::DeleteReplica(
            Box::new(DeleteReplica::new(request.clone(), false)),
            admission().fm_source,
            generation.clone(),
        ))
        .unwrap();
        let provider_barrier = args == "--block-provider-dispose";
        let dispatched = until(&mut node, |n| {
            count(
                n,
                if provider_barrier {
                    "LOCAL_DATA_PROVIDER dispose begin"
                } else {
                    "LOCAL_DISPOSE begin"
                },
            ) == 1
        })
        .await;
        let close = dispatched
            .iter()
            .find(|r| matches!(r.dispatch.effect, ra::Effect::Close { flags: 8, .. }))
            .unwrap()
            .dispatch
            .clone();
        assert!(!dispatched
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::DeleteReplicaReply(_))));
        let delayed = advance(&mut node, Duration::from_millis(150)).await;
        assert!(!delayed
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::DeleteReplicaReply(_))));
        assert_eq!(
            node.snapshot(request.unit.id.guid.0)
                .committed
                .unwrap()
                .phase,
            ra::Phase::Closing
        );
        assert_eq!(node.retained_usage(), 1);
        assert!(node.instance_disposal().is_none());
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        if provider_barrier {
            assert!(resource(&node)
                .with_file_name("local-provider.resource")
                .is_file());
            for command in ["write\n", "read\n"] {
                let response = primary_request(&address, command).await;
                assert!(
                    response == "ERR NotPrimary\n" || response == "ERR ObjectClosed\n",
                    "{response}"
                );
            }
        } else {
            assert!(!resource(&node)
                .with_file_name("local-provider.resource")
                .exists());
            assert!(std::net::TcpStream::connect(&address).is_err());
        }
        // A duplicate retains the same Closing lifetime, not another Close or use.
        node.fm_work(fube_node::local::FmWork::DeleteReplica(
            Box::new(DeleteReplica::new(request.clone(), false)),
            admission().fm_source,
            generation.clone(),
        ))
        .unwrap();
        fs::write(
            resource(&node).with_file_name(if provider_barrier {
                "local-provider-dispose.release"
            } else {
                "local-dispose.release"
            }),
            b"release actual disposer",
        )
        .unwrap();
        let completed = until(&mut node, |n| n.instance_disposal().is_some()).await;
        assert_eq!(
            completed
                .iter()
                .filter(|r| matches!(r.dispatch.effect, ra::Effect::DeleteReplicaReply(_)))
                .count(),
            1
        );
        assert_eq!(
            completed
                .iter()
                .filter(|r| matches!(r.dispatch.effect, ra::Effect::ReleaseUsage { .. }))
                .count(),
            1
        );
        assert_eq!(node.retained_usage(), 0);
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed written"), 1);
        assert_eq!(count(&node, "LOCAL_PRIMARY released"), 1);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
        assert!(node.external_pending().iter().all(|d| !matches!(
            d.effect,
            ra::Effect::StatefulClosePending { .. } | ra::Effect::StatefulDroppedPending(_)
        )));
        assert!(std::net::TcpStream::connect(&address).is_err());
        assert!(std::net::TcpStream::connect(&replication).is_err());
        let proof = node.instance_disposal().unwrap();
        assert_eq!(proof.body.replica.id, request.replica.id);
        replay(&mut node, &close, false).await;
        node.fm_work(fube_node::local::FmWork::DeleteReplica(
            Box::new(DeleteReplica::new(request.clone(), false)),
            admission().fm_source,
            generation,
        ))
        .unwrap();
        let duplicate = until(&mut node, |n| n.pending_commands() == 0 && n.quiescent()).await;
        assert!(duplicate
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::DeleteReplicaReply(_))));
        assert!(!duplicate
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::ReleaseUsage { .. })));
        assert_eq!(
            node.instance_disposal().unwrap().acquisition,
            proof.acquisition
        );
        node.add_primary_from_fm(
            request.clone(),
            &admission().fm_source,
            &GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        )
        .unwrap();
        let unchanged = until(&mut node, |n| n.pending_commands() == 0 && n.quiescent()).await;
        assert!(unchanged.iter().all(|receipt| !matches!(
            receipt.dispatch.effect,
            ra::Effect::RuntimeOpen { .. } | ra::Effect::ReplicaUp(_) | ra::Effect::PrimaryReply(_)
        )));
        assert_eq!(
            node.instance_disposal().unwrap().acquisition,
            proof.acquisition
        );
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        let mut successor = request;
        successor.replica.id += 1;
        successor.replica.instance += 1;
        let generation = GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        };
        for _ in 0..2 {
            node.add_primary_from_fm(successor.clone(), &admission().fm_source, &generation)
                .unwrap();
        }
        assert_eq!(node.pending_commands(), 2);
        let mut conflicting = successor.clone();
        conflicting.unit.current.configuration += 1;
        assert_eq!(
            node.add_primary_from_fm(conflicting, &admission().fm_source, &generation),
            Err(fube_node::local::Error::Busy)
        );
        until(&mut node, |node| {
            node.pending_commands() == 0
                && node
                    .snapshot(successor.unit.id.guid.0)
                    .committed
                    .is_some_and(|state| {
                        state.phase == ra::Phase::Ready
                            && state.body.replica.id == successor.replica.id
                    })
        })
        .await;
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 2);
        assert_eq!(node.retained_usage(), 1);
        let fresh_endpoint = primary_endpoint(&node);
        primary_ready(&mut node, &fresh_endpoint).await;
        assert!(primary_request(&fresh_endpoint, "read\n")
            .await
            .contains("assigned=0 applied=0 committed=0 value=\n"));
        assert_eq!(primary_request(&fresh_endpoint, "write\n").await, written);
        assert_eq!(primary_request(&fresh_endpoint, "read\n").await, written);
        node.fm_work(fube_node::local::FmWork::DeleteReplica(
            Box::new(DeleteReplica::new(successor.clone(), false)),
            admission().fm_source,
            generation,
        ))
        .unwrap();
        until(&mut node, |node| {
            node.instance_disposal()
                .is_some_and(|new_proof| new_proof.body.replica.id == successor.replica.id)
        })
        .await;
        assert_ne!(
            node.instance_disposal().unwrap().acquisition,
            proof.acquisition
        );
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
    }
}

#[tokio::test]
async fn stateful_fm_primary_lease_loss_retains_down_reporting_without_disposal_proof() {
    let fixture = Fixture::fm_primary();
    let (mut node, store) = fixture.node("").await;
    until(&mut node, |node| node.package_registration().registered).await;
    let mut body = body(&node);
    body.unit.current.configuration = (1 << 32) + 5;
    assert!(node.open_stateful_primary(body.clone()).is_err());
    let generation = GenerationHeader {
        number: admission().generation,
        for_fm_replica: false,
    };
    node.add_primary_from_fm(body.clone(), &admission().fm_source, &generation)
        .unwrap();
    let effects = until(&mut node, |node| {
        node.snapshot(body.unit.id.guid.0)
            .committed
            .is_some_and(|state| state.phase == ra::Phase::Ready)
    })
    .await;
    assert!(effects
        .iter()
        .any(|effect| matches!(effect.dispatch.effect, ra::Effect::PrimaryReply(_))));
    assert!(!effects.iter().any(|effect| matches!(
        effect.dispatch.effect,
        ra::Effect::LocalStatefulOpenReply(_)
    )));
    let address = primary_endpoint(&node);
    let replication = data_endpoint(&node).address;
    primary_ready(&mut node, &address).await;
    assert!(primary_request(&address, "write\n")
        .await
        .contains("assigned=1 applied=1 committed=1"));
    let pid = node.activation().process_status().unwrap().pid;
    node.activation().host_lease().unwrap().stop_renewal();
    until(&mut node, |node| {
        node.snapshot(body.unit.id.guid.0)
            .committed
            .is_some_and(|state| state.phase == ra::Phase::Closed)
            && node.retained_usage() == 0
    })
    .await;
    until(&mut node, |_| {
        fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| stat.contains(") Z "))
            && std::net::TcpStream::connect(&address).is_err()
            && std::net::TcpStream::connect(&replication).is_err()
    })
    .await;
    assert!(node.instance_disposal().is_none());
    assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed"), 0);
    assert!(node
        .external_pending()
        .iter()
        .any(|effect| matches!(effect.effect, ra::Effect::RequestReportRetry)));
    let closed = node.snapshot(body.unit.id.guid.0).committed.unwrap();
    assert_eq!(closed.fm_stage, ra::FmStage::ReplicaDown);
    assert!(!closed.local_deleted && !closed.cleanup_pending);
    assert!(!node
        .external_pending()
        .iter()
        .any(|effect| matches!(effect.effect, ra::Effect::StatefulClosePending { .. })));
    let root = node.activation().root().unwrap().to_path_buf();
    cleanup(&mut node, &store).await;
    assert!(!root.exists());
}

#[tokio::test]
async fn stateful_real_data_idle_denies_writes_and_joins_actual_provider_and_service_disposal() {
    for (mode, args) in [
        (ra::CloseMode::Close, "--block-provider-dispose"),
        (ra::CloseMode::Close, "--block-dispose"),
        (ra::CloseMode::Close, "--close-error-block-dispose"),
        (ra::CloseMode::ForceAbort, "--block-dispose"),
    ] {
        let provider_barrier = args == "--block-provider-dispose";
        let aborted = args == "--close-error-block-dispose" || mode == ra::CloseMode::ForceAbort;
        let fixture = Fixture::stateful();
        let (mut node, store) = fixture.node(&format!("--data-replicator {args}")).await;
        until(&mut node, |n| n.package_registration().registered).await;
        native_free(&node);
        assert_eq!(count(&node, "LOCAL_DATA constructed"), 0);
        let mut b = body(&node);
        b.unit.current.configuration = (1 << 32) + 5;
        let mut primary = b.clone();
        primary.replica.current_role = 4;
        assert!(node.open_stateful(primary).is_err());
        assert_eq!(node.retained_usage(), 0);
        node.open_stateful(b.clone()).unwrap();
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
        let opened = node.snapshot(b.unit.id.guid.0).committed.unwrap();
        assert_eq!(
            (opened.body.replica.current_role, opened.body.replica.state),
            (2, 1)
        );
        assert_eq!(opened.body.unit.current, b.unit.current);
        assert!(opened.body.replica.service_endpoint.is_empty());
        let endpoint = data_endpoint(&node);
        assert_eq!(
            opened.body.replica.replication_endpoint,
            endpoint.to_string()
        );
        let address = endpoint.address;
        assert!(std::net::TcpStream::connect(&address).is_ok());
        assert_eq!(
            count(
                &node,
                "LOCAL_DATA idle role=IdleSecondary ready=false assigned=0"
            ),
            1
        );
        assert_eq!(
            count(&node, "LOCAL_DATA write_denied NotPrimary assigned=0"),
            2
        );
        assert_eq!(count(&node, "LOCAL_CONTROL acquired"), 0);
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER acquired"), 1);
        let provider_resource = resource(&node).with_file_name("local-provider.resource");
        assert!(provider_resource.is_file());
        assert!(effects
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::LocalStatefulOpenReply(_))));
        assert!(!effects
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::Reply(_))));
        node.open_stateful(b.clone()).unwrap();
        replay(&mut node, &open, false).await;
        replay(&mut node, &open, true).await;
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        assert_eq!(count(&node, "LOCAL_DATA constructed"), 1);
        assert_eq!(count(&node, "LOCAL_DATA write_denied"), 2);
        assert_eq!(node.retained_usage(), 1);
        node.close(b.unit.id.clone(), mode).unwrap();
        let effects = until(&mut node, |n| {
            count(
                n,
                if provider_barrier {
                    "LOCAL_DATA_PROVIDER dispose begin"
                } else {
                    "LOCAL_DISPOSE begin"
                },
            ) == 1
        })
        .await;
        let close = effects
            .iter()
            .find(|r| matches!(r.dispatch.effect, ra::Effect::Close { .. }))
            .unwrap()
            .dispatch
            .clone();
        advance(&mut node, Duration::from_millis(150)).await;
        assert_eq!(
            node.snapshot(b.unit.id.guid.0).committed.unwrap().phase,
            ra::Phase::Closing
        );
        assert_eq!(node.retained_usage(), 1);
        assert!(node.instance_disposal().is_none());
        assert!(resource(&node).is_file());
        assert!(
            node.activation()
                .session_status()
                .unwrap()
                .unwrap()
                .connected
        );
        if provider_barrier {
            assert!(provider_resource.is_file());
            assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed"), 0);
            assert_eq!(count(&node, "LOCAL_DATA retired"), 0);
        } else {
            assert!(!provider_resource.exists());
            assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed empty"), 1);
            assert_eq!(count(&node, "LOCAL_DATA retired"), 1);
            assert!(std::net::TcpStream::connect(&address).is_err());
        }
        fs::write(
            resource(&node).with_file_name(if provider_barrier {
                "local-provider-dispose.release"
            } else {
                "local-dispose.release"
            }),
            b"release actual disposer",
        )
        .unwrap();
        until(&mut node, |n| n.instance_disposal().is_some()).await;
        assert!(!resource(&node).exists() && !provider_resource.exists());
        assert!(std::net::TcpStream::connect(&address).is_err());
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed empty"), 1);
        assert_eq!(count(&node, "LOCAL_DATA retired"), 1);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
        assert_eq!(count(&node, "LOCAL_CALLBACK abort"), usize::from(aborted));
        assert_eq!(node.retained_usage(), 0);
        assert_eq!(
            node.snapshot(b.unit.id.guid.0).committed.unwrap().fm_stage,
            ra::FmStage::None
        );
        assert_eq!(
            node.external_pending()
                .iter()
                .filter(|effect| {
                    matches!(effect.effect, ra::Effect::StatefulClosePending { .. })
                })
                .count(),
            1
        );
        assert!(!node.activation().process_status().unwrap().exit_observed);
        replay(&mut node, &close, false).await;
        replay(&mut node, &open, false).await;
        assert_eq!(count(&node, "LOCAL_DATA constructed"), 1);
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed"), 1);
        assert_eq!(node.retained_usage(), 0);
        assert!(node.open_stateful(b.clone()).is_err());
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
        assert_eq!(node.activation().residency().resources().logical_bytes, 0);
    }
}

#[tokio::test]
async fn stateful_real_data_lease_loss_fences_opening_and_idle_without_disposal_receipts() {
    for opening in [false, true] {
        let fixture = Fixture::stateful();
        let (mut node, store) = fixture
            .node(if opening {
                "--data-replicator --open-delay-ms 4000"
            } else {
                "--data-replicator"
            })
            .await;
        until(&mut node, |n| n.package_registration().registered).await;
        native_free(&node);
        let b = body(&node);
        node.open_stateful(b.clone()).unwrap();
        until(&mut node, |n| {
            if opening {
                count(n, "LOCAL_DATA constructed") == 1
            } else {
                n.snapshot(b.unit.id.guid.0)
                    .committed
                    .is_some_and(|s| s.phase == ra::Phase::Ready)
            }
        })
        .await;
        let address = data_endpoint(&node).address;
        assert!(std::net::TcpStream::connect(&address).is_ok());
        assert_eq!(
            count(&node, "LOCAL_DATA write_denied"),
            if opening { 0 } else { 2 }
        );
        let pid = node.activation().process_status().unwrap().pid;
        node.activation().host_lease().unwrap().stop_renewal();
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
        assert_eq!(node.retained_usage(), usize::from(opening));
        assert!(node.instance_disposal().is_none());
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
        // Observe the engine socket independently from the waitable leader.
        until(&mut node, |_| {
            fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| s.contains(") Z "))
                && std::net::TcpStream::connect(&address).is_err()
        })
        .await;
        assert_eq!(count(&node, "LOCAL_DATA_PROVIDER disposed"), 0);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 0);
        assert!(resource(&node).is_file());
        assert!(resource(&node)
            .with_file_name("local-provider.resource")
            .is_file());
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
    }
}

#[tokio::test]
async fn stateful_idle_open_close_replay_and_actual_disposal_preserve_nonprimary_authority() {
    for error in [false, true] {
        let fixture = Fixture::stateful();
        let (mut node, store) = fixture
            .node(if error {
                "--close-error-block-dispose"
            } else {
                "--block-dispose"
            })
            .await;
        until(&mut node, |n| n.package_registration().registered).await;
        native_free(&node);
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 0);
        let b = body(&node);
        assert!(matches!(
            node.add(b.clone()),
            Err(fube_node::local::Error::Unsupported)
        ));
        let mut primary = b.clone();
        primary.replica.current_role = 4;
        let mut secondary = b.clone();
        secondary.replica.current_role = 3;
        let mut epoch = b.clone();
        epoch.unit.current.configuration = 0;
        let mut stateless = b.clone();
        stateless.service.stateful = false;
        for rejected in [primary, secondary, epoch, stateless] {
            assert!(matches!(
                node.open_stateful(rejected),
                Err(fube_node::local::Error::Unsupported)
            ));
        }
        let mut persisted = b.clone();
        persisted.service.persisted = true;
        assert!(node.open_stateful(persisted).is_err());
        let mut oversized = b.clone();
        oversized.service.target = ra::MAX_CONFIGURATION_REPLICAS as i32 + 1;
        assert!(node.open_stateful(oversized).is_err());
        assert_eq!(node.retained_usage(), 0);
        node.open_stateful(b.clone()).unwrap();
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
        let opened = node.snapshot(b.unit.id.guid.0).committed.unwrap();
        assert_eq!(
            (opened.body.replica.current_role, opened.body.replica.state),
            (2, 1)
        );
        assert!(opened.body.replica.service_endpoint.is_empty());
        let address = opened.body.replica.replication_endpoint.clone();
        assert!(std::net::TcpStream::connect(&address).is_ok());
        assert_eq!(count(&node, "LOCAL_ACCESS denied"), 1);
        assert_eq!(count(&node, "LOCAL_CALLBACK role IdleSecondary"), 1);
        assert_eq!(count(&node, "LOCAL_CONTROL role IdleSecondary"), 1);
        assert!(effects
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::LocalStatefulOpenReply(_))));
        assert!(!effects
            .iter()
            .any(|r| matches!(r.dispatch.effect, ra::Effect::Reply(_))));
        node.open_stateful(b.clone()).unwrap();
        replay(&mut node, &open, false).await;
        replay(&mut node, &open, true).await;
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        assert_eq!(count(&node, "LOCAL_CALLBACK open"), 1);
        assert_eq!(node.retained_usage(), 1);
        node.close(b.unit.id.clone(), ra::CloseMode::Close).unwrap();
        let effects = until(&mut node, |n| count(n, "LOCAL_DISPOSE begin") == 1).await;
        let close = effects
            .iter()
            .find(|r| matches!(r.dispatch.effect, ra::Effect::Close { .. }))
            .unwrap()
            .dispatch
            .clone();
        advance(&mut node, Duration::from_millis(150)).await;
        assert_eq!(
            node.snapshot(b.unit.id.guid.0).committed.unwrap().phase,
            ra::Phase::Closing
        );
        assert_eq!(node.retained_usage(), 1);
        assert!(resource(&node).is_file());
        assert!(node.instance_disposal().is_none());
        assert_eq!(count(&node, "LOCAL_CONTROL released"), 1);
        assert!(std::net::TcpStream::connect(&address).is_err());
        fs::write(
            resource(&node).with_file_name("local-dispose.release"),
            b"dispose",
        )
        .unwrap();
        until(&mut node, |n| n.instance_disposal().is_some()).await;
        assert_eq!(count(&node, "LOCAL_CALLBACK abort"), usize::from(error));
        assert_eq!(count(&node, "LOCAL_CONTROL disposed"), 1);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
        assert!(!resource(&node).exists());
        assert_eq!(node.retained_usage(), 0);
        assert_eq!(
            node.snapshot(b.unit.id.guid.0).committed.unwrap().fm_stage,
            ra::FmStage::None
        );
        assert_eq!(
            node.external_pending()
                .iter()
                .filter(|effect| matches!(effect.effect, ra::Effect::StatefulClosePending { .. }))
                .count(),
            1
        );
        replay(&mut node, &close, false).await;
        replay(&mut node, &open, false).await;
        assert_eq!(count(&node, "LOCAL_CALLBACK create_replica"), 1);
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 1);
        assert!(
            node.open_stateful(b.clone()).is_err(),
            "no stateful reopen/adoption"
        );
        assert_eq!(count(&node, "LOCAL_CALLBACK role Primary"), 0);
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
        assert_eq!(node.activation().residency().resources().logical_bytes, 0);
    }
}

#[tokio::test]
async fn stateful_lease_loss_fences_open_and_opening_without_fabricating_disposal() {
    for opening in [false, true] {
        let fixture = Fixture::stateful();
        let (mut node, store) = fixture
            .node(if opening { "--open-delay-ms 4000" } else { "" })
            .await;
        until(&mut node, |n| n.package_registration().registered).await;
        let b = body(&node);
        node.open_stateful(b.clone()).unwrap();
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
        let pid = node.activation().process_status().unwrap().pid;
        node.activation().host_lease().unwrap().stop_renewal();
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
        assert!(node.instance_disposal().is_none());
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
        until(&mut node, |_| {
            fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| s.contains(") Z "))
        })
        .await;
        assert_eq!(count(&node, "LOCAL_DISPOSE complete"), 0);
        assert!(resource(&node).is_file());
        let root = node.activation().root().unwrap().to_path_buf();
        cleanup(&mut node, &store).await;
        assert!(!root.exists());
        let status = node.activation().process_status().unwrap();
        assert!(status.exit_observed && status.group_drained && status.reaped);
    }
}

#[tokio::test]
async fn stateful_lifecycle_activation_requires_the_original_consumer_before_launch() {
    for fixture in [Fixture::stateful(), Fixture::primary()] {
        let mut options = Options::new(fixture.parent.clone());
        options.profile = fixture.profile;
        let mut activation = Activation::new(options).unwrap();
        assert!(activation
            .start(&fixture.package, "fabric:/Local", &[])
            .is_err());
        assert!(activation.root().is_none());
        assert!(activation.process_status().is_none());
    }
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
        assert!(node.instance_disposal().is_none());
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
            node.instance_disposal().is_none(),
            "held real disposer cannot issue proof"
        );
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
        until(&mut node, |n| n.instance_disposal().is_some()).await;
        let proof = node.instance_disposal().unwrap();
        let mut expected_unit = b.unit.clone();
        // The native-created FT takes target/minimum from its service.
        expected_unit.target = 1;
        expected_unit.minimum = 1;
        assert_eq!(proof.body.unit, expected_unit);
        assert_eq!(proof.body.replica.id, 40);
        assert_eq!(proof.body.replica.instance, 50);
        assert!(proof.command > 0 && proof.ra_operation > 0);
        assert!(proof.lookup > 0 && proof.acquisition > 0);
        assert!(!proof.host.is_empty() && !proof.runtime.is_empty());
        cleanup(&mut node, &store).await;
        assert!(
            node.instance_disposal().is_none(),
            "retired owner cannot reauthorize"
        );
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
    assert!(
        node.instance_disposal().is_none(),
        "new authority invalidates old proof"
    );
    until(&mut node, |n| {
        n.snapshot(b.unit.id.guid.0)
            .committed
            .is_some_and(|s| s.phase == ra::Phase::Ready && s.body.replica.id == next.replica.id)
    })
    .await;
    replay(&mut node, &open, false).await;
    replay(&mut node, &close, false).await; // actual old request on this same session.
    assert!(
        node.instance_disposal().is_none(),
        "old reply cannot prove current cleanup"
    );
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
