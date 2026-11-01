//! Actual existing Rust factory client against the owned production node.
//! The blocked-store FT/readiness premise is qualification-only, not deployment.
use super::*;
use crate::hosting_tests::{marker, node, observe, Observation};
use fube_hosting::{ipc, wire as hw};
use fube_node::{Config, Input, Node, Response};
use fube_ra::{entity as ra, protocol as p};
use fube_ra_store::{
    Backend, BackendContext, MemoryBackend, Output, Request, Settings as StoreSettings, Store,
    StoreFuture,
};
use fube_transport::{
    connection::{read_message, write_message},
    message::MessageId,
    serialization::serde::{from_slice, to_vec},
};
use std::{collections::HashMap, process::Stdio, time::Instant};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{mpsc, Notify},
    task::JoinSet,
};

struct PauseStore {
    inner: MemoryBackend,
    release: Arc<Notify>,
    paused: bool,
}
impl Backend for PauseStore {
    fn initialize(
        &mut self,
        settings: StoreSettings,
        context: BackendContext,
    ) -> StoreFuture<'_, ()> {
        self.inner.initialize(settings, context)
    }
    fn execute(&mut self, request: Request) -> StoreFuture<'_, Output> {
        Box::pin(async move {
            if self.paused && matches!(request, Request::Insert(_)) {
                self.release.notified().await;
                self.paused = false;
            }
            self.inner.execute(request).await
        })
    }
    fn close(&mut self) -> StoreFuture<'_, ()> {
        self.inner.close()
    }
}
async fn run(mode: &str) {
    let started = Instant::now();
    let fixture = crate::packages::tests::Fixture::new();
    let release = Arc::new(Notify::new());
    let store = Arc::new(
        Store::new(
            Box::new(PauseStore {
                inner: MemoryBackend::new(),
                release: release.clone(),
                paused: mode == "factory-blocked",
            }),
            StoreSettings {
                operation_timeout: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let admission = ra::Admission {
        local_node: p::NodeInstance {
            id: [1; 16],
            instance: 1,
        },
        fm_source: p::NodeInstance {
            id: [2; 16],
            instance: 1,
        },
        generation: p::GenerationNumber::default(),
        owner_ready: mode == "factory-blocked",
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    };
    let ra = Arc::new(
        ra::Executor::new(store.clone(), admission.clone(), ra::Settings::default()).unwrap(),
    );
    let mut cfg = Config::registration(ipc::Config {
        node: node(&fixture.root),
        core_limits: Default::default(),
        max_admissions: 1,
        scratch_bytes: 8 * 1024 * 1024,
        decode_bytes: 256 * 1024,
        projection_bytes: 256 * 1024,
        reply_bytes: 64 * 1024,
        lease_duration: ipc::LOCAL_LEASE_DURATION,
    });
    if mode == "factory-blocked" {
        cfg.readiness = ra::HostingReadiness {
            fm_ready: true,
            node_active: true,
            upgrading: false,
            ra_open: true,
            ra_closing: false,
        };
        cfg.decision_deadline = Duration::from_millis(20);
    }
    let mut owned = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone()).unwrap();
    let handle = owned.handle();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut environment = crate::context::tests::env();
    environment.insert(
        "Fabric_RuntimeConnectionAddress".into(),
        listener.local_addr().unwrap().to_string(),
    );
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "hosting_tests::hosting_child", "--nocapture"])
        .env("FUBE_HOSTING_CHILD", mode)
        .envs(&environment)
        .env_remove("Fabric_ServicePackageActivationGuid")
        .env_remove("Fabric_ServicePackageActivationId")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut expected: hw::RuntimeContext =
        from_slice(&to_vec(&fixture.activation.runtime).unwrap()).unwrap();
    expected.host.pid = child.id().unwrap();
    let add = if mode == "factory-blocked" {
        let mut b = p::AddInstance::default();
        b.unit.id.guid.0 = [1; 16];
        b.unit.consistency.id = b.unit.id.clone();
        b.replica.node = admission.local_node.clone();
        b.replica.id = 1;
        b.replica.instance = 1;
        b.replica.state = 1;
        b.replica.version = from_slice(&to_vec(&expected.code.version).unwrap()).unwrap();
        b.service.version = b.replica.version.clone();
        b.service.name = "fabric:/app/service".into();
        b.service.service_type = p::ServiceType {
            package: from_slice(&to_vec(&expected.code.id.package.package).unwrap()).unwrap(),
            name: "Type".into(),
        };
        Some(
            ra.submit(ra::Command::Add {
                body: Box::new(b),
                source: admission.fm_source,
                generation: p::GenerationHeader {
                    number: admission.generation,
                    for_fm_replica: false,
                },
            })
            .unwrap(),
        )
    } else {
        None
    };
    let Response::Prepared(prepared) = handle
        .submit(Input::Admit {
            admission: ipc::Admission {
                host: expected.host,
                code: expected.code,
                deadlines: ipc::RequestDeadlines::Propagated,
            },
            types: vec!["Type".into()],
            source: "prepared-manifest".into(),
        })
        .unwrap()
        .wait(Duration::from_secs(3))
        .unwrap()
    else {
        panic!("prepared runtime admission");
    };
    assert!(prepared.failure.is_none());
    let (observed, mut observations) = mpsc::channel(64);
    let address = owned.address();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = TcpStream::connect(address).await.unwrap();
        let (mut cr, mut cw) = client.into_split();
        let (mut sr, mut sw) = server.into_split();
        let upstream = observed.clone();
        let requests = async move {
            while let Ok(m) = read_message(&mut cr).await {
                upstream.try_send(Observation::Request(m.clone())).unwrap();
                if write_message(&mut sw, &m).await.is_err() {
                    break;
                }
            }
        };
        let replies = async move {
            while let Ok(m) = read_message(&mut sr).await {
                observed.try_send(Observation::Reply(m.clone())).unwrap();
                if write_message(&mut cw, &m).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! { _ = requests => {}, _ = replies => {} }
    });
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().unwrap());
    let mut correlations = HashMap::<MessageId, (i64, String)>::new();
    let mut actions = Vec::new();
    let mut leases = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while leases.is_empty() {
            observe(
                observations.recv().await.unwrap(),
                &mut correlations,
                &mut actions,
                &mut leases,
            );
        }
    })
    .await
    .unwrap();
    marker(&mut stdout, "hosting-attached").await;
    let Response::Sources(sources) = handle
        .submit(Input::HealthSources)
        .unwrap()
        .wait(Duration::from_secs(3))
        .unwrap()
    else {
        panic!("sources");
    };
    let Response::HostStatus(status) = handle
        .submit(Input::HostStatus(prepared.lease.clone()))
        .unwrap()
        .wait(Duration::from_secs(3))
        .unwrap()
    else {
        panic!("status");
    };
    let Response::Lookup(lookup) = handle
        .submit(Input::Lookup(fube_hosting::LookupRequest {
            service_type: sources[0].id.service_type.clone(),
            version: status.runtimes[0].version.clone(),
            activation: fube_hosting::Activation::SHARED,
            service_name: "fabric:/app/service".into(),
            application_name: sources[0].application_name.clone(),
            should_activate: false,
        }))
        .unwrap()
        .wait(Duration::from_secs(3))
        .unwrap()
    else {
        panic!("lookup");
    };
    assert_eq!(lookup.result, Ok(()));
    assert_eq!(
        lookup.candidate.unwrap().runtime_id,
        status.runtimes[0].runtime_id
    );
    if mode == "factory-blocked" {
        marker(&mut stdout, "hosting-executor-entered").await;
        assert_eq!(
            add.as_ref().unwrap().wait(Duration::from_millis(100)).await,
            Err(ra::Failure::Unknown)
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while handle.progress().2 == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(ra.snapshot([1; 16]).fenced);
        assert!(ra.take_effects().is_empty());
        prepared.lease.revoke();
    }
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(if mode == "factory" { 0 } else { 70 }));
    if mode == "factory-blocked" {
        assert!(started.elapsed() < Duration::from_millis(1500));
    }
    tasks.join_next().await.unwrap().unwrap();
    while let Ok(o) = observations.try_recv() {
        observe(o, &mut correlations, &mut actions, &mut leases);
    }
    assert!(actions.contains(&(9, "RegisterServiceTypeRequest".into())));
    assert!(leases.iter().all(|ttl| *ttl > 0 && *ttl <= 300_000_000));
    if mode == "factory" {
        assert!(actions.contains(&(9, "UnregisterFabricRuntimeRequest".into())));
        assert!(actions.contains(&(7, "UnregisterApplicationHostRequest".into())));
    }
    owned.stop().unwrap();
    let residual = handle.take_shutdown().unwrap();
    let receipts = handle.take_receipts();
    if mode == "factory" {
        assert!(receipts.iter().any(|r| matches!(
            r.input,
            ipc::Delivery::Registration(fube_hosting::ClosedRegisteredEvent::Registered { .. })
        )));
        assert!(receipts.iter().any(|r| matches!(
            r.input,
            ipc::Delivery::Registration(fube_hosting::ClosedRegisteredEvent::RuntimeClosed { .. })
        )));
    } else {
        assert!(residual.pending.iter().any(|p| matches!(
            p.input,
            ipc::Delivery::Registration(fube_hosting::ClosedRegisteredEvent::Registered { .. })
        )));
        release.notify_one();
        add.unwrap().resolve(Duration::from_secs(3)).await.unwrap();
    }
    assert!(residual
        .owner
        .take_operations()
        .unwrap()
        .iter()
        .all(|r| r.primary.is_ok()));
    ra.close(Duration::from_secs(3)).await.unwrap();
    store.close().await.unwrap();
    println!(
        "actual Rust factory/node mode={mode}: polls={}, exit={}",
        leases.len(),
        status.code().unwrap()
    );
}
#[tokio::test]
async fn actual_factory_registration_lookup_and_owned_closure() {
    run("factory").await;
}
#[tokio::test]
async fn actual_factory_watchdog_ignores_unknown_store_and_blocked_caller() {
    run("factory-blocked").await;
}
