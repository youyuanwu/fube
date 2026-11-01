//! Actual owned node/Hosting/RA paths with explicit trusted qualification
//! readiness and simulated FT creation. No H2 acquisition/RAP/FM completion.
use fube_hosting::{self as hosting, ipc::*, wire};
use fube_node::{Config, Handle, Input, Node, Response};
use fube_ra::{entity as ra, protocol as p};
use fube_ra_store::{
    Backend, BackendContext, MemoryBackend, Output, Request as StoreRequest,
    Settings as StoreSettings, Store, StoreFuture,
};
use fube_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId, EXPECTS_REPLY, IPC},
    serialization::serde::{from_slice, to_vec, Guid, ObjectArray, Pointer},
};
use serde::Serialize;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::net::TcpStream;
const WAIT: Duration = Duration::from_secs(3);

struct PausedWrite {
    inner: MemoryBackend,
    gate: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
    writes: Arc<AtomicUsize>,
}
impl Backend for PausedWrite {
    fn initialize(
        &mut self,
        settings: StoreSettings,
        context: BackendContext,
    ) -> StoreFuture<'_, ()> {
        self.inner.initialize(settings, context)
    }
    fn execute(&mut self, request: StoreRequest) -> StoreFuture<'_, Output> {
        Box::pin(async move {
            if matches!(request, StoreRequest::Insert(_)) {
                self.writes.fetch_add(1, Ordering::SeqCst);
                if let Some((entered, release)) = self.gate.take() {
                    let _ = entered.send(());
                    release.await.unwrap();
                }
            }
            self.inner.execute(request).await
        })
    }
    fn close(&mut self) -> StoreFuture<'_, ()> {
        self.inner.close()
    }
}
fn host_config(duration: Duration) -> hosting::ipc::Config {
    hosting::ipc::Config {
        node: wire::StartReply {
            node_id: "1".into(),
            node_pid: std::process::id(),
            lease_handle: 0,
            lease_duration: 0,
            error: 0,
            node_instance: 1,
            node_name: "local".into(),
            node_type: "local".into(),
            client_address: "127.0.0.1:19000".into(),
            deployment: "/tmp/prepared".into(),
            ip: "127.0.0.1".into(),
            work: "/tmp".into(),
            initial_ttl: 0,
            application_directories: ObjectArray(vec![]),
            node_directories: ObjectArray(vec![]),
            application_log: Pointer(None),
            system_log: Pointer(None),
        },
        core_limits: Default::default(),
        max_admissions: 2,
        scratch_bytes: 16 * 1024 * 1024,
        decode_bytes: 256 * 1024,
        projection_bytes: 256 * 1024,
        reply_bytes: 64 * 1024,
        lease_duration: duration,
    }
}
fn host_admission() -> Admission {
    Admission {
        deadlines: RequestDeadlines::Propagated,
        host: wire::HostContext {
            id: uuid::Uuid::new_v4().to_string(),
            kind: 1,
            pid: std::process::id(),
            container: false,
            activator: false,
        },
        code: wire::CodeContext {
            id: wire::CodeId {
                package: wire::PackageInstance {
                    package: wire::PackageId {
                        application: wire::ApplicationId {
                            type_name: "App".into(),
                            number: 1,
                        },
                        name: "Pkg".into(),
                    },
                    activation: wire::Activation {
                        guid: Guid([0; 16]),
                        mode: 0,
                    },
                    public_id: String::new(),
                },
                name: "Code".into(),
            },
            code_sequence: 1,
            package_sequence: 2,
            version: wire::VersionInstance {
                version: wire::PackageVersion {
                    application: wire::ApplicationVersion {
                        value: wire::Rollout { major: 1, minor: 0 },
                    },
                    value: wire::Rollout { major: 1, minor: 0 },
                },
                instance: 0,
            },
            application_name: "fabric:/App".into(),
        },
    }
}
fn ra_admission() -> ra::Admission {
    ra::Admission {
        local_node: p::NodeInstance {
            id: [4; 16],
            instance: 20,
        },
        fm_source: p::NodeInstance {
            id: [8; 16],
            instance: 30,
        },
        generation: p::GenerationNumber {
            generation: 7,
            owner: [8; 16],
        },
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}
fn ra_body() -> p::AddInstance {
    let mut b = p::AddInstance::default();
    b.unit.id.guid.0 = [1; 16];
    b.unit.consistency.id = b.unit.id.clone();
    b.unit.current = p::Epoch {
        configuration: 5,
        data_loss: 1,
    };
    b.replica.node = ra_admission().local_node;
    b.replica.id = 40;
    b.replica.instance = 50;
    b.replica.state = 1;
    b.replica.version = p::VersionInstance::default();
    b.service.name = "fabric:/App/service".into();
    b.service.service_type = p::ServiceType {
        package: p::PackageId {
            application: p::ApplicationId {
                type_name: "App".into(),
                number: 1,
            },
            name: "Pkg".into(),
        },
        name: "Widget".into(),
    };
    b
}
fn add() -> ra::Command {
    ra::Command::Add {
        body: Box::new(ra_body()),
        source: ra_admission().fm_source,
        generation: p::GenerationHeader {
            number: ra_admission().generation,
            for_fm_replica: false,
        },
    }
}
fn config() -> Config {
    let mut config = Config::registration(host_config(LOCAL_LEASE_DURATION));
    // Trusted qualification facts only; no deployed/native FM-ready assertion.
    config.readiness = ra::HostingReadiness {
        fm_ready: true,
        node_active: true,
        upgrading: false,
        ra_open: true,
        ra_closing: false,
    };
    config
}
fn request(handle: &Handle, input: Input) -> Response {
    handle.submit(input).unwrap().wait(WAIT).unwrap()
}
struct Peer {
    socket: TcpStream,
    admission: Admission,
    context: wire::RuntimeContext,
    guid: [u8; 16],
    index: u32,
}
impl Peer {
    async fn new(node: &Node, admission: Admission) -> Self {
        let mut socket = TcpStream::connect(node.address()).await.unwrap();
        write_message(
            &mut socket,
            &Message::listen_instance(
                MessageId {
                    guid: uuid::Uuid::new_v4().to_bytes_le(),
                    index: 1,
                },
                &ListenInstance::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        read_message(&mut socket).await.unwrap();
        let context = wire::RuntimeContext {
            id: uuid::Uuid::new_v4().to_string(),
            host: admission.host.clone(),
            code: admission.code.clone(),
        };
        Self {
            socket,
            admission,
            context,
            guid: uuid::Uuid::new_v4().to_bytes_le(),
            index: 0,
        }
    }
    async fn call<T: Serialize>(&mut self, actor: i64, action: &str, body: &T) -> Message {
        self.raw(actor, action, to_vec(body).unwrap()).await
    }
    async fn raw(&mut self, actor: i64, action: &str, body: Vec<u8>) -> Message {
        self.index += 1;
        let id = MessageId {
            guid: self.guid,
            index: self.index,
        };
        let mut headers = vec![
            Header::actor(actor).unwrap(),
            Header::message_id(id).unwrap(),
            Header::boolean(EXPECTS_REPLY, true).unwrap(),
            Header::timeout(50_000_000).unwrap(),
            Header::object(IPC, |e| {
                e.string(&self.admission.host.id)?;
                e.u32(self.admission.host.pid);
                Ok(())
            })
            .unwrap(),
        ];
        if !action.is_empty() {
            headers.push(Header::action(action).unwrap());
        }
        write_message(&mut self.socket, &Message { headers, body })
            .await
            .unwrap();
        let response = tokio::time::timeout(WAIT, read_message(&mut self.socket))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.properties().unwrap().relates_to, id);
        response
    }
    async fn bootstrap(&mut self) {
        let h = self.admission.host.clone();
        let reply = self
            .call(
                7,
                "StartRegisterApplicationHostRequest",
                &wire::StartRequest {
                    id: h.id.clone(),
                    kind: h.kind,
                    pid: h.pid,
                    timeout: 300_000_000,
                    container: false,
                    activator: false,
                },
            )
            .await;
        assert_eq!(
            from_slice::<wire::StartReply>(&reply.body).unwrap().error,
            0
        );
        let reply = self
            .call(
                7,
                "FinishRegisterApplicationHostRequest",
                &wire::IdRequest { id: h.id },
            )
            .await;
        assert_eq!(
            from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
            0
        );
        let context = self.context.clone();
        let reply = self
            .call(
                9,
                "RegisterFabricRuntimeRequest",
                &wire::RuntimeRequest { context },
            )
            .await;
        assert_eq!(
            from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
            0
        );
        assert!(self.poll().await > 0);
    }
    async fn factory(&mut self) {
        let request = wire::TypeRequest {
            context: self.context.clone(),
            id: wire::TypeId {
                package: self.context.code.id.package.clone(),
                name: "Widget".into(),
            },
        };
        let reply = self.call(9, "RegisterServiceTypeRequest", &request).await;
        assert_eq!(
            from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
            0
        );
    }
    async fn poll(&mut self) -> i64 {
        let reply = self.raw(15, "", vec![]).await;
        from_slice::<wire::LeaseReply>(&reply.body).unwrap().ttl
    }
}
async fn receipt(handle: &Handle) -> fube_node::Receipt {
    tokio::time::timeout(WAIT, async {
        loop {
            let mut receipts = handle.take_receipts();
            if !receipts.is_empty() {
                assert_eq!(receipts.len(), 1);
                return receipts.remove(0);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn actual_node_registration_lookup_all_five_consumers_and_owned_gated_residuals() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
    );
    ra.submit(add()).unwrap().wait(WAIT).await.unwrap();
    let issued_lookup = ra.take_effects();
    assert!(matches!(issued_lookup[0].effect, ra::Effect::Open { .. }));
    let before = ra.snapshot([1; 16]).committed.unwrap();
    let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), config(), ra.clone()).unwrap();
    let handle = node.handle();
    let a = host_admission();
    let Response::Prepared(prepared) = request(
        &handle,
        Input::Admit {
            admission: a.clone(),
            types: vec!["Widget".into(), "Probe".into()],
            source: "prepared-package".into(),
        },
    ) else {
        panic!("admission");
    };
    assert!(prepared.failure.is_none());
    let mut peer = Peer::new(&node, a).await;
    peer.bootstrap().await;
    peer.factory().await;
    let registered = receipt(&handle).await;
    assert!(
        matches!(&registered.input, Delivery::Registration(hosting::ClosedRegisteredEvent::Registered { registration, .. })
        if registration.runtime_id == peer.context.id && registration.package_instance_sequence == 2)
    );
    assert_eq!(
        registered.entities,
        vec![(
            [1; 16],
            ra::Status::Complete(Ok(ra::Disposition::HostingOpenRetryRequired))
        )]
    );
    assert!(!registered.downstream_completion);
    assert_eq!(ra.snapshot([1; 16]).committed.as_ref().unwrap(), &before);
    assert!(ra.take_effects().is_empty());
    let Response::Sources(sources) = request(&handle, Input::HealthSources) else {
        panic!("sources");
    };
    let widget = sources
        .iter()
        .find(|s| s.id.service_type.name == "Widget")
        .unwrap();
    let probe = sources
        .iter()
        .find(|s| s.id.service_type.name == "Probe")
        .unwrap();
    let Response::Lookup(lookup) = request(
        &handle,
        Input::Lookup(hosting::LookupRequest {
            service_type: widget.id.service_type.clone(),
            version: hosting::VersionInstance::default(),
            activation: hosting::Activation::SHARED,
            service_name: "fabric:/App/service".into(),
            application_name: "fabric:/App".into(),
            should_activate: false,
        }),
    ) else {
        panic!("lookup");
    };
    assert_eq!(lookup.result, Ok(()));
    assert_eq!(lookup.candidate.unwrap().runtime_id, peer.context.id);
    request(
        &handle,
        Input::PreparedFailure {
            id: probe.id.clone(),
            failure: "qualified-failure".into(),
            grace_ticks: 1,
        },
    );
    let disabled = receipt(&handle).await;
    assert!(matches!(
        disabled.input,
        Delivery::Status(hosting::EnabledDisabledEvent::Disabled { .. })
    ));
    assert_eq!(disabled.root, ra::Disposition::HostingStatusProcessed);
    let Response::Sources(sources) = request(&handle, Input::HealthSources) else {
        panic!("sources");
    };
    let report = sources
        .iter()
        .find(|s| s.id.service_type.name == "Probe")
        .unwrap()
        .last_disabled_report
        .as_ref()
        .unwrap();
    assert_eq!(report.kind, hosting::DisabledReportKind::TypeDisabled);
    assert_eq!(report.health_state, 3);
    request(&handle, Input::RemoveFailure("qualified-failure".into()));
    let enabled = receipt(&handle).await;
    assert!(matches!(
        enabled.input,
        Delivery::Status(hosting::EnabledDisabledEvent::Enabled { .. })
    ));
    assert_eq!(enabled.root, ra::Disposition::HostingStatusProcessed);
    let runtime = peer.context.id.clone();
    let reply = peer
        .call(
            9,
            "UnregisterFabricRuntimeRequest",
            &wire::IdRequest {
                id: runtime.clone(),
            },
        )
        .await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
        0
    );
    let closed = receipt(&handle).await;
    assert!(
        matches!(closed.input, Delivery::Registration(hosting::ClosedRegisteredEvent::RuntimeClosed { runtime_id, .. }) if runtime_id == runtime)
    );
    assert_eq!(
        closed.entities[0].1,
        ra::Status::Complete(Ok(ra::Disposition::IgnoredStale))
    );
    let host = peer.admission.host.id.clone();
    let reply = peer
        .call(
            7,
            "UnregisterApplicationHostRequest",
            &wire::UnregisterHost {
                id: host.clone(),
                activity: wire::Activity {
                    id: wire::ActivityId {
                        guid: Guid([2; 16]),
                        index: 1,
                    },
                    kind: 0,
                },
            },
        )
        .await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
        0
    );
    let closed = receipt(&handle).await;
    assert!(
        matches!(closed.input, Delivery::Registration(hosting::ClosedRegisteredEvent::HostClosed { host_id, .. }) if host_id == host)
    );
    node.stop().unwrap();
    let residual = handle.take_shutdown().unwrap();
    assert!(residual.pending.iter().any(|p| matches!(
        p.input,
        Delivery::Obligation(hosting::Obligation::TypesUnregistered { .. })
    ) && p.blocked
        == Some(fube_node::Error::Hosting(
            hosting::Error::UnsupportedCapability
        ))));
    assert!(residual
        .statuses
        .pending()
        .iter()
        .any(|s| s.service_type.name == "Probe" && s.enabled));
    assert_eq!(residual.statuses.message_sequence(), 2);
    assert_eq!(residual.owner.health_sources().unwrap().len(), 2);
    assert_eq!(ra.snapshot([1; 16]).committed.as_ref().unwrap(), &before);
    assert!(ra.take_effects().is_empty());
    ra.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn abandoned_command_observers_retain_bounded_output_slots_and_can_be_recovered() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
    );
    let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), config(), ra.clone()).unwrap();
    let handle = node.handle();
    for _ in 0..3 {
        drop(handle.submit(Input::HealthSources).unwrap());
    }
    assert!(matches!(
        handle.submit(Input::HealthSources),
        Err(fube_node::Error::Capacity)
    ));
    let mut recovered = Vec::new();
    tokio::time::timeout(WAIT, async {
        while recovered.len() < 3 {
            recovered.extend(handle.recover_commands());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(recovered.len(), 3);
    for (_, result) in recovered {
        assert!(matches!(result.unwrap(), Response::Sources(s) if s.is_empty()));
    }
    assert!(matches!(request(&handle, Input::HealthSources), Response::Sources(s) if s.is_empty()));
    node.stop().unwrap();
    handle.take_shutdown().unwrap();
    ra.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn unknown_store_retains_accepted_event_tickets_without_blocking_polls_or_shutdown() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let writes = Arc::new(AtomicUsize::new(0));
    let store_settings = StoreSettings {
        operation_timeout: Duration::from_millis(20),
        ..Default::default()
    };
    let store = Arc::new(
        Store::new(
            Box::new(PausedWrite {
                inner: MemoryBackend::new(),
                gate: Some((entered_tx, release_rx)),
                writes: writes.clone(),
            }),
            store_settings,
        )
        .await
        .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
    );
    let mut cfg = config();
    cfg.decision_deadline = Duration::from_millis(20);
    cfg.events_per_lane = 1;
    cfg.max_entities = 1;
    cfg.fanout_bytes_per_lane = 640;
    let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone()).unwrap();
    let handle = node.handle();
    let add = ra.submit(add()).unwrap();
    entered_rx.await.unwrap();
    assert_eq!(
        add.wait(Duration::from_millis(100)).await,
        Err(ra::Failure::Unknown)
    );
    assert!(ra.snapshot([1; 16]).fenced);
    assert!(ra.take_effects().is_empty());
    let a = host_admission();
    let Response::Prepared(prepared) = request(
        &handle,
        Input::Admit {
            admission: a.clone(),
            types: vec!["Widget".into()],
            source: "prepared-package".into(),
        },
    ) else {
        panic!("admit");
    };
    assert!(prepared.failure.is_none());
    let mut peer = Peer::new(&node, a).await;
    peer.bootstrap().await;
    peer.factory().await;
    tokio::time::timeout(WAIT, async {
        while handle.progress().2 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(handle.take_receipts().is_empty());
    assert_eq!(handle.fanout_usage()[0], 640);
    assert!(ra.take_effects().is_empty());
    for _ in 0..3 {
        assert!(peer.poll().await > 0);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let start = std::time::Instant::now();
    node.stop().unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    let residual = handle.take_shutdown().unwrap();
    assert_eq!(handle.fanout_usage()[0], 0); // explicit residual transfer releases the reservation
    let pending = residual
        .pending
        .iter()
        .find(|p| {
            matches!(
                p.input,
                Delivery::Registration(hosting::ClosedRegisteredEvent::Registered { .. })
            )
        })
        .unwrap();
    assert_eq!(pending.blocked, Some(fube_node::Error::Unknown));
    let event_ticket = pending.tickets[0].as_ref().unwrap();
    assert_eq!(event_ticket.status(), ra::Status::Pending);
    assert!(ra.snapshot([1; 16]).fenced);
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    assert_eq!(add.resolve(WAIT).await.unwrap(), ra::Disposition::Applied);
    assert_eq!(
        event_ticket.resolve(WAIT).await.unwrap(),
        ra::Disposition::HostingOpenRetryRequired
    );
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert!(!ra.snapshot([1; 16]).fenced);
    let effects = ra.take_effects();
    assert_eq!(effects.len(), 1);
    assert!(matches!(effects[0].effect, ra::Effect::Open { .. }));
    assert_eq!(effects[0].operation, add.id);
    ra.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn full_fanout_budget_preserves_empty_closure_head_and_uncollected_receipt_ownership() {
    for shortage in [true, false] {
        let store = Arc::new(
            Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
                .await
                .unwrap(),
        );
        let ra = Arc::new(
            ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
        );
        for id in [1u8, 2] {
            let mut b = ra_body();
            b.unit.id.guid.0 = [id; 16];
            b.unit.consistency.id = b.unit.id.clone();
            ra.submit(ra::Command::Add {
                body: Box::new(b),
                source: ra_admission().fm_source,
                generation: p::GenerationHeader {
                    number: ra_admission().generation,
                    for_fm_replica: false,
                },
            })
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
        }
        let owned_lookup_effects = ra.take_effects();
        assert_eq!(owned_lookup_effects.len(), 2);
        let mut cfg = config();
        cfg.max_entities = 2;
        cfg.event_bytes_per_lane = 160;
        cfg.events_per_lane = 2;
        cfg.fanout_bytes_per_lane = 128 + 2 * 512 - usize::from(shortage);
        let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone()).unwrap();
        let handle = node.handle();
        let a = host_admission();
        let Response::Prepared(prepared) = request(
            &handle,
            Input::Admit {
                admission: a.clone(),
                types: vec!["Widget".into()],
                source: "prepared-package".into(),
            },
        ) else {
            panic!("admit");
        };
        assert!(prepared.failure.is_none());
        let mut peer = Peer::new(&node, a).await;
        peer.bootstrap().await;
        let id = peer.context.id.clone();
        peer.call(9, "UnregisterFabricRuntimeRequest", &wire::IdRequest { id })
            .await;
        let id = peer.admission.host.id.clone();
        peer.call(
            7,
            "UnregisterApplicationHostRequest",
            &wire::UnregisterHost {
                id,
                activity: wire::Activity {
                    id: wire::ActivityId {
                        guid: Guid([2; 16]),
                        index: 1,
                    },
                    kind: 0,
                },
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        if shortage {
            assert_eq!(handle.progress().0[0], 0);
            assert_eq!(handle.fanout_usage()[0], 0);
            assert!(handle.take_receipts().is_empty());
            node.stop().unwrap();
            let residual = handle.take_shutdown().unwrap();
            assert!(
                matches!(residual.owner.next_delivery(DeliveryLane::Registration, usize::MAX).unwrap(),
                Some(Delivery::Registration(hosting::ClosedRegisteredEvent::RuntimeClosed { affected, .. })) if affected.is_empty())
            );
        } else {
            assert_eq!(handle.progress().0[0], 1); // second head cannot steal retained result budget
            assert_eq!(handle.fanout_usage()[0], 1152);
            let first = handle.take_receipts();
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].entities.len(), 2);
            assert!(
                matches!(&first[0].input, Delivery::Registration(hosting::ClosedRegisteredEvent::RuntimeClosed { affected, .. }) if affected.is_empty())
            );
            let second = receipt(&handle).await;
            assert!(matches!(
                second.input,
                Delivery::Registration(hosting::ClosedRegisteredEvent::HostClosed { .. })
            ));
            assert_eq!(second.entities.len(), 2);
            assert_eq!(handle.fanout_usage()[0], 0);
            assert_eq!(handle.progress().0[0], 0);
            assert!(handle.take_receipts().is_empty());
            node.stop().unwrap();
            handle.take_shutdown().unwrap();
        }
        ra.close(WAIT).await.unwrap();
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn checked_command_reservations_reject_per_command_and_aggregate_overflow_before_enqueue() {
    for aggregate in [false, true] {
        let store = Arc::new(
            Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
                .await
                .unwrap(),
        );
        let ra = Arc::new(
            ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
        );
        let mut cfg = config();
        cfg.queued_command_bytes = usize::MAX;
        cfg.hosting.core_limits.state_bytes = if aggregate {
            usize::MAX - 1536
        } else {
            usize::MAX
        };
        let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone()).unwrap();
        let handle = node.handle();
        if aggregate {
            let first = handle.submit(Input::HealthSources).unwrap();
            assert!(matches!(
                handle.submit(Input::RemoveFailure("x".into())),
                Err(fube_node::Error::Capacity)
            ));
            assert!(matches!(first.wait(WAIT).unwrap(), Response::Sources(s) if s.is_empty()));
            assert!(matches!(
                request(&handle, Input::RemoveFailure("x".into())),
                Response::Done
            ));
        } else {
            assert!(matches!(
                handle.submit(Input::HealthSources),
                Err(fube_node::Error::Capacity)
            ));
        }
        assert!(handle.recover_commands().is_empty());
        node.stop().unwrap();
        assert!(handle.take_shutdown().unwrap().shutdown.is_ok());
        ra.close(WAIT).await.unwrap();
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn terminal_ra_loss_fences_existing_factory_and_new_admission_without_another_event() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let store = Arc::new(
        Store::new(
            Box::new(PausedWrite {
                inner: MemoryBackend::new(),
                gate: Some((entered_tx, release_rx)),
                writes: Arc::new(AtomicUsize::new(0)),
            }),
            StoreSettings {
                operation_timeout: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
    );
    let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), config(), ra.clone()).unwrap();
    let handle = node.handle();
    let add = ra.submit(add()).unwrap();
    entered_rx.await.unwrap();
    let terminate = ra
        .submit(ra::Command::QualificationTerminate(ra_body().unit.id))
        .unwrap();
    let a = host_admission();
    let Response::Prepared(prepared) = request(
        &handle,
        Input::Admit {
            admission: a.clone(),
            types: vec!["Widget".into()],
            source: "prepared-package".into(),
        },
    ) else {
        panic!("admit");
    };
    assert!(prepared.failure.is_none());
    let mut peer = Peer::new(&node, a).await;
    peer.bootstrap().await;
    peer.factory().await;
    tokio::time::timeout(WAIT, async {
        while ra.statistics().outstanding < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(add.resolve(WAIT).await.unwrap(), ra::Disposition::Applied);
    assert_eq!(terminate.resolve(WAIT).await, Err(ra::Failure::Unknown));
    tokio::time::timeout(WAIT, async {
        while handle.progress().3.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!ra.consumer_lifetime().is_accepting());
    peer.index += 1;
    let id = MessageId {
        guid: peer.guid,
        index: peer.index,
    };
    let request = wire::TypeRequest {
        context: peer.context.clone(),
        id: wire::TypeId {
            package: peer.context.code.id.package.clone(),
            name: "Widget".into(),
        },
    };
    let headers = vec![
        Header::actor(9).unwrap(),
        Header::message_id(id).unwrap(),
        Header::boolean(EXPECTS_REPLY, true).unwrap(),
        Header::timeout(50_000_000).unwrap(),
        Header::action("RegisterServiceTypeRequest").unwrap(),
        Header::object(IPC, |e| {
            e.string(&peer.admission.host.id)?;
            e.u32(peer.admission.host.pid);
            Ok(())
        })
        .unwrap(),
    ];
    if write_message(
        &mut peer.socket,
        &Message {
            headers,
            body: to_vec(&request).unwrap(),
        },
    )
    .await
    .is_ok()
    {
        if let Ok(Ok(reply)) = tokio::time::timeout(WAIT, read_message(&mut peer.socket)).await {
            let error = from_slice::<wire::ErrorReply>(&reply.body).unwrap().error;
            assert_ne!(error, 0);
            assert_ne!(error, 0x80071d31u32 as i32 as i64);
        }
    }
    assert!(matches!(
        handle.submit(Input::Admit {
            admission: host_admission(),
            types: vec!["Other".into()],
            source: "new".into()
        }),
        Err(fube_node::Error::Closed)
    ));
    node.stop().unwrap();
    let residual = handle.take_shutdown().unwrap();
    assert!(residual.pending.iter().any(|p| p
        .tickets
        .iter()
        .any(|t| t.as_ref().is_some_and(ra::Ticket::owner_closed))));
    assert!(handle.take_receipts().is_empty());
    assert_eq!(ra.close(WAIT).await, Err(ra::Failure::Unknown));
    store.close().await.unwrap();
}

#[tokio::test]
async fn legal_multibyte_lookup_payloads_remain_reserved_until_once_only_collection() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
    );
    let mut cfg = config();
    cfg.queued_command_bytes = 300_000;
    let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone()).unwrap();
    let handle = node.handle();
    let mut a = host_admission();
    a.code.id.name = "界".repeat(28_000); // 56K native UTF-16 data, 84K retained UTF-8
    let Response::Prepared(prepared) = request(
        &handle,
        Input::Admit {
            admission: a.clone(),
            types: vec!["Widget".into()],
            source: "prepared-package".into(),
        },
    ) else {
        panic!("admit");
    };
    assert!(prepared.failure.is_none());
    let mut peer = Peer::new(&node, a).await;
    peer.bootstrap().await;
    peer.factory().await;
    let lookup = hosting::LookupRequest {
        service_type: hosting::ServiceTypeId {
            package: hosting::PackageId {
                application: hosting::ApplicationId {
                    type_name: "App".into(),
                    number: 1,
                },
                name: "Pkg".into(),
            },
            name: "widget".into(),
        },
        version: hosting::VersionInstance::default(),
        activation: hosting::Activation::SHARED,
        service_name: "fabric:/App/service".into(),
        application_name: "fabric:/App".into(),
        should_activate: false,
    };
    let first = handle.submit(Input::Lookup(lookup.clone())).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await; // completed but deliberately uncollected
    for _ in 0..3 {
        assert!(matches!(
            handle.submit(Input::Lookup(lookup.clone())),
            Err(fube_node::Error::Capacity)
        ));
    }
    let Response::Lookup(result) = first.wait(WAIT).unwrap() else {
        panic!("lookup");
    };
    assert_eq!(result.result, Ok(()));
    assert_eq!(result.candidate.unwrap().code_package.len(), 84_000);
    assert!(first.take().is_none());
    let mut mismatch = lookup.clone();
    mismatch.version.instance += 1;
    let Response::Lookup(result) = request(&handle, Input::Lookup(mismatch)) else {
        panic!("mismatch");
    };
    assert_eq!(result.result, Err(hosting::Error::UnsupportedCapability));
    assert!(result.sequence.is_some());
    let candidate = result.candidate.unwrap();
    assert_eq!(candidate.code_package.len(), 84_000);
    assert_eq!(candidate.type_instance.service_type.name, "widget");
    assert!(
        matches!(request(&handle, Input::Lookup(lookup)), Response::Lookup(r) if r.result.is_ok())
    );
    node.stop().unwrap();
    handle.take_shutdown().unwrap();
    ra.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn required_unsupported_consumers_are_rejected_before_profile_admission() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(store.clone(), ra_admission(), ra::Settings::default()).unwrap(),
    );
    for required in [
        RequiredCapabilities {
            health_reporting: true,
            ..Default::default()
        },
        RequiredCapabilities {
            application_reports: true,
            ..Default::default()
        },
        RequiredCapabilities {
            fm_status_completion: true,
            ..Default::default()
        },
        RequiredCapabilities {
            deployed_lifecycle: true,
            ..Default::default()
        },
    ] {
        let mut config = config();
        config.required = required;
        assert!(matches!(
            Node::bind("127.0.0.1:0".parse().unwrap(), config, ra.clone()),
            Err(fube_node::Error::Hosting(
                hosting::Error::UnsupportedCapability
            ))
        ));
    }
    ra.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}
