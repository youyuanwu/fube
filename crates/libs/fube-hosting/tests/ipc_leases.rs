use fube_hosting::{ipc::*, wire, ClosedRegisteredEvent, Error};
use fube_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId, EXPECTS_REPLY, IPC},
    serialization::serde::{from_slice, to_vec, Guid, ObjectArray, Pointer},
};
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::{io::AsyncWriteExt, net::TcpStream};

fn node() -> wire::StartReply {
    wire::StartReply {
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
        application_directories: ObjectArray(vec![wire::Pair {
            key: "work".into(),
            value: "/tmp".into(),
        }]),
        node_directories: ObjectArray(vec![]),
        application_log: Pointer(None),
        system_log: Pointer(None),
    }
}
fn admission() -> Admission {
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
                            type_name: "RustApp".into(),
                            number: 0,
                        },
                        name: "RustService".into(),
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
                    value: wire::Rollout { major: 2, minor: 0 },
                },
                instance: 3,
            },
            application_name: "fabric:/RustApp".into(),
        },
    }
}
fn server(duration: Duration) -> Server {
    Server::bind("127.0.0.1:0".parse().unwrap(), config(duration)).unwrap()
}
fn config(duration: Duration) -> Config {
    Config {
        node: node(),
        core_limits: Default::default(),
        max_admissions: 2,
        scratch_bytes: 16 * 1024 * 1024,
        decode_bytes: 256 * 1024,
        projection_bytes: 256 * 1024,
        reply_bytes: 64 * 1024,
        lease_duration: duration,
    }
}
struct Peer {
    socket: TcpStream,
    admission: Admission,
    runtime: String,
    guid: [u8; 16],
    sequence: std::cell::Cell<u32>,
}
impl Peer {
    async fn connect(server: &Server, admission: Admission) -> Self {
        let mut socket = TcpStream::connect(server.address()).await.unwrap();
        write_message(
            &mut socket,
            &Message::listen_instance(new_id(), &ListenInstance::default()).unwrap(),
        )
        .await
        .unwrap();
        let response = read_message(&mut socket).await.unwrap();
        assert_eq!(response.properties().unwrap().actor, 1);
        let listen = ListenInstance::decode(&response.body).unwrap();
        assert_eq!(listen.instance, 1);
        assert_ne!(listen.nonce, [0; 16]);
        Self {
            socket,
            admission,
            runtime: uuid::Uuid::new_v4().to_string(),
            guid: uuid::Uuid::new_v4().to_bytes_le(),
            sequence: std::cell::Cell::new(0),
        }
    }
    fn frame(&self, actor: i64, action: &str, body: Vec<u8>) -> Message {
        let index = self.sequence.get().checked_add(1).unwrap();
        self.sequence.set(index);
        let mut headers = vec![
            Header::actor(actor).unwrap(),
            Header::message_id(MessageId {
                guid: self.guid,
                index,
            })
            .unwrap(),
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
        Message { headers, body }
    }
    async fn raw(&mut self, request: Message) -> fube_transport::Result<Message> {
        let id = request.properties()?.id;
        write_message(&mut self.socket, &request).await?;
        let response = tokio::time::timeout(Duration::from_secs(2), read_message(&mut self.socket))
            .await
            .unwrap()?;
        assert_eq!(response.properties()?.relates_to, id);
        assert!(response.properties()?.action.is_empty());
        Ok(response)
    }
    async fn call<T: Serialize>(&mut self, actor: i64, action: &str, body: &T) -> Message {
        let frame = self.frame(actor, action, to_vec(body).unwrap());
        self.raw(frame).await.unwrap()
    }
    async fn start(&mut self, timeout: i64) -> wire::StartReply {
        let h = self.admission.host.clone();
        let response = self
            .call(
                7,
                "StartRegisterApplicationHostRequest",
                &wire::StartRequest {
                    id: h.id,
                    kind: h.kind,
                    pid: h.pid,
                    timeout,
                    container: h.container,
                    activator: h.activator,
                },
            )
            .await;
        from_slice(&response.body).unwrap()
    }
    async fn finish(&mut self) -> i64 {
        let id = self.admission.host.id.clone();
        let response = self
            .call(
                7,
                "FinishRegisterApplicationHostRequest",
                &wire::IdRequest { id },
            )
            .await;
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error
    }
    async fn register(&mut self) -> i64 {
        let context = wire::RuntimeContext {
            id: self.runtime.clone(),
            host: self.admission.host.clone(),
            code: self.admission.code.clone(),
        };
        let response = self
            .call(
                9,
                "RegisterFabricRuntimeRequest",
                &wire::RuntimeRequest { context },
            )
            .await;
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error
    }
    async fn bootstrap(&mut self) {
        let node = self.start(300_000_000).await;
        assert_eq!(node.error, 0);
        assert_eq!(node.lease_handle, 1);
        assert!(node.initial_ttl > 0 && node.initial_ttl <= 300_000_000);
        assert_eq!(node.application_directories.0.len(), 1);
        assert_eq!(self.finish().await, 0);
        assert_eq!(self.register().await, 0);
    }
    async fn poll(&mut self) -> fube_transport::Result<i64> {
        let frame = self.frame(15, "", vec![]);
        self.raw(frame)
            .await
            .and_then(|message| from_slice::<wire::LeaseReply>(&message.body).map(|v| v.ttl))
    }
}
fn new_id() -> MessageId {
    MessageId {
        guid: uuid::Uuid::new_v4().to_bytes_le(),
        index: 0,
    }
}
fn type_request(peer: &Peer, name: &str) -> wire::TypeRequest {
    wire::TypeRequest {
        context: wire::RuntimeContext {
            id: peer.runtime.clone(),
            host: peer.admission.host.clone(),
            code: peer.admission.code.clone(),
        },
        id: wire::TypeId {
            package: peer.admission.code.id.package.clone(),
            name: name.into(),
        },
    }
}

#[tokio::test]
async fn admitted_factory_lookup_duplicates_and_owned_health_disabled_sources() {
    let mut server = server(LOCAL_LEASE_DURATION);
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
        assert!(matches!(
            server.registration_owner(required),
            Err(Error::UnsupportedCapability)
        ));
    }
    let owner = server.registration_owner(Default::default()).unwrap();
    assert!(matches!(server.take_outputs(), Err(Error::InvalidState)));
    let a = admission();
    let prepared = server
        .admit_registration(&owner, a.clone(), vec!["Widget".into()], "prepared-package")
        .unwrap();
    assert_eq!(prepared.admitted, 1);
    assert!(prepared.failure.is_none());
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    assert!(peer.poll().await.unwrap() > 0);
    let request = type_request(&peer, "wIDGET");
    let response = peer.call(9, "RegisterServiceTypeRequest", &request).await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error,
        0
    );
    let source = owner.health_sources().unwrap().remove(0);
    let report = source.last_disabled_report.as_ref().unwrap();
    assert_eq!(report.property, "ServiceTypeRegistration:Widget");
    assert_eq!(report.application_name, "fabric:/RustApp");
    assert_eq!(report.node_name, "local");
    let lookup = owner
        .lookup(&fube_hosting::LookupRequest {
            service_type: fube_hosting::ServiceTypeId {
                name: "widget".into(),
                ..source.id.service_type.clone()
            },
            version: server.status(&prepared.lease).unwrap().runtimes[0]
                .version
                .clone(),
            activation: fube_hosting::Activation::SHARED,
            service_name: "fabric:/RustApp/service".into(),
            application_name: "fabric:/RustApp".into(),
            should_activate: false,
        })
        .unwrap();
    assert_eq!(lookup.result, Ok(()));
    let candidate = lookup.candidate.unwrap();
    assert_eq!(candidate.type_instance.service_type.name, "widget");
    assert_eq!(candidate.package_instance_sequence, 2);
    let events = owner.take_outputs().unwrap();
    assert!(
        matches!(events.events.as_slice(), [ClosedRegisteredEvent::Registered { registration, .. }]
            if registration.type_instance.service_type.name == "Widget")
    );
    let response = peer.call(9, "RegisterServiceTypeRequest", &request).await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error,
        0x80071d31u32 as i32 as i64
    );
    assert!(owner.take_outputs().unwrap().events.is_empty());
    let response = peer
        .call(
            9,
            "RegisterServiceTypeRequest",
            &type_request(&peer, "Missing"),
        )
        .await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error,
        0x80071c21u32 as i32 as i64
    );
    assert_eq!(owner.health_sources().unwrap()[0], source);
    drop(peer);
    disconnected(&server, &prepared.lease).await;
    let outputs = owner.take_outputs().unwrap();
    assert!(outputs
        .events
        .iter()
        .any(|e| matches!(e, ClosedRegisteredEvent::HostClosed { .. })));
    let removed = owner
        .remove_declaration(&source.id, "prepared-package")
        .unwrap();
    assert_eq!(removed, Some(source));
    assert!(owner.health_sources().unwrap().is_empty());
    server.stop().unwrap();
}

#[tokio::test]
async fn factory_context_and_consumer_withdrawal_are_fail_closed_with_residuals() {
    for change in 0..5 {
        let mut server = server(LOCAL_LEASE_DURATION);
        let owner = server.registration_owner(Default::default()).unwrap();
        let a = admission();
        let prepared = server
            .admit_registration(&owner, a.clone(), vec!["Widget".into()], "prepared-package")
            .unwrap();
        let mut peer = Peer::connect(&server, a).await;
        peer.bootstrap().await;
        let mut request = type_request(&peer, "Widget");
        match change {
            0 => request.context.code.code_sequence += 1,
            1 => request.context.code.version.instance += 1,
            2 => request.id.package.activation.mode = 1,
            3 => request.context.host.pid += 1,
            _ => {}
        }
        if change == 4 {
            drop(owner);
        } else {
            let _retained_owner = owner;
            let frame = peer.frame(9, "RegisterServiceTypeRequest", to_vec(&request).unwrap());
            assert!(peer.raw(frame).await.is_err());
            disconnected(&server, &prepared.lease).await;
            assert!(!_retained_owner
                .take_outputs()
                .unwrap()
                .events
                .iter()
                .any(|e| matches!(e, ClosedRegisteredEvent::Registered { .. })));
            server.stop().unwrap();
            continue;
        }
        let frame = peer.frame(9, "RegisterServiceTypeRequest", to_vec(&request).unwrap());
        assert!(peer.raw(frame).await.is_err());
        disconnected(&server, &prepared.lease).await;
        assert!(!server
            .take_registration_residuals()
            .unwrap()
            .events
            .iter()
            .any(|e| matches!(e, ClosedRegisteredEvent::Registered { .. })));
        server.stop().unwrap();
    }
}
#[test]
fn registration_preparation_capacity_retains_prefix_and_never_admits_failed_profile() {
    let mut cfg = config(LOCAL_LEASE_DURATION);
    cfg.core_limits.sources = 1;
    let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let owner = server.registration_owner(Default::default()).unwrap();
    let prepared = server
        .admit_registration(
            &owner,
            admission(),
            vec!["First".into(), "Second".into()],
            "prepared-package",
        )
        .unwrap();
    assert_eq!(prepared.admitted, 1);
    assert_eq!(prepared.failure, Some((1, Error::Capacity)));
    let sources = owner.health_sources().unwrap();
    assert_eq!(sources.len(), 1);
    assert!(sources[0].last_disabled_report.is_none());
    assert!(!server.status(&prepared.lease).unwrap().registered_host);
    server.release(prepared.lease).unwrap();
    owner
        .remove_declaration(&sources[0].id, "prepared-package")
        .unwrap();
    assert!(owner.health_sources().unwrap().is_empty());
    assert!(owner.close().unwrap().events.is_empty());
    let next = server.registration_owner(Default::default()).unwrap();
    assert!(next.close().unwrap().events.is_empty());
    server.stop().unwrap();
}

#[tokio::test]
async fn bootstrap_profile_does_not_inherit_factory_capability() {
    let mut server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let request = type_request(&peer, "Widget");
    let frame = peer.frame(9, "RegisterServiceTypeRequest", to_vec(&request).unwrap());
    assert!(peer.raw(frame).await.is_err());
    disconnected(&server, &lease).await;
    assert!(!server
        .take_outputs()
        .unwrap()
        .events
        .iter()
        .any(|e| matches!(e, ClosedRegisteredEvent::Registered { .. })));
    server.stop().unwrap();
}

#[tokio::test]
async fn factory_other_runtime_duplicate_preserves_original_owner_and_one_health_source() {
    let mut server = server(LOCAL_LEASE_DURATION);
    let owner = server.registration_owner(Default::default()).unwrap();
    let a = admission();
    let b = admission();
    let first = server
        .admit_registration(
            &owner,
            a.clone(),
            vec!["Widget".into()],
            "first-package-component",
        )
        .unwrap();
    let second = server
        .admit_registration(
            &owner,
            b.clone(),
            vec!["widget".into()],
            "second-package-component",
        )
        .unwrap();
    assert!(first.failure.is_none() && second.failure.is_none());
    assert_eq!(owner.health_sources().unwrap().len(), 1);
    let mut p = Peer::connect(&server, a).await;
    let mut q = Peer::connect(&server, b).await;
    p.bootstrap().await;
    q.bootstrap().await;
    let request = type_request(&p, "Widget");
    let reply = p.call(9, "RegisterServiceTypeRequest", &request).await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
        0
    );
    let report = owner.health_sources().unwrap();
    let request = type_request(&q, "widget");
    let reply = q.call(9, "RegisterServiceTypeRequest", &request).await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
        0x80071bd1u32 as i32 as i64
    );
    assert_eq!(owner.health_sources().unwrap(), report);
    let events = owner.take_outputs().unwrap();
    assert_eq!(
        events
            .events
            .iter()
            .filter(|e| matches!(e, ClosedRegisteredEvent::Registered { .. }))
            .count(),
        1
    );
    drop(p);
    drop(q);
    disconnected(&server, &first.lease).await;
    disconnected(&server, &second.lease).await;
    server.stop().unwrap();
}

#[tokio::test]
async fn positive_but_expired_factory_deadline_never_publishes_registration() {
    let mut server = server(LOCAL_LEASE_DURATION);
    let owner = server.registration_owner(Default::default()).unwrap();
    let a = admission();
    let prepared = server
        .admit_registration(&owner, a.clone(), vec!["Widget".into()], "prepared-package")
        .unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let request = type_request(&peer, "Widget");
    let mut frame = peer.frame(9, "RegisterServiceTypeRequest", to_vec(&request).unwrap());
    *frame
        .headers
        .iter_mut()
        .find(|h| h.id == fube_transport::message::TIMEOUT)
        .unwrap() = Header::timeout(1).unwrap();
    assert!(peer.raw(frame).await.is_err());
    disconnected(&server, &prepared.lease).await;
    assert!(!owner
        .take_outputs()
        .unwrap()
        .events
        .iter()
        .any(|e| matches!(e, ClosedRegisteredEvent::Registered { .. })));
    assert!(owner.health_sources().unwrap()[0]
        .last_disabled_report
        .is_none());
    server.stop().unwrap();
}

#[tokio::test]
async fn stale_factory_session_cannot_register_into_same_identity_successor() {
    let mut server = server(LOCAL_LEASE_DURATION);
    let owner = server.registration_owner(Default::default()).unwrap();
    let a = admission();
    let old = server
        .admit_registration(&owner, a.clone(), vec!["Widget".into()], "old-component")
        .unwrap();
    let mut stale = Peer::connect(&server, a.clone()).await;
    stale.bootstrap().await;
    let request = type_request(&stale, "Widget");
    old.lease.revoke();
    disconnected(&server, &old.lease).await;
    owner.take_outputs().unwrap();
    server.release(old.lease).unwrap();
    let successor = server
        .admit_registration(&owner, a.clone(), vec!["Widget".into()], "new-component")
        .unwrap();
    assert!(successor.failure.is_none());
    let mut current = Peer::connect(&server, a).await;
    current.bootstrap().await;
    let stale_frame = stale.frame(9, "RegisterServiceTypeRequest", to_vec(&request).unwrap());
    assert!(stale.raw(stale_frame).await.is_err());
    assert!(!owner
        .take_outputs()
        .unwrap()
        .events
        .iter()
        .any(|e| matches!(e, ClosedRegisteredEvent::Registered { .. })));
    let request = type_request(&current, "Widget");
    let reply = current
        .call(9, "RegisterServiceTypeRequest", &request)
        .await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&reply.body).unwrap().error,
        0
    );
    assert!(matches!(owner.take_outputs().unwrap().events.as_slice(),
        [ClosedRegisteredEvent::Registered { registration, .. }] if registration.runtime_id == current.runtime));
    drop(current);
    disconnected(&server, &successor.lease).await;
    server.stop().unwrap();
}

async fn disconnected(server: &Server, lease: &HostLease) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !server.status(lease).unwrap().connected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn finite_frames_and_registration_outcomes() {
    let mut server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    assert_eq!(peer.finish().await, 0); // Native public/state split, not a fresh grant.
    assert_eq!(peer.register().await, 0x80071d2fu32 as i32 as i64);
    for _ in 0..2 {
        assert!((1..=300_000_000).contains(&peer.poll().await.unwrap()));
    }
    let status = server.status(&lease).unwrap();
    assert!(status.registered_host);
    assert_eq!(status.runtimes.len(), 1);
    assert_eq!(status.runtimes[0].package_instance_sequence, 2);
    assert_eq!(status.runtimes[0].version.instance, 3);
    assert_eq!(status.timers, 0);
    assert_eq!(status.outstanding_operations, 0);
    let response = peer
        .call(
            9,
            "UnregisterFabricRuntimeRequest",
            &wire::IdRequest {
                id: peer.runtime.clone(),
            },
        )
        .await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error,
        0
    );
    let response = peer
        .call(
            7,
            "UnregisterApplicationHostRequest",
            &wire::UnregisterHost {
                id: peer.admission.host.id.clone(),
                activity: wire::Activity {
                    id: wire::ActivityId {
                        guid: Guid([1; 16]),
                        index: 9,
                    },
                    kind: 0,
                },
            },
        )
        .await;
    assert_eq!(
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error,
        0
    );
    assert!(peer.poll().await.is_err());
    disconnected(&server, &lease).await;
    let outputs = server.take_outputs().unwrap();
    assert_eq!(outputs.events.len(), 2);
    assert!(
        matches!(&outputs.events[0], ClosedRegisteredEvent::RuntimeClosed { affected, .. } if affected.is_empty())
    );
    assert!(
        matches!(&outputs.events[1], ClosedRegisteredEvent::HostClosed { activity, affected, .. } if activity.ends_with(":9:0") && affected.is_empty())
    );
    assert!(outputs.operations.iter().any(|result| result.state_method
        == Some(Err(Error::InvalidState))
        && result.primary.is_ok()));
    server.stop().unwrap();
}

#[tokio::test]
async fn stop_renewal_expires_and_old_session_never_revives() {
    let server = server(Duration::from_millis(150));
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    assert!(peer.poll().await.unwrap() <= 1_500_000);
    lease.stop_renewal();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let ttl = peer.poll().await.unwrap();
    assert!(ttl < 1_200_000);
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert!(peer.poll().await.is_err());
    disconnected(&server, &lease).await;
    assert!(server.status(&lease).unwrap().loss.is_some());
    let mut old = Peer::connect(&server, peer.admission.clone()).await;
    assert!(old.poll().await.is_err());
}

#[tokio::test]
async fn registration_deadline_disposes_timer_before_replacement() {
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a.clone()).await;
    assert_eq!(peer.start(200_000).await.error, 0);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(peer
        .raw(
            peer.frame(
                7,
                "FinishRegisterApplicationHostRequest",
                to_vec(&wire::IdRequest {
                    id: a.host.id.clone()
                })
                .unwrap()
            )
        )
        .await
        .is_err());
    disconnected(&server, &lease).await;
    let status = server.status(&lease).unwrap();
    assert_eq!(status.loss, Some(Loss::RegistrationTimeout));
    assert_eq!(status.timers, 0);
    assert_eq!(status.outstanding_operations, 0);
    assert!(server.take_outputs().unwrap().events.is_empty()); // Not HostClosed.
    server.release(lease).unwrap();
    let replacement = server.admit(a.clone()).unwrap();
    let mut fresh = Peer::connect(&server, a).await;
    fresh.bootstrap().await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(fresh.poll().await.unwrap() > 0);
    assert!(server.status(&replacement).unwrap().registered_host);
}

#[tokio::test]
async fn revoked_session_cannot_mutate_same_identity_successor() {
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut old = Peer::connect(&server, a.clone()).await;
    old.bootstrap().await;
    let started = Instant::now();
    lease.revoke();
    assert!(old.poll().await.is_err());
    disconnected(&server, &lease).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(server.release(lease.clone()).is_err()); // Results must transfer first.
    assert!(!server.take_outputs().unwrap().events.is_empty());
    server.release(lease).unwrap();
    let replacement = server.admit(a.clone()).unwrap();
    let mut fresh = Peer::connect(&server, a).await;
    fresh.runtime = old.runtime.clone();
    fresh.bootstrap().await;
    assert!(old
        .raw(
            old.frame(
                9,
                "UnregisterFabricRuntimeRequest",
                to_vec(&wire::IdRequest {
                    id: old.runtime.clone()
                })
                .unwrap()
            )
        )
        .await
        .is_err());
    assert!(fresh.poll().await.unwrap() > 0);
    assert_eq!(server.status(&replacement).unwrap().runtimes.len(), 1);
}

#[tokio::test]
async fn foreign_id_only_mutation_and_duplicate_cannot_steal_runtime() {
    for action in [
        "UnregisterFabricRuntimeRequest",
        "UnregisterApplicationHostRequest",
        "FinishRegisterApplicationHostRequest",
    ] {
        let server = server(LOCAL_LEASE_DURATION);
        let a = admission();
        let b = admission();
        let lease_a = server.admit(a.clone()).unwrap();
        let lease_b = server.admit(b.clone()).unwrap();
        let mut pa = Peer::connect(&server, a).await;
        pa.bootstrap().await;
        let mut pb = Peer::connect(&server, b).await;
        pb.bootstrap().await;
        let body = if action == "UnregisterApplicationHostRequest" {
            to_vec(&wire::UnregisterHost {
                id: pb.admission.host.id.clone(),
                activity: wire::Activity {
                    id: wire::ActivityId {
                        guid: Guid([0; 16]),
                        index: 0,
                    },
                    kind: 0,
                },
            })
            .unwrap()
        } else {
            to_vec(&wire::IdRequest {
                id: if action == "UnregisterFabricRuntimeRequest" {
                    pb.runtime.clone()
                } else {
                    pb.admission.host.id.clone()
                },
            })
            .unwrap()
        };
        let actor = if action == "UnregisterFabricRuntimeRequest" {
            9
        } else {
            7
        };
        assert!(pa.raw(pa.frame(actor, action, body)).await.is_err());
        disconnected(&server, &lease_a).await;
        assert!(pb.poll().await.unwrap() > 0);
        assert_eq!(server.status(&lease_b).unwrap().runtimes.len(), 1);
    }
}

#[tokio::test]
async fn malformed_actor15_and_unsupported_profiles_fail_closed() {
    for (actor, action, body) in [
        (15, "", vec![1]),
        (15, "bogus", vec![]),
        (9, "RegisterServiceTypeRequest", vec![]),
        (7, "ReportHealth", vec![]),
        (12, "RAPReportLoad", vec![]),
    ] {
        let server = server(LOCAL_LEASE_DURATION);
        let a = admission();
        let lease = server.admit(a.clone()).unwrap();
        let mut peer = Peer::connect(&server, a).await;
        peer.bootstrap().await;
        assert!(peer.raw(peer.frame(actor, action, body)).await.is_err());
        disconnected(&server, &lease).await;
        assert!(!server.status(&lease).unwrap().registered_host);
        assert!(server.status(&lease).unwrap().runtimes.is_empty());
    }
}

#[tokio::test]
async fn truncated_start_requests_never_register() {
    let a = admission();
    let body = to_vec(&wire::StartRequest {
        id: a.host.id.clone(),
        kind: 1,
        pid: a.host.pid,
        timeout: 300_000_000,
        container: false,
        activator: false,
    })
    .unwrap();
    for end in 0..body.len() {
        let server = server(LOCAL_LEASE_DURATION);
        let lease = server.admit(a.clone()).unwrap();
        let mut peer = Peer::connect(&server, a.clone()).await;
        assert!(
            peer.raw(peer.frame(
                7,
                "StartRegisterApplicationHostRequest",
                body[..end].to_vec()
            ))
            .await
            .is_err(),
            "prefix {end}"
        );
        disconnected(&server, &lease).await;
        let status = server.status(&lease).unwrap();
        assert!(!status.registered_host);
        assert!(status.runtimes.is_empty());
        assert_eq!(status.timers, 0);
        assert!(server.take_outputs().unwrap().operations.is_empty());
    }
}

#[tokio::test]
async fn malformed_headers_pid_and_frame_allocation_limit() {
    for variant in 0..5 {
        let server = server(LOCAL_LEASE_DURATION);
        let a = admission();
        let lease = server.admit(a.clone()).unwrap();
        let mut peer = Peer::connect(&server, a).await;
        let mut request = peer.frame(15, "", vec![]);
        match variant {
            0 => request.headers.push(Header::actor(15).unwrap()),
            1 => peer.admission.host.pid += 1,
            2 => request.headers.retain(|h| h.id != IPC),
            3 => request.headers.push(Header::timeout(-1).unwrap()),
            _ => request
                .headers
                .retain(|h| h.id != fube_transport::message::TIMEOUT),
        }
        if variant == 1 {
            request = peer.frame(15, "", vec![]);
        }
        assert!(peer.raw(request).await.is_err());
        assert!(server.status(&lease).unwrap().runtimes.is_empty());
    }
    let server = server(LOCAL_LEASE_DURATION);
    let mut socket = TcpStream::connect(server.address()).await.unwrap();
    let mut fixed = [0u8; 12];
    fixed[..4].copy_from_slice(&(1024u32 * 1024).to_le_bytes());
    socket.write_all(&fixed).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), read_message(&mut socket))
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn closure_pressure_revokes_before_cleanup_and_retains_accounting() {
    let mut config = Config {
        node: node(),
        core_limits: Default::default(),
        max_admissions: 2,
        scratch_bytes: 16 * 1024 * 1024,
        decode_bytes: 256 * 1024,
        projection_bytes: 256 * 1024,
        reply_bytes: 64 * 1024,
        lease_duration: LOCAL_LEASE_DURATION,
    };
    config.core_limits.events_per_lane = 1;
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), config).unwrap();
    let a = admission();
    let b = admission();
    let lease_a = server.admit(a.clone()).unwrap();
    let lease_b = server.admit(b.clone()).unwrap();
    let mut pa = Peer::connect(&server, a).await;
    pa.bootstrap().await;
    let mut pb = Peer::connect(&server, b).await;
    pb.bootstrap().await;
    lease_a.revoke();
    assert!(pa.poll().await.is_err());
    disconnected(&server, &lease_a).await;
    lease_b.revoke();
    assert!(pb.poll().await.is_err());
    disconnected(&server, &lease_b).await;
    assert_eq!(
        server.status(&lease_b).unwrap().cleanup,
        Some(Err(Error::Capacity))
    );
    assert!(server.status(&lease_b).unwrap().registered_host); // Atomic H1 veto preserved.
    assert!(server.admit(admission()).is_err()); // No unbounded tombstones.
    let outputs = server.take_outputs().unwrap();
    assert_eq!(outputs.events.len(), 1);
    assert!(server.release(lease_b).is_err()); // No silent cleanup recovery/admission.
}

#[tokio::test]
async fn retained_results_and_abandoned_request_observers_are_bounded() {
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    for _ in 3..16 {
        assert_eq!(peer.finish().await, 0);
    }
    assert!(peer.poll().await.unwrap() > 0); // Output pressure does not block leases.
    let request = peer.frame(
        7,
        "FinishRegisterApplicationHostRequest",
        to_vec(&wire::IdRequest {
            id: peer.admission.host.id.clone(),
        })
        .unwrap(),
    );
    // Abandon reply observation after full local send; accepted outcomes are not
    // undone or charged only until observation.
    write_message(&mut peer.socket, &request).await.unwrap();
    disconnected(&server, &lease).await;
    let outputs = server.take_outputs().unwrap();
    assert_eq!(outputs.operations.len(), 16);
    assert!(outputs.operations.iter().all(|value| value.primary.is_ok()));
    assert_eq!(outputs.events.len(), 1);
    assert!(server.take_outputs().unwrap().operations.is_empty());
}

#[tokio::test]
async fn fields_dropped_by_h1_are_checked_before_wire_registration() {
    for variant in 0..4 {
        let server = server(LOCAL_LEASE_DURATION);
        let a = admission();
        let lease = server.admit(a.clone()).unwrap();
        let mut peer = Peer::connect(&server, a).await;
        assert_eq!(peer.start(300_000_000).await.error, 0);
        assert_eq!(peer.finish().await, 0);
        let mut context = wire::RuntimeContext {
            id: peer.runtime.clone(),
            host: peer.admission.host.clone(),
            code: peer.admission.code.clone(),
        };
        match variant {
            0 => context.code.code_sequence += 1,
            1 => context.code.application_name = "fabric:/Wrong".into(),
            2 => context.host.activator = true,
            _ => context.code.id.package.activation.mode = 1,
        }
        let request = peer.frame(
            9,
            "RegisterFabricRuntimeRequest",
            to_vec(&wire::RuntimeRequest { context }).unwrap(),
        );
        assert!(peer.raw(request).await.is_err());
        disconnected(&server, &lease).await;
        assert!(server.status(&lease).unwrap().runtimes.is_empty());
        assert!(!server
            .take_outputs()
            .unwrap()
            .operations
            .iter()
            .any(|value| value.action == "RegisterFabricRuntimeRequest"));
    }
}

#[tokio::test]
async fn replay_and_queued_stale_correlations_cannot_extend_authority() {
    let server = server(Duration::from_millis(200));
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let request = peer.frame(15, "", vec![]);
    assert!(peer.raw(request.clone()).await.is_ok());
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(peer.raw(request).await.is_err());
    disconnected(&server, &lease).await;
    assert_eq!(server.status(&lease).unwrap().loss, Some(Loss::Protocol));
}

#[tokio::test]
async fn malformed_handshake_and_uncorrelated_duplicates_never_succeed() {
    for variant in 0..3 {
        let server = server(LOCAL_LEASE_DURATION);
        let mut socket = TcpStream::connect(server.address()).await.unwrap();
        let mut control = Message::listen_instance(new_id(), &ListenInstance::default()).unwrap();
        match variant {
            0 => control.headers.insert(0, Header::actor(7).unwrap()),
            1 => control.headers.push(
                Header::object(fube_transport::message::FAULT, |e| {
                    e.i64(-1);
                    e.boolean(false);
                    Ok(())
                })
                .unwrap(),
            ),
            _ => control
                .headers
                .push(Header::boolean(EXPECTS_REPLY, true).unwrap()),
        }
        write_message(&mut socket, &control).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), read_message(&mut socket))
                .await
                .unwrap()
                .is_err()
        );
    }
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let mut request = peer.frame(15, "", vec![]);
    for value in [true, false] {
        request
            .headers
            .push(Header::boolean(fube_transport::message::UNCORRELATED_REPLY, value).unwrap());
    }
    assert!(peer.raw(request).await.is_err());
    disconnected(&server, &lease).await;
    assert_eq!(server.status(&lease).unwrap().loss, Some(Loss::Protocol));
}

#[tokio::test]
async fn decode_projection_and_reply_budgets_reject_before_allocation() {
    for stage in 0..3 {
        let mut config = config(LOCAL_LEASE_DURATION);
        match stage {
            0 => config.decode_bytes = 100,
            1 => config.projection_bytes = 1,
            _ => config.reply_bytes = 1,
        }
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), config).unwrap();
        let a = admission();
        let lease = server.admit(a.clone()).unwrap();
        let mut peer = Peer::connect(&server, a).await;
        if stage < 2 {
            assert_eq!(peer.start(300_000_000).await.error, 0);
            assert_eq!(peer.finish().await, 0);
            let request = peer.frame(
                9,
                "RegisterFabricRuntimeRequest",
                to_vec(&wire::RuntimeRequest {
                    context: wire::RuntimeContext {
                        id: peer.runtime.clone(),
                        host: peer.admission.host.clone(),
                        code: peer.admission.code.clone(),
                    },
                })
                .unwrap(),
            );
            assert!(peer.raw(request).await.is_err());
        } else {
            let h = peer.admission.host.clone();
            let request = peer.frame(
                7,
                "StartRegisterApplicationHostRequest",
                to_vec(&wire::StartRequest {
                    id: h.id,
                    kind: 1,
                    pid: h.pid,
                    timeout: 300_000_000,
                    container: false,
                    activator: false,
                })
                .unwrap(),
            );
            assert!(peer.raw(request).await.is_err());
        }
        disconnected(&server, &lease).await;
        let status = server.status(&lease).unwrap();
        assert!(status.runtimes.is_empty());
        assert!(status.reserved_bytes <= 512 * 1024);
        let outputs = server.take_outputs().unwrap();
        assert_eq!(outputs.operations.len(), if stage == 2 { 1 } else { 2 });
        assert!(outputs
            .operations
            .iter()
            .all(|result| result.primary.is_ok()));
        assert_eq!(outputs.events.len(), 1); // Accepted state survives reply failure.
    }
}

#[tokio::test]
async fn finish_deadline_race_has_one_owned_result_and_no_stale_timer() {
    for _ in 0..8 {
        let server = server(LOCAL_LEASE_DURATION);
        let a = admission();
        let lease = server.admit(a.clone()).unwrap();
        let mut peer = Peer::connect(&server, a).await;
        assert_eq!(peer.start(200_000).await.error, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let request = peer.frame(
            7,
            "FinishRegisterApplicationHostRequest",
            to_vec(&wire::IdRequest {
                id: peer.admission.host.id.clone(),
            })
            .unwrap(),
        );
        let result = peer.raw(request).await;
        if let Ok(message) = result {
            assert_eq!(
                from_slice::<wire::ErrorReply>(&message.body).unwrap().error,
                0
            );
            assert!(server.status(&lease).unwrap().registered_host);
        } else {
            disconnected(&server, &lease).await;
            assert_eq!(
                server.status(&lease).unwrap().loss,
                Some(Loss::RegistrationTimeout)
            );
        }
        let status = server.status(&lease).unwrap();
        assert_eq!(status.timers, 0);
        assert_eq!(status.outstanding_operations, 0);
    }
}

#[tokio::test]
async fn blocked_writer_does_not_hold_registration_deadline() {
    let mut config = config(LOCAL_LEASE_DURATION);
    config.node.application_directories.0[0].value = "x".repeat(15_000);
    config.node.application_directories.0.push(wire::Pair {
        key: "log".into(),
        value: "y".repeat(15_000),
    });
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), config).unwrap();
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    socket.set_tcp_mss(88).unwrap();
    socket.connect(&server.address().into()).unwrap();
    socket.set_nonblocking(true).unwrap();
    let mut stream = TcpStream::from_std(socket.into()).unwrap();
    write_message(
        &mut stream,
        &Message::listen_instance(new_id(), &ListenInstance::default()).unwrap(),
    )
    .await
    .unwrap();
    read_message(&mut stream).await.unwrap();
    let mut peer = Peer {
        socket: stream,
        admission: a.clone(),
        runtime: uuid::Uuid::new_v4().to_string(),
        guid: uuid::Uuid::new_v4().to_bytes_le(),
        sequence: std::cell::Cell::new(0),
    };
    for _ in 0..16 {
        let request = peer.frame(
            7,
            "StartRegisterApplicationHostRequest",
            to_vec(&wire::StartRequest {
                id: a.host.id.clone(),
                kind: 1,
                pid: a.host.pid,
                timeout: 1_000_000,
                container: false,
                activator: false,
            })
            .unwrap(),
        );
        write_message(&mut peer.socket, &request).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let status = server.status(&lease).unwrap();
    assert!(!status.connected);
    assert_eq!(status.loss, Some(Loss::RegistrationTimeout));
    assert_eq!(status.timers, 0);
    assert_eq!(status.outstanding_operations, 0);
    assert!(!status.registered_host);
    assert!(server.take_outputs().unwrap().events.is_empty());
}

#[tokio::test]
async fn eof_and_shutdown_retain_bounded_failure_and_output_ownership() {
    let mut config = config(LOCAL_LEASE_DURATION);
    config.core_limits.events_per_lane = 1;
    let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), config).unwrap();
    let a = admission();
    let b = admission();
    let la = server.admit(a.clone()).unwrap();
    let lb = server.admit(b.clone()).unwrap();
    let mut pa = Peer::connect(&server, a).await;
    pa.bootstrap().await;
    let mut pb = Peer::connect(&server, b.clone()).await;
    pb.bootstrap().await;
    drop(pa);
    disconnected(&server, &la).await;
    drop(pb);
    disconnected(&server, &lb).await;
    assert_eq!(
        server.status(&la).unwrap().loss,
        Some(Loss::ConnectionClosed)
    );
    assert_eq!(
        server.status(&lb).unwrap().cleanup,
        Some(Err(Error::Capacity))
    );
    for _ in 0..8 {
        assert!(server.admit(b.clone()).is_err());
    }
    let start = Instant::now();
    server.stop().unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(
        server.status(&lb).unwrap().cleanup,
        Some(Err(Error::Capacity))
    );
    assert!(server.status(&lb).unwrap().reserved_bytes <= 2 * 512 * 1024);
    let outputs = server.take_outputs().unwrap();
    assert_eq!(outputs.events.len(), 1);
    assert_eq!(outputs.operations.len(), 6);
}

#[tokio::test]
async fn cross_host_duplicate_does_not_acquire_id_only_cleanup_ownership() {
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let b = admission();
    let la = server.admit(a.clone()).unwrap();
    let lb = server.admit(b.clone()).unwrap();
    let mut pa = Peer::connect(&server, a).await;
    pa.bootstrap().await;
    let mut pb = Peer::connect(&server, b).await;
    assert_eq!(pb.start(300_000_000).await.error, 0);
    assert_eq!(pb.finish().await, 0);
    pb.runtime = pa.runtime.clone();
    assert_eq!(pb.register().await, 0x80071d2fu32 as i32 as i64);
    let request = pb.frame(
        9,
        "UnregisterFabricRuntimeRequest",
        to_vec(&wire::IdRequest {
            id: pa.runtime.clone(),
        })
        .unwrap(),
    );
    assert!(pb.raw(request).await.is_err());
    disconnected(&server, &lb).await;
    assert!(pa.poll().await.unwrap() > 0);
    assert_eq!(server.status(&la).unwrap().runtimes.len(), 1);
}

#[tokio::test]
async fn queued_replayed_poll_is_not_a_new_correlated_grant() {
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let first = peer.frame(15, "", vec![]);
    let later = peer.frame(15, "", vec![]);
    for message in [&first, &first, &later] {
        write_message(&mut peer.socket, message).await.unwrap();
    }
    let response = read_message(&mut peer.socket).await.unwrap();
    assert_eq!(
        response.properties().unwrap().relates_to,
        first.properties().unwrap().id
    );
    assert!(from_slice::<wire::LeaseReply>(&response.body).unwrap().ttl > 0);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_message(&mut peer.socket))
            .await
            .unwrap()
            .is_err()
    );
    disconnected(&server, &lease).await;
    assert_eq!(server.status(&lease).unwrap().loss, Some(Loss::Protocol));
}

#[tokio::test]
async fn queued_changed_producer_guid_never_gets_a_correlated_grant() {
    let server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let first = peer.frame(15, "", vec![]);
    peer.guid = uuid::Uuid::new_v4().to_bytes_le();
    let changed = peer.frame(15, "", vec![]);
    for value in [&first, &changed] {
        write_message(&mut peer.socket, value).await.unwrap();
    }
    let response = read_message(&mut peer.socket).await.unwrap();
    assert_eq!(
        response.properties().unwrap().relates_to,
        first.properties().unwrap().id
    );
    assert!(from_slice::<wire::LeaseReply>(&response.body).unwrap().ttl > 0);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_message(&mut peer.socket))
            .await
            .unwrap()
            .is_err()
    );
    disconnected(&server, &lease).await;
    assert_eq!(server.status(&lease).unwrap().loss, Some(Loss::Protocol));
}

#[tokio::test]
async fn active_shutdown_closes_frames_and_retains_exact_owned_outputs() {
    let mut server = server(LOCAL_LEASE_DURATION);
    let a = admission();
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    assert!(peer.poll().await.unwrap() > 0);
    let start = Instant::now();
    server.stop().unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(peer.poll().await.is_err());
    let status = server.status(&lease).unwrap();
    assert!(!status.connected);
    assert_eq!(status.loss, Some(Loss::Shutdown));
    assert!(status.reserved_bytes <= 512 * 1024);
    assert_eq!(status.timers, 0);
    assert_eq!(status.outstanding_operations, 0);
    let outputs = server.take_outputs().unwrap();
    assert_eq!(outputs.operations.len(), 3);
    assert_eq!(outputs.events.len(), 1);
    assert!(
        matches!(&outputs.events[0], ClosedRegisteredEvent::HostClosed { affected, .. } if affected.is_empty())
    );
    assert!(matches!(
        server.admit(admission()),
        Err(Error::ObjectClosed)
    ));
}

#[tokio::test]
async fn blocked_reply_writer_and_queued_polls_do_not_prevent_withdrawal() {
    for revoke in [true, false] {
        let server = server(Duration::from_secs(1));
        let a = admission();
        let lease = server.admit(a.clone()).unwrap();
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        socket.set_recv_buffer_size(1024).unwrap();
        socket.set_send_buffer_size(1024).unwrap();
        socket.set_tcp_mss(88).unwrap();
        socket.connect(&server.address().into()).unwrap();
        socket.set_nonblocking(true).unwrap();
        let mut stream = TcpStream::from_std(socket.into()).unwrap();
        write_message(
            &mut stream,
            &Message::listen_instance(new_id(), &ListenInstance::default()).unwrap(),
        )
        .await
        .unwrap();
        read_message(&mut stream).await.unwrap();
        let mut peer = Peer {
            socket: stream,
            admission: a,
            runtime: uuid::Uuid::new_v4().to_string(),
            guid: uuid::Uuid::new_v4().to_bytes_le(),
            sequence: std::cell::Cell::new(0),
        };
        peer.bootstrap().await;
        assert!(peer.poll().await.unwrap() > 0);
        // No reply reader: bounded client send/receive buffers cause actual TCP
        // backpressure. The server's only suspension on this path is its writer.
        let pressure = tokio::time::timeout(Duration::from_millis(150), async {
            for _ in 0..30_000 {
                let request = peer.frame(15, "", vec![]);
                write_message(&mut peer.socket, &request).await.unwrap();
            }
        })
        .await;
        assert!(pressure.is_err(), "expected a blocked actual frame writer");
        let start = Instant::now();
        if revoke {
            lease.revoke();
        } else {
            lease.stop_renewal();
        }
        disconnected(&server, &lease).await;
        assert!(start.elapsed() < Duration::from_millis(1500));
        assert_eq!(server.status(&lease).unwrap().loss, Some(Loss::Authority));
        // The cancelled partial client write is never reused. Observe buffered
        // complete replies until terminal EOF/truncation; no unbounded drain.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if read_message(&mut peer.socket).await.is_err() {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(server.status(&lease).unwrap().reserved_bytes, 512 * 1024);
    }
}

#[tokio::test]
async fn native_local_deadlines_are_explicit_and_do_not_accept_expired_headers() {
    let server = server(LOCAL_LEASE_DURATION);
    let mut a = admission();
    a.deadlines = RequestDeadlines::NativeClientLocal;
    let lease = server.admit(a.clone()).unwrap();
    let mut peer = Peer::connect(&server, a).await;
    peer.bootstrap().await;
    let mut request = peer.frame(15, "", vec![]);
    request
        .headers
        .retain(|h| h.id != fube_transport::message::TIMEOUT);
    let reply = peer.raw(request).await.unwrap();
    assert!(from_slice::<wire::LeaseReply>(&reply.body).unwrap().ttl > 0);
    let mut expired = peer.frame(15, "", vec![]);
    expired
        .headers
        .retain(|h| h.id != fube_transport::message::TIMEOUT);
    expired.headers.push(Header::timeout(-1).unwrap());
    assert!(peer.raw(expired).await.is_err());
    disconnected(&server, &lease).await;
    assert_eq!(server.status(&lease).unwrap().loss, Some(Loss::Protocol));
}
