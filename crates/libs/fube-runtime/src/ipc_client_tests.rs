use super::*;
use crate::wire::{ErrorReply, IdRequest};
use fube_core::protocol::NativeActivityType;
use fube_core::protocol::NativeApiName;
use fube_core::protocol::NativeInterfaceName;
use fube_core::protocol::NativeProxyFlags;
use fube_core::protocol::{NativeReplicaRole, NativeReplicaState};
use fubst::serde::{from_slice, to_vec, ByteArray, Guid, ObjectArray};
use std::future::{ready, Ready};

type Captured = Arc<Mutex<Option<Request<Body>>>>;

struct Probe {
    received: Captured,
    response: Option<Response<Body>>,
}

impl Service<Request<Body>> for Probe {
    type Response = Response<Body>;
    type Error = Error;
    type Future = Ready<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        *self.received.lock().unwrap() = Some(request);
        ready(Ok(self.response.take().unwrap()))
    }
}

#[derive(Serialize, serde::Deserialize)]
struct Identity {
    host: String,
    pid: u32,
}

fn identity() -> HeaderRecord {
    HeaderRecord::native(
        HeaderId::new(IPC).unwrap(),
        &Identity {
            host: "owned-host".into(),
            pid: 17,
        },
    )
    .unwrap()
}

fn session() -> Arc<Session> {
    Session::new()
}

fn service(response: Response<Body>, health: bool, valid: Check) -> (IpcService<Probe>, Captured) {
    let received = Arc::new(Mutex::new(None));
    (
        IpcService {
            transport: Probe {
                received: received.clone(),
                response: Some(response),
            },
            identity: identity(),
            validate_reply: if health {
                check_health_reply
            } else {
                check_hosting_reply
            },
            valid,
        },
        received,
    )
}

fn request(actor: i64, action: &str) -> Request<IdRequest> {
    Request::new(IdRequest {
        id: "runtime".into(),
    })
    .append_header(header::actor(self::actor(actor).unwrap()).unwrap())
    .unwrap()
    .append_header(header::action(action).unwrap())
    .unwrap()
}

fn client(response: Response<Body>, serving: Option<Check>) -> (HostingClient<Probe>, Captured) {
    let (service, captured) = service(response, false, Arc::new(|| true));
    (
        HostingClient::new(service.transport, service.identity, session(), serving),
        captured,
    )
}

fn health_request() -> Request<wire::HealthBody> {
    Request::new(wire::HealthBody {
        reports: fubst::serde::ObjectArray(vec![]),
        streams: fubst::serde::EmptyObjectArray,
    })
}

#[tokio::test]
async fn typed_rpc_projects_identity_and_remaining_deadline_without_legacy_messages() {
    let response =
        Response::new(Body::from_bytes(to_vec(&ErrorReply { error: 0 }).unwrap()).unwrap());
    let (mut client, received) = client(response, None);
    let deadline = deadline_after(Duration::from_secs(2)).unwrap();
    let reply = client
        .finish_register_application_host(
            Request::new(IdRequest {
                id: "runtime".into(),
            }),
            deadline,
        )
        .await
        .unwrap();
    assert_eq!(reply.into_body().error, 0);
    let sent = received.lock().unwrap().take().unwrap();
    assert_eq!(sent.headers().effective().actor.get(), 7);
    assert_eq!(
        action(sent.headers()).unwrap(),
        "FinishRegisterApplicationHostRequest"
    );
    let identity: Identity = from_slice(
        sent.headers()
            .first_raw(HeaderId::new(IPC).unwrap())
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!((identity.host.as_str(), identity.pid), ("owned-host", 17));
    assert_eq!(sent.deadline(), Some(deadline));
    let bytes: Vec<_> = sent.body().segments().flatten().copied().collect();
    assert_eq!(from_slice::<IdRequest>(&bytes).unwrap().id, "runtime");
    #[derive(serde::Deserialize)]
    struct Timeout {
        ticks: i64,
    }
    let timeout: Timeout = from_slice(
        sent.headers()
            .first_raw(HeaderId::new(TIMEOUT).unwrap())
            .unwrap()
            .value(),
    )
    .unwrap();
    assert!((1..=20_000_000).contains(&timeout.ticks));
}

#[tokio::test]
async fn health_envelope_is_rejected_before_success_body_decoding() {
    let mut response = Response::new(Body::from_bytes(vec![0xff]).unwrap());
    response
        .headers_mut()
        .append(header::actor(actor(8).unwrap()).unwrap())
        .unwrap();
    let (mut client, _) = client(response, None);
    let error = client
        .report_health(
            health_request(),
            deadline_after(Duration::from_secs(2)).unwrap(),
            Arc::new(|| true),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol("unexpected runtime IPC reply envelope")
    ));
    assert!(client.session.check().is_err());
}

#[tokio::test]
async fn typed_lease_poll_uses_empty_native_body_and_decodes_ttl() {
    let response =
        Response::new(Body::from_bytes(to_vec(&wire::LeaseReply { ttl: 123 }).unwrap()).unwrap());
    let (mut client, received) = client(response, None);
    let reply = client
        .poll_lease(
            Request::new(()),
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.into_body().ttl, 123);
    let sent = received.lock().unwrap().take().unwrap();
    assert_eq!(sent.headers().effective().actor.get(), 15);
    assert!(sent.headers().first_raw(header::ACTION).is_none());
    assert!(sent.body().is_empty());
}

#[tokio::test]
async fn unregister_preserves_cleanup_exemption_after_serving_authority_retires() {
    let response =
        Response::new(Body::from_bytes(to_vec(&ErrorReply { error: 0 }).unwrap()).unwrap());
    let (mut client, received) = client(response, Some(Arc::new(|| false)));
    let error = client
        .finish_register_application_host(
            Request::new(IdRequest { id: "host".into() }),
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Closed));
    assert!(received.lock().unwrap().is_none());
    assert!(client.session.check().is_ok());

    let reply = client
        .unregister_fabric_runtime(
            Request::new(IdRequest {
                id: "runtime".into(),
            }),
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.into_body().error, 0);
    let sent = received.lock().unwrap().take().unwrap();
    assert_eq!(sent.headers().effective().actor.get(), 9);
    assert_eq!(
        action(sent.headers()).unwrap(),
        "UnregisterFabricRuntimeRequest"
    );
}

#[tokio::test]
async fn typed_health_method_retains_its_per_report_admission_fence() {
    let response = Response::new(Body::empty());
    let (mut client, received) = client(response, None);
    let error = client
        .report_health(
            health_request(),
            deadline_after(Duration::from_secs(2)).unwrap(),
            Arc::new(|| false),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Closed));
    assert!(received.lock().unwrap().is_none());
    assert!(client.session.check().is_ok());
}

#[tokio::test]
async fn retired_or_foreign_requests_are_rejected_before_byte_submission() {
    for (actor, action, valid) in [
        (9, "RegisterFabricRuntimeRequest", false),
        (5, "RegisterFabricRuntimeRequest", true),
        (7, "RegisterFabricRuntimeRequest", true),
        (12, "RAPReportLoad", true),
        (15, "RAPReportLoad", true),
    ] {
        let response =
            Response::new(Body::from_bytes(to_vec(&ErrorReply { error: 0 }).unwrap()).unwrap());
        let (service, received) = service(response, false, Arc::new(move || valid));
        let error = RpcClient::new(service)
            .call(
                request(actor, action),
                FubstEncoder::<IdRequest>::default(),
                FubstDecoder::<ErrorReply>::default(),
                deadline_after(Duration::from_secs(2)).unwrap(),
            )
            .await
            .unwrap_err();
        let error = native_error(error);
        if valid {
            assert!(matches!(
                error,
                Error::InvalidInput("unauthorized runtime IPC submission")
            ));
        } else {
            assert!(matches!(error, Error::Closed));
        }
        assert!(received.lock().unwrap().is_none());
    }
}

#[test]
fn reply_codec_resource_failure_retains_resource_classification_and_source() {
    let error = native_error(furpc::Error::Codec {
        phase: "decode",
        source: furpc::CodecError::resource_limit(std::io::Error::other("allocation failure")),
    });
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
    assert!(std::error::Error::source(&error).is_some());
}

struct SendProbe {
    received: Captured,
    ready: bool,
}

impl Service<Request<Body>> for SendProbe {
    type Response = WriteReceipt;
    type Error = Error;
    type Future = Ready<Result<WriteReceipt>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        assert!(std::mem::replace(&mut self.ready, false));
        *self.received.lock().unwrap() = Some(request);
        ready(Err(Error::InvalidInput("send probe")))
    }
}

fn rap_client() -> (RapClient<SendProbe>, Captured) {
    let received = Arc::new(Mutex::new(None));
    (
        RapClient::new(
            SendProbe {
                received: received.clone(),
                ready: false,
            },
            identity(),
            session(),
        ),
        received,
    )
}

#[tokio::test]
async fn rap_guard_rejects_foreign_profiles_without_submitting_or_retiring() {
    for (actor, action) in [
        (actors::FABRIC_RUNTIME_MANAGER, "RAPReportLoad"),
        (actors::RA, "RegisterFabricRuntimeRequest"),
        (actors::RA, "ReplicaOpen"),
        (actors::RA, ""),
    ] {
        let (mut client, received) = rap_client();
        let service = client.inner.get_mut();
        let mut request = Request::new(Body::empty())
            .append_header(header::actor(self::actor(actor).unwrap()).unwrap())
            .unwrap()
            .append_header(header::action(action).unwrap())
            .unwrap();
        request.set_deadline(deadline_after(Duration::from_secs(2)).unwrap());
        std::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let error = service.call(request).await.unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidInput("unauthorized runtime IPC submission")
        ));
        assert!(received.lock().unwrap().is_none());
        service.session.check().unwrap();
    }
}

fn proxy_reply() -> wire::ProxyReply {
    let id = wire::PartitionId { id: Guid([1; 16]) };
    let rollout = wire::Rollout { major: 1, minor: 0 };
    wire::ProxyReply {
        unit: wire::FailoverUnit {
            id: id.clone(),
            consistency: wire::ConsistencyUnit {
                id,
                low: 0,
                high: 0,
                name: String::new(),
                kind: 1,
            },
            current: wire::Epoch {
                configuration: 1,
                data_loss: 0,
            },
            previous: wire::Epoch {
                configuration: 0,
                data_loss: 0,
            },
            target: 1,
            minimum: 1,
        },
        local: wire::Replica {
            node: wire::Node {
                id: ByteArray(vec![1; 16]),
                instance: 1,
            },
            id: 1,
            instance: 1,
            current_role: NativeReplicaRole::Unknown,
            previous_role: NativeReplicaRole::Unknown,
            up: true,
            last_lsn: -1,
            first_lsn: -1,
            state: NativeReplicaState::Ready,
            version: wire::VersionInstance {
                version: wire::PackageVersion {
                    application: wire::ApplicationVersion {
                        value: rollout.clone(),
                    },
                    value: rollout,
                },
                instance: 1,
            },
            service_endpoint: String::new(),
            replication_endpoint: String::new(),
        },
        remote: ObjectArray(vec![]),
        error: wire::ProxyError {
            error: 0,
            message: String::new(),
            api: wire::ApiName {
                api: NativeApiName::Invalid,
                interface: NativeInterfaceName::Invalid,
                metadata: String::new(),
            },
        },
        flags: NativeProxyFlags::NONE,
    }
}

fn fault_report() -> FaultReport {
    let reply = proxy_reply();
    FaultReport::prepare(wire::FaultBody {
        unit: reply.unit,
        local: reply.local,
        fault: 1,
        activity: wire::Activity {
            id: wire::ActivityId {
                guid: Guid([7; 16]),
                index: 17,
            },
            kind: NativeActivityType::ServiceReportFaultEvent,
        },
    })
    .unwrap()
}

#[tokio::test]
async fn typed_rap_replies_preserve_distinct_lanes_and_ipc_metadata_for_the_same_action() {
    let (mut client, received) = rap_client();
    let session = client.inner.get_mut().session.clone();
    let _ordinary = session
        .ordinary
        .clone()
        .try_acquire_many_owned(OPERATIONS as u32)
        .unwrap();
    let deadline = deadline_after(Duration::from_secs(2)).unwrap();
    let error = client
        .send_replica_reply(
            ProxyReplyKind::ReplicaOpen,
            proxy_reply(),
            deadline,
            Arc::new(|| true),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput("send probe")));
    let request = received.lock().unwrap().take().unwrap();
    assert_eq!(request.deadline(), Some(deadline));
    assert_eq!(request.headers().effective().actor.get(), actors::RA);
    assert_eq!(action(request.headers()).unwrap(), "ReplicaOpenReply");
    let identity: Identity = from_slice(
        request
            .headers()
            .first_raw(HeaderId::new(IPC).unwrap())
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!((identity.host.as_str(), identity.pid), ("owned-host", 17));
    assert!(request
        .headers()
        .first_raw(HeaderId::new(TIMEOUT).unwrap())
        .is_none());
    let bytes = request
        .body()
        .segments()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(from_slice::<wire::ProxyReply>(&bytes).unwrap().local.id, 1);
    assert_eq!(session.replies.available_permits(), OPERATIONS);
    let error = client
        .send_stateful_reply(
            ProxyReplyKind::ReplicaOpen,
            proxy_reply(),
            deadline,
            Arc::new(|| true),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::ResourceLimit("runtime report output admission")
    ));
    assert!(received.lock().unwrap().is_none());
    session.check().unwrap();
}

#[tokio::test]
async fn rap_local_encoding_and_authority_failures_do_not_submit_or_retire_the_session() {
    let (mut client, received) = rap_client();
    client.inner = client.inner.max_encoded_body_bytes(0).unwrap();
    let error = client
        .report_load(
            wire::LoadBody {
                legacy: fubst::serde::EmptyObjectArray,
                reports: ObjectArray(vec![]),
                time: 123,
            },
            deadline_after(Duration::from_secs(2)).unwrap(),
            Arc::new(|| true),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ResourceLimit(_)));
    assert!(received.lock().unwrap().is_none());
    assert_eq!(
        client.inner.get_mut().session.ordinary.available_permits(),
        OPERATIONS
    );
    client.inner.get_mut().session.check().unwrap();

    let (mut client, received) = rap_client();
    let error = client
        .report_fault(
            fault_report(),
            deadline_after(Duration::from_secs(2)).unwrap(),
            Arc::new(|| false),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Closed));
    assert!(received.lock().unwrap().is_none());
    client.inner.get_mut().session.check().unwrap();
}

#[tokio::test]
async fn prepared_rap_payload_retries_share_original_bytes_without_native_reserialization() {
    let (mut client, received) = rap_client();
    let report = fault_report();
    let pointer = report.0.segments().next().unwrap().as_ptr();
    for report in [report.clone(), report] {
        let error = client
            .report_fault(
                report,
                deadline_after(Duration::from_secs(2)).unwrap(),
                Arc::new(|| true),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput("send probe")));
        let request = received.lock().unwrap().take().unwrap();
        assert_eq!(request.body().segments().next().unwrap().as_ptr(), pointer);
        let bytes = request
            .body()
            .segments()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let body = from_slice::<wire::FaultBody>(&bytes).unwrap();
        assert_eq!(body.fault, 1);
        assert_eq!(body.activity.id.index, 17);
    }
}

#[tokio::test]
async fn runtime_connector_never_dials_a_replacement_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = session();
    let mut connector = Connector::new(address, Arc::downgrade(&session));
    let uri = format!("tcp://{address}").parse::<http::Uri>().unwrap();
    let connected = connector.call(uri.clone()).await.unwrap();
    let (_accepted, _) = listener.accept().await.unwrap();
    assert_eq!(connected.info().peer_addr, Some(address));
    assert!(connected.info().local_addr.unwrap().ip().is_loopback());

    let error = connector.call(uri).await.err().unwrap();
    assert!(matches!(error, Error::Closed));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    session.check().unwrap();

    drop(connected);
    assert!(matches!(session.closed().await, Error::Closed));
}

#[tokio::test]
async fn retired_runtime_session_prevents_dialing_and_preserves_its_cause() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = session();
    session.retire(Error::Protocol("registration retired"));
    let mut connector = Connector::new(address, Arc::downgrade(&session));
    let error = connector
        .call(format!("tcp://{address}").parse().unwrap())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::Protocol("registration retired")));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    assert!(matches!(
        session.closed().await,
        Error::Protocol("registration retired")
    ));
}
