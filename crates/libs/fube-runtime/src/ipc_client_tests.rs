use super::*;
use crate::wire::{ErrorReply, IdRequest};
use fubst::serde::{from_slice, to_vec};
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

fn service(response: Response<Body>, health: bool, valid: Check) -> (IpcService<Probe>, Captured) {
    let received = Arc::new(Mutex::new(None));
    (
        IpcService {
            transport: Probe {
                received: received.clone(),
                response: Some(response),
            },
            identity: HeaderRecord::native(
                HeaderId::new(IPC).unwrap(),
                &Identity {
                    host: "owned-host".into(),
                    pid: 17,
                },
            )
            .unwrap(),
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
    let session = Arc::new(Session {
        state: Mutex::new(SessionState {
            channel: None,
            error: None,
        }),
        stopped: watch::channel(None).0,
        ordinary: Semaphore::new(OPERATIONS),
        replies: Semaphore::new(OPERATIONS),
    });
    (
        HostingClient::new(service.transport, service.identity, session, serving),
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
    for (actor, valid) in [(9, false), (5, true)] {
        let response =
            Response::new(Body::from_bytes(to_vec(&ErrorReply { error: 0 }).unwrap()).unwrap());
        let (service, received) = service(response, false, Arc::new(move || valid));
        let error = RpcClient::new(service)
            .call(
                request(actor, "RegisterFabricRuntimeRequest"),
                FubstCodec::<IdRequest, ErrorReply>::default(),
                deadline_after(Duration::from_secs(2)).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            native_error(error),
            Error::Closed | Error::InvalidInput(_)
        ));
        assert!(received.lock().unwrap().is_none());
    }
}
