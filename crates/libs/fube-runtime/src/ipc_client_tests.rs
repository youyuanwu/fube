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
            health,
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

#[tokio::test]
async fn typed_rpc_projects_identity_and_remaining_deadline_without_legacy_messages() {
    let response =
        Response::new(Body::from_bytes(to_vec(&ErrorReply { error: 0 }).unwrap()).unwrap());
    let (service, received) = service(response, false, Arc::new(|| true));
    let deadline = deadline_after(Duration::from_secs(2)).unwrap();
    let reply = RpcClient::new(service)
        .call(
            request(9, "RegisterFabricRuntimeRequest"),
            FubstCodec::<IdRequest, ErrorReply>::default(),
            deadline,
        )
        .await
        .unwrap();
    assert_eq!(reply.into_body().error, 0);
    let sent = received.lock().unwrap().take().unwrap();
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
    let (service, _) = service(response, true, Arc::new(|| true));
    let error = RpcClient::new(service)
        .call(
            request(7, "ReportHealth"),
            FubstCodec::<IdRequest, ErrorReply>::default(),
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        native_error(error),
        Error::Protocol("unexpected runtime IPC reply envelope")
    ));
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
