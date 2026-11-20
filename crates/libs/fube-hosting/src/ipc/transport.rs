//! Byte-facing furpc ingress; Hosting alone owns admission and mutation policy.
use super::{IO_DEADLINE, MAX_INPUT};
use furpc::{
    header, Accept, Accepted, ActorId, Body, Error, HandlerLimits, Headers, IngressLimits,
    IngressOptions, IngressResponse, IngressService, IoInfo, Message, Peer, Request, RequestPolicy,
    Result, SendKind, SendPolicy, Server, ServiceBinding, Source, TransportLimits,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, watch},
};
use tower_service::Service;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
const HANDLERS: usize = 4;

pub(super) struct IpcMessage {
    pub headers: Headers,
    pub body: Vec<u8>,
}

pub(super) struct Incoming {
    pub message: IpcMessage,
    pub received_at: Instant,
    pub deadline: tokio::time::Instant,
    pub response: oneshot::Sender<IngressResponse>,
}

pub(super) struct Connection {
    pub serving: BoxFuture<Result<()>>,
    pub incoming: mpsc::Receiver<Incoming>,
    pub ready: oneshot::Receiver<Peer>,
    pub failure: watch::Receiver<Option<Error>>,
}

pub(super) fn message(actor: i64, action: &str, body: Vec<u8>) -> Result<Message> {
    let mut builder = Message::builder().append_header(header::actor(ActorId::new(actor)?)?);
    if !action.is_empty() {
        builder = builder.append_header(header::action(action)?);
    }
    builder.body(Body::from_bytes(body)?).build()
}

pub(super) fn action(headers: &Headers) -> Result<String> {
    String::from_utf16(headers.effective().action())
        .map_err(|_| Error::Protocol("invalid Hosting action"))
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    fubst::serde::to_vec(value).map_err(|source| Error::Codec {
        phase: "encode",
        source: source.into(),
    })
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    let mut left = limit;
    fubst::serde::from_slice_with_budget(bytes, &mut |capacity| {
        left = left
            .checked_sub(capacity)
            .ok_or(fubst::Error::ResourceLimit("Hosting decode budget"))?;
        Ok(())
    })
    .map_err(|source| Error::Codec {
        phase: "decode",
        source: source.into(),
    })
}

fn allow(_: &Headers, _: &Source) -> Result<()> {
    Ok(())
}

fn response(headers: &Headers, _: &Source) -> Result<()> {
    let common = headers.effective();
    if common.actor.get() != 0
        || !common.action().is_empty()
        || common.expects_reply
        || headers.first_raw(header::FAULT).is_some()
    {
        return Err(Error::Protocol("invalid Hosting response"));
    }
    Ok(())
}

fn outbound(kind: SendKind, headers: &Headers, _: &Source) -> Result<()> {
    let common = headers.effective();
    if kind != SendKind::OneWay
        || common.actor.get() != 12
        || !matches!(
            action(headers)?.as_str(),
            "ReplicaOpen"
                | "ReplicaClose"
                | "ProxyUpdateServiceDescription"
                | "ReplicatorBuildIdleReplica"
                | "ReplicatorRemoveIdleReplica"
                | "UpdateConfiguration"
                | "ReplicatorUpdateEpochAndGetStatus"
                | "ProxyReplicaEndpointUpdatedReply"
        )
        || common.expects_reply
        || common.relates_to != furpc::MessageId::default()
    {
        return Err(Error::InvalidInput("invalid Hosting lifecycle send"));
    }
    Ok(())
}

pub(super) fn accepted(socket: TcpStream) -> Result<Connection> {
    let (send, incoming) = mpsc::channel(HANDLERS);
    let (failure, observed) = watch::channel(None);
    let ingress = IngressService::new(
        Ingress { send, failure },
        [7, 9, 12, 15]
            .into_iter()
            .map(|actor| {
                let limits = IngressLimits {
                    max_body_bytes: MAX_INPUT,
                };
                ServiceBinding::actor(
                    ActorId::new(actor)?,
                    if actor == 12 {
                        IngressOptions::one_way(allow, limits)
                    } else {
                        IngressOptions::requests(RequestPolicy::new(allow, response), limits)
                    },
                )
            })
            .collect::<Result<_>>()?,
    )?;
    let (ready, wait) = oneshot::channel();
    let server = Server::builder()
        .limits(TransportLimits {
            max_receive_frame_bytes: MAX_INPUT,
            max_send_frame_bytes: Some(MAX_INPUT),
            send_queue_bytes: 8 * MAX_INPUT,
            max_pending_sends: 8,
            max_pending_requests: 1,
            max_connections: 1,
            max_handshakes: 1,
            frame_timeout: IO_DEADLINE,
            write_timeout: IO_DEADLINE,
            ..TransportLimits::default()
        })
        .handler_limits(HandlerLimits {
            max_in_flight: HANDLERS,
            max_readiness_waiters: HANDLERS,
            max_encoded_body_bytes_per_response: MAX_INPUT,
            timeout: Duration::from_secs(30),
        })
        .handshake_timeout(IO_DEADLINE)
        .peer_ready_timeout(IO_DEADLINE)
        .drain_timeout(IO_DEADLINE)
        .service(ingress)?
        .on_peer_ready(
            SendPolicy::new(outbound),
            Ready(Arc::new(Mutex::new(Some(ready)))),
        )?
        .build()?;
    Ok(Connection {
        serving: Box::pin(server.serve_with_shutdown(
            OneSocket(Some(socket)),
            furpc::ConnectionOptions::managed(),
            std::future::pending(),
        )),
        incoming,
        ready: wait,
        failure: observed,
    })
}

struct OneSocket(Option<TcpStream>);
impl Accept for OneSocket {
    type Io = TcpStream;
    type Error = std::io::Error;
    async fn accept(&mut self) -> std::io::Result<Option<Accepted<TcpStream>>> {
        let Some(socket) = self.0.take() else {
            return Ok(None);
        };
        let local = socket.local_addr()?;
        let peer = socket.peer_addr()?;
        if !local.ip().is_loopback() || !peer.ip().is_loopback() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Hosting requires actual loopback endpoints",
            ));
        }
        socket.set_nodelay(true)?;
        Ok(Some(Accepted::new(
            socket,
            IoInfo {
                local_addr: Some(local),
                peer_addr: Some(peer),
            },
        )))
    }
}

#[derive(Clone)]
struct Ready(Arc<Mutex<Option<oneshot::Sender<Peer>>>>);
impl Service<Peer> for Ready {
    type Response = ();
    type Error = Error;
    type Future = std::future::Ready<Result<()>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, peer: Peer) -> Self::Future {
        std::future::ready(
            self.0
                .lock()
                .unwrap()
                .take()
                .ok_or(Error::Internal("Hosting accepted a second generation"))
                .and_then(|ready| ready.send(peer).map_err(|_| Error::Closed)),
        )
    }
}

#[derive(Clone)]
struct Ingress {
    send: mpsc::Sender<Incoming>,
    failure: watch::Sender<Option<Error>>,
}
impl Service<Request<Body>> for Ingress {
    type Response = IngressResponse;
    type Error = Error;
    type Future = BoxFuture<Result<IngressResponse>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let send = self.send.clone();
        let failure = self.failure.clone();
        Box::pin(async move {
            let mut guard = CallGuard {
                failure,
                completed: false,
            };
            let result = async {
                let deadline = request
                    .context()
                    .ok_or(Error::Internal("Hosting ingress context"))?
                    .deadline();
                let mut body = Vec::new();
                body.try_reserve_exact(request.body().len())
                    .map_err(|_| Error::ResourceLimit("Hosting ingress body"))?;
                for segment in request.body().segments() {
                    body.extend_from_slice(segment);
                }
                let (response, wait) = oneshot::channel();
                send.try_send(Incoming {
                    message: IpcMessage {
                        headers: request.headers().clone(),
                        body,
                    },
                    received_at: Instant::now(),
                    deadline,
                    response,
                })
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => {
                        Error::ResourceLimit("Hosting ingress handoff")
                    }
                    mpsc::error::TrySendError::Closed(_) => Error::Closed,
                })?;
                wait.await.map_err(|_| Error::Closed)
            }
            .await;
            if let Err(error) = &result {
                guard.failure.send_replace(Some(error.clone()));
            }
            guard.completed = true;
            result
        })
    }
}
struct CallGuard {
    failure: watch::Sender<Option<Error>>,
    completed: bool,
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.failure
                .send_replace(Some(Error::Timeout("Hosting ingress handoff")));
        }
    }
}
