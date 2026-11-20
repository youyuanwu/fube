//! Runtime IPC policy over furpc's typed client and canonical ingress services.
//!
//! The session owns one Channel and captures its original Peer. Retirement
//! releases that ownership; it is not a physical-driver join or a first-write
//! authority fence. Runtime authority remains checked at domain boundaries.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::Duration,
};

use furpc::{
    header, ActorId, Body, BoxError, Channel, Connected, Endpoint, FubstCodec, HandlerLimits,
    HeaderId, HeaderRecord, Headers, IngressLimits, IngressResponse, Message, MessageId,
    OneWayRouteOptions, Peer, Request, RequestPolicy, RequestRouteOptions, Response, Router,
    RpcClient, Source, TransportIo,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{oneshot, watch, Semaphore},
    time::{timeout_at, Instant},
};
use tower_service::Service;

use crate::{Error, ErrorKind, Result};

#[cfg(test)]
#[path = "ipc_client_tests.rs"]
mod tests;

pub(crate) type Check = Arc<dyn Fn() -> bool + Send + Sync>;
type Handoff = Arc<dyn Fn(IpcMessage) -> Result<()> + Send + Sync>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
const OPERATIONS: usize = 32;
const IPC: u16 = 0x800b;
const TIMEOUT: u16 = 0x8023;

pub(crate) fn deadline_after(timeout: Duration) -> Result<Instant> {
    let ticks = timeout.as_nanos() / 100;
    if ticks == 0 || ticks > i64::MAX as u128 {
        return Err(Error::InvalidInput("invalid runtime IPC timeout"));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidInput("runtime IPC deadline overflow"))
}

pub(crate) struct IpcMessage {
    pub actor: i64,
    pub action: String,
    pub id: MessageId,
    pub body: Vec<u8>,
    pub received_at: std::time::Instant,
    pub deadline: Instant,
    pub reply: Option<IpcResponse>,
}

/// Domain reply data for the waiting ingress handler, never native reply authority.
pub(crate) struct IpcResponse {
    actor: i64,
    response: oneshot::Sender<Message>,
}

impl IpcResponse {
    pub(crate) async fn reply(
        self,
        actor: i64,
        action: Option<&str>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<()> {
        self.reply_checked(actor, action, body, deadline, Arc::new(|| true))
            .await
    }

    pub(crate) async fn reply_checked(
        self,
        actor: i64,
        action: Option<&str>,
        body: Vec<u8>,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        if self.actor != actor {
            return Err(Error::InvalidInput("foreign IPC reply actor"));
        }
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        if !valid() {
            return Err(Error::Closed);
        }
        self.response
            .send(envelope(actor, action, body)?)
            .map_err(|_| Error::Closed)
    }
}

struct Connection {
    channel: Option<Channel>,
    error: Option<Error>,
}

struct Session {
    connection: Mutex<Connection>,
    stopped: watch::Sender<Option<Error>>,
    ordinary: Semaphore,
    replies: Semaphore,
}

impl Session {
    fn check(&self) -> Result<()> {
        match &self.connection.lock().unwrap().error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn retire(&self, error: Error) {
        let channel = {
            let mut connection = self.connection.lock().unwrap();
            if connection.error.is_some() {
                return;
            }
            connection.error = Some(error.clone());
            connection.channel.take()
        };
        self.ordinary.close();
        self.replies.close();
        self.stopped.send_replace(Some(error));
        drop(channel);
    }
}

#[derive(Clone)]
pub(crate) struct IpcClient {
    session: Arc<Session>,
    peer: Peer,
    identity: HeaderRecord,
    serving: Option<Check>,
}

impl IpcClient {
    pub(crate) async fn connect(
        address: SocketAddr,
        deadline: Instant,
        host_id: &str,
        handoff: Handoff,
        serving: Option<Check>,
    ) -> Result<Self> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(Error::InvalidInput("runtime IPC requires numeric loopback"));
        }
        if host_id.is_empty() || host_id.contains('\0') {
            return Err(Error::InvalidInput("runtime IPC requires a host identity"));
        }
        #[derive(Serialize)]
        struct Identity<'a> {
            host: &'a str,
            pid: u32,
        }
        let identity = HeaderRecord::native(
            HeaderId::new(IPC).map_err(native_error)?,
            &Identity {
                host: host_id,
                pid: std::process::id(),
            },
        )
        .map_err(native_error)?;
        let session = Arc::new(Session {
            connection: Mutex::new(Connection {
                channel: None,
                error: None,
            }),
            stopped: watch::channel(None).0,
            ordinary: Semaphore::new(OPERATIONS),
            replies: Semaphore::new(OPERATIONS),
        });
        let service = Ingress {
            session: Arc::downgrade(&session),
            handoff,
        };
        let policy = RequestRouteOptions::new(
            RequestPolicy::new(incoming, response),
            IngressLimits::default(),
        );
        let mut router = Router::new();
        for action in ["InvalidateLeaseRequest", "UpdateCodePackageContextRequest"] {
            router = router
                .request(actor(8)?, action, policy.clone(), service.clone())
                .map_err(native_error)?;
        }
        router = router
            .request(actor(12)?, "RAPQuery", policy, service.clone())
            .map_err(native_error)?;
        for action in [
            "ReplicaOpen",
            "ReplicaClose",
            "ProxyUpdateServiceDescription",
            "UpdateConfiguration",
            "ReplicatorBuildIdleReplica",
            "ReplicatorRemoveIdleReplica",
            "ReplicatorGetStatus",
            "ReplicatorUpdateEpochAndGetStatus",
            "CancelCatchupReplicaSet",
            "ProxyReplicaEndpointUpdatedReply",
        ] {
            router = router
                .one_way(
                    actor(12)?,
                    action,
                    OneWayRouteOptions::new(incoming, IngressLimits::default()),
                    service.clone(),
                )
                .map_err(native_error)?;
        }
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::Timeout)?;
        let endpoint = Endpoint::try_from(address)
            .map_err(native_error)?
            .connect_timeout(timeout)
            .handler_limits(HandlerLimits {
                max_in_flight: OPERATIONS,
                max_readiness_waiters: OPERATIONS,
                ..HandlerLimits::default()
            })
            .service(router.into_service().map_err(native_error)?)
            .map_err(native_error)?;
        let channel = timeout_at(
            deadline,
            endpoint.connect_with_connector(Connector {
                address,
                session: Arc::downgrade(&session),
            }),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(native_error)?;
        let peer = channel.peer(deadline).await.map_err(native_error)?;
        {
            let mut connection = session.connection.lock().unwrap();
            if let Some(error) = &connection.error {
                return Err(error.clone());
            }
            connection.channel = Some(channel);
        }
        Ok(Self {
            session,
            peer,
            identity,
            serving,
        })
    }

    pub(crate) fn abort(&self) {
        self.session.retire(Error::Closed);
    }

    pub(crate) fn abort_with_error(&self, error: Error) {
        self.session.retire(error);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.session.check().is_err()
    }

    pub(crate) async fn closed(&self) -> Error {
        let mut stopped = self.session.stopped.subscribe();
        loop {
            if let Some(error) = stopped.borrow_and_update().clone() {
                return error;
            }
            if stopped.changed().await.is_err() {
                return Error::Closed;
            }
        }
    }

    /// Release domain ownership, not an awaitable furpc driver teardown barrier.
    pub(crate) async fn close(&self) -> Result<()> {
        self.abort();
        Ok(())
    }

    pub(crate) async fn request<Q, R>(
        &self,
        action: &str,
        actor: i64,
        mut request: Request<Q>,
        deadline: Instant,
        valid: Option<Check>,
    ) -> Result<Response<R>>
    where
        Q: Serialize + Send + 'static,
        R: DeserializeOwned + Send + 'static,
    {
        let deadline = request
            .deadline()
            .map_or(deadline, |local| local.min(deadline));
        let _admission = timeout_at(deadline, self.session.ordinary.acquire())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)?;
        self.session.check()?;
        let cleanup = matches!(
            action,
            "UnregisterFabricRuntimeRequest" | "UnregisterApplicationHostRequest"
        );
        let serving = (!cleanup).then(|| self.serving.clone()).flatten();
        let check = Arc::new(move || {
            serving.as_ref().is_none_or(|check| check())
                && valid.as_ref().is_none_or(|check| check())
        });
        request
            .headers_mut()
            .append(header::actor(self::actor(actor)?).map_err(native_error)?)
            .map_err(native_error)?;
        if !action.is_empty() {
            request
                .headers_mut()
                .append(header::action(action).map_err(native_error)?)
                .map_err(native_error)?;
        }
        let mut rpc = RpcClient::new(IpcService {
            transport: PeerService(self.peer.clone()),
            identity: self.identity.clone(),
            health: action == "ReportHealth",
            valid: check.clone(),
        });
        let result = rpc
            .call(request, FubstCodec::<Q, R>::default(), deadline)
            .await;
        let encoding = matches!(
            &result,
            Err(furpc::Error::Codec {
                phase: "encode",
                ..
            })
        );
        let result = result.map_err(native_error);
        if let Err(error) = &result {
            if !encoding
                && (matches!(error, Error::Protocol(_))
                    || (matches!(error.kind(), ErrorKind::ObjectClosed | ErrorKind::Transport)
                        && check()))
            {
                self.session.retire(error.clone());
            }
        }
        let response = result?;
        self.session.check()?;
        if !check() {
            return Err(Error::Closed);
        }
        Ok(response)
    }

    pub(crate) async fn send_one_way(
        &self,
        actor: i64,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        if !matches!(
            action,
            "ReplicaOpenReply" | "ReplicaCloseReply" | "ProxyUpdateServiceDescriptionReply"
        ) {
            return Err(Error::InvalidInput("unauthorized runtime semantic reply"));
        }
        let _slot = self
            .session
            .replies
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("runtime semantic reply admission"))?;
        self.send(actor, action, body, deadline, valid).await
    }

    pub(crate) async fn send_one_way_checked(
        &self,
        actor: i64,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        let _slot = self
            .session
            .ordinary
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("runtime report output admission"))?;
        self.send(actor, action, body, deadline, valid).await
    }

    async fn send(
        &self,
        actor: i64,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.session.check()?;
        if !valid() {
            return Err(Error::Closed);
        }
        let mut message = envelope(actor, Some(action), body)?;
        outbound(message.headers(), false)?;
        let mut builder = Message::builder();
        for header in message.headers().records() {
            builder = builder.append_header(header.to_owned());
        }
        message = builder
            .append_header(self.identity.clone())
            .body(message.into_body())
            .build()
            .map_err(native_error)?;
        let result = self
            .peer
            .send(message, deadline)
            .await
            .map_err(native_error);
        if let Err(error) = &result {
            if matches!(error.kind(), ErrorKind::ObjectClosed | ErrorKind::Transport) {
                self.session.retire(error.clone());
            }
        }
        result?;
        self.session.check()?;
        if !valid() {
            return Err(Error::Closed);
        }
        Ok(())
    }
}

fn actor(value: i64) -> Result<ActorId> {
    ActorId::new(value).map_err(native_error)
}

fn envelope(actor: i64, action: Option<&str>, body: Vec<u8>) -> Result<Message> {
    let mut builder =
        Message::builder().append_header(header::actor(self::actor(actor)?).map_err(native_error)?);
    if let Some(action) = action.filter(|action| !action.is_empty()) {
        builder = builder.append_header(header::action(action).map_err(native_error)?);
    }
    builder
        .body(Body::from_bytes(body).map_err(native_error)?)
        .build()
        .map_err(native_error)
}

fn action(headers: &Headers) -> Result<String> {
    String::from_utf16(headers.effective().action())
        .map_err(|_| Error::Protocol("invalid runtime IPC action"))
}

fn outbound(headers: &Headers, request: bool) -> Result<()> {
    let actor = headers.effective().actor.get();
    let action = action(headers)?;
    let allowed = if request {
        match actor {
            7 => matches!(
                action.as_str(),
                "StartRegisterApplicationHostRequest"
                    | "FinishRegisterApplicationHostRequest"
                    | "UnregisterApplicationHostRequest"
                    | "ReportHealth"
            ),
            9 => matches!(
                action.as_str(),
                "RegisterFabricRuntimeRequest"
                    | "UnregisterFabricRuntimeRequest"
                    | "RegisterServiceTypeRequest"
            ),
            15 => action.is_empty(),
            _ => false,
        }
    } else {
        actor == 12
            && matches!(
                action.as_str(),
                "RAPReportLoad"
                    | "RAReportFault"
                    | "ProxyReplicaEndpointUpdated"
                    | "ReplicaOpenReply"
                    | "ReplicaCloseReply"
                    | "ProxyUpdateServiceDescriptionReply"
                    | "UpdateConfigurationReply"
                    | "ReplicatorBuildIdleReplicaReply"
                    | "ReplicatorRemoveIdleReplicaReply"
                    | "ReplicatorGetStatusReply"
                    | "ReplicatorUpdateEpochAndGetStatusReply"
                    | "CancelCatchupReplicaSetReply"
            )
    };
    if allowed {
        Ok(())
    } else {
        Err(Error::InvalidInput("unauthorized runtime IPC submission"))
    }
}

fn incoming(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if common.message_id == MessageId::default()
        || common.uncorrelated_reply
        || headers.first_raw(header::FAULT).is_some()
    {
        return Err(furpc::Error::Protocol("invalid runtime IPC ingress"));
    }
    Ok(())
}

fn response(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if (common.actor.get() == 8 && common.action().is_empty())
        || (common.actor.get() == 12
            && common
                .action()
                .iter()
                .copied()
                .eq("RAPQueryReply".encode_utf16()))
    {
        Ok(())
    } else {
        Err(furpc::Error::InvalidInput(
            "unauthorized runtime IPC response",
        ))
    }
}

#[derive(Clone)]
struct IpcService<T> {
    transport: T,
    identity: HeaderRecord,
    health: bool,
    valid: Check,
}

impl<T> Service<Request<Body>> for IpcService<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send + 'static,
    T::Error: Into<BoxError>,
{
    type Response = Response<Body>;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.transport
            .poll_ready(cx)
            .map_err(|error| service_error(error.into()))
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let prepare = (|| {
            if !(self.valid)() {
                return Err(Error::Closed);
            }
            outbound(request.headers(), true)?;
            let ticks = request
                .deadline()
                .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                .ok_or(Error::Timeout)?
                .as_nanos()
                / 100;
            if ticks == 0 {
                return Err(Error::Timeout);
            }
            #[derive(Serialize)]
            struct Timeout {
                ticks: i64,
            }
            request
                .headers_mut()
                .append(self.identity.clone())
                .map_err(native_error)?;
            request
                .headers_mut()
                .append(
                    HeaderRecord::native(
                        HeaderId::new(TIMEOUT).map_err(native_error)?,
                        &Timeout {
                            ticks: i64::try_from(ticks).map_err(|_| {
                                Error::InvalidInput("runtime timeout exceeds native TimeSpan")
                            })?,
                        },
                    )
                    .map_err(native_error)?,
                )
                .map_err(native_error)?;
            Ok(())
        })();
        if let Err(error) = prepare {
            return Box::pin(async move { Err(error) });
        }
        let future = self.transport.call(request);
        let health = self.health;
        Box::pin(async move {
            let response = future.await.map_err(|error| service_error(error.into()))?;
            let properties = response.headers().effective();
            if matches!(properties.actor.get(), 25 | 27)
                || (health
                    && (properties.actor.get() != 16
                        || !properties
                            .action()
                            .iter()
                            .copied()
                            .eq("OperationSuccessAction".encode_utf16())))
            {
                return Err(Error::Protocol("unexpected runtime IPC reply envelope"));
            }
            Ok(response)
        })
    }
}

#[derive(Clone)]
struct PeerService(Peer);

impl Service<Request<Body>> for PeerService {
    type Response = Response<Body>;
    type Error = furpc::Error;
    type Future = furpc::Operation<Self::Response>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let deadline = request
            .deadline()
            .expect("RpcClient sets the request deadline");
        self.0.request(request, deadline)
    }
}

#[derive(Clone)]
struct Ingress {
    session: Weak<Session>,
    handoff: Handoff,
}

impl Service<Request<Body>> for Ingress {
    type Response = IngressResponse;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let session = self.session.clone();
        let handoff = self.handoff.clone();
        Box::pin(async move {
            let session = session.upgrade().ok_or(Error::Closed)?;
            let mut guard = IngressCall {
                session: session.clone(),
                armed: true,
            };
            let result: Result<IngressResponse> = async {
                session.check()?;
                let context = request
                    .context()
                    .ok_or(Error::Protocol("runtime ingress requires furpc context"))?;
                let deadline = context.deadline();
                let properties = request.headers().effective();
                let (send, receive) = oneshot::channel();
                let expects_reply = properties.expects_reply;
                let mut body = Vec::new();
                body.try_reserve_exact(request.body().len())
                    .map_err(|_| Error::ResourceLimit("runtime ingress body"))?;
                for segment in request.body().segments() {
                    body.extend_from_slice(segment);
                }
                handoff(IpcMessage {
                    actor: properties.actor.get(),
                    action: action(request.headers())?,
                    id: properties.message_id,
                    body,
                    received_at: std::time::Instant::now(),
                    deadline,
                    reply: expects_reply.then_some(IpcResponse {
                        actor: properties.actor.get(),
                        response: send,
                    }),
                })?;
                if expects_reply {
                    let message = timeout_at(deadline, receive)
                        .await
                        .map_err(|_| Error::Timeout)?
                        .map_err(|_| Error::Closed)?;
                    Ok(IngressResponse::Reply(message))
                } else {
                    Ok(IngressResponse::NoReply)
                }
            }
            .await;
            guard.armed = false;
            if let Err(error) = &result {
                session.retire(error.clone());
            }
            result
        })
    }
}

struct IngressCall {
    session: Arc<Session>,
    armed: bool,
}

impl Drop for IngressCall {
    fn drop(&mut self) {
        if self.armed {
            self.session.retire(Error::Closed);
        }
    }
}

struct Connector {
    address: SocketAddr,
    session: Weak<Session>,
}

impl Service<http::Uri> for Connector {
    type Response = Connected<ObservedIo<tokio::net::TcpStream>>;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let address = self.address;
        let session = self.session.clone();
        Box::pin(async move {
            if uri
                != format!("tcp://{address}")
                    .parse::<http::Uri>()
                    .map_err(|_| Error::InvalidInput("invalid runtime IPC URI"))?
            {
                return Err(Error::InvalidInput("runtime IPC destination changed"));
            }
            let io = tokio::net::TcpStream::connect(address).await?;
            io.set_nodelay(true)?;
            let local = io.local_addr()?;
            let peer = io.peer_addr()?;
            if !local.ip().is_loopback() || peer != address {
                return Err(Error::Protocol("runtime IPC requires genuine loopback I/O"));
            }
            Ok(Connected::new(
                ObservedIo { io, session },
                furpc::IoInfo {
                    local_addr: Some(local),
                    peer_addr: Some(peer),
                },
            ))
        })
    }
}

struct ObservedIo<I: TransportIo> {
    io: I,
    session: Weak<Session>,
}

impl<I: TransportIo> Drop for ObservedIo<I> {
    fn drop(&mut self) {
        if let Some(session) = self.session.upgrade() {
            session.retire(Error::Closed);
        }
    }
}

impl<I: TransportIo> AsyncRead for ObservedIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}

impl<I: TransportIo> AsyncWrite for ObservedIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, buffers)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

fn service_error(error: BoxError) -> Error {
    match error.downcast::<Error>() {
        Ok(error) => *error,
        Err(error) => match error.downcast::<furpc::Error>() {
            Ok(error) => native_error(*error),
            Err(error) => Error::Classified {
                kind: ErrorKind::Transport,
                message: "runtime IPC service".into(),
                cause: Some(Arc::from(error)),
            },
        },
    }
}

pub(crate) fn native_error(error: furpc::Error) -> Error {
    match error {
        furpc::Error::Protocol(reason) => Error::Protocol(reason),
        furpc::Error::InvalidInput(reason) => Error::InvalidInput(reason),
        furpc::Error::ResourceLimit(resource) => Error::ResourceLimit(resource),
        furpc::Error::Closed | furpc::Error::Cancelled => Error::Closed,
        furpc::Error::Timeout(_) => Error::Timeout,
        furpc::Error::RemoteFault(fault) => Error::Remote {
            code: fault.code(),
            message: "runtime IPC transport fault".into(),
        },
        furpc::Error::Codec { phase, source } => {
            if let Some(error) = std::error::Error::source(&source)
                .and_then(|source| source.downcast_ref::<fubst::Error>())
            {
                return Error::from(*error);
            }
            Error::with_cause(ErrorKind::Transport, format!("runtime IPC {phase}"), source)
        }
        error @ furpc::Error::Transport { .. } => {
            if let Some(original) =
                std::error::Error::source(&error).and_then(|source| source.downcast_ref::<Error>())
            {
                return original.clone();
            }
            Error::with_cause(ErrorKind::Transport, "runtime IPC", error)
        }
        error => Error::with_cause(ErrorKind::Transport, "runtime IPC", error),
    }
}
