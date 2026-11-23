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

use fube_core::protocol::actor as actors;
use furpc::{
    codec, header, ActorId, Body, BoxError, Channel, ClientCodecLimits, Codec, Connected,
    EncodeBuf, Encoder, Endpoint, FubstCodec, HeaderId, HeaderRecord, Headers, OneWayClient, Peer,
    PeerRequestService, PeerSendService, Request, Response, RpcClient, TransportIo, WriteReceipt,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{watch, Semaphore},
    time::{timeout_at, Instant},
};
use tower_service::Service;

use crate::{ipc_server::RuntimeServer, wire, Error, ErrorKind, Result};

#[cfg(test)]
#[path = "ipc_client_tests.rs"]
mod tests;

pub(crate) type Check = Arc<dyn Fn() -> bool + Send + Sync>;
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

struct SessionState {
    channel: Option<Channel>,
    error: Option<Error>,
}

pub(super) struct Session {
    state: Mutex<SessionState>,
    stopped: watch::Sender<Option<Error>>,
    ordinary: Arc<Semaphore>,
    replies: Arc<Semaphore>,
}

impl Session {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SessionState {
                channel: None,
                error: None,
            }),
            stopped: watch::channel(None).0,
            ordinary: Arc::new(Semaphore::new(OPERATIONS)),
            replies: Arc::new(Semaphore::new(OPERATIONS)),
        })
    }

    async fn connect(
        address: SocketAddr,
        deadline: Instant,
        server: RuntimeServer,
    ) -> Result<(Arc<Self>, Peer)> {
        let session = Self::new();
        let router = server.router(Arc::downgrade(&session))?;
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::Timeout)?;
        let endpoint = Endpoint::try_from(address)
            .map_err(native_error)?
            .connect_timeout(timeout)
            .service(router.into_service().map_err(native_error)?)
            .map_err(native_error)?;
        let channel = timeout_at(
            deadline,
            endpoint.connect_with_connector(Connector::new(address, Arc::downgrade(&session))),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(native_error)?;
        let peer = channel.peer(deadline).await.map_err(native_error)?;
        {
            let mut state = session.state.lock().unwrap();
            if let Some(error) = &state.error {
                return Err(error.clone());
            }
            state.channel = Some(channel);
        }
        Ok((session, peer))
    }

    pub(super) fn check(&self) -> Result<()> {
        match &self.state.lock().unwrap().error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(super) fn retire(&self, error: Error) {
        let channel = {
            let mut state = self.state.lock().unwrap();
            if state.error.is_some() {
                return;
            }
            state.error = Some(error.clone());
            state.channel.take()
        };
        self.ordinary.close();
        self.replies.close();
        self.stopped.send_replace(Some(error));
        drop(channel);
    }

    async fn closed(&self) -> Error {
        let mut stopped = self.stopped.subscribe();
        loop {
            if let Some(error) = stopped.borrow_and_update().clone() {
                return error;
            }
            if stopped.changed().await.is_err() {
                return Error::Closed;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct IpcClient {
    session: Arc<Session>,
    hosting: HostingClient<PeerRequestService>,
    rap: RapClient<PeerSendService>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProxyReplyKind {
    ReplicaOpen,
    ReplicaClose,
    UpdateConfiguration,
    BuildIdleReplica,
    RemoveIdleReplica,
    GetStatus,
    UpdateEpochAndGetStatus,
    CancelCatchupReplicaSet,
}

impl ProxyReplyKind {
    fn action(self) -> &'static str {
        match self {
            Self::ReplicaOpen => "ReplicaOpenReply",
            Self::ReplicaClose => "ReplicaCloseReply",
            Self::UpdateConfiguration => "UpdateConfigurationReply",
            Self::BuildIdleReplica => "ReplicatorBuildIdleReplicaReply",
            Self::RemoveIdleReplica => "ReplicatorRemoveIdleReplicaReply",
            Self::GetStatus => "ReplicatorGetStatusReply",
            Self::UpdateEpochAndGetStatus => "ReplicatorUpdateEpochAndGetStatusReply",
            Self::CancelCatchupReplicaSet => "CancelCatchupReplicaSetReply",
        }
    }
}

#[derive(Clone, Copy)]
enum RapMethod {
    ReplicaReply(ProxyReplyKind),
    ServiceUpdateReply,
    Load,
    Fault,
    EndpointUpdated,
}

impl RapMethod {
    const ALL: [Self; 12] = [
        Self::ReplicaReply(ProxyReplyKind::ReplicaOpen),
        Self::ReplicaReply(ProxyReplyKind::ReplicaClose),
        Self::ReplicaReply(ProxyReplyKind::UpdateConfiguration),
        Self::ReplicaReply(ProxyReplyKind::BuildIdleReplica),
        Self::ReplicaReply(ProxyReplyKind::RemoveIdleReplica),
        Self::ReplicaReply(ProxyReplyKind::GetStatus),
        Self::ReplicaReply(ProxyReplyKind::UpdateEpochAndGetStatus),
        Self::ReplicaReply(ProxyReplyKind::CancelCatchupReplicaSet),
        Self::ServiceUpdateReply,
        Self::Load,
        Self::Fault,
        Self::EndpointUpdated,
    ];

    fn envelope(self) -> (i64, &'static str) {
        let action = match self {
            Self::ReplicaReply(kind) => kind.action(),
            Self::ServiceUpdateReply => "ProxyUpdateServiceDescriptionReply",
            Self::Load => "RAPReportLoad",
            Self::Fault => "RAReportFault",
            Self::EndpointUpdated => "ProxyReplicaEndpointUpdated",
        };
        (actors::RA, action)
    }
}

/// Encoded once so domain-owned fault retries preserve the original payload bytes.
#[derive(Clone)]
pub(crate) struct FaultReport(Body);

impl FaultReport {
    pub(crate) fn prepare(body: wire::FaultBody) -> Result<Self> {
        encode(body).map(Self)
    }
}

/// Encoded once so endpoint retries retain the original advertised description.
#[derive(Clone)]
pub(crate) struct ReplicaEndpointUpdate(Body);

impl ReplicaEndpointUpdate {
    pub(crate) fn prepare(body: wire::ReplicaMessage) -> Result<Self> {
        encode(body).map(Self)
    }
}

#[derive(Clone, Copy)]
enum SendLane {
    Replies,
    Ordinary,
}

impl IpcClient {
    pub(crate) async fn connect(
        address: SocketAddr,
        deadline: Instant,
        host_id: &str,
        server: RuntimeServer,
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
        let (session, peer) = Session::connect(address, deadline, server).await?;
        let hosting = HostingClient::new(
            PeerRequestService::new(peer.clone()),
            identity.clone(),
            session.clone(),
            serving,
        );
        let rap = RapClient::new(PeerSendService::new(peer), identity, session.clone());
        Ok(Self {
            session,
            hosting,
            rap,
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
        self.session.closed().await
    }

    /// Release domain ownership, not an awaitable furpc driver teardown barrier.
    pub(crate) async fn close(&self) -> Result<()> {
        self.abort();
        Ok(())
    }

    /// Clone the typed stub for independent calls on the original generation.
    pub(crate) fn hosting(&self) -> HostingClient<PeerRequestService> {
        self.hosting.clone()
    }

    pub(crate) fn rap(&self) -> RapClient<PeerSendService> {
        self.rap.clone()
    }
}

/// Typed RAP sends over a byte service bound to the original IPC generation.
#[derive(Clone)]
pub(crate) struct RapClient<T> {
    inner: OneWayClient<RapSendService<T>>,
}

impl<T> RapClient<T> {
    fn new(transport: T, identity: HeaderRecord, session: Arc<Session>) -> Self {
        Self {
            inner: OneWayClient::new(RapSendService {
                transport,
                identity,
                session,
                valid: Arc::new(|| true),
                lane: SendLane::Ordinary,
            }),
        }
    }
}

impl<T> RapClient<T>
where
    T: Service<Request<Body>, Response = WriteReceipt> + Send,
    T::Future: Send + 'static,
    T::Error: Into<BoxError>,
{
    pub(crate) async fn send_replica_reply(
        &mut self,
        kind: ProxyReplyKind,
        body: wire::ProxyReply,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        if !matches!(
            kind,
            ProxyReplyKind::ReplicaOpen | ProxyReplyKind::ReplicaClose
        ) {
            return Err(Error::InvalidInput("unauthorized runtime semantic reply"));
        }
        self.send(
            RapMethod::ReplicaReply(kind),
            body,
            FubstCodec::<_, ()>::default(),
            deadline,
            valid,
            SendLane::Replies,
        )
        .await
    }

    pub(crate) async fn send_stateful_reply(
        &mut self,
        kind: ProxyReplyKind,
        body: wire::ProxyReply,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.send(
            RapMethod::ReplicaReply(kind),
            body,
            FubstCodec::<_, ()>::default(),
            deadline,
            valid,
            SendLane::Ordinary,
        )
        .await
    }

    pub(crate) async fn send_service_update_reply(
        &mut self,
        body: wire::UpdateServiceReply,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.send(
            RapMethod::ServiceUpdateReply,
            body,
            FubstCodec::<_, ()>::default(),
            deadline,
            valid,
            SendLane::Replies,
        )
        .await
    }

    pub(crate) async fn send_stateful_service_update_reply(
        &mut self,
        body: wire::UpdateServiceReply,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.send(
            RapMethod::ServiceUpdateReply,
            body,
            FubstCodec::<_, ()>::default(),
            deadline,
            valid,
            SendLane::Ordinary,
        )
        .await
    }

    pub(crate) async fn report_load(
        &mut self,
        body: wire::LoadBody,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.send(
            RapMethod::Load,
            body,
            FubstCodec::<_, ()>::default(),
            deadline,
            valid,
            SendLane::Ordinary,
        )
        .await
    }

    pub(crate) async fn report_fault(
        &mut self,
        report: FaultReport,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.send(
            RapMethod::Fault,
            report.0,
            PreparedBodyCodec,
            deadline,
            valid,
            SendLane::Ordinary,
        )
        .await
    }

    pub(crate) async fn notify_replica_endpoint_updated(
        &mut self,
        update: ReplicaEndpointUpdate,
        deadline: Instant,
        valid: Check,
    ) -> Result<()> {
        self.send(
            RapMethod::EndpointUpdated,
            update.0,
            PreparedBodyCodec,
            deadline,
            valid,
            SendLane::Ordinary,
        )
        .await
    }

    async fn send<C>(
        &mut self,
        method: RapMethod,
        body: C::Encode,
        codec: C,
        deadline: Instant,
        valid: Check,
        lane: SendLane,
    ) -> Result<()>
    where
        C: Codec + Send,
        C::Encode: Send,
        C::Encoder: Send,
    {
        let (actor, action) = method.envelope();
        let request = Request::new(body)
            .append_header(header::actor(self::actor(actor)?).map_err(native_error)?)
            .map_err(native_error)?
            .append_header(header::action(action).map_err(native_error)?)
            .map_err(native_error)?;
        let service = self.inner.get_mut();
        service.valid = valid;
        service.lane = lane;
        self.inner
            .send(request, codec, deadline)
            .await
            .map_err(native_error)?;
        Ok(())
    }
}

fn encode<T: Serialize>(value: T) -> Result<Body> {
    codec::encode(
        &mut FubstCodec::<T, ()>::default().encoder(),
        value,
        ClientCodecLimits::default().max_encoded_body_bytes,
    )
    .map_err(|source| {
        native_error(furpc::Error::Codec {
            phase: "encode",
            source,
        })
    })
}

// Adopt already-serialized retry bytes without serializing or copying them again.
struct PreparedBodyCodec;

impl Codec for PreparedBodyCodec {
    type Encode = Body;
    type Decode = ();
    type Encoder = Self;
    type Decoder = <FubstCodec<(), ()> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        Self
    }

    fn decoder(&mut self) -> Self::Decoder {
        FubstCodec::<(), ()>::default().decoder()
    }
}

impl Encoder for PreparedBodyCodec {
    type Item = Body;

    fn encode(
        &mut self,
        body: Body,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), furpc::CodecError> {
        dst.append_body(body)
    }
}

#[derive(Clone)]
struct RapSendService<T> {
    transport: T,
    identity: HeaderRecord,
    session: Arc<Session>,
    valid: Check,
    lane: SendLane,
}

impl<T> Service<Request<Body>> for RapSendService<T>
where
    T: Service<Request<Body>, Response = WriteReceipt> + Send,
    T::Future: Send + 'static,
    T::Error: Into<BoxError>,
{
    type Response = WriteReceipt;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.transport
            .poll_ready(cx)
            .map_err(|error| service_error(error.into()))
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let prepare = (|| {
            let (admission, exhausted) = match self.lane {
                SendLane::Replies => (&self.session.replies, "runtime semantic reply admission"),
                SendLane::Ordinary => (&self.session.ordinary, "runtime report output admission"),
            };
            let slot = admission
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::ResourceLimit(exhausted))?;
            self.session.check()?;
            if !(self.valid)() {
                return Err(Error::Closed);
            }
            request
                .headers_mut()
                .append(self.identity.clone())
                .map_err(native_error)?;
            outbound(request.headers(), false)?;
            Ok(slot)
        })();
        let slot = match prepare {
            Ok(slot) => slot,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let future = self.transport.call(request);
        let session = self.session.clone();
        let valid = self.valid.clone();
        Box::pin(async move {
            let _slot = slot;
            let result = future.await.map_err(|error| service_error(error.into()));
            if let Err(error) = &result {
                if matches!(error.kind(), ErrorKind::ObjectClosed | ErrorKind::Transport) {
                    session.retire(error.clone());
                }
            }
            let receipt = result?;
            session.check()?;
            if !valid() {
                return Err(Error::Closed);
            }
            Ok(receipt)
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum HostingMethod {
    StartHost,
    FinishHost,
    RegisterRuntime,
    RegisterType,
    UnregisterRuntime,
    UnregisterHost,
    Lease,
    Health,
}

impl HostingMethod {
    const ALL: [Self; 8] = [
        Self::StartHost,
        Self::FinishHost,
        Self::RegisterRuntime,
        Self::RegisterType,
        Self::UnregisterRuntime,
        Self::UnregisterHost,
        Self::Lease,
        Self::Health,
    ];

    fn envelope(self) -> (i64, &'static str) {
        match self {
            Self::StartHost => (
                actors::APPLICATION_HOST_MANAGER,
                "StartRegisterApplicationHostRequest",
            ),
            Self::FinishHost => (
                actors::APPLICATION_HOST_MANAGER,
                "FinishRegisterApplicationHostRequest",
            ),
            Self::RegisterRuntime => (
                actors::FABRIC_RUNTIME_MANAGER,
                "RegisterFabricRuntimeRequest",
            ),
            Self::RegisterType => (actors::FABRIC_RUNTIME_MANAGER, "RegisterServiceTypeRequest"),
            Self::UnregisterRuntime => (
                actors::FABRIC_RUNTIME_MANAGER,
                "UnregisterFabricRuntimeRequest",
            ),
            Self::UnregisterHost => (
                actors::APPLICATION_HOST_MANAGER,
                "UnregisterApplicationHostRequest",
            ),
            Self::Lease => (actors::HOSTING, ""),
            Self::Health => (actors::APPLICATION_HOST_MANAGER, "ReportHealth"),
        }
    }

    fn cleanup(self) -> bool {
        matches!(self, Self::UnregisterRuntime | Self::UnregisterHost)
    }

    fn reply_validator(self) -> fn(&Response<Body>) -> Result<()> {
        match self {
            Self::Health => check_health_reply,
            _ => check_hosting_reply,
        }
    }
}

/// Typed Hosting RPCs over a byte service; clone explicitly for concurrent calls.
#[derive(Clone)]
pub(crate) struct HostingClient<T> {
    inner: RpcClient<IpcService<T>>,
    session: Arc<Session>,
    serving: Option<Check>,
}

impl<T> HostingClient<T> {
    fn new(
        transport: T,
        identity: HeaderRecord,
        session: Arc<Session>,
        serving: Option<Check>,
    ) -> Self {
        Self {
            inner: RpcClient::new(IpcService {
                transport,
                identity,
                validate_reply: check_hosting_reply,
                valid: Arc::new(|| true),
            }),
            session,
            serving,
        }
    }
}

impl<T> HostingClient<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send + 'static,
    T::Error: Into<BoxError>,
{
    pub(crate) async fn start_register_application_host(
        &mut self,
        request: Request<wire::StartRequest>,
        deadline: Instant,
    ) -> Result<Response<wire::StartReply>> {
        self.unary(request, HostingMethod::StartHost, deadline, None)
            .await
    }

    pub(crate) async fn finish_register_application_host(
        &mut self,
        request: Request<wire::IdRequest>,
        deadline: Instant,
    ) -> Result<Response<wire::ErrorReply>> {
        self.unary(request, HostingMethod::FinishHost, deadline, None)
            .await
    }

    pub(crate) async fn register_fabric_runtime(
        &mut self,
        request: Request<wire::RuntimeRequest>,
        deadline: Instant,
    ) -> Result<Response<wire::ErrorReply>> {
        self.unary(request, HostingMethod::RegisterRuntime, deadline, None)
            .await
    }

    pub(crate) async fn register_service_type(
        &mut self,
        request: Request<wire::TypeRequest>,
        deadline: Instant,
    ) -> Result<Response<wire::ErrorReply>> {
        self.unary(request, HostingMethod::RegisterType, deadline, None)
            .await
    }

    pub(crate) async fn unregister_fabric_runtime(
        &mut self,
        request: Request<wire::IdRequest>,
        deadline: Instant,
    ) -> Result<Response<wire::ErrorReply>> {
        self.unary(request, HostingMethod::UnregisterRuntime, deadline, None)
            .await
    }

    pub(crate) async fn unregister_application_host(
        &mut self,
        request: Request<wire::UnregisterHost>,
        deadline: Instant,
    ) -> Result<Response<wire::ErrorReply>> {
        self.unary(request, HostingMethod::UnregisterHost, deadline, None)
            .await
    }

    pub(crate) async fn poll_lease(
        &mut self,
        request: Request<()>,
        deadline: Instant,
    ) -> Result<Response<wire::LeaseReply>> {
        self.unary(request, HostingMethod::Lease, deadline, None)
            .await
    }

    pub(crate) async fn report_health(
        &mut self,
        request: Request<wire::HealthBody>,
        deadline: Instant,
        valid: Check,
    ) -> Result<Response<wire::HealthReply>> {
        self.unary(request, HostingMethod::Health, deadline, Some(valid))
            .await
    }

    pub(super) async fn unary<Q, R>(
        &mut self,
        mut request: Request<Q>,
        method: HostingMethod,
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
        let serving = (!method.cleanup()).then(|| self.serving.clone()).flatten();
        let check = Arc::new(move || {
            serving.as_ref().is_none_or(|check| check())
                && valid.as_ref().is_none_or(|check| check())
        });
        let (actor, action) = method.envelope();
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
        self.inner.get_mut().validate_reply = method.reply_validator();
        self.inner.get_mut().valid = check.clone();
        let result = self
            .inner
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
}

pub(super) fn actor(value: i64) -> Result<ActorId> {
    ActorId::new(value).map_err(native_error)
}

fn action(headers: &Headers) -> Result<String> {
    String::from_utf16(headers.effective().action())
        .map_err(|_| Error::Protocol("invalid runtime IPC action"))
}

fn outbound(headers: &Headers, request: bool) -> Result<()> {
    let actor = headers.effective().actor.get();
    let action = action(headers)?;
    let allowed = if request {
        HostingMethod::ALL
            .iter()
            .any(|method| method.envelope() == (actor, action.as_str()))
    } else {
        RapMethod::ALL
            .iter()
            .any(|method| method.envelope() == (actor, action.as_str()))
    };
    if allowed {
        Ok(())
    } else {
        Err(Error::InvalidInput("unauthorized runtime IPC submission"))
    }
}

#[derive(Clone)]
struct IpcService<T> {
    transport: T,
    identity: HeaderRecord,
    validate_reply: fn(&Response<Body>) -> Result<()>,
    valid: Check,
}

fn check_hosting_reply(response: &Response<Body>) -> Result<()> {
    if matches!(
        response.headers().effective().actor.get(),
        actors::FILE_SENDER | actors::FILE_TRANSFER_CLIENT
    ) {
        return Err(Error::Protocol("unexpected runtime IPC reply envelope"));
    }
    Ok(())
}

fn check_health_reply(response: &Response<Body>) -> Result<()> {
    let properties = response.headers().effective();
    if properties.actor.get() != actors::HM
        || !properties
            .action()
            .iter()
            .copied()
            .eq("OperationSuccessAction".encode_utf16())
    {
        return Err(Error::Protocol("unexpected runtime IPC reply envelope"));
    }
    Ok(())
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
        let validate_reply = self.validate_reply;
        Box::pin(async move {
            let response = future.await.map_err(|error| service_error(error.into()))?;
            validate_reply(&response)?;
            Ok(response)
        })
    }
}

struct Connector {
    address: SocketAddr,
    session: Weak<Session>,
    attempted: bool,
}

impl Connector {
    fn new(address: SocketAddr, session: Weak<Session>) -> Self {
        Self {
            address,
            session,
            attempted: false,
        }
    }
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
        let attempted = std::mem::replace(&mut self.attempted, true);
        Box::pin(async move {
            let owner = session.upgrade().ok_or(Error::Closed)?;
            owner.check()?;
            // Replacement I/O cannot inherit this attachment's registration.
            if attempted {
                return Err(Error::Closed);
            }
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
            let kind = if matches!(&source, furpc::CodecError::ResourceLimit { .. }) {
                ErrorKind::ResourceExhausted
            } else {
                ErrorKind::Transport
            };
            Error::with_cause(kind, format!("runtime IPC {phase}"), source)
        }
        error @ (furpc::Error::Transport { .. } | furpc::Error::Handler { .. }) => {
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
