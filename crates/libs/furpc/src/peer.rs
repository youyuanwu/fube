use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};
use std::task::{Context, Poll};

use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;
use tower_service::Service;

use crate::{
    connection::{ConnectionHandle, ConnectionObserver, ListenInstance, Phase, SendReceipt},
    rpc,
    service::Registry,
    transport::{BoxFuture, IoInfo},
    Body, EnvelopeCheck, Error, Headers, Message, MessageId, NotificationPolicy, Operation,
    Request, Response, Result, WriteReceipt,
};

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub generation: u64,
    pub io: IoInfo,
    pub listen_address: Arc<[u16]>,
    pub listen_instance: u64,
}

#[derive(Debug, Clone)]
pub struct Source {
    info: PeerInfo,
}
impl Source {
    pub fn generation(&self) -> u64 {
        self.info.generation
    }
    pub fn info(&self) -> &PeerInfo {
        &self.info
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    OneWay,
    Request,
    Notification,
}
pub type SendCheck = fn(SendKind, &Headers, &Source) -> Result<()>;

#[derive(Debug, Clone, Copy)]
pub struct SendPolicy {
    check: SendCheck,
}
impl SendPolicy {
    pub fn new(check: SendCheck) -> Self {
        Self { check }
    }
}

pub(crate) struct Generation {
    pub(crate) handle: ConnectionHandle,
    pub(crate) source: Source,
    pub(crate) registry: Arc<Registry>,
    pub(crate) rpc: Arc<rpc::Table>,
}
impl Generation {
    pub(crate) fn new(
        handle: ConnectionHandle,
        io: IoInfo,
        remote: ListenInstance,
        registry: Arc<Registry>,
    ) -> Result<Arc<Self>> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let generation = NEXT
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| Error::ResourceLimit("connection generation space"))?;
        let core = Arc::new(Self {
            handle,
            source: Source {
                info: PeerInfo {
                    generation,
                    io,
                    listen_address: remote.address.into(),
                    listen_instance: remote.instance,
                },
            },
            registry,
            rpc: Arc::new(rpc::Table::default()),
        });
        core.registry.track(&core)?;
        Ok(core)
    }
    pub(crate) fn peer(self: &Arc<Self>, policy: Option<SendPolicy>) -> Peer {
        Peer {
            generation: Arc::downgrade(self),
            connection: self.handle.observer(),
            source: self.source.clone(),
            policy,
        }
    }
    pub(crate) fn is_open(&self) -> bool {
        !self.registry.is_stopping() && self.handle.phase() == Phase::Open
    }
    pub(crate) fn reply(
        &self,
        message: Message,
        id: MessageId,
        check: EnvelopeCheck,
        deadline: Instant,
        slot: OwnedSemaphorePermit,
    ) -> Result<SendReceipt> {
        let message = rpc::envelope(
            message.headers().clone(),
            message.into_body(),
            rpc::next_id(),
            rpc::EnvelopeKind::Reply(id),
        )?;
        let authorization = SendAuthorization {
            headers: message.headers().clone(),
            source: self.source.clone(),
            policy: None,
            registry: Arc::downgrade(&self.registry),
            kind: SendKind::OneWay,
            notification: None,
            reply: Some(check),
            _reply_slot: Some(slot),
        };
        self.handle
            .send_authorized(message, deadline, Some(authorization))
            .map_err(Error::from)
    }
}
impl Drop for Generation {
    fn drop(&mut self) {
        self.rpc.close(
            self.handle
                .observer()
                .error()
                .map(Error::from)
                .unwrap_or(Error::Closed),
        );
    }
}

pub(crate) struct Reservation {
    pub(crate) peer: Peer,
    pub(crate) slot: OwnedSemaphorePermit,
}

/// Nonowning authority for one original generation. It never reconnects.
#[derive(Clone)]
pub struct Peer {
    generation: Weak<Generation>,
    connection: ConnectionObserver,
    source: Source,
    policy: Option<SendPolicy>,
}
impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("generation", &self.source.generation())
            .finish_non_exhaustive()
    }
}
impl Peer {
    pub fn info(&self) -> PeerInfo {
        self.source.info.clone()
    }
    pub fn send(&self, message: Message, deadline: Instant) -> Operation<WriteReceipt> {
        Operation::send(self.clone(), message, deadline)
    }
    pub fn request(&self, request: Request<Body>, deadline: Instant) -> Operation<Response<Body>> {
        Operation::request(self.clone(), request, deadline, None, None)
    }
    /// Send a native notification and await its service-defined acknowledgment.
    /// Requires managed mode and does not reconnect or replay on generation loss.
    ///
    /// Either connection role may originate notifications. The receiving service
    /// returns [`crate::IngressResponse::Reply`] with application-defined data;
    /// the transport supplies the acknowledgment's native correlation and flags.
    ///
    /// ```no_run
    /// use furpc::{Body, NotificationPolicy, Peer, Request};
    /// use tokio::time::{Duration, Instant};
    ///
    /// async fn push(peer: &Peer) -> furpc::Result<Body> {
    ///     let policy = NotificationPolicy::new(|_, _| Ok(()), |_, _| Ok(()));
    ///     let response = peer.notify(
    ///         Request::new(Body::from_bytes(vec![1, 2, 3])?),
    ///         policy, Instant::now() + Duration::from_secs(5),
    ///     ).await?;
    ///     Ok(response.into_body())
    /// }
    /// ```
    pub fn notify(
        &self,
        request: Request<Body>,
        policy: NotificationPolicy,
        deadline: Instant,
    ) -> Operation<Response<Body>> {
        Operation::request(self.clone(), request, deadline, None, Some(policy))
    }
    fn core(&self) -> Result<Arc<Generation>> {
        if let Some(error) = self.connection.error() {
            return Err(error.into());
        }
        let core = self.generation.upgrade().ok_or(Error::Closed)?;
        if !core.is_open() {
            return Err(Error::Closed);
        }
        Ok(core)
    }
    pub(crate) fn reserve(&self) -> Result<Reservation> {
        let core = self.core()?;
        if !core.registry.managed {
            return Err(Error::Unsupported("RPC in raw-only mode"));
        }
        let slot = core
            .registry
            .rpc_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("pending RPC/results"))?;
        Ok(Reservation {
            peer: self.clone(),
            slot,
        })
    }
    pub(crate) fn start_request(
        &self,
        request: Request<Body>,
        deadline: Instant,
        slot: Option<OwnedSemaphorePermit>,
        notification: Option<NotificationPolicy>,
    ) -> Result<(SendReceipt, rpc::Registration)> {
        let core = self.core()?;
        if !core.registry.managed {
            return Err(Error::Unsupported("RPC in raw-only mode"));
        }
        let slot = match slot {
            Some(slot) => slot,
            None => self.reserve()?.slot,
        };
        let id = rpc::next_id();
        let (parts, body) = request.into_parts();
        let kind = if notification.is_some() {
            rpc::EnvelopeKind::Notification
        } else {
            rpc::EnvelopeKind::Request
        };
        let message = rpc::envelope(parts.headers, body, id, kind)?;
        let registration = core.rpc.insert(
            id,
            slot,
            deadline,
            notification.map(|policy| policy.acknowledgment),
        )?;
        let authorization = SendAuthorization {
            headers: message.headers().clone(),
            source: self.source.clone(),
            policy: self.policy,
            registry: Arc::downgrade(&core.registry),
            kind: if notification.is_some() {
                SendKind::Notification
            } else {
                SendKind::Request
            },
            notification: notification.map(|policy| policy.notification),
            reply: None,
            _reply_slot: None,
        };
        let receipt = core
            .handle
            .send_authorized(message, deadline, Some(authorization))
            .map_err(Error::from)?;
        Ok((receipt, registration))
    }
    pub(crate) fn submit(&self, message: Message, deadline: Instant) -> Result<SendReceipt> {
        if let Some(error) = self.connection.error() {
            return Err(error.into());
        }
        let core = self.core()?;
        let common = message.headers().effective();
        if !core.registry.managed
            && (common.expects_reply
                || common.uncorrelated_reply
                || common.relates_to != crate::MessageId::default())
        {
            return Err(Error::Unsupported(
                "raw-only one-way sends cannot originate replies or RPC",
            ));
        }
        let message = if core.registry.managed {
            rpc::envelope(
                message.headers().clone(),
                message.into_body(),
                rpc::next_id(),
                rpc::EnvelopeKind::OneWay,
            )?
        } else {
            message
        };
        let authorization = SendAuthorization {
            headers: message.headers().clone(),
            source: self.source.clone(),
            policy: self.policy,
            registry: Arc::downgrade(&core.registry),
            kind: SendKind::OneWay,
            notification: None,
            reply: None,
            _reply_slot: None,
        };
        core.handle
            .send_authorized(message, deadline, Some(authorization))
            .map_err(Error::from)
    }
}

/// Byte RPC service over an already-authorized, nonowning original Peer.
/// Clones never reconnect or acquire replacement generation authority.
#[derive(Debug, Clone)]
pub struct PeerRequestService {
    peer: Peer,
}

impl PeerRequestService {
    pub fn new(peer: Peer) -> Self {
        Self { peer }
    }
}

impl Service<Request<Body>> for PeerRequestService {
    type Response = Response<Body>;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let peer = self.peer.clone();
        Box::pin(async move {
            let deadline = request
                .deadline()
                .ok_or(Error::InvalidInput("byte RPC requires a deadline"))?;
            peer.request(request, deadline).await
        })
    }
}

/// Byte one-way service over an already-authorized, nonowning original Peer.
/// Native correlation fields are not accepted, even in raw-only mode.
#[derive(Debug, Clone)]
pub struct PeerSendService {
    peer: Peer,
}

impl PeerSendService {
    pub fn new(peer: Peer) -> Self {
        Self { peer }
    }
}

impl Service<Request<Body>> for PeerSendService {
    type Response = WriteReceipt;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let peer = self.peer.clone();
        Box::pin(async move {
            let deadline = request
                .deadline()
                .ok_or(Error::InvalidInput("byte send requires a deadline"))?;
            let (parts, body) = request.into_parts();
            rpc::check_managed(&parts.headers)?;
            peer.send(Message::from_received(parts.headers, body), deadline)
                .await
        })
    }
}

pub(crate) struct SendAuthorization {
    headers: Headers,
    source: Source,
    policy: Option<SendPolicy>,
    registry: Weak<Registry>,
    kind: SendKind,
    notification: Option<EnvelopeCheck>,
    reply: Option<EnvelopeCheck>,
    _reply_slot: Option<OwnedSemaphorePermit>,
}
impl SendAuthorization {
    pub(crate) fn is_reply(&self) -> bool {
        self.reply.is_some()
    }
    pub(crate) fn check(&self) -> Result<()> {
        let registry = self.registry.upgrade().ok_or(Error::Closed)?;
        if registry.is_stopping() && self.reply.is_none() {
            return Err(Error::Closed);
        }
        self.check_commit()
    }
    pub(crate) fn check_commit(&self) -> Result<()> {
        let _registry = self.registry.upgrade().ok_or(Error::Closed)?;
        if let Some(check) = self.reply {
            return check(&self.headers, &self.source);
        }
        if let Some(policy) = self.policy {
            (policy.check)(self.kind, &self.headers, &self.source)?;
        }
        if let Some(check) = self.notification {
            check(&self.headers, &self.source)?;
        }
        Ok(())
    }
}
