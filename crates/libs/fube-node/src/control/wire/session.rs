use super::{bounded, inbound, outbound, Role};
use furpc::{
    Accept, Accepted, ActorId, Body, Connected, ConnectionOptions, Error, HandlerLimits,
    IngressLimits, IngressResponse, IoInfo, Message, OneWayRouteOptions, Peer, Request,
    RequestPolicy, RequestRouteOptions, Router, Server, Source, TransportLimits,
};
use std::{
    future::{ready, Future, Ready},
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{mpsc, oneshot, Notify},
    task::{AbortHandle, JoinHandle},
    time::{timeout_at, Instant},
};
use tower_service::Service;

type FutureResult<T> = Pin<Box<dyn Future<Output = furpc::Result<T>> + Send>>;

#[derive(Debug, Clone)]
pub enum Failure {
    Io(Arc<io::Error>),
    Native(Error),
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Native(error) => error.fmt(f),
        }
    }
}

#[derive(Default)]
struct State {
    error: Mutex<Option<Failure>>,
    changed: Notify,
    released: Notify,
    io_live: std::sync::atomic::AtomicBool,
}
impl State {
    fn fail(&self, error: Failure) {
        self.error.lock().unwrap().get_or_insert(error);
        self.changed.notify_waiters();
    }
    async fn stopped(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.error.lock().unwrap().is_some() {
                return;
            }
            changed.await;
        }
    }
    async fn released(&self) {
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if !self.io_live.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            released.await;
        }
    }
}

struct OwnedIo {
    stream: Option<TcpStream>,
    state: Arc<State>,
}
impl Drop for OwnedIo {
    fn drop(&mut self) {
        drop(self.stream.take());
        self.state.fail(Failure::Native(Error::Closed));
        self.state
            .io_live
            .store(false, std::sync::atomic::Ordering::Release);
        self.state.released.notify_waiters();
    }
}
impl AsyncRead for OwnedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let available = buf.remaining();
        let result = Pin::new(self.stream.as_mut().expect("owned socket")).poll_read(cx, buf);
        match &result {
            Poll::Ready(Ok(())) if available > 0 && buf.filled().len() == before => {
                self.state.fail(Failure::Native(Error::Closed))
            }
            Poll::Ready(Err(error)) => self.state.fail(Failure::Io(Arc::new(
                error
                    .raw_os_error()
                    .map(io::Error::from_raw_os_error)
                    .unwrap_or_else(|| io::Error::new(error.kind(), error.to_string())),
            ))),
            _ => {}
        }
        result
    }
}
impl AsyncWrite for OwnedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.stream.as_mut().expect("owned socket")).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("owned socket")).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("owned socket")).poll_shutdown(cx)
    }
}

pub struct Incoming {
    pub message: Message,
    pub reply: Option<ResponseSlot>,
    pub source: Source,
    pub(super) one_way: Option<OneWaySlot>,
    pub(super) prepared: Option<super::one_way::Prepared>,
}

impl Incoming {
    pub fn action(&self) -> super::Result<String> {
        match &self.prepared {
            Some(prepared) => Ok(prepared.action.clone()),
            None => super::action(&self.message),
        }
    }

    pub(in crate::control) fn prepared(&self) -> super::Result<&super::one_way::Prepared> {
        self.prepared
            .as_ref()
            .ok_or_else(|| "one-way intake has no original typed handoff".into())
    }
}

pub(super) struct OneWaySlot {
    pub(super) request: Request<Body>,
    pub(super) response: oneshot::Sender<furpc::Result<IngressResponse>>,
    pub(super) role: Role,
}

pub struct ResponseSlot {
    pub(super) request: Request<Body>,
    pub(super) response: oneshot::Sender<furpc::Result<IngressResponse>>,
    role: Role,
}
impl ResponseSlot {
    /// Hand response data to the original furpc runner, not a local-write receipt.
    pub async fn reply(self, message: Message, deadline: Instant) -> furpc::Result<()> {
        if Instant::now() >= deadline {
            return Err(Error::Timeout("control response handoff"));
        }
        bounded(&message)?;
        let props = super::Properties::new(&message)?;
        match self.role {
            Role::Fm => super::ack(&message, &props)?,
            Role::Node => super::proposal_ack(&message, &props)?,
            Role::Peer => return Err(Error::Unsupported("peer RPC")),
        }
        self.response
            .send(Ok(IngressResponse::Reply(message)))
            .map_err(|_| Error::Closed)
    }
}

#[derive(Clone)]
struct Ingress {
    tx: mpsc::Sender<Incoming>,
    role: Role,
    state: Arc<State>,
}
impl Service<Request<Body>> for Ingress {
    type Response = IngressResponse;
    type Error = Error;
    type Future = FutureResult<IngressResponse>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let source = request
            .context()
            .expect("furpc received context")
            .source()
            .clone();
        let expects_reply = request.headers().effective().expects_reply;
        let mut builder = Message::builder().body(request.body().clone());
        for header in request.headers().records() {
            builder = builder.append_header(header.to_owned());
        }
        let result = builder.build().and_then(|message| {
            inbound(self.role, &message)?;
            let (response, one_way, receive) = if expects_reply {
                let (tx, rx) = oneshot::channel();
                (
                    Some(ResponseSlot {
                        request,
                        response: tx,
                        role: self.role,
                    }),
                    None,
                    Some(rx),
                )
            } else if self.role != Role::Fm {
                let (tx, rx) = oneshot::channel();
                (
                    None,
                    Some(OneWaySlot {
                        request,
                        response: tx,
                        role: self.role,
                    }),
                    Some(rx),
                )
            } else {
                (None, None, None)
            };
            self.tx
                .try_send(Incoming {
                    message,
                    source,
                    reply: response,
                    one_way,
                    prepared: None,
                })
                .map_err(|_| Error::ResourceLimit("Node control inbox"))?;
            Ok(receive)
        });
        if let Err(error) = &result {
            self.state.fail(Failure::Native(error.clone()));
        }
        Box::pin(async move {
            match result? {
                Some(response) => response.await.map_err(|_| Error::Closed)?,
                None => Ok(IngressResponse::NoReply),
            }
        })
    }
}

struct Connector(Option<Connected<OwnedIo>>);
impl<Q> Service<Q> for Connector {
    type Response = Connected<OwnedIo>;
    type Error = Error;
    type Future = Ready<furpc::Result<Self::Response>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Q) -> Self::Future {
        ready(self.0.take().ok_or(Error::Closed))
    }
}
struct OneSocket(Option<Accepted<OwnedIo>>);
impl Accept for OneSocket {
    type Io = OwnedIo;
    type Error = Error;
    async fn accept(&mut self) -> furpc::Result<Option<Accepted<OwnedIo>>> {
        Ok(self.0.take())
    }
}
#[derive(Clone)]
struct PeerReady(Arc<Mutex<Option<oneshot::Sender<Peer>>>>);
impl Service<Peer> for PeerReady {
    type Response = ();
    type Error = Error;
    type Future = Ready<furpc::Result<()>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, peer: Peer) -> Self::Future {
        ready(
            self.0
                .lock()
                .unwrap()
                .take()
                .ok_or(Error::Protocol("second control generation"))
                .and_then(|tx| tx.send(peer).map_err(|_| Error::Closed)),
        )
    }
}

struct Owner {
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    abort: AbortHandle,
    state: Arc<State>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.state.fail(Failure::Native(Error::Closed));
        self.abort.abort();
    }
}
pub struct Session {
    owner: Arc<Owner>,
    pub(super) peer: Peer,
    incoming: tokio::sync::Mutex<mpsc::Receiver<Incoming>>,
    role: Role,
}
impl Session {
    pub(super) fn role(&self) -> Role {
        self.role
    }
    pub fn terminal_error(&self) -> Option<Failure> {
        self.owner.state.error.lock().unwrap().clone()
    }
    pub fn retire(&self) {
        self.owner.state.fail(Failure::Native(Error::Closed));
    }
    pub async fn closed(&self, deadline: Instant) -> super::Result<()> {
        self.retire();
        timeout_at(deadline, async {
            let mut task = self.owner.task.lock().await;
            let result = match task.as_mut() {
                Some(task) => task.await.map_err(super::err),
                None => Ok(()),
            };
            task.take();
            result?;
            self.owner.state.released().await;
            let mut incoming = self.incoming.lock().await;
            incoming.close();
            while incoming.try_recv().is_ok() {}
            Ok(())
        })
        .await
        .map_err(super::err)?
    }
    pub async fn receive(
        &self,
        deadline: Instant,
    ) -> std::result::Result<Option<Incoming>, Failure> {
        let Ok(mut incoming) = timeout_at(deadline, self.incoming.lock()).await else {
            return Ok(None);
        };
        tokio::select! {
            biased;
            message = incoming.recv() => message.map(Some).ok_or_else(|| self.terminal_error().unwrap_or(Failure::Native(Error::Closed))),
            _ = self.owner.state.stopped() => Err(self.terminal_error().expect("stopped session")),
            _ = tokio::time::sleep_until(deadline) => Ok(None),
        }
    }
    pub async fn send(&self, message: Message, deadline: Instant) -> furpc::Result<()> {
        if self.terminal_error().is_some() {
            return Err(Error::Closed);
        }
        outbound(self.role, &message, false)?;
        self.peer.send(message, deadline).await.map(|_| ())
    }
    #[cfg(test)]
    pub(super) fn queued(&self) -> usize {
        self.incoming.try_lock().expect("unpolled test inbox").len()
    }
}

fn limits() -> TransportLimits {
    TransportLimits {
        max_receive_frame_bytes: super::LIMIT,
        max_send_frame_bytes: Some(super::LIMIT),
        max_pending_sends: 8,
        max_pending_requests: 8,
        max_connections: 1,
        max_handshakes: 1,
        max_peer_waiters: 1,
        send_queue_bytes: 8 * super::LIMIT,
        frame_timeout: Duration::from_secs(3),
        discard_timeout: Duration::from_secs(3),
        write_timeout: Duration::from_secs(3),
        max_discard_bytes: super::LIMIT,
        ..TransportLimits::default()
    }
}

pub(super) async fn establish(
    stream: TcpStream,
    role: Role,
    until: Instant,
    accepted: bool,
) -> super::Result<Session> {
    stream.set_nodelay(true).map_err(super::err)?;
    let info = IoInfo {
        local_addr: Some(stream.local_addr().map_err(super::err)?),
        peer_addr: Some(stream.peer_addr().map_err(super::err)?),
    };
    let address = info.peer_addr.expect("TCP metadata");
    let state = Arc::new(State::default());
    state
        .io_live
        .store(true, std::sync::atomic::Ordering::Release);
    let io = OwnedIo {
        stream: Some(stream),
        state: state.clone(),
    };
    let (tx, incoming) = mpsc::channel(8);
    let (actor, actions, requests): (_, &[&str], &[&str]) = match role {
        Role::Fm => (
            super::FM_ACTOR,
            &[
                "ReplicaUp",
                "ServiceTypeNotification",
                "AddInstanceReply",
                "AddPrimaryReply",
                "AddReplicaReply",
                "DoReconfigurationReply",
                "ChangeConfiguration",
                "ReplicaEndpointUpdated",
                "RemoveReplicaReply",
                "RemoveInstanceReply",
                "DeleteReplicaReply",
                "NodeActivateReply",
                "NodeDeactivateReply",
                "NodeUpdateServiceReply",
                "ServiceTypeEnabled",
                "ServiceTypeDisabled",
                "ReportLoad",
                "GenerationUpdateReject",
            ],
            &["NodeUp", "LFUMUpload"],
        ),
        Role::Node => (
            super::RA_ACTOR,
            &[
                "AddInstance",
                "AddPrimary",
                "AddReplica",
                "DoReconfiguration",
                "ChangeConfigurationReply",
                "RemoveReplica",
                "RemoveInstance",
                "DeleteReplica",
                "ReplicaUpReply",
                "ServiceTypeNotificationReply",
                "ReplicaEndpointUpdatedReply",
                "NodeActivateRequest",
                "NodeDeactivateRequest",
                "NodeUpdateServiceRequest",
                "ServiceTypeEnabledReply",
                "ServiceTypeDisabledReply",
                "GenerationUpdate",
            ],
            &["GenerationProposal"],
        ),
        Role::Peer => (
            super::RA_ACTOR,
            &[
                "CreateReplica",
                "CreateReplicaReply",
                "Activate",
                "ActivateReply",
                "Deactivate",
                "DeactivateReply",
                "GetLSN",
                "GetLSNReply",
                "DoReconfiguration",
            ],
            &[],
        ),
    };
    let actor = ActorId::new(actor).map_err(super::err)?;
    let service = Ingress {
        tx,
        role,
        state: state.clone(),
    };
    let mut router = Router::new();
    for action in actions {
        router = router
            .one_way(
                actor,
                *action,
                OneWayRouteOptions::new(
                    |_, _| Ok(()),
                    IngressLimits {
                        max_body_bytes: super::LIMIT,
                    },
                ),
                service.clone(),
            )
            .map_err(super::err)?;
    }
    for action in requests {
        router = router
            .request(
                actor,
                *action,
                RequestRouteOptions::new(
                    RequestPolicy::new(|_, _| Ok(()), |_, _| Ok(())),
                    IngressLimits {
                        max_body_bytes: super::LIMIT,
                    },
                ),
                service.clone(),
            )
            .map_err(super::err)?;
    }
    let service = router.into_service().map_err(super::err)?;
    let handlers = HandlerLimits {
        max_encoded_body_bytes_per_response: super::LIMIT,
        timeout: Duration::from_secs(3),
    };
    let (ready_tx, ready_rx) = oneshot::channel();
    let ready = Arc::new(Mutex::new(Some(ready_tx)));
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        let run = async {
            if accepted {
                let server = Server::builder()
                    .limits(limits())
                    .handler_limits(handlers)
                    .handshake_timeout(until.saturating_duration_since(Instant::now()))
                    .service(service)?
                    .on_peer_ready(
                        furpc::SendPolicy::new(|_, _, _| Ok(())),
                        PeerReady(ready.clone()),
                    )?
                    .build()?;
                tokio::select! {
                    biased;
                    _ = task_state.stopped() => Ok(()),
                    result = server.serve_with_shutdown(OneSocket(Some(Accepted::new(io, info))), ConnectionOptions::managed(), std::future::pending()) => result,
                }
            } else {
                let endpoint = furpc::Endpoint::try_from(address)?
                    .limits(limits())
                    .handler_limits(handlers)
                    .connect_timeout(until.saturating_duration_since(Instant::now()))
                    .service(service)?;
                let channel = endpoint
                    .connect_with_connector(Connector(Some(Connected::new(io, info))))
                    .await?;
                let peer = channel.peer(until).await?;
                ready
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or(Error::Closed)?
                    .send(peer)
                    .map_err(|_| Error::Closed)?;
                task_state.stopped().await;
                drop(channel);
                Ok(())
            }
        };
        if let Err(error) = run.await {
            task_state.fail(Failure::Native(error));
        }
        task_state.fail(Failure::Native(Error::Closed));
    });
    let owner = Arc::new(Owner {
        abort: task.abort_handle(),
        task: tokio::sync::Mutex::new(Some(task)),
        state,
    });
    let peer = match timeout_at(until, ready_rx).await {
        Ok(Ok(peer)) => peer,
        result => {
            owner.state.fail(Failure::Native(Error::Closed));
            let task = owner.task.lock().await.take().expect("new owner");
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(format!("control driver join failed: {error}")),
            }
            owner.state.released().await;
            return Err(format!(
                "control establishment failed: {result:?}; {:?}",
                owner.state.error.lock().unwrap()
            ));
        }
    };
    Ok(Session {
        owner,
        peer,
        incoming: tokio::sync::Mutex::new(incoming),
        role,
    })
}
