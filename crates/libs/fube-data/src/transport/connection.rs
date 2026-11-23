//! Public furpc composition; route, queue, ACK and retirement policy stays in data.
use super::*;
use crate::wire::transport_error;
use furpc::{
    header, Accept, Accepted, Connected, ConnectionOptions, HandlerLimits, Headers, IngressLimits,
    IngressOptions, IngressResponse, IngressService, IoInfo, PeerInfo, Request, SendKind,
    SendPolicy, Server, ServiceBinding, Source, TransportLimits,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    future::{pending, Future},
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::oneshot,
    time::timeout_at,
};
use tower_service::Service;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

fn limits(settings: &Settings) -> TransportLimits {
    TransportLimits {
        max_receive_frame_bytes: MAX_FRAME_SIZE,
        max_send_frame_bytes: Some(MAX_FRAME_SIZE),
        send_queue_bytes: settings.queue_bytes + settings.control_bytes,
        max_pending_sends: settings.queue_operations + settings.control_messages,
        max_pending_requests: 1,
        max_connections: 1,
        max_handshakes: 1,
        max_peer_waiters: 1,
        frame_timeout: settings.callback_timeout,
        discard_timeout: settings.callback_timeout,
        max_discard_bytes: MAX_FRAME_SIZE,
        write_timeout: settings.callback_timeout,
        ..TransportLimits::default()
    }
}

fn handlers(settings: &Settings) -> HandlerLimits {
    HandlerLimits {
        max_encoded_body_bytes_per_response: MAX_FRAME_SIZE,
        timeout: settings.callback_timeout,
    }
}

fn envelope(headers: &Headers) -> furpc::Result<()> {
    let common = headers.effective();
    if common.actor.get() != 0
        || common.expects_reply
        || common.uncorrelated_reply
        || common.relates_to != MessageId::default()
        || headers
            .records()
            .filter(|header| header.id() == header::ACTOR)
            .count()
            > 1
        || headers
            .records()
            .any(|header| matches!(header.id().get(), 0x800b | 0x8029 | 0x8048))
    {
        return Err(furpc::Error::Protocol("invalid classic peer envelope"));
    }
    Ok(())
}

fn inbound(headers: &Headers, _: &Source) -> furpc::Result<()> {
    envelope(headers)
}

fn outbound(kind: SendKind, headers: &Headers, _: &Source) -> furpc::Result<()> {
    if kind != SendKind::OneWay {
        return Err(furpc::Error::InvalidInput("replication sends are one-way"));
    }
    envelope(headers)
}

struct Remote {
    address: String,
    socket: SocketAddr,
}

impl Remote {
    fn new(info: &PeerInfo) -> Result<Self> {
        let address = String::from_utf16(&info.listen_address)
            .map_err(|_| Error::Protocol("invalid peer listener UTF-16"))?;
        Endpoint {
            address: address.clone(),
            actor: Actor {
                partition: Uuid::from_u128(1),
                replica: 1,
                incarnation: Uuid::from_u128(1),
            },
            transport_id: None,
        }
        .to_string()
        .parse::<Endpoint>()?;
        let peer = info
            .io
            .peer_addr
            .ok_or(Error::Protocol("missing actual peer socket"))?;
        let socket = match address.parse::<SocketAddr>() {
            Ok(socket) => socket,
            Err(_) => {
                let port = address
                    .rsplit_once(':')
                    .and_then(|(_, port)| port.parse::<u16>().ok())
                    .filter(|port| *port != 0)
                    .ok_or(Error::Protocol("invalid peer listener port"))?;
                SocketAddr::new(peer.ip(), port)
            }
        };
        Ok(Self { address, socket })
    }
}

#[derive(Clone)]
struct IngressServiceImpl {
    id: u64,
    network: Arc<Network>,
    state: Arc<SessionState<Error>>,
}

impl IngressServiceImpl {
    fn receive(&self, request: Request<furpc::Body>) -> Result<()> {
        let context = request
            .context()
            .ok_or(Error::Protocol("missing peer ingress context"))?;
        if Instant::now() >= context.deadline() {
            return Err(Error::Timeout);
        }
        if self.state.error().is_some() {
            return Err(Error::Closed);
        }
        let peer = context
            .peer()
            .ok_or(Error::Protocol("missing original peer"))?
            .clone();
        let remote = Remote::new(&peer.info())?;
        let native = Message::builder().body(request.body().clone());
        let mut native = native;
        for header in request.headers().records() {
            native = native.append_header(header.to_owned());
        }
        let native = native.build().map_err(transport_error)?;
        let packet = Packet::decode(&native)?;
        if packet.from.address != remote.address {
            return Err(Error::Protocol("packet source differs from ListenInstance"));
        }
        validate_operation_limits(&packet, &self.network.settings)?;
        {
            let mut routes = self.network.routes.lock().unwrap();
            let full = routes.len() >= self.network.settings.connections;
            if let std::collections::hash_map::Entry::Vacant(entry) =
                routes.entry(packet.from.actor)
            {
                if full {
                    return Err(Error::ResourceLimit("peer routes"));
                }
                entry.insert(Route {
                    address: remote.socket.to_string(),
                    socket: Some(remote.socket),
                    learned_on: Some(self.id),
                });
            }
        }
        let data = is_data(&packet.body);
        #[cfg(any(test, feature = "qualification"))]
        self.network.note(
            if data {
                "received_data"
            } else {
                "received_control"
            },
            format!(
                "{}:{}",
                self.id,
                match &packet.body {
                    Body::Replication { operations, .. } =>
                        operations.first().map_or(0, |operation| operation.sequence),
                    Body::Copy { sequence, .. } | Body::CopyContext { sequence, .. } => *sequence,
                    _ => 0,
                }
            ),
        );
        let source = SourceTarget {
            route: Route {
                address: remote.socket.to_string(),
                socket: None,
                learned_on: Some(self.id),
            },
            actor: packet.from.actor,
            address: packet.from.address.clone(),
            state: Arc::downgrade(&self.state),
            peer: Arc::new(peer),
        };
        let charge = (packet_bytes(&packet) - std::mem::size_of::<Packet>()
            + std::mem::size_of::<Ingress>()
            + source.address.len()
            + source.route.address.len())
        .max(
            12 + native
                .headers()
                .records()
                .map(|header| 4 + header.value().len())
                .sum::<usize>()
                + native.body().len()
                + std::mem::size_of::<Packet>(),
        );
        let class = self.network.class(data);
        let delivered = class
            .reserve(charge)
            .and_then(|permit| class.deliver(packet, permit, source));
        match delivered {
            Err(Error::ResourceLimit(_)) if data => {
                tracing::debug!(
                    session = self.id,
                    "replication ingress dropped at data capacity"
                );
                #[cfg(any(test, feature = "qualification"))]
                self.network.note("incoming_drop", "capacity".into());
                Ok(())
            }
            result => result,
        }
    }
}

impl Service<Request<furpc::Body>> for IngressServiceImpl {
    type Response = IngressResponse;
    type Error = Error;
    type Future = std::future::Ready<Result<IngressResponse>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<furpc::Body>) -> Self::Future {
        let result = self.receive(request);
        if let Err(error) = &result {
            self.state.terminate(error.clone());
        }
        std::future::ready(result.map(|()| IngressResponse::NoReply))
    }
}

#[derive(Default)]
struct IoTracker {
    owned: AtomicUsize,
    changed: Notify,
}

impl IoTracker {
    fn lease(self: &Arc<Self>) -> IoLease {
        self.owned.fetch_add(1, Ordering::AcqRel);
        IoLease(self.clone())
    }
    async fn joined(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.owned.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct IoLease(Arc<IoTracker>);
impl Drop for IoLease {
    fn drop(&mut self) {
        self.0.owned.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

#[cfg(any(test, feature = "qualification"))]
struct Capture {
    bytes: Vec<u8>,
    size: usize,
    overflow: bool,
}

#[derive(Default)]
struct Observation {
    #[cfg(any(test, feature = "qualification"))]
    capture: Mutex<Option<Capture>>,
}

struct OwnedIo {
    socket: TcpStream,
    state: Arc<SessionState<Error>>,
    _lease: IoLease,
    #[cfg(any(test, feature = "qualification"))]
    observation: Arc<Observation>,
}

impl OwnedIo {
    fn new(
        socket: TcpStream,
        state: Arc<SessionState<Error>>,
        lease: IoLease,
        observation: Arc<Observation>,
    ) -> Self {
        #[cfg(not(any(test, feature = "qualification")))]
        let _ = observation;
        Self {
            socket,
            state,
            _lease: lease,
            #[cfg(any(test, feature = "qualification"))]
            observation,
        }
    }
}

impl Drop for OwnedIo {
    fn drop(&mut self) {
        self.state.terminate(Error::Closed);
    }
}

impl AsyncRead for OwnedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let remaining = buffer.remaining();
        let result = Pin::new(&mut self.socket).poll_read(cx, buffer);
        match &result {
            Poll::Ready(Ok(())) if remaining != 0 && buffer.filled().len() == before => {
                self.state.terminate(Error::Closed)
            }
            Poll::Ready(Err(error)) => {
                let cause = error
                    .raw_os_error()
                    .map(std::io::Error::from_raw_os_error)
                    .unwrap_or_else(|| std::io::Error::new(error.kind(), error.to_string()));
                self.state.terminate(Error::Io(Arc::new(cause)));
            }
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
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, bytes);
        #[cfg(any(test, feature = "qualification"))]
        if let Poll::Ready(Ok(count)) = result {
            if let Some(capture) = self.observation.capture.lock().unwrap().as_mut() {
                if count > capture.size - capture.bytes.len() {
                    capture.overflow = true;
                } else {
                    capture.bytes.extend_from_slice(&bytes[..count]);
                }
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

struct Connector {
    address: SocketAddr,
    state: Arc<SessionState<Error>>,
    tracker: Arc<IoTracker>,
    observation: Arc<Observation>,
}

impl<Q: Send + 'static> Service<Q> for Connector {
    type Response = Connected<OwnedIo>;
    type Error = std::io::Error;
    type Future = BoxFuture<std::io::Result<Self::Response>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Q) -> Self::Future {
        let address = self.address;
        let state = self.state.clone();
        let tracker = self.tracker.clone();
        let observation = self.observation.clone();
        Box::pin(async move {
            let lease = tracker.lease();
            let socket = TcpStream::connect(address).await?;
            socket.set_nodelay(true)?;
            let info = IoInfo {
                local_addr: Some(socket.local_addr()?),
                peer_addr: Some(socket.peer_addr()?),
            };
            Ok(Connected::new(
                OwnedIo::new(socket, state, lease, observation),
                info,
            ))
        })
    }
}

struct OneSocket(Option<Accepted<OwnedIo>>);
impl Accept for OneSocket {
    type Io = OwnedIo;
    type Error = Error;
    async fn accept(&mut self) -> Result<Option<Accepted<OwnedIo>>> {
        Ok(self.0.take())
    }
}

#[derive(Clone)]
struct PeerReady(Arc<Mutex<Option<oneshot::Sender<Peer>>>>);
impl Service<Peer> for PeerReady {
    type Response = ();
    type Error = Error;
    type Future = std::future::Ready<Result<()>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, peer: Peer) -> Self::Future {
        std::future::ready(Remote::new(&peer.info()).and_then(|_| {
            self.0
                .lock()
                .unwrap()
                .take()
                .ok_or(Error::Protocol("second replication generation"))?
                .send(peer)
                .map_err(|_| Error::Closed)
        }))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_session(
    socket: Socket,
    id: u64,
    network: Arc<Network>,
    ready: mpsc::Sender<Ready>,
    data: mpsc::Receiver<Outgoing>,
    control: mpsc::Receiver<Outgoing>,
    state: Arc<SessionState<Error>>,
    deadline: Instant,
) -> Result<()> {
    let tracker = Arc::new(IoTracker::default());
    let observation = Arc::new(Observation::default());
    let scope = async {
        let ingress = IngressService::new(
            IngressServiceImpl {
                id,
                network: network.clone(),
                state: state.clone(),
            },
            vec![ServiceBinding::raw(
                IngressOptions::one_way(
                    inbound,
                    IngressLimits {
                        max_body_bytes: MAX_FRAME_SIZE,
                    },
                )
                .with_send_policy(SendPolicy::new(outbound)),
            )
            .map_err(transport_error)?],
        )
        .map_err(transport_error)?;
        match socket {
            Socket::Connect(address) => {
                let endpoint = furpc::Endpoint::from_shared(format!("tcp://{address}"))
                    .map_err(transport_error)?
                    .connection_options(ConnectionOptions::raw_only())
                    .listen_address(network.local.address.clone())
                    .map_err(transport_error)?
                    .instance(network.local.instance)
                    .limits(limits(&network.settings))
                    .handler_limits(handlers(&network.settings))
                    .connect_timeout(network.settings.callback_timeout)
                    .service(ingress)
                    .map_err(transport_error)?;
                let channel = timeout_at(
                    deadline,
                    endpoint.connect_with_connector(Connector {
                        address,
                        state: state.clone(),
                        tracker: tracker.clone(),
                        observation: observation.clone(),
                    }),
                )
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(transport_error)?;
                let peer = channel.peer(deadline).await.map_err(transport_error)?;
                let _owner = announce(id, &network, &ready, &peer)?;
                write(peer, data, control, &state, &observation, &network, id).await
            }
            Socket::Accepted(socket) => {
                socket.set_nodelay(true)?;
                let info = IoInfo {
                    local_addr: Some(socket.local_addr()?),
                    peer_addr: Some(socket.peer_addr()?),
                };
                let io = OwnedIo::new(socket, state.clone(), tracker.lease(), observation.clone());
                let (published, peer) = oneshot::channel();
                let server = Server::builder()
                    .listen_address(network.local.address.clone())
                    .map_err(transport_error)?
                    .instance(network.local.instance)
                    .limits(limits(&network.settings))
                    .handler_limits(handlers(&network.settings))
                    .handshake_timeout(network.settings.callback_timeout)
                    .peer_ready_timeout(network.settings.callback_timeout)
                    .drain_timeout(network.settings.callback_timeout)
                    .service(ingress)
                    .map_err(transport_error)?
                    .on_peer_ready(
                        SendPolicy::new(outbound),
                        PeerReady(Arc::new(Mutex::new(Some(published)))),
                    )
                    .map_err(transport_error)?
                    .build()
                    .map_err(transport_error)?;
                let serving = server.serve_with_shutdown(
                    OneSocket(Some(Accepted::new(io, info))),
                    ConnectionOptions::raw_only(),
                    pending(),
                );
                tokio::pin!(serving);
                let peer = tokio::select! {
                    biased;
                    result = &mut serving => return result.map_err(transport_error).and(Err(Error::Closed)),
                    result = timeout_at(deadline, peer) => result.map_err(|_| Error::Timeout)?.map_err(|_| Error::Closed)?,
                };
                let _owner = announce(id, &network, &ready, &peer)?;
                tokio::select! {
                    biased;
                    result = &mut serving => result.map_err(transport_error).and(Err(Error::Closed)),
                    result = write(peer, data, control, &state, &observation, &network, id) => result,
                }
            }
        }
    };
    let result = tokio::select! {
        biased;
        _ = state.stopped() => Err(state.error().unwrap_or(Error::Closed)),
        result = scope => result,
    };
    state.terminate(result.as_ref().err().cloned().unwrap_or(Error::Closed));
    tracker.joined().await;
    if let Err(error) = &result {
        tracing::debug!(session = id, %error, "replication connection retired");
        #[cfg(any(test, feature = "qualification"))]
        network.note("connection_error", format!("{id}: {error:?}"));
    }
    #[cfg(any(test, feature = "qualification"))]
    network.note("session_end", format!("{id}: {result:?}"));
    result
}

fn announce(
    id: u64,
    network: &Arc<Network>,
    ready: &mpsc::Sender<Ready>,
    peer: &Peer,
) -> Result<AddressOwner> {
    let remote = Remote::new(&peer.info())?;
    let owner = network.own_socket_address(&remote.socket.to_string());
    ready
        .try_send(Ready {
            id,
            address: remote.socket.to_string(),
        })
        .map_err(|_| Error::ResourceLimit("session announcements"))?;
    Ok(owner)
}

#[allow(clippy::too_many_arguments)]
async fn write(
    peer: Peer,
    mut data: mpsc::Receiver<Outgoing>,
    mut control: mpsc::Receiver<Outgoing>,
    state: &SessionState<Error>,
    observation: &Observation,
    network: &Network,
    id: u64,
) -> Result<()> {
    let mut pending = FuturesUnordered::new();
    let capacity = network.settings.queue_operations + network.settings.control_messages;
    loop {
        let outgoing = tokio::select! {
            biased;
            _ = state.stopped() => return Err(state.error().unwrap_or(Error::Closed)),
            Some(result) = pending.next(), if !pending.is_empty() => {
                result?;
                continue;
            }
            Some(outgoing) = control.recv(), if pending.len() < capacity => outgoing,
            Some(outgoing) = data.recv(), if pending.len() < capacity => outgoing,
            else => return Ok(()),
        };
        if Instant::now() >= outgoing.deadline {
            continue;
        }
        #[cfg(any(test, feature = "qualification"))]
        if outgoing
            .completion
            .as_ref()
            .is_some_and(|(_, sender)| sender.is_closed())
        {
            continue;
        }
        #[cfg(any(test, feature = "qualification"))]
        if outgoing.completion.is_some() {
            while let Some(result) = pending.next().await {
                result?;
            }
            send(&peer, outgoing, observation, network, id).await?;
            continue;
        }
        pending.push(send(&peer, outgoing, observation, network, id));
    }
}

async fn send(
    peer: &Peer,
    outgoing: Outgoing,
    observation: &Observation,
    network: &Network,
    id: u64,
) -> Result<()> {
    #[cfg(not(any(test, feature = "qualification")))]
    let _ = (observation, network, id);
    let target = outgoing.peer.as_deref().unwrap_or(peer);
    #[cfg(any(test, feature = "qualification"))]
    if outgoing.completion.is_some() {
        let size = 12
            + outgoing
                .message
                .headers()
                .records()
                .map(|header| 4 + header.value().len())
                .sum::<usize>()
            + outgoing.message.body().len();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| Error::ResourceLimit("qualification write capture"))?;
        *observation.capture.lock().unwrap() = Some(Capture {
            bytes,
            size,
            overflow: false,
        });
    }
    target
        .send(outgoing.message, outgoing.deadline)
        .await
        .map_err(transport_error)?;
    #[cfg(any(test, feature = "qualification"))]
    {
        network.note(
            if outgoing.data {
                "written_data"
            } else {
                "written_control"
            },
            id.to_string(),
        );
        if let Some((message_id, sender)) = outgoing.completion {
            let capture = observation
                .capture
                .lock()
                .unwrap()
                .take()
                .ok_or(Error::Protocol("missing qualification write capture"))?;
            if capture.overflow || capture.bytes.len() != capture.size {
                return Err(Error::Protocol("qualification write capture extent"));
            }
            let info = target.info();
            let _ = sender.send(WriteReceipt {
                session: id,
                local: info
                    .io
                    .local_addr
                    .ok_or(Error::Protocol("missing actual local socket"))?,
                remote: info
                    .io
                    .peer_addr
                    .ok_or(Error::Protocol("missing actual peer socket"))?,
                remote_listener: String::from_utf16(&info.listen_address)
                    .map_err(|_| Error::Protocol("invalid listener UTF-16"))?,
                message_id,
                frame: capture.bytes,
                _bytes: outgoing._bytes,
                _slot: outgoing._slot,
            });
        }
    }
    Ok(())
}
