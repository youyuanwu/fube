//! Public furpc-backed model peers and test-only TCP observation.
//! These are offline test adapters, not native interoperability evidence.
use furpc::{
    header, Accept, Accepted, ActorId, Body, Error, Headers, IngressLimits, IngressOptions,
    IngressResponse, IngressService, IoInfo, Message, MessageId, Peer, Request, RequestPolicy,
    Result, SendPolicy, Server, ServiceBinding,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Notify},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use tower_service::Service;

pub mod relay;

pub fn message(actor: i64, action: &str, body: Vec<u8>) -> Result<Message> {
    Message::builder()
        .append_header(header::actor(ActorId::new(actor)?)?)
        .append_header(header::action(action)?)
        .body(Body::from_bytes(body)?)
        .build()
}

#[derive(Debug)]
pub struct Metadata {
    pub actor: i64,
    pub action: String,
    pub id: MessageId,
    pub relates_to: MessageId,
}

pub fn metadata(headers: &Headers) -> Result<Metadata> {
    let common = headers.effective();
    Ok(Metadata {
        actor: common.actor.get(),
        action: String::from_utf16(common.action())
            .map_err(|_| Error::Protocol("model action is not valid UTF-16"))?,
        id: common.message_id,
        relates_to: common.relates_to,
    })
}

/// Materialized application data. Only real ingress requests carry a response
/// sender; furpc's runner retains correlation and original-generation authority.
#[derive(Debug, Clone)]
pub struct Received {
    pub headers: Headers,
    pub body: Vec<u8>,
    pub request_action: Option<String>,
    response: Option<Arc<Mutex<Option<oneshot::Sender<Message>>>>>,
}
impl Received {
    pub fn metadata(&self) -> Result<Metadata> {
        metadata(&self.headers)
    }
    pub fn respond(&self, message: Message) -> Result<()> {
        self.response
            .as_ref()
            .ok_or(Error::InvalidInput("model message is not a request"))?
            .lock()
            .unwrap()
            .take()
            .ok_or(Error::InvalidInput("model request already answered"))?
            .send(message)
            .map_err(|_| Error::Closed)
    }
}

#[derive(Clone)]
struct Function<F>(F);
impl<F, Fut, Q, R> Service<Q> for Function<F>
where
    F: FnMut(Q) -> Fut,
    Fut: Future<Output = Result<R>>,
{
    type Response = R;
    type Error = Error;
    type Future = Fut;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Q) -> Fut {
        (self.0)(request)
    }
}

/// One accepted generation, with all model requests and serving tasks owned by
/// this handle. No independent socket reader or reconnecting client is added.
pub struct ModelPeer {
    peer: Peer,
    received: mpsc::Receiver<Result<Received>>,
    outgoing: mpsc::Sender<Result<Received>>,
    _driver: Driver,
    requests: JoinSet<()>,
    pub reads: ReadGate,
    last_read: usize,
}
impl ModelPeer {
    pub async fn accept(listener: TcpListener) -> Result<Self> {
        let reads = ReadGate::default();
        let (outgoing, received) = mpsc::channel(64);
        let handler = Function({
            let outgoing = outgoing.clone();
            move |request: Request<Body>| {
                let outgoing = outgoing.clone();
                async move {
                    let expects_reply = request.headers().effective().expects_reply;
                    let (response, wait) = oneshot::channel();
                    let (parts, body) = request.into_parts();
                    let observed = Received {
                        headers: parts.headers,
                        body: collect(&body)?,
                        request_action: None,
                        response: expects_reply.then(|| Arc::new(Mutex::new(Some(response)))),
                    };
                    outgoing
                        .try_send(Ok(observed))
                        .map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                Error::ResourceLimit("model ingress")
                            }
                            mpsc::error::TrySendError::Closed(_) => Error::Closed,
                        })?;
                    if expects_reply {
                        Ok(IngressResponse::Reply(
                            wait.await.map_err(|_| Error::Closed)?,
                        ))
                    } else {
                        Ok(IngressResponse::NoReply)
                    }
                }
            }
        });
        let allow = |_: &Headers, _: &furpc::Source| Ok(());
        let ingress = IngressService::new(
            handler,
            vec![
                ServiceBinding::actor(
                    ActorId::new(12)?,
                    IngressOptions::one_way(allow, IngressLimits::default()),
                )?,
                ServiceBinding::actor(
                    ActorId::new(7)?,
                    IngressOptions::requests(
                        RequestPolicy::new(allow, allow),
                        IngressLimits::default(),
                    ),
                )?,
                ServiceBinding::actor(
                    ActorId::new(9)?,
                    IngressOptions::requests(
                        RequestPolicy::new(allow, allow),
                        IngressLimits::default(),
                    ),
                )?,
                ServiceBinding::actor(
                    ActorId::new(15)?,
                    IngressOptions::requests(
                        RequestPolicy::new(allow, allow),
                        IngressLimits::default(),
                    ),
                )?,
            ],
        )?;
        let (ready, wait) = oneshot::channel();
        let ready = Arc::new(Mutex::new(Some(ready)));
        let ready_reads = reads.clone();
        let server = Server::builder()
            .service(ingress)?
            .on_peer_ready(
                SendPolicy::new(|_, _, _| Ok(())),
                Function(move |peer: Peer| {
                    let ready = ready.lock().unwrap().take();
                    let handshake_bytes = ready_reads.bytes_read();
                    async move {
                        ready
                            .ok_or(Error::InvalidInput("model accepted a second peer"))?
                            .send((peer, handshake_bytes))
                            .map_err(|_| Error::Closed)
                    }
                }),
            )?
            .build()?;
        let acceptor = OneAcceptor {
            listener: Some(listener),
            reads: reads.clone(),
        };
        let ended = outgoing.clone();
        let ended_reads = reads.clone();
        let driver = Driver(tokio::spawn(async move {
            let result = server
                .serve_with_shutdown(
                    acceptor,
                    furpc::ConnectionOptions::managed(),
                    std::future::pending(),
                )
                .await;
            let _ = ended
                .send(Err(result.clone().err().unwrap_or(Error::Closed)))
                .await;
            ended_reads.inner.activity.notify_one();
            result
        }));
        let (peer, last_read) = wait.await.map_err(|_| Error::Closed)?;
        Ok(Self {
            peer,
            received,
            outgoing,
            _driver: driver,
            requests: JoinSet::new(),
            reads,
            last_read,
        })
    }
    pub async fn send(&self, message: Message) -> Result<()> {
        self.peer.send(message, deadline()).await?;
        Ok(())
    }
    /// Starts a real managed request without holding up receipt of other
    /// traffic. Its eventual response retains the observed native correlation.
    pub fn request(&mut self, message: Message) -> Result<()> {
        self.reap_requests();
        let action = metadata(message.headers())?.action;
        let mut request = Request::new(message.body().clone());
        *request.headers_mut() = message.headers().clone();
        let operation = self.peer.request(request, deadline());
        let outgoing = self.outgoing.clone();
        let reads = self.reads.clone();
        self.requests.spawn(async move {
            let result = operation.await.and_then(|response| {
                let (parts, body) = response.into_parts();
                Ok(Received {
                    headers: parts.headers,
                    body: collect(&body)?,
                    request_action: Some(action),
                    response: None,
                })
            });
            let _ = outgoing.send(result).await;
            reads.inner.activity.notify_one();
        });
        Ok(())
    }
    pub async fn receive(&mut self) -> Result<Received> {
        let message = self.received.recv().await.ok_or(Error::Closed)??;
        self.reap_requests();
        // Account for the consumed frame, not all socket reads: the driver may
        // already have read part of the following frame before this receive.
        let header_bytes: usize = message
            .headers
            .raw_records()
            .map(|record| 4 + record.value().len())
            .sum();
        self.last_read += 12 + header_bytes + message.body.len();
        Ok(message)
    }
    fn reap_requests(&mut self) {
        while let Some(result) = self.requests.try_join_next() {
            result.expect("model request task panicked");
        }
    }
    /// Detects even an incomplete frame, not just completed service ingress.
    pub async fn activity(&mut self) {
        loop {
            let notified = self.reads.inner.activity.notified();
            if self.reads.bytes_read() != self.last_read || !self.received.is_empty() {
                return;
            }
            notified.await;
        }
    }
}
impl Drop for ModelPeer {
    fn drop(&mut self) {
        self.reads.resume();
        self.requests.abort_all();
    }
}

struct Driver(JoinHandle<Result<()>>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn collect(body: &Body) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(body.len())
        .map_err(|_| Error::ResourceLimit("model body materialization"))?;
    for segment in body.segments() {
        bytes.extend_from_slice(segment);
    }
    Ok(bytes)
}

struct OneAcceptor {
    listener: Option<TcpListener>,
    reads: ReadGate,
}
impl Accept for OneAcceptor {
    type Io = ObservedIo;
    type Error = std::io::Error;
    async fn accept(&mut self) -> std::io::Result<Option<Accepted<ObservedIo>>> {
        let Some(listener) = &self.listener else {
            return Ok(None);
        };
        let (io, _) = listener.accept().await?;
        io.set_nodelay(true)?;
        let info = IoInfo {
            local_addr: Some(io.local_addr()?),
            peer_addr: Some(io.peer_addr()?),
        };
        self.listener.take();
        Ok(Some(Accepted::new(
            ObservedIo {
                io,
                reads: self.reads.clone(),
            },
            info,
        )))
    }
}

#[derive(Default)]
struct ReadState {
    remaining: Option<usize>,
    bytes: usize,
    waker: Option<Waker>,
}
#[derive(Default)]
struct ReadControl {
    state: Mutex<ReadState>,
    blocked: Notify,
    activity: Notify,
}
#[derive(Clone, Default)]
pub struct ReadGate {
    inner: Arc<ReadControl>,
}
impl ReadGate {
    pub fn hold_after(&self, bytes: usize) {
        self.set_budget(Some(bytes));
    }
    pub fn resume(&self) {
        self.set_budget(None);
    }
    fn set_budget(&self, remaining: Option<usize>) {
        let mut state = self.inner.state.lock().unwrap();
        state.remaining = remaining;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
    pub async fn blocked(&self) {
        self.inner.blocked.notified().await;
    }
    fn bytes_read(&self) -> usize {
        self.inner.state.lock().unwrap().bytes
    }
}
struct ObservedIo {
    io: TcpStream,
    reads: ReadGate,
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let reads = self.reads.clone();
        let mut state = reads.inner.state.lock().unwrap();
        let limit = state
            .remaining
            .unwrap_or(buffer.remaining())
            .min(buffer.remaining());
        if limit == 0 && buffer.remaining() != 0 {
            state.waker = Some(cx.waker().clone());
            reads.inner.blocked.notify_one();
            return Poll::Pending;
        }
        let mut bounded = ReadBuf::new(buffer.initialize_unfilled_to(limit));
        let result = Pin::new(&mut self.io).poll_read(cx, &mut bounded);
        if let Poll::Ready(Ok(())) = &result {
            let count = bounded.filled().len();
            buffer.advance(count);
            state.bytes += count;
            if let Some(remaining) = &mut state.remaining {
                *remaining -= count;
            }
            if count != 0 {
                reads.inner.activity.notify_one();
            }
        }
        result
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
