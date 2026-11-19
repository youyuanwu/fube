use std::{
    cell::Cell,
    collections::VecDeque,
    future::{pending, Future},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use furpc::{
    header, Accept, Accepted, Body, BoxError, CancelOutcome, Connected, ConnectionOptions,
    Endpoint, Error, HandlerLimits, Headers, IngressLimits, IngressOptions, IngressResponse,
    IngressService, IoInfo, Message, Peer, Request, SendKind, SendPolicy, Server, ServiceBinding,
    Source, StreamAcceptor, TcpAcceptor, TransportIo, TransportLimits,
};
use futures_util::{future::join_all, stream, FutureExt};
use http::Uri;
use tokio::{
    io::DuplexStream,
    sync::{mpsc, oneshot, Notify, Semaphore},
    time::{timeout, Instant},
};
use tower_service::Service;

#[derive(Clone)]
struct Function<F>(F);
impl<F, Fut, Q, R, E> Service<Q> for Function<F>
where
    F: FnMut(Q) -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    type Response = R;
    type Error = E;
    type Future = Fut;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), E>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Q) -> Fut {
        (self.0)(request)
    }
}

fn allow(_: &Headers, _: &Source) -> furpc::Result<()> {
    Ok(())
}
fn action(headers: &Headers) -> String {
    String::from_utf16(headers.effective().action()).unwrap()
}
fn control_sends(kind: SendKind, headers: &Headers, _: &Source) -> furpc::Result<()> {
    if kind == SendKind::OneWay && matches!(action(headers).as_str(), "bootstrap" | "ack") {
        Ok(())
    } else {
        Err(Error::InvalidInput("restricted send"))
    }
}
fn packet(name: &str, body: &[u8]) -> Message {
    Message::builder()
        .append_header(header::action(name).unwrap())
        .body(Body::from_bytes(body.to_vec()).unwrap())
        .build()
        .unwrap()
}
fn ingress<S>(service: S, sends: bool) -> IngressService<S>
where
    S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    let options = IngressOptions::one_way(allow, IngressLimits::default());
    let options = if sends {
        options.with_send_policy(SendPolicy::new(control_sends))
    } else {
        options
    };
    IngressService::new(service, vec![ServiceBinding::raw(options).unwrap()]).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("test timed out")
}
fn raw(uri: impl Into<bytes::Bytes>) -> Endpoint {
    Endpoint::from_shared(uri)
        .unwrap()
        .connection_options(ConnectionOptions::raw_only())
}

struct Connector {
    ios: VecDeque<Option<DuplexStream>>,
    calls: Arc<AtomicUsize>,
    uris: Arc<Mutex<Vec<Uri>>>,
    gate: Arc<Semaphore>,
    ready: Cell<bool>,
}
impl Connector {
    fn new(ios: Vec<DuplexStream>) -> Self {
        Self {
            ios: ios.into_iter().map(Some).collect(),
            calls: Arc::new(AtomicUsize::new(0)),
            uris: Arc::new(Mutex::new(Vec::new())),
            gate: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            ready: Cell::new(false),
        }
    }
}
impl Service<Uri> for Connector {
    type Response = Connected<DuplexStream>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready.set(true);
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        assert!(
            self.ready.replace(false),
            "call did not consume this instance's readiness"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.uris.lock().unwrap().push(uri);
        let io = self.ios.pop_front().flatten();
        let gate = self.gate.clone();
        Box::pin(async move {
            let permit = gate.acquire_owned().await.unwrap();
            permit.forget();
            io.map(|io| Connected::new(io, IoInfo::unknown()))
                .ok_or_else(|| std::io::Error::other("no fresh I/O"))
        })
    }
}

fn one<I: TransportIo>(
    io: I,
) -> StreamAcceptor<impl futures_util::Stream<Item = Result<Accepted<I>, std::io::Error>> + Send> {
    StreamAcceptor::new(stream::iter([Ok(Accepted::new(io, IoInfo::unknown()))]))
}

#[derive(Default)]
struct GateState {
    remaining: Option<usize>,
    waker: Option<std::task::Waker>,
}
#[derive(Default)]
struct WriteGate {
    state: Mutex<GateState>,
    blocked: Notify,
}
impl WriteGate {
    fn limit(&self, remaining: Option<usize>) {
        let mut state = self.state.lock().unwrap();
        state.remaining = remaining;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}
struct GuardedIo {
    io: DuplexStream,
    gate: Arc<WriteGate>,
    dropped: Option<oneshot::Sender<()>>,
}
impl Drop for GuardedIo {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}
impl tokio::io::AsyncRead for GuardedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}
impl tokio::io::AsyncWrite for GuardedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let gate = self.gate.clone();
        let mut state = gate.state.lock().unwrap();
        let count = state.remaining.unwrap_or(bytes.len()).min(bytes.len());
        if count == 0 {
            state.waker = Some(cx.waker().clone());
            gate.blocked.notify_one();
            return Poll::Pending;
        }
        let result = Pin::new(&mut self.io).poll_write(cx, &bytes[..count]);
        if let Poll::Ready(Ok(count)) = result {
            if let Some(remaining) = &mut state.remaining {
                *remaining -= count;
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[test]
fn endpoint_configuration_is_pure_and_preserves_complete_custom_uris() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Endpoint>();
    send_sync::<furpc::Channel>();
    for uri in [
        "tcp://localhost:1234",
        "tcp://[::1]:1234",
        "memory://echo/path?target=one",
    ] {
        let endpoint = raw(uri);
        assert_eq!(endpoint.uri(), &uri.parse::<Uri>().unwrap());
    }
    for uri in [
        "relative",
        "/path",
        "tcp://user@host:1",
        "tcp://host:1/#fragment",
    ] {
        assert!(Endpoint::from_shared(uri).is_err());
    }
    assert!(Endpoint::try_from("[::1]:0".parse::<std::net::SocketAddr>().unwrap()).is_err());
    let source = Request::new(Body::empty());
    assert!(source.context().is_none());
}

#[tokio::test]
async fn managed_mode_and_invalid_configuration_fail_before_connector_polling() {
    for endpoint in [
        Endpoint::from_shared("memory://echo").unwrap(),
        raw("memory://echo").connect_timeout(Duration::ZERO),
        raw("memory://echo").limits(TransportLimits {
            max_pending_sends: 0,
            ..Default::default()
        }),
    ] {
        let connector = Connector::new(vec![]);
        let calls = connector.calls.clone();
        assert!(endpoint.connect_with_connector(connector).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    for uri in [
        "https://host:1234",
        "tcp://host",
        "tcp://host:0",
        "tcp://host:1/path",
        "tcp://host:1/?",
    ] {
        assert!(raw(uri).connect().await.is_err());
    }
}

#[tokio::test]
async fn tcp_bootstrap_reverse_ingress_and_source_bound_ack_share_one_driver() {
    let (arrivals, mut received) = mpsc::channel(4);
    let (peers, mut accepted_peers) = mpsc::channel(1);
    let incoming = ingress(
        Function(move |request: Request<Body>| {
            assert!(request.headers().first_actor().is_none());
            let context = request.context().unwrap();
            assert!(context.source().info().io.peer_addr.is_some());
            let peer = context.peer().unwrap().clone();
            let generation = context.source().generation();
            assert_eq!(generation, peer.info().generation);
            async move {
                peer.send(packet("ack", b"accepted"), deadline()).await?;
                Ok::<_, Error>(IngressResponse::NoReply)
            }
        }),
        true,
    );
    let hook = Function(move |peer: Peer| {
        let peers = peers.clone();
        async move {
            peers.send(peer.clone()).await.unwrap();
            peer.send(packet("bootstrap", b"hello"), deadline()).await?;
            Ok::<_, Error>(())
        }
    });
    let server = Server::builder()
        .service(incoming)
        .unwrap()
        .on_peer_ready(SendPolicy::new(control_sends), hook)
        .unwrap()
        .build()
        .unwrap();
    let acceptor = TcpAcceptor::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = acceptor.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let serving =
        tokio::spawn(
            server.serve_with_shutdown(acceptor, ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
        );
    let reverse = ingress(
        Function(move |request: Request<Body>| {
            let arrivals = arrivals.clone();
            assert!(request.context().unwrap().peer().is_none());
            async move {
                arrivals.send(action(request.headers())).await.unwrap();
                Ok::<_, Error>(IngressResponse::NoReply)
            }
        }),
        false,
    );
    let endpoint = Endpoint::try_from(address)
        .unwrap()
        .connection_options(ConnectionOptions::raw_only())
        .service(reverse)
        .unwrap();
    let channel = bounded(endpoint.connect()).await.unwrap();
    assert_eq!(bounded(received.recv()).await.unwrap(), "bootstrap");
    let accepted = bounded(accepted_peers.recv()).await.unwrap();
    assert!(accepted
        .send(packet("forbidden", b""), deadline())
        .await
        .is_err());
    let peer = channel.peer(deadline()).await.unwrap();
    let operation = peer.send(packet("data", b"payload"), deadline());
    let attempt = operation.attempt();
    assert_eq!(
        attempt.disposition(),
        furpc::AttemptDisposition::Uncommitted
    );
    operation.await.unwrap();
    assert_eq!(
        attempt.disposition(),
        furpc::AttemptDisposition::LocallyWritten
    );
    assert_eq!(bounded(received.recv()).await.unwrap(), "ack");
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    assert!(matches!(
        accepted.send(packet("ack", b""), deadline()).await,
        Err(Error::Closed)
    ));
    drop(channel);
}

#[tokio::test]
async fn finite_custom_io_exhaustion_waits_for_connection_and_preserves_original_uri() {
    let (client, accepted) = tokio::io::duplex(7);
    let connector = Connector::new(vec![client]);
    let uris = connector.uris.clone();
    let server = Server::builder().build().unwrap();
    let serving = tokio::spawn(server.serve_with_shutdown(
        one(accepted),
        ConnectionOptions::raw_only(),
        pending(),
    ));
    let channel = bounded(raw("memory://logical/path?x=1").connect_with_connector(connector))
        .await
        .unwrap();
    assert!(
        !serving.is_finished(),
        "exhaustion incorrectly started shutdown"
    );
    assert_eq!(
        uris.lock().unwrap()[0],
        "memory://logical/path?x=1".parse::<Uri>().unwrap()
    );
    let peer = channel.peer(deadline()).await.unwrap();
    let operation = peer.send(packet("not sent", b""), deadline());
    let attempt = operation.attempt();
    assert_eq!(attempt.cancel_uncommitted(), CancelOutcome::Retracted);
    assert!(matches!(operation.await, Err(Error::Cancelled)));
    drop(channel);
    bounded(serving).await.unwrap().unwrap();
    assert!(matches!(
        peer.send(packet("late", b""), deadline()).await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn reconnect_is_coalesced_reuses_send_only_connector_and_never_retargets_old_peers() {
    let (first, first_accepted) = tokio::io::duplex(32);
    let (second, second_accepted) = tokio::io::duplex(32);
    let mut connector = Connector::new(vec![first, second]);
    connector.ios.insert(1, None);
    let calls = connector.calls.clone();
    let uris = connector.uris.clone();
    let gate = connector.gate.clone();
    let first_server = tokio::spawn(Server::builder().build().unwrap().serve_with_shutdown(
        one(first_accepted),
        ConnectionOptions::raw_only(),
        pending(),
    ));
    let second_server = tokio::spawn(
        Server::builder()
            .on_peer_ready(
                SendPolicy::new(control_sends),
                Function(|peer: Peer| async move {
                    peer.send(packet("bootstrap", b"new generation"), deadline())
                        .await?;
                    Ok::<_, Error>(())
                }),
            )
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(
                one(second_accepted),
                ConnectionOptions::raw_only(),
                pending(),
            ),
    );
    let channel = bounded(raw("memory://original/path?key=1").connect_with_connector(connector))
        .await
        .unwrap();
    let old = channel.peer(deadline()).await.unwrap();
    first_server.abort();
    assert!(first_server.await.unwrap_err().is_cancelled());
    bounded(async {
        loop {
            if old.send(packet("retired", b""), deadline()).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        channel.peer(deadline()).await,
        Err(Error::Transport {
            phase: "connector",
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let installed = Arc::new(Notify::new());
    let service = ingress(
        Function({
            let installed = installed.clone();
            let not_sync = Cell::new(0);
            move |request: Request<Body>| {
                not_sync.set(not_sync.get() + 1);
                let original = request.context().unwrap().deadline();
                let (mut parts, body) = request.into_parts();
                parts
                    .headers
                    .append(header::action("edited metadata").unwrap())
                    .unwrap();
                parts.deadline = Some(Instant::now() + Duration::from_secs(60));
                let mapped = Request::from_parts(parts, body).map(|body| body.len());
                assert_eq!(mapped.context().unwrap().deadline(), original);
                let installed = installed.clone();
                async move {
                    installed.notify_one();
                    Ok::<_, Error>(IngressResponse::NoReply)
                }
            }
        }),
        false,
    );
    channel.register_service(service).unwrap();
    assert!(channel
        .register_service(ingress(
            Function(|_: Request<Body>| async { Ok::<_, Error>(IngressResponse::NoReply) }),
            false
        ))
        .is_err());
    gate.forget_permits(gate.available_permits());
    let short = channel
        .peer(Instant::now() + Duration::from_millis(10))
        .await;
    assert!(matches!(short, Err(Error::Timeout("peer acquisition"))));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let waiters = (0..8).map(|_| {
        let channel = channel.clone();
        async move { channel.peer(deadline()).await }
    });
    gate.add_permits(1);
    let peers = bounded(join_all(waiters)).await;
    let generation = peers[0].as_ref().unwrap().info().generation;
    assert_ne!(generation, old.info().generation);
    assert!(peers
        .iter()
        .all(|peer| peer.as_ref().unwrap().info().generation == generation));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(uris.lock().unwrap().len(), 3);
    assert!(uris
        .lock()
        .unwrap()
        .iter()
        .all(|uri| uri == "memory://original/path?key=1"));
    bounded(installed.notified()).await;
    assert!(old
        .send(packet("never replay", b""), deadline())
        .await
        .is_err());
    drop(channel);
    bounded(second_server).await.unwrap().unwrap();
}

#[tokio::test]
async fn graceful_shutdown_waits_for_admitted_handler_but_closes_new_native_sends() {
    let (client, accepted) = tokio::io::duplex(128);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = ingress(
        Function({
            let started = started.clone();
            let release = release.clone();
            move |request: Request<Body>| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    assert!(request.context().is_some());
                    started.notify_one();
                    release.notified().await;
                    Ok::<_, Error>(IngressResponse::NoReply)
                }
            }
        }),
        false,
    );
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let channel =
        bounded(raw("memory://drain").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    peer.send(packet("admitted", b""), deadline())
        .await
        .unwrap();
    bounded(started.notified()).await;
    stop.send(()).unwrap();
    bounded(async {
        loop {
            if peer.send(packet("closed", b""), deadline()).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!serving.is_finished());
    release.notify_one();
    bounded(serving).await.unwrap().unwrap();
    drop(channel);
}

#[tokio::test]
async fn shutdown_deadline_cancels_stalled_service_work() {
    let (client, accepted) = tokio::io::duplex(128);
    let started = Arc::new(Notify::new());
    let handler = ingress(
        Function({
            let started = started.clone();
            move |_: Request<Body>| {
                started.notify_one();
                pending::<Result<IngressResponse, Error>>()
            }
        }),
        false,
    );
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .drain_timeout(Duration::from_millis(20))
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let channel =
        bounded(raw("memory://timeout").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    channel
        .peer(deadline())
        .await
        .unwrap()
        .send(packet("block", b""), deadline())
        .await
        .unwrap();
    bounded(started.notified()).await;
    stop.send(()).unwrap();
    assert!(matches!(
        bounded(serving).await.unwrap(),
        Err(Error::Timeout("shutdown drain"))
    ));
    drop(channel);
}

#[tokio::test]
async fn initial_connect_drop_disposes_unpublished_io_and_configuration_is_inert() {
    let (client, remote) = tokio::io::duplex(32);
    let (dropped, drop_observed) = oneshot::channel();
    let called = Arc::new(Notify::new());
    let mut io = Some(GuardedIo {
        io: client,
        gate: Arc::new(WriteGate::default()),
        dropped: Some(dropped),
    });
    let calls = called.clone();
    let connector = Function(move |_: Uri| {
        calls.notify_one();
        std::future::ready(Ok::<_, Error>(Connected::new(
            io.take().unwrap(),
            IoInfo::unknown(),
        )))
    });
    let endpoint = raw("memory://cancel");
    let mut connecting = Box::pin(endpoint.connect_with_connector(connector));
    assert!(connecting.as_mut().now_or_never().is_none());
    bounded(called.notified()).await;
    drop(connecting);
    bounded(drop_observed).await.unwrap();
    drop(remote);

    let polls = Arc::new(AtomicUsize::new(0));
    let prototype = Function({
        let polls = polls.clone();
        let not_sync = Cell::new(0);
        move |_: Request<Body>| {
            not_sync.set(not_sync.get() + 1);
            polls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, Error>(IngressResponse::NoReply))
        }
    });
    let endpoint = raw("memory://inert")
        .service(ingress(prototype, false))
        .unwrap();
    let unpolled = endpoint.connect_with_connector(Connector::new(vec![]));
    drop(unpolled);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn handler_timeout_includes_body_acquisition_and_does_not_invoke_the_service() {
    let (client, accepted) = tokio::io::duplex(256);
    let gate = Arc::new(WriteGate::default());
    let (dropped, drop_observed) = oneshot::channel();
    let accepted = GuardedIo {
        io: accepted,
        gate: Arc::new(WriteGate::default()),
        dropped: Some(dropped),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = ingress(
        Function({
            let calls = calls.clone();
            move |_: Request<Body>| {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok::<_, Error>(IngressResponse::NoReply))
            }
        }),
        false,
    );
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .handler_limits(HandlerLimits {
                timeout: Duration::from_millis(20),
                ..Default::default()
            })
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), pending()),
    );
    let mut client = Some(GuardedIo {
        io: client,
        gate: gate.clone(),
        dropped: None,
    });
    let connector = Function(move |_: Uri| {
        std::future::ready(Ok::<_, Error>(Connected::new(
            client.take().unwrap(),
            IoInfo::unknown(),
        )))
    });
    let channel = bounded(raw("memory://body-timeout").connect_with_connector(connector))
        .await
        .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    let message = packet("body is withheld", &[1; 100]);
    let prefix = 12
        + message
            .headers()
            .raw_records()
            .map(|record| 4 + record.value().len())
            .sum::<usize>();
    gate.limit(Some(prefix));
    let mut send = Box::pin(peer.send(message, deadline()));
    assert!(send.as_mut().now_or_never().is_none());
    bounded(gate.blocked.notified()).await;
    bounded(drop_observed).await.unwrap();
    bounded(serving).await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(send);
    drop(channel);
}

#[tokio::test]
async fn public_attempt_drop_after_commit_preserves_the_full_frame_before_later_output() {
    let (client, accepted) = tokio::io::duplex(64);
    let gate = Arc::new(WriteGate::default());
    let (delivered, mut messages) = mpsc::channel(2);
    let handler = ingress(
        Function(move |request: Request<Body>| {
            let delivered = delivered.clone();
            async move {
                let body = request
                    .body()
                    .segments()
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                delivered
                    .send((action(request.headers()), body))
                    .await
                    .unwrap();
                Ok::<_, Error>(IngressResponse::NoReply)
            }
        }),
        false,
    );
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let mut client = Some(GuardedIo {
        io: client,
        gate: gate.clone(),
        dropped: None,
    });
    let connector = Function(move |_: Uri| {
        std::future::ready(Ok::<_, Error>(Connected::new(
            client.take().unwrap(),
            IoInfo::unknown(),
        )))
    });
    let channel = bounded(raw("memory://committed").connect_with_connector(connector))
        .await
        .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    gate.limit(Some(1));
    let mut sending = Box::pin(peer.send(packet("first", &[1; 100]), deadline()));
    let attempt = sending.attempt();
    assert!(sending.as_mut().now_or_never().is_none());
    bounded(gate.blocked.notified()).await;
    assert_eq!(attempt.cancel_uncommitted(), CancelOutcome::Committed);
    drop(sending);
    gate.limit(None);
    peer.send(packet("second", &[2; 10]), deadline())
        .await
        .unwrap();
    assert_eq!(
        bounded(messages.recv()).await.unwrap(),
        ("first".into(), vec![1; 100])
    );
    assert_eq!(
        bounded(messages.recv()).await.unwrap(),
        ("second".into(), vec![2; 10])
    );
    assert_eq!(
        attempt.disposition(),
        furpc::AttemptDisposition::LocallyWritten
    );
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    drop(channel);
}

#[tokio::test]
async fn shared_handler_capacity_is_checked_before_body_and_released_after_completion() {
    let (one_client, one_accepted) = tokio::io::duplex(1);
    let (two_client, two_accepted) = tokio::io::duplex(1);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let marker = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = ingress(
        Function({
            let started = started.clone();
            let release = release.clone();
            let marker = marker.clone();
            let calls = calls.clone();
            move |request: Request<Body>| {
                calls.fetch_add(1, Ordering::SeqCst);
                let first = action(request.headers()) == "hold";
                let started = started.clone();
                let release = release.clone();
                let marker = marker.clone();
                async move {
                    if first {
                        started.notify_one();
                        release.notified().await;
                    } else {
                        marker.notify_one();
                    }
                    Ok::<_, Error>(IngressResponse::NoReply)
                }
            }
        }),
        false,
    );
    let acceptor = StreamAcceptor::new(stream::iter([
        Ok::<_, std::io::Error>(Accepted::new(one_accepted, IoInfo::unknown())),
        Ok(Accepted::new(two_accepted, IoInfo::unknown())),
    ]));
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .handler_limits(HandlerLimits {
                max_in_flight: 1,
                ..Default::default()
            })
            .build()
            .unwrap()
            .serve_with_shutdown(acceptor, ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let first =
        bounded(raw("memory://first").connect_with_connector(Connector::new(vec![one_client])))
            .await
            .unwrap();
    let second =
        bounded(raw("memory://second").connect_with_connector(Connector::new(vec![two_client])))
            .await
            .unwrap();
    first
        .peer(deadline())
        .await
        .unwrap()
        .send(packet("hold", &[1; 10]), deadline())
        .await
        .unwrap();
    bounded(started.notified()).await;
    let second_peer = second.peer(deadline()).await.unwrap();
    second_peer
        .send(packet("capacity rejected", &[2; 100]), deadline())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    tokio::task::yield_now().await;
    second_peer
        .send(packet("marker", &[3; 10]), deadline())
        .await
        .unwrap();
    bounded(marker.notified()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    drop((first, second));
}

struct CountedAccept {
    ios: VecDeque<DuplexStream>,
    polls: Arc<AtomicUsize>,
    dropped: Option<oneshot::Sender<()>>,
}
impl Drop for CountedAccept {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}
impl Accept for CountedAccept {
    type Io = DuplexStream;
    type Error = Error;
    async fn accept(&mut self) -> furpc::Result<Option<Accepted<DuplexStream>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.ios.pop_front() {
            Some(io) => Ok(Some(Accepted::new(io, IoInfo::unknown()))),
            None => pending().await,
        }
    }
}

#[tokio::test]
async fn server_reserves_connection_and_handshake_capacity_before_accepting() {
    for (connections, handshakes) in [(1, 2), (2, 1)] {
        let (_first_client, first) = tokio::io::duplex(1);
        let (_second_client, second) = tokio::io::duplex(1);
        let polls = Arc::new(AtomicUsize::new(0));
        let hooks = Arc::new(AtomicUsize::new(0));
        let (dropped, drop_observed) = oneshot::channel();
        let acceptor = CountedAccept {
            ios: [first, second].into(),
            polls: polls.clone(),
            dropped: Some(dropped),
        };
        let hook = Function({
            let hooks = hooks.clone();
            move |_: Peer| {
                hooks.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok::<_, Error>(()))
            }
        });
        let server = Server::builder()
            .limits(TransportLimits {
                max_connections: connections,
                max_handshakes: handshakes,
                ..Default::default()
            })
            .on_peer_ready(SendPolicy::new(control_sends), hook)
            .unwrap()
            .build()
            .unwrap();
        let (stop, stopped) = oneshot::channel();
        let serving = tokio::spawn(server.serve_with_shutdown(
            acceptor,
            ConnectionOptions::raw_only(),
            async {
                stopped.await.unwrap();
            },
        ));
        bounded(async {
            while polls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        stop.send(()).unwrap();
        bounded(drop_observed).await.unwrap();
        bounded(serving).await.unwrap().unwrap();
        assert_eq!(hooks.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn accept_and_connector_failures_preserve_the_original_io_source() {
    let endpoint = raw("memory://error");
    let connector = Function(|_: Uri| {
        std::future::ready(Err::<Connected<DuplexStream>, _>(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connector detail",
        )))
    });
    let error = match endpoint.connect_with_connector(connector).await {
        Ok(_) => panic!("connector failure became success"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::Transport {
            phase: "connector",
            ..
        }
    ));
    let source = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::ConnectionRefused);
    assert_eq!(source.to_string(), "connector detail");
    assert_eq!(error.clone(), error);

    let (remote, accepted) = tokio::io::duplex(1);
    let (dropped, drop_observed) = oneshot::channel();
    let io = GuardedIo {
        io: accepted,
        gate: Arc::new(WriteGate::default()),
        dropped: Some(dropped),
    };
    let incoming = StreamAcceptor::new(stream::iter([
        Ok(Accepted::new(io, IoInfo::unknown())),
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "accept detail",
        )),
    ]));
    let error = Server::builder()
        .build()
        .unwrap()
        .serve_with_shutdown(incoming, ConnectionOptions::raw_only(), pending())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Transport {
            phase: "accept",
            ..
        }
    ));
    let source = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::ConnectionAborted);
    bounded(drop_observed).await.unwrap();
    drop(remote);
}

struct Readiness {
    enabled: Arc<std::sync::atomic::AtomicBool>,
    polled: Arc<Notify>,
    called: Arc<Notify>,
    ready: Cell<bool>,
}
impl Clone for Readiness {
    fn clone(&self) -> Self {
        Self {
            enabled: self.enabled.clone(),
            polled: self.polled.clone(),
            called: self.called.clone(),
            ready: Cell::new(false),
        }
    }
}
impl Service<Request<Body>> for Readiness {
    type Response = IngressResponse;
    type Error = Error;
    type Future = std::future::Ready<furpc::Result<IngressResponse>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        self.polled.notify_one();
        if self.enabled.load(Ordering::SeqCst) {
            self.ready.set(true);
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn call(&mut self, _: Request<Body>) -> Self::Future {
        assert!(
            self.ready.replace(false),
            "ready service was cloned or replaced"
        );
        self.called.notify_one();
        std::future::ready(Ok(IngressResponse::NoReply))
    }
}

#[tokio::test]
async fn fail_fast_readiness_does_not_stall_the_driver_or_clone_ready_services() {
    let (client, accepted) = tokio::io::duplex(1);
    let service = Readiness {
        enabled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        polled: Arc::new(Notify::new()),
        called: Arc::new(Notify::new()),
        ready: Cell::new(false),
    };
    let state = service.clone();
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(ingress(service, false))
            .unwrap()
            .handler_limits(HandlerLimits {
                max_readiness_waiters: 0,
                ..Default::default()
            })
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let channel =
        bounded(raw("memory://readiness").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    peer.send(packet("not ready", &[1; 10]), deadline())
        .await
        .unwrap();
    bounded(state.polled.notified()).await;
    state.enabled.store(true, Ordering::SeqCst);
    peer.send(packet("ready", &[2; 10]), deadline())
        .await
        .unwrap();
    bounded(state.called.notified()).await;
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    drop(channel);
}

#[tokio::test]
async fn peer_ready_work_has_independent_handler_capacity_and_a_finite_timeout() {
    let (client, accepted) = tokio::io::duplex(64);
    let invoked = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let done = Arc::new(Notify::new());
    let handler = ingress(
        Function({
            let invoked = invoked.clone();
            move |_: Request<Body>| {
                invoked.notify_one();
                std::future::ready(Ok::<_, Error>(IngressResponse::NoReply))
            }
        }),
        false,
    );
    let hook = Function({
        let invoked = invoked.clone();
        let ready = ready.clone();
        let done = done.clone();
        move |_: Peer| {
            let invoked = invoked.clone();
            let done = done.clone();
            ready.notify_one();
            async move {
                invoked.notified().await;
                done.notify_one();
                Ok::<_, Error>(())
            }
        }
    });
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .handler_limits(HandlerLimits {
                max_in_flight: 1,
                ..Default::default()
            })
            .on_peer_ready(SendPolicy::new(control_sends), hook)
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let channel =
        bounded(raw("memory://hook").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    bounded(ready.notified()).await;
    channel
        .peer(deadline())
        .await
        .unwrap()
        .send(packet("release hook", b""), deadline())
        .await
        .unwrap();
    bounded(done.notified()).await;
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    drop(channel);

    let (client, accepted) = tokio::io::duplex(64);
    let (escaped, mut peers) = mpsc::channel(1);
    let serving = tokio::spawn(
        Server::builder()
            .peer_ready_timeout(Duration::from_millis(20))
            .on_peer_ready(
                SendPolicy::new(control_sends),
                Function(move |peer: Peer| {
                    let escaped = escaped.clone();
                    async move {
                        escaped.send(peer).await.unwrap();
                        pending::<furpc::Result<()>>().await
                    }
                }),
            )
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), pending()),
    );
    let channel =
        bounded(raw("memory://hook-timeout").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    bounded(serving).await.unwrap().unwrap();
    let peer = bounded(peers.recv()).await.unwrap();
    assert!(matches!(
        peer.send(packet("ack", b""), deadline()).await,
        Err(Error::Timeout("peer ready"))
    ));
    drop(channel);
}

static POLICY_CHECKS: AtomicUsize = AtomicUsize::new(0);
fn reject_at_commit(_: SendKind, _: &Headers, _: &Source) -> furpc::Result<()> {
    if POLICY_CHECKS.fetch_add(1, Ordering::SeqCst) == 0 {
        Ok(())
    } else {
        Err(Error::InvalidInput("policy changed before commitment"))
    }
}

#[tokio::test]
async fn restricted_send_policy_is_rechecked_at_the_synchronized_commit_gate() {
    POLICY_CHECKS.store(0, Ordering::SeqCst);
    let (client, accepted) = tokio::io::duplex(64);
    let (peers, mut escaped) = mpsc::channel(1);
    let hook = Function(move |peer: Peer| {
        let peers = peers.clone();
        async move {
            peers.send(peer).await.unwrap();
            Ok::<_, Error>(())
        }
    });
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .on_peer_ready(SendPolicy::new(reject_at_commit), hook)
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(one(accepted), ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let channel =
        bounded(raw("memory://commit-policy").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    let peer = bounded(escaped.recv()).await.unwrap();
    let operation = peer.send(packet("checked twice", b""), deadline());
    let attempt = operation.attempt();
    assert!(matches!(
        operation.await,
        Err(Error::InvalidInput("policy changed before commitment"))
    ));
    assert_eq!(POLICY_CHECKS.load(Ordering::SeqCst), 2);
    assert_eq!(
        attempt.disposition(),
        furpc::AttemptDisposition::Retired { committed: false }
    );
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    drop(channel);
}

#[tokio::test(start_paused = true)]
async fn on_time_local_write_does_not_expire_when_observed_late() {
    let (client, accepted) = tokio::io::duplex(64);
    let serving = tokio::spawn(Server::builder().build().unwrap().serve_with_shutdown(
        one(accepted),
        ConnectionOptions::raw_only(),
        pending(),
    ));
    let channel =
        bounded(raw("memory://settlement").connect_with_connector(Connector::new(vec![client])))
            .await
            .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut operation = Box::pin(peer.send(packet("on time", b""), deadline));
    let attempt = operation.attempt();
    assert!(operation.as_mut().now_or_never().is_none());
    bounded(async {
        while attempt.disposition() != furpc::AttemptDisposition::LocallyWritten {
            tokio::task::yield_now().await;
        }
    })
    .await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(operation.await.unwrap().written_at() <= deadline);
    drop(channel);
    bounded(serving).await.unwrap().unwrap();
}
