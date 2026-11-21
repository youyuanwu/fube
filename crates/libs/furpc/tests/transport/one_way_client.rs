use super::*;
use furpc::{
    ActorId, AttemptDisposition, Codec, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder,
    HeaderId, HeaderRecord, OneWayClient, OneWayRouteOptions, PeerRequestService, PeerSendService,
    Router, WriteReceipt,
};
use std::{marker::PhantomData, rc::Rc};
use tokio::time::{advance, sleep, Sleep};

const ACTOR: i64 = 70000;

struct Pair {
    channel: furpc::Channel,
    peer: Peer,
    received: mpsc::Receiver<Request<Body>>,
    gate: Arc<WriteGate>,
    release: Arc<Notify>,
    stop: oneshot::Sender<()>,
    serving: tokio::task::JoinHandle<furpc::Result<()>>,
}

impl Pair {
    async fn start(managed: bool) -> Self {
        let (client, accepted) = tokio::io::duplex(512);
        let (delivered, received) = mpsc::channel(8);
        let release = Arc::new(Notify::new());
        let queued = delivered.clone();
        let mut router = Router::new()
            .one_way(
                ActorId::new(ACTOR).unwrap(),
                "Report",
                OneWayRouteOptions::new(allow, IngressLimits::default()),
                Function(move |request| {
                    let queued = queued.clone();
                    async move {
                        queued.send(request).await.unwrap();
                        Ok::<_, Error>(IngressResponse::NoReply)
                    }
                }),
            )
            .unwrap();
        if managed {
            for name in ["Hold", "Echo"] {
                let delivered = delivered.clone();
                let release = release.clone();
                router = router
                    .request(
                        ActorId::new(ACTOR).unwrap(),
                        name,
                        furpc::RequestRouteOptions::new(
                            RequestPolicy::new(allow, allow),
                            IngressLimits::default(),
                        ),
                        Function(move |request: Request<Body>| {
                            let delivered = delivered.clone();
                            let release = release.clone();
                            async move {
                                let response =
                                    Message::builder().body(request.body().clone()).build()?;
                                delivered.send(request).await.unwrap();
                                if name == "Hold" {
                                    release.notified().await;
                                }
                                Ok::<_, Error>(IngressResponse::Reply(response))
                            }
                        }),
                    )
                    .unwrap();
            }
        }
        let options = if managed {
            ConnectionOptions::managed()
        } else {
            ConnectionOptions::raw_only()
        };
        let (stop, stopped) = oneshot::channel();
        let serving = tokio::spawn(
            Server::builder()
                .service(router.into_service().unwrap())
                .unwrap()
                .build()
                .unwrap()
                .serve_with_shutdown(one(accepted), options, async move {
                    stopped.await.unwrap();
                }),
        );
        let gate = Arc::new(WriteGate::default());
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
        let channel = bounded(
            Endpoint::from_shared("memory://typed-one-way")
                .unwrap()
                .connection_options(options)
                .limits(TransportLimits {
                    max_pending_requests: 1,
                    ..Default::default()
                })
                .connect_with_connector(connector),
        )
        .await
        .unwrap();
        let peer = channel.peer(deadline()).await.unwrap();
        Self {
            channel,
            peer,
            received,
            gate,
            release,
            stop,
            serving,
        }
    }

    async fn receive(&mut self) -> Request<Body> {
        bounded(self.received.recv()).await.unwrap()
    }

    async fn finish(self) {
        self.release.notify_waiters();
        self.stop.send(()).unwrap();
        bounded(self.serving).await.unwrap().unwrap();
        drop(self.channel);
    }
}

fn report<T>(body: T) -> Request<T> {
    Request::new(body)
        .append_header(header::actor(ActorId::new(ACTOR).unwrap()).unwrap())
        .unwrap()
        .append_header(header::action("Report").unwrap())
        .unwrap()
}

// Neither the service nor its borrowed state is Clone/Sync/'static.
struct PreparedSend<'a> {
    inner: PeerSendService,
    calls: &'a AtomicUsize,
    ready: Cell<bool>,
    delay: Option<Pin<Box<Sleep>>>,
}

impl Service<Request<Body>> for PreparedSend<'_> {
    type Response = WriteReceipt;
    type Error = Error;
    type Future = <PeerSendService as Service<Request<Body>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        if let Some(delay) = &mut self.delay {
            std::task::ready!(delay.as_mut().poll(cx));
        }
        self.ready.set(true);
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        assert!(
            self.ready.replace(false),
            "same-instance readiness required"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.call(request)
    }
}

struct EncodeOnly {
    encoded: Arc<AtomicUsize>,
    state: Cell<bool>,
}
struct CountEncoder(Arc<AtomicUsize>, Cell<bool>);
struct UnusedDecoder(PhantomData<Rc<()>>);

impl Codec for EncodeOnly {
    type Encode = u32;
    type Decode = Rc<()>;
    type Encoder = CountEncoder;
    type Decoder = UnusedDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        assert!(!self.state.replace(true));
        CountEncoder(self.encoded.clone(), Cell::new(false))
    }

    fn decoder(&mut self) -> Self::Decoder {
        panic!("typed sends must not construct a decoder")
    }
}

impl Encoder for CountEncoder {
    type Item = u32;

    fn encode(&mut self, item: u32, dst: &mut EncodeBuf<'_>) -> Result<(), CodecError> {
        assert!(!self.1.replace(true));
        self.0.fetch_add(1, Ordering::SeqCst);
        dst.write_all(&item.to_le_bytes())
    }
}

impl Decoder for UnusedDecoder {
    type Item = Rc<()>;

    fn decode(&mut self, _: &mut DecodeBuf<'_>) -> Result<Rc<()>, CodecError> {
        panic!("typed sends must not decode")
    }
}

fn encode_only(encoded: &Arc<AtomicUsize>) -> EncodeOnly {
    EncodeOnly {
        encoded: encoded.clone(),
        state: Cell::new(false),
    }
}

#[tokio::test]
async fn typed_send_is_inert_encodes_only_and_preserves_borrowed_service_readiness() {
    let mut pair = Pair::start(true).await;
    let encoded = Arc::new(AtomicUsize::new(0));
    let calls = AtomicUsize::new(0);
    let mut client = OneWayClient::new(PreparedSend {
        inner: PeerSendService::new(pair.peer.clone()),
        calls: &calls,
        ready: Cell::new(false),
        delay: None,
    });
    drop(client.send(report(1), encode_only(&encoded), deadline()));
    assert_eq!(encoded.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    fn send<F: Send>(future: F) -> F {
        future
    }
    let expiry = deadline();
    let unknown = HeaderId::new(0x9001).unwrap();
    let receipt = send(
        client.send(
            report(42)
                .append_header(HeaderRecord::from_encoded(unknown, vec![0xff]).unwrap())
                .unwrap(),
            encode_only(&encoded),
            expiry,
        ),
    )
    .await
    .unwrap();
    assert!(receipt.written_at() <= expiry);
    let received = pair.receive().await;
    assert_eq!(
        received.headers().first_raw(unknown).unwrap().value(),
        [0xff]
    );
    assert!(!received.headers().effective().expects_reply);
    assert_eq!(
        received.headers().effective().relates_to,
        furpc::MessageId::default()
    );
    assert_eq!(
        received.body().segments().next().unwrap(),
        42u32.to_le_bytes()
    );
    assert_eq!(encoded.load(Ordering::SeqCst), 1);

    let mut client = client.max_encoded_body_bytes(0).unwrap();
    client
        .send(report(()), FubstCodec::<(), ()>::default(), expiry)
        .await
        .unwrap();
    assert!(pair.receive().await.body().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(client.into_inner());
    pair.finish().await;
}

#[tokio::test]
async fn typed_send_needs_no_rpc_slot_or_correlated_response() {
    let mut pair = Pair::start(true).await;
    let mut hold = Request::new(Body::empty())
        .append_header(header::actor(ActorId::new(ACTOR).unwrap()).unwrap())
        .unwrap()
        .append_header(header::action("Hold").unwrap())
        .unwrap();
    hold.set_deadline(deadline());
    let mut pending = Box::pin(pair.peer.request(hold, deadline()));
    assert!(pending.as_mut().now_or_never().is_none());
    assert_eq!(action(pair.receive().await.headers()), "Hold");
    let mut requests = RpcClient::new(PeerRequestService::new(pair.peer.clone()));
    assert_eq!(
        requests
            .call(report(()), FubstCodec::<(), ()>::default(), deadline())
            .await
            .unwrap_err(),
        Error::ResourceLimit("pending RPC/results"),
    );
    let mut client = OneWayClient::new(PeerSendService::new(pair.peer.clone()));
    for value in [1u32, 2] {
        client
            .send(report(value), FubstCodec::<u32, ()>::default(), deadline())
            .await
            .unwrap();
        let received = pair.receive().await;
        assert!(!received.headers().effective().expects_reply);
        let bytes = received
            .body()
            .segments()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(fubst::serde::from_slice::<u32>(&bytes).unwrap(), value);
    }
    drop(pending);
    pair.finish().await;
}

#[tokio::test(start_paused = true)]
async fn typed_send_limits_and_shortened_deadlines_prevent_peer_submission() {
    let pair = Pair::start(true).await;
    let encoded = Arc::new(AtomicUsize::new(0));
    let calls = AtomicUsize::new(0);
    for delay in [0, 1, 2] {
        let mut client = OneWayClient::new(PreparedSend {
            inner: PeerSendService::new(pair.peer.clone()),
            calls: &calls,
            ready: Cell::new(false),
            delay: Some(Box::pin(sleep(Duration::from_secs(delay)))),
        });
        let local = Instant::now() + Duration::from_secs(u64::from(delay != 0));
        let mut request = report(42);
        request.set_deadline(local);
        assert_eq!(
            client
                .send(
                    request,
                    encode_only(&encoded),
                    local + Duration::from_secs(20)
                )
                .await
                .unwrap_err(),
            Error::Timeout("send"),
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(encoded.load(Ordering::SeqCst), 2);
    let mut client = OneWayClient::new(PreparedSend {
        inner: PeerSendService::new(pair.peer.clone()),
        calls: &calls,
        ready: Cell::new(false),
        delay: None,
    })
    .max_encoded_body_bytes(3)
    .unwrap();
    assert!(matches!(
        client
            .send(report(42), encode_only(&encoded), deadline())
            .await,
        Err(Error::Codec {
            phase: "encode",
            source: CodecError::ResourceLimit { .. }
        }),
    ));
    assert!(!client.get_mut().ready.get());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    pair.finish().await;
}

#[tokio::test]
async fn peer_services_require_deadlines_and_reject_correlation_fields_even_in_raw_mode() {
    for managed in [false, true] {
        let mut pair = Pair::start(managed).await;
        let mut sends = PeerSendService::new(pair.peer.clone());
        assert_eq!(
            sends.call(report(Body::empty())).await.unwrap_err(),
            Error::InvalidInput("byte send requires a deadline"),
        );
        let mut requests = PeerRequestService::new(pair.peer.clone());
        assert_eq!(
            requests
                .call(Request::new(Body::empty()))
                .await
                .unwrap_err(),
            Error::InvalidInput("byte RPC requires a deadline"),
        );
        for id in [
            header::MESSAGE_ID,
            header::RELATES_TO,
            header::EXPECTS_REPLY,
            header::UNCORRELATED_REPLY,
        ] {
            let bytes = if matches!(id, header::MESSAGE_ID | header::RELATES_TO) {
                fubst::serde::to_vec(&furpc::MessageId {
                    guid: [7; 16],
                    index: 1,
                })
                .unwrap()
            } else {
                #[derive(serde::Serialize)]
                struct Flag {
                    value: bool,
                }
                fubst::serde::to_vec(&Flag { value: false }).unwrap()
            };
            let mut request = report(Body::empty())
                .append_header(HeaderRecord::from_encoded(id, bytes).unwrap())
                .unwrap();
            request.set_deadline(deadline());
            assert_eq!(
                sends.call(request).await.unwrap_err(),
                Error::InvalidInput("caller supplied managed correlation fields"),
            );
        }
        OneWayClient::new(sends)
            .send(report(()), FubstCodec::<(), ()>::default(), deadline())
            .await
            .unwrap();
        assert!(pair.receive().await.body().is_empty());
        if managed {
            let response = RpcClient::new(requests)
                .call(
                    Request::new(42u32)
                        .append_header(header::actor(ActorId::new(ACTOR).unwrap()).unwrap())
                        .unwrap()
                        .append_header(header::action("Echo").unwrap())
                        .unwrap(),
                    FubstCodec::<u32, u32>::default(),
                    deadline(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.source().unwrap().generation(),
                pair.peer.info().generation
            );
            assert_eq!(response.into_body(), 42);
        }
        pair.finish().await;
    }
}

#[tokio::test]
async fn dropping_typed_sends_retracts_only_uncommitted_frames_without_replay() {
    for committed in [false, true] {
        let mut pair = Pair::start(true).await;
        let mut client = OneWayClient::new(PeerSendService::new(pair.peer.clone()));
        pair.gate.limit(Some(usize::from(committed)));
        let mut sending =
            Box::pin(client.send(report(1u32), FubstCodec::<u32, ()>::default(), deadline()));
        assert!(sending.as_mut().now_or_never().is_none());
        bounded(pair.gate.blocked.notified()).await;
        drop(sending);
        pair.gate.limit(None);
        client
            .send(report(2u32), FubstCodec::<u32, ()>::default(), deadline())
            .await
            .unwrap();
        for value in if committed { vec![1u32, 2] } else { vec![2] } {
            let received = pair.receive().await;
            let bytes = received
                .body()
                .segments()
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            assert_eq!(fubst::serde::from_slice::<u32>(&bytes).unwrap(), value);
        }
        assert!(pair.received.try_recv().is_err());
        pair.finish().await;
    }
}

#[tokio::test(start_paused = true)]
async fn typed_native_write_settlement_survives_late_observation_and_retirement() {
    let pair = Pair::start(true).await;
    let peer = pair.peer.clone();
    let attempt = Arc::new(Mutex::new(None));
    let captured = attempt.clone();
    let transport = Function(move |request: Request<Body>| {
        let expiry = request.deadline().unwrap();
        let (parts, body) = request.into_parts();
        let mut builder = Message::builder();
        for record in parts.headers.records() {
            builder = builder.append_header(record.to_owned());
        }
        let operation = peer.send(builder.body(body).build().unwrap(), expiry);
        *captured.lock().unwrap() = Some(operation.attempt());
        operation
    });
    let expiry = Instant::now() + Duration::from_secs(1);
    let mut client = OneWayClient::new(transport);
    let mut sending =
        Box::pin(client.send(report(42u32), FubstCodec::<u32, ()>::default(), expiry));
    assert!(sending.as_mut().now_or_never().is_none());
    bounded(async {
        loop {
            if attempt.lock().unwrap().as_ref().unwrap().disposition()
                == AttemptDisposition::LocallyWritten
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    advance(Duration::from_secs(2)).await;
    pair.finish().await;
    assert!(sending.await.unwrap().written_at() <= expiry);
}
