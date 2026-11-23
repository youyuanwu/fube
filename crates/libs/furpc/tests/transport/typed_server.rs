use super::*;
use furpc::{server, Codec, CodecError, DecodeBuf, Decoder, EncodeBuf, Encoder, Reply};
use std::{marker::PhantomData, rc::Rc};

#[derive(Default)]
struct CodecCounts {
    encoded: AtomicUsize,
    decoded: AtomicUsize,
}

struct BytesCodec {
    counts: Arc<CodecCounts>,
    _not_sync: Cell<()>,
}
impl BytesCodec {
    fn new(counts: &Arc<CodecCounts>) -> Self {
        Self {
            counts: counts.clone(),
            _not_sync: Cell::new(()),
        }
    }
}
struct BytesEncoder(Arc<CodecCounts>, Cell<()>);
struct BytesDecoder(Arc<CodecCounts>, Cell<()>);
impl Codec for BytesCodec {
    type Encode = Body;
    type Decode = Body;
    type Encoder = BytesEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BytesEncoder(self.counts.clone(), Cell::new(()))
    }
    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder(self.counts.clone(), Cell::new(()))
    }
}
impl Encoder for BytesEncoder {
    type Item = Body;

    fn encode(&mut self, body: Body, dst: &mut EncodeBuf<'_>) -> Result<(), CodecError> {
        self.1.set(());
        self.0.encoded.fetch_add(1, Ordering::SeqCst);
        dst.append_body(body)
    }
}
impl Decoder for BytesDecoder {
    type Item = Body;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Body, CodecError> {
        self.1.set(());
        self.0.decoded.fetch_add(1, Ordering::SeqCst);
        src.take(src.remaining())
    }
}

struct DecodeOnly(Arc<CodecCounts>, Cell<()>);
struct UnusedEncoder {
    _not_send: PhantomData<Rc<()>>,
}
impl Encoder for UnusedEncoder {
    type Item = Rc<()>;

    fn encode(&mut self, _: Rc<()>, _: &mut EncodeBuf<'_>) -> Result<(), CodecError> {
        panic!("one-way adaptation must not encode")
    }
}
impl Codec for DecodeOnly {
    type Encode = Rc<()>;
    type Decode = Body;
    type Encoder = UnusedEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        panic!("one-way adaptation must not construct an encoder")
    }
    fn decoder(&mut self) -> Self::Decoder {
        self.1.set(());
        BytesDecoder(self.0.clone(), Cell::new(()))
    }
}

struct Prepared<F> {
    f: F,
    ready: Cell<bool>,
    pending_once: bool,
}
impl<F> Prepared<F> {
    fn new(f: F) -> Self {
        Self {
            f,
            ready: Cell::new(false),
            pending_once: true,
        }
    }
}
impl<F, Fut, B, R, E> Service<Request<B>> for Prepared<F>
where
    F: FnMut(Request<B>) -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    type Response = R;
    type Error = E;
    type Future = Fut;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        if self.pending_once {
            self.pending_once = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.ready.set(true);
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<B>) -> Fut {
        assert!(
            self.ready.replace(false),
            "typed readiness was not preserved"
        );
        (self.f)(request)
    }
}

type Work = (
    Request<Body>,
    oneshot::Sender<furpc::Result<IngressResponse>>,
);
struct QueuedIngress {
    channel: furpc::Channel,
    work: mpsc::Receiver<Work>,
    stop: oneshot::Sender<()>,
    shutting_down: Arc<Notify>,
    serving: tokio::task::JoinHandle<furpc::Result<()>>,
}
impl QueuedIngress {
    async fn start(options: IngressOptions, limits: HandlerLimits) -> Self {
        let (client, accepted) = tokio::io::duplex(128);
        let (queued, work) = mpsc::channel::<Work>(4);
        let handler = IngressService::new(
            Function(move |request| {
                let queued = queued.clone();
                async move {
                    let (complete, completed) = oneshot::channel();
                    queued.send((request, complete)).await.unwrap();
                    completed.await.unwrap()
                }
            }),
            vec![ServiceBinding::raw(options).unwrap()],
        )
        .unwrap();
        let (stop, stopped) = oneshot::channel();
        let shutting_down = Arc::new(Notify::new());
        let shutdown = shutting_down.clone();
        let serving = tokio::spawn(
            Server::builder()
                .service(handler)
                .unwrap()
                .handler_limits(limits)
                .limits(TransportLimits {
                    max_receive_frame_bytes: 256,
                    ..Default::default()
                })
                .build()
                .unwrap()
                .serve_with_shutdown(one(accepted), ConnectionOptions::managed(), async move {
                    stopped.await.unwrap();
                    shutdown.notify_one();
                }),
        );
        let channel = bounded(
            Endpoint::from_shared("memory://typed-queue")
                .unwrap()
                .connect_with_connector(Connector::new(vec![client])),
        )
        .await
        .unwrap();
        Self {
            channel,
            work,
            stop,
            shutting_down,
            serving,
        }
    }

    async fn finish(self) {
        self.stop.send(()).unwrap();
        bounded(self.serving).await.unwrap().unwrap();
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Echo {
    text: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct EchoReply {
    text: String,
    count: u32,
}

#[tokio::test]
async fn typed_codecs_round_trip_without_a_router_and_preserve_original_reply_affinity() {
    fn response_policy(headers: &Headers, _: &Source) -> furpc::Result<()> {
        if action(headers) == "TypedReply" {
            Ok(())
        } else {
            Err(Error::InvalidInput("typed response action"))
        }
    }
    let counts = Arc::new(CodecCounts::default());
    let handler = IngressService::new(
        Function({
            let counts = counts.clone();
            move |mut request: Request<Body>| {
                let counts = counts.clone();
                async move {
                    let native = action(request.headers()) == "native";
                    let source = request.context().unwrap().source().generation();
                    let original_deadline = request.context().unwrap().deadline();
                    request.set_deadline(Instant::now() + Duration::from_secs(3600));
                    request.headers_mut().remove_all(header::MESSAGE_ID)?;
                    request.headers_mut().remove_all(header::EXPECTS_REPLY)?;
                    request.headers_mut().append(furpc::HeaderRecord::native(
                        header::MESSAGE_ID,
                        &furpc::MessageId {
                            guid: [9; 16],
                            index: 99,
                        },
                    )?)?;
                    if native {
                        server::Rpc::new(FubstCodec::<EchoReply, Echo>::default())
                            .unary(
                                Prepared::new(move |request: Request<Echo>| async move {
                                    assert_eq!(
                                        request.context().unwrap().source().generation(),
                                        source
                                    );
                                    assert_eq!(
                                        request.context().unwrap().deadline(),
                                        original_deadline
                                    );
                                    assert!(request.context().unwrap().peer().is_none());
                                    assert!(!request.headers().effective().expects_reply);
                                    Reply::new(EchoReply {
                                        text: request.into_body().text,
                                        count: 1,
                                    })
                                    .append_header(header::action("TypedReply")?)
                                }),
                                request,
                            )
                            .await
                    } else {
                        server::Rpc::new(BytesCodec::new(&counts))
                            .unary(
                                Prepared::new(move |request: Request<Body>| async move {
                                    assert_eq!(
                                        request.context().unwrap().source().generation(),
                                        source
                                    );
                                    Reply::new(request.into_body())
                                        .append_header(header::action("TypedReply")?)
                                }),
                                request,
                            )
                            .await
                    }
                }
            }
        }),
        vec![ServiceBinding::raw(IngressOptions::requests(
            RequestPolicy::new(allow, response_policy),
            IngressLimits::default(),
        ))
        .unwrap()],
    )
    .unwrap();
    let listener = TcpAcceptor::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .service(handler)
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(listener, ConnectionOptions::managed(), async {
                stopped.await.unwrap();
            }),
    );
    let channel = bounded(Endpoint::try_from(address).unwrap().connect())
        .await
        .unwrap();
    let generation = channel.peer(deadline()).await.unwrap().info().generation;
    let mut rpc = RpcClient::new(channel);
    let response = rpc
        .call(
            Request::new(Echo {
                text: "hello".into(),
            })
            .append_header(header::action("native").unwrap())
            .unwrap(),
            FubstCodec::<Echo, EchoReply>::default(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(response.body().text, "hello");
    assert_eq!(response.body().count, 1);
    assert_eq!(response.source().unwrap().generation(), generation);
    assert_eq!(action(response.headers()), "TypedReply");
    assert_ne!(response.headers().effective().relates_to.guid, [9; 16]);
    let body = Body::from_bytes(b"custom bytes".to_vec()).unwrap();
    let response = rpc
        .call(Request::new(body), BytesCodec::new(&counts), deadline())
        .await
        .unwrap();
    assert_eq!(response.body().segments().next().unwrap(), b"custom bytes");
    assert_eq!(response.source().unwrap().generation(), generation);
    assert_eq!(action(response.headers()), "TypedReply");
    assert_eq!(counts.encoded.load(Ordering::SeqCst), 2);
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
}

#[tokio::test]
async fn typed_methods_reject_missing_wrong_kind_and_stale_context_before_decode() {
    let counts = Arc::new(CodecCounts::default());
    let error = server::Rpc::new(BytesCodec::new(&counts))
        .unary(
            Function(|_: Request<Body>| async { Ok::<_, Error>(Reply::new(Body::empty())) }),
            Request::new(Body::empty()),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput(_)));
    for requests in [false, true] {
        let options = if requests {
            IngressOptions::requests(RequestPolicy::new(allow, allow), IngressLimits::default())
        } else {
            IngressOptions::one_way(allow, IngressLimits::default())
        };
        let mut queued = QueuedIngress::start(options, HandlerLimits::default()).await;
        let peer = queued.channel.peer(deadline()).await.unwrap();
        let mut operation = if requests {
            let mut operation = Box::pin(peer.request(rpc_request(b""), deadline()));
            assert!(operation.as_mut().now_or_never().is_none());
            Some(operation)
        } else {
            peer.send(packet("one-way", b""), deadline()).await.unwrap();
            None
        };
        let (mut request, completed) = bounded(queued.work.recv()).await.unwrap();
        if requests {
            request
                .headers_mut()
                .remove_all(header::EXPECTS_REPLY)
                .unwrap();
        } else {
            #[derive(serde::Serialize)]
            struct Flag {
                value: bool,
            }
            request
                .headers_mut()
                .append(
                    furpc::HeaderRecord::native(header::EXPECTS_REPLY, &Flag { value: true })
                        .unwrap(),
                )
                .unwrap();
        }
        let error = if requests {
            server::Rpc::new(DecodeOnly(counts.clone(), Cell::new(())))
                .one_way(
                    Function(|_: Request<Body>| async { Ok::<_, Error>(()) }),
                    request,
                )
                .await
                .unwrap_err()
        } else {
            server::Rpc::new(BytesCodec::new(&counts))
                .unary(
                    Function(|_: Request<Body>| async {
                        Ok::<_, Error>(Reply::new(Body::empty()))
                    }),
                    request,
                )
                .await
                .unwrap_err()
        };
        assert!(matches!(error, Error::InvalidInput(_)));
        completed.send(Err(error)).unwrap();
        operation.take();
        let mut stale_operation = if requests {
            let mut operation = Box::pin(peer.request(rpc_request(b""), deadline()));
            assert!(operation.as_mut().now_or_never().is_none());
            Some(operation)
        } else {
            peer.send(packet("stale", b""), deadline()).await.unwrap();
            None
        };
        let (request, completed) = bounded(queued.work.recv()).await.unwrap();
        completed.send(Err(Error::Cancelled)).unwrap();
        stale_operation.take();
        queued.finish().await;
        let error = if requests {
            server::Rpc::new(BytesCodec::new(&counts))
                .unary(
                    Function(|_: Request<Body>| async {
                        Ok::<_, Error>(Reply::new(Body::empty()))
                    }),
                    request,
                )
                .await
                .unwrap_err()
        } else {
            server::Rpc::new(DecodeOnly(counts.clone(), Cell::new(())))
                .one_way(
                    Function(|_: Request<Body>| async { Ok::<_, Error>(()) }),
                    request,
                )
                .await
                .unwrap_err()
        };
        assert_eq!(error, Error::Closed);
    }
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 0);
    assert_eq!(counts.encoded.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn typed_adaptation_uses_admitted_input_frame_and_output_limits_and_error_sources() {
    let counts = Arc::new(CodecCounts::default());
    let mut rpc = server::Rpc::new(BytesCodec::new(&counts));
    let mut queued = QueuedIngress::start(
        IngressOptions::requests(
            RequestPolicy::new(allow, allow),
            IngressLimits { max_body_bytes: 8 },
        ),
        HandlerLimits {
            max_encoded_body_bytes_per_response: 2,
            ..Default::default()
        },
    )
    .await;
    let peer = queued.channel.peer(deadline()).await.unwrap();
    for case in 0..6 {
        let mut operation = Box::pin(peer.request(rpc_request(b""), deadline()));
        assert!(operation.as_mut().now_or_never().is_none());
        let (mut request, completed) = bounded(queued.work.recv()).await.unwrap();
        if case == 0 {
            request = request.map(|_| Body::from_bytes(vec![0; 9]).unwrap());
        } else if case == 1 {
            request
                .headers_mut()
                .append(header::action(&"x".repeat(256)).unwrap())
                .unwrap();
        }
        let error = if case == 4 {
            server::Rpc::new(FubstCodec::<EchoReply, Echo>::default())
                .unary(
                    Function(|_: Request<Echo>| async {
                        Ok::<_, Error>(Reply::new(EchoReply {
                            text: String::new(),
                            count: 0,
                        }))
                    }),
                    request,
                )
                .await
                .unwrap_err()
        } else if case == 5 {
            rpc.unary(FailingReadiness, request).await.unwrap_err()
        } else {
            rpc.unary(
                Function(move |_: Request<Body>| async move {
                    if case == 3 {
                        return Err(std::io::Error::other("typed handler detail"));
                    }
                    Ok(Reply::new(Body::from_bytes(vec![0; 3]).unwrap()))
                }),
                request,
            )
            .await
            .unwrap_err()
        };
        match case {
            0 | 1 => assert!(matches!(error, Error::ResourceLimit(_))),
            2 => assert!(matches!(
                error,
                Error::Codec {
                    phase: "encode",
                    source: CodecError::ResourceLimit { .. }
                }
            )),
            4 => assert!(matches!(
                error,
                Error::Codec {
                    phase: "decode",
                    source: CodecError::InvalidPayload { .. }
                }
            )),
            _ => {
                let expected_phase = if case == 3 {
                    "typed handler"
                } else {
                    "typed service readiness"
                };
                let expected_source = if case == 3 {
                    "typed handler detail"
                } else {
                    "typed readiness detail"
                };
                assert!(matches!(error, Error::Handler { phase, .. } if phase == expected_phase));
                assert_eq!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .downcast_ref::<std::io::Error>()
                        .unwrap()
                        .to_string(),
                    expected_source
                );
            }
        }
        completed.send(Err(error)).unwrap();
        drop(operation);
    }
    assert_eq!(counts.decoded.load(Ordering::SeqCst), 3);
    assert_eq!(counts.encoded.load(Ordering::SeqCst), 1);
    queued.finish().await;
}

struct FailingReadiness;
impl Service<Request<Body>> for FailingReadiness {
    type Response = Reply<Body>;
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(std::io::Error::other("typed readiness detail")))
    }
    fn call(&mut self, _: Request<Body>) -> Self::Future {
        panic!("typed readiness error was ignored")
    }
}

struct PendingTyped;
impl Service<Request<Body>> for PendingTyped {
    type Response = ();
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<(), std::io::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
    fn call(&mut self, _: Request<Body>) -> Self::Future {
        panic!("pending typed service was called")
    }
}

#[tokio::test]
async fn typed_readiness_has_no_implicit_waiter_cap_and_preserves_cancellation() {
    fn send<T: Send>(value: T) -> T {
        value
    }

    {
        let counts = Arc::new(CodecCounts::default());
        let mut queued = QueuedIngress::start(
            IngressOptions::one_way(allow, IngressLimits::default()),
            HandlerLimits::default(),
        )
        .await;
        let peer = queued.channel.peer(deadline()).await.unwrap();
        let mut pending = vec![];
        for _ in 0..12 {
            peer.send(packet("pending", b""), deadline()).await.unwrap();
            let (request, complete) = bounded(queued.work.recv()).await.unwrap();
            let counts = counts.clone();
            let mut call = Box::pin(async move {
                server::Rpc::new(DecodeOnly(counts, Cell::new(())))
                    .one_way(PendingTyped, request)
                    .await
            });
            assert!(call.as_mut().now_or_never().is_none());
            pending.push((call, complete));
        }
        assert_eq!(counts.decoded.load(Ordering::SeqCst), 12);
        for (call, complete) in pending {
            complete.send(Err(Error::Cancelled)).unwrap();
            assert_eq!(bounded(call).await.unwrap_err(), Error::Closed);
        }
        peer.send(packet("ready", b""), deadline()).await.unwrap();
        let (request, completed) = bounded(queued.work.recv()).await.unwrap();
        let mut rpc = server::Rpc::new(DecodeOnly(counts.clone(), Cell::new(())));
        let result = send(rpc.one_way(
            Function(|_: Request<Body>| async { Ok::<_, Error>(()) }),
            request,
        ))
        .await
        .unwrap();
        assert!(matches!(result, IngressResponse::NoReply));
        completed.send(Ok(result)).unwrap();
        assert_eq!(counts.encoded.load(Ordering::SeqCst), 0);
        queued.finish().await;
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn queued_typed_work_observes_original_deadlines_and_runner_cancellation() {
    for expire in [false, true] {
        let counts = Arc::new(CodecCounts::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut queued = QueuedIngress::start(
            IngressOptions::one_way(allow, IngressLimits::default()),
            HandlerLimits {
                timeout: if expire {
                    Duration::from_millis(20)
                } else {
                    Duration::from_secs(3)
                },
                ..Default::default()
            },
        )
        .await;
        queued
            .channel
            .peer(deadline())
            .await
            .unwrap()
            .send(packet("queued", b""), deadline())
            .await
            .unwrap();
        let (mut request, completed) = bounded(queued.work.recv()).await.unwrap();
        request.set_deadline(Instant::now() + Duration::from_secs(3600));
        let mut rpc = server::Rpc::new(DecodeOnly(counts.clone(), Cell::new(())));
        let mut method = Box::pin(rpc.one_way(
            Function({
                let dropped = dropped.clone();
                move |_: Request<Body>| {
                    let guard = DropCount(dropped.clone());
                    async move {
                        let _guard = guard;
                        pending::<furpc::Result<()>>().await
                    }
                }
            }),
            request,
        ));
        assert!(method.as_mut().now_or_never().is_none());
        if !expire {
            completed.send(Err(Error::Cancelled)).unwrap();
        }
        let error = bounded(method).await.unwrap_err();
        assert!(matches!(error, Error::Closed | Error::Timeout("handler")));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(counts.decoded.load(Ordering::SeqCst), 1);
        assert_eq!(counts.encoded.load(Ordering::SeqCst), 0);
        queued.finish().await;
    }
}

#[tokio::test]
async fn typed_reply_finishes_during_graceful_drain_without_a_general_peer() {
    let mut queued = QueuedIngress::start(
        IngressOptions::requests(RequestPolicy::new(allow, allow), IngressLimits::default()),
        HandlerLimits::default(),
    )
    .await;
    let mut client = RpcClient::new(queued.channel.clone());
    let mut call = Box::pin(client.call(
        Request::new(Echo {
            text: "drain".into(),
        }),
        FubstCodec::<Echo, Echo>::default(),
        deadline(),
    ));
    assert!(call.as_mut().now_or_never().is_none());
    let (request, completed) = bounded(queued.work.recv()).await.unwrap();
    assert!(request.context().unwrap().peer().is_none());
    queued.stop.send(()).unwrap();
    bounded(queued.shutting_down.notified()).await;
    assert!(!queued.serving.is_finished());
    let result = server::Rpc::new(FubstCodec::<Echo, Echo>::default())
        .unary(
            Prepared::new(|request: Request<Echo>| async move {
                Ok::<_, Error>(Reply::new(request.into_body()))
            }),
            request,
        )
        .await
        .unwrap();
    completed.send(Ok(result)).unwrap();
    assert_eq!(bounded(call).await.unwrap().body().text, "drain");
    bounded(queued.serving).await.unwrap().unwrap();
}
