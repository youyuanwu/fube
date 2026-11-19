use super::*;
use furpc::{server, ActorId, OneWayRouteOptions, Reply, RequestRouteOptions, Router};

fn actor(value: i64) -> ActorId {
    ActorId::new(value).unwrap()
}
fn one_way_options() -> OneWayRouteOptions {
    OneWayRouteOptions::new(allow, IngressLimits::default())
}
fn request_options() -> RequestRouteOptions {
    RequestRouteOptions::new(RequestPolicy::new(allow, allow), IngressLimits::default())
}
fn routed(actor_id: ActorId, name: &str, body: &[u8]) -> Message {
    Message::builder()
        .append_header(header::actor(actor_id).unwrap())
        .append_header(header::action(name).unwrap())
        .body(Body::from_bytes(body.to_vec()).unwrap())
        .build()
        .unwrap()
}
fn no_reply(_: Request<Body>) -> std::future::Ready<furpc::Result<IngressResponse>> {
    std::future::ready(Ok(IngressResponse::NoReply))
}

struct Running {
    channel: furpc::Channel,
    stop: oneshot::Sender<()>,
    shutting_down: Arc<Notify>,
    serving: tokio::task::JoinHandle<furpc::Result<()>>,
}
impl Running {
    async fn start<S>(service: IngressService<S>, limits: HandlerLimits) -> Self
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        let (client, accepted) = tokio::io::duplex(128);
        let (stop, stopped) = oneshot::channel();
        let shutting_down = Arc::new(Notify::new());
        let shutdown = shutting_down.clone();
        let serving = tokio::spawn(
            Server::builder()
                .service(service)
                .unwrap()
                .handler_limits(limits)
                .build()
                .unwrap()
                .serve_with_shutdown(one(accepted), ConnectionOptions::managed(), async move {
                    stopped.await.unwrap();
                    shutdown.notify_one();
                }),
        );
        let channel = bounded(
            Endpoint::from_shared("memory://routing")
                .unwrap()
                .connect_with_connector(Connector::new(vec![client])),
        )
        .await
        .unwrap();
        Self {
            channel,
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

#[test]
fn routing_configuration_rejects_reserved_duplicate_and_overlapping_claims() {
    for reserved in [1, 29, 35] {
        assert!(ServiceBinding::actor(
            actor(reserved),
            IngressOptions::one_way(allow, IngressLimits::default())
        )
        .is_err());
        assert!(Router::new()
            .one_way(actor(reserved), "x", one_way_options(), Function(no_reply))
            .is_err());
    }
    let options = || IngressOptions::one_way(allow, IngressLimits::default());
    for bindings in [
        vec![],
        vec![
            ServiceBinding::raw(options()).unwrap(),
            ServiceBinding::raw(options()).unwrap(),
        ],
        vec![
            ServiceBinding::raw(options()).unwrap(),
            ServiceBinding::actor(actor(7), options()).unwrap(),
        ],
        vec![
            ServiceBinding::actor(actor(7), options()).unwrap(),
            ServiceBinding::actor(actor(7), options()).unwrap(),
        ],
    ] {
        assert!(IngressService::new(Function(no_reply), bindings).is_err());
    }
    let router = Router::new()
        .one_way(
            actor(i64::MAX),
            "same",
            one_way_options(),
            Function(no_reply),
        )
        .unwrap();
    assert!(router
        .clone()
        .request(
            actor(i64::MAX),
            "same",
            request_options(),
            Function(no_reply)
        )
        .is_err());
    assert!(router
        .one_way(
            actor(i64::MAX),
            "other",
            one_way_options(),
            Function(no_reply)
        )
        .unwrap()
        .into_service()
        .is_ok());
}

#[tokio::test]
async fn exact_routes_preserve_effective_headers_and_mix_typed_rpc_with_opaque_services() {
    fn response(headers: &Headers, _: &Source) -> furpc::Result<()> {
        if action(headers) == "reply" {
            Ok(())
        } else {
            Err(Error::InvalidInput("reply policy"))
        }
    }
    let (events, mut received) = mpsc::channel(4);
    let raw_calls = Arc::new(AtomicUsize::new(0));
    let rpc_options = || {
        RequestRouteOptions::new(
            RequestPolicy::new(allow, response),
            IngressLimits::default(),
        )
    };
    let router = Router::new()
        .request(
            actor(i64::MAX),
            "native\u{1f980}",
            rpc_options(),
            Function(|request: Request<Body>| async move {
                server::Rpc::new(FubstCodec::<String, u32>::default())
                    .unary(
                        Function(|request: Request<u32>| async move {
                            assert!(request.context().unwrap().peer().is_none());
                            Reply::new(format!("value {}", request.into_body()))
                                .append_header(header::action("reply")?)
                        }),
                        request,
                    )
                    .await
            }),
        )
        .unwrap()
        .request(
            actor(i64::MAX),
            "raw",
            RequestRouteOptions::new(
                RequestPolicy::new(allow, response),
                IngressLimits { max_body_bytes: 2 },
            ),
            Function({
                let raw_calls = raw_calls.clone();
                move |request: Request<Body>| {
                    raw_calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        Ok::<_, Error>(IngressResponse::Reply(
                            Message::builder()
                                .append_header(header::action("reply")?)
                                .body(request.into_body())
                                .build()?,
                        ))
                    }
                }
            }),
        )
        .unwrap()
        .one_way(
            actor(70_001),
            "\u{c9}vent",
            one_way_options().with_send_policy(SendPolicy::new(control_sends)),
            Function(move |request: Request<Body>| {
                let events = events.clone();
                async move {
                    let (parts, body) = request.into_parts();
                    let request = Request::from_parts(parts, body);
                    assert_eq!(request.headers().first_actor().unwrap(), actor(90_000));
                    assert_eq!(
                        request.headers().first_action().unwrap(),
                        [b'o' as u16, b'l' as u16, b'd' as u16]
                    );
                    assert_eq!(request.headers().effective().actor, actor(70_001));
                    assert_eq!(
                        request.headers().effective().action(),
                        "\u{c9}vent".encode_utf16().collect::<Vec<_>>()
                    );
                    assert_eq!(
                        request
                            .headers()
                            .first_raw(furpc::HeaderId::new(0x9001)?)
                            .unwrap()
                            .value(),
                        [0xab]
                    );
                    let context = request.context().unwrap();
                    assert!(matches!(
                        context
                            .peer()
                            .unwrap()
                            .send(packet("forbidden", b""), deadline())
                            .await,
                        Err(Error::InvalidInput("restricted send"))
                    ));
                    events
                        .send((
                            context.source().generation(),
                            request
                                .body()
                                .segments()
                                .flatten()
                                .copied()
                                .collect::<Vec<_>>(),
                        ))
                        .await
                        .unwrap();
                    Ok::<_, Error>(IngressResponse::NoReply)
                }
            }),
        )
        .unwrap();
    let running = Running::start(router.into_service().unwrap(), HandlerLimits::default()).await;
    let peer = running.channel.peer(deadline()).await.unwrap();
    for packet in [
        routed(actor(42), "unregistered", &[0; 1024]),
        routed(actor(70_001), "unknown", &[0; 1024]),
        routed(actor(70_001), "\u{e9}vent", b""),
        routed(actor(70_001), "E\u{301}vent", b""),
        Message::builder()
            .append_header(header::actor(actor(70_001)).unwrap())
            .build()
            .unwrap(),
        packet("\u{c9}vent", b"no Actor"),
        Message::builder()
            .append_header(header::actor(actor(70_001)).unwrap())
            .append_header(header::actor(actor(42)).unwrap())
            .append_header(header::action("\u{c9}vent").unwrap())
            .build()
            .unwrap(),
    ] {
        peer.send(packet, deadline()).await.unwrap();
    }
    let mut invalid_utf16 = header::action("x").unwrap().value().to_vec();
    let unit = invalid_utf16.len() - 4;
    invalid_utf16[unit..unit + 2].copy_from_slice(&0xd800u16.to_le_bytes());
    peer.send(
        Message::builder()
            .append_header(header::actor(actor(70_001)).unwrap())
            .append_header(
                furpc::HeaderRecord::from_encoded(header::ACTION, invalid_utf16).unwrap(),
            )
            .build()
            .unwrap(),
        deadline(),
    )
    .await
    .unwrap();
    let mut rejected = Box::pin(
        peer.request(
            rpc_request(b"bad")
                .append_header(header::actor(actor(i64::MAX)).unwrap())
                .unwrap()
                .append_header(header::action("raw").unwrap())
                .unwrap(),
            deadline(),
        ),
    );
    assert!(rejected.as_mut().now_or_never().is_none());
    let mut client = RpcClient::new(running.channel.clone());
    let result = client
        .call(
            Request::new(41u32)
                .append_header(header::actor(actor(i64::MAX)).unwrap())
                .unwrap()
                .append_header(header::action("native\u{1f980}").unwrap())
                .unwrap(),
            FubstCodec::<u32, String>::default(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(result.body(), "value 41");
    assert!(result.headers().first_actor().is_none());
    assert_eq!(
        result.source().unwrap().generation(),
        peer.info().generation
    );
    drop(rejected);
    let response = peer
        .request(
            rpc_request(b"ok")
                .append_header(header::actor(actor(i64::MAX)).unwrap())
                .unwrap()
                .append_header(header::action("raw").unwrap())
                .unwrap(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(response.body().segments().next().unwrap(), b"ok");
    assert_eq!(raw_calls.load(Ordering::SeqCst), 1);
    peer.send(
        Message::builder()
            .append_header(header::actor(actor(90_000)).unwrap())
            .append_header(header::action("old").unwrap())
            .append_header(
                furpc::HeaderRecord::from_encoded(
                    furpc::HeaderId::new(0x9001).unwrap(),
                    vec![0xab],
                )
                .unwrap(),
            )
            .append_header(header::actor(actor(70_001)).unwrap())
            .append_header(header::action("\u{c9}vent").unwrap())
            .body(
                Body::from_segments(vec![
                    Body::from_bytes(b"seg".to_vec()).unwrap(),
                    Body::from_bytes(b"mented".to_vec()).unwrap(),
                ])
                .unwrap(),
            )
            .build()
            .unwrap(),
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(bounded(received.recv()).await.unwrap().1, b"segmented");
    assert!(received.try_recv().is_err());
    running.finish().await;
}

#[tokio::test]
async fn router_rejects_edited_keys_foreign_tables_and_stale_or_missing_provenance() {
    let (called, mut calls) = mpsc::channel(8);
    let (observed, mut results) = mpsc::channel(8);
    let (retained, mut retained_requests) = mpsc::channel(1);
    let target = Function(move |request: Request<Body>| {
        let called = called.clone();
        async move {
            called
                .send(request.body().segments().next().unwrap()[0])
                .await
                .unwrap();
            Ok::<_, Error>(IngressResponse::NoReply)
        }
    });
    let mut router = Router::new()
        .one_way(
            actor(7),
            "from",
            OneWayRouteOptions::new(allow, IngressLimits { max_body_bytes: 8 }),
            target.clone(),
        )
        .unwrap()
        .one_way(actor(7), "to", one_way_options(), target.clone())
        .unwrap()
        .one_way(actor(8), "from", one_way_options(), target.clone())
        .unwrap();
    assert!(matches!(
        router
            .call(
                Request::new(Body::empty())
                    .append_header(header::actor(actor(7)).unwrap())
                    .unwrap()
                    .append_header(header::action("from").unwrap())
                    .unwrap()
            )
            .await,
        Err(Error::InvalidInput(_))
    ));
    let foreign = Router::new()
        .one_way(actor(7), "from", one_way_options(), target)
        .unwrap();
    let mut stale_router = router.clone();
    let older = router.clone();
    let router = router
        .one_way(actor(9), "new", one_way_options(), Function(no_reply))
        .unwrap();
    let service = router.into_service().unwrap().map_service(move |original| {
        Function(move |mut request: Request<Body>| {
            let case = request.body().segments().next().unwrap()[0];
            let mut selected = if case == 4 {
                foreign.clone()
            } else if case == 5 {
                older.clone()
            } else {
                original.clone()
            };
            let observed = observed.clone();
            let retained = retained.clone();
            async move {
                if case == 7 {
                    retained.send(request).await.unwrap();
                    observed.send(Ok(())).await.unwrap();
                    return Ok(IngressResponse::NoReply);
                }
                match case {
                    1 => {
                        request.headers_mut().append(header::action("to")?)?;
                    }
                    2 => {
                        request.headers_mut().append(header::actor(actor(8))?)?;
                    }
                    3 => {
                        request.headers_mut().append(header::action("absent")?)?;
                    }
                    5 => {
                        request.headers_mut().append(header::action("from")?)?;
                    }
                    6 => {
                        request = request.map(|_| Body::from_bytes(vec![0; 9]).unwrap());
                    }
                    _ => {}
                }
                poll_fn(|cx| selected.poll_ready(cx)).await?;
                let result = selected.call(request).await;
                observed
                    .send(result.as_ref().map(|_| ()).map_err(Clone::clone))
                    .await
                    .unwrap();
                result
            }
        })
    });
    let running = Running::start(service, HandlerLimits::default()).await;
    let peer = running.channel.peer(deadline()).await.unwrap();
    for case in 0..8 {
        peer.send(routed(actor(7), "from", &[case]), deadline())
            .await
            .unwrap();
        let result = bounded(results.recv()).await.unwrap();
        match case {
            0 | 5 | 7 => result.unwrap(),
            6 => assert!(matches!(
                result,
                Err(Error::ResourceLimit("ingress body bytes"))
            )),
            _ => assert!(matches!(result, Err(Error::InvalidInput(_)))),
        }
    }
    assert_eq!(calls.try_recv().unwrap(), 0);
    assert_eq!(calls.try_recv().unwrap(), 5);
    assert!(calls.try_recv().is_err());
    let stale = bounded(retained_requests.recv()).await.unwrap();
    running.finish().await;
    assert!(matches!(stale_router.call(stale).await, Err(Error::Closed)));
}

#[tokio::test]
async fn whole_actor_services_share_admission_but_may_rewrite_actions_within_their_binding() {
    let (observed, mut results) = mpsc::channel(4);
    let service = IngressService::new(
        Function(move |mut request: Request<Body>| {
            let observed = observed.clone();
            async move {
                let requests = request.headers().effective().expects_reply;
                if action(request.headers()) == "retarget" {
                    request.headers_mut().append(header::actor(actor(9))?)?;
                }
                request.headers_mut().append(header::action("rewritten")?)?;
                let result = if requests {
                    server::Rpc::new(FubstCodec::<u32, u32>::default())
                        .unary(
                            Function(|request: Request<u32>| async move {
                                assert_eq!(action(request.headers()), "rewritten");
                                Ok::<_, Error>(Reply::new(request.into_body() + 1))
                            }),
                            request,
                        )
                        .await
                } else {
                    server::Rpc::new(FubstCodec::<(), u32>::default())
                        .one_way(
                            Function(|request: Request<u32>| async move {
                                assert_eq!(request.headers().effective().actor, actor(7));
                                assert_eq!(action(request.headers()), "rewritten");
                                Ok::<_, Error>(())
                            }),
                            request,
                        )
                        .await
                };
                observed
                    .send(result.as_ref().map(|_| ()).map_err(Clone::clone))
                    .await
                    .unwrap();
                result
            }
        }),
        vec![
            ServiceBinding::actor(
                actor(9),
                IngressOptions::requests(
                    RequestPolicy::new(allow, allow),
                    IngressLimits::default(),
                ),
            )
            .unwrap(),
            ServiceBinding::actor(
                actor(7),
                IngressOptions::one_way(allow, IngressLimits::default()),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let running = Running::start(service, HandlerLimits::default()).await;
    let peer = running.channel.peer(deadline()).await.unwrap();
    let body = fubst::serde::to_vec(&1u32).unwrap();
    peer.send(routed(actor(7), "any action", &body), deadline())
        .await
        .unwrap();
    bounded(results.recv()).await.unwrap().unwrap();
    peer.send(routed(actor(7), "retarget", &body), deadline())
        .await
        .unwrap();
    assert!(matches!(
        bounded(results.recv()).await.unwrap(),
        Err(Error::InvalidInput(_))
    ));
    let mut wrong_kind = Box::pin(
        peer.request(
            rpc_request(&body)
                .append_header(header::actor(actor(7)).unwrap())
                .unwrap(),
            deadline(),
        ),
    );
    assert!(wrong_kind.as_mut().now_or_never().is_none());
    let mut client = RpcClient::new(running.channel.clone());
    let reply = client
        .call(
            Request::new(41u32)
                .append_header(header::actor(actor(9)).unwrap())
                .unwrap(),
            FubstCodec::<u32, u32>::default(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(reply.body(), &42);
    bounded(results.recv()).await.unwrap().unwrap();
    assert!(results.try_recv().is_err());
    drop(wrong_kind);
    running.finish().await;
}

#[tokio::test]
async fn reverse_actor_routes_work_independently_of_managed_or_raw_id_ownership() {
    fn reverse_send(kind: SendKind, headers: &Headers, _: &Source) -> furpc::Result<()> {
        if kind == SendKind::OneWay
            && headers.effective().actor == actor(7)
            && action(headers) == "reverse"
        {
            Ok(())
        } else {
            Err(Error::InvalidInput("reverse sends only"))
        }
    }
    for options in [ConnectionOptions::managed(), ConnectionOptions::raw_only()] {
        let (client, accepted) = tokio::io::duplex(128);
        let (seen, mut arrived) = mpsc::channel(1);
        let router = Router::new()
            .one_way(
                actor(7),
                "reverse",
                one_way_options(),
                Function(move |request: Request<Body>| {
                    let seen = seen.clone();
                    async move {
                        assert!(request.context().unwrap().peer().is_none());
                        seen.send(request.into_body()).await.unwrap();
                        Ok::<_, Error>(IngressResponse::NoReply)
                    }
                }),
            )
            .unwrap();
        let (stop, stopped) = oneshot::channel();
        let serving = tokio::spawn(
            Server::builder()
                .on_peer_ready(
                    SendPolicy::new(reverse_send),
                    Function(|peer: Peer| async move {
                        peer.send(routed(actor(7), "reverse", b"accepted"), deadline())
                            .await?;
                        Ok::<_, Error>(())
                    }),
                )
                .unwrap()
                .build()
                .unwrap()
                .serve_with_shutdown(one(accepted), options, async {
                    stopped.await.unwrap();
                }),
        );
        let channel = bounded(
            Endpoint::from_shared("memory://reverse-routes")
                .unwrap()
                .connection_options(options)
                .service(router.into_service().unwrap())
                .unwrap()
                .connect_with_connector(Connector::new(vec![client])),
        )
        .await
        .unwrap();
        let body = bounded(arrived.recv()).await.unwrap();
        assert_eq!(body.segments().next().unwrap(), b"accepted");
        stop.send(()).unwrap();
        bounded(serving).await.unwrap().unwrap();
        drop(channel);
    }
}

#[derive(Clone, Copy)]
enum ReadyMode {
    Ready,
    Once,
    Pending,
    Error,
}
struct RouteReadiness {
    mode: ReadyMode,
    ready: bool,
    fail_call: bool,
}
impl Clone for RouteReadiness {
    fn clone(&self) -> Self {
        Self {
            mode: self.mode,
            ready: false,
            fail_call: self.fail_call,
        }
    }
}
impl Service<Request<Body>> for RouteReadiness {
    type Response = IngressResponse;
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<IngressResponse, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.mode {
            ReadyMode::Pending => Poll::Pending,
            ReadyMode::Once => {
                self.mode = ReadyMode::Ready;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            ReadyMode::Error => Poll::Ready(Err(std::io::Error::other("route readiness detail"))),
            ReadyMode::Ready => {
                self.ready = true;
                Poll::Ready(Ok(()))
            }
        }
    }
    fn call(&mut self, _: Request<Body>) -> Self::Future {
        assert!(
            std::mem::take(&mut self.ready),
            "selected service was cloned after readiness"
        );
        std::future::ready(if self.fail_call {
            Err(std::io::Error::other("route handler detail"))
        } else {
            Ok(IngressResponse::NoReply)
        })
    }
}

struct PendingMethod(Arc<Notify>);
impl Service<Request<u32>> for PendingMethod {
    type Response = ();
    type Error = Error;
    type Future = std::future::Ready<furpc::Result<()>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        self.0.notify_one();
        Poll::Pending
    }
    fn call(&mut self, _: Request<u32>) -> Self::Future {
        panic!("pending method must not be called")
    }
}

#[tokio::test(start_paused = true)]
async fn routed_readiness_reuses_method_waiters_and_keeps_same_instance_and_error_sources() {
    async fn result(
        results: &mut mpsc::Receiver<(String, furpc::Result<()>)>,
        expected: &str,
    ) -> furpc::Result<()> {
        loop {
            let (name, result) = bounded(results.recv()).await.unwrap();
            if name == expected {
                return result;
            }
            assert_eq!(name, "typed");
            assert!(matches!(
                result,
                Err(Error::Closed | Error::Timeout("handler"))
            ));
        }
    }
    for waiters in [0, 1] {
        let entered = Arc::new(Notify::new());
        let (observed, mut results) = mpsc::channel(16);
        let mut router = Router::new()
            .one_way(
                actor(7),
                "typed",
                one_way_options(),
                Function({
                    let entered = entered.clone();
                    move |request| {
                        let entered = entered.clone();
                        async move {
                            server::Rpc::new(FubstCodec::<(), u32>::default())
                                .one_way(PendingMethod(entered), request)
                                .await
                        }
                    }
                }),
            )
            .unwrap();
        for (name, mode, fail_call) in [
            ("pending", ReadyMode::Pending, false),
            ("ready", ReadyMode::Ready, false),
            ("once", ReadyMode::Once, false),
            ("error", ReadyMode::Error, false),
            ("call-error", ReadyMode::Ready, true),
        ] {
            router = router
                .one_way(
                    actor(7),
                    name,
                    one_way_options(),
                    RouteReadiness {
                        mode,
                        ready: false,
                        fail_call,
                    },
                )
                .unwrap();
        }
        let service = router.into_service().unwrap().map_service(move |router| {
            Function(move |request: Request<Body>| {
                let mut router = router.clone();
                let observed = observed.clone();
                async move {
                    let name = action(request.headers());
                    poll_fn(|cx| router.poll_ready(cx)).await?;
                    let result = router.call(request).await;
                    observed
                        .send((name, result.as_ref().map(|_| ()).map_err(Clone::clone)))
                        .await
                        .unwrap();
                    result
                }
            })
        });
        let running = Running::start(
            service,
            HandlerLimits {
                max_readiness_waiters: waiters,
                timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await;
        let peer = running.channel.peer(deadline()).await.unwrap();
        peer.send(
            routed(actor(7), "typed", &fubst::serde::to_vec(&1u32).unwrap()),
            deadline(),
        )
        .await
        .unwrap();
        bounded(entered.notified()).await;
        if waiters == 0 {
            assert_eq!(
                result(&mut results, "typed").await.unwrap_err(),
                Error::ResourceLimit("service readiness waiters")
            );
        } else {
            peer.send(routed(actor(7), "pending", b""), deadline())
                .await
                .unwrap();
            assert_eq!(
                result(&mut results, "pending").await.unwrap_err(),
                Error::ResourceLimit("service readiness waiters")
            );
        }
        peer.send(routed(actor(7), "ready", b""), deadline())
            .await
            .unwrap();
        result(&mut results, "ready").await.unwrap();
        for (name, phase, detail) in [
            ("error", "route readiness", "route readiness detail"),
            ("call-error", "route handler", "route handler detail"),
        ] {
            peer.send(routed(actor(7), name, b""), deadline())
                .await
                .unwrap();
            let error = result(&mut results, name).await.unwrap_err();
            assert!(matches!(&error, Error::Handler { phase: actual, .. } if *actual == phase));
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .to_string(),
                detail
            );
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        peer.send(routed(actor(7), "once", b""), deadline())
            .await
            .unwrap();
        let ready = result(&mut results, "once").await;
        if waiters == 0 {
            assert_eq!(
                ready.unwrap_err(),
                Error::ResourceLimit("service readiness waiters")
            );
        } else {
            ready.unwrap();
        }
        running.finish().await;
    }
}

#[tokio::test]
async fn routed_typed_replies_finish_during_drain_under_the_original_runner_ticket() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let router = Router::new()
        .request(
            actor(7),
            "rpc",
            request_options(),
            Function({
                let entered = entered.clone();
                let release = release.clone();
                move |request: Request<Body>| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        server::Rpc::new(FubstCodec::<u32, u32>::default())
                            .unary(
                                Function(move |request: Request<u32>| {
                                    let entered = entered.clone();
                                    let release = release.clone();
                                    async move {
                                        assert!(request.context().unwrap().peer().is_none());
                                        entered.notify_one();
                                        release.notified().await;
                                        Ok::<_, Error>(Reply::new(request.into_body()))
                                    }
                                }),
                                request,
                            )
                            .await
                    }
                }
            }),
        )
        .unwrap();
    let running = Running::start(router.into_service().unwrap(), HandlerLimits::default()).await;
    let mut client = RpcClient::new(running.channel.clone());
    let mut call = Box::pin(
        client.call(
            Request::new(42u32)
                .append_header(header::actor(actor(7)).unwrap())
                .unwrap()
                .append_header(header::action("rpc").unwrap())
                .unwrap(),
            FubstCodec::<u32, u32>::default(),
            deadline(),
        ),
    );
    assert!(call.as_mut().now_or_never().is_none());
    bounded(entered.notified()).await;
    running.stop.send(()).unwrap();
    bounded(running.shutting_down.notified()).await;
    assert!(!running.serving.is_finished());
    release.notify_one();
    assert_eq!(bounded(call).await.unwrap().body(), &42);
    bounded(running.serving).await.unwrap().unwrap();
}
