use super::*;
use furpc::{
    server, ActorId, HeaderRecord, NotificationPolicy, NotificationRouteOptions, Reply,
    RequestRouteOptions, Router, ServerBuilder,
};

fn policy() -> NotificationPolicy {
    NotificationPolicy::new(allow, allow)
}
fn endpoint() -> Endpoint {
    Endpoint::from_shared("memory://notifications").unwrap()
}
fn sends(_: SendKind, _: &Headers, _: &Source) -> furpc::Result<()> {
    Ok(())
}
fn notification_ingress<S>(service: S) -> IngressService<S>
where
    S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    IngressService::new(
        service,
        vec![ServiceBinding::raw(IngressOptions::notifications(
            policy(),
            IngressLimits::default(),
        ))
        .unwrap()],
    )
    .unwrap()
}
fn ack(request: Request<Body>) -> std::future::Ready<furpc::Result<IngressResponse>> {
    std::future::ready(
        Message::builder()
            .body(request.into_body())
            .build()
            .map(IngressResponse::Reply),
    )
}
fn routed<B>(body: B) -> Request<B> {
    Request::new(body)
        .append_header(header::actor(ActorId::new(70_000).unwrap()).unwrap())
        .unwrap()
        .append_header(header::action("Changed").unwrap())
        .unwrap()
}
fn bytes(body: &Body) -> Vec<u8> {
    body.segments().flatten().copied().collect()
}
fn short_deadline() -> Instant {
    Instant::now() + Duration::from_millis(30)
}

struct Link {
    channel: furpc::Channel,
    accepted: Peer,
    stop: oneshot::Sender<()>,
    shutting_down: Arc<Notify>,
    serving: tokio::task::JoinHandle<furpc::Result<()>>,
}
impl Link {
    async fn start(server: ServerBuilder, endpoint: Endpoint, policy: SendPolicy) -> Self {
        let (client, accepted) = tokio::io::duplex(128);
        Self::with_io(server, endpoint, policy, client, accepted).await
    }

    async fn with_io<I: TransportIo, J: TransportIo>(
        server: ServerBuilder,
        endpoint: Endpoint,
        policy: SendPolicy,
        client: I,
        accepted: J,
    ) -> Self {
        let (peers, mut ready) = mpsc::channel(1);
        let server = server
            .on_peer_ready(
                policy,
                Function(move |peer| {
                    let peers = peers.clone();
                    async move {
                        peers.send(peer).await.unwrap();
                        Ok::<_, Error>(())
                    }
                }),
            )
            .unwrap()
            .build()
            .unwrap();
        let (stop, stopped) = oneshot::channel();
        let shutting_down = Arc::new(Notify::new());
        let shutdown = shutting_down.clone();
        let serving = tokio::spawn(server.serve_with_shutdown(
            one(accepted),
            ConnectionOptions::managed(),
            async move {
                stopped.await.unwrap();
                shutdown.notify_one();
            },
        ));
        let mut io = Some(client);
        let channel = bounded(endpoint.connect_with_connector(Function(move |_: Uri| {
            std::future::ready(
                io.take()
                    .map(|io| Connected::new(io, IoInfo::unknown()))
                    .ok_or_else(|| std::io::Error::other("no replacement I/O")),
            )
        })))
        .await
        .unwrap();
        let accepted = bounded(ready.recv()).await.unwrap();
        Self {
            channel,
            accepted,
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

#[tokio::test]
async fn notifications_return_typed_data_in_both_directions_and_share_keys_with_requests() {
    fn ack_envelope(headers: &Headers, _: &Source) -> furpc::Result<()> {
        if action(headers) == "Acknowledged"
            && !headers.effective().uncorrelated_reply
            && !headers.effective().expects_reply
            && headers.effective().relates_to != furpc::MessageId::default()
        {
            Ok(())
        } else {
            Err(Error::InvalidInput("acknowledgment envelope"))
        }
    }
    fn notification_sends(kind: SendKind, _: &Headers, _: &Source) -> furpc::Result<()> {
        if kind == SendKind::Notification {
            Ok(())
        } else {
            Err(Error::InvalidInput("not a notification"))
        }
    }
    let policy = NotificationPolicy::new(allow, ack_envelope);
    let router = Router::new()
        .request(
            ActorId::new(70_000).unwrap(),
            "Changed",
            RequestRouteOptions::new(RequestPolicy::new(allow, allow), IngressLimits::default()),
            Function(|_: Request<Body>| async {
                Ok::<_, Error>(IngressResponse::Reply(packet("ordinary", b"ordinary")))
            }),
        )
        .unwrap()
        .notification(
            ActorId::new(70_000).unwrap(),
            "Changed",
            NotificationRouteOptions::new(policy, IngressLimits::default()),
            Function(|mut request: Request<Body>| async move {
                let original = request.headers().effective().message_id;
                assert_ne!(original, furpc::MessageId::default());
                assert!(!request.headers().effective().uncorrelated_reply);
                assert!(request.headers().effective().expects_reply);
                assert!(request.context().unwrap().peer().is_none());
                request.headers_mut().remove_all(header::MESSAGE_ID)?;
                request.headers_mut().append(HeaderRecord::native(
                    header::MESSAGE_ID,
                    &furpc::MessageId {
                        guid: [9; 16],
                        index: 99,
                    },
                )?)?;
                server::Rpc::new(FubstCodec::<String, u32>::default())
                    .unary(
                        Function(|request: Request<u32>| async move {
                            Reply::new(format!("ack {}", request.into_body()))
                                .append_header(header::action("Acknowledged")?)
                        }),
                        request,
                    )
                    .await
            }),
        )
        .unwrap();
    let ingress = router.into_service().unwrap().map_service(|router| {
        Function(move |mut request: Request<Body>| {
            let mut router = router.clone();
            if request.headers().effective().uncorrelated_reply {
                // Middleware cannot move notifications into the ordinary route namespace.
                request
                    .headers_mut()
                    .remove_all(header::UNCORRELATED_REPLY)
                    .unwrap();
            }
            async move { router.call(request).await }
        })
    });
    let link = Link::start(
        Server::builder().service(ingress.clone()).unwrap(),
        endpoint().service(ingress).unwrap(),
        SendPolicy::new(notification_sends),
    )
    .await;
    let peer = link.channel.peer(deadline()).await.unwrap();
    let ordinary = peer
        .request(routed(Body::empty()), deadline())
        .await
        .unwrap();
    assert_eq!(bytes(ordinary.body()), b"ordinary");

    for target in [peer, link.accepted.clone()] {
        let generation = target.info().generation;
        let mut client = RpcClient::new(Function(move |request: Request<Body>| {
            let target = target.clone();
            async move {
                let deadline = request
                    .deadline()
                    .ok_or(Error::InvalidInput("notification requires a deadline"))?;
                target.notify(request, policy, deadline).await
            }
        }));
        let response = client
            .call(
                routed(42u32),
                FubstCodec::<u32, String>::default(),
                deadline(),
            )
            .await
            .unwrap();
        assert_eq!(response.body(), "ack 42");
        assert_eq!(response.source().unwrap().generation(), generation);
        assert_ne!(response.headers().effective().relates_to.guid, [9; 16]);
        assert_eq!(action(response.headers()), "Acknowledged");
    }
    link.finish().await;
}

#[test]
fn binding_and_route_conflicts_are_class_local() {
    let ordinary = || IngressOptions::one_way(allow, IngressLimits::default());
    let notifications = || IngressOptions::notifications(policy(), IngressLimits::default());
    for bindings in [
        vec![
            ServiceBinding::raw(ordinary()).unwrap(),
            ServiceBinding::raw(notifications()).unwrap(),
        ],
        vec![
            ServiceBinding::raw(ordinary()).unwrap(),
            ServiceBinding::actor(ActorId::new(7).unwrap(), notifications()).unwrap(),
        ],
        vec![
            ServiceBinding::actor(ActorId::new(7).unwrap(), ordinary()).unwrap(),
            ServiceBinding::raw(notifications()).unwrap(),
        ],
    ] {
        assert!(IngressService::new(Function(ack), bindings).is_ok());
    }
    for bindings in [
        vec![
            ServiceBinding::raw(notifications()).unwrap(),
            ServiceBinding::raw(notifications()).unwrap(),
        ],
        vec![
            ServiceBinding::raw(notifications()).unwrap(),
            ServiceBinding::actor(ActorId::new(7).unwrap(), notifications()).unwrap(),
        ],
        vec![
            ServiceBinding::actor(ActorId::new(7).unwrap(), notifications()).unwrap(),
            ServiceBinding::actor(ActorId::new(7).unwrap(), notifications()).unwrap(),
        ],
    ] {
        assert!(IngressService::new(Function(ack), bindings).is_err());
    }
    let router = Router::new()
        .notification(
            ActorId::new(7).unwrap(),
            "x",
            NotificationRouteOptions::new(policy(), IngressLimits::default()),
            Function(ack),
        )
        .unwrap();
    assert!(router
        .notification(
            ActorId::new(7).unwrap(),
            "x",
            NotificationRouteOptions::new(policy(), IngressLimits::default()),
            Function(ack),
        )
        .is_err());
}

#[tokio::test]
async fn notification_misses_never_fall_back_to_ordinary_raw_and_raw_only_can_ack() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = IngressService::new(
        Function({
            let calls = calls.clone();
            move |request: Request<Body>| {
                calls.fetch_add(1, Ordering::SeqCst);
                ack(request)
            }
        }),
        vec![
            ServiceBinding::raw(IngressOptions::requests(
                RequestPolicy::new(allow, allow),
                IngressLimits::default(),
            ))
            .unwrap(),
            ServiceBinding::actor(
                ActorId::new(70_000).unwrap(),
                IngressOptions::notifications(policy(), IngressLimits { max_body_bytes: 2 }),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let link = Link::start(
        Server::builder(),
        endpoint()
            .connection_options(ConnectionOptions::raw_only())
            .service(service)
            .unwrap(),
        SendPolicy::new(sends),
    )
    .await;
    let peer = link.channel.peer(deadline()).await.unwrap();
    assert!(matches!(
        peer.notify(Request::new(Body::empty()), policy(), deadline())
            .await,
        Err(Error::Unsupported(_))
    ));
    for request in [
        Request::new(Body::empty()),
        routed(Body::from_bytes(vec![0; 3]).unwrap()),
    ] {
        let mut missed = Box::pin(link.accepted.notify(request, policy(), deadline()));
        assert!(missed.as_mut().now_or_never().is_none());
        let response = link
            .accepted
            .request(rpc_request(b"barrier"), deadline())
            .await
            .unwrap();
        assert_eq!(bytes(response.body()), b"barrier");
        drop(missed);
    }
    let response = link
        .accepted
        .notify(
            routed(Body::from_bytes(vec![4, 5]).unwrap()),
            policy(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(bytes(response.body()), [4, 5]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    link.finish().await;
}

#[tokio::test(start_paused = true)]
async fn failures_and_missing_acknowledgments_do_not_fabricate_success_or_faults() {
    let service = notification_ingress(Function(|request: Request<Body>| async move {
        match bytes(request.body())[0] {
            0 => Ok(IngressResponse::NoReply),
            1 => Err(Error::InvalidInput("application failure")),
            2 => Ok(IngressResponse::Reply(
                Message::builder()
                    .append_header(HeaderRecord::native(
                        header::RELATES_TO,
                        &furpc::MessageId {
                            guid: [9; 16],
                            index: 9,
                        },
                    )?)
                    .build()?,
            )),
            3 => Ok(IngressResponse::Reply(
                Message::builder()
                    .body(Body::from_bytes(vec![0; 129])?)
                    .build()?,
            )),
            4 => Ok(IngressResponse::Reply(
                Message::builder()
                    .append_header(HeaderRecord::native(
                        header::FAULT,
                        &furpc::Fault {
                            code: 11,
                            has_body: true,
                        },
                    )?)
                    .body(Body::from_bytes(fault_body("notification failure"))?)
                    .build()?,
            )),
            6 => {
                let result = server::Rpc::new(FubstCodec::<u32, u32>::default())
                    .one_way(
                        Function(|_: Request<u32>| {
                            std::future::ready(Err::<(), _>(Error::Internal(
                                "notification invoked a one-way typed handler",
                            )))
                        }),
                        request,
                    )
                    .await;
                assert!(matches!(
                    result,
                    Err(Error::InvalidInput("typed ingress exchange kind mismatch"))
                ));
                result
            }
            _ => ack(request).await,
        }
    }));
    let link = Link::start(
        Server::builder()
            .handler_limits(HandlerLimits {
                max_encoded_body_bytes_per_response: 128,
                ..Default::default()
            })
            .service(service)
            .unwrap(),
        endpoint(),
        SendPolicy::new(sends),
    )
    .await;
    let peer = link.channel.peer(deadline()).await.unwrap();
    for case in [0, 1, 2, 3, 6] {
        assert!(matches!(
            peer.notify(rpc_request(&[case]), policy(), short_deadline())
                .await,
            Err(Error::Timeout(_))
        ));
    }
    let Error::RemoteFault(fault) = peer
        .notify(rpc_request(&[4]), policy(), deadline())
        .await
        .unwrap_err()
    else {
        panic!("native Fault was not preserved");
    };
    assert_eq!(fault.header_code(), 11);
    assert_eq!(fault.code(), 33);
    assert_eq!(
        String::from_utf16(fault.message()).unwrap(),
        "notification failure"
    );
    assert_eq!(
        bytes(
            peer.notify(rpc_request(&[5]), policy(), deadline())
                .await
                .unwrap()
                .body()
        ),
        [5]
    );
    link.finish().await;
}

#[tokio::test(start_paused = true)]
async fn notification_and_acknowledgment_policies_apply_on_both_ends() {
    fn notification(headers: &Headers, _: &Source) -> furpc::Result<()> {
        if action(headers) == "denied" {
            Err(Error::InvalidInput("notification rejected"))
        } else {
            Ok(())
        }
    }
    fn acknowledgment(headers: &Headers, _: &Source) -> furpc::Result<()> {
        if action(headers) == "ack" {
            Ok(())
        } else {
            Err(Error::InvalidInput("acknowledgment rejected"))
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let service = IngressService::new(
        Function({
            let calls = calls.clone();
            move |request: Request<Body>| {
                calls.fetch_add(1, Ordering::SeqCst);
                let name = action(request.headers());
                async move { Ok::<_, Error>(IngressResponse::Reply(packet(&name, b"ack data"))) }
            }
        }),
        vec![ServiceBinding::raw(IngressOptions::notifications(
            NotificationPolicy::new(notification, acknowledgment),
            IngressLimits::default(),
        ))
        .unwrap()],
    )
    .unwrap();
    let client_service = notification_ingress(Function(ack));
    let link = Link::start(
        Server::builder().service(service).unwrap(),
        endpoint().service(client_service).unwrap(),
        SendPolicy::new(control_sends),
    )
    .await;
    let peer = link.channel.peer(deadline()).await.unwrap();
    let request = |name| {
        Request::new(Body::empty())
            .append_header(header::action(name).unwrap())
            .unwrap()
    };
    assert!(matches!(
        peer.notify(
            request("denied"),
            NotificationPolicy::new(notification, allow),
            deadline()
        )
        .await,
        Err(Error::InvalidInput("notification rejected"))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // Inbound rejection, then a handler response rejected by its ACK policy.
    for name in ["denied", "not-an-ack"] {
        assert!(matches!(
            peer.notify(request(name), policy(), short_deadline()).await,
            Err(Error::Timeout(_))
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // A valid server ACK can still fail the caller's independent ACK check.
    peer.notify(
        request("ack"),
        NotificationPolicy::new(allow, notification),
        deadline(),
    )
    .await
    .unwrap();
    fn deny(_: &Headers, _: &Source) -> furpc::Result<()> {
        Err(Error::InvalidInput("caller rejects acknowledgment"))
    }
    assert!(matches!(
        peer.notify(
            request("ack"),
            NotificationPolicy::new(allow, deny),
            short_deadline()
        )
        .await,
        Err(Error::Timeout(_))
    ));
    // The notify policy must not widen a Peer's captured send restriction.
    assert!(matches!(
        link.accepted
            .notify(request("ack"), policy(), deadline())
            .await,
        Err(Error::InvalidInput("restricted send"))
    ));
    assert_eq!(
        bytes(
            peer.notify(request("ack"), policy(), deadline())
                .await
                .unwrap()
                .body()
        ),
        b"ack data"
    );
    link.finish().await;
}

#[tokio::test]
async fn notifications_share_pending_capacity_and_cancel_without_replay_or_late_retargeting() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let service = notification_ingress(Function({
        let entered = entered.clone();
        let release = release.clone();
        move |request: Request<Body>| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                if bytes(request.body()) == [1] {
                    entered.notify_one();
                    release.notified().await;
                }
                ack(request).await
            }
        }
    }));
    let link = Link::start(
        Server::builder().service(service).unwrap(),
        endpoint().limits(TransportLimits {
            max_pending_requests: 1,
            ..Default::default()
        }),
        SendPolicy::new(sends),
    )
    .await;
    let peer = link.channel.peer(deadline()).await.unwrap();
    let unpolled = peer.notify(rpc_request(&[1]), policy(), deadline());
    assert_eq!(
        unpolled.attempt().cancel_uncommitted(),
        CancelOutcome::Retracted
    );
    assert!(matches!(unpolled.await, Err(Error::Cancelled)));
    let mut slow = Box::pin(peer.notify(rpc_request(&[1]), policy(), deadline()));
    assert!(slow.as_mut().now_or_never().is_none());
    bounded(entered.notified()).await;
    assert!(matches!(
        peer.request(rpc_request(b"no separate pool"), deadline())
            .await,
        Err(Error::ResourceLimit("pending RPC/results"))
    ));
    let attempt = slow.attempt();
    drop(slow);
    assert_eq!(attempt.cancel_uncommitted(), CancelOutcome::Settled);
    assert_eq!(
        bytes(
            peer.notify(rpc_request(&[2]), policy(), deadline())
                .await
                .unwrap()
                .body()
        ),
        [2]
    );
    release.notify_one();
    assert_eq!(
        bytes(
            peer.notify(rpc_request(&[3]), policy(), deadline())
                .await
                .unwrap()
                .body()
        ),
        [3]
    );
    let mut expired = Request::new(Body::empty());
    expired.set_deadline(Instant::now());
    assert!(matches!(
        peer.notify(expired, policy(), deadline()).await,
        Err(Error::Timeout(_))
    ));
    #[derive(serde::Serialize)]
    struct Flag {
        value: bool,
    }
    for id in [
        header::MESSAGE_ID,
        header::RELATES_TO,
        header::EXPECTS_REPLY,
        header::UNCORRELATED_REPLY,
    ] {
        let record = if id == header::MESSAGE_ID || id == header::RELATES_TO {
            HeaderRecord::native(
                id,
                &furpc::MessageId {
                    guid: [1; 16],
                    index: 1,
                },
            )
            .unwrap()
        } else {
            HeaderRecord::native(id, &Flag { value: false }).unwrap()
        };
        assert!(matches!(
            peer.notify(
                rpc_request(&[4]).append_header(record.clone()).unwrap(),
                policy(),
                deadline()
            )
            .await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            peer.send(
                Message::builder().append_header(record).build().unwrap(),
                deadline()
            )
            .await,
            Err(Error::InvalidInput(_))
        ));
    }
    link.stop.send(()).unwrap();
    bounded(link.serving).await.unwrap().unwrap();
    assert!(matches!(
        link.accepted
            .notify(rpc_request(&[4]), policy(), deadline())
            .await,
        Err(Error::Closed)
    ));
}

#[tokio::test(start_paused = true)]
async fn notification_and_ack_checks_are_repeated_at_the_first_positive_write() {
    use std::sync::atomic::AtomicBool;
    static ALLOWED: AtomicBool = AtomicBool::new(true);
    fn check(_: &Headers, _: &Source) -> furpc::Result<()> {
        if ALLOWED.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::InvalidInput("commit policy"))
        }
    }
    for block_ack in [false, true] {
        ALLOWED.store(true, Ordering::SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let policy = NotificationPolicy::new(check, check);
        let service = IngressService::new(
            Function({
                let calls = calls.clone();
                move |request| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    ack(request)
                }
            }),
            vec![ServiceBinding::raw(IngressOptions::notifications(
                policy,
                IngressLimits::default(),
            ))
            .unwrap()],
        )
        .unwrap();
        let (client, accepted) = tokio::io::duplex(128);
        let gate = Arc::new(WriteGate::default());
        let (client_gate, accepted_gate) = if block_ack {
            (Arc::new(WriteGate::default()), gate.clone())
        } else {
            (gate.clone(), Arc::new(WriteGate::default()))
        };
        let link = Link::with_io(
            Server::builder().service(service).unwrap(),
            endpoint(),
            SendPolicy::new(sends),
            GuardedIo {
                io: client,
                gate: client_gate,
                dropped: None,
            },
            GuardedIo {
                io: accepted,
                gate: accepted_gate,
                dropped: None,
            },
        )
        .await;
        gate.limit(Some(0));
        let peer = link.channel.peer(deadline()).await.unwrap();
        let mut operation = Box::pin(peer.notify(rpc_request(b"data"), policy, short_deadline()));
        assert!(operation.as_mut().now_or_never().is_none());
        bounded(gate.blocked.notified()).await;
        ALLOWED.store(false, Ordering::SeqCst);
        gate.limit(None);
        let error = operation.await.unwrap_err();
        if block_ack {
            assert!(matches!(error, Error::Timeout(_)));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            assert!(matches!(error, Error::InvalidInput("commit policy")));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
        ALLOWED.store(true, Ordering::SeqCst);
        assert_eq!(
            bytes(
                peer.notify(rpc_request(b"next"), policy, deadline())
                    .await
                    .unwrap()
                    .body()
            ),
            b"next"
        );
        link.finish().await;
    }
}

#[tokio::test]
async fn admitted_notification_data_can_complete_during_graceful_drain() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let service = notification_ingress(Function({
        let entered = entered.clone();
        let release = release.clone();
        move |request: Request<Body>| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                server::Rpc::new(FubstCodec::<u32, u32>::default())
                    .unary(
                        Function(|request: Request<u32>| async move {
                            Ok::<_, Error>(Reply::new(request.into_body() + 1))
                        }),
                        request,
                    )
                    .await
            }
        }
    }));
    let link = Link::start(
        Server::builder().service(service).unwrap(),
        endpoint(),
        SendPolicy::new(sends),
    )
    .await;
    let peer = link.channel.peer(deadline()).await.unwrap();
    let payload = fubst::serde::to_vec(&41u32).unwrap();
    let mut notification = Box::pin(peer.notify(rpc_request(&payload), policy(), deadline()));
    assert!(notification.as_mut().now_or_never().is_none());
    bounded(entered.notified()).await;
    link.stop.send(()).unwrap();
    bounded(link.shutting_down.notified()).await;
    assert!(matches!(
        link.accepted
            .notify(Request::new(Body::empty()), policy(), deadline())
            .await,
        Err(Error::Closed)
    ));
    assert!(!link.serving.is_finished());
    release.notify_one();
    let response = bounded(notification).await.unwrap();
    assert_eq!(
        fubst::serde::from_slice::<u32>(&bytes(response.body())).unwrap(),
        42
    );
    bounded(link.serving).await.unwrap().unwrap();
}
