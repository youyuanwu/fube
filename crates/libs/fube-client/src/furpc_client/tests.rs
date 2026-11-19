use super::*;
use furpc::{
    IngressLimits, IngressOptions, IngressResponse, IngressService, Message, RequestPolicy, Server,
    ServiceBinding, TcpAcceptor,
};
use std::{
    cell::Cell,
    future::{ready, Future, Ready},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::oneshot;
use tower_service::Service;

fn deadline() -> Instant {
    Instant::now() + crate::DEFAULT_TIMEOUT
}

#[derive(Serialize)]
struct NodeInstance(crate::NodeId, u64);
#[derive(Serialize)]
struct Gateway(String, NodeInstance, String);
#[derive(Serialize)]
struct Ping {
    gateway: Gateway,
}
fn body(address: &str) -> Vec<u8> {
    fubst::serde::to_vec(&Ping {
        gateway: Gateway(
            address.into(),
            NodeInstance(crate::NodeId { high: 1, low: 2 }, 3),
            "N0010".into(),
        ),
    })
    .unwrap()
}

fn response(body: Body) -> Response<Body> {
    let mut response = Response::new(body);
    for record in [
        header::actor(ActorId::new(5).unwrap()).unwrap(),
        header::action("PingRequest").unwrap(),
        HeaderRecord::native(
            HeaderId::new(CLIENT_VERSION).unwrap(),
            &ProtocolVersion(1, 2),
        )
        .unwrap(),
    ] {
        response.headers_mut().append(record).unwrap();
    }
    response
}

struct Function<F> {
    f: F,
    ready: Cell<bool>,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

fn service<F>(f: F) -> Function<F> {
    Function {
        f,
        ready: Cell::new(false),
        delay: None,
    }
}

impl<F, Fut, E> Service<Request<Body>> for Function<F>
where
    F: FnMut(Request<Body>) -> Fut,
    Fut: Future<Output = std::result::Result<Response<Body>, E>>,
{
    type Response = Response<Body>;
    type Error = E;
    type Future = Fut;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), E>> {
        if let Some(delay) = &mut self.delay {
            std::task::ready!(delay.as_mut().poll(cx));
        }
        self.ready.set(true);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        assert!(
            self.ready.replace(false),
            "same-instance readiness required"
        );
        (self.f)(request)
    }
}

#[derive(Clone)]
struct Replies(Arc<AtomicUsize>);
impl Service<Request<Body>> for Replies {
    type Response = IngressResponse;
    type Error = furpc::Error;
    type Future = Ready<furpc::Result<IngressResponse>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        assert!(
            request.body().is_empty(),
            "native Ping has no serialized request object"
        );
        let headers = request.headers();
        assert_eq!(headers.effective().actor, ActorId::new(5).unwrap());
        assert!(headers
            .effective()
            .action()
            .iter()
            .copied()
            .eq("PingRequest".encode_utf16()));
        assert!(headers.effective().expects_reply);
        let activity = headers.first_raw(HeaderId::new(ACTIVITY).unwrap()).unwrap();
        let mut decoder = fubst::Decoder::new(activity.value());
        decoder
            .object(|decoder| {
                decoder.object(|decoder| {
                    assert_ne!(decoder.guid()?, [0; 16]);
                    assert_eq!(decoder.u64()?, 0);
                    Ok(())
                })
            })
            .unwrap();
        decoder.finish().unwrap();
        let timeout = headers.first_raw(HeaderId::new(TIMEOUT).unwrap()).unwrap();
        let mut decoder = fubst::Decoder::new(timeout.value());
        decoder
            .object(|decoder| {
                let ticks = decoder.i64()?;
                assert!(ticks > 0 && ticks <= 300_000_000);
                Ok(())
            })
            .unwrap();
        decoder.finish().unwrap();
        let ProtocolVersion(major, minor) = fubst::serde::from_slice(
            headers
                .first_raw(HeaderId::new(CLIENT_VERSION).unwrap())
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!((major, minor), (1, 1));

        let case = self.0.fetch_add(1, Ordering::SeqCst);
        let failure = matches!(case, 1 | 2);
        let mut reply = Message::builder()
            .append_header(
                header::actor(ActorId::new(if case == 3 { 6 } else { 5 }).unwrap()).unwrap(),
            )
            .append_header(
                header::action(if failure {
                    "ClientOperationFailure"
                } else if case == 4 {
                    "PingReply"
                } else {
                    "PingRequest"
                })
                .unwrap(),
            );
        if case != 9 {
            reply = reply.append_header(
                HeaderRecord::native(
                    HeaderId::new(CLIENT_VERSION).unwrap(),
                    &ProtocolVersion(if case == 5 { 2 } else { 1 }, 2),
                )
                .unwrap(),
            );
        }
        let payload = if failure {
            #[derive(Serialize)]
            struct Failure(i64, String);
            fubst::serde::to_vec(&Failure(if case == 2 { 0 } else { -123 }, "domain".into()))
                .unwrap()
        } else if case == 6 {
            vec![0xff]
        } else {
            body(if case == 7 { "" } else { "gateway:19000" })
        };
        if case == 8 {
            reply = reply.append_header(
                HeaderRecord::native(
                    header::FAULT,
                    &furpc::Fault {
                        code: -456,
                        has_body: false,
                    },
                )
                .unwrap(),
            );
        }
        ready(
            reply
                .body(Body::from_bytes(payload).unwrap())
                .build()
                .map(IngressResponse::Reply),
        )
    }
}

#[tokio::test]
async fn ping_preserves_error_categories_and_reuses_the_connection_after_bad_replies() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ingress = IngressService::new(
        Replies(calls.clone()),
        vec![ServiceBinding::raw(IngressOptions::requests(
            RequestPolicy::new(|_, _| Ok(()), |_, _| Ok(())),
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
            .service(ingress)
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(listener, furpc::ConnectionOptions::managed(), async {
                stopped.await.unwrap();
            }),
    );
    let mut client = FurpcClient::new(
        FurpcClient::endpoint_clear(address)
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    let now = Instant::now();
    for expired in [now, now - Duration::from_nanos(1)] {
        assert!(matches!(
            client.ping(Request::new(()), expired).await,
            Err(FurpcClientError::Native(furpc::Error::Timeout(_)))
        ));
    }
    if let Some(excessive) = now.checked_add(Duration::from_secs(i64::MAX as u64 / 10_000_000 + 1))
    {
        assert!(matches!(
            client.ping(Request::new(()), excessive).await,
            Err(FurpcClientError::Native(furpc::Error::InvalidInput(_)))
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for case in 0..11 {
        let result = client
            .ping(Request::new(()), deadline())
            .await
            .map(|response| {
                assert!(response.source().is_some());
                response.into_body()
            });
        match (case, result) {
            (0 | 10, Ok(gateway)) => {
                assert_eq!(gateway.address, "gateway:19000");
                assert_eq!(gateway.node_id, (1 << 64) | 2);
                assert_eq!(gateway.node_instance, 3);
                assert_eq!(gateway.node_name, "N0010");
            }
            (1, Err(FurpcClientError::Domain(failure))) => {
                assert_eq!(failure.code, -123);
                assert_eq!(failure.message, "domain");
            }
            (2 | 3 | 4 | 5 | 7 | 9, Err(FurpcClientError::Protocol(_))) => {}
            (6, Err(FurpcClientError::Decode(_))) => {}
            (8, Err(FurpcClientError::Native(furpc::Error::RemoteFault(fault)))) => {
                assert_eq!(fault.code(), -456);
            }
            (case, result) => panic!("case {case}: {result:?}"),
        }
    }
    let mut other = client.clone();
    let (a, b) = tokio::try_join!(
        client.ping(Request::new(()), deadline()),
        other.ping(Request::new(()), deadline())
    )
    .unwrap();
    assert_eq!(a.into_body(), b.into_body());
    assert_eq!(calls.load(Ordering::SeqCst), 13);
    drop(other);
    assert!(client.ping(Request::new(()), deadline()).await.is_ok());
    let mut raw = FurpcClient::new(
        FurpcClient::endpoint_clear(address)
            .unwrap()
            .connection_options(furpc::ConnectionOptions::raw_only())
            .connect()
            .await
            .unwrap(),
    );
    assert!(matches!(
        raw.ping(Request::new(()), deadline()).await,
        Err(FurpcClientError::Native(furpc::Error::Unsupported(_)))
    ));
    drop((raw, client));
    stop.send(()).unwrap();
    serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn segmented_ping_bodies_and_effective_version_keep_the_native_schema() {
    let encoded = body("native");
    let body = Body::from_segments(vec![
        Body::from_bytes(encoded[..7].to_vec()).unwrap(),
        Body::from_bytes(encoded[7..].to_vec()).unwrap(),
    ])
    .unwrap();
    let mut valid = Response::new(body);
    for record in [
        header::actor(ActorId::new(5).unwrap()).unwrap(),
        header::action("PingRequest").unwrap(),
        HeaderRecord::native(
            HeaderId::new(CLIENT_VERSION).unwrap(),
            &ProtocolVersion(2, 0),
        )
        .unwrap(),
        HeaderRecord::native(
            HeaderId::new(CLIENT_VERSION).unwrap(),
            &ProtocolVersion(1, 2),
        )
        .unwrap(),
    ] {
        valid.headers_mut().append(record).unwrap();
    }
    let mut malformed = response(Body::from_bytes(encoded).unwrap());
    malformed
        .headers_mut()
        .append(
            HeaderRecord::from_encoded(HeaderId::new(CLIENT_VERSION).unwrap(), vec![0xff]).unwrap(),
        )
        .unwrap();
    let mut replies = [valid, malformed].into_iter();
    let mut client = FurpcClient::new(service(move |_| {
        ready(Ok::<_, furpc::Error>(replies.next().unwrap()))
    }));
    assert_eq!(
        client
            .ping(Request::new(()), deadline())
            .await
            .unwrap()
            .body()
            .address,
        "native"
    );
    assert!(matches!(
        client.ping(Request::new(()), deadline()).await,
        Err(FurpcClientError::Decode(_))
    ));
}

#[tokio::test]
async fn generic_ping_is_lazy_borrows_a_send_only_service_and_preserves_metadata() {
    fn send<T: Send>(value: T) -> T {
        value
    }
    let address = String::from("borrowed");
    let calls = AtomicUsize::new(0);
    let effective = deadline();
    let extension = HeaderId::new(0x9000).unwrap();
    let transport = service(|request: Request<Body>| {
        calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.body().is_empty());
        assert_eq!(request.deadline(), Some(effective));
        assert_eq!(request.headers().first_raw(extension).unwrap().value(), [7]);
        let address = &address;
        async move {
            let mut response = response(Body::from_bytes(body(address)).unwrap());
            response
                .headers_mut()
                .append(HeaderRecord::from_encoded(extension, vec![8]).unwrap())
                .unwrap();
            Ok::<_, furpc::Error>(response)
        }
    });
    let mut client = FurpcClient::new(transport);
    drop(client.ping(Request::new(()), effective));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut request = Request::new(())
        .append_header(HeaderRecord::from_encoded(extension, vec![7]).unwrap())
        .unwrap();
    request.set_deadline(effective);
    let response = send(client.ping(request, effective + Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(response.body().address, address);
    assert!(response.source().is_none());
    assert_eq!(
        response.headers().first_raw(extension).unwrap().value(),
        [8]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn readiness_uses_the_same_deadline_and_timeout_header_contains_remaining_time() {
    let effective = Instant::now() + Duration::from_secs(5);
    let transport = service(|request: Request<Body>| {
        assert_eq!(request.deadline(), Some(effective));
        #[derive(Deserialize)]
        struct Timeout {
            ticks: i64,
        }
        let timeout: Timeout = fubst::serde::from_slice(
            request
                .headers()
                .first_raw(HeaderId::new(TIMEOUT).unwrap())
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(timeout.ticks, 30_000_000);
        ready(Ok::<_, furpc::Error>(response(
            Body::from_bytes(body("ready")).unwrap(),
        )))
    });
    let mut client = FurpcClient::new(Function {
        delay: Some(Box::pin(tokio::time::sleep(Duration::from_secs(2)))),
        ..transport
    });
    let mut request = Request::new(());
    request.set_deadline(effective);
    assert!(client
        .ping(request, effective + Duration::from_secs(30))
        .await
        .is_ok());

    let calls = AtomicUsize::new(0);
    let transport = service(|_: Request<Body>| {
        calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok::<_, furpc::Error>(response(Body::empty())))
    });
    let mut client = FurpcClient::new(Function {
        delay: Some(Box::pin(tokio::time::sleep(Duration::from_secs(6)))),
        ..transport
    });
    assert!(matches!(
        client
            .ping(Request::new(()), Instant::now() + Duration::from_secs(5))
            .await,
        Err(FurpcClientError::Native(furpc::Error::Timeout(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_transport_errors_keep_the_original_source() {
    use std::error::Error;

    let mut client = FurpcClient::new(service(|_: Request<Body>| {
        ready(Err::<Response<Body>, _>(std::io::Error::other(
            "custom transport",
        )))
    }));
    let error = client.ping(Request::new(()), deadline()).await.unwrap_err();
    let FurpcClientError::Native(furpc::Error::Transport { .. }) = &error else {
        panic!("lost transport error: {error:?}");
    };
    let original = error
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(original.to_string(), "custom transport");
}
