use super::*;
use furpc::{
    IngressLimits, IngressOptions, IngressResponse, IngressService, Message, RequestPolicy, Server,
    ServiceBinding, TcpAcceptor,
};
use std::{
    future::{ready, Ready},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tower_service::Service;

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
    let client = FurpcClient::new(
        FurpcClient::endpoint_clear(address)
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    for timeout in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
        assert!(matches!(
            client.ping(Some(timeout)).await,
            Err(FurpcClientError::Native(furpc::Error::InvalidInput(_)))
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for case in 0..11 {
        let result = client.ping(None).await;
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
    let other = client.clone();
    let (a, b) = tokio::try_join!(client.ping(None), other.ping(None)).unwrap();
    assert_eq!(a, b);
    assert_eq!(calls.load(Ordering::SeqCst), 13);
    drop(other);
    assert!(client.ping(None).await.is_ok());
    stop.send(()).unwrap();
    serving.await.unwrap().unwrap();
}

#[test]
fn segmented_ping_bodies_and_effective_version_keep_the_native_schema() {
    let encoded = body("native");
    let body = Body::from_segments(vec![
        Body::from_bytes(encoded[..7].to_vec()).unwrap(),
        Body::from_bytes(encoded[7..].to_vec()).unwrap(),
    ])
    .unwrap();
    let mut response = Response::new(body);
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
        response.headers_mut().append(record).unwrap();
    }
    assert_eq!(decode_ping(&response).unwrap().address, "native");
    response
        .headers_mut()
        .append(
            HeaderRecord::from_encoded(HeaderId::new(CLIENT_VERSION).unwrap(), vec![0xff]).unwrap(),
        )
        .unwrap();
    assert!(matches!(
        decode_ping(&response),
        Err(FurpcClientError::Decode(_))
    ));
}
