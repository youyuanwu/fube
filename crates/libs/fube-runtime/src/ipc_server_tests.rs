use super::*;
use crate::ipc_client::{deadline_after, IpcClient};
use fube_rap_test_support::transport::{message, ModelPeer};
use fubst::serde::from_slice;
use std::{
    future::{ready, Ready},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

async fn rpc_pair(handoff: ControlHandoff) -> (IpcClient, ModelPeer) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, peer) = tokio::join!(
        IpcClient::connect(
            address,
            deadline_after(Duration::from_secs(2)).unwrap(),
            "typed-host",
            RuntimeServer::new(
                handoff,
                Arc::new(|_| Err(Error::Protocol("unexpected test RAP call"))),
            ),
            None,
        ),
        ModelPeer::accept(listener),
    );
    (client.unwrap(), peer.unwrap())
}

fn capture(send: tokio::sync::mpsc::Sender<ControlCall>) -> ControlHandoff {
    Arc::new(move |incoming| {
        send.try_send(incoming).map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => Error::ResourceLimit("test ingress"),
            tokio::sync::mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    })
}

struct Handler<F>(F);

impl<Q, R, F: FnMut(Request<Q>) -> Result<R>> Service<Request<Q>> for Handler<F> {
    type Response = R;
    type Error = Error;
    type Future = Ready<Result<R>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Q>) -> Self::Future {
        ready((self.0)(request))
    }
}

#[tokio::test]
async fn typed_host_service_uses_fubst_encoding_and_original_runner_correlation() {
    let (send, mut received) = tokio::sync::mpsc::channel(1);
    let (client, mut peer) = rpc_pair(capture(send)).await;
    peer.request(message(8, "InvalidateLeaseRequest", vec![]).unwrap())
        .unwrap();
    let ControlCall::InvalidateLease(call) = received.recv().await.unwrap() else {
        panic!("lease invalidation expected");
    };
    let request_id = call.request.headers().effective().message_id;
    call.unary(Handler(move |request: Request<()>| {
        assert_eq!(request.headers().effective().message_id, request_id);
        assert!(request.context().is_some());
        Ok(Reply::new(wire::ErrorReply { error: -123 }))
    }))
    .await
    .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), peer.receive())
        .await
        .unwrap()
        .unwrap();
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.actor, 8);
    assert!(metadata.action.is_empty());
    assert_eq!(metadata.relates_to, request_id);
    assert_eq!(
        from_slice::<wire::ErrorReply>(&response.body)
            .unwrap()
            .error,
        -123
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn retired_invocation_cannot_call_a_retained_typed_service() {
    let (send, mut received) = tokio::sync::mpsc::channel(1);
    let (client, mut peer) = rpc_pair(capture(send)).await;
    peer.request(message(8, "InvalidateLeaseRequest", vec![]).unwrap())
        .unwrap();
    let ControlCall::InvalidateLease(call) = received.recv().await.unwrap() else {
        panic!("lease invalidation expected");
    };
    client.abort();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !call.response.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let error = call
        .unary(Handler(move |_: Request<()>| {
            observed.store(true, Ordering::SeqCst);
            Ok(Reply::new(wire::ErrorReply { error: 0 }))
        }))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Closed));
    assert!(!called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn malformed_update_is_deferred_until_execution_and_never_calls_typed_service() {
    let (send, mut received) = tokio::sync::mpsc::channel(1);
    let (client, mut peer) = rpc_pair(capture(send)).await;
    peer.request(message(8, "UpdateCodePackageContextRequest", vec![3, 1, 4]).unwrap())
        .unwrap();
    let ControlCall::UpdateContext(call) = received.recv().await.unwrap() else {
        panic!("context update expected");
    };
    assert!(!client.is_closed());
    let called = Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let error = call
        .unary(Handler(move |_: Request<wire::UpdateContext>| {
            observed.store(true, Ordering::SeqCst);
            Ok(Reply::new(wire::ErrorReply { error: 0 }))
        }))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Protocol(_) | Error::InvalidInput(_)));
    assert!(!called.load(Ordering::SeqCst));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), client.closed())
            .await
            .unwrap(),
        Error::Protocol(_) | Error::InvalidInput(_),
    ));
}

#[tokio::test]
async fn typed_handler_failure_retains_the_original_runtime_error() {
    let (send, mut received) = tokio::sync::mpsc::channel(1);
    let (client, mut peer) = rpc_pair(capture(send)).await;
    peer.request(message(8, "InvalidateLeaseRequest", vec![]).unwrap())
        .unwrap();
    let ControlCall::InvalidateLease(call) = received.recv().await.unwrap() else {
        panic!("lease invalidation expected");
    };
    let error = call
        .unary(Handler(|_: Request<()>| {
            Err::<Reply<wire::ErrorReply>, _>(Error::Protocol("typed handler refused"))
        }))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Protocol("typed handler refused")));
    let error = tokio::time::timeout(Duration::from_secs(2), client.closed())
        .await
        .unwrap();
    assert!(matches!(error, Error::Protocol("typed handler refused")));
}

#[tokio::test]
async fn bounded_handoff_failure_retires_the_original_session() {
    let (client, mut peer) = rpc_pair(Arc::new(|_| {
        Err(Error::ResourceLimit("typed handoff refused"))
    }))
    .await;
    peer.request(message(8, "InvalidateLeaseRequest", vec![]).unwrap())
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), client.closed())
        .await
        .unwrap();
    assert!(matches!(
        error,
        Error::ResourceLimit("typed handoff refused")
    ));
}
