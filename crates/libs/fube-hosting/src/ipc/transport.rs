//! Accepted I/O and inline furpc serving; protocol policy lives in the facades.
use super::{
    client::LifecycleClient,
    server::{HostingServer, Incoming, HANDLERS},
    IO_DEADLINE, MAX_INPUT,
};
use furpc::{
    Accept, Accepted, Error, HandlerLimits, IoInfo, Peer, Result, Server, TransportLimits,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, watch},
};
use tower_service::Service;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub(super) struct Connection {
    pub serving: BoxFuture<Result<()>>,
    pub incoming: mpsc::Receiver<Incoming>,
    pub ready: oneshot::Receiver<Peer>,
    pub failure: watch::Receiver<Option<Error>>,
}

pub(super) fn accepted(socket: TcpStream) -> Result<Connection> {
    let (send, incoming) = mpsc::channel(HANDLERS);
    let (failure, observed) = watch::channel(None);
    let ingress = HostingServer::new(send, failure).ingress()?;
    let (ready, wait) = oneshot::channel();
    let server = Server::builder()
        .limits(TransportLimits {
            max_receive_frame_bytes: MAX_INPUT,
            max_send_frame_bytes: Some(MAX_INPUT),
            send_queue_bytes: 8 * MAX_INPUT,
            max_pending_sends: 8,
            max_pending_requests: 1,
            max_connections: 1,
            max_handshakes: 1,
            frame_timeout: IO_DEADLINE,
            write_timeout: IO_DEADLINE,
            ..TransportLimits::default()
        })
        .handler_limits(HandlerLimits {
            max_in_flight: HANDLERS,
            max_readiness_waiters: HANDLERS,
            max_encoded_body_bytes_per_response: MAX_INPUT,
            timeout: Duration::from_secs(30),
        })
        .handshake_timeout(IO_DEADLINE)
        .peer_ready_timeout(IO_DEADLINE)
        .drain_timeout(IO_DEADLINE)
        .service(ingress)?
        .on_peer_ready(
            LifecycleClient::send_policy(),
            Ready(Arc::new(Mutex::new(Some(ready)))),
        )?
        .build()?;
    Ok(Connection {
        serving: Box::pin(server.serve_with_shutdown(
            OneSocket(Some(socket)),
            furpc::ConnectionOptions::managed(),
            std::future::pending(),
        )),
        incoming,
        ready: wait,
        failure: observed,
    })
}

struct OneSocket(Option<TcpStream>);
impl Accept for OneSocket {
    type Io = TcpStream;
    type Error = std::io::Error;
    async fn accept(&mut self) -> std::io::Result<Option<Accepted<TcpStream>>> {
        let Some(socket) = self.0.take() else {
            return Ok(None);
        };
        let local = socket.local_addr()?;
        let peer = socket.peer_addr()?;
        if !local.ip().is_loopback() || !peer.ip().is_loopback() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Hosting requires actual loopback endpoints",
            ));
        }
        socket.set_nodelay(true)?;
        Ok(Some(Accepted::new(
            socket,
            IoInfo {
                local_addr: Some(local),
                peer_addr: Some(peer),
            },
        )))
    }
}

#[derive(Clone)]
struct Ready(Arc<Mutex<Option<oneshot::Sender<Peer>>>>);
impl Service<Peer> for Ready {
    type Response = ();
    type Error = Error;
    type Future = std::future::Ready<Result<()>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, peer: Peer) -> Self::Future {
        std::future::ready(
            self.0
                .lock()
                .unwrap()
                .take()
                .ok_or(Error::Internal("Hosting accepted a second generation"))
                .and_then(|ready| ready.send(peer).map_err(|_| Error::Closed)),
        )
    }
}
