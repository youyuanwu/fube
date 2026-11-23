use crate::{HostError, Result};
use furpc::{
    header, Accept, Accepted, ActorId, Body, HandlerLimits, IngressLimits, IngressOptions,
    IngressResponse, IngressService, IoInfo, Message, Peer, Request, RequestPolicy, SendPolicy,
    Server, ServiceBinding, TransportLimits,
};
use std::{
    future::{ready, Future, Ready},
    net::TcpStream,
    pin::Pin,
    sync::{
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex,
    },
    task::{Context, Poll},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::{mpsc as async_mpsc, oneshot};
use tower_service::Service;

#[cfg(test)]
#[path = "../../fube-hosting/tests/support/ipc_wire.rs"]
#[allow(dead_code)]
mod fixture;

const FRAME_LIMIT: usize = 4 * 1024 * 1024;
const INBOX_LIMIT: usize = 64;

pub struct Incoming {
    pub message: Message,
    pub response: Option<oneshot::Sender<Message>>,
}
impl Incoming {
    /// Return response data; furpc alone owns correlation and the actual write.
    pub fn reply(self, message: Message) -> Result<()> {
        self.response
            .ok_or_else(|| HostError("IPC message is not a request".into()))?
            .send(message)
            .map_err(|_| HostError("IPC request no longer live".into()))
    }
}

struct Command {
    message: Message,
    result: SyncSender<Result<()>>,
}

pub struct Session {
    pub incoming: Receiver<Result<Incoming>>,
    commands: async_mpsc::Sender<Command>,
    stop: Option<oneshot::Sender<()>>,
    owner: Option<JoinHandle<Result<()>>>,
}

#[derive(Clone)]
struct Ingress {
    incoming: SyncSender<Result<Incoming>>,
    fatal: async_mpsc::Sender<HostError>,
}
impl Service<Request<Body>> for Ingress {
    type Response = IngressResponse;
    type Error = furpc::Error;
    type Future = Pin<Box<dyn Future<Output = furpc::Result<IngressResponse>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let expects_reply = request.headers().effective().expects_reply;
        let (parts, body) = request.into_parts();
        let mut builder = Message::builder().body(body);
        for header in parts.headers.records() {
            builder = builder.append_header(header.to_owned());
        }
        let result = builder.build().and_then(|message| {
            let (response, receive) = oneshot::channel();
            self.incoming
                .try_send(Ok(Incoming {
                    message,
                    response: expects_reply.then_some(response),
                }))
                .map_err(|_| furpc::Error::ResourceLimit("test-host IPC inbox"))?;
            Ok(receive)
        });
        if let Err(error) = &result {
            let _ = self.fatal.try_send(HostError(error.to_string()));
        }
        Box::pin(async move {
            let receive = result?;
            if expects_reply {
                Ok(IngressResponse::Reply(
                    receive.await.map_err(|_| furpc::Error::Closed)?,
                ))
            } else {
                Ok(IngressResponse::NoReply)
            }
        })
    }
}

struct OneSocket(Option<Accepted<tokio::net::TcpStream>>);
impl Accept for OneSocket {
    type Io = tokio::net::TcpStream;
    type Error = furpc::Error;
    async fn accept(&mut self) -> furpc::Result<Option<Accepted<Self::Io>>> {
        Ok(self.0.take())
    }
}

#[derive(Clone)]
struct ReadyPeer(Arc<Mutex<Option<oneshot::Sender<Peer>>>>);
impl Service<Peer> for ReadyPeer {
    type Response = ();
    type Error = furpc::Error;
    type Future = Ready<furpc::Result<()>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, peer: Peer) -> Self::Future {
        let info = peer.info();
        ready(
            if !info.listen_address.is_empty() || info.listen_instance != 0 {
                Err(furpc::Error::Protocol(
                    "expected native client-only listen instance",
                ))
            } else {
                self.0
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or(furpc::Error::Protocol("second test-host IPC generation"))
                    .and_then(|ready| ready.send(peer).map_err(|_| furpc::Error::Closed))
            },
        )
    }
}

async fn serve(
    stream: TcpStream,
    address: String,
    incoming: SyncSender<Result<Incoming>>,
    mut commands: async_mpsc::Receiver<Command>,
    stop: oneshot::Receiver<()>,
) -> Result<()> {
    let stream = tokio::net::TcpStream::from_std(stream)?;
    let info = IoInfo {
        local_addr: Some(stream.local_addr()?),
        peer_addr: Some(stream.peer_addr()?),
    };
    let (fatal, mut failures) = async_mpsc::channel(1);
    let service = Ingress { incoming, fatal };
    let allow = |_: &furpc::Headers, _: &furpc::Source| Ok(());
    let mut bindings = vec![ServiceBinding::actor(
        ActorId::new(12)?,
        IngressOptions::one_way(
            allow,
            IngressLimits {
                max_body_bytes: FRAME_LIMIT,
            },
        ),
    )?];
    for actor in [7, 9, 15] {
        bindings.push(ServiceBinding::actor(
            ActorId::new(actor)?,
            IngressOptions::requests(
                RequestPolicy::new(allow, allow),
                IngressLimits {
                    max_body_bytes: FRAME_LIMIT,
                },
            ),
        )?);
    }
    let (ready, peer) = oneshot::channel();
    let server = Server::builder()
        .listen_address(address)?
        .instance(1)
        .limits(TransportLimits {
            max_receive_frame_bytes: FRAME_LIMIT,
            max_send_frame_bytes: Some(FRAME_LIMIT),
            max_pending_sends: INBOX_LIMIT,
            max_pending_requests: INBOX_LIMIT,
            max_connections: 1,
            max_handshakes: 1,
            max_peer_waiters: 1,
            send_queue_bytes: FRAME_LIMIT,
            frame_timeout: Duration::from_secs(120),
            write_timeout: Duration::from_secs(5),
            max_discard_bytes: FRAME_LIMIT,
            ..Default::default()
        })
        .handler_limits(HandlerLimits {
            max_encoded_body_bytes_per_response: FRAME_LIMIT,
            timeout: Duration::from_secs(120),
        })
        .handshake_timeout(Duration::from_secs(120))
        .service(IngressService::new(service, bindings)?)?
        .on_peer_ready(
            SendPolicy::new(|_, _, _| Ok(())),
            ReadyPeer(Arc::new(Mutex::new(Some(ready)))),
        )?
        .build()?;
    let sends = async {
        let peer = peer
            .await
            .map_err(|_| HostError("IPC handshake failed".into()))?;
        while let Some(command) = commands.recv().await {
            let result = peer
                .send(
                    command.message,
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
                .map(|_| ())
                .map_err(Into::into);
            let _ = command.result.send(result);
        }
        Ok(())
    };
    tokio::select! {
        biased;
        _ = stop => Ok(()),
        Some(error) = failures.recv() => Err(error),
        result = server.serve_with_shutdown(
            OneSocket(Some(Accepted::new(stream, info))),
            furpc::ConnectionOptions::managed(),
            std::future::pending(),
        ) => result.map_err(Into::into),
        result = sends => result,
    }
}

impl Session {
    pub fn new(stream: TcpStream, address: String) -> Result<Self> {
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (sender, incoming) = mpsc::sync_channel(INBOX_LIMIT);
        let (commands, receive) = async_mpsc::channel(8);
        let (stop, stopped) = oneshot::channel();
        let owner = thread::Builder::new()
            .name("fube-test-host-ipc".into())
            .spawn(move || {
                let result =
                    runtime.block_on(serve(stream, address, sender.clone(), receive, stopped));
                if let Err(error) = &result {
                    let _ = sender.try_send(Err(HostError(error.to_string())));
                }
                result
            })?;
        Ok(Self {
            incoming,
            commands,
            stop: Some(stop),
            owner: Some(owner),
        })
    }

    pub fn one_way(&mut self, actor: i64, action: &str, body: Vec<u8>) -> Result<()> {
        let message = Message::builder()
            .append_header(header::actor(ActorId::new(actor)?)?)
            .append_header(header::action(action)?)
            .body(Body::from_bytes(body)?)
            .build()?;
        let (result, receive) = mpsc::sync_channel(1);
        self.commands
            .try_send(Command { message, result })
            .map_err(|error| HostError(format!("IPC send admission: {error}")))?;
        receive
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| HostError(format!("IPC local write completion unknown: {error}")))?
    }

    pub fn close(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let result = match self.owner.take() {
            Some(owner) => owner
                .join()
                .map_err(|_| HostError("IPC owner panicked".into()))
                .and_then(|result| result),
            None => Ok(()),
        };
        while self.incoming.try_recv().is_ok() {}
        result
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("test-host IPC cleanup failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fixture::{Header, ListenInstance, Message as WireMessage};
    use furpc::MessageId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::timeout;

    async fn pair() -> (Session, tokio::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();
        let mut client = tokio::net::TcpStream::from_std(client).unwrap();
        let session = Session::new(server, listener.local_addr().unwrap().to_string()).unwrap();
        let hello = fixture::read_message(&mut client).await.unwrap();
        assert_eq!(hello.properties().unwrap().actor, 1);
        let hello = WireMessage::listen_instance(
            MessageId {
                guid: [1; 16],
                index: 1,
            },
            &ListenInstance::default(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        fixture::write_message(&mut bytes, &hello).await.unwrap();
        for chunk in bytes.chunks(2) {
            client.write_all(chunk).await.unwrap();
        }
        (session, client)
    }

    async fn request(client: &mut tokio::net::TcpStream) -> MessageId {
        let id = MessageId {
            guid: [2; 16],
            index: 7,
        };
        fixture::write_message(
            client,
            &WireMessage {
                headers: vec![
                    Header::actor(7).unwrap(),
                    Header::action("FinishRegisterApplicationHostRequest").unwrap(),
                    Header::message_id(id).unwrap(),
                    Header::boolean(header::EXPECTS_REPLY.get(), true).unwrap(),
                ],
                body: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
        id
    }

    async fn eof(client: &mut tokio::net::TcpStream) {
        let result = timeout(Duration::from_secs(3), client.read(&mut [0]))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0)) || result.is_err(), "{result:?}");
    }

    #[tokio::test]
    async fn fragmented_handshake_runner_correlation_and_joined_reply_revocation() {
        let (mut session, mut client) = pair().await;
        let id = request(&mut client).await;
        let incoming = session
            .incoming
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        assert_eq!(incoming.message.headers().effective().message_id, id);
        incoming
            .reply(
                Message::builder()
                    .body(Body::from_bytes(vec![9]).unwrap())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let reply = fixture::read_message(&mut client).await.unwrap();
        assert_eq!(reply.properties().unwrap().relates_to, id);
        assert_eq!(reply.body, [9]);
        session.one_way(12, "ReplicaOpen", vec![8]).unwrap();
        let sent = fixture::read_message(&mut client).await.unwrap();
        assert_eq!(sent.properties().unwrap().action, "ReplicaOpen");
        assert_eq!(sent.body, [8]);

        request(&mut client).await;
        let retained = session
            .incoming
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        session.close().unwrap();
        assert!(retained.reply(Message::builder().build().unwrap()).is_err());
        eof(&mut client).await;
    }

    #[tokio::test]
    async fn oversized_frames_and_domain_inbox_overflow_retire_the_owned_socket() {
        let (mut session, mut client) = pair().await;
        let mut oversized = [0; 12];
        oversized[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        client.write_all(&oversized).await.unwrap();
        eof(&mut client).await;
        session.close().unwrap();

        let (mut session, mut client) = pair().await;
        for _ in 0..=INBOX_LIMIT {
            fixture::write_message(
                &mut client,
                &WireMessage {
                    headers: vec![
                        Header::actor(12).unwrap(),
                        Header::action("ReplicaOpenReply").unwrap(),
                    ],
                    body: vec![],
                },
            )
            .await
            .unwrap();
        }
        eof(&mut client).await;
        assert!(session.close().unwrap_err().0.contains("inbox"));
        assert!(session.incoming.try_recv().is_err());
    }

    #[tokio::test]
    async fn close_joins_even_before_remote_handshake() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();
        let mut client = tokio::net::TcpStream::from_std(client).unwrap();
        let mut session = Session::new(server, listener.local_addr().unwrap().to_string()).unwrap();
        session.close().unwrap();
        timeout(Duration::from_secs(3), client.read_to_end(&mut Vec::new()))
            .await
            .unwrap()
            .unwrap();
        assert!(session.incoming.try_recv().is_err());
    }
}
