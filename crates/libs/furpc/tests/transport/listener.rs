use super::*;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct RecordedIo {
    io: DuplexStream,
    written: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for RecordedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}

impl AsyncWrite for RecordedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(count)) = result {
            self.written
                .lock()
                .unwrap()
                .extend_from_slice(&bytes[..count]);
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

fn recorded(io: DuplexStream) -> (RecordedIo, Arc<Mutex<Vec<u8>>>) {
    let written = Arc::new(Mutex::new(Vec::new()));
    (
        RecordedIo {
            io,
            written: written.clone(),
        },
        written,
    )
}

fn hello(written: &Mutex<Vec<u8>>) -> (String, u64, [u8; 16]) {
    let bytes = written.lock().unwrap();
    let size = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let body = 12 + usize::from(u16::from_le_bytes([bytes[6], bytes[7]]));
    assert_eq!(bytes.len(), size, "only the completed hello was written");
    assert_eq!(bytes[4], 0, "clear transport");
    let mut decoder = fubst::Decoder::new(&bytes[body..size]);
    let hello = decoder
        .object(|decoder| Ok((decoder.string()?, decoder.u64()?, decoder.guid()?)))
        .unwrap();
    decoder.finish().unwrap();
    hello
}

struct UnusedConnector(Arc<AtomicUsize>);

impl Service<Uri> for UnusedConnector {
    type Response = Connected<DuplexStream>;
    type Error = Error;
    type Future = std::future::Ready<furpc::Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Err(Error::Internal("unexpected connector readiness")))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Err(Error::Internal("unexpected connector call")))
    }
}

#[tokio::test]
async fn listener_settings_preserve_defaults_metadata_and_directional_nonces_in_both_modes() {
    let client_info = IoInfo {
        local_addr: Some("127.0.0.1:31001".parse().unwrap()),
        peer_addr: Some("127.0.0.1:31002".parse().unwrap()),
    };
    let server_info = IoInfo {
        local_addr: client_info.peer_addr,
        peer_addr: client_info.local_addr,
    };
    let mut nonces = Vec::new();
    for options in [ConnectionOptions::managed(), ConnectionOptions::raw_only()] {
        for instance in [None, Some(0), Some(u64::MAX)] {
            let (client, accepted) = tokio::io::duplex(32);
            let (client, client_bytes) = recorded(client);
            let (accepted, server_bytes) = recorded(accepted);
            let (ready, mut peers) = mpsc::channel(1);
            let mut server = Server::builder();
            let mut endpoint = Endpoint::from_shared("memory://remote/path?unchanged=1")
                .unwrap()
                .connection_options(options);
            if let Some(instance) = instance {
                server = server
                    .listen_address("server-listener:32002")
                    .unwrap()
                    .instance(instance);
                endpoint = endpoint
                    .instance(instance)
                    .listen_address("client-listener:32001")
                    .unwrap();
            }
            let server = server
                .on_peer_ready(
                    SendPolicy::new(control_sends),
                    Function(move |peer: Peer| {
                        let ready = ready.clone();
                        async move {
                            ready.send(peer).await.unwrap();
                            Ok::<_, Error>(())
                        }
                    }),
                )
                .unwrap()
                .build()
                .unwrap();
            let acceptor = StreamAcceptor::new(stream::iter([Ok::<_, std::io::Error>(
                Accepted::new(accepted, server_info.clone()),
            )]));
            let serving = tokio::spawn(server.serve_with_shutdown(acceptor, options, pending()));
            let mut client = Some(client);
            let info = client_info.clone();
            let channel = bounded(endpoint.connect_with_connector(Function(move |uri: Uri| {
                assert_eq!(
                    uri,
                    "memory://remote/path?unchanged=1".parse::<Uri>().unwrap()
                );
                std::future::ready(Ok::<_, Error>(Connected::new(
                    client.take().unwrap(),
                    info.clone(),
                )))
            })))
            .await
            .unwrap();
            let accepted = bounded(peers.recv()).await.unwrap();
            let peer = channel.peer(deadline()).await.unwrap();
            let (client_address, client_instance, client_nonce) = hello(&client_bytes);
            let (server_address, server_instance, server_nonce) = hello(&server_bytes);
            assert_eq!(client_nonce, [0; 16]);
            assert_ne!(server_nonce, [0; 16]);
            assert!(!nonces.contains(&server_nonce));
            nonces.push(server_nonce);
            assert_eq!(peer.info().io, client_info);
            assert_eq!(accepted.info().io, server_info);
            if let Some(instance) = instance {
                assert_eq!(client_address, "client-listener:32001");
                assert_eq!(server_address, "server-listener:32002");
                assert_eq!(client_instance, instance);
                assert_eq!(server_instance, instance);
            } else {
                assert_eq!(client_address, "");
                assert_eq!(client_instance, 0);
                assert_eq!(server_address, "127.0.0.1:31002");
                assert_eq!(server_instance, 1);
            }
            assert_eq!(
                String::from_utf16(&accepted.info().listen_address).unwrap(),
                client_address
            );
            assert_eq!(accepted.info().listen_instance, client_instance);
            assert_eq!(
                String::from_utf16(&peer.info().listen_address).unwrap(),
                server_address
            );
            assert_eq!(peer.info().listen_instance, server_instance);
            drop(channel);
            bounded(serving).await.unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn explicit_listener_defaults_to_zero_without_uri_or_legacy_string_limits() {
    let address = format!("listener/{}", "\u{1f680}".repeat(fubst::MAX_STRING_UNITS));
    let (client, accepted) = tokio::io::duplex(1024);
    let (client, bytes) = recorded(client);
    let server = Server::builder()
        .listen_address(address.clone())
        .unwrap()
        .build()
        .unwrap();
    let serving = tokio::spawn(server.serve_with_shutdown(
        one(accepted),
        ConnectionOptions::raw_only(),
        pending(),
    ));
    let mut client = Some(client);
    let channel = bounded(
        raw("memory://remote")
            .listen_address(address.clone())
            .unwrap()
            .connect_with_connector(Function(move |_: Uri| {
                std::future::ready(Ok::<_, Error>(Connected::new(
                    client.take().unwrap(),
                    IoInfo::unknown(),
                )))
            })),
    )
    .await
    .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    assert_eq!(hello(&bytes), (address.clone(), 0, [0; 16]));
    assert_eq!(
        String::from_utf16(&peer.info().listen_address).unwrap(),
        address
    );
    assert_eq!(peer.info().listen_instance, 0);
    assert_eq!(peer.info().io, IoInfo::unknown());
    drop(channel);
    bounded(serving).await.unwrap().unwrap();
}

#[tokio::test]
async fn wildcard_listener_advertisement_does_not_replace_actual_tcp_endpoints() {
    let listener = TcpAcceptor::bind("0.0.0.0:0".parse().unwrap())
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let advertised = format!("127.0.0.2:{port}");
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(
        Server::builder()
            .listen_address(advertised.clone())
            .unwrap()
            .build()
            .unwrap()
            .serve_with_shutdown(listener, ConnectionOptions::raw_only(), async {
                stopped.await.unwrap();
            }),
    );
    let actual = format!("127.0.0.1:{port}");
    let channel = bounded(raw(format!("tcp://{actual}")).connect())
        .await
        .unwrap();
    let peer = channel.peer(deadline()).await.unwrap();
    assert_eq!(
        String::from_utf16(&peer.info().listen_address).unwrap(),
        advertised
    );
    assert_eq!(peer.info().listen_instance, 0);
    assert_eq!(peer.info().io.peer_addr, Some(actual.parse().unwrap()));
    assert!(peer.info().io.local_addr.unwrap().ip().is_loopback());
    stop.send(()).unwrap();
    bounded(serving).await.unwrap().unwrap();
    drop(channel);
}

#[tokio::test]
async fn invalid_listener_settings_and_hello_frame_limits_fail_before_connector_readiness() {
    assert!(matches!(
        raw("memory://remote").listen_address(""),
        Err(Error::InvalidInput("local listen address is empty"))
    ));
    assert!(matches!(
        Server::builder().listen_address(""),
        Err(Error::InvalidInput("local listen address is empty"))
    ));
    for instance in [0, u64::MAX] {
        assert!(matches!(
            Server::builder().instance(instance).build(),
            Err(Error::InvalidInput(
                "instance requires a local listen address"
            ))
        ));
        let polled = Arc::new(AtomicUsize::new(0));
        let connector = UnusedConnector(polled.clone());
        assert!(matches!(
            raw("memory://remote")
                .instance(instance)
                .connect_with_connector(connector)
                .await,
            Err(Error::InvalidInput(
                "instance requires a local listen address"
            ))
        ));
        assert_eq!(polled.load(Ordering::SeqCst), 0);
    }
    let limits = TransportLimits {
        max_send_frame_bytes: Some(128),
        ..TransportLimits::default()
    };
    let address = "a".repeat(32);
    assert!(matches!(
        Server::builder()
            .listen_address(address.clone())
            .unwrap()
            .limits(limits.clone())
            .build(),
        Err(Error::ResourceLimit("frame bytes"))
    ));
    let polls = Arc::new(AtomicUsize::new(0));
    let connector = UnusedConnector(polls.clone());
    assert!(matches!(
        raw("memory://remote")
            .limits(limits)
            .listen_address(address)
            .unwrap()
            .connect_with_connector(connector)
            .await,
        Err(Error::ResourceLimit("frame bytes"))
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}
