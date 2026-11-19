use std::{
    future::{poll_fn, Future},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Stream;
use http::Uri;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};
use tower_service::Service;

use super::BoxFuture;
use crate::{BoxError, Error, Result};

/// Owned raw byte I/O. An encrypted adapter does not implement native Fabric TLS.
pub trait TransportIo: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> TransportIo for T {}

/// Diagnostics from the actual adapter, never authenticated peer identity.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IoInfo {
    pub local_addr: Option<SocketAddr>,
    pub peer_addr: Option<SocketAddr>,
}

impl IoInfo {
    pub fn unknown() -> Self {
        Self::default()
    }

    fn tcp(io: &TcpStream) -> Result<Self> {
        Ok(Self {
            local_addr: Some(
                io.local_addr()
                    .map_err(|error| Error::transport("local address", error))?,
            ),
            peer_addr: Some(
                io.peer_addr()
                    .map_err(|error| Error::transport("peer address", error))?,
            ),
        })
    }
}

pub struct Connected<I: TransportIo> {
    pub(crate) io: I,
    pub(crate) info: IoInfo,
}
impl<I: TransportIo> Connected<I> {
    pub fn new(io: I, info: IoInfo) -> Self {
        Self { io, info }
    }
    pub fn info(&self) -> &IoInfo {
        &self.info
    }
}

pub struct Accepted<I: TransportIo> {
    pub(crate) io: I,
    pub(crate) info: IoInfo,
}
impl<I: TransportIo> Accepted<I> {
    pub fn new(io: I, info: IoInfo) -> Self {
        Self { io, info }
    }
    pub fn info(&self) -> &IoInfo {
        &self.info
    }
}

/// Cancellation-safe source of owned I/O. `None` stops acceptance but lets
/// yielded connections finish; an error ends serving with its original source.
/// Adapters must bound external queues and dispose or retain unpublished I/O
/// when an accept future is dropped. Errors must not retain live I/O ownership.
pub trait Accept: Send + 'static {
    type Io: TransportIo;
    type Error: Into<BoxError>;
    fn accept(
        &mut self,
    ) -> impl Future<Output = std::result::Result<Option<Accepted<Self::Io>>, Self::Error>> + Send;
    fn info(&self) -> IoInfo {
        IoInfo::unknown()
    }
}

/// Pins a stream without adding a queue or adopting its producer's tasks.
pub struct StreamAcceptor<S> {
    stream: Pin<Box<S>>,
}
impl<S> StreamAcceptor<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream: Box::pin(stream),
        }
    }
}
impl<S, I, E> Accept for StreamAcceptor<S>
where
    S: Stream<Item = std::result::Result<Accepted<I>, E>> + Send + 'static,
    I: TransportIo,
    E: Into<BoxError>,
{
    type Io = I;
    type Error = E;
    async fn accept(&mut self) -> std::result::Result<Option<Accepted<I>>, E> {
        poll_fn(|cx| self.stream.as_mut().poll_next(cx))
            .await
            .transpose()
    }
}

pub struct TcpAcceptor {
    listener: TcpListener,
    nodelay: bool,
}
impl TcpAcceptor {
    pub async fn bind(address: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| Error::transport("bind", error))?;
        Ok(Self::from_listener(listener))
    }
    pub fn from_listener(listener: TcpListener) -> Self {
        Self {
            listener,
            nodelay: true,
        }
    }
    pub fn nodelay(mut self, enabled: bool) -> Self {
        self.nodelay = enabled;
        self
    }
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}
impl Accept for TcpAcceptor {
    type Io = TcpStream;
    type Error = Error;
    async fn accept(&mut self) -> Result<Option<Accepted<TcpStream>>> {
        let (io, _) = self
            .listener
            .accept()
            .await
            .map_err(|error| Error::transport("accept", error))?;
        io.set_nodelay(self.nodelay)
            .map_err(|error| Error::transport("TCP_NODELAY", error))?;
        let info = IoInfo::tcp(&io)?;
        Ok(Some(Accepted::new(io, info)))
    }
    fn info(&self) -> IoInfo {
        match self.local_addr() {
            Ok(address) => IoInfo {
                local_addr: Some(address),
                peer_addr: None,
            },
            Err(error) => {
                tracing::warn!(%error, "TCP listener metadata unavailable");
                IoInfo::unknown()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TcpConnector {
    nodelay: bool,
}
impl Default for TcpConnector {
    fn default() -> Self {
        Self { nodelay: true }
    }
}
impl TcpConnector {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn nodelay(mut self, enabled: bool) -> Self {
        self.nodelay = enabled;
        self
    }
}

pub(super) fn validate_uri(uri: &Uri) -> Result<()> {
    if uri.scheme().is_none()
        || uri
            .authority()
            .is_none_or(|authority| authority.host().is_empty())
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
    {
        return Err(Error::InvalidInput(
            "absolute hierarchical URI with credential-free authority required",
        ));
    }
    Ok(())
}

pub(super) fn tcp_destination(uri: &Uri) -> Result<(&str, u16)> {
    validate_uri(uri)?;
    if !uri
        .scheme_str()
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("tcp"))
    {
        return Err(Error::Unsupported("built-in connector requires tcp URI"));
    }
    if !matches!(uri.path(), "" | "/") || uri.query().is_some() {
        return Err(Error::InvalidInput(
            "TCP URI does not accept a path or query",
        ));
    }
    let authority = uri.authority().expect("validated authority");
    let port = authority
        .port()
        .filter(|port| port.as_str().bytes().all(|byte| byte.is_ascii_digit()))
        .map(|port| port.as_u16())
        .filter(|port| *port != 0)
        .ok_or(Error::InvalidInput(
            "TCP URI requires an explicit port in 1..=65535",
        ))?;
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    if host.contains('%') || host.is_empty() {
        return Err(Error::InvalidInput("unsupported TCP host syntax"));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        validate_ip(ip)?;
    }
    Ok((host, port))
}

fn validate_ip(ip: IpAddr) -> Result<()> {
    if ip.is_unspecified() || ip.is_multicast() || matches!(ip, IpAddr::V4(ip) if ip.is_broadcast())
    {
        return Err(Error::InvalidInput("invalid TCP destination address"));
    }
    Ok(())
}

impl Service<Uri> for TcpConnector {
    type Response = Connected<TcpStream>;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        let nodelay = self.nodelay;
        Box::pin(async move {
            let (host, port) = tcp_destination(&uri)?;
            let addresses = tokio::net::lookup_host((host, port))
                .await
                .map_err(|error| Error::transport("resolve", error))?;
            let mut last = None;
            for address in addresses {
                validate_ip(address.ip())?;
                match TcpStream::connect(address).await {
                    Ok(io) => {
                        io.set_nodelay(nodelay)
                            .map_err(|error| Error::transport("TCP_NODELAY", error))?;
                        let info = IoInfo::tcp(&io)?;
                        return Ok(Connected::new(io, info));
                    }
                    Err(error) => last = Some(error),
                }
            }
            Err(Error::transport(
                "connect",
                last.unwrap_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "resolver returned no addresses",
                    )
                }),
            ))
        })
    }
}
