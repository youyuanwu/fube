use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use furpc::{
    header, ActorId, Body, BoxError, Channel, Codec, CodecError, EncodeBuf, Encoder, Endpoint,
    FubstCodec, HeaderId, HeaderRecord, Request, Response, RpcClient,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::Instant;
use tower_service::Service;

use crate::{protocol, DomainFailure, GatewayDescription};

const ACTIVITY: u16 = 0x8020;
const TIMEOUT: u16 = 0x8023;
const CLIENT_VERSION: u16 = 0x8025;

/// Errors from the experimental furpc-backed client, without legacy transport conversion.
#[derive(Debug, Clone)]
pub enum FurpcClientError {
    Native(furpc::Error),
    Decode(fubst::Error),
    Protocol(&'static str),
    Domain(DomainFailure),
}
impl fmt::Display for FurpcClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => write!(f, "furpc client: {error}"),
            Self::Decode(error) => write!(f, "gateway decode: {error}"),
            Self::Protocol(reason) => write!(f, "gateway protocol: {reason}"),
            Self::Domain(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for FurpcClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}
impl From<furpc::Error> for FurpcClientError {
    fn from(error: furpc::Error) -> Self {
        Self::Native(error)
    }
}
impl From<fubst::Error> for FurpcClientError {
    fn from(error: fubst::Error) -> Self {
        Self::Decode(error)
    }
}
impl FurpcClientError {
    fn from_rpc(error: furpc::Error) -> Self {
        // RpcClient preserves byte-service and codec errors as their original sources.
        match &error {
            furpc::Error::Transport { .. } => {
                if let Some(error) = std::error::Error::source(&error)
                    .and_then(|source| source.downcast_ref::<Self>())
                {
                    return error.clone();
                }
            }
            furpc::Error::Codec {
                phase: "decode",
                source,
            } => {
                if let Some(error) = std::error::Error::source(source)
                    .and_then(|source| source.downcast_ref::<fubst::Error>())
                {
                    return Self::Decode(*error);
                }
            }
            _ => {}
        }
        Self::Native(error)
    }
}
type Result<T> = std::result::Result<T, FurpcClientError>;

/// Typed gateway stub over a byte-facing Tower service.
///
/// Only read-only Ping is implemented, separately from [`crate::FabricClient`].
/// The default transport is a furpc Channel; clones share its lifecycle and
/// dropping the last owner closes it. Later calls may reconnect, but a submitted
/// call is never replayed. Custom transports need not be Clone, Sync or static.
/// Clone a cloneable client explicitly before concurrent calls.
///
/// ```no_run
/// use std::time::Duration;
/// use fube_client::FurpcClient;
/// use furpc::Request;
/// use tokio::time::Instant;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let channel = FurpcClient::endpoint_clear("127.0.0.1:19000".parse()?)?
///     .connect().await?;
/// let mut client = FurpcClient::new(channel);
/// let deadline = Instant::now() + Duration::from_secs(30);
/// let gateway = client.ping(Request::new(()), deadline).await?.into_body();
/// # let _ = gateway;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FurpcClient<T = Channel> {
    inner: RpcClient<GatewayService<T>>,
}
impl FurpcClient<Channel> {
    /// Explicitly select an unsecured native gateway; performs no I/O.
    pub fn endpoint_clear(address: SocketAddr) -> furpc::Result<Endpoint> {
        Endpoint::try_from(address)
    }
}
impl<T> FurpcClient<T> {
    /// Bind locally without a handshake, readiness polling, or transport cloning.
    pub fn new(transport: T) -> Self {
        Self {
            inner: RpcClient::new(GatewayService {
                transport,
                validate_reply: check_ping_reply,
            }),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner().transport
    }
}
impl<T> FurpcClient<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    /// Discover the gateway's identity without retry or HTTP fallback.
    ///
    /// The earlier of the explicit and request-local deadlines covers encoding,
    /// readiness/reconnect, submission and receipt, using furpc settlement rules.
    /// The unit request encodes to zero bytes, not a serialized unit object.
    /// Channel transports require managed mode; raw-only mode returns Unsupported.
    pub async fn ping(
        &mut self,
        request: Request<()>,
        deadline: Instant,
    ) -> Result<Response<GatewayDescription>> {
        let deadline = request
            .deadline()
            .map_or(deadline, |local| local.min(deadline));
        timeout_ticks(deadline)?;
        let request = request
            .append_header(header::actor(ActorId::new(5)?)?)?
            .append_header(header::action("PingRequest")?)?
            .append_header(HeaderRecord::native(
                HeaderId::new(ACTIVITY)?,
                &ActivityHeader {
                    activity: ActivityId(uuid::Uuid::new_v4().to_bytes_le(), 0),
                },
            )?)?
            .append_header(HeaderRecord::native(
                HeaderId::new(CLIENT_VERSION)?,
                &ProtocolVersion(1, 1),
            )?)?;
        let response = self
            .inner
            .call(request, PingCodec, deadline)
            .await
            .map_err(FurpcClientError::from_rpc)?;
        let (parts, body) = response.into_parts();
        let gateway = body.into_gateway().map_err(FurpcClientError::Protocol)?;
        Ok(Response::from_parts(parts, gateway))
    }
}

#[derive(Clone)]
struct GatewayService<T> {
    transport: T,
    validate_reply: fn(&Response<Body>) -> Result<()>,
}
impl<T> Service<Request<Body>> for GatewayService<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = GatewayFuture<T::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), BoxError>> {
        self.transport.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        match stamp_timeout(request) {
            Ok(request) => GatewayFuture::Call {
                future: Box::pin(self.transport.call(request)),
                validate_reply: self.validate_reply,
            },
            Err(error) => GatewayFuture::Failed(Some(error)),
        }
    }
}

enum GatewayFuture<F> {
    Call {
        future: Pin<Box<F>>,
        validate_reply: fn(&Response<Body>) -> Result<()>,
    },
    Failed(Option<furpc::Error>),
}
impl<F, E> Future for GatewayFuture<F>
where
    F: Future<Output = std::result::Result<Response<Body>, E>>,
    E: Into<BoxError>,
{
    type Output = std::result::Result<Response<Body>, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Call {
                future,
                validate_reply,
            } => {
                let response = std::task::ready!(future.as_mut().poll(cx)).map_err(Into::into)?;
                validate_reply(&response)?;
                Poll::Ready(Ok(response))
            }
            Self::Failed(error) => Poll::Ready(Err(error
                .take()
                .expect("polled completed gateway call")
                .into())),
        }
    }
}

fn timeout_ticks(deadline: Instant) -> furpc::Result<i64> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let ticks = i64::try_from(remaining.as_nanos() / 100)
        .map_err(|_| furpc::Error::InvalidInput("timeout exceeds native signed ticks"))?;
    if ticks == 0 {
        return Err(furpc::Error::Timeout("gateway"));
    }
    Ok(ticks)
}

fn stamp_timeout(request: Request<Body>) -> furpc::Result<Request<Body>> {
    let deadline = request.deadline().ok_or(furpc::Error::InvalidInput(
        "gateway request requires a deadline",
    ))?;
    let ticks = timeout_ticks(deadline)?;
    request.append_header(HeaderRecord::native(
        HeaderId::new(TIMEOUT)?,
        &TimeoutHeader { ticks },
    )?)
}

struct PingCodec;
impl Codec for PingCodec {
    type Encode = ();
    type Decode = protocol::PingReplyBody;
    type Encoder = Self;
    type Decoder = <FubstCodec<(), protocol::PingReplyBody> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        Self
    }

    fn decoder(&mut self) -> Self::Decoder {
        FubstCodec::<(), protocol::PingReplyBody>::default().decoder()
    }
}
impl Encoder for PingCodec {
    type Item = ();

    fn encode(&mut self, (): (), _: &mut EncodeBuf<'_>) -> std::result::Result<(), CodecError> {
        Ok(())
    }
}

#[derive(Serialize)]
struct ActivityHeader {
    activity: ActivityId,
}
#[derive(Serialize)]
struct ActivityId(#[serde(with = "fubst::serde::guid")] [u8; 16], u64);
#[derive(Serialize, Deserialize)]
struct ProtocolVersion(i64, i64);
#[derive(Serialize)]
struct TimeoutHeader {
    ticks: i64,
}

fn decode_body<T: DeserializeOwned>(body: &Body) -> Result<T> {
    let mut segments = body.segments();
    let first = segments.next().unwrap_or(&[]);
    if segments.next().is_none() {
        return Ok(fubst::serde::from_slice(first)?);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(body.len())
        .map_err(|_| furpc::Error::ResourceLimit("gateway decode input"))?;
    for segment in body.segments() {
        bytes.extend_from_slice(segment);
    }
    Ok(fubst::serde::from_slice(&bytes)?)
}

fn check_ping_reply(response: &Response<Body>) -> Result<()> {
    let headers = response.headers();
    let common = headers.effective();
    if common.actor != ActorId::new(5)? {
        return Err(FurpcClientError::Protocol(
            "reply is not from NamingGateway",
        ));
    }
    let action_is = |action: &str| common.action().iter().copied().eq(action.encode_utf16());
    if action_is("ClientOperationFailure") {
        let failure: protocol::GatewayFailureBody = decode_body(response.body())?;
        if failure.code == 0 {
            return Err(FurpcClientError::Protocol(
                "operation failure has no nonzero error code",
            ));
        }
        return Err(FurpcClientError::Domain(DomainFailure {
            code: failure.code,
            message: failure.diagnostic,
        }));
    }
    if !action_is("PingRequest") {
        return Err(FurpcClientError::Protocol("unexpected ping reply action"));
    }
    let mut version = None;
    for record in headers
        .raw_records()
        .filter(|record| record.id().get() == CLIENT_VERSION)
    {
        version = Some(fubst::serde::from_slice::<ProtocolVersion>(record.value())?);
    }
    if !matches!(version, Some(ProtocolVersion(1, minor)) if minor >= 1) {
        return Err(FurpcClientError::Protocol(
            "ping requires protocol version 1.1 or newer 1.x",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
