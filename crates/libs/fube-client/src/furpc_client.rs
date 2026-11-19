use std::{fmt, net::SocketAddr, time::Duration};

use furpc::{header, ActorId, Body, Channel, Endpoint, HeaderId, HeaderRecord, Request, Response};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::Instant;

use crate::{protocol, DomainFailure, GatewayDescription, DEFAULT_TIMEOUT};

const ACTIVITY: u16 = 0x8020;
const TIMEOUT: u16 = 0x8023;
const CLIENT_VERSION: u16 = 0x8025;

/// Errors from the experimental furpc-backed client, without legacy transport conversion.
#[derive(Debug)]
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
type Result<T> = std::result::Result<T, FurpcClientError>;

/// Minimal furpc-backed gateway client, separate from [`crate::FabricClient`].
///
/// Only read-only Ping is implemented. Clones own the same furpc Channel;
/// dropping the last owner closes it. Later calls may reconnect, but a submitted
/// call is never replayed. No legacy channel, budget, or lifecycle is adopted.
///
/// ```no_run
/// use fube_client::FurpcClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let channel = FurpcClient::endpoint_clear("127.0.0.1:19000".parse()?)?
///     .connect().await?;
/// let client = FurpcClient::new(channel);
/// let gateway = client.ping(None).await?;
/// # let _ = gateway;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FurpcClient {
    channel: Channel,
}
impl FurpcClient {
    /// Explicitly select an unsecured native gateway; performs no I/O.
    pub fn endpoint_clear(address: SocketAddr) -> furpc::Result<Endpoint> {
        Endpoint::try_from(address)
    }

    /// Bind locally to an owning furpc Channel without a domain handshake.
    /// Ping requires managed mode; a raw-only Channel returns Unsupported.
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    /// Discover the gateway's full identity with one deadline, including reconnect.
    /// `None` uses [`DEFAULT_TIMEOUT`]. There is no retry or HTTP fallback.
    pub async fn ping(&self, timeout: Option<Duration>) -> Result<GatewayDescription> {
        let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);
        i64::try_from(timeout.as_nanos() / 100)
            .map_err(|_| furpc::Error::InvalidInput("timeout exceeds native signed ticks"))?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .filter(|_| !timeout.is_zero() && timeout.as_nanos() >= 100)
            .ok_or(furpc::Error::InvalidInput("invalid gateway timeout"))?;
        let peer = self.channel.peer(deadline).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let ticks = i64::try_from(remaining.as_nanos() / 100)
            .map_err(|_| furpc::Error::InvalidInput("timeout exceeds native signed ticks"))?;
        let request = Request::new(Body::empty())
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
            )?)?
            .append_header(HeaderRecord::native(
                HeaderId::new(TIMEOUT)?,
                &TimeoutHeader { ticks },
            )?)?;
        let response = peer.request(request, deadline).await?;
        decode_ping(&response)
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

fn decode_ping(response: &Response<Body>) -> Result<GatewayDescription> {
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
    decode_body::<protocol::PingReplyBody>(response.body())?
        .into_gateway()
        .map_err(FurpcClientError::Protocol)
}

#[cfg(test)]
mod tests;
