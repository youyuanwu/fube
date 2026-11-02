//! Native IPC session envelopes above the shared connection/channel driver.
//!
//! SF IpcClient::OnSending adds a missing MessageId and host/PID identity to
//! requests and one-way sends, but not original-context replies. Accepted IPC
//! owns its client-only hello and server envelope policy here as well. The
//! shared channel owns ID allocation, correlation, lifecycle and raw I/O.

use std::{net::SocketAddr, sync::Arc};

use tokio::{
    net::TcpStream,
    time::{timeout_at, Instant},
};

use crate::{
    connection::{write_message, MAX_FRAME_TIME},
    message::{self, Header, ListenInstance, Message, MessageId},
    native_channel::{envelope, ChannelLimits, GatewayChannel, SessionEnvelope},
    native_demux::ChannelComposition,
    raw_session, Error, Result,
};

/// Establish the supported clear-loopback IPC session on a shared channel.
///
/// The immutable session envelope remains bound to every cloned channel owner.
/// Callers cannot override its identity or bypass it through raw/RPC methods.
/// Normal-request reply authority is configured independently by composition.
pub async fn connect_unsecured_loopback(
    endpoint: SocketAddr,
    composition: ChannelComposition,
    limits: ChannelLimits,
    deadline: Instant,
    host_id: &str,
) -> Result<GatewayChannel> {
    let session = Arc::new(IpcSession::new(host_id, std::process::id())?);
    GatewayChannel::connect_with_envelope(endpoint, composition, limits, deadline, session).await
}

/// Establish one accepted, explicitly trusted numeric-loopback IPC session.
///
/// The restricted client-only ListenInstance exchange precedes adoption by the
/// same bounded channel driver as outgoing sessions. IPC identity is admitted
/// to actor policy, not authenticated here: Hosting owns host/PID/session/replay
/// admission. Retirement must explicitly choose ordinary shutdown or whole
/// socket disposal; accepted IPC does not choose a lifecycle mode.
pub async fn accept_unsecured_loopback(
    mut stream: TcpStream,
    composition: ChannelComposition,
    limits: ChannelLimits,
    deadline: Instant,
) -> Result<GatewayChannel> {
    limits.validate()?;
    let local = stream.local_addr()?;
    if !stream.peer_addr()?.ip().is_loopback() || !local.ip().is_loopback() {
        return Err(Error::InvalidInput("accepted IPC requires loopback"));
    }
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    let process_id = uuid::Uuid::new_v4().to_bytes_le();
    timeout_at(deadline, async {
        let first = raw_session::read_message(
            &mut stream,
            raw_session::Limits {
                max_frame_bytes: limits.max_frame_bytes,
                frame_time: MAX_FRAME_TIME,
            },
        )
        .await?;
        let props = envelope(&first, true)?;
        if props.id == MessageId::default()
            || ListenInstance::decode(&first.body)? != ListenInstance::default()
            || props.actor != 1
            || !props.action.is_empty()
            || first.headers.iter().any(|h| {
                !matches!(
                    h.id,
                    message::ACTOR | message::ACTION | message::MESSAGE_ID | message::HIGH_PRIORITY
                )
            })
        {
            return Err(Error::Protocol("invalid client-only ListenInstance"));
        }
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: process_id,
                    index: 1,
                },
                &ListenInstance {
                    address: local.to_string(),
                    instance: 1,
                    nonce: process_id,
                },
            )?,
        )
        .await
    })
    .await
    .map_err(|_| Error::Timeout)??;
    GatewayChannel::adopt_established(
        stream,
        composition,
        limits,
        process_id,
        ListenInstance::default(),
        deadline,
        Arc::new(AcceptedSession),
    )
}

struct AcceptedSession;

impl SessionEnvelope for AcceptedSession {
    fn requires_message_id(&self) -> bool {
        true
    }

    fn incoming(&self, _: &Message) -> Result<()> {
        // Common envelope/control/reply guards still apply in the channel.
        // Domain admission, including identity validation, is not session policy.
        Ok(())
    }
}

pub(crate) struct IpcSession {
    identity: Header,
}

impl IpcSession {
    pub(crate) fn new(host_id: &str, process_id: u32) -> Result<Self> {
        if host_id.is_empty() || host_id.len() > 4096 {
            return Err(Error::InvalidInput("IPC requires bounded nonempty host ID"));
        }
        Ok(Self {
            identity: Header::object(message::IPC, |e| {
                e.string(host_id)?;
                e.u32(process_id);
                Ok(())
            })?,
        })
    }
}

impl SessionEnvelope for IpcSession {
    fn header(&self, message: &Message, original_reply: bool) -> Result<Option<Header>> {
        if original_reply {
            return Ok(None);
        }
        if message.headers.iter().any(|h| h.id == message::MESSAGE_ID)
            && message.properties()?.id == MessageId::default()
        {
            return Err(Error::InvalidInput("explicitly empty IPC MessageId"));
        }
        Ok(Some(self.identity.clone()))
    }

    fn requires_message_id(&self) -> bool {
        true
    }
}
