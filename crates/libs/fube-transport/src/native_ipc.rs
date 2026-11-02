//! Native IPC session envelopes above the shared connection/channel driver.
//!
//! SF IpcClient::OnSending adds a missing MessageId and host/PID identity to
//! requests and one-way sends, but not original-context replies. This module
//! owns that protocol choice; the shared channel owns ID allocation and I/O.

use std::{net::SocketAddr, sync::Arc};

use tokio::time::Instant;

use crate::{
    message::{self, Header, Message, MessageId},
    native_channel::{ChannelLimits, GatewayChannel, OutboundEnvelope},
    native_demux::ChannelComposition,
    Error, Result,
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
    GatewayChannel::connect_with_envelope(endpoint, composition, limits, deadline, Some(session))
        .await
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

impl OutboundEnvelope for IpcSession {
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
