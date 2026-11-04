use std::sync::Arc;

use tokio::{net::TcpStream, time::Instant};

use crate::{
    connection::MAX_FRAME_TIME,
    message::{ListenInstance, Message, MessageId},
    native_channel::{NativeEnvelope, SessionEnvelope},
    native_demux::ChannelComposition,
    raw_session::{
        self,
        engine::{control_instance, envelope},
    },
    Channel, ChannelLimits, Error, Result,
};

/// Establish one accepted, explicitly trusted numeric-loopback native channel.
///
/// The caller owns listening and domain admission; [`crate::Endpoint`] owns
/// dialing. Both actual socket endpoints must be loopback. A bounded native
/// ListenInstance exchange uses the accepted role (nonempty local nonce, empty
/// initiating nonce), then publishes the existing common channel engine with
/// exactly the supplied composition and limits. Advertised peer metadata is
/// diagnostic only; the channel target remains the actual connected peer.
///
/// One absolute deadline covers hello and publication. Error or cancellation
/// disposes the unpublished socket. An established channel retains the normal
/// Channel shutdown/disposal ownership. This is not IPC: no host/PID identity
/// is added or admitted. Loopback and hello are not authentication, FM/node
/// admission, semantic completion or service readiness.
pub async fn accept_unsecured_loopback(
    stream: TcpStream,
    composition: ChannelComposition,
    limits: ChannelLimits,
    deadline: Instant,
) -> Result<Channel> {
    limits.validate()?;
    let local_address = stream.local_addr()?;
    let peer = stream.peer_addr()?;
    if !local_address.ip().is_loopback()
        || local_address.port() == 0
        || !peer.ip().is_loopback()
        || peer.port() == 0
    {
        return Err(Error::InvalidInput(
            "accepted native channel requires loopback",
        ));
    }
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    let process_id = uuid::Uuid::new_v4().to_bytes_le();
    let id = MessageId {
        guid: process_id,
        index: 1,
    };
    let local = ListenInstance {
        address: local_address.to_string(),
        instance: 1,
        // establish replaces this with a fresh nonempty accepted-role nonce.
        // A nonempty GUID here reserves that same wire size before any I/O.
        nonce: process_id,
    };
    if Message::listen_instance(id, &local)?
        .to_frame()?
        .encode()?
        .len()
        > limits.max_frame_bytes
    {
        return Err(Error::ResourceLimit("native ListenInstance frame limit"));
    }
    stream.set_nodelay(true)?;
    let (stream, hello) = raw_session::establish(
        stream,
        raw_session::Role::Accepted,
        local,
        id,
        raw_session::Limits {
            max_frame_bytes: limits.max_frame_bytes,
            frame_time: MAX_FRAME_TIME,
        },
        deadline,
        |_, _| Ok(raw_session::Admission::Retain(())),
    )
    .await?;
    let props = envelope(&hello.message, true)?;
    let remote = control_instance(&hello.message, &props)?;
    NativeEnvelope.incoming(&hello.message)?;
    if props.id == MessageId::default() || remote.nonce != [0; 16] {
        return Err(Error::Protocol("invalid initiating native ListenInstance"));
    }
    Channel::adopt_established(
        stream,
        composition,
        limits,
        process_id,
        remote,
        deadline,
        Arc::new(NativeEnvelope),
    )
}
