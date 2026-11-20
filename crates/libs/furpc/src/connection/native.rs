//! Clear ListenInstance follows Transport/TcpConnection.cpp's initial enqueue
//! and ListenInstance.h at the pinned native baseline. Advertisements are
//! diagnostics, not authenticated identity or application/domain readiness.

use super::*;
use crate::{header, ActorId, Body, HeaderRecord, Headers, MessageId};

pub(super) const TRANSPORT: i64 = 1;
pub(super) const SEND_TARGET: i64 = 29;
pub(super) const SECURITY: i64 = 35;

pub(super) fn is_control_actor(actor: i64) -> bool {
    matches!(actor, TRANSPORT | SEND_TARGET | SECURITY)
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Role {
    Initiating,
    Accepted,
}

pub(crate) struct LocalInstance {
    pub(crate) address: String,
    pub(crate) instance: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ListenInstance {
    pub(crate) address: Vec<u16>,
    pub(crate) instance: u64,
    pub(crate) nonce: [u8; 16],
}

#[derive(serde::Serialize)]
struct LocalHello<'a> {
    address: &'a str,
    instance: u64,
    #[serde(with = "fubst::serde::guid")]
    nonce: [u8; 16],
}

#[derive(serde::Serialize)]
struct Boolean {
    value: bool,
}

pub(crate) fn hello(
    local: LocalInstance,
    role: Role,
    config: &ConnectionConfig,
) -> ConnectionResult<Message> {
    let nonce = match role {
        Role::Initiating => [0; 16],
        Role::Accepted => uuid::Uuid::new_v4().to_bytes_le(),
    };
    let bytes = fubst::serde::to_vec_with_limits(
        &LocalHello {
            address: &local.address,
            instance: local.instance,
            nonce,
        },
        config.headers.control_limits(config.send_frame_limit()),
    )
    .map_err(|error| Arc::new(ConnectionError::Control(error)))?;
    let message = Message::builder()
        .append_header(HeaderRecord::native(
            header::MESSAGE_ID,
            &MessageId {
                guid: uuid::Uuid::new_v4().to_bytes_le(),
                index: 1,
            },
        )?)
        .append_header(header::actor(ActorId::new(TRANSPORT)?)?)
        .append_header(HeaderRecord::native(
            header::HIGH_PRIORITY,
            &Boolean { value: true },
        )?)
        .body(Body::from_bytes(bytes)?)
        .build()?;
    message.validate(&config.headers, config.send_frame_limit())?;
    Ok(message)
}

pub(super) fn check_hello_headers(headers: &Headers) -> ConnectionResult<()> {
    let common = headers.effective();
    if common.actor.get() != TRANSPORT
        || !common.action().is_empty()
        || common.message_id == MessageId::default()
        || common.relates_to != MessageId::default()
        || common.uncorrelated_reply
    {
        return Err(Error::Protocol("expected clear native ListenInstance").into());
    }
    Ok(())
}

pub(super) fn decode_hello(
    body: &Body,
    role: Role,
    config: &ConnectionConfig,
) -> ConnectionResult<ListenInstance> {
    let mut segments = body.segments();
    let bytes = segments.next().unwrap_or(&[]);
    if segments.next().is_some() {
        return Err(Error::Internal("received control body is not contiguous").into());
    }
    let decode = || {
        let mut decoder = fubst::Decoder::with_limits(
            bytes,
            config
                .headers
                .control_limits(config.max_receive_frame_bytes),
        )?;
        let result = decoder.object(|decoder| {
            let raw = if decoder.is_scope_end()? {
                &[][..]
            } else {
                decoder.string_utf16_bytes()?
            };
            let mut address = Vec::new();
            address
                .try_reserve_exact(raw.len() / 2)
                .map_err(|_| fubst::Error::ResourceLimit("ListenInstance address"))?;
            address.extend(
                raw.as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u16::from_le_bytes(*pair)),
            );
            let instance = if decoder.is_scope_end()? {
                0
            } else {
                decoder.u64()?
            };
            let nonce = if decoder.is_scope_end()? {
                [0; 16]
            } else {
                decoder.guid()?
            };
            Ok(ListenInstance {
                address,
                instance,
                nonce,
            })
        })?;
        decoder.finish()?;
        Ok(result)
    };
    let instance = decode().map_err(|error| Arc::new(ConnectionError::Control(error)))?;
    let valid_nonce = match role {
        Role::Initiating => instance.nonce != [0; 16],
        Role::Accepted => instance.nonce == [0; 16],
    };
    if !valid_nonce {
        return Err(Error::Protocol("invalid peer ListenInstance role nonce").into());
    }
    Ok(instance)
}
