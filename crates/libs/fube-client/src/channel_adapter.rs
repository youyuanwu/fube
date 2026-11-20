//! Client-owned native envelope authorization, separate from actor selection.
//!
//! These stateless checks run before channel admission. Domain body decoding,
//! upload correlation and notification semantic processing remain in their
//! respective client protocols, outside the connection driver.

use fube_transport::{
    message::{self, Message, MessageId, Properties},
    native_demux::{ActorDemux, ActorRegistration, ChannelComposition, DuplexPolicy, OutboundKind},
};

use crate::{Error, Result};

fn gateway_outbound(_: &Message, props: &Properties, kind: OutboundKind) -> Result<()> {
    if kind == OutboundKind::Reserved || props.actor != 5 || props.action.is_empty() {
        return Err(Error::InvalidInput("unauthorized gateway submission"));
    }
    Ok(())
}

fn ordinary_reply(_: &Message, props: &Properties) -> Result<()> {
    // An upload envelope must never acquire RPC authority through RelatesTo.
    // Other ordinary domain actors/actions stay operation-local as before.
    if matches!(props.actor, 25 | 27) {
        return Err(Error::Protocol(
            "upload envelope cannot complete ordinary RPC",
        ));
    }
    Ok(())
}

pub(crate) fn ordinary() -> Result<ChannelComposition> {
    Ok(ChannelComposition {
        outbound: gateway_outbound,
        ordinary_reply,
        actors: ActorDemux::new(vec![])?,
        duplex: None,
        requests: None,
        ordered_ingress: false,
    })
}

fn notification_incoming(_: &Message, props: &Properties) -> Result<()> {
    if props.actor != 5
        || props.action != "ServiceNotificationRequest"
        || props.id == MessageId::default()
        || props.relates_to != MessageId::default()
        || !props.expects_reply
        || !props.uncorrelated_reply
        || props.fault.is_some()
    {
        return Err(Error::Protocol("unauthorized native notification envelope"));
    }
    Ok(())
}

fn notification_reply(_: &Message, props: &Properties) -> Result<()> {
    if props.actor != 5 || props.action != "ServiceNotificationReply" {
        return Err(Error::InvalidInput("unauthorized notification response"));
    }
    Ok(())
}

pub(crate) fn notifications() -> Result<ChannelComposition> {
    Ok(ChannelComposition {
        duplex: Some(DuplexPolicy {
            incoming: notification_incoming,
            reply: notification_reply,
        }),
        ..ordinary()?
    })
}

fn upload_outbound(_: &Message, props: &Properties, kind: OutboundKind) -> Result<()> {
    let actor = match kind {
        OutboundKind::Raw => 26,
        OutboundKind::Rpc => 5,
        OutboundKind::Reserved => return Err(Error::InvalidInput("reserved upload send")),
    };
    if props.actor != actor || props.action.is_empty() {
        return Err(Error::InvalidInput("unauthorized upload submission"));
    }
    Ok(())
}

fn upload_incoming(message: &Message, props: &Properties) -> Result<()> {
    if !matches!(props.actor, 25 | 27)
        || props.action.is_empty()
        || props.relates_to != MessageId::default()
        || props.expects_reply
        || props.uncorrelated_reply
        || props.fault.is_some()
        || message
            .headers
            .iter()
            .any(|header| header.id == message::IPC)
    {
        return Err(Error::Protocol("unauthorized native upload envelope"));
    }
    Ok(())
}

pub(crate) fn upload() -> Result<ChannelComposition> {
    Ok(ChannelComposition {
        outbound: upload_outbound,
        ordinary_reply,
        actors: ActorDemux::new(
            [25, 27]
                .into_iter()
                .map(|actor| ActorRegistration {
                    actor,
                    authorize: upload_incoming,
                })
                .collect(),
        )?,
        duplex: None,
        requests: None,
        ordered_ingress: false,
    })
}
