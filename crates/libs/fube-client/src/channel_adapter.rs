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

fn gateway_outbound(_: &Message, props: &Properties, _: OutboundKind) -> Result<()> {
    if props.actor != 5 || props.action.is_empty() {
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        Message {
            headers: vec![],
            body: vec![],
        }
    }

    #[test]
    fn all_client_compositions_reject_upload_rpc_fallthrough() {
        for composition in [
            ordinary().unwrap(),
            notifications().unwrap(),
            upload().unwrap(),
        ] {
            for actor in [25, 27] {
                let props = Properties {
                    actor,
                    action: "ClientOperationSuccess".into(),
                    relates_to: MessageId {
                        guid: [9; 16],
                        index: 1,
                    },
                    ..Default::default()
                };
                assert!(matches!(
                    (composition.ordinary_reply)(&message(), &props),
                    Err(Error::Protocol(_))
                ));
            }
            // Ordinary domain checking stays above correlation, not replaced
            // by the strict raw upload decoder's header uniqueness rules.
            let props = Properties {
                actor: 123,
                ..Default::default()
            };
            (composition.ordinary_reply)(&message(), &props).unwrap();
        }
    }

    #[test]
    fn client_sender_authorization_is_separate_from_actor_registration() {
        for kind in [OutboundKind::Raw, OutboundKind::Rpc] {
            for actor in [5, 25, 26, 27, 70000] {
                let props = Properties {
                    actor,
                    action: "DomainAction".into(),
                    ..Default::default()
                };
                for composition in [ordinary().unwrap(), notifications().unwrap()] {
                    assert_eq!(
                        (composition.outbound)(&message(), &props, kind).is_ok(),
                        actor == 5
                    );
                }
                let composition = upload().unwrap();
                let expected = if kind == OutboundKind::Raw { 26 } else { 5 };
                assert_eq!(
                    (composition.outbound)(&message(), &props, kind).is_ok(),
                    actor == expected
                );
            }
        }
        assert!(ordinary().unwrap().actors.is_empty());
        assert!(notifications().unwrap().actors.is_empty());
        assert!(ordinary().unwrap().duplex.is_none());
        assert!(upload().unwrap().duplex.is_none());
    }

    #[test]
    fn notification_grant_checks_remain_exact_and_reply_policy_is_distinct() {
        let composition = notifications().unwrap();
        let duplex = composition.duplex.unwrap();
        for variant in 0..8 {
            let mut props = Properties {
                actor: 5,
                action: "ServiceNotificationRequest".into(),
                id: MessageId {
                    guid: [9; 16],
                    index: 1,
                },
                expects_reply: true,
                uncorrelated_reply: true,
                ..Default::default()
            };
            match variant {
                0 => {}
                1 => props.actor = 27,
                2 => props.action = "Other".into(),
                3 => props.id = MessageId::default(),
                4 => props.relates_to = props.id,
                5 => props.expects_reply = false,
                6 => props.uncorrelated_reply = false,
                7 => {
                    props.fault = Some(message::Fault {
                        code: 0,
                        has_body: false,
                    })
                }
                _ => unreachable!(),
            }
            assert_eq!((duplex.incoming)(&message(), &props).is_ok(), variant == 0);
            assert!((duplex.reply)(&message(), &props).is_err());
        }
        let props = Properties {
            actor: 5,
            action: "ServiceNotificationReply".into(),
            ..Default::default()
        };
        (duplex.reply)(&message(), &props).unwrap();
        assert!((duplex.incoming)(&message(), &props).is_err());
    }
}
