//! Minimal native actor demultiplexing, not the declared actor/action router.
//!
//! SF `DemuxerT.h:188-227` at revision
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` selects actors only for empty
//! effective RelatesTo and nonspecial messages. Other messages enter the reply
//! path; special replies are then separated by DuplexRequestReply.
//!
//! These immutable registrations select bounded pull ingress. They are not
//! executors, sender permissions or reply grants. Envelope validators are
//! trusted, stateless composition code: they must not block, perform I/O or
//! decode domain bodies. Domain processing happens after receive, not in the
//! socket driver. Unlike native log/drop, unsupported ingress is an error.

use crate::{
    message::{Message, MessageId, Properties},
    Error, Result,
};

/// A bounded, synchronous envelope check, independent of domain processing.
pub type EnvelopeValidator = fn(&Message, &Properties) -> Result<()>;

/// Distinct outbound mechanisms. Neither permits replying to an ingress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundKind {
    Raw,
    Rpc,
}

/// Explicit sender authorization; actor registration never supplies this.
pub type OutboundValidator = fn(&Message, &Properties, OutboundKind) -> Result<()>;

/// One actor's ingress selection and authorization, without reply authority.
pub struct ActorRegistration {
    pub actor: i64,
    pub authorize: EnvelopeValidator,
}

/// Immutable, finite native actor table. Action is not a lookup key.
pub struct ActorDemux {
    actors: Box<[ActorRegistration]>,
}

/// Maximum number of connect-time actor registrations.
pub const MAX_ACTOR_REGISTRATIONS: usize = 32;

pub(crate) fn is_control(actor: i64) -> bool {
    matches!(actor, 1 | 21 | 29 | 35 | 36)
}

pub(crate) enum Dispatch {
    Actor(EnvelopeValidator),
    Reply,
}

impl ActorDemux {
    /// Validate registrations before connection establishment.
    ///
    /// Nonpositive, duplicate and reserved control actors are rejected.
    /// Shrinking the table prevents caller vector capacity becoming uncharged
    /// retained state. Registration cannot intercept controls or RPC replies.
    pub fn new(actors: Vec<ActorRegistration>) -> Result<Self> {
        if actors.len() > MAX_ACTOR_REGISTRATIONS {
            return Err(Error::InvalidInput("too many native actor registrations"));
        }
        for (index, entry) in actors.iter().enumerate() {
            if entry.actor <= 0
                || is_control(entry.actor)
                || actors[..index]
                    .iter()
                    .any(|other| other.actor == entry.actor)
            {
                return Err(Error::InvalidInput("invalid or duplicate native actor"));
            }
        }
        Ok(Self {
            actors: actors.into_boxed_slice(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    pub(crate) fn select(&self, props: &Properties) -> Result<Dispatch> {
        if props.relates_to != MessageId::default() || props.uncorrelated_reply {
            return Ok(Dispatch::Reply);
        }
        self.actors
            .iter()
            .find(|entry| entry.actor == props.actor)
            .map(|entry| Dispatch::Actor(entry.authorize))
            .ok_or(Error::Protocol("unregistered native actor ingress"))
    }
}

/// Separate authorization for special ingress and its consuming response.
///
/// The channel captures `reply` only after `incoming` succeeds. Neither actor
/// registration nor public IncomingMessage assembly can create that context.
pub struct DuplexPolicy {
    pub incoming: EnvelopeValidator,
    pub reply: EnvelopeValidator,
}

/// Connect-time composition of independent mechanism authorization.
///
/// Explicit trusted policy is required even with no actor registrations.
/// Ordinary reply policy runs before request-table lookup, allowing adapters to
/// reject contradictory domain envelopes without changing native classification.
/// Common transport/correlation/affinity validation cannot be overridden.
pub struct ChannelComposition {
    pub outbound: OutboundValidator,
    pub ordinary_reply: EnvelopeValidator,
    pub actors: ActorDemux,
    pub duplex: Option<DuplexPolicy>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(_: &Message, _: &Properties) -> Result<()> {
        Ok(())
    }

    fn registration(actor: i64) -> ActorRegistration {
        ActorRegistration {
            actor,
            authorize: allow,
        }
    }

    #[test]
    fn native_registration_is_finite_unique_and_cannot_capture_controls() {
        for actor in [i64::MIN, -1, 0, 1, 21, 29, 35, 36] {
            assert!(ActorDemux::new(vec![registration(actor)]).is_err());
        }
        assert!(ActorDemux::new(vec![registration(70000), registration(70000)]).is_err());
        assert!(ActorDemux::new((70000..70033).map(registration).collect()).is_err());
        assert!(ActorDemux::new((70000..70032).map(registration).collect()).is_ok());
        assert!(ActorDemux::new(vec![]).unwrap().is_empty());
        let mut oversized_capacity = Vec::with_capacity(4096);
        oversized_capacity.push(registration(70000));
        assert_eq!(ActorDemux::new(oversized_capacity).unwrap().actors.len(), 1);
    }

    #[test]
    fn native_selection_uses_actor_not_action_or_expects_reply_and_never_reply_fallback() {
        let demux = ActorDemux::new(vec![registration(70000)]).unwrap();
        let mut props = Properties {
            actor: 70000,
            expects_reply: true,
            ..Default::default()
        };
        assert!(matches!(demux.select(&props).unwrap(), Dispatch::Actor(_)));
        props.action = "Anything".into();
        assert!(matches!(demux.select(&props).unwrap(), Dispatch::Actor(_)));
        props.actor = 70001;
        assert!(demux.select(&props).is_err());
        props.relates_to = MessageId {
            guid: [1; 16],
            index: 1,
        };
        assert!(matches!(demux.select(&props).unwrap(), Dispatch::Reply));
        props.relates_to = MessageId::default();
        props.uncorrelated_reply = true;
        assert!(matches!(demux.select(&props).unwrap(), Dispatch::Reply));
    }
}
