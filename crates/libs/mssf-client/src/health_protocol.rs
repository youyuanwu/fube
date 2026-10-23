use std::borrow::Cow;

use mssf_transport::serialization::serde::{from_fields, ObjectArray};
use mssf_transport::{message::Message, serialization::Decoder};
use serde::Deserialize;

use crate::{health::*, Error, HealthState, NodeTimestamp, Result};

pub(crate) fn encode_query(query: &NodeHealthQueryDescription) -> Result<Vec<u8>> {
    // Bound caller work before UTF-16 scanning, not just before wire allocation.
    let name = &query.node_name;
    if name.is_empty()
        || name.len() > 4096 * 3
        || name.contains('\0')
        || name.encode_utf16().count() > 4096
    {
        return Err(Error::InvalidInput(
            "node health name must be 1..=4096 UTF-16 units without NUL",
        ));
    }
    let mut args = Vec::with_capacity(3);
    if let Some(policy) = query.health_policy {
        args.push((
            "ClusterHealthPolicy",
            Cow::Owned(format!(
                "{{\"ConsiderWarningAsError\":{}}}",
                policy.consider_warning_as_error
            )),
        ));
    }
    if let Some(filter) = query.events_filter {
        args.push((
            "EventsFilter",
            Cow::Owned(format!("{{\"HealthStateFilter\":{}}}", filter.bits())),
        ));
    }
    args.push(("NodeName", Cow::Borrowed(name.as_str())));
    crate::query_protocol::encode("GetNodeHealth", &args)
}

pub(crate) fn decode_reply(message: &Message) -> Result<NodeHealth> {
    crate::protocol::check_failure(message)?;
    let properties = message.properties()?;
    if properties.actor != 16 || properties.action != "OperationSuccessAction" {
        return Err(Error::Protocol("unexpected node health success envelope"));
    }
    let result = crate::query_protocol::decode(&message.body, |decoder, tag| {
        if tag != 16i32.to_le_bytes() {
            return Err(Error::Protocol("query wrapper is not native NodeHealth"));
        }
        // An item constructor serializes an empty list, then an untyped item.
        // Decode no speculative list payload if that invariant is violated.
        decoder.object_array::<()>(|_| Err(Error::Protocol("node health item contains a list")))?;
        decoder
            .pointer(|decoder, tag| {
                if !tag.is_empty() {
                    return Err(Error::Protocol("unexpected node health item type tag"));
                }
                decode_health(decoder)
            })?
            .ok_or(Error::Protocol("null node health item"))
    })?;
    if result.kind != 1 || result.paging.is_some() {
        return Err(Error::Protocol("node health result is not an unpaged Item"));
    }
    result
        .payload
        .ok_or(Error::Protocol("node health has no typed wrapper"))
}

fn decode_health(decoder: &mut Decoder<'_>) -> Result<NodeHealth> {
    #[derive(Deserialize)]
    struct Prefix {
        node_name: String,
        health_events: ObjectArray<Vec<EventWire>>,
        aggregated_health_state: i64,
    }
    #[derive(Deserialize)]
    struct Evaluation {
        description: String,
        unhealthy_event: EventWire,
        consider_warning_as_error: bool,
        aggregated_health_state: i64,
    }
    let prefix: Prefix = from_fields(decoder)?;
    Ok(NodeHealth {
        node_name: prefix.node_name,
        health_events: prefix.health_events.0.into_iter().map(Into::into).collect(),
        aggregated_health_state: HealthState::from_raw(prefix.aggregated_health_state),
        unhealthy_evaluations: decoder.object_array(|decoder| {
            decoder
                .pointer(|decoder, tag| {
                    if tag != 1i32.to_le_bytes() {
                        return Err(Error::Protocol(
                            "unsupported health evaluation dynamic kind",
                        ));
                    }
                    if decoder.i64()? != 1 {
                        return Err(Error::Protocol("health evaluation tag and kind disagree"));
                    }
                    let wire: Evaluation = from_fields(decoder)?;
                    Ok(HealthEvaluation::Event(EventHealthEvaluation {
                        description: wire.description,
                        unhealthy_event: wire.unhealthy_event.into(),
                        consider_warning_as_error: wire.consider_warning_as_error,
                        aggregated_health_state: HealthState::from_raw(
                            wire.aggregated_health_state,
                        ),
                    }))
                })?
                .ok_or(Error::Protocol("null health evaluation"))
        })?,
    })
}

#[derive(Deserialize)]
struct EventWire {
    source_id: String,
    property: String,
    time_to_live_ticks: i64,
    health_state: i64,
    description: String,
    sequence_number: i64,
    source_utc_timestamp: i64,
    last_modified_utc_timestamp: i64,
    is_expired: bool,
    remove_when_expired: bool,
    last_ok_transition_at: i64,
    last_warning_transition_at: i64,
    last_error_transition_at: i64,
}

impl From<EventWire> for HealthEvent {
    fn from(wire: EventWire) -> Self {
        Self {
            source_id: wire.source_id,
            property: wire.property,
            time_to_live_ticks: wire.time_to_live_ticks,
            health_state: HealthState::from_raw(wire.health_state),
            description: wire.description,
            sequence_number: wire.sequence_number,
            source_utc_timestamp: NodeTimestamp(wire.source_utc_timestamp),
            last_modified_utc_timestamp: NodeTimestamp(wire.last_modified_utc_timestamp),
            is_expired: wire.is_expired,
            remove_when_expired: wire.remove_when_expired,
            last_ok_transition_at: NodeTimestamp(wire.last_ok_transition_at),
            last_warning_transition_at: NodeTimestamp(wire.last_warning_transition_at),
            last_error_transition_at: NodeTimestamp(wire.last_error_transition_at),
        }
    }
}
