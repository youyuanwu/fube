use std::borrow::Cow;

use mssf_transport::{message::Message, serialization::Decoder};

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
    Ok(NodeHealth {
        node_name: decoder.string()?,
        health_events: decoder.object_array(decode_event)?,
        aggregated_health_state: HealthState::from_raw(decoder.i64()?),
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
                    Ok(HealthEvaluation::Event(EventHealthEvaluation {
                        description: decoder.string()?,
                        unhealthy_event: decoder.object(decode_event)?,
                        consider_warning_as_error: decoder.boolean()?,
                        aggregated_health_state: HealthState::from_raw(decoder.i64()?),
                    }))
                })?
                .ok_or(Error::Protocol("null health evaluation"))
        })?,
    })
}

fn decode_event(decoder: &mut Decoder<'_>) -> Result<HealthEvent> {
    Ok(HealthEvent {
        source_id: decoder.string()?,
        property: decoder.string()?,
        time_to_live_ticks: decoder.i64()?,
        health_state: HealthState::from_raw(decoder.i64()?),
        description: decoder.string()?,
        sequence_number: decoder.i64()?,
        source_utc_timestamp: NodeTimestamp(decoder.i64()?),
        last_modified_utc_timestamp: NodeTimestamp(decoder.i64()?),
        is_expired: decoder.boolean()?,
        remove_when_expired: decoder.boolean()?,
        last_ok_transition_at: NodeTimestamp(decoder.i64()?),
        last_warning_transition_at: NodeTimestamp(decoder.i64()?),
        last_error_transition_at: NodeTimestamp(decoder.i64()?),
    })
}
