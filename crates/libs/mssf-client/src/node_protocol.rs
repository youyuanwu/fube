use mssf_transport::{
    message::{Message, ACTION, ACTOR},
    serialization::{Decoder, Encoder},
};

use crate::{nodes::*, Error, Result};

pub(crate) fn encode_query(query: &NodeQueryDescription) -> Result<Vec<u8>> {
    let mut args: Vec<(&str, Cow<'_, str>)> = Vec::new();
    if let Some(token) = query
        .continuation_token
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        args.push(("ContinuationToken", Cow::Borrowed(token)));
    }
    if let Some(max) = query.max_results {
        if max.get() > i64::MAX as u64 {
            return Err(Error::InvalidInput(
                "MaxResults exceeds native signed range",
            ));
        }
        args.push(("MaxResults", Cow::Owned(max.to_string())));
    }
    if let Some(name) = query.node_name.as_deref().filter(|s| !s.is_empty()) {
        args.push(("NodeName", Cow::Borrowed(name)));
    }
    if query.status_filter != NodeStatusFilter::DEFAULT {
        args.push((
            "NodeStatusFilter",
            Cow::Owned(query.status_filter.bits().to_string()),
        ));
    }
    let mut encoder = Encoder::new();
    encoder.object(|encoder| {
        encoder.object(|encoder| {
            encoder.object_array(&args, |encoder, (key, value)| {
                encoder.object(|encoder| encoder.string(key))?;
                encoder.string(value)
            })
        })?;
        encoder.string("GetNodeList")
    })?;
    Ok(encoder.finish())
}

pub(crate) fn decode_reply(message: &Message) -> Result<NodeListPage> {
    crate::protocol::check_failure(message)?;
    let properties = message.properties()?;
    // The native aggregate constructs a bare Message(QueryResult), unlike the
    // generic QueryReply factory. Absent headers are not actor/action defaults.
    if message.headers.iter().any(|h| h.id == ACTOR) && properties.actor != 5 {
        return Err(Error::Protocol("query reply is not from NamingGateway"));
    }
    if message.headers.iter().any(|h| h.id == ACTION) && properties.action != "QueryReply" {
        return Err(Error::Protocol("unexpected query reply action"));
    }
    let mut decoder = Decoder::new(&message.body);
    let (kind, nodes, error, token) = decoder.object(|decoder| {
        let kind = decoder.i64()?;
        let nodes = decoder.pointer(|decoder, tag| {
            if tag != 1i32.to_le_bytes() {
                return Err(Error::Protocol("query wrapper is not the native Node type"));
            }
            let nodes = decoder.object_array(decode_node)?;
            if decoder.pointer(|_, _| Ok(()))?.is_some() {
                return Err(Error::Protocol("node list has a single-item payload"));
            }
            Ok(nodes)
        })?;
        let error = decoder.i64()?;
        let token = decoder.pointer(|decoder, _| decoder.string())?;
        Ok((kind, nodes, error, token))
    })?;
    decoder.finish()?;
    if error != 0 {
        return Err(Error::Remote {
            code: error,
            message: String::new(),
        });
    }
    if kind != 2 {
        return Err(Error::Protocol("query result is not a List"));
    }
    let nodes = nodes.ok_or(Error::Protocol("node list has no typed wrapper"))?;
    Ok(NodeListPage {
        nodes,
        continuation_token: token.filter(|token| !token.is_empty()),
    })
}

fn decode_id(decoder: &mut Decoder<'_>) -> Result<NodeId> {
    let bytes = decoder.byte_array()?;
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Protocol("node ID must contain exactly 16 bytes"))?;
    Ok(NodeId {
        high: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        low: u64::from_le_bytes(bytes[8..].try_into().unwrap()),
    })
}

fn decode_node(decoder: &mut Decoder<'_>) -> Result<Node> {
    let name = decoder.string()?;
    let id = decode_id(decoder)?;
    let ip_address_or_fqdn = decoder.string()?;
    let node_type = decoder.string()?;
    let code_version = decoder.string()?;
    let config_version = decoder.string()?;
    let status = NodeStatus::from_raw(decoder.i64()?);
    let up_time_seconds = decoder.i64()?;
    let health_state = HealthState::from_raw(decoder.i64()?);
    let is_seed_node = decoder.boolean()?;
    let upgrade_domain = decoder.string()?;
    let fault_domain = decoder.string()?;
    if decode_id(decoder)? != id {
        return Err(Error::Protocol("node ID copies disagree"));
    }
    let instance_id = decoder.u64()?;
    let cluster_connection_port = decoder.u32()?;
    let deactivation = decoder.object(|decoder| {
        let effective_intent = NodeDeactivationIntent::from_raw(decoder.i64()?);
        let status = NodeDeactivationStatus::from_raw(decoder.i64()?);
        let tasks = decoder.object_array(|decoder| {
            let (id, task_type) = decoder.object(|decoder| {
                Ok((
                    decoder.string()?,
                    NodeDeactivationTaskType::from_raw(decoder.i64()?),
                ))
            })?;
            Ok(NodeDeactivationTask {
                id,
                task_type,
                intent: NodeDeactivationIntent::from_raw(decoder.i64()?),
            })
        })?;
        let pending_safety_checks = decoder.object_array(|decoder| {
            decoder
                .pointer(|decoder, tag| {
                    let kind = decoder.i64()?;
                    let raw: i32 = kind
                        .try_into()
                        .map_err(|_| Error::Protocol("safety-check kind out of range"))?;
                    if tag != raw.to_le_bytes() {
                        return Err(Error::Protocol("safety-check tag and kind disagree"));
                    }
                    match kind {
                        0 => Ok(SafetyCheck::Invalid),
                        1 => Ok(SafetyCheck::EnsureSeedNodeQuorum),
                        2..=7 => {
                            decoder.next_scope()?;
                            Ok(SafetyCheck::Partition {
                                kind: SafetyCheckKind::from_raw(kind),
                                partition_id: uuid::Uuid::from_bytes_le(decoder.guid()?),
                            })
                        }
                        _ => Err(Error::Protocol("unsupported safety-check dynamic kind")),
                    }
                })?
                .ok_or(Error::Protocol("null pending safety check"))
        })?;
        Ok(NodeDeactivation {
            effective_intent,
            status,
            tasks,
            pending_safety_checks,
        })
    })?;
    Ok(Node {
        name,
        id,
        ip_address_or_fqdn,
        node_type,
        code_version,
        config_version,
        status,
        up_time_seconds,
        health_state,
        is_seed_node,
        upgrade_domain,
        fault_domain,
        instance_id,
        cluster_connection_port,
        deactivation,
        is_stopped: decoder.boolean()?,
        down_time_seconds: decoder.i64()?,
        up_at: NodeTimestamp(decoder.i64()?),
        down_at: NodeTimestamp(decoder.i64()?),
        http_gateway_port: decoder.u16()?,
    })
}
use std::borrow::Cow;
