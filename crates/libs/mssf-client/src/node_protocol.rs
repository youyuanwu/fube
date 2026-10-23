use mssf_transport::{message::Message, serialization::Decoder};
use serde::Serialize;

use crate::{
    nodes::*, query_arguments::QueryArguments, query_protocol::Query, Error, Paging, Result,
};

impl Query for NodeQueryDescription {
    type Output = NodeListPage;
    const NAME: &'static str = "GetNodeList";
    const PAGING_FIRST: bool = true;

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Fields<'a> {
            node_name: Option<&'a str>,
            node_status_filter: Option<NodeStatusFilter>,
        }
        arguments.append(&Fields {
            node_name: self
                .parameters
                .node_name
                .as_deref()
                .filter(|s| !s.is_empty()),
            node_status_filter: (self.parameters.status_filter != NodeStatusFilter::DEFAULT)
                .then_some(self.parameters.status_filter),
        })
    }

    fn paging(&self) -> Option<&Paging> {
        Some(&self.paging)
    }

    fn decode_reply(message: &Message) -> Result<NodeListPage> {
        crate::query_protocol::decode_list(message, 1, false, decode_node)
    }
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
