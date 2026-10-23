use mssf_transport::{
    message::Message,
    serialization::serde::{Derived, Guid},
};
use serde::{Deserialize, Serialize};

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
        crate::query_protocol::decode_list::<NodeWire, _, 1>(message, false, NodeWire::into_node)
    }
}

#[derive(Deserialize)]
struct NodeWire {
    name: String,
    id: NodeId,
    ip_address_or_fqdn: String,
    node_type: String,
    code_version: String,
    config_version: String,
    status: NodeStatus,
    up_time_seconds: i64,
    health_state: HealthState,
    is_seed_node: bool,
    upgrade_domain: String,
    fault_domain: String,
    id_copy: NodeId,
    instance_id: u64,
    cluster_connection_port: u32,
    deactivation: NodeDeactivation,
    is_stopped: bool,
    down_time_seconds: i64,
    up_at: NodeTimestamp,
    down_at: NodeTimestamp,
    http_gateway_port: u16,
}

impl NodeWire {
    fn into_node(self) -> Result<Node> {
        let id = self.id;
        if self.id_copy != id {
            return Err(Error::Protocol("node ID copies disagree"));
        }
        Ok(Node {
            name: self.name,
            id,
            ip_address_or_fqdn: self.ip_address_or_fqdn,
            node_type: self.node_type,
            code_version: self.code_version,
            config_version: self.config_version,
            status: self.status,
            up_time_seconds: self.up_time_seconds,
            health_state: self.health_state,
            is_seed_node: self.is_seed_node,
            upgrade_domain: self.upgrade_domain,
            fault_domain: self.fault_domain,
            instance_id: self.instance_id,
            cluster_connection_port: self.cluster_connection_port,
            deactivation: self.deactivation,
            is_stopped: self.is_stopped,
            down_time_seconds: self.down_time_seconds,
            up_at: self.up_at,
            down_at: self.down_at,
            http_gateway_port: self.http_gateway_port,
        })
    }
}

#[derive(Deserialize)]
struct TaskId(String, NodeDeactivationTaskType);
impl<'de> Deserialize<'de> for NodeDeactivationTask {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields(TaskId, NodeDeactivationIntent);
        let Fields(TaskId(id, task_type), intent) = Fields::deserialize(deserializer)?;
        Ok(Self {
            id,
            task_type,
            intent,
        })
    }
}

pub(crate) fn deserialize_safety_checks<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<SafetyCheck>, D::Error> {
    crate::native::tagged_pointer_slots::deserialize::<SafetyWire, D>(deserializer)
        .map(|checks| checks.into_iter().map(Into::into).collect())
}

#[derive(Deserialize)]
enum SafetyWire {
    #[serde(rename = "0")]
    Invalid,
    #[serde(rename = "1")]
    EnsureSeedNodeQuorum,
    #[serde(rename = "2")]
    EnsurePartitionQuorum(PartitionWire),
    #[serde(rename = "3")]
    WaitForPrimaryPlacement(PartitionWire),
    #[serde(rename = "4")]
    WaitForPrimarySwap(PartitionWire),
    #[serde(rename = "5")]
    WaitForReconfiguration(PartitionWire),
    #[serde(rename = "6")]
    WaitForInBuildReplica(PartitionWire),
    #[serde(rename = "7")]
    EnsureAvailability(PartitionWire),
}
#[derive(Deserialize)]
struct PartitionWire {
    partition: Derived<PartitionId>,
}
#[derive(Deserialize)]
struct PartitionId {
    id: Guid,
}
impl From<SafetyWire> for SafetyCheck {
    fn from(value: SafetyWire) -> Self {
        let partition = |kind, wire: PartitionWire| Self::Partition {
            kind,
            partition_id: uuid::Uuid::from_bytes_le(wire.partition.0.id.0),
        };
        match value {
            SafetyWire::Invalid => Self::Invalid,
            SafetyWire::EnsureSeedNodeQuorum => Self::EnsureSeedNodeQuorum,
            SafetyWire::EnsurePartitionQuorum(wire) => {
                partition(SafetyCheckKind::EnsurePartitionQuorum, wire)
            }
            SafetyWire::WaitForPrimaryPlacement(wire) => {
                partition(SafetyCheckKind::WaitForPrimaryPlacement, wire)
            }
            SafetyWire::WaitForPrimarySwap(wire) => {
                partition(SafetyCheckKind::WaitForPrimarySwap, wire)
            }
            SafetyWire::WaitForReconfiguration(wire) => {
                partition(SafetyCheckKind::WaitForReconfiguration, wire)
            }
            SafetyWire::WaitForInBuildReplica(wire) => {
                partition(SafetyCheckKind::WaitForInBuildReplica, wire)
            }
            SafetyWire::EnsureAvailability(wire) => {
                partition(SafetyCheckKind::EnsureAvailability, wire)
            }
        }
    }
}
