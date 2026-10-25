//! Native RA12 stateless subset. Source: ServiceModel/reliability/failover/
//! and Reliability/Failover/{common,ra}/ declarations at the pinned revision.
use crate::wire::{PackageId, VersionInstance};
use mssf_transport::serialization::serde::{ByteArray, EmptyObjectArray, Guid};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionId {
    pub id: Guid,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsistencyUnit {
    pub id: PartitionId,
    pub low: i64,
    pub high: i64,
    pub name: String,
    pub kind: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Epoch {
    pub configuration: i64,
    pub data_loss: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailoverUnit {
    pub id: PartitionId,
    pub consistency: ConsistencyUnit,
    pub current: Epoch,
    pub previous: Epoch,
    pub target: i32,
    pub minimum: i32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    // Native NodeId is FABRIC_SERIALIZE_AS_BYTEARRAY, high then low LE words.
    pub id: ByteArray,
    pub instance: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Replica {
    pub node: Node,
    pub id: i64,
    pub instance: i64,
    pub current_role: i64,
    pub previous_role: i64,
    pub up: bool,
    pub last_lsn: i64,
    pub first_lsn: i64,
    pub state: i64,
    pub version: VersionInstance,
    pub service_endpoint: String,
    pub replication_endpoint: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct ServiceType {
    pub package: PackageId,
    pub name: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct Service {
    pub name: String,
    pub instance: u64,
    pub service_type: ServiceType,
    pub application: String,
    pub version: VersionInstance,
    pub partitions: i32,
    pub target: i32,
    pub minimum: i32,
    pub stateful: bool,
    pub persisted: bool,
    pub initialization: ByteArray,
    pub group: bool,
    pub correlations: EmptyObjectArray,
    pub constraints: String,
    pub scaleout: i32,
    pub metrics: EmptyObjectArray,
    pub move_cost: u32,
    pub restart_wait: i64,
    pub quorum_wait: i64,
    pub update_version: u64,
    pub placement: EmptyObjectArray,
    pub standby_keep: i64,
    pub activation_mode: i64,
    pub dns: String,
    pub scaling: EmptyObjectArray,
}
#[derive(Debug, Clone, Serialize)]
pub struct ProxyRequest {
    pub runtime: String,
    pub unit: FailoverUnit,
    pub local: Replica,
    pub remote: EmptyObjectArray,
    pub service: Service,
    pub flags: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiName {
    pub api: i64,
    pub interface: i64,
    pub metadata: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyError {
    pub code: i64,
    pub message: String,
    pub api: ApiName,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyReply {
    pub unit: FailoverUnit,
    pub local: Replica,
    pub remote: EmptyObjectArray,
    pub error: ProxyError,
    pub flags: i64,
}
