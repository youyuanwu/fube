//! Private source-corresponding hosting records. Native source baseline:
//! 3988b451, Hosting2 and ServiceModel FABRIC_FIELDS declarations.
use fube_transport::serialization::serde::{ByteArray, Guid, ObjectArray, Pointer};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostContext {
    pub id: String,
    pub kind: i64,
    pub pid: u32,
    pub container: bool,
    pub activator: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationId {
    pub type_name: String,
    pub number: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageId {
    pub application: ApplicationId,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Activation {
    pub guid: Guid,
    pub mode: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageInstance {
    pub package: PackageId,
    pub activation: Activation,
    pub public_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeId {
    pub package: PackageInstance,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rollout {
    pub major: u32,
    pub minor: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationVersion {
    pub value: Rollout,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageVersion {
    pub application: ApplicationVersion,
    pub value: Rollout,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionInstance {
    pub version: PackageVersion,
    pub instance: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeContext {
    pub id: CodeId,
    pub code_sequence: i64,
    pub package_sequence: i64,
    pub version: VersionInstance,
    pub application_name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeContext {
    pub id: String,
    pub host: HostContext,
    pub code: CodeContext,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRequest {
    pub id: String,
    pub kind: i64,
    pub pid: u32,
    pub timeout: i64,
    pub container: bool,
    pub activator: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pair {
    pub key: String,
    pub value: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedLog {
    pub path: ByteArray,
    pub disk: Guid,
    pub container: Guid,
    pub size: i64,
    pub streams: u32,
    pub record: u32,
    pub flags: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartReply {
    pub node_id: String,
    pub node_pid: u32,
    pub lease_handle: u64,
    pub lease_duration: i64,
    pub error: i64,
    pub node_instance: u64,
    pub node_name: String,
    pub node_type: String,
    pub client_address: String,
    pub deployment: String,
    pub ip: String,
    pub work: String,
    pub initial_ttl: i64,
    pub application_directories: ObjectArray<Vec<Pair>>,
    pub node_directories: ObjectArray<Vec<Pair>>,
    pub application_log: Pointer<SharedLog>,
    pub system_log: Pointer<SharedLog>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdRequest {
    pub id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRequest {
    pub context: RuntimeContext,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorReply {
    pub error: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseReply {
    pub ttl: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityId {
    pub guid: Guid,
    pub index: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub id: ActivityId,
    pub kind: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterHost {
    pub activity: Activity,
    pub id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeActivation {
    pub host: String,
    pub instance: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateContext {
    pub code: CodeContext,
    pub activation: CodeActivation,
    pub timeout: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeId {
    pub package: PackageInstance,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeRequest {
    pub context: RuntimeContext,
    pub id: TypeId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionId {
    pub id: Guid,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyUnit {
    pub id: PartitionId,
    pub low: i64,
    pub high: i64,
    pub name: String,
    pub kind: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Epoch {
    pub configuration: i64,
    pub data_loss: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverUnit {
    pub id: PartitionId,
    pub consistency: ConsistencyUnit,
    pub current: Epoch,
    pub previous: Epoch,
    pub target: i32,
    pub minimum: i32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: ByteArray,
    pub instance: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceType {
    pub package: PackageId,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Correlation {
    pub service: String,
    pub scheme: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub weight: i64,
    pub primary: u32,
    pub secondary: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Placement {
    pub domain: String,
    pub kind: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub correlations: ObjectArray<Vec<Correlation>>,
    pub constraints: String,
    pub scaleout: i32,
    pub metrics: ObjectArray<Vec<Metric>>,
    pub move_cost: u32,
    pub restart_wait: i64,
    pub quorum_wait: i64,
    pub update_version: u64,
    pub placement: ObjectArray<Vec<Placement>>,
    pub standby_keep: i64,
    pub activation_mode: i64,
    pub dns: String,
    pub scaling: fube_transport::serialization::serde::EmptyObjectArray,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyRequest {
    pub runtime: String,
    pub unit: FailoverUnit,
    pub local: Replica,
    pub remote: ObjectArray<Vec<Replica>>,
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
    pub error: i64,
    pub message: String,
    pub api: ApiName,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyReply {
    pub unit: FailoverUnit,
    pub local: Replica,
    pub remote: ObjectArray<Vec<Replica>>,
    pub error: ProxyError,
    pub flags: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMessage {
    pub unit: FailoverUnit,
    pub local: Replica,
    pub service: Service,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateServiceReply {
    pub error: i64,
    pub unit: FailoverUnit,
    pub local: Replica,
    pub service: Service,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyRecord {}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaDetail {
    pub kind: i64,
    pub service: String,
    pub partition: Guid,
    pub replica_id: i64,
    pub instance_id: i64,
    pub operation: i64,
    pub operation_start: i64,
    pub replicator_operation: i64,
    pub loads: ObjectArray<Vec<ReportedLoad>>,
    pub read_status: i64,
    pub write_status: i64,
    pub replicator: Pointer<EmptyRecord>,
    pub replica: Pointer<EmptyRecord>,
    pub deployed_replica: Pointer<EmptyRecord>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryReply {
    pub error: i64,
    pub timestamp: i64,
    pub unit: FailoverUnit,
    pub local: Replica,
    pub detail: ReplicaDetail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportedLoad {
    pub name: String,
    pub value: u32,
    pub time: i64,
    pub current: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadStats {
    pub name: String,
    pub value: u32,
    pub count: u32,
    pub weighted_sum: f64,
    pub weight: f64,
    pub time: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLoads {
    pub node: ByteArray,
    pub loads: ObjectArray<Vec<LoadStats>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadDescription {
    pub partition: Guid,
    pub service: String,
    pub stateful: bool,
    pub primary: ObjectArray<Vec<LoadStats>>,
    pub secondary: ObjectArray<Vec<LoadStats>>,
    pub nodes: ObjectArray<Vec<NodeLoads>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBody {
    pub legacy: fube_transport::serialization::serde::EmptyObjectArray,
    pub reports: ObjectArray<Vec<LoadDescription>>,
    pub time: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultBody {
    pub unit: FailoverUnit,
    pub local: Replica,
    pub fault: i64,
    pub activity: Activity,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthEntity {
    #[serde(rename = "1")]
    Replica {
        partition: Guid,
        replica: i64,
        replica_instance: i64,
    },
    #[serde(rename = "2")]
    Instance { partition: Guid, instance: i64 },
    #[serde(rename = "3")]
    Partition { partition: Guid },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attributes {
    pub values: fube_transport::serialization::serde::EmptyObjectArray,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityHealth {
    pub entity: Pointer<fube_transport::serialization::serde::NativeEnum<HealthEntity>>,
    pub source: String,
    pub property: String,
    pub ttl: i64,
    pub state: i64,
    pub description: String,
    pub sequence: i64,
    pub time: i64,
    pub attributes: Attributes,
    pub remove: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthBody {
    pub reports: ObjectArray<Vec<EntityHealth>>,
    pub streams: fube_transport::serialization::serde::EmptyObjectArray,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResult {
    pub kind: i64,
    pub source: String,
    pub sequence: i64,
    pub entity_property: String,
    pub error: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResult {
    pub kind: i64,
    pub source: String,
    pub instance: i64,
    pub upto: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReply {
    pub results: ObjectArray<Vec<HealthResult>>,
    pub streams: ObjectArray<Vec<StreamResult>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fube_transport::serialization::serde::{from_slice, to_vec};
    #[test]
    fn start_reply_recursive_maps_and_pointers_and_all_truncations() {
        let reply = StartReply {
            node_id: "abcdef".into(),
            node_pid: 42,
            lease_handle: 123,
            lease_duration: 300_000_000,
            error: 0,
            node_instance: 55,
            node_name: "N0010".into(),
            node_type: "NodeType0".into(),
            client_address: "172.30.91.11:19000".into(),
            deployment: "/apps".into(),
            ip: "172.30.91.11".into(),
            work: "/work".into(),
            initial_ttl: 250_000_000,
            application_directories: ObjectArray(vec![Pair {
                key: "name".into(),
                value: "path".into(),
            }]),
            node_directories: ObjectArray(vec![]),
            application_log: Pointer(Some(SharedLog {
                path: ByteArray(vec![0; 1024]),
                disk: Guid([2; 16]),
                container: Guid([3; 16]),
                size: 1024,
                streams: 128,
                record: 512,
                flags: 0,
            })),
            system_log: Pointer(None),
        };
        let bytes = to_vec(&reply).unwrap();
        let decoded: StartReply = from_slice(&bytes).unwrap();
        assert_eq!(decoded.application_directories.0[0].value, "path");
        assert_eq!(decoded.application_log.0.unwrap().path.0.len(), 1024);
        assert_eq!(decoded.initial_ttl, 250_000_000);
        for n in 0..bytes.len() {
            assert!(from_slice::<StartReply>(&bytes[..n]).is_err());
        }
    }
}
