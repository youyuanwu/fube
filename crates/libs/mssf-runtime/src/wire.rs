//! Private source-corresponding hosting records. Native source baseline:
//! 3988b451, Hosting2 and ServiceModel FABRIC_FIELDS declarations.
use mssf_transport::serialization::serde::{ByteArray, Guid, ObjectArray, Pointer};
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

#[cfg(test)]
mod tests {
    use super::*;
    use mssf_transport::serialization::serde::{from_slice, to_vec};
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
