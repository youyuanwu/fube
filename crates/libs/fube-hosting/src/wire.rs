//! Hosting2 ordered wire records, pinned native baseline 3988b451.
//! This bootstrap-only schema does not enable factories, reports or deployment.
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

impl RuntimeContext {
    pub(crate) fn project(&self) -> crate::RuntimeContext {
        let package = &self.code.id.package;
        crate::RuntimeContext {
            runtime_id: self.id.clone(),
            host_id: self.host.id.clone(),
            process_id: self.host.pid,
            package: crate::PackageInstance {
                package: crate::PackageId {
                    application: crate::ApplicationId {
                        type_name: package.package.application.type_name.clone(),
                        number: package.package.application.number,
                    },
                    name: package.package.name.clone(),
                },
                activation: crate::Activation(
                    *uuid::Uuid::from_bytes_le(package.activation.guid.0).as_bytes(),
                ),
                public_activation: package.public_id.clone(),
            },
            version: crate::VersionInstance {
                application: crate::Rollout {
                    major: self.code.version.version.application.value.major,
                    minor: self.code.version.version.application.value.minor,
                },
                package: crate::Rollout {
                    major: self.code.version.version.value.major,
                    minor: self.code.version.version.value.minor,
                },
                instance: self.code.version.instance,
            },
            code_package: self.code.id.name.clone(),
            // ServiceTypeRegistrationHelper consumes the package, not code, sequence.
            package_instance_sequence: self.code.package_sequence,
        }
    }
}
