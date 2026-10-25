use mssf_transport::serialization::serde::{EmptyObjectArray, Guid, Pointer};
use serde::{Deserialize, Serialize};

pub const IPC_HEADER: u16 = 0x800b;
pub const TTL_TICKS: i64 = 30 * 10_000_000;
pub const APP_TYPE: &str = "NativeTestHost";
pub const APP_ID: &str = "NativeTestHost_App1";
pub const APP_NAME: &str = "fabric:/NativeTestHost";
pub const PACKAGE: &str = "NativeService";
pub const CODE: &str = "Code";
pub const SERVICE_TYPE: &str = "NativeStatelessType";

// Field orders below follow the pinned Hosting2 and ServiceModel FABRIC_FIELDS
// declarations inventoried in CodeResearch, not the public COM ABI.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpcIdentity {
    pub host: String,
    pub pid: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HostStart {
    pub host: String,
    pub kind: i64,
    pub pid: u32,
    pub timeout: i64,
    pub container: bool,
    pub activator: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HostId {
    pub host: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorReply {
    pub error: i64,
}

#[derive(Debug, Serialize)]
pub struct LeaseReply {
    pub ttl: i64,
}

#[derive(Debug, Serialize)]
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
    pub application_directories: EmptyObjectArray,
    pub node_directories: EmptyObjectArray,
    pub app_log: Pointer<SharedLog>,
    pub system_log: Pointer<SharedLog>,
}

#[derive(Debug, Serialize)]
pub struct SharedLog {
    pub path: mssf_transport::serialization::serde::ByteArray,
    pub disk: Guid,
    pub id: Guid,
    pub size: i64,
    pub streams: u32,
    pub record_size: u32,
    pub flags: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationId {
    pub kind: String,
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
    pub rollout: Rollout,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageVersion {
    pub application: ApplicationVersion,
    pub rollout: Rollout,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionInstance {
    pub version: PackageVersion,
    pub instance: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeContext {
    pub code: CodeId,
    pub code_sequence: i64,
    pub package_sequence: i64,
    pub version: VersionInstance,
    pub application_name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostContext {
    pub host: String,
    pub kind: i64,
    pub pid: u32,
    pub container: bool,
    pub activator: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeContext {
    pub id: String,
    pub host: HostContext,
    pub code: CodeContext,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RuntimeRegistration {
    pub context: RuntimeContext,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TypeId {
    pub package: PackageInstance,
    pub name: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct TypeRegistration {
    pub context: RuntimeContext,
    pub service_type: TypeId,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RuntimeId {
    pub id: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ActivityId {
    pub guid: Guid,
    pub index: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Activity {
    pub id: ActivityId,
    pub kind: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct HostUnregister {
    pub activity: Activity,
    pub host: String,
}

pub fn expected_code() -> CodeContext {
    CodeContext {
        code: CodeId {
            package: PackageInstance {
                package: PackageId {
                    application: ApplicationId {
                        kind: APP_TYPE.into(),
                        number: 1,
                    },
                    name: PACKAGE.into(),
                },
                activation: Activation {
                    guid: Guid([0; 16]),
                    mode: 0,
                },
                public_id: String::new(),
            },
            name: CODE.into(),
        },
        code_sequence: 1,
        package_sequence: 1,
        version: VersionInstance {
            version: PackageVersion {
                application: ApplicationVersion {
                    rollout: Rollout { major: 1, minor: 0 },
                },
                rollout: Rollout { major: 1, minor: 0 },
            },
            instance: 1,
        },
        application_name: APP_NAME.into(),
    }
}
