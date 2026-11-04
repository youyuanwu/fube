use fube_ra::protocol::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, String>;
pub const LIMIT: usize = 64 * 1024;
pub const APPLICATION: &str = "fabric:/Cluster";
pub const SERVICE: &str = "fabric:/Cluster/Service";
pub const TYPE: &str = "ClusterStatelessType";

pub fn err(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

pub fn private_dir(path: &Path) -> Result<()> {
    fs::create_dir(path).map_err(err)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(err)
}

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(err)?;
    if bytes.len() > LIMIT {
        return Err("metadata capacity".into());
    }
    let staging = path.with_extension("writing");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&staging)
        .map_err(err)?;
    file.write_all(&bytes).map_err(err)?;
    file.sync_all().map_err(err)?;
    fs::rename(staging, path).map_err(err)?;
    fs::File::open(path.parent().ok_or("metadata parent")?)
        .map_err(err)?
        .sync_all()
        .map_err(err)
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(err)?;
    if !file.metadata().map_err(err)?.is_file() {
        return Err("metadata is not a regular file".into());
    }
    let mut bytes = vec![];
    file.take((LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(err)?;
    if bytes.len() > LIMIT {
        return Err("metadata capacity".into());
    }
    serde_json::from_slice(&bytes).map_err(err)
}

pub fn node_hex(node: &NodeInstance) -> String {
    let high = u64::from_le_bytes(node.id[..8].try_into().unwrap());
    let low = u64::from_le_bytes(node.id[8..].try_into().unwrap());
    format!("{high:016x}{low:016x}")
}

// Native NodeId's ByteArray visitor intentionally is not a JSON sequence
// visitor. Local launch metadata uses explicit JSON DTOs, never the native
// byte-array deserializer or an invented field in a native message.
mod json_node {
    use super::*;
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        id: [u8; 16],
        instance: u64,
    }
    pub fn serialize<S: serde::Serializer>(
        node: &NodeInstance,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Identity {
            id: node.id,
            instance: node.instance,
        }
        .serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<NodeInstance, D::Error> {
        let value = Identity::deserialize(deserializer)?;
        Ok(NodeInstance {
            id: value.id,
            instance: value.instance,
        })
    }
}
mod json_generation {
    use super::*;
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Number {
        generation: i64,
        owner: [u8; 16],
    }
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Header {
        number: Number,
        for_fm_replica: bool,
    }
    pub fn serialize<S: serde::Serializer>(
        generation: &GenerationHeader,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Header {
            number: Number {
                generation: generation.number.generation,
                owner: generation.number.owner,
            },
            for_fm_replica: generation.for_fm_replica,
        }
        .serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<GenerationHeader, D::Error> {
        let value = Header::deserialize(deserializer)?;
        Ok(GenerationHeader {
            number: GenerationNumber {
                generation: value.number.generation,
                owner: value.number.owner,
            },
            for_fm_replica: value.for_fm_replica,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Launch {
    pub deployment: String,
    pub session: String,
    pub capability: String,
    pub parent_pid: u32,
    #[serde(with = "json_node")]
    pub node: NodeInstance,
    #[serde(with = "json_node")]
    pub fm: NodeInstance,
    #[serde(with = "json_generation")]
    pub generation: GenerationHeader,
    pub placement: u8,
    pub directory: PathBuf,
    pub package: PathBuf,
    pub partition: String,
    pub replica: i64,
    pub incarnation: i64,
}
impl Launch {
    pub fn name(&self) -> String {
        format!("node{}", self.placement)
    }
    pub fn validate(&self) -> Result<()> {
        if !(1..=2).contains(&self.placement)
            || self.node.id == self.fm.id
            || self.node.instance == 0
            || self.fm.instance == 0
            || self.generation.for_fm_replica
            || self.generation.number.owner != self.fm.id
            || self.generation.number.generation <= 0
            || self.parent_pid == 0
            || self.replica < 0
            || self.incarnation <= 0
            || !self.directory.is_absolute()
            || !self.package.is_absolute()
        {
            return Err("invalid static child authority".into());
        }
        for id in [
            &self.deployment,
            &self.session,
            &self.capability,
            &self.partition,
        ] {
            uuid::Uuid::parse_str(id).map_err(err)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub deployment: String,
    pub session: String,
    #[serde(with = "json_node")]
    pub node: NodeInstance,
    pub pid: u32,
    pub address: SocketAddr,
    pub profile: Option<ActualProfile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActualProfile {
    pub service: String,
    pub service_type: ServiceType,
    pub version: VersionInstance,
    pub code_package: String,
    pub app_pid: u32,
    pub runtime_root: PathBuf,
}
impl ActualProfile {
    pub fn validate(&self, launch: &Launch) -> Result<()> {
        if self.service != SERVICE
            || self.service_type != fixed_type()
            || self.code_package != "Code"
            || self.version.version != PackageVersion::default()
            || self.version.instance == 0
            || self.app_pid == 0
            || self.app_pid == std::process::id()
            || !self
                .runtime_root
                .starts_with(launch.directory.join("hosting"))
        {
            return Err("unexpected actual fixed package metadata".into());
        }
        Ok(())
    }
    pub fn info(&self) -> ServiceTypeInfo {
        ServiceTypeInfo {
            versioned: VersionedServiceType {
                id: self.service_type.clone(),
                version: self.version.clone(),
            },
            code_package: self.code_package.clone(),
        }
    }
}

pub fn fixed_type() -> ServiceType {
    ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: "ClusterStatelessApp".into(),
                number: 1,
            },
            name: "ClusterService".into(),
        },
        name: TYPE.into(),
    }
}

pub fn add_body(launch: &Launch, profile: &ActualProfile) -> AddInstance {
    let mut body = AddInstance::default();
    body.unit.id.guid.0 = uuid::Uuid::parse_str(&launch.partition)
        .unwrap()
        .to_bytes_le();
    body.unit.consistency.id = body.unit.id.clone();
    body.unit.current = Epoch {
        configuration: 1,
        data_loss: 1,
    };
    body.unit.target = 1;
    body.unit.minimum = 1;
    body.replica.node = launch.node.clone();
    body.replica.id = launch.replica;
    body.replica.instance = launch.incarnation;
    body.replica.state = 1;
    body.replica.version = profile.version.clone();
    body.service.name = SERVICE.into();
    body.service.application = APPLICATION.into();
    body.service.instance = 1;
    body.service.service_type = profile.service_type.clone();
    body.service.version = profile.version.clone();
    body.service.partitions = 1;
    body.service.target = 1;
    body.service.minimum = 1;
    body
}

/// Trusted local original-owner observation, deliberately outside native
/// schemas. Semantic Remove and this exact positive proof are separate inputs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementReceipt {
    pub deployment: String,
    pub session: String,
    #[serde(with = "json_node")]
    pub node: NodeInstance,
    #[serde(with = "json_node")]
    pub fm: NodeInstance,
    #[serde(with = "json_generation")]
    pub generation: GenerationHeader,
    pub node_pid: u32,
    pub app_pid: u32,
    pub runtime_root: PathBuf,
    pub disposal: fube_node::local::InstanceDisposal,
}
impl RetirementReceipt {
    pub fn validate(&self, launch: &Launch, profile: &ActualProfile, node_pid: u32) -> Result<()> {
        let proof = &self.disposal;
        if self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || proof.body.unit != add_body(launch, profile).unit
            || proof.body.service != add_body(launch, profile).service
            || proof.body.replica.node != launch.node
            || proof.body.replica.version != profile.version
            || proof.command == 0
            || proof.ra_operation == 0
            || proof.lookup == 0
            || proof.acquisition == 0
            || proof.host.is_empty()
            || proof.runtime.is_empty()
        {
            return Err("retirement proof does not match original owner/profile".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupReceipt {
    pub deployment: String,
    pub session: String,
    #[serde(with = "json_node")]
    pub node: NodeInstance,
    pub node_pid: u32,
    pub app_pid: Option<u32>,
    pub runtime_root: Option<PathBuf>,
    pub activation_acquired: bool,
    pub original_activation_cleaned: bool,
    pub executor_closed: bool,
    pub store_closed: bool,
    pub external_pending: usize,
    pub adapter_pending: usize,
    pub semantic_pending: usize,
}
