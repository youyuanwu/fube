use crate::{err, Result};
use fube_ra::protocol::{GenerationHeader, GenerationNumber, NodeInstance, PartitionId};
use fube_transport::serialization::serde::Guid;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    net::SocketAddrV4,
    path::{Component, Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub id: String,
    pub instance: u64,
}

impl Identity {
    pub fn native(&self) -> Result<NodeInstance> {
        if self.id.is_empty()
            || self.id.len() > 32
            || self.instance == 0
            || !self.id.bytes().all(|value| value.is_ascii_hexdigit())
        {
            return Err("invalid exact Node identity".into());
        }
        let id = u128::from_str_radix(&self.id, 16).map_err(err)?;
        if id == 0 {
            return Err("Node ID must be nonzero".into());
        }
        Ok(NodeInstance {
            id: id.to_le_bytes(),
            instance: self.instance,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub identity: Identity,
    pub address: SocketAddrV4,
    pub replica_id: i64,
    pub replica_instance: i64,
}

impl Placement {
    fn validate(&self) -> Result<()> {
        self.identity.native()?;
        address(self.address)?;
        if self.replica_id < 0 || self.replica_instance <= 0 {
            return Err("invalid original replica identity".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fm {
    pub identity: Identity,
    pub address: SocketAddrV4,
    pub generation: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub deployment: String,
    pub name: String,
    pub node: Placement,
    pub fm: Fm,
    pub peers: Vec<Placement>,
    pub partition: String,
    pub package: PathBuf,
    pub application: String,
    pub package_version_instance: u64,
    #[serde(default)]
    pub parameters: Vec<(String, String)>,
    pub run_directory: PathBuf,
    #[serde(default = "startup_ms")]
    pub startup_timeout_ms: u64,
    #[serde(default = "shutdown_ms")]
    pub shutdown_timeout_ms: u64,
}

fn startup_ms() -> u64 {
    30_000
}
fn shutdown_ms() -> u64 {
    20_000
}

fn address(value: SocketAddrV4) -> Result<()> {
    if value.port() == 0
        || value.ip().is_unspecified()
        || value.ip().is_multicast()
        || value.ip().is_broadcast()
    {
        return Err("configured TCP address must be concrete unicast with a nonzero port".into());
    }
    Ok(())
}

fn absolute(path: &Path) -> bool {
    path.to_str().is_some()
        && path.is_absolute()
        && path.parent().is_some()
        && !path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
}

impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        if !fs::symlink_metadata(path).map_err(err)?.is_file() {
            return Err("configuration must be a regular file".into());
        }
        let mut bytes = vec![];
        fs::File::open(path)
            .map_err(err)?
            .take((fube_node::control::MAX_BODY_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(err)?;
        if bytes.len() > fube_node::control::MAX_BODY_BYTES {
            return Err("configuration exceeds 64 KiB".into());
        }
        let config: Self = serde_json::from_slice(&bytes).map_err(err)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let deployment = uuid::Uuid::parse_str(&self.deployment).map_err(err)?;
        let partition = uuid::Uuid::parse_str(&self.partition).map_err(err)?;
        if self.schema_version != 1
            || deployment.is_nil()
            || partition.is_nil()
            || self.name.is_empty()
            || self.name.len() > 128
            || self.package_version_instance == 0
            || !(100..=60_000).contains(&self.startup_timeout_ms)
            || !(1_000..=30_000).contains(&self.shutdown_timeout_ms)
            || !absolute(&self.package)
            || !absolute(&self.run_directory)
            || self.run_directory.starts_with(&self.package)
            || !self.application.starts_with("fabric:/")
            || self.application.len() > 1024
            || self.parameters.len() > 32
        {
            return Err("unsupported standalone Node configuration".into());
        }
        self.node.validate()?;
        let fm = self.fm.identity.native()?;
        address(self.fm.address)?;
        if self.fm.generation <= 0 || self.peers.len() != 2 {
            return Err(
                "healthy 3/2 profile requires one fixed FM generation and two peers".into(),
            );
        }
        let mut ids = BTreeSet::from([fm.id]);
        let mut ips = BTreeSet::new();
        let mut replicas = BTreeSet::new();
        for node in std::iter::once(&self.node).chain(&self.peers) {
            node.validate()?;
            if node.address == self.fm.address
                || !ids.insert(node.identity.native()?.id)
                || !ips.insert(*node.address.ip())
                || !replicas.insert(node.replica_id)
            {
                return Err(
                    "Node IDs, Node IPv4 addresses and replica IDs must be distinct".into(),
                );
            }
        }
        let mut names = BTreeSet::new();
        let mut bytes = 0usize;
        for (name, value) in &self.parameters {
            bytes += name.len() + value.len();
            if name.is_empty() || name.len() > 128 || value.len() > 4096 || !names.insert(name) {
                return Err("invalid or duplicate application parameter".into());
            }
        }
        if bytes > 16 * 1024 {
            return Err("application parameter budget exceeded".into());
        }
        Ok(())
    }

    pub fn generation(&self) -> Result<GenerationHeader> {
        Ok(GenerationHeader {
            number: GenerationNumber {
                owner: self.fm.identity.native()?.id,
                generation: self.fm.generation,
            },
            for_fm_replica: false,
        })
    }

    pub fn partition_id(&self) -> Result<PartitionId> {
        Ok(PartitionId {
            guid: Guid(
                uuid::Uuid::parse_str(&self.partition)
                    .map_err(err)?
                    .to_bytes_le(),
            ),
        })
    }

    pub fn startup_budget(&self) -> Duration {
        Duration::from_millis(self.startup_timeout_ms)
    }
    pub fn shutdown_budget(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }
}
