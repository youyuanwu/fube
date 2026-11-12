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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceProfile {
    #[default]
    Stateless,
    StatefulSingleton,
    StatefulPeerIdle,
    StatefulThree,
    Stateful {
        target: i32,
        minimum: i32,
    },
}
impl ServiceProfile {
    pub fn service(self) -> &'static str {
        match self {
            Self::Stateless => SERVICE,
            Self::StatefulSingleton
            | Self::StatefulPeerIdle
            | Self::StatefulThree
            | Self::Stateful { .. } => "fabric:/Cluster/Singleton",
        }
    }
    pub fn stateful(self) -> bool {
        self != Self::Stateless
    }
    pub fn peer_replicas(self) -> bool {
        matches!(self, Self::StatefulPeerIdle | Self::StatefulThree)
            || matches!(self, Self::Stateful { target, .. } if target > 1)
    }
    pub fn configured_sizes(self) -> Option<(i32, i32)> {
        match self {
            Self::Stateful { target, minimum } => Some((target, minimum)),
            _ => None,
        }
    }
    pub fn mesh_peers(self) -> bool {
        matches!(self, Self::StatefulThree | Self::Stateful { .. })
    }
    pub fn validate(self) -> Result<()> {
        if let Some((target, minimum)) = self.configured_sizes() {
            if minimum < 1
                || target < minimum
                || target as usize > fube_ra::entity::MAX_CONFIGURATION_REPLICAS
            {
                return Err("stateful sizes require 1 <= minimum <= target <= 64".into());
            }
        }
        Ok(())
    }
    pub fn node_count(self) -> usize {
        if let Self::Stateful { target, .. } = self {
            target as usize
        } else if self == Self::StatefulThree {
            3
        } else {
            2
        }
    }
}

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
pub struct Peer {
    #[serde(with = "json_node")]
    pub node: NodeInstance,
    pub session: String,
    pub partition: String,
    pub replica: i64,
    pub incarnation: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Launch {
    #[serde(default)]
    pub profile: ServiceProfile,
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
    #[serde(default)]
    pub peers: Vec<Peer>,
}
impl Launch {
    pub fn check_service_sizes(&self, unit: &FailoverUnit, service: &Service) -> Result<()> {
        if self
            .profile
            .configured_sizes()
            .is_some_and(|(target, minimum)| {
                unit.target != target
                    || unit.minimum != minimum
                    || service.target != target
                    || service.minimum != minimum
            })
        {
            return Err("replica-set settings are immutable for this service".into());
        }
        Ok(())
    }
    pub fn host_replacement_path(&self, replica: i64, instance: i64) -> PathBuf {
        self.directory
            .join(format!("repaired-host-{replica}-{instance}.json"))
    }
    pub fn idle_successor(&self) -> Result<(i64, i64)> {
        if self.profile != ServiceProfile::StatefulPeerIdle {
            return Err("replacement outside the original two-node profile is gated".into());
        }
        let peer = self.peers.first().ok_or("missing original peer")?;
        let destination_instance = if self.placement == 2 {
            self.incarnation
        } else {
            peer.incarnation
        };
        let next = self
            .replica
            .max(peer.replica)
            .max(destination_instance)
            .checked_add(1)
            .ok_or("Idle replica identity exhausted")?;
        Ok((next, next))
    }

    pub fn name(&self) -> String {
        format!("node{}", self.placement)
    }
    pub fn validate(&self) -> Result<()> {
        self.profile.validate()?;
        if !(1..=self.profile.node_count()).contains(&usize::from(self.placement))
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
        let expected = if self.profile.peer_replicas() {
            if self.placement == 1 || self.profile.mesh_peers() {
                self.profile.node_count() - 1
            } else {
                1
            }
        } else {
            0
        };
        if self.peers.len() != expected {
            return Err("invalid original peer count".into());
        }
        for (i, peer) in self.peers.iter().enumerate() {
            if peer.node != self.node
                && peer.node.id != self.fm.id
                && peer.node.id != self.node.id
                && peer.node.instance > 0
                && peer.session != self.session
                && peer.partition == self.partition
                && peer.replica >= 0
                && peer.replica != self.replica
                && peer.incarnation > 0
                && self.peers[..i].iter().all(|other| {
                    other.node.id != peer.node.id
                        && other.replica != peer.replica
                        && other.session != peer.session
                })
            {
                uuid::Uuid::parse_str(&peer.session).map_err(err)?;
            } else {
                return Err("invalid original peer binding".into());
            }
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
    #[serde(default)]
    pub peer_address: Option<SocketAddr>,
    #[serde(default)]
    pub peer_addresses: Vec<SocketAddr>,
    pub profile: Option<ActualProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActualProfile {
    pub service: String,
    pub service_type: ServiceType,
    pub version: VersionInstance,
    pub code_package: String,
    pub app_pid: u32,
    pub runtime_root: PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostReplacementReceipt {
    pub deployment: String,
    pub session: String,
    pub node_pid: u32,
    pub predecessor: ActualProfile,
    pub profile: ActualProfile,
    pub old_host: String,
    pub new_host: String,
}
impl HostReplacementReceipt {
    pub fn validate(&self, launch: &Launch, old: &ActualProfile, node_pid: u32) -> Result<()> {
        self.profile.validate(launch)?;
        if launch.profile != ServiceProfile::StatefulThree
            || self.deployment != launch.deployment
            || self.session != launch.session
            || self.node_pid != node_pid
            || self.old_host == self.new_host
            || &self.predecessor != old
            || self.profile.app_pid == old.app_pid
            || self.profile.runtime_root == old.runtime_root
            || self.profile.service != old.service
            || self.profile.service_type != old.service_type
            || self.profile.version != old.version
            || self.profile.code_package != old.code_package
        {
            return Err("replacement host does not match the owned repair lineage".into());
        }
        uuid::Uuid::parse_str(&self.old_host).map_err(err)?;
        uuid::Uuid::parse_str(&self.new_host).map_err(err)?;
        Ok(())
    }
}
impl ActualProfile {
    pub fn validate(&self, launch: &Launch) -> Result<()> {
        if self.service != launch.profile.service()
            || self.service_type != fixed_type_for(launch.profile)
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

#[cfg(test)]
pub fn fixed_type() -> ServiceType {
    fixed_type_for(ServiceProfile::Stateless)
}
pub fn fixed_type_for(profile: ServiceProfile) -> ServiceType {
    let (application, package, name) = match profile {
        ServiceProfile::Stateless => ("ClusterStatelessApp", "ClusterService", TYPE),
        ServiceProfile::StatefulSingleton
        | ServiceProfile::StatefulPeerIdle
        | ServiceProfile::StatefulThree
        | ServiceProfile::Stateful { .. } => (
            "HostingLocalStatefulApp",
            "LocalService",
            "LocalStatefulType",
        ),
    };
    ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: application.into(),
                number: 1,
            },
            name: package.into(),
        },
        name: name.into(),
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
    let (target, minimum) = launch.profile.configured_sizes().unwrap_or((1, 1));
    body.unit.target = target;
    body.unit.minimum = minimum;
    body.replica.node = launch.node.clone();
    body.replica.id = launch.replica;
    body.replica.instance = launch.incarnation;
    body.replica.state = 1;
    body.replica.version = profile.version.clone();
    body.service.name = launch.profile.service().into();
    body.service.application = APPLICATION.into();
    body.service.instance = 1;
    body.service.service_type = profile.service_type.clone();
    body.service.version = profile.version.clone();
    body.service.partitions = 1;
    body.service.target = target;
    body.service.minimum = minimum;
    if launch.profile.stateful() {
        body.service.stateful = true;
        body.replica.current_role = 4;
        body.unit.current.configuration = (1 << 32) + 1;
    }
    body
}

pub fn dropped_replica(replica: &Replica) -> Replica {
    Replica {
        node: replica.node.clone(),
        id: replica.id,
        instance: replica.instance,
        up: false,
        state: 3,
        ..Replica::default()
    }
}

pub fn absent_replica_body(body: &AddInstance) -> AddInstance {
    AddInstance {
        unit: FailoverUnit {
            id: body.unit.id.clone(),
            consistency: body.unit.consistency.clone(),
            current: body.unit.current,
            ..FailoverUnit::default()
        },
        replica: dropped_replica(&body.replica),
        service: body.service.clone(),
    }
}

pub fn pre_open_cancelled_body(body: &AddInstance) -> AddInstance {
    let mut body = body.clone();
    body.replica.current_role = 2;
    body.replica.state = 3;
    body.replica.up = false;
    body
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreOpenCancellationReceipt {
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
    pub cancellation: fube_node::local::PreOpenCancellation,
}
impl PreOpenCancellationReceipt {
    pub fn validate(&self, launch: &Launch, profile: &ActualProfile, node_pid: u32) -> Result<()> {
        if launch.profile != ServiceProfile::StatefulPeerIdle
            || launch.placement != 2
            || launch.peers.is_empty()
            || self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || self.cancellation.body != pre_open_cancelled_body(&add_body(launch, profile))
            || self.cancellation.creation == 0
            || self.cancellation.command == 0
            || self.cancellation.ra_operation == 0
            || self.cancellation.creation == self.cancellation.ra_operation
        {
            return Err(
                "pre-Open cancellation proof does not match the original peer creation".into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailedRegistrationReceipt {
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
    pub deletion: fube_node::local::FailedRegistrationDeletion,
}
impl FailedRegistrationReceipt {
    pub fn validate(&self, launch: &Launch, profile: &ActualProfile, node_pid: u32) -> Result<()> {
        let failure = &self.deletion.failure;
        if launch.profile != ServiceProfile::StatefulPeerIdle
            || launch.placement != 2
            || launch.peers.is_empty()
            || self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || failure.body != pre_open_cancelled_body(&add_body(launch, profile))
            || failure.error == 0
            || failure.creation == 0
            || failure.lookup == 0
            || failure.command == 0
            || failure.ra_operation == 0
            || self.deletion.command == 0
            || self.deletion.ra_operation == 0
            || failure.command == self.deletion.command
            || failure.ra_operation == self.deletion.ra_operation
            || failure.creation == failure.ra_operation
            || failure.creation == self.deletion.ra_operation
        {
            return Err(
                "failed registration deletion does not match the original owner/failure lineage"
                    .into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailedOpenReceipt {
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
    pub deletion: fube_node::local::FailedOpenDeletion,
}
impl FailedOpenReceipt {
    pub fn validate(&self, launch: &Launch, profile: &ActualProfile, node_pid: u32) -> Result<()> {
        let failure = &self.deletion.failure;
        let disposal = &failure.disposal;
        let mut original = add_body(launch, profile);
        original.replica.current_role = 2;
        if launch.profile != ServiceProfile::StatefulPeerIdle
            || launch.placement != 2
            || launch.peers.is_empty()
            || self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || disposal.body != original
            || disposal.host.is_empty()
            || disposal.runtime.is_empty()
            || disposal.command == 0
            || disposal.ra_operation == 0
            || disposal.lookup == 0
            || disposal.acquisition == 0
            || failure.error == 0
            || failure.error == fube_ra::entity::APPLICATION_HOST_CRASH
            || failure.error == fube_ra::entity::PROXY_STATE_CHANGED_ON_DATA_LOSS
            || failure.creation == 0
            || failure.creation == disposal.ra_operation
            || failure.creation == self.deletion.ra_operation
            || self.deletion.command == 0
            || self.deletion.ra_operation == 0
            || self.deletion.command == disposal.command
            || self.deletion.ra_operation == disposal.ra_operation
        {
            return Err(
                "failed Open deletion does not match the original runtime/failure lineage".into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbsenceReceipt {
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
    pub absence: fube_node::local::ReplicaAbsence,
}
impl AbsenceReceipt {
    pub fn validate(&self, launch: &Launch, profile: &ActualProfile, node_pid: u32) -> Result<()> {
        if launch.profile != ServiceProfile::StatefulPeerIdle
            || launch.placement != 2
            || launch.peers.is_empty()
            || self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || self.absence.body != absent_replica_body(&add_body(launch, profile))
            || self.absence.command == 0
            || self.absence.ra_operation == 0
        {
            return Err(
                "absence proof does not match the original never-created peer replica".into(),
            );
        }
        Ok(())
    }
}

/// Trusted local original-owner observation, deliberately outside native
/// schemas. Semantic RemoveInstance/DeleteReplica and this proof are separate inputs.
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
        self.validate_body(launch, profile, node_pid, &add_body(launch, profile))
    }
    pub fn validate_body(
        &self,
        launch: &Launch,
        profile: &ActualProfile,
        node_pid: u32,
        body: &AddInstance,
    ) -> Result<()> {
        let proof = &self.disposal;
        if self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || proof.body.unit != body.unit
            || proof.body.service != body.service
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
pub struct HostLossReceipt {
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
    pub loss: fube_node::local::ReplicaLoss,
}
impl HostLossReceipt {
    pub fn validate(&self, launch: &Launch, profile: &ActualProfile, node_pid: u32) -> Result<()> {
        if launch.profile != ServiceProfile::StatefulPeerIdle
            || launch.placement != 2
            || self.deployment != launch.deployment
            || self.session != launch.session
            || self.node != launch.node
            || self.fm != launch.fm
            || self.generation != launch.generation
            || self.node_pid != node_pid
            || self.app_pid != profile.app_pid
            || self.loss.app_pid != profile.app_pid
            || self.runtime_root != profile.runtime_root
            || self.loss.body != pre_open_cancelled_body(&add_body(launch, profile))
            || self.loss.creation == 0
            || self.loss.operation == 0
            || self.loss.creation == self.loss.operation
            || self.loss.host.is_empty()
        {
            return Err("host-loss proof does not match the original destination owner".into());
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
