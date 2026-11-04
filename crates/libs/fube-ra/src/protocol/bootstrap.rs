//! Native NodeUp / NodeUpAck schemas; not a private registration/ready protocol.
//!
//! Source: Service Fabric `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`,
//! paths below relative to `src/prod/src/`. Each ordinary struct has one native
//! object scope: none of these classes declares a serialized derived scope.
//!
//! | Schema | Native field-order authority |
//! |---|---|
//! | NodeUp, ServicePackageInfo | `Reliability/Failover/common/NodeUpMessageBody.h:35,103` |
//! | NodeDescription (11 fields, NOT a NodeInstance) | `Reliability/Failover/common/NodeDescription.h:75` |
//! | Uri (10 fields, NOT URI text) | `Common/Uri.h:87` |
//! | NodeUpAck (upgrades BEFORE FabricVersionInstance) | `Reliability/Failover/common/NodeUpAckMessageBody.h:54` |
//! | UpgradeDescription | `Reliability/Failover/common/UpgradeDescription.h:50` |
//! | ApplicationUpgradeSpecification (12 fields) | `ServiceModel/ApplicationUpgradeSpecification.h:91-104` |
//! | Package upgrade / type removal | `ServiceModel/ServicePackageUpgradeSpecification.h:40`, `ServiceModel/ServiceTypeRemovalSpecification.h:41` |
//! | Resource governance / container images | `ServiceModel/ServicePackageResourceGovernanceDescription.h:58`, `ServiceModel/CodePackageContainersImagesDescription.h:33` |
//! | Fabric version instance / version / code / config | `Common/FabricVersionInstance.h:46`, `Common/FabricVersion.h:50`, `Common/FabricCodeVersion.h:55`, `Common/FabricConfigVersion.h:44` |
//!
//! Reused package/version closure (`ServiceModel/`): `ApplicationIdentifier.h:51`,
//! `ServicePackageIdentifier.h:69`, `ApplicationVersion.h:38`,
//! `ServicePackageVersion.h:49`, `ServicePackageVersionInstance.h:64`,
//! `RolloutVersion.h:44` (`FABRIC_FIELDS`). Native maps are object arrays of `(key,value)` objects
//! (`Common/FabricSerializationHelper.h:4428-4467,5161-5184`), NOT count/value
//! sequences. String vectors and string sets use UInt32 count then strings
//! (`Common/FabricSerializationHelper.h:4516-4545,4877-4919`).
//!
//! Factories follow `Reliability/Failover/common/RSMessage.cpp:32-33` and
//! `RSMessage.h:245-253`: recipient FM=11 for NodeUp, RS=13 (not RA=12) for
//! NodeUpAck; both default high priority. FM's correlated reply attaches the
//! actual GenerationHeader (`Reliability/Failover/fm/FailoverManager.cpp:1150-1153`).
//! Node incarnation/session and correlation belong to the authenticated
//! envelope, not extra fields invented inside NodeUp.
//!
//! Decoding retains nonempty package/recovery/upgrade contents. Call the explicit
//! profile validators before using bootstrap facts; a decoded Ack is NOT
//! readiness. The fresh RA must still complete its final empty ReplicaUp/Reply
//! (`Reliability/Failover/ra/Node.PendingReplicaUploadState.cpp:30-45`).
//! These are pinned schema codecs, not native execution/interoperability proof.

use super::{
    reliability_message, unique, ApplicationId, ApplicationVersion, GenerationHeader, PackageId,
    Rollout, VersionInstance, FM_ACTOR, GENERATION_HEADER,
};
use fube_transport::{
    message::{Message, ACTION, ACTOR, HIGH_PRIORITY},
    serialization::serde::{from_slice_with_budget, CountedStrings, ObjectArray},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub const RS_ACTOR: i64 = 13;
pub const BOOTSTRAP_MAX_BODY_BYTES: usize = 64 * 1024;
pub const BOOTSTRAP_MAX_HEADER_BYTES: usize = 4096;
pub const BOOTSTRAP_MAX_HEADERS: usize = 16;
pub const BOOTSTRAP_MAX_DECODE_ALLOCATION: usize = 4 * BOOTSTRAP_MAX_BODY_BYTES;

/// Native map entry object. Vectors preserve all received entries for caller
/// validation rather than silently collapsing duplicate keys.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapEntry<K, V> {
    pub key: K,
    pub value: V,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricCodeVersion {
    pub major: u32,
    pub minor: u32,
    pub build: u32,
    pub hotfix: u32,
}
impl FabricCodeVersion {
    /// Native static `Default`, distinct from the zero-valued constructor.
    /// `Common/FabricVersionConstants.cpp:11-14`.
    pub fn native_default() -> Self {
        Self {
            major: 1,
            minor: 0,
            build: 960,
            hotfix: 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricConfigVersion {
    pub value: String,
}
impl FabricConfigVersion {
    /// Native static `Default`; not an installed-cluster version claim.
    pub fn native_default() -> Self {
        Self {
            value: "__default__".into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricVersion {
    pub code: FabricCodeVersion,
    pub config: FabricConfigVersion,
}
impl FabricVersion {
    pub fn native_default() -> Self {
        Self {
            code: FabricCodeVersion::native_default(),
            config: FabricConfigVersion::native_default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricVersionInstance {
    pub version: FabricVersion,
    pub instance: u64,
}
impl FabricVersionInstance {
    /// `Common/FabricVersionConstants.cpp:16-25`. Rust `Default` instead matches
    /// the native default constructor (zero code, empty config, instance zero).
    pub fn native_default() -> Self {
        Self {
            version: FabricVersion::native_default(),
            instance: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uri {
    pub kind: i64,
    pub scheme: String,
    pub authority: String,
    pub host_kind: i64,
    pub host: String,
    pub port: i32,
    pub path: String,
    pub query: String,
    pub fragment: String,
    pub segments: CountedStrings<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescription {
    pub upgrade_domain: String,
    pub fault_domains: ObjectArray<Vec<Uri>>,
    pub properties: ObjectArray<Vec<MapEntry<String, String>>>,
    pub capacity_ratios: ObjectArray<Vec<MapEntry<String, u32>>>,
    pub capacities: ObjectArray<Vec<MapEntry<String, u32>>>,
    pub version: FabricVersionInstance,
    pub name: String,
    pub node_type: String,
    pub address: String,
    pub cluster_connection_port: u32,
    pub http_gateway_port: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicePackageInfo {
    pub package: PackageId,
    pub version: VersionInstance,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeUp {
    pub node: NodeDescription,
    pub packages: ObjectArray<Vec<ServicePackageInfo>>,
    pub any_replica_found: bool,
}
impl NodeUp {
    /// Empty-inventory stateless bootstrap with explicit actual node metadata.
    /// The caller separately binds the node's NodeInstance to the connection.
    pub fn fresh_stateless(
        name: String,
        node_type: String,
        address: SocketAddr,
        version: FabricVersionInstance,
    ) -> Result<Self> {
        if name.is_empty()
            || node_type.is_empty()
            || !address.ip().is_loopback()
            || address.port() == 0
        {
            return Err(Error::InvalidInput("invalid fresh stateless node metadata"));
        }
        Ok(Self {
            node: NodeDescription {
                name,
                node_type,
                address: address.ip().to_string(),
                cluster_connection_port: u32::from(address.port()),
                version,
                ..NodeDescription::default()
            },
            ..Self::default()
        })
    }

    /// A decoder does not grant recovery or package reconciliation support.
    pub fn validate_fresh_stateless(&self) -> Result<()> {
        if self.any_replica_found || !self.packages.0.is_empty() {
            return Err(Error::Protocol(
                "NodeUp recovery/package inventory unsupported",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicePackageUpgradeSpecification {
    pub package_name: String,
    pub rollout: Rollout,
    pub code_package_info_included: bool,
    pub code_package_names: CountedStrings<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTypeRemovalSpecification {
    pub package_name: String,
    pub rollout: Rollout,
    pub type_names: CountedStrings<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServicePackageResourceGovernance {
    pub governed: bool,
    pub legacy_cpu_cores: u32,
    pub memory_mb: u32,
    pub cpu_cores: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodePackageContainerImages {
    pub images: CountedStrings<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplicationUpgradeSpecification {
    pub application: ApplicationId,
    pub version: ApplicationVersion,
    pub instance: u64,
    pub upgrade_type: i64,
    pub monitored: bool,
    pub packages: ObjectArray<Vec<ServicePackageUpgradeSpecification>>,
    pub application_name: String,
    pub removed_types: ObjectArray<Vec<ServiceTypeRemovalSpecification>>,
    pub manual: bool,
    pub resource_governance:
        ObjectArray<Vec<MapEntry<PackageId, ServicePackageResourceGovernance>>>,
    pub container_images: ObjectArray<Vec<MapEntry<PackageId, CodePackageContainerImages>>>,
    pub networks: CountedStrings<Vec<String>>,
}
impl Default for ApplicationUpgradeSpecification {
    fn default() -> Self {
        Self {
            application: ApplicationId::default(),
            version: ApplicationVersion::default(),
            instance: 0,
            upgrade_type: 1,
            monitored: false,
            packages: ObjectArray(vec![]),
            application_name: String::new(),
            removed_types: ObjectArray(vec![]),
            manual: false,
            resource_governance: ObjectArray(vec![]),
            container_images: ObjectArray(vec![]),
            networks: CountedStrings(vec![]),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UpgradeDescription {
    pub specification: ApplicationUpgradeSpecification,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeUpAck {
    pub upgrades: ObjectArray<Vec<UpgradeDescription>>,
    pub version: FabricVersionInstance,
    pub activated: bool,
    pub activation_sequence: i64,
}
impl Default for NodeUpAck {
    fn default() -> Self {
        Self {
            upgrades: ObjectArray(vec![]),
            version: FabricVersionInstance::default(),
            activated: true,
            activation_sequence: 0,
        }
    }
}
impl NodeUpAck {
    pub fn fresh_activated(
        version: FabricVersionInstance,
        activation_sequence: i64,
    ) -> Result<Self> {
        if activation_sequence < 0 {
            return Err(Error::InvalidInput("negative activation sequence"));
        }
        Ok(Self {
            version,
            activation_sequence,
            ..Self::default()
        })
    }

    /// Fail closed for upgrades, deactivation and a changed fabric target.
    /// Generation, node incarnation, session and final upload admission remain
    /// separate caller responsibilities.
    pub fn validate_fresh_activated(&self, expected: &FabricVersionInstance) -> Result<()> {
        if !self.upgrades.0.is_empty() {
            return Err(Error::Protocol(
                "NodeUpAck application upgrades unsupported",
            ));
        }
        if &self.version != expected {
            return Err(Error::Protocol("NodeUpAck fabric upgrade unsupported"));
        }
        if !self.activated || self.activation_sequence < 0 {
            return Err(Error::Protocol("NodeUpAck node not activated"));
        }
        Ok(())
    }
}

pub fn node_up_message(body: &NodeUp) -> Result<Message> {
    let message = reliability_message(body, "NodeUp", FM_ACTOR, true, None)?;
    check_envelope(&message)?;
    Ok(message)
}

pub fn node_up_ack_message(body: &NodeUpAck, generation: &GenerationHeader) -> Result<Message> {
    let message = reliability_message(body, "NodeUpAck", RS_ACTOR, true, Some(generation))?;
    check_envelope(&message)?;
    Ok(message)
}

fn check_envelope(message: &Message) -> Result<()> {
    if message.body.len() > BOOTSTRAP_MAX_BODY_BYTES
        || message.headers.len() > BOOTSTRAP_MAX_HEADERS
    {
        return Err(Error::ResourceLimit("bootstrap message size"));
    }
    let mut remaining = BOOTSTRAP_MAX_HEADER_BYTES;
    for header in &message.headers {
        remaining = remaining
            .checked_sub(header.value.len())
            .ok_or(Error::ResourceLimit("bootstrap headers"))?;
    }
    Ok(())
}

fn check_bootstrap_factory(
    message: &Message,
    action: &str,
    actor: i64,
    reserve: &mut impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    check_envelope(message)?;
    // Transport headers are primitive Encoder values, not FabricSerializable
    // body object scopes. Reserve the bounded UTF-16 projection before reading.
    let actual_actor = unique(message, ACTOR)?.read(|d| d.i64())?;
    let action_header = unique(message, ACTION)?;
    reserve(action_header.value.len().saturating_mul(2))?;
    let actual_action = action_header.read(|d| d.string())?;
    if actual_actor != actor || actual_action != action {
        return Err(Error::Protocol("wrong reliability actor/action"));
    }
    // Priority is decoded, not promoted to lifecycle or sender authority.
    unique(message, HIGH_PRIORITY)?.read(|d| d.boolean())?;
    Ok(())
}

fn allocation_budget() -> impl FnMut(usize) -> Result<()> {
    let mut remaining = BOOTSTRAP_MAX_DECODE_ALLOCATION;
    move |bytes| {
        remaining = remaining
            .checked_sub(bytes)
            .ok_or(Error::ResourceLimit("bootstrap decode allocation"))?;
        Ok(())
    }
}

/// Decode with a 64 KiB body / 4 KiB header / 16 header envelope and a 256 KiB
/// codec allocation budget. Does not perform profile/session admission.
pub fn decode_node_up(message: &Message) -> Result<NodeUp> {
    decode_node_up_with_budget(message, &mut allocation_budget())
}

/// Same envelope bounds; caller owns reservations for all decoded headers/body.
/// Reservations are not refunded on errors. The transport must bound the frame
/// before constructing Message; this decoder cannot undo prior allocations.
pub fn decode_node_up_with_budget(
    message: &Message,
    reserve: &mut impl FnMut(usize) -> Result<()>,
) -> Result<NodeUp> {
    check_bootstrap_factory(message, "NodeUp", FM_ACTOR, reserve)?;
    from_slice_with_budget(&message.body, reserve)
}

pub fn decode_node_up_ack(message: &Message) -> Result<(NodeUpAck, GenerationHeader)> {
    decode_node_up_ack_with_budget(message, &mut allocation_budget())
}

pub fn decode_node_up_ack_with_budget(
    message: &Message,
    reserve: &mut impl FnMut(usize) -> Result<()>,
) -> Result<(NodeUpAck, GenerationHeader)> {
    check_bootstrap_factory(message, "NodeUpAck", RS_ACTOR, reserve)?;
    let generation = from_slice_with_budget(&unique(message, GENERATION_HEADER)?.value, reserve)?;
    Ok((from_slice_with_budget(&message.body, reserve)?, generation))
}

#[cfg(test)]
mod tests;
