//! Owned native node query data. Scalar enum numbers are never silently coerced.

use mssf_transport::serialization::serde::ByteArray;
use serde::{Deserialize, Serialize};

use crate::{PagedQuery, QueryPage};

/// The numeric native node ID, not a GUID. Ordering is high word, then low word.
/// Native Serde uses a 16-byte array of little-endian words; JSON uses those bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId {
    pub high: u64,
    pub low: u64,
}

impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.high.to_le_bytes());
        bytes[8..].copy_from_slice(&self.low.to_le_bytes());
        ByteArray(bytes).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = if deserializer.is_human_readable() {
            Vec::<u8>::deserialize(deserializer)?
        } else {
            ByteArray::deserialize(deserializer)?.0
        };
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("node ID must contain exactly 16 bytes"))?;
        Ok(Self {
            high: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            low: u64::from_le_bytes(bytes[8..].try_into().unwrap()),
        })
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.high == 0 {
            write!(f, "{:x}", self.low)
        } else {
            write!(f, "{:x}{:016x}", self.high, self.low)
        }
    }
}

/// Native DateTime: signed 100-nanosecond ticks since 1601-01-01 UTC.
/// Zero is the native default, not an inferred observation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeTimestamp(pub i64);

macro_rules! native_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $value:expr),* $(,)? }) => {
        $(#[$meta])*
        /// Serde uses the signed i64 code in native and JSON formats, retaining unknown values.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $name {
            $($variant,)*
            /// A numeric value not named by the profiled native source.
            Unrecognized(i64),
        }
        impl $name {
            pub(crate) fn from_raw(value: i64) -> Self {
                match value {
                    $($value => Self::$variant,)*
                    other => Self::Unrecognized(other),
                }
            }
            /// Return the native signed value, including unrecognized values.
            pub fn as_raw(self) -> i64 {
                match self {
                    $(Self::$variant => $value,)*
                    Self::Unrecognized(other) => other,
                }
            }
        }
        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                serializer.serialize_i64(self.as_raw())
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
                i64::deserialize(deserializer).map(Self::from_raw)
            }
        }
    };
}

native_enum!(
    /// Native node state, distinct from the bits in [`NodeStatusFilter`].
    NodeStatus {
        Invalid = 0, Up = 1, Down = 2, Enabling = 3, Disabling = 4,
        Disabled = 5, Unknown = 6, Removed = 7,
    }
);
native_enum!(
    /// Native health assessment; `Unknown` is a named value, not a fallback.
    HealthState {
        Invalid = 0, Ok = 1, Warning = 2, Error = 3, Unknown = 65535,
    }
);
native_enum!(
    /// Requested or effective action for native node deactivation.
    NodeDeactivationIntent {
        Invalid = 0, Pause = 1, Restart = 2, RemoveData = 3, RemoveNode = 4,
    }
);
native_enum!(
    /// Progress of the node's native deactivation safety checks and action.
    NodeDeactivationStatus {
        None = 0, SafetyCheckInProgress = 1, SafetyCheckComplete = 2, Completed = 3,
    }
);
native_enum!(
    /// Native subsystem that owns a node deactivation task.
    NodeDeactivationTaskType {
        Invalid = 0, Infrastructure = 1, Repair = 2, Client = 3,
    }
);
native_enum!(
    /// Native safety-check discriminant; unknown structural kinds fail decoding.
    SafetyCheckKind {
        Invalid = 0, EnsureSeedNodeQuorum = 1, EnsurePartitionQuorum = 2,
        WaitForPrimaryPlacement = 3, WaitForPrimarySwap = 4,
        WaitForReconfiguration = 5, WaitForInBuildReplica = 6, EnsureAvailability = 7,
    }
);

bitflags::bitflags! {
    /// Native filter bits, distinct from `NodeStatus` discriminants.
    ///
    /// DEFAULT excludes Unknown/Removed, not all non-Up states. ALL bypasses status
    /// matching. Other masks combine individual bits; raw bits are sent unchanged.
    ///
    /// Use `from_bits_retain` for raw UInt32 masks. Every bit is retained by
    /// bitwise operations; native `ALL` differs from bitflags' `all()`.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct NodeStatusFilter: u32 {
        const DEFAULT = 0;
        const ALL = 0xffff;
        const UP = 1;
        const DOWN = 2;
        const ENABLING = 4;
        const DISABLING = 8;
        const DISABLED = 16;
        const UNKNOWN = 32;
        const REMOVED = 64;
        const _ = !0;
    }
}

/// Native GetNodeList parameters. Empty names are omitted like the native
/// facade. Node name is exact and case-preserving, not a prefix or naming URI.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeQueryParameters {
    pub node_name: Option<String>,
    pub status_filter: NodeStatusFilter,
}

/// One native node-list request with separate parameters and paging.
pub type NodeQueryDescription = PagedQuery<NodeQueryParameters>;

/// An explicit, best-effort node page, not a cluster membership snapshot.
pub type NodeListPage = QueryPage<Node>;

/// Owned native node record, including deactivation and raw timing fields.
///
/// Aggregation can include CM-only records with native default/unknown values;
/// this type does not infer missing observations from other fields.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Node {
    pub name: String,
    pub id: NodeId,
    pub ip_address_or_fqdn: String,
    pub node_type: String,
    pub code_version: String,
    pub config_version: String,
    pub status: NodeStatus,
    pub up_time_seconds: i64,
    pub health_state: HealthState,
    pub is_seed_node: bool,
    pub upgrade_domain: String,
    pub fault_domain: String,
    /// Repeated native ID; node-list replies require this to equal `id`.
    pub id_copy: NodeId,
    pub instance_id: u64,
    pub cluster_connection_port: u32,
    pub deactivation: NodeDeactivation,
    pub is_stopped: bool,
    pub down_time_seconds: i64,
    pub up_at: NodeTimestamp,
    pub down_at: NodeTimestamp,
    pub http_gateway_port: u16,
}

/// Native effective deactivation state with its tasks and pending safety checks.
/// Native deserialization preserves the nested task-ID and safety-check layouts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NodeDeactivation {
    pub effective_intent: NodeDeactivationIntent,
    pub status: NodeDeactivationStatus,
    #[serde(deserialize_with = "crate::native::object_array::deserialize")]
    pub tasks: Vec<NodeDeactivationTask>,
    #[serde(deserialize_with = "crate::native::tagged_pointer_slots::deserialize")]
    pub pending_safety_checks: Vec<SafetyCheck>,
}

/// An owned deactivation task, preserving the native nested identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NodeDeactivationTask {
    pub id: NodeDeactivationTaskId,
    pub intent: NodeDeactivationIntent,
}

/// Native deactivation task identity and its owning subsystem.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NodeDeactivationTaskId {
    pub id: String,
    pub task_type: NodeDeactivationTaskType,
}

/// Native safety-check variants, with explicit numeric Serde discriminants.
/// Native decoding uses `NativeEnum` and rejects unknown or disagreeing tags.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum SafetyCheck {
    #[serde(rename = "0")]
    Invalid,
    #[serde(rename = "1")]
    EnsureSeedNodeQuorum,
    #[serde(rename = "2")]
    EnsurePartitionQuorum(PartitionSafetyCheck),
    #[serde(rename = "3")]
    WaitForPrimaryPlacement(PartitionSafetyCheck),
    #[serde(rename = "4")]
    WaitForPrimarySwap(PartitionSafetyCheck),
    #[serde(rename = "5")]
    WaitForReconfiguration(PartitionSafetyCheck),
    #[serde(rename = "6")]
    WaitForInBuildReplica(PartitionSafetyCheck),
    #[serde(rename = "7")]
    EnsureAvailability(PartitionSafetyCheck),
}

impl SafetyCheck {
    /// Return the native kind implied by this variant.
    pub fn kind(&self) -> SafetyCheckKind {
        match self {
            Self::Invalid => SafetyCheckKind::Invalid,
            Self::EnsureSeedNodeQuorum => SafetyCheckKind::EnsureSeedNodeQuorum,
            Self::EnsurePartitionQuorum(_) => SafetyCheckKind::EnsurePartitionQuorum,
            Self::WaitForPrimaryPlacement(_) => SafetyCheckKind::WaitForPrimaryPlacement,
            Self::WaitForPrimarySwap(_) => SafetyCheckKind::WaitForPrimarySwap,
            Self::WaitForReconfiguration(_) => SafetyCheckKind::WaitForReconfiguration,
            Self::WaitForInBuildReplica(_) => SafetyCheckKind::WaitForInBuildReplica,
            Self::EnsureAvailability(_) => SafetyCheckKind::EnsureAvailability,
        }
    }
}

/// Shared derived-scope payload for partition safety checks.
/// Partition IDs use UUID semantics, not numeric NodeId semantics.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PartitionSafetyCheck {
    #[serde(deserialize_with = "crate::native::derived_guid::deserialize")]
    pub partition_id: uuid::Uuid,
}
