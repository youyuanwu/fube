//! Owned native node query data. Scalar enum numbers are never silently coerced.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

/// The numeric native node ID, not a GUID. Ordering is high word, then low word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId {
    pub high: u64,
    pub low: u64,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTimestamp(pub i64);

macro_rules! native_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $value:expr),* $(,)? }) => {
        $(#[$meta])*
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

/// One native GetNodeList request. Empty name/token are omitted like the native
/// facade. Node name is exact and case-preserving, not a prefix or naming URI.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeQueryDescription {
    pub node_name: Option<String>,
    pub status_filter: NodeStatusFilter,
    /// Opaque server continuation. No snapshot is promised across requests.
    pub continuation_token: Option<String>,
    /// Positive native page limit, at most i64::MAX; larger values are rejected
    /// locally. None leaves count unspecified (the server can still byte-page).
    pub max_results: Option<NonZeroU64>,
}

/// An explicit server page, never an automatically collected cluster snapshot.
///
/// None means paging ended, not proof that every native aggregation subquery
/// succeeded: GetNodeList can return best-effort data after partial failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeListPage {
    pub nodes: Vec<Node>,
    pub continuation_token: Option<String>,
}

/// Owned native node record, including deactivation and raw timing fields.
///
/// Aggregation can include CM-only records with native default/unknown values;
/// this type does not infer missing observations from other fields.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDeactivation {
    pub effective_intent: NodeDeactivationIntent,
    pub status: NodeDeactivationStatus,
    pub tasks: Vec<NodeDeactivationTask>,
    pub pending_safety_checks: Vec<SafetyCheck>,
}

/// An owned deactivation task, flattening the native task-ID object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDeactivationTask {
    pub id: String,
    pub task_type: NodeDeactivationTaskType,
    pub intent: NodeDeactivationIntent,
}

/// Known polymorphic safety check, with a partition ID only for partition kinds.
/// Partition IDs use UUID semantics, not numeric NodeId semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafetyCheck {
    Invalid,
    EnsureSeedNodeQuorum,
    Partition {
        kind: SafetyCheckKind,
        partition_id: uuid::Uuid,
    },
}
