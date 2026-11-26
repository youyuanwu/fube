pub use crate::fm::GenerationPhase;
use crate::{
    control::OperationOutcome,
    fm::{Desired, Observed},
};
use fube_ra::protocol::{Configuration, Epoch, FailoverUnit};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

macro_rules! states {
    ($name:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($variant),+ }
    };
}

states!(RepairState {
    Absent,
    Pending,
    Ready,
    Failed,
    Unknown,
    Cancelled,
    Superseded
});
states!(RepairKind {
    SpareReplacement,
    Restoration,
    Secondary,
    Primary
});
states!(ConfigurationState {
    Absent,
    Pending,
    Ready,
    Unknown
});
states!(ConfigurationKind {
    SpareReplacement,
    SequentialRestoration,
    SecondaryRepair,
    PostFailoverRepair,
    SecondaryLossRemoval,
    NodeDownReconfiguration,
    PrimaryFailover,
    PlannedPrimaryTransfer,
    SamePrimaryRetirement,
    SamePrimaryConfiguration
});
states!(IdleState {
    Absent,
    Pending,
    Ready,
    Failed,
    Unknown,
    Cancelled,
    Lost
});
states!(IdleKind { FmIdleBuild });
states!(RemovalState {
    Absent,
    Pending,
    Removed,
    Unknown
});
states!(DeletionState {
    Absent,
    Pending,
    Deleted,
    Unknown
});
states!(CleanupState {
    Complete,
    Unknown,
    Unproved,
    PendingReply
});
states!(RemovalCompletion {
    FailedCreationRetired,
    CreationCancelled,
    ReplicatorRemoved
});
states!(DeletionCompletion {
    Disposed,
    NeverCreated,
    PreOpenCancelled,
    FailedRegistration,
    FailedOpen,
    HostLost
});
states!(PeerIdleState {
    Absent,
    Pending,
    Open,
    Failed,
    Removing,
    Removed,
    Lost,
    ActiveSecondary,
    RetiredSecondary
});
states!(ReplicaReconfiguration {
    Blocked,
    Pending,
    NotRequested,
    NotRequired,
    Recorded,
    Complete,
    Unknown,
    Down,
    Rejected,
    Failed,
    Cancelled,
    Superseded,
    Lost,
    Error
});
impl From<crate::OperationState> for ReplicaReconfiguration {
    fn from(state: crate::OperationState) -> Self {
        match state {
            crate::OperationState::Pending => Self::Pending,
            crate::OperationState::Complete => Self::Complete,
            crate::OperationState::Down => Self::Down,
            crate::OperationState::Rejected => Self::Rejected,
            crate::OperationState::Failed => Self::Failed,
            crate::OperationState::Unknown => Self::Unknown,
            crate::OperationState::Cancelled => Self::Cancelled,
            crate::OperationState::Superseded => Self::Superseded,
            crate::OperationState::Lost => Self::Lost,
            crate::OperationState::Error => Self::Error,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeDownStatus {
    pub state: crate::OperationState,
    pub node: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate: Option<bool>,
    pub replica_reconfiguration: ReplicaReconfiguration,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "S: Deserialize<'de>, K: Deserialize<'de>"))]
pub struct Progress<S, K> {
    pub state: S,
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<K>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
impl<S, K> Progress<S, K> {
    pub(crate) fn new(state: S, ready: bool, kind: Option<K>) -> Self {
        Self {
            state,
            ready,
            kind,
            details: Map::new(),
        }
    }
    pub(crate) fn detail(mut self, key: &str, value: impl Serialize) -> Self {
        self.details.insert(key.into(), serde_json::json!(value));
        self
    }
}
pub type RepairStatus = Progress<RepairState, RepairKind>;
pub type IdleStatus = Progress<IdleState, IdleKind>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigurationRequest {
    pub configuration: ConfigurationSnapshot,
    pub phase0_duration: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigurationSnapshot {
    pub unit: FailoverUnitSnapshot,
    #[serde(flatten)]
    pub(crate) details: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailoverUnitSnapshot {
    pub current: Epoch,
    pub previous: Epoch,
    pub target: i32,
    pub minimum: i32,
    #[serde(flatten)]
    pub(crate) details: Map<String, Value>,
}

impl From<&FailoverUnit> for FailoverUnitSnapshot {
    fn from(unit: &FailoverUnit) -> Self {
        Self {
            current: unit.current,
            previous: unit.previous,
            target: unit.target,
            minimum: unit.minimum,
            details: [
                ("id".into(), serde_json::json!(unit.id)),
                ("consistency".into(), serde_json::json!(unit.consistency)),
            ]
            .into_iter()
            .collect(),
        }
    }
}

impl From<&Configuration> for ConfigurationSnapshot {
    fn from(configuration: &Configuration) -> Self {
        Self {
            unit: (&configuration.unit).into(),
            details: [
                ("replicas".into(), serde_json::json!(configuration.replicas)),
                ("service".into(), serde_json::json!(configuration.service)),
            ]
            .into_iter()
            .collect(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigurationStatus {
    pub state: ConfigurationState,
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ConfigurationKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<ConfigurationRequest>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
impl ConfigurationStatus {
    pub(crate) fn detail(mut self, key: &str, value: impl Serialize) -> Self {
        self.details.insert(key.into(), serde_json::json!(value));
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemovalStatus {
    pub state: RemovalState,
    pub pending_remove: bool,
    pub removed: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::control::present"
    )]
    pub completion: Option<Option<RemovalCompletion>>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeletionStatus {
    pub state: DeletionState,
    pub destination_deleted: bool,
    pub disposed: bool,
    pub never_created: bool,
    pub pre_open_cancelled: bool,
    pub failed_registration: bool,
    pub failed_open: bool,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_state: Option<CleanupState>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::control::present"
    )]
    pub completion: Option<Option<DeletionCompletion>>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerIdleStatus {
    pub state: PeerIdleState,
    pub built: bool,
    pub ready: bool,
    pub source: Option<PeerObservation>,
    pub target: Option<ReplicaObservation>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node: u8,
    pub node_instance: u64,
    pub desired: Desired,
    pub observed: Observed,
    pub created_lifetime: bool,
    pub admitted: bool,
    pub link_alive: bool,
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_work: Option<NodeObservation>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaPhase {
    Opening,
    Ready,
    Closing,
    Closed,
}
impl From<fube_ra::entity::Phase> for ReplicaPhase {
    fn from(phase: fube_ra::entity::Phase) -> Self {
        match phase {
            fube_ra::entity::Phase::Opening => Self::Opening,
            fube_ra::entity::Phase::Ready => Self::Ready,
            fube_ra::entity::Phase::Closing => Self::Closing,
            fube_ra::entity::Phase::Closed => Self::Closed,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeObservation {
    pub deployment: String,
    pub session: String,
    pub node_pid: u32,
    pub peer_idle: Option<PeerObservation>,
    pub local_replica: Option<ReplicaObservation>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerObservation {
    pub reply: Option<PeerCreationReply>,
    pub ready: bool,
    pub build: Option<InvocationObservation>,
    pub removal: Option<InvocationObservation>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerCreationReply {
    pub error: i64,
    pub replica: PeerReplicaSnapshot,
    #[serde(flatten)]
    pub(crate) details: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerReplicaSnapshot {
    pub id: i64,
    pub instance: i64,
    pub current_role: i64,
    pub state: i64,
    pub replication_endpoint: String,
    #[serde(flatten)]
    pub(crate) details: Map<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvocationObservation {
    pub result: Option<i64>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaObservation {
    pub phase: ReplicaPhase,
    pub open: bool,
    pub registration: Option<Value>,
    pub retained_usage: u64,
    pub pre_open_failure: Option<CreationFailure>,
    pub runtime_open_failure: Option<CreationFailure>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreationFailure {
    pub error: i64,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenerationStatus {
    pub phase: GenerationPhase,
    pub complete: bool,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

/// Typed control facts; opaque native receipts and observations remain intact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub nodes: Vec<NodeStatus>,
    pub operations: Vec<OperationOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_controls: Option<Vec<OperationOutcome>>,
    pub configuration: ConfigurationStatus,
    pub repair: RepairStatus,
    pub idle_build: IdleStatus,
    pub idle_removal: RemovalStatus,
    pub idle_deletion: DeletionStatus,
    pub generation_handshake: Option<GenerationStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_idle: Option<PeerIdleStatus>,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}
impl ClusterStatus {
    pub fn json(&self) -> Value {
        serde_json::json!(self)
    }
}
