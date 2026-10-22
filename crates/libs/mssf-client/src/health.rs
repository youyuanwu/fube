//! Owned native single-node health data, not node-list summary health.

use std::ops::BitOr;

use crate::{HealthState, NodeTimestamp};

/// Native event filter over **evaluated**, not reported, event health.
///
/// Exact DEFAULT (0) and ALL (65535) select all; exact NONE (1) selects none.
/// Otherwise bits OK/WARNING/ERROR are ORed; other bits are sent unchanged.
/// For example NONE | ERROR selects Error. With warning-as-error enabled an
/// ERROR filter can return events whose reported state is Warning.
/// Filtering never reduces aggregate/evaluation computation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HealthEventsFilter(u32);

impl HealthEventsFilter {
    pub const DEFAULT: Self = Self(0);
    pub const NONE: Self = Self(1);
    pub const OK: Self = Self(2);
    pub const WARNING: Self = Self(4);
    pub const ERROR: Self = Self(8);
    pub const ALL: Self = Self(0xffff);

    /// Retain the native UInt32 mask, including unnamed bits.
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }
}

impl BitOr for HealthEventsFilter {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Single-node policy override, deliberately not a full cluster-health policy.
///
/// An explicit default/false overrides the cluster policy; omitting the policy
/// in [`NodeHealthQueryDescription`] inherits the native effective cluster policy.
/// Cluster percentage/application thresholds do not govern this node query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeHealthPolicy {
    pub consider_warning_as_error: bool,
}

/// One GetNodeHealth request; no paging, child filters or statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHealthQueryDescription {
    /// Required, case-preserving name: 1..=4096 UTF-16 units, without NUL.
    /// Native HM resolves node identity; this is not a naming URI or prefix.
    pub node_name: String,
    /// None omits the argument; explicit DEFAULT also selects all events.
    pub events_filter: Option<HealthEventsFilter>,
    /// None inherits cluster policy, unlike Some(default()).
    pub health_policy: Option<NodeHealthPolicy>,
}

impl NodeHealthQueryDescription {
    pub fn new(node_name: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
            events_filter: None,
            health_policy: None,
        }
    }
}

/// Owned detail health. An unhealthy aggregate is successful query data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHealth {
    pub node_name: String,
    pub health_events: Vec<HealthEvent>,
    pub aggregated_health_state: HealthState,
    /// Native representative explanations, not necessarily every unhealthy event.
    pub unhealthy_evaluations: Vec<HealthEvaluation>,
}

/// Full flattened native health event, including reported state and raw times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthEvent {
    pub source_id: String,
    pub property: String,
    /// Signed 100ns duration ticks; i64::MAX is the native infinite-TTL sentinel.
    pub time_to_live_ticks: i64,
    /// Reported state, before expiration or warning-as-error evaluation.
    pub health_state: HealthState,
    pub description: String,
    pub sequence_number: i64,
    pub source_utc_timestamp: NodeTimestamp,
    pub last_modified_utc_timestamp: NodeTimestamp,
    pub is_expired: bool,
    pub remove_when_expired: bool,
    pub last_ok_transition_at: NodeTimestamp,
    pub last_warning_transition_at: NodeTimestamp,
    pub last_error_transition_at: NodeTimestamp,
}

/// Supported node-detail polymorphism at the pinned native source.
///
/// Every reachable node-detail explanation is Event, including synthetic HM
/// reports. Unsupported structural tags fail the operation and retire the
/// connection rather than being silently discarded or guessed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HealthEvaluation {
    Event(EventHealthEvaluation),
}

/// Why an event made the node unhealthy, with the original full event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventHealthEvaluation {
    pub description: String,
    pub unhealthy_event: HealthEvent,
    pub consider_warning_as_error: bool,
    pub aggregated_health_state: HealthState,
}
