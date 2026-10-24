//! Owned native single-node health data, not node-list summary health.

use serde::{Deserialize, Serialize};

use crate::{HealthState, NodeTimestamp, QueryDescription};

bitflags::bitflags! {
    /// Native event filter over **evaluated**, not reported, event health.
    ///
    /// Exact DEFAULT (0) and ALL (65535) select all; exact NONE (1) selects none.
    /// Otherwise bits OK/WARNING/ERROR are ORed; other bits are sent unchanged.
    /// For example NONE | ERROR selects Error. With warning-as-error enabled an
    /// ERROR filter can return events whose reported state is Warning.
    /// Filtering never reduces aggregate/evaluation computation.
    ///
    /// Use `from_bits_retain` for raw UInt32 masks. Every bit is retained by
    /// bitwise operations; native `ALL` differs from bitflags' `all()`.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct HealthEventsFilter: u32 {
        const DEFAULT = 0;
        const NONE = 1;
        const OK = 2;
        const WARNING = 4;
        const ERROR = 8;
        const ALL = 0xffff;
        const _ = !0;
    }
}

/// Single-node policy override, deliberately not a full cluster-health policy.
///
/// An explicit default/false overrides the cluster policy; omitting the policy
/// in [`NodeHealthQueryDescription`] inherits the native effective cluster policy.
/// Cluster percentage/application thresholds do not govern this node query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct NodeHealthPolicy {
    pub consider_warning_as_error: bool,
}

/// Native JSON event-filter object; its mask is numeric, including DEFAULT (0).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct HealthEventsQueryFilter {
    #[serde(with = "crate::query_arguments::flags")]
    pub health_state_filter: HealthEventsFilter,
}

impl From<HealthEventsFilter> for HealthEventsQueryFilter {
    fn from(health_state_filter: HealthEventsFilter) -> Self {
        Self {
            health_state_filter,
        }
    }
}

/// GetNodeHealth parameters in native argument order; no paging or child filters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct NodeHealthQueryParameters {
    /// None inherits cluster policy, unlike Some(default()).
    #[serde(
        rename = "ClusterHealthPolicy",
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::query_arguments::json"
    )]
    pub health_policy: Option<NodeHealthPolicy>,
    /// None omits the argument; an explicit DEFAULT mask also selects all events.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::query_arguments::json"
    )]
    pub events_filter: Option<HealthEventsQueryFilter>,
    /// Required, case-preserving name: 1..=4096 UTF-16 units, without NUL.
    /// Native HM resolves node identity; this is not a naming URI or prefix.
    pub node_name: String,
}

impl NodeHealthQueryParameters {
    pub fn new(node_name: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
            events_filter: None,
            health_policy: None,
        }
    }
}

/// One unpaged node-health request.
pub type NodeHealthQueryDescription = QueryDescription<NodeHealthQueryParameters>;

/// Owned detail health. An unhealthy aggregate is successful query data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_name: String,
    #[serde(with = "crate::native::object_array")]
    pub health_events: Vec<HealthEvent>,
    pub aggregated_health_state: HealthState,
    /// Native representative explanations, not necessarily every unhealthy event.
    #[serde(with = "crate::native::tagged_pointer_slots")]
    pub unhealthy_evaluations: Vec<HealthEvaluation>,
}

/// Full flattened native health event, including reported state and raw times.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HealthEvaluation {
    #[serde(rename = "1")]
    Event(EventHealthEvaluation),
}

/// Why an event made the node unhealthy, with the original full event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventHealthEvaluation {
    pub description: String,
    pub unhealthy_event: HealthEvent,
    pub consider_warning_as_error: bool,
    pub aggregated_health_state: HealthState,
}
