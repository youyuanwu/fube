//! Owned ordinary application-scoped service list data.

use std::num::NonZeroU64;

use crate::HealthState;

/// Exactly one service selector; matching type names are case-sensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ServiceListFilter {
    #[default]
    All,
    Name(String),
    TypeName(String),
}

/// One page scoped to an ordinary application. Root/ad-hoc and fabric:/System
/// are rejected; neither denotes all services. No status/kind/health filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceQueryDescription {
    pub application_name: String,
    pub filter: ServiceListFilter,
    pub continuation_token: Option<String>,
    pub max_results: Option<NonZeroU64>,
}

impl ServiceQueryDescription {
    pub fn new(application_name: impl Into<String>) -> Self {
        Self {
            application_name: application_name.into(),
            filter: ServiceListFilter::All,
            continuation_token: None,
            max_results: None,
        }
    }
}

/// Kind and persistence are inseparable; stateless cannot claim persisted state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceKind {
    Stateless,
    Stateful { has_persisted_state: bool },
}

/// Native service lifecycle status. A missing optional suffix is Unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    Unknown,
    Active,
    Upgrading,
    Deleting,
    Creating,
    Failed,
}

/// An aggregate observation, not a readiness guarantee. Manifest version can be
/// empty and is not the application type version. Unknown health is legitimate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Service {
    pub name: String,
    pub type_name: String,
    pub manifest_version: String,
    pub kind: ServiceKind,
    pub health_state: HealthState,
    pub status: ServiceStatus,
    pub is_service_group: bool,
}

/// Native count limits apply to subqueries; a divergent CM/FM union can exceed
/// the requested count. There is no automatic paging or snapshot guarantee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceListPage {
    pub services: Vec<Service>,
    pub continuation_token: Option<String>,
}
