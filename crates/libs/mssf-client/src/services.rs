//! Owned ordinary application-scoped service list data.

use crate::{HealthState, PagedQuery, QueryPage};
use serde::{Deserialize, Serialize};

/// Exactly one service selector; matching type names are case-sensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceListFilter {
    #[default]
    All,
    Name(String),
    TypeName(String),
}

/// Selectors scoped to an ordinary application. Root/ad-hoc and fabric:/System
/// are rejected; neither denotes all services. No status/kind/health filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceQueryParameters {
    pub application_name: String,
    pub filter: ServiceListFilter,
}

impl ServiceQueryParameters {
    pub fn new(application_name: impl Into<String>) -> Self {
        Self {
            application_name: application_name.into(),
            filter: ServiceListFilter::All,
        }
    }
}

/// One ordinary application-scoped service request with independent paging.
pub type ServiceQueryDescription = PagedQuery<ServiceQueryParameters>;

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
pub type ServiceListPage = QueryPage<Service>;
