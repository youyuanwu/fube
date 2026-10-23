//! Owned ordinary application-scoped service list data.

use crate::{HealthState, NamingUri, PagedQuery, QueryPage};
use serde::{Deserialize, Serialize};

/// Exactly one service selector; matching type names are case-sensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceListFilter {
    #[default]
    All,
    Name(NamingUri),
    TypeName(String),
}

/// Selectors scoped to an ordinary application. Root/ad-hoc and fabric:/System
/// are rejected; neither denotes all services. No status/kind/health filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceQueryParameters {
    pub application_name: NamingUri,
    pub filter: ServiceListFilter,
}

impl ServiceQueryParameters {
    pub fn new(application_name: NamingUri) -> Self {
        Self {
            application_name,
            filter: ServiceListFilter::All,
        }
    }
}

/// One ordinary application-scoped service request with independent paging.
pub type ServiceQueryDescription = PagedQuery<ServiceQueryParameters>;

/// Native scalar kind; persistence is a separate service field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceKind {
    Invalid,
    Stateless,
    Stateful,
}
crate::native::strict_enum!(ServiceKind {
    Invalid = 0, Stateless = 1, Stateful = 2,
});

/// Native service lifecycle status. A missing optional suffix is Unknown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ServiceStatus {
    #[default]
    Unknown,
    Active,
    Upgrading,
    Deleting,
    Creating,
    Failed,
}
crate::native::strict_enum!(ServiceStatus {
    Unknown = 0, Active = 1, Upgrading = 2, Deleting = 3, Creating = 4, Failed = 5,
});

/// An aggregate observation, not a readiness guarantee. Manifest version can be
/// empty and is not the application type version. Unknown health is legitimate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Service {
    pub kind: ServiceKind,
    pub name: NamingUri,
    pub type_name: String,
    pub manifest_version: String,
    pub has_persisted_state: bool,
    pub health_state: HealthState,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub status: ServiceStatus,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub is_service_group: bool,
}

/// Native count limits apply to subqueries; a divergent CM/FM union can exceed
/// the requested count. There is no automatic paging or snapshot guarantee.
pub type ServiceListPage = QueryPage<Service>;
