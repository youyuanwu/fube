//! Owned application list data for the explicitly supported native query profile.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Error, HealthState, NamingUri, PagedQuery, QueryPage, Result};

bitflags::bitflags! {
    /// Native definition-kind selection bits, distinct from result enum numbers.
    ///
    /// Use [`Self::try_from_bits`] for the supported query profile: combinations
    /// of bits 1/2/4 or exact ALL (65535). The generated `from_bits` only checks
    /// the bitflags mask, which includes every low bit because ALL is a sentinel.
    /// Queries revalidate masks from all constructors, operators and Serde.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct ApplicationDefinitionKindFilter: u32 {
        const DEFAULT = 0;
        const ALL = 65535;
        const SERVICE_FABRIC_APPLICATION_DESCRIPTION = 1;
        const COMPOSE = 2;
        const MESH_APPLICATION_DESCRIPTION = 4;
    }
}

impl ApplicationDefinitionKindFilter {
    /// Reject unnamed bits rather than accepting native's ignored-bit behavior.
    pub fn try_from_bits(bits: u32) -> Result<Self> {
        let kinds = Self::SERVICE_FABRIC_APPLICATION_DESCRIPTION
            | Self::COMPOSE
            | Self::MESH_APPLICATION_DESCRIPTION;
        if bits != Self::ALL.bits() && bits & !kinds.bits() != 0 {
            return Err(Error::InvalidInput(
                "unsupported application definition filter bits",
            ));
        }
        Ok(Self::from_bits_retain(bits))
    }
}

/// Exactly one native application selector; combinations are not supported.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplicationListFilter {
    #[default]
    All,
    Name(NamingUri),
    TypeName(String),
    DefinitionKind(ApplicationDefinitionKindFilter),
}

/// Native application selectors. Application parameters are included by default.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationQueryParameters {
    pub filter: ApplicationListFilter,
    pub exclude_application_parameters: bool,
}

/// One application-list request with separate parameters and paging.
pub type ApplicationQueryDescription = PagedQuery<ApplicationQueryParameters>;

/// Native application deployment status, not its health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationStatus {
    Invalid,
    Ready,
    Upgrading,
    Creating,
    Deleting,
    Failed,
}
crate::native::strict_enum!(ApplicationStatus {
    Invalid = 0, Ready = 1, Upgrading = 2, Creating = 3, Deleting = 4, Failed = 5,
});

/// Native result definition kind. Missing optional suffix defaults to Invalid.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ApplicationDefinitionKind {
    ServiceFabricApplicationDescription,
    Compose,
    MeshApplicationDescription,
    #[default]
    Invalid,
}
crate::native::strict_enum!(ApplicationDefinitionKind {
    ServiceFabricApplicationDescription = 0, Compose = 1, MeshApplicationDescription = 2, Invalid = 65535,
});

/// An owned aggregate observation in native field order.
/// Health may be Unknown when unavailable. Missing trailing fields use native
/// defaults; application-list replies require deprecated upgrade fields to be empty.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Application {
    pub name: NamingUri,
    pub type_name: String,
    pub type_version: String,
    pub status: ApplicationStatus,
    pub health_state: HealthState,
    #[serde(deserialize_with = "crate::native::string_map::deserialize")]
    pub parameters: BTreeMap<String, String>,
    /// Deprecated native upgrade version, empty in the supported query profile.
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub upgrade_type_version: String,
    /// Deprecated native upgrade parameters, empty in the supported query profile.
    #[serde(deserialize_with = "crate::native::string_map::deserialize_trailing")]
    pub upgrade_parameters: BTreeMap<String, String>,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub definition_kind: ApplicationDefinitionKind,
}

/// One native page, not an atomic snapshot. Inspect the opaque continuation even
/// without a count limit; native byte limits can also produce a next page.
pub type ApplicationListPage = QueryPage<Application>;
