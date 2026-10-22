//! Owned application list data for the explicitly supported native query profile.

use std::{collections::BTreeMap, num::NonZeroU64, ops::BitOr};

use crate::{Error, HealthState, Result};

/// Native definition-kind selection bits, distinct from result enum numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationDefinitionKindFilter(u32);

impl ApplicationDefinitionKindFilter {
    pub const DEFAULT: Self = Self(0);
    pub const ALL: Self = Self(65535);
    pub const SERVICE_FABRIC_APPLICATION_DESCRIPTION: Self = Self(1);
    pub const COMPOSE: Self = Self(2);
    pub const MESH_APPLICATION_DESCRIPTION: Self = Self(4);

    /// Reject unnamed bits rather than accepting native's ignored-bit behavior.
    pub fn from_bits(bits: u32) -> Result<Self> {
        if bits != 65535 && bits & !7 != 0 {
            return Err(Error::InvalidInput(
                "unsupported application definition filter bits",
            ));
        }
        Ok(Self(bits))
    }

    pub fn bits(self) -> u32 {
        self.0
    }
}

impl BitOr for ApplicationDefinitionKindFilter {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Exactly one native application selector; combinations are not supported.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ApplicationListFilter {
    #[default]
    All,
    Name(String),
    TypeName(String),
    DefinitionKind(ApplicationDefinitionKindFilter),
}

/// One native page request. Empty continuation means absent. Parameters are
/// included by default; count is positive and must fit a signed native Int64.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplicationQueryDescription {
    pub filter: ApplicationListFilter,
    pub exclude_application_parameters: bool,
    pub continuation_token: Option<String>,
    pub max_results: Option<NonZeroU64>,
}

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

/// Native result definition kind. Missing optional suffix defaults to Invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationDefinitionKind {
    ServiceFabricApplicationDescription,
    Compose,
    MeshApplicationDescription,
    Invalid,
}

/// An owned aggregate observation. Health may be Unknown when unavailable.
/// Deprecated upgrade fields are not exposed; nonempty values are unsupported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Application {
    pub name: String,
    pub type_name: String,
    pub type_version: String,
    pub status: ApplicationStatus,
    pub health_state: HealthState,
    pub parameters: BTreeMap<String, String>,
    pub definition_kind: ApplicationDefinitionKind,
}

/// One native page, not an atomic snapshot. Inspect the opaque continuation even
/// without a count limit; native byte limits can also produce a next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationListPage {
    pub applications: Vec<Application>,
    pub continuation_token: Option<String>,
}
