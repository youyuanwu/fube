//! Owned application list data for the explicitly supported native query profile.

use std::{collections::BTreeMap, num::NonZeroU64};

use serde::{Deserialize, Serialize};

use crate::{Error, HealthState, Result};

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
