//! Owned, uncached service endpoint discovery.
use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, NamingUri, NodeId, PartitionInformation, PartitionKind, Result, ServiceKind};

/// A partition key, independent of URI path syntax.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKey {
    Singleton,
    Int64(i64),
    Named(String),
}

impl PartitionKey {
    pub(crate) fn validate(&self) -> fubst::Result<()> {
        if let Self::Named(name) = self {
            if name.is_empty() || name.contains('\0') {
                return Err(fubst::Error::InvalidInput("invalid named partition key"));
            }
        }
        Ok(())
    }

    pub(crate) fn selects(&self, info: &PartitionInformation) -> bool {
        match (self, info.kind) {
            (Self::Singleton, PartitionKind::Singleton) => true,
            (Self::Int64(key), PartitionKind::Int64Range) => {
                info.low_key <= *key && *key <= info.high_key
            }
            (Self::Named(key), PartitionKind::Named) => key == &info.name,
            _ => false,
        }
    }
}

/// Owned resolution selection and minimal previous refresh metadata, not an endpoint cache.
#[derive(Debug, Clone)]
pub struct ResolutionRequest {
    pub(crate) name: NamingUri,
    pub(crate) key: PartitionKey,
    pub(crate) previous: Option<Refresh>,
}

impl ResolutionRequest {
    /// Validate the selection before copying it. Previous results must retain
    /// their original identity/version and select the requested partition.
    pub fn new(
        name: &NamingUri,
        key: &PartitionKey,
        previous: Option<&ResolvedServicePartition>,
    ) -> fubst::Result<Self> {
        key.validate()?;
        if let Some(previous) = previous {
            previous.validate_previous(name, key)?;
        }
        Ok(Self {
            name: name.clone(),
            key: key.clone(),
            previous: previous.map(|result| result.refresh.clone()),
        })
    }
}

/// Native generation ordering includes its owning node ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Generation {
    pub value: i64,
    pub owner: NodeId,
}

impl Default for Generation {
    fn default() -> Self {
        Self {
            value: 0,
            owner: NodeId { high: 0, low: 0 },
        }
    }
}

/// Identity-scoped endpoint order, not a timestamp or globally ordered integer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceEndpointVersion {
    pub(crate) name: NamingUri,
    pub(crate) partition: Uuid,
    pub(crate) generation: Generation,
    pub(crate) lookup: i64,
}

impl ServiceEndpointVersion {
    pub fn compare(&self, other: &Self) -> Result<Ordering> {
        if self.name != other.name || self.partition != other.partition {
            return Err(Error::InvalidInput(
                "endpoint versions have different identities",
            ));
        }
        Ok((self.generation, self.lookup).cmp(&(other.generation, other.lookup)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceEndpointRole {
    Stateless,
    StatefulPrimary,
    StatefulSecondary,
}

/// An opaque service-published address, not necessarily a URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceEndpoint {
    pub address: String,
    pub role: ServiceEndpointRole,
}

#[derive(Debug, Clone)]
pub struct ResolvedServicePartition {
    pub service_name: NamingUri,
    pub partition_information: PartitionInformation,
    pub kind: ServiceKind,
    pub endpoints: Vec<ServiceEndpoint>,
    pub version: ServiceEndpointVersion,
    pub(crate) refresh: Refresh,
}

#[derive(Debug, Clone)]
pub(crate) struct Refresh {
    pub name: NamingUri,
    pub partition: PartitionInformation,
    pub version: ServiceEndpointVersion,
    pub store: i64,
}

impl ResolvedServicePartition {
    pub(crate) fn validate_previous(
        &self,
        name: &NamingUri,
        key: &PartitionKey,
    ) -> fubst::Result<()> {
        if name != &self.refresh.name
            || self.service_name != self.refresh.name
            || self.partition_information != self.refresh.partition
            || self.version != self.refresh.version
            || !key.selects(&self.refresh.partition)
        {
            return Err(fubst::Error::InvalidInput(
                "previous result is not bound to this selection",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_keys_reject_empty_and_nul_without_a_legacy_string_ceiling() {
        let name = "fabric:/App/Service".parse().unwrap();
        for key in ["".into(), "blue\0green".into()] {
            assert!(ResolutionRequest::new(&name, &PartitionKey::Named(key), None).is_err());
        }
        let key = PartitionKey::Named("x".repeat(fubst::MAX_STRING_UNITS + 1));
        ResolutionRequest::new(&name, &key, None).unwrap();
    }

    #[test]
    fn previous_metadata_is_selection_bound_but_does_not_retain_endpoints() {
        let name: NamingUri = "fabric:/App/Service".parse().unwrap();
        let info = PartitionInformation {
            kind: PartitionKind::Int64Range,
            id: Uuid::new_v4(),
            name: String::new(),
            low_key: -10,
            high_key: 9,
        };
        let version = ServiceEndpointVersion {
            name: name.clone(),
            partition: info.id,
            generation: Generation::default(),
            lookup: 1,
        };
        let mut result = ResolvedServicePartition {
            service_name: name.clone(),
            partition_information: info.clone(),
            kind: ServiceKind::Stateful,
            endpoints: vec![],
            version: version.clone(),
            refresh: Refresh {
                name: name.clone(),
                partition: info,
                version,
                store: 1,
            },
        };
        result.endpoints.push(ServiceEndpoint {
            address: "caller-edited".into(),
            role: ServiceEndpointRole::StatefulPrimary,
        });
        ResolutionRequest::new(&name, &PartitionKey::Int64(-10), Some(&result)).unwrap();
        ResolutionRequest::new(&name, &PartitionKey::Int64(9), Some(&result)).unwrap();
        assert!(ResolutionRequest::new(&name, &PartitionKey::Int64(10), Some(&result)).is_err());
        assert!(ResolutionRequest::new(&name, &PartitionKey::Singleton, Some(&result)).is_err());
        let other = "fabric:/Other/Service".parse().unwrap();
        assert!(ResolutionRequest::new(&other, &PartitionKey::Int64(0), Some(&result)).is_err());
        let mut altered = result.clone();
        altered.partition_information.id = Uuid::new_v4();
        assert!(ResolutionRequest::new(&name, &PartitionKey::Int64(0), Some(&altered)).is_err());
        result.version.lookup += 1;
        assert!(ResolutionRequest::new(&name, &PartitionKey::Int64(0), Some(&result)).is_err());
    }

    #[test]
    fn version_order_is_generation_owner_then_lookup_and_identity_checked() {
        let base = ServiceEndpointVersion {
            name: "fabric:/App/Service".parse().unwrap(),
            partition: Uuid::new_v4(),
            generation: Generation::default(),
            lookup: 10,
        };
        let mut newer = base.clone();
        newer.lookup += 1;
        assert_eq!(base.compare(&newer).unwrap(), Ordering::Less);
        newer.generation.owner.low = 1;
        newer.lookup = 0;
        assert_eq!(base.compare(&newer).unwrap(), Ordering::Less);
        newer.generation.value = 1;
        newer.generation.owner.low = 0;
        assert_eq!(base.compare(&newer).unwrap(), Ordering::Less);
        newer.partition = Uuid::new_v4();
        assert!(base.compare(&newer).is_err());
        newer.partition = base.partition;
        newer.name = "fabric:/Different".parse().unwrap();
        assert!(base.compare(&newer).is_err());
    }
}
