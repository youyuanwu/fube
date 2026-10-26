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
    pub(crate) fn validate(&self) -> Result<()> {
        if let Self::Named(name) = self {
            if name.is_empty()
                || name.contains('\0')
                || name.encode_utf16().count() > mssf_transport::serialization::MAX_STRING_UNITS
            {
                return Err(Error::InvalidInput("invalid named partition key"));
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
    pub(crate) fn validate_previous(&self, name: &NamingUri, key: &PartitionKey) -> Result<()> {
        if name != &self.refresh.name
            || self.service_name != self.refresh.name
            || self.partition_information != self.refresh.partition
            || self.version != self.refresh.version
            || !key.selects(&self.refresh.partition)
        {
            return Err(Error::InvalidInput(
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
