use mssf_transport::{
    message::Message,
    serialization::serde::{from_slice, to_vec, CountedStrings, Guid, Pointer},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    resolution::{Generation, Refresh},
    Error, NamingUri, PartitionInformation, PartitionKey, PartitionKind, ResolvedServicePartition,
    Result, ServiceEndpoint, ServiceEndpointRole, ServiceEndpointVersion, ServiceKind,
};

#[derive(Serialize)]
struct Key {
    kind: i64,
    integer: i64,
    name: String,
    whole_service: bool,
}
#[derive(Serialize)]
struct LocationVersion {
    lookup: i64,
    generation: Generation,
    store: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Cuid {
    pub id: Guid,
}
#[derive(Serialize)]
struct RequestData {
    key: Key,
    version: LocationVersion,
    cuid: Cuid,
    include_description: bool,
}
#[derive(Serialize)]
struct Request<'a> {
    name: &'a NamingUri,
    request: RequestData,
    bypass_cache: bool,
}

pub(crate) fn encode(
    name: &NamingUri,
    key: &PartitionKey,
    previous: Option<&ResolvedServicePartition>,
) -> Result<Vec<u8>> {
    key.validate()?;
    if let Some(previous) = previous {
        previous.validate_previous(name, key)?;
    }
    let key = match key {
        PartitionKey::Singleton => Key {
            kind: 1,
            integer: 0,
            name: String::new(),
            whole_service: false,
        },
        PartitionKey::Int64(value) => Key {
            kind: 2,
            integer: *value,
            name: String::new(),
            whole_service: false,
        },
        PartitionKey::Named(value) => Key {
            kind: 3,
            integer: 0,
            name: value.clone(),
            whole_service: false,
        },
    };
    let (lookup, generation, store, id) =
        previous.map_or((-1, Generation::default(), -1, Uuid::nil()), |p| {
            (
                p.refresh.version.lookup,
                p.refresh.version.generation,
                p.refresh.store,
                p.refresh.partition.id,
            )
        });
    to_vec(&Request {
        name,
        request: RequestData {
            key,
            version: LocationVersion {
                lookup,
                generation,
                store,
            },
            cuid: Cuid {
                id: Guid(id.to_bytes_le()),
            },
            include_description: false,
        },
        bypass_cache: false,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReplicaSet {
    pub stateful: bool,
    pub primary_valid: bool,
    pub primary: String,
    pub replicas: CountedStrings<Vec<String>>,
    pub lookup: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub cuid: Cuid,
    pub name: String,
    pub replicas: ReplicaSet,
    pub found: bool,
}

impl TableEntry {
    pub fn endpoints(&self) -> (ServiceKind, Vec<ServiceEndpoint>) {
        let set = &self.replicas;
        let mut endpoints =
            Vec::with_capacity(set.replicas.0.len() + usize::from(set.primary_valid));
        if set.stateful && set.primary_valid {
            endpoints.push(ServiceEndpoint {
                address: set.primary.clone(),
                role: ServiceEndpointRole::StatefulPrimary,
            });
        }
        endpoints.extend(set.replicas.0.iter().map(|address| ServiceEndpoint {
            address: address.clone(),
            role: if set.stateful {
                ServiceEndpointRole::StatefulSecondary
            } else {
                ServiceEndpointRole::Stateless
            },
        }));
        (
            if set.stateful {
                ServiceKind::Stateful
            } else {
                ServiceKind::Stateless
            },
            endpoints,
        )
    }

    pub fn identity(&self, generation: Generation) -> Result<ServiceEndpointVersion> {
        let name = NamingUri::parse(&self.name)
            .map_err(|_| Error::Protocol("invalid endpoint service name"))?;
        let partition = Uuid::from_bytes_le(self.cuid.id.0);
        if partition.is_nil() || self.replicas.lookup < 0 {
            return Err(Error::Protocol("invalid endpoint identity/version"));
        }
        Ok(ServiceEndpointVersion {
            name,
            partition,
            generation,
            lookup: self.replicas.lookup,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PartitionData {
    pub kind: PartitionKind,
    pub low: i64,
    pub high: i64,
    pub name: String,
}
// Bounded object framing still validates any unknown description tail. No hidden lookup.
#[derive(Serialize, Deserialize)]
pub(crate) struct Description {}
#[derive(Serialize, Deserialize)]
pub(crate) struct ResolveReply {
    pub group: bool,
    pub locations: TableEntry,
    pub partition: PartitionData,
    pub store: i64,
    pub generation: Generation,
    pub description: Pointer<Description>,
}

pub(crate) fn decode(message: Message) -> Result<ResolvedServicePartition> {
    // NamingMessage::GetResolveServiceReply actually uses ResolveServiceAction.
    crate::protocol::check_reply(&message, &["ResolveServiceRequest"])?;
    let reply: ResolveReply = from_slice(&message.body)?;
    if reply.group {
        return Err(Error::InvalidInput(
            "service-group resolution is unsupported",
        ));
    }

    let version = reply.locations.identity(reply.generation)?;
    if !reply.locations.found {
        return Err(Error::Protocol(
            "successful resolution has no service location",
        ));
    }
    let info = PartitionInformation {
        kind: reply.partition.kind,
        id: version.partition,
        name: reply.partition.name,
        low_key: reply.partition.low,
        high_key: reply.partition.high,
    };
    if (info.kind == PartitionKind::Int64Range && info.low_key > info.high_key)
        || (info.kind == PartitionKind::Named && (info.name.is_empty() || info.name.contains('\0')))
    {
        return Err(Error::Protocol("invalid resolved partition details"));
    }
    let (kind, endpoints) = reply.locations.endpoints();
    Ok(ResolvedServicePartition {
        service_name: version.name.clone(),
        partition_information: info.clone(),
        kind,
        endpoints,
        refresh: Refresh {
            name: version.name.clone(),
            partition: info,
            version: version.clone(),
            store: reply.store,
        },
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opaque_roles_empty_and_invalid_identities() {
        let mut entry = TableEntry {
            cuid: Cuid {
                id: Guid(Uuid::new_v4().to_bytes_le()),
            },
            name: "fabric:/App/Service".into(),
            replicas: ReplicaSet {
                stateful: true,
                primary_valid: false,
                primary: "not valid".into(),
                replicas: CountedStrings(vec!["opaque".into()]),
                lookup: 1,
            },
            found: true,
        };
        let (kind, endpoints) = entry.endpoints();
        assert_eq!(kind, ServiceKind::Stateful);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].role, ServiceEndpointRole::StatefulSecondary);
        entry.replicas.stateful = false;
        assert_eq!(entry.endpoints().1[0].role, ServiceEndpointRole::Stateless);
        entry.replicas.replicas.0.clear();
        assert!(entry.endpoints().1.is_empty());
        entry.cuid.id = Guid([0; 16]);
        assert!(entry.identity(Generation::default()).is_err());
    }
}
