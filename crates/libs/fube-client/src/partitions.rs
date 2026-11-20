//! Owned service partition list records and native query parameters.

use crate::{
    query_protocol::{result_tags, ListReply, Query},
    HealthState, NamingUri, PagedQuery, QueryPage, ServiceKind,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A service-scoped partition list. A nil selector is omitted by the native facade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionQueryParameters {
    #[serde(rename = "ServiceName")]
    pub service_name: NamingUri,
    #[serde(
        rename = "PartitionId",
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_nil"
    )]
    pub partition_id: Option<Uuid>,
}

impl PartitionQueryParameters {
    pub fn new(service_name: NamingUri) -> Self {
        Self {
            service_name,
            partition_id: None,
        }
    }
}

/// One partition request with separate paging. The native query supports tokens,
/// but rejects `paging.max_results`; pages are limited by message size.
pub type PartitionQueryDescription = PagedQuery<PartitionQueryParameters>;

/// An explicit best-effort page, not a partition-map snapshot.
pub type PartitionListPage = QueryPage<Partition>;

crate::native::strict_enum! {
    /// Structural partition key kind. Invalid and unknown kinds are unsupported.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PartitionKind {
        Singleton = 1,
        Int64Range = 2,
        Named = 3,
    }
}

/// Native partition metadata, retaining all five positional fields.
///
/// Only Named uses `name`; only Int64Range uses the inclusive signed key bounds.
/// Native tag validation is selected by the containing field's adapter; standalone
/// native consumers can use `fubst::serde::Untagged<Self>`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PartitionInformation {
    pub kind: PartitionKind,
    #[serde(deserialize_with = "crate::native::guid::deserialize")]
    pub id: Uuid,
    pub name: String,
    pub low_key: i64,
    pub high_key: i64,
}

crate::native::strict_enum! {
    /// Native partition lifecycle state, independent of aggregated health.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PartitionStatus {
        Invalid = 0,
        Ready = 1,
        NotReady = 2,
        InQuorumLoss = 3,
        Reconfiguring = 4,
        Deleting = 5,
    }
}

/// Reliability::Epoch wire representation. The native FM partition-list sender
/// projects its value with ToPrimaryEpoch: its configuration low 32 bits are
/// masked, unlike the full RA/RAP configuration epoch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct ConfigurationEpoch {
    pub configuration_version: i64,
    pub data_loss_version: i64,
}

/// Native partition result. Stateful records use replica-set sizes, quorum-loss
/// duration and epoch; stateless records use the signed instance count (-1 means
/// every node). Inactive fields remain available rather than being discarded.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Partition {
    pub kind: ServiceKind,
    #[serde(deserialize_with = "crate::native::untagged::deserialize")]
    pub partition_information: PartitionInformation,
    pub instance_count: i32,
    pub target_replica_set_size: u32,
    pub min_replica_set_size: u32,
    pub health_state: HealthState,
    pub status: PartitionStatus,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub last_quorum_loss_duration_in_seconds: i64,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub current_configuration_epoch: ConfigurationEpoch,
}

impl Query for PartitionQueryParameters {
    type Output = PartitionListPage;
    type Reply = ListReply<Partition, { result_tags::PARTITION }>;
    const NAME: &'static str = "GetServicePartitionList";
    const SUPPORTS_MAX_RESULTS: bool = false;

    fn validate_record(partition: Partition) -> std::result::Result<Partition, &'static str> {
        if partition.kind == ServiceKind::Invalid {
            return Err("unsupported partition service kind");
        }
        if matches!(partition.health_state, HealthState::Unrecognized(_)) {
            return Err("unsupported partition health state");
        }
        let information = &partition.partition_information;
        if information.kind == PartitionKind::Int64Range
            && information.low_key > information.high_key
        {
            return Err("partition key range is inverted");
        }
        Ok(partition)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) const ID: &str = "00112233-4455-6677-8899-aabbccddeeff";

    #[test]
    fn partition_record_json_keeps_numeric_kinds_signed_counts_and_raw_epoch_order() {
        let json = serde_json::json!({
            "kind": 2,
            "partition_information": {
                "kind": 2, "id": ID, "name": "", "low_key": i64::MIN, "high_key": i64::MAX,
            },
            "instance_count": i32::MIN,
            "target_replica_set_size": u32::MAX,
            "min_replica_set_size": 3,
            "health_state": 1,
            "status": 3,
            "last_quorum_loss_duration_in_seconds": -1,
            "current_configuration_epoch": {
                "configuration_version": 12, "data_loss_version": 34,
            },
        });
        let partition: Partition = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(partition.partition_information.id.to_string(), ID);
        assert_eq!(partition.instance_count, i32::MIN);
        assert_eq!(partition.target_replica_set_size, u32::MAX);
        assert_eq!(partition.status, PartitionStatus::InQuorumLoss);
        assert_eq!(
            partition.current_configuration_epoch,
            ConfigurationEpoch {
                configuration_version: 12,
                data_loss_version: 34,
            }
        );
        for (field, value) in [
            ("kind", serde_json::json!({ "Stateful": {} })),
            ("instance_count", serde_json::json!(i64::MAX)),
            ("target_replica_set_size", serde_json::json!(-1)),
            ("min_replica_set_size", serde_json::json!(u64::MAX)),
            ("status", serde_json::json!("Ready")),
            ("current_configuration_epoch", serde_json::Value::Null),
        ] {
            let mut malformed = json.clone();
            malformed[field] = value;
            assert!(
                serde_json::from_value::<Partition>(malformed).is_err(),
                "{field}"
            );
        }
        for field in [
            "last_quorum_loss_duration_in_seconds",
            "current_configuration_epoch",
        ] {
            let mut missing = json.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<Partition>(missing).is_err());
        }
    }

    #[test]
    fn partition_parameter_json_is_native_named_strict_and_nested() {
        let mut parameters = PartitionQueryParameters::new("fabric:/App/Service".parse().unwrap());
        parameters.partition_id = Some(ID.parse().unwrap());
        let query = PartitionQueryDescription::new(parameters);
        let json = serde_json::json!({
            "parameters": { "ServiceName": "fabric:/App/Service", "PartitionId": ID },
            "paging": {}
        });
        assert_eq!(serde_json::to_value(&query).unwrap(), json);
        assert_eq!(
            serde_json::from_value::<PartitionQueryDescription>(json).unwrap(),
            query
        );
        for json in [
            serde_json::json!({ "ServiceName": "fabric:/App/Service", "Unexpected": 1 }),
            serde_json::json!({ "ServiceName": "fabric:/App/Service", "PartitionId": "bad" }),
            serde_json::json!({ "ServiceName": "fabric:/App/Service", "MaxResults": 1 }),
            serde_json::json!({ "ServiceName": "fabric:/bad/" }),
        ] {
            assert!(serde_json::from_value::<PartitionQueryParameters>(json).is_err());
        }
    }
}
