//! Owned stateful replicas and stateless instances from the native list query.

use crate::{
    query_protocol::{result_tags, ListReply, Query},
    HealthState, PagedQuery, QueryPage, ServiceKind,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

bitflags::bitflags! {
    /// Native DWORD status mask. DEFAULT excludes Dropped; ALL is exactly
    /// 0xffff. Unknown bits are retained and forwarded without reinterpretation.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct ReplicaStatusFilter: u32 {
        const DEFAULT = 0;
        const ALL = 0xffff;
        const IN_BUILD = 1;
        const STANDBY = 2;
        const READY = 4;
        const DOWN = 8;
        const DROPPED = 16;
        const _ = !0;
    }
}

/// Partition-scoped replicas or instances. `ReplicaId` is the native facade's
/// selector for either kind; zero is omitted. IDs remain signed end to end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaQueryParameters {
    #[serde(rename = "PartitionId")]
    pub partition_id: Uuid,
    #[serde(
        rename = "ReplicaId",
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_zero"
    )]
    pub replica_or_instance_id: Option<i64>,
    #[serde(
        rename = "ReplicaStatusFilter",
        default,
        skip_serializing_if = "crate::query_arguments::is_default",
        with = "crate::query_arguments::flags"
    )]
    pub status_filter: ReplicaStatusFilter,
}

impl ReplicaQueryParameters {
    pub fn new(partition_id: Uuid) -> Self {
        Self {
            partition_id,
            replica_or_instance_id: None,
            status_filter: ReplicaStatusFilter::DEFAULT,
        }
    }
}

/// One replica request with separate paging. The native query supports tokens,
/// but rejects `paging.max_results`; pages are limited by message size.
pub type ReplicaQueryDescription = PagedQuery<ReplicaQueryParameters>;

/// An explicit best-effort page, not a replica configuration snapshot.
pub type ReplicaListPage = QueryPage<Replica>;

crate::native::strict_enum! {
    /// Native replica lifecycle state, distinct from the status-filter bitmask.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReplicaStatus {
        Invalid = 0,
        InBuild = 1,
        Standby = 2,
        Ready = 3,
        Down = 4,
        Dropped = 5,
    }
}

crate::native::strict_enum! {
    /// Native stateful replica role. Stateless records carry Unknown (zero).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReplicaRole {
        Unknown = 0,
        None = 1,
        Primary = 2,
        IdleSecondary = 3,
        ActiveSecondary = 4,
    }
}

/// Complete native positional result. Stateful records use `replica_id` and
/// `role`; stateless records use `instance_id`. The inactive ID is retained.
/// Address is an opaque service-location string, not necessarily a URI.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Replica {
    pub kind: ServiceKind,
    pub replica_id: i64,
    pub role: ReplicaRole,
    pub instance_id: i64,
    pub status: ReplicaStatus,
    pub address: String,
    pub node_name: String,
    pub health_state: HealthState,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub last_in_build_duration_in_seconds: i64,
}

impl Query for ReplicaQueryParameters {
    type Output = ReplicaListPage;
    type Reply = ListReply<Replica, { result_tags::REPLICA }>;
    const NAME: &'static str = "GetServicePartitionReplicaList";
    const SUPPORTS_MAX_RESULTS: bool = false;

    fn validate_record(replica: Replica) -> std::result::Result<Replica, &'static str> {
        if replica.kind == ServiceKind::Invalid {
            return Err("unsupported replica service kind");
        }
        if matches!(replica.health_state, HealthState::Unrecognized(_)) {
            return Err("unsupported replica health state");
        }
        Ok(replica)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partitions::tests::ID;

    #[test]
    fn replica_parameter_json_keeps_native_names_numeric_masks_and_nested_paging() {
        let mut parameters = ReplicaQueryParameters::new(ID.parse().unwrap());
        parameters.replica_or_instance_id = Some(i64::MIN);
        parameters.status_filter = ReplicaStatusFilter::ALL;
        let mut query = ReplicaQueryDescription::new(parameters);
        query.paging.continuation_token = Some("opaque".into());
        let json = serde_json::json!({
            "parameters": { "PartitionId": ID, "ReplicaId": i64::MIN, "ReplicaStatusFilter": 65535 },
            "paging": { "ContinuationToken": "opaque" }
        });
        assert_eq!(serde_json::to_value(&query).unwrap(), json);
        assert_eq!(
            serde_json::from_value::<ReplicaQueryDescription>(json).unwrap(),
            query
        );
        assert_eq!(
            serde_json::to_value(ReplicaQueryParameters::new(Uuid::nil())).unwrap(),
            serde_json::json!({ "PartitionId": Uuid::nil().to_string() })
        );
        let raw: ReplicaQueryParameters = serde_json::from_value(serde_json::json!({
            "PartitionId": ID, "ReplicaStatusFilter": u32::MAX,
        }))
        .unwrap();
        assert_eq!(raw.status_filter.bits(), u32::MAX);
        for json in [
            serde_json::json!({ "PartitionId": ID, "InstanceId": 1 }),
            serde_json::json!({ "PartitionId": ID, "ReplicaId": u64::MAX }),
            serde_json::json!({ "PartitionId": ID, "ReplicaStatusFilter": -1 }),
            serde_json::json!({ "PartitionId": ID, "ReplicaStatusFilter": 4294967296u64 }),
            serde_json::json!({ "PartitionId": ID, "ReplicaStatusFilter": "READY" }),
            serde_json::json!({ "PartitionId": "bad" }),
            serde_json::json!({ "PartitionId": ID, "Unknown": 1 }),
        ] {
            assert!(serde_json::from_value::<ReplicaQueryParameters>(json).is_err());
        }
    }

    #[test]
    fn replica_record_json_retains_scalar_kind_and_both_signed_ids() {
        let json = serde_json::json!({
            "kind": 2,
            "replica_id": i64::MIN,
            "role": 4,
            "instance_id": 0,
            "status": 3,
            "address": "opaque",
            "node_name": "_Node_0",
            "health_state": 65535,
            "last_in_build_duration_in_seconds": -1,
        });
        let replica: Replica = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(replica.replica_id, i64::MIN);
        assert_eq!(replica.role, ReplicaRole::ActiveSecondary);
        assert_eq!(replica.last_in_build_duration_in_seconds, -1);
        for (field, value) in [
            ("kind", serde_json::json!({ "Stateful": {} })),
            ("role", serde_json::json!("Primary")),
            ("replica_id", serde_json::json!(u64::MAX)),
            ("status", serde_json::json!(6)),
            ("last_in_build_duration_in_seconds", serde_json::Value::Null),
        ] {
            let mut malformed = json.clone();
            malformed[field] = value;
            assert!(
                serde_json::from_value::<Replica>(malformed).is_err(),
                "{field}"
            );
        }
        let mut missing = json;
        missing
            .as_object_mut()
            .unwrap()
            .remove("last_in_build_duration_in_seconds");
        assert!(serde_json::from_value::<Replica>(missing).is_err());
    }
}
