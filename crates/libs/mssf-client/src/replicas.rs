//! Owned stateful replicas and stateless instances from the native list query.

use crate::{
    query_protocol::{result_tags, ListReply, Query},
    Error, HealthState, PagedQuery, QueryPage, Result, ServiceKind,
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

    fn validate_record(replica: Replica) -> Result<Replica> {
        if replica.kind == ServiceKind::Invalid {
            return Err(Error::Protocol("unsupported replica service kind"));
        }
        if matches!(replica.health_state, HealthState::Unrecognized(_)) {
            return Err(Error::Protocol("unsupported replica health state"));
        }
        Ok(replica)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        partitions::tests::ID,
        query_protocol::{encode_query, tests::assert_request},
        Paging,
    };
    use mssf_transport::serialization::{serde::from_slice, Encoder};
    use std::num::NonZeroU64;

    // ServiceModel/ServiceReplicaQueryResult.h FABRIC_FIELDS_09.
    fn base(
        e: &mut Encoder,
        kind: i64,
        replica_id: i64,
        instance_id: i64,
        role: i64,
        status: i64,
        health: i64,
    ) -> Result<()> {
        e.i64(kind);
        e.i64(replica_id);
        e.i64(role);
        e.i64(instance_id);
        e.i64(status);
        e.string(r#"{"Endpoints":{"rpc":"opaque🦀"}}"#)?;
        e.string("_Node_0")?;
        e.i64(health);
        Ok(())
    }

    fn fields(e: &mut Encoder, kind: i64, id: i64, suffix: bool) -> Result<()> {
        base(
            e,
            kind,
            if kind == 2 { id } else { 0 },
            if kind == 1 { id } else { 0 },
            if kind == 2 { 2 } else { 0 },
            3,
            1,
        )?;
        if suffix {
            e.i64(i64::MAX);
        }
        Ok(())
    }

    fn decode_fields(fields: impl FnOnce(&mut Encoder) -> Result<()>) -> Result<Replica> {
        let mut e = Encoder::new();
        e.object(fields)?;
        ReplicaQueryParameters::validate_record(from_slice(&e.finish())?)
    }

    #[test]
    fn replica_arguments_preserve_signed_ids_numeric_masks_and_omissions() {
        let mut parameters = ReplicaQueryParameters::new(ID.parse().unwrap());
        assert_request(
            &parameters,
            Some(&Paging::default()),
            "GetServicePartitionReplicaList",
            &[("PartitionId", ID)],
        );
        parameters.replica_or_instance_id = Some(0);
        assert_request(
            &parameters,
            Some(&Paging {
                continuation_token: Some(String::new()),
                ..Default::default()
            }),
            "GetServicePartitionReplicaList",
            &[("PartitionId", ID)],
        );
        for id in [i64::MIN, -1, 1, i64::MAX] {
            parameters.replica_or_instance_id = Some(id);
            parameters.status_filter = ReplicaStatusFilter::READY | ReplicaStatusFilter::DOWN;
            assert_request(
                &parameters,
                Some(&Paging {
                    continuation_token: Some("opaque".into()),
                    max_results: None,
                }),
                "GetServicePartitionReplicaList",
                &[
                    ("PartitionId", ID),
                    ("ReplicaId", &id.to_string()),
                    ("ReplicaStatusFilter", "12"),
                    ("ContinuationToken", "opaque"),
                ],
            );
        }
        parameters.status_filter = ReplicaStatusFilter::from_bits_retain(u32::MAX);
        parameters.replica_or_instance_id = None;
        parameters.partition_id = Uuid::nil();
        assert_request(
            &parameters,
            None,
            "GetServicePartitionReplicaList",
            &[
                ("PartitionId", "00000000-0000-0000-0000-000000000000"),
                ("ReplicaStatusFilter", "4294967295"),
            ],
        );
        for limit in [1, i64::MAX as u64, i64::MAX as u64 + 1] {
            assert!(matches!(
                encode_query(
                    &parameters,
                    Some(&Paging {
                        max_results: NonZeroU64::new(limit),
                        ..Default::default()
                    })
                ),
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[test]
    fn replicas_decode_stateful_and_stateless_with_full_signed_id_range() {
        for kind in [1, 2] {
            for id in [i64::MIN, -1, 0, 1, i64::MAX] {
                let replica = decode_fields(|e| fields(e, kind, id, true)).unwrap();
                assert_eq!(
                    replica.kind,
                    if kind == 1 {
                        ServiceKind::Stateless
                    } else {
                        ServiceKind::Stateful
                    }
                );
                assert_eq!(replica.replica_id, if kind == 2 { id } else { 0 });
                assert_eq!(replica.instance_id, if kind == 1 { id } else { 0 });
                assert_eq!(
                    replica.role,
                    if kind == 2 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::Unknown
                    }
                );
                assert_eq!(replica.status, ReplicaStatus::Ready);
                assert_eq!(replica.address, r#"{"Endpoints":{"rpc":"opaque🦀"}}"#);
                assert_eq!(replica.node_name, "_Node_0");
                assert_eq!(replica.health_state, HealthState::Ok);
                assert_eq!(replica.last_in_build_duration_in_seconds, i64::MAX);
            }
        }
    }

    #[test]
    fn replica_suffix_default_is_not_a_fallback_for_bad_metadata_or_truncation() {
        let replica = decode_fields(|e| fields(e, 2, 123, false)).unwrap();
        assert_eq!(replica.last_in_build_duration_in_seconds, 0);
        assert!(matches!(
            decode_fields(|e| {
                fields(e, 2, 123, false)?;
                e.string("bad duration metadata")
            }),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            decode_fields(|e| {
                e.i64(2);
                e.i64(123);
                e.i64(2);
                e.i64(0);
                e.i64(3);
                e.string("address")?;
                e.string("node")
            }),
            Err(Error::Protocol(_))
        ));
        let mut e = Encoder::new();
        e.object(|e| fields(e, 2, 123, true)).unwrap();
        let bytes = e.finish();
        let mut bytes = bytes;
        // Keep the enclosing extent valid but advertise another varint byte.
        let last_duration_byte = bytes.len() - 3;
        bytes[last_duration_byte] |= 0x80;
        assert!(from_slice::<Replica>(&bytes).is_err());
    }

    #[test]
    fn replica_enum_codes_and_filter_masks_keep_their_distinct_numeric_meanings() {
        for role in 0..=4 {
            let replica = decode_fields(|e| base(e, 2, -1, 0, role, 0, 65535)).unwrap();
            assert_eq!(serde_json::to_value(replica.role).unwrap(), role);
            assert_eq!(replica.status, ReplicaStatus::Invalid);
            assert_eq!(replica.health_state, HealthState::Unknown);
        }
        for status in 0..=5 {
            let replica = decode_fields(|e| base(e, 2, 1, 0, 4, status, 0)).unwrap();
            assert_eq!(serde_json::to_value(replica.status).unwrap(), status);
        }
        for kind in [-1, 0, 3, i64::MAX] {
            assert!(decode_fields(|e| fields(e, kind, 1, true)).is_err());
        }
        for (role, status, health) in [
            (-1, 3, 1),
            (5, 3, 1),
            (2, -1, 1),
            (2, 6, 1),
            (2, 3, 4),
            (2, 3, -1),
        ] {
            assert!(decode_fields(|e| base(e, 2, 1, 0, role, status, health)).is_err());
        }
        assert_eq!(ReplicaStatusFilter::DEFAULT.bits(), 0);
        assert_eq!(ReplicaStatusFilter::ALL.bits(), 0xffff);
        assert_eq!(ReplicaStatusFilter::IN_BUILD.bits(), 1);
        assert_eq!(ReplicaStatusFilter::STANDBY.bits(), 2);
        assert_eq!(ReplicaStatusFilter::READY.bits(), 4);
        assert_eq!(ReplicaStatusFilter::DOWN.bits(), 8);
        assert_eq!(ReplicaStatusFilter::DROPPED.bits(), 16);
        assert_eq!(ReplicaStatusFilter::all().bits(), u32::MAX);
        for invalid in [-1, 5, i64::MAX] {
            assert!(serde_json::from_value::<ReplicaRole>(invalid.into()).is_err());
        }
        for invalid in [-1, 6, i64::MAX] {
            assert!(serde_json::from_value::<ReplicaStatus>(invalid.into()).is_err());
        }
    }

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
