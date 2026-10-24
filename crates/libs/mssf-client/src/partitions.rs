//! Owned service partition list records and native query parameters.

use crate::{
    query_protocol::{ListReply, Query},
    Error, HealthState, NamingUri, PagedQuery, QueryPage, Result, ServiceKind,
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
/// native consumers can use `mssf_transport::serialization::serde::Untagged<Self>`.
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

/// Raw Reliability::Epoch, not the masked public PrimaryEpoch projection.
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
    type Reply = ListReply<Partition, 5>;
    const NAME: &'static str = "GetServicePartitionList";
    const SUPPORTS_MAX_RESULTS: bool = false;

    fn validate_record(partition: Partition) -> Result<Partition> {
        if partition.kind == ServiceKind::Invalid {
            return Err(Error::Protocol("unsupported partition service kind"));
        }
        if matches!(partition.health_state, HealthState::Unrecognized(_)) {
            return Err(Error::Protocol("unsupported partition health state"));
        }
        let information = &partition.partition_information;
        if information.kind == PartitionKind::Int64Range
            && information.low_key > information.high_key
        {
            return Err(Error::Protocol("partition key range is inverted"));
        }
        Ok(partition)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::query_protocol::{decode_reply, encode_query};
    use mssf_transport::{
        message::{Header, Message},
        serialization::{serde::from_slice, Encoder},
    };
    use std::num::NonZeroU64;

    pub(crate) const ID: &str = "00112233-4455-6677-8899-aabbccddeeff";
    const GUID_BYTES: [u8; 16] = [
        0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];

    pub(crate) fn assert_request<Q: Query>(
        parameters: &Q,
        paging: Option<&crate::Paging>,
        name: &str,
        arguments: &[(&str, &str)],
    ) {
        let mut e = Encoder::new();
        e.object(|e| {
            e.object(|e| {
                e.object_array(arguments, |e, (key, value)| {
                    e.object(|e| e.string(key))?;
                    e.string(value)
                })
            })?;
            e.string(name)
        })
        .unwrap();
        assert_eq!(encode_query(parameters, paging).unwrap(), e.finish());
    }

    pub(crate) fn reply(
        tag: Option<i32>,
        record: Option<impl FnOnce(&mut Encoder) -> Result<()>>,
        error: i64,
        paging: Option<(&[u8], &str)>,
        single_item: bool,
    ) -> Message {
        let mut record = record;
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(2);
            let tag = tag.map(i32::to_le_bytes);
            e.pointer(tag.as_ref().map(|tag| &tag[..]), |e| {
                e.object_array(if record.is_some() { &[()][..] } else { &[] }, |e, _| {
                    record.take().unwrap()(e)
                })?;
                e.pointer(single_item.then_some(&[][..]), |_| Ok(()))
            })?;
            e.i64(error);
            e.pointer(paging.map(|(tag, _)| tag), |e| e.string(paging.unwrap().1))
        })
        .unwrap();
        Message {
            headers: Vec::new(),
            body: e.finish(),
        }
    }

    fn information(e: &mut Encoder, key: i64, low: i64, high: i64) -> Result<()> {
        e.object(|e| {
            e.i64(key);
            e.guid(&GUID_BYTES);
            e.string(if key == 3 { "named🦀" } else { "" })?;
            e.i64(low);
            e.i64(high);
            Ok(())
        })
    }

    fn base(e: &mut Encoder, kind: i64, key: i64, low: i64, high: i64) -> Result<()> {
        e.i64(kind);
        information(e, key, low, high)?;
        e.i32(if kind == 1 { -1 } else { 0 });
        e.u32(if kind == 2 { u32::MAX } else { 0 });
        e.u32(if kind == 2 { 3 } else { 0 });
        e.i64(65535);
        e.i64(4);
        Ok(())
    }

    pub(crate) fn partition_fields(
        e: &mut Encoder,
        kind: i64,
        key: i64,
        suffix: usize,
    ) -> Result<()> {
        base(e, kind, key, i64::MIN, i64::MAX)?;
        if suffix >= 1 {
            e.i64(i64::MAX);
        }
        if suffix >= 2 {
            e.object(|e| {
                e.i64(i64::MIN);
                e.i64(i64::MAX);
                Ok(())
            })?;
        }
        Ok(())
    }

    fn decode_fields(fields: impl FnOnce(&mut Encoder) -> Result<()>) -> Result<Partition> {
        let mut e = Encoder::new();
        e.object(fields)?;
        PartitionQueryParameters::validate_record(from_slice(&e.finish())?)
    }

    #[test]
    fn partition_arguments_follow_facade_order_with_hyphenated_guids_and_omissions() {
        let mut parameters = PartitionQueryParameters::new("fabric:/App/Service".parse().unwrap());
        assert_request(
            &parameters,
            Some(&crate::Paging::default()),
            "GetServicePartitionList",
            &[("ServiceName", "fabric:/App/Service")],
        );
        parameters.partition_id = Some(Uuid::nil());
        assert_request(
            &parameters,
            None,
            "GetServicePartitionList",
            &[("ServiceName", "fabric:/App/Service")],
        );
        parameters.partition_id = Some(ID.parse().unwrap());
        assert_request(
            &parameters,
            Some(&crate::Paging {
                continuation_token: Some("opaque".into()),
                max_results: None,
            }),
            "GetServicePartitionList",
            &[
                ("ServiceName", "fabric:/App/Service"),
                ("PartitionId", ID),
                ("ContinuationToken", "opaque"),
            ],
        );
        for limit in [1, i64::MAX as u64, i64::MAX as u64 + 1] {
            assert!(matches!(
                encode_query(
                    &parameters,
                    Some(&crate::Paging {
                        max_results: NonZeroU64::new(limit),
                        ..Default::default()
                    })
                ),
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[test]
    fn partitions_decode_both_service_kinds_all_key_kinds_and_native_guid_order() {
        for kind in [1, 2] {
            for key in [1, 2, 3] {
                let page = decode_reply::<PartitionQueryParameters>(&reply(
                    Some(5),
                    Some(|e: &mut Encoder| partition_fields(e, kind, key, 2)),
                    0,
                    Some((&[], "opaque-next")),
                    false,
                ))
                .unwrap();
                assert_eq!(page.continuation_token.as_deref(), Some("opaque-next"));
                let partition = &page.items[0];
                assert_eq!(
                    partition.kind,
                    if kind == 1 {
                        ServiceKind::Stateless
                    } else {
                        ServiceKind::Stateful
                    }
                );
                assert_eq!(partition.partition_information.id.to_string(), ID);
                assert_eq!(
                    partition.partition_information.kind,
                    match key {
                        1 => PartitionKind::Singleton,
                        2 => PartitionKind::Int64Range,
                        _ => PartitionKind::Named,
                    }
                );
                assert_eq!(
                    partition.partition_information.name,
                    if key == 3 { "named🦀" } else { "" }
                );
                assert_eq!(partition.partition_information.low_key, i64::MIN);
                assert_eq!(partition.partition_information.high_key, i64::MAX);
                assert_eq!(partition.instance_count, if kind == 1 { -1 } else { 0 });
                assert_eq!(
                    partition.target_replica_set_size,
                    if kind == 2 { u32::MAX } else { 0 }
                );
                assert_eq!(
                    partition.min_replica_set_size,
                    if kind == 2 { 3 } else { 0 }
                );
                assert_eq!(partition.health_state, HealthState::Unknown);
                assert_eq!(partition.status, PartitionStatus::Reconfiguring);
                assert_eq!(partition.last_quorum_loss_duration_in_seconds, i64::MAX);
                assert_eq!(
                    partition.current_configuration_epoch,
                    ConfigurationEpoch {
                        configuration_version: i64::MIN,
                        data_loss_version: i64::MAX,
                    }
                );
            }
        }
    }

    #[test]
    fn partition_suffixes_default_only_when_absent_not_malformed_or_truncated() {
        for suffix in 0..=2 {
            let partition = decode_fields(|e| partition_fields(e, 2, 2, suffix)).unwrap();
            assert_eq!(
                partition.last_quorum_loss_duration_in_seconds,
                if suffix == 0 { 0 } else { i64::MAX }
            );
            assert_eq!(
                partition.current_configuration_epoch,
                if suffix < 2 {
                    ConfigurationEpoch::default()
                } else {
                    ConfigurationEpoch {
                        configuration_version: i64::MIN,
                        data_loss_version: i64::MAX,
                    }
                }
            );
        }
        for suffix in [0, 1] {
            assert!(matches!(
                decode_fields(|e| {
                    partition_fields(e, 2, 2, suffix)?;
                    e.boolean(false);
                    Ok(())
                }),
                Err(Error::Protocol(_))
            ));
        }
        assert!(matches!(
            decode_fields(|e| {
                partition_fields(e, 2, 2, 1)?;
                e.object(|e| {
                    e.i64(1);
                    Ok(())
                })
            }),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            decode_fields(|e| {
                e.i64(2);
                information(e, 1, 0, 0)
            }),
            Err(Error::Protocol(_))
        ));
        let mut e = Encoder::new();
        e.object(|e| partition_fields(e, 2, 2, 2)).unwrap();
        let bytes = e.finish();
        for end in 0..bytes.len() {
            assert!(from_slice::<Partition>(&bytes[..end]).is_err(), "{end}");
        }
    }

    #[test]
    fn partition_records_reject_unknown_structures_scalars_and_inverted_ranges() {
        for kind in [-1, 0, 3, i64::MAX] {
            assert!(decode_fields(|e| partition_fields(e, kind, 1, 2)).is_err());
        }
        for key in [-1, 0, 4, i64::MAX] {
            assert!(decode_fields(|e| partition_fields(e, 2, key, 2)).is_err());
        }
        assert!(decode_fields(|e| base(e, 2, 2, 5, -5)).is_err());
        assert!(decode_fields(|e| {
            e.i64(2);
            e.typed_object(&[1], |e| {
                e.i64(1);
                e.guid(&GUID_BYTES);
                e.string("")?;
                e.i64(0);
                e.i64(0);
                Ok(())
            })
        })
        .is_err());
        for invalid in [-1, 6, i64::MAX] {
            assert!(serde_json::from_value::<PartitionStatus>(invalid.into()).is_err());
        }
        for status in 0..=5 {
            let value: PartitionStatus = serde_json::from_value(status.into()).unwrap();
            assert_eq!(serde_json::to_value(value).unwrap(), status);
        }
        for malformed in [
            serde_json::json!({"kind": 1, "id": ID, "name": "", "low_key": 0, "high_key": 18446744073709551615u64}),
            serde_json::json!({"kind": 1, "id": vec![0u8; 16], "name": "", "low_key": 0, "high_key": 0}),
        ] {
            assert!(serde_json::from_value::<PartitionInformation>(malformed).is_err());
        }
    }

    #[test]
    fn partition_information_field_requires_untagged_objects_and_native_guid_metadata() {
        for malformed in 0..3 {
            let message = reply(
                Some(5),
                Some(|e: &mut Encoder| {
                    e.i64(1);
                    e.typed_object(if malformed == 0 { &[1] } else { &[] }, |e| {
                        e.i64(1);
                        if malformed == 1 {
                            e.byte_array(&GUID_BYTES)?;
                        } else if malformed == 2 {
                            e.string(ID)?;
                        } else {
                            e.guid(&GUID_BYTES);
                        }
                        e.string("")?;
                        e.i64(0);
                        e.i64(0);
                        Ok(())
                    })?;
                    e.i32(1);
                    e.u32(0);
                    e.u32(0);
                    e.i64(1);
                    e.i64(1);
                    Ok(())
                }),
                0,
                None,
                false,
            );
            assert!(matches!(
                decode_reply::<PartitionQueryParameters>(&message),
                Err(Error::Protocol(_))
            ));
        }
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(1);
            e.guid(&[0; 16]);
            e.string("")?;
            e.i64(0);
            e.i64(0);
            Ok(())
        })
        .unwrap();
        assert!(from_slice::<PartitionInformation>(&e.finish())
            .unwrap()
            .id
            .is_nil());
    }

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
    fn partition_reply_profile_validates_structure_before_embedded_remote_errors() {
        const REMOTE: i64 = -123;
        for message in [
            reply(
                Some(6),
                Some(|e: &mut Encoder| partition_fields(e, 2, 1, 2)),
                REMOTE,
                None,
                false,
            ),
            reply(
                Some(5),
                Some(|e: &mut Encoder| partition_fields(e, 0, 1, 2)),
                REMOTE,
                None,
                false,
            ),
            reply(
                Some(5),
                Some(|e: &mut Encoder| partition_fields(e, 2, 1, 2)),
                REMOTE,
                Some((&[1], "next")),
                false,
            ),
            reply(
                Some(5),
                Some(|e: &mut Encoder| partition_fields(e, 2, 1, 2)),
                REMOTE,
                None,
                true,
            ),
        ] {
            assert!(matches!(
                decode_reply::<PartitionQueryParameters>(&message),
                Err(Error::Protocol(_))
            ));
        }
        let mut message = reply(
            Some(5),
            Some(|e: &mut Encoder| partition_fields(e, 2, 1, 2)),
            REMOTE,
            None,
            false,
        );
        assert!(matches!(
            decode_reply::<PartitionQueryParameters>(&message),
            Err(Error::Remote { code: REMOTE, .. })
        ));
        message.headers.push(Header::actor(3).unwrap());
        assert!(matches!(
            decode_reply::<PartitionQueryParameters>(&message),
            Err(Error::Protocol(_))
        ));
        let message = reply(
            None,
            None::<fn(&mut Encoder) -> Result<()>>,
            REMOTE,
            None,
            false,
        );
        assert!(matches!(
            decode_reply::<PartitionQueryParameters>(&message),
            Err(Error::Remote { code: REMOTE, .. })
        ));
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
