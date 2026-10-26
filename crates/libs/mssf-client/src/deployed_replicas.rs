//! Owned deployed replicas and instances from the native unpaged node query.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, Query, UnpagedListReply},
    Error, NamingUri, NodeTimestamp, QueryDescription, ReplicaRole, ReplicaStatus, Result,
    ServiceKind,
};

/// Native facade selectors, in argument order. There is no paging or
/// service-package-activation selector on this facade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DeployedReplicaQueryParameters {
    /// Required, case-preserving node name: 1..=4096 UTF-16 units without NUL.
    pub node_name: String,
    pub application_name: NamingUri,
    #[serde(
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_empty"
    )]
    pub service_manifest_name: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_nil"
    )]
    pub partition_id: Option<Uuid>,
}

impl DeployedReplicaQueryParameters {
    pub fn new(node_name: impl Into<String>, application_name: NamingUri) -> Self {
        Self {
            node_name: node_name.into(),
            application_name,
            service_manifest_name: None,
            partition_id: None,
        }
    }
}

/// An unpaged, node/application-scoped deployed-replica request.
pub type DeployedReplicaQueryDescription = QueryDescription<DeployedReplicaQueryParameters>;

crate::native::strict_enum! {
    /// Native reconfiguration phase; Zero is code 2, not the default.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub enum ReconfigurationPhase {
        #[default]
        Invalid = 0,
        None = 1,
        Zero = 2,
        One = 3,
        Two = 4,
        Three = 5,
        Four = 6,
        AbortPhaseZero = 7,
    }
}

crate::native::strict_enum! {
    /// Native reconfiguration category. None is distinct from Invalid.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub enum ReconfigurationType {
        #[default]
        Invalid = 0,
        SwapPrimary = 1,
        Failover = 2,
        Other = 3,
        None = 4,
    }
}

/// Complete native reconfiguration observation, not a configuration snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconfigurationInformation {
    pub previous_configuration_role: ReplicaRole,
    pub reconfiguration_phase: ReconfigurationPhase,
    pub reconfiguration_type: ReconfigurationType,
    /// Signed 100-nanosecond ticks since 1601-01-01 UTC; zero is native default.
    pub reconfiguration_start_time_utc: NodeTimestamp,
}

impl Default for ReconfigurationInformation {
    fn default() -> Self {
        Self {
            previous_configuration_role: ReplicaRole::None,
            reconfiguration_phase: ReconfigurationPhase::Invalid,
            reconfiguration_type: ReconfigurationType::Invalid,
            reconfiguration_start_time_utc: NodeTimestamp(0),
        }
    }
}

/// Complete native positional deployed-replica result.
///
/// Stateful records use `replica_id`; stateless records use `instance_id`.
/// Native constructors zero the inactive ID; stateless role is None, not
/// Unknown. Both IDs and the role are retained exactly as received. Addresses
/// are opaque strings, and service names are native strings, not validated URIs.
/// Older records may omit the suffix beginning at `service_manifest_name`.
/// Serialization supports human-readable evidence, not native re-encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployedReplica {
    pub kind: ServiceKind,
    pub service_name: String,
    pub service_type_name: String,
    pub service_manifest_version: String,
    pub code_package_name: String,
    #[serde(deserialize_with = "crate::native::guid::deserialize")]
    pub partition_id: Uuid,
    pub replica_id: i64,
    pub instance_id: i64,
    pub replica_role: ReplicaRole,
    pub replica_status: ReplicaStatus,
    pub address: String,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub service_manifest_name: String,
    /// Native null is None; a nonnull pointer to an empty string is Some("").
    #[serde(
        default,
        deserialize_with = "crate::native::string_pointer::deserialize_trailing"
    )]
    pub service_package_activation_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub host_process_id: i64,
    #[serde(
        default,
        deserialize_with = "crate::native::untagged::deserialize_trailing"
    )]
    pub reconfiguration_information: ReconfigurationInformation,
}

impl Query for DeployedReplicaQueryParameters {
    type Output = Vec<DeployedReplica>;
    type Reply = UnpagedListReply<DeployedReplica, { result_tags::DEPLOYED_REPLICA }, 13>;
    const NAME: &'static str = "GetServiceReplicaListDeployedOnNode";
    const SUPPORTS_MAX_RESULTS: bool = false;

    fn validate_parameters(&self) -> Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed replica node name must be 1..=4096 UTF-16 units without NUL",
        )
    }

    fn validate_record(record: DeployedReplica) -> Result<DeployedReplica> {
        if record.kind == ServiceKind::Invalid {
            return Err(Error::Protocol("unsupported deployed replica service kind"));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_protocol::{encode_query, tests::assert_request};
    use mssf_transport::serialization::{
        serde::{from_slice, to_vec},
        Encoder, MAX_STRING_UNITS,
    };

    const ID: &str = "00112233-4455-6677-8899-aabbccddeeff";
    const GUID_BYTES: [u8; 16] = [
        0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];

    #[derive(Clone)]
    struct Fixture<'a> {
        kind: i64,
        replica_id: i64,
        instance_id: i64,
        role: i64,
        status: i64,
        activation: Option<&'a str>,
        process_id: i64,
        previous_role: i64,
        phase: i64,
        reconfiguration_type: i64,
        start: i64,
    }

    impl Default for Fixture<'_> {
        fn default() -> Self {
            Self {
                kind: 2,
                replica_id: i64::MIN,
                instance_id: 0,
                role: 2,
                status: 3,
                activation: Some("activation🦀"),
                process_id: i64::MAX,
                previous_role: 4,
                phase: 6,
                reconfiguration_type: 1,
                start: 133_801_234_567_890_123,
            }
        }
    }

    // Independent ServiceModel/DeployedServiceReplicaQueryResult.h
    // FABRIC_FIELDS_15, not serialization of the Rust result model.
    fn fields(e: &mut Encoder, f: &Fixture<'_>, count: usize) -> Result<()> {
        for field in 0..count {
            match field {
                0 => e.i64(f.kind),
                1 => e.string("fabric:/应用/Service")?,
                2 => e.string("ServiceType")?,
                3 => e.string("")?,
                4 => e.string("Code🦀")?,
                5 => e.guid(&GUID_BYTES),
                6 => e.i64(f.replica_id),
                7 => e.i64(f.instance_id),
                8 => e.i64(f.role),
                9 => e.i64(f.status),
                10 => e.string(r#"{"Endpoints":{"rpc":"opaque🦀\u0000"}}"#)?,
                11 => e.string("Manifest")?,
                12 => {
                    // shared_ptr<wstring> uses an untagged PointerWrapper
                    // object with one string field, not a direct string.
                    e.pointer(f.activation.map(|_| &[][..]), |e| {
                        e.string(f.activation.unwrap())
                    })?;
                }
                13 => e.i64(f.process_id),
                14 => e.object(|e| reconfiguration_fields(e, f, 4))?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    fn reconfiguration_fields(e: &mut Encoder, f: &Fixture<'_>, count: usize) -> Result<()> {
        for field in 0..count {
            e.i64(match field {
                0 => f.previous_role,
                1 => f.phase,
                2 => f.reconfiguration_type,
                3 => f.start,
                _ => unreachable!(),
            });
        }
        Ok(())
    }

    fn record(f: &Fixture<'_>, count: usize) -> Vec<u8> {
        let mut e = Encoder::new();
        e.object(|e| fields(e, f, count)).unwrap();
        e.finish()
    }

    fn decode_record(bytes: &[u8]) -> Result<DeployedReplica> {
        DeployedReplicaQueryParameters::validate_record(from_slice(bytes)?)
    }

    fn parameters() -> DeployedReplicaQueryParameters {
        DeployedReplicaQueryParameters::new("Node_1", NamingUri::parse("fabric:/App").unwrap())
    }

    #[test]
    fn deployed_replica_query_name_filters_and_nil_omission() {
        let mut p = parameters();
        let required = [("NodeName", "Node_1"), ("ApplicationName", "fabric:/App")];
        assert_request(&p, None, "GetServiceReplicaListDeployedOnNode", &required);
        p.service_manifest_name = Some(String::new());
        p.partition_id = Some(Uuid::nil());
        assert_request(&p, None, "GetServiceReplicaListDeployedOnNode", &required);
        p.service_manifest_name = Some("Manifest🦀".into());
        assert_request(
            &p,
            None,
            "GetServiceReplicaListDeployedOnNode",
            &[
                ("NodeName", "Node_1"),
                ("ApplicationName", "fabric:/App"),
                ("ServiceManifestName", "Manifest🦀"),
            ],
        );
        p.partition_id = Some(ID.parse().unwrap());
        assert_request(
            &p,
            None,
            "GetServiceReplicaListDeployedOnNode",
            &[
                ("NodeName", "Node_1"),
                ("ApplicationName", "fabric:/App"),
                ("ServiceManifestName", "Manifest🦀"),
                ("PartitionId", ID),
            ],
        );
        p.service_manifest_name = None;
        assert_request(
            &p,
            None,
            "GetServiceReplicaListDeployedOnNode",
            &[
                ("NodeName", "Node_1"),
                ("ApplicationName", "fabric:/App"),
                ("PartitionId", ID),
            ],
        );
    }

    #[test]
    fn deployed_replica_query_bounds_utf16_strings_and_rejects_only_node_nul() {
        for name in [
            String::new(),
            "bad\0node".into(),
            "a".repeat(4097),
            "🦀".repeat(2049),
        ] {
            let mut p = parameters();
            p.node_name = name;
            assert!(matches!(
                encode_query(&p, None),
                Err(Error::InvalidInput(_))
            ));
        }
        for name in ["a".repeat(4096), "🦀".repeat(2048), " 节点 ".into()] {
            let mut p = parameters();
            p.node_name = name;
            encode_query(&p, None).unwrap();
        }
        for name in [
            "a".repeat(MAX_STRING_UNITS + 1),
            "🦀".repeat(MAX_STRING_UNITS / 2 + 1),
        ] {
            let mut p = parameters();
            p.service_manifest_name = Some(name);
            assert!(matches!(
                encode_query(&p, None),
                Err(Error::InvalidInput(_))
            ));
        }
        for name in [
            "Manifest\0Suffix".into(),
            "a".repeat(MAX_STRING_UNITS),
            "🦀".repeat(MAX_STRING_UNITS / 2),
        ] {
            let mut p = parameters();
            p.service_manifest_name = Some(name);
            encode_query(&p, None).unwrap();
        }
    }

    #[test]
    fn deployed_replica_parameter_json_is_unpaged_and_uses_only_facade_selectors() {
        let mut p = parameters();
        let required = serde_json::json!({
            "NodeName": "Node_1", "ApplicationName": "fabric:/App"
        });
        assert_eq!(serde_json::to_value(&p).unwrap(), required);
        p.partition_id = Some(Uuid::nil());
        p.service_manifest_name = Some(String::new());
        assert_eq!(serde_json::to_value(&p).unwrap(), required);
        p.partition_id = Some(ID.parse().unwrap());
        p.service_manifest_name = Some("Manifest".into());
        let query = DeployedReplicaQueryDescription::new(p);
        let json = serde_json::json!({
            "parameters": {
                "NodeName": "Node_1", "ApplicationName": "fabric:/App",
                "ServiceManifestName": "Manifest", "PartitionId": ID
            }
        });
        assert_eq!(serde_json::to_value(&query).unwrap(), json);
        assert_eq!(
            serde_json::from_value::<DeployedReplicaQueryDescription>(json).unwrap(),
            query
        );
        for json in [
            serde_json::json!({"NodeName": "Node_1"}),
            serde_json::json!({"ApplicationName": "fabric:/App"}),
            serde_json::json!({"NodeName": "Node_1", "ApplicationName": "fabric:/"}),
            serde_json::json!({
                "NodeName": "Node_1", "ApplicationName": "fabric:/App", "PartitionId": "bad"
            }),
            serde_json::json!({
                "NodeName": "Node_1", "ApplicationName": "fabric:/App",
                "ServicePackageActivationId": "not-a-facade-selector"
            }),
            serde_json::json!({
                "NodeName": "Node_1", "ApplicationName": "fabric:/App", "ContinuationToken": "next"
            }),
            serde_json::json!({
                "NodeName": "Node_1", "ApplicationName": "fabric:/App", "MaxResults": 1
            }),
        ] {
            assert!(serde_json::from_value::<DeployedReplicaQueryParameters>(json).is_err());
        }
    }

    #[test]
    fn deployed_replica_complete_native_fields_windows_guid_and_native_constructor_defaults() {
        for kind in [1, 2] {
            for id in [i64::MIN, -1, 0, 1, i64::MAX] {
                let f = Fixture {
                    kind,
                    replica_id: if kind == 2 { id } else { 0 },
                    instance_id: if kind == 1 { id } else { 0 },
                    role: if kind == 2 { 2 } else { 1 },
                    process_id: id,
                    start: id,
                    ..Default::default()
                };
                let bytes = record(&f, 15);
                let r = decode_record(&bytes).unwrap();
                assert_eq!(
                    r.kind,
                    if kind == 2 {
                        ServiceKind::Stateful
                    } else {
                        ServiceKind::Stateless
                    }
                );
                assert_eq!(r.service_name, "fabric:/应用/Service");
                assert_eq!(r.service_type_name, "ServiceType");
                assert!(r.service_manifest_version.is_empty());
                assert_eq!(r.code_package_name, "Code🦀");
                assert_eq!(r.partition_id.to_string(), ID);
                let mut guid_field = vec![0x0c];
                guid_field.extend_from_slice(&GUID_BYTES);
                assert!(bytes.windows(17).any(|window| window == guid_field));
                assert_eq!(r.replica_id, f.replica_id);
                assert_eq!(r.instance_id, f.instance_id);
                assert_eq!(
                    r.replica_role,
                    if kind == 2 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::None
                    }
                );
                assert_eq!(r.replica_status, ReplicaStatus::Ready);
                assert_eq!(r.address, r#"{"Endpoints":{"rpc":"opaque🦀\u0000"}}"#);
                assert_eq!(r.service_manifest_name, "Manifest");
                assert_eq!(r.service_package_activation_id.as_deref(), f.activation);
                assert_eq!(r.host_process_id, id);
                assert_eq!(
                    r.reconfiguration_information,
                    ReconfigurationInformation {
                        previous_configuration_role: ReplicaRole::ActiveSecondary,
                        reconfiguration_phase: ReconfigurationPhase::Four,
                        reconfiguration_type: ReconfigurationType::SwapPrimary,
                        reconfiguration_start_time_utc: NodeTimestamp(id),
                    }
                );
            }
        }
        let f = Fixture {
            replica_id: i64::MAX,
            instance_id: i64::MIN,
            ..Default::default()
        };
        let r = decode_record(&record(&f, 15)).unwrap();
        assert_eq!(r.replica_id, i64::MAX);
        assert_eq!(r.instance_id, i64::MIN);
        let f = Fixture {
            kind: 1,
            replica_id: 0,
            instance_id: 1,
            role: 1,
            previous_role: 1,
            phase: 0,
            reconfiguration_type: 0,
            start: 0,
            ..Default::default()
        };
        let r = decode_record(&record(&f, 15)).unwrap();
        assert_eq!(
            r.reconfiguration_information,
            ReconfigurationInformation::default()
        );
        let mut e = Encoder::new();
        e.object(|e| {
            fields(e, &f, 5)?;
            e.guid(&[0; 16]);
            e.i64(0);
            e.i64(0);
            e.i64(1);
            e.i64(0);
            e.string("")
        })
        .unwrap();
        assert!(decode_record(&e.finish()).unwrap().partition_id.is_nil());
    }

    #[test]
    fn deployed_replica_activation_pointer_preserves_null_empty_and_value() {
        for activation in [None, Some(""), Some("opaque\0activation🦀")] {
            let f = Fixture {
                activation,
                ..Default::default()
            };
            assert_eq!(
                decode_record(&record(&f, 15))
                    .unwrap()
                    .service_package_activation_id
                    .as_deref(),
                activation
            );
        }
        for tag in [&[1][..], &12_i32.to_le_bytes()[..]] {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, &Fixture::default(), 12)?;
                e.pointer(Some(tag), |e| e.string("activation"))
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        for bad in 0..3 {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, &Fixture::default(), 12)?;
                match bad {
                    0 => e.string("not-a-pointer"),
                    1 => e.pointer(Some(&[]), |_| Ok(())),
                    2 => e.pointer(Some(&[]), |e| {
                        e.i64(1);
                        Ok(())
                    }),
                    _ => unreachable!(),
                }
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn deployed_replica_compatible_suffixes_default_only_at_actual_scope_end() {
        let f = Fixture::default();
        for count in 0..11 {
            assert!(decode_record(&record(&f, count)).is_err(), "{count}");
        }
        for count in 11..=15 {
            let r = decode_record(&record(&f, count)).unwrap();
            assert_eq!(
                r.service_manifest_name,
                if count > 11 { "Manifest" } else { "" }
            );
            assert_eq!(
                r.service_package_activation_id.as_deref(),
                if count > 12 { f.activation } else { None }
            );
            assert_eq!(r.host_process_id, if count > 13 { i64::MAX } else { 0 });
            if count < 15 {
                assert_eq!(
                    r.reconfiguration_information,
                    ReconfigurationInformation::default()
                );
            }
        }
        for count in 11..15 {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, &f, count)?;
                e.boolean(false);
                Ok(())
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        for count in 0..4 {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, &f, 14)?;
                e.object(|e| reconfiguration_fields(e, &f, count))
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        for count in 0..4 {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, &f, 14)?;
                e.object(|e| {
                    reconfiguration_fields(e, &f, count)?;
                    e.string("bad reconfiguration scalar")
                })
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        let mut e = Encoder::new();
        e.object(|e| {
            fields(e, &f, 14)?;
            e.typed_object(&[1], |e| reconfiguration_fields(e, &f, 4))
        })
        .unwrap();
        assert!(matches!(
            decode_record(&e.finish()),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn deployed_replica_enums_use_explicit_native_numeric_values_and_defaults() {
        for phase in 0..=7 {
            let f = Fixture {
                phase,
                ..Default::default()
            };
            let r = decode_record(&record(&f, 15)).unwrap();
            assert_eq!(
                serde_json::to_value(r.reconfiguration_information.reconfiguration_phase).unwrap(),
                phase
            );
        }
        let abort = decode_record(&record(
            &Fixture {
                phase: 7,
                ..Default::default()
            },
            15,
        ))
        .unwrap();
        assert_eq!(
            abort.reconfiguration_information.reconfiguration_phase,
            ReconfigurationPhase::AbortPhaseZero,
        );
        for reconfiguration_type in 0..=4 {
            let f = Fixture {
                reconfiguration_type,
                ..Default::default()
            };
            let r = decode_record(&record(&f, 15)).unwrap();
            assert_eq!(
                serde_json::to_value(r.reconfiguration_information.reconfiguration_type).unwrap(),
                reconfiguration_type
            );
        }
        for role in 0..=4 {
            let f = Fixture {
                role,
                previous_role: role,
                ..Default::default()
            };
            let r = decode_record(&record(&f, 15)).unwrap();
            assert_eq!(serde_json::to_value(r.replica_role).unwrap(), role);
            assert_eq!(
                serde_json::to_value(r.reconfiguration_information.previous_configuration_role)
                    .unwrap(),
                role
            );
        }
        for status in 0..=5 {
            let f = Fixture {
                status,
                ..Default::default()
            };
            assert_eq!(
                serde_json::to_value(decode_record(&record(&f, 15)).unwrap().replica_status)
                    .unwrap(),
                status
            );
        }
        for kind in [-1, 0, 3, i64::MAX] {
            assert!(decode_record(&record(
                &Fixture {
                    kind,
                    ..Default::default()
                },
                15
            ))
            .is_err());
        }
        for (field, invalids) in [
            (0, [-1, 5, i64::MAX]),
            (1, [-1, 6, i64::MAX]),
            (2, [-1, 5, i64::MAX]),
            (3, [-1, 8, i64::MAX]),
            (4, [-1, 5, i64::MAX]),
        ] {
            for invalid in invalids {
                let mut f = Fixture::default();
                match field {
                    0 => f.role = invalid,
                    1 => f.status = invalid,
                    2 => f.previous_role = invalid,
                    3 => f.phase = invalid,
                    4 => f.reconfiguration_type = invalid,
                    _ => unreachable!(),
                }
                assert!(matches!(
                    decode_record(&record(&f, 15)),
                    Err(Error::Protocol(_))
                ));
            }
        }
        assert_eq!(
            ReconfigurationPhase::default(),
            ReconfigurationPhase::Invalid
        );
        assert_eq!(ReconfigurationType::default(), ReconfigurationType::Invalid);
        assert_eq!(
            ReconfigurationInformation::default(),
            ReconfigurationInformation {
                previous_configuration_role: ReplicaRole::None,
                reconfiguration_phase: ReconfigurationPhase::Invalid,
                reconfiguration_type: ReconfigurationType::Invalid,
                reconfiguration_start_time_utc: NodeTimestamp(0),
            }
        );
        for invalid in [
            serde_json::json!(-1),
            serde_json::json!(8),
            serde_json::json!("Four"),
        ] {
            assert!(serde_json::from_value::<ReconfigurationPhase>(invalid).is_err());
        }
        for invalid in [
            serde_json::json!(-1),
            serde_json::json!(5),
            serde_json::json!("None"),
        ] {
            assert!(serde_json::from_value::<ReconfigurationType>(invalid).is_err());
        }
    }

    #[test]
    fn deployed_replica_rejects_wrong_guid_metadata_and_unterminated_timestamp() {
        let f = Fixture::default();
        for wrong in 0..2 {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, &f, 5)?;
                if wrong == 0 {
                    e.byte_array(&GUID_BYTES)
                } else {
                    e.string(ID)
                }
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        let bytes = record(&f, 15);
        let mut bytes = bytes;
        let last_timestamp_byte = bytes.len() - 5;
        bytes[last_timestamp_byte] |= 0x80;
        assert!(decode_record(&bytes).is_err());
    }

    #[test]
    fn deployed_replica_record_json_preserves_signed_ids_nullable_activation_and_full_reconfiguration(
    ) {
        let json = serde_json::json!({
            "kind": 2,
            "service_name": "opaque native service name",
            "service_type_name": "Type",
            "service_manifest_version": "",
            "code_package_name": "Code",
            "partition_id": ID,
            "replica_id": i64::MIN,
            "instance_id": i64::MAX,
            "replica_role": 2,
            "replica_status": 3,
            "address": "opaque\0address",
            "service_manifest_name": "Manifest",
            "service_package_activation_id": "",
            "host_process_id": i64::MIN,
            "reconfiguration_information": {
                "previous_configuration_role": 1,
                "reconfiguration_phase": 0,
                "reconfiguration_type": 4,
                "reconfiguration_start_time_utc": i64::MAX
            }
        });
        let r: DeployedReplica = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(r.replica_id, i64::MIN);
        assert_eq!(r.instance_id, i64::MAX);
        assert_eq!(r.host_process_id, i64::MIN);
        assert_eq!(r.service_package_activation_id.as_deref(), Some(""));
        assert_eq!(r.partition_id.to_string(), ID);
        assert_eq!(r.address, "opaque\0address");
        assert_eq!(serde_json::to_value(&r).unwrap(), json);
        assert!(to_vec(&r).is_err());
        assert_eq!(
            serde_json::to_value(&r.reconfiguration_information).unwrap(),
            json["reconfiguration_information"]
        );
        let mut nullable = json.clone();
        nullable["service_package_activation_id"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<DeployedReplica>(nullable)
            .unwrap()
            .service_package_activation_id
            .is_none());
        let mut missing = json.clone();
        for suffix in [
            "service_manifest_name",
            "service_package_activation_id",
            "host_process_id",
            "reconfiguration_information",
        ] {
            missing.as_object_mut().unwrap().remove(suffix);
        }
        let r: DeployedReplica = serde_json::from_value(missing).unwrap();
        assert_eq!(
            r.reconfiguration_information,
            ReconfigurationInformation::default()
        );
        assert_eq!(r.host_process_id, 0);
        assert!(r.service_manifest_name.is_empty());
        assert!(r.service_package_activation_id.is_none());
        for (field, value) in [
            ("partition_id", serde_json::json!(vec![0; 16])),
            ("replica_id", serde_json::json!(u64::MAX)),
            ("instance_id", serde_json::json!(u64::MAX)),
            ("host_process_id", serde_json::Value::Null),
            (
                "service_package_activation_id",
                serde_json::json!({"value": ""}),
            ),
            ("reconfiguration_information", serde_json::Value::Null),
            ("reconfiguration_information", serde_json::json!({})),
            ("replica_role", serde_json::json!("Primary")),
            ("replica_status", serde_json::json!(6)),
        ] {
            let mut malformed = json.clone();
            malformed[field] = value;
            assert!(
                serde_json::from_value::<DeployedReplica>(malformed).is_err(),
                "{field}"
            );
        }
    }
}
