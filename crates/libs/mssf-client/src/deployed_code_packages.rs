//! Owned native node/application-scoped code packages and entry-point observations.

use serde::{Deserialize, Serialize};

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, Query, UnpagedListReply},
    DeploymentStatus, NamingUri, NodeTimestamp, QueryDescription, Result,
};

/// Native unpaged code-package parameters in facade argument order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DeployedCodePackageQueryParameters {
    /// Required, case-preserving node name: 1..=4096 UTF-16 units without NUL.
    pub node_name: String,
    pub application_name: NamingUri,
    /// None and empty strings omit this native filter.
    #[serde(
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_empty"
    )]
    pub service_manifest_name: Option<String>,
    /// None and empty strings omit this native filter.
    #[serde(
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_empty"
    )]
    pub code_package_name: Option<String>,
}

impl DeployedCodePackageQueryParameters {
    pub fn new(node_name: impl Into<String>, application_name: NamingUri) -> Self {
        Self {
            node_name: node_name.into(),
            application_name,
            service_manifest_name: None,
            code_package_name: None,
        }
    }
}

/// One native list request, without paging or automatic retries.
pub type DeployedCodePackageQueryDescription = QueryDescription<DeployedCodePackageQueryParameters>;

crate::native::strict_enum! {
    /// Native entry-point lifecycle status, not deployment status.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum EntryPointStatus {
        Invalid = 0,
        Pending = 1,
        Starting = 2,
        Started = 3,
        Stopping = 4,
        Stopped = 5,
    }
}

crate::native::strict_enum! {
    /// Native code-package host kind. Invalid is the absent native suffix default.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub enum HostType {
        #[default]
        Invalid = 0,
        ExeHost = 1,
        ContainerHost = 2,
    }
}

crate::native::strict_enum! {
    /// Native host isolation. None is the absent native suffix default.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub enum HostIsolationMode {
        #[default]
        None = 0,
        Process = 1,
        HyperV = 2,
    }
}

/// Native entry-point counters and timestamps, in `FABRIC_FIELDS_11` order.
///
/// Activation and exit timestamps follow the native binary fields, not the
/// swapped labels in the native statistics JSON property declarations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodePackageEntryPointStatistics {
    pub last_exit_code: u32,
    pub last_activation_time: NodeTimestamp,
    pub last_exit_time: NodeTimestamp,
    pub last_successful_activation_time: NodeTimestamp,
    pub last_successful_exit_time: NodeTimestamp,
    pub activation_failure_count: u32,
    pub continuous_activation_failure_count: u32,
    pub exit_failure_count: u32,
    pub continuous_exit_failure_count: u32,
    pub activation_count: u32,
    pub exit_count: u32,
}

/// An owned native entry point. Locations and user names are not normalized.
///
/// Process and instance IDs retain their signed native storage, including
/// negative sentinel values. An absent older instance-ID suffix defaults to 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodePackageEntryPoint {
    pub entry_point_location: String,
    pub process_id: i64,
    pub run_as_user_name: String,
    pub next_activation_time: NodeTimestamp,
    pub status: EntryPointStatus,
    #[serde(deserialize_with = "crate::native::untagged::deserialize")]
    pub statistics: CodePackageEntryPointStatistics,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub instance_id: i64,
}

/// A native deployed-code-package observation, not an atomic deployment snapshot.
///
/// Both entry-point objects are always present on the wire, even when
/// `has_setup_entry_point` is false. Activation/host/node/internal-service
/// suffixes absent in older records use their native constructor defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployedCodePackage {
    pub name: String,
    pub version: String,
    pub service_manifest_name: String,
    pub status: DeploymentStatus,
    /// Native run interval in seconds.
    pub run_frequency_interval: u32,
    #[serde(deserialize_with = "crate::native::untagged::deserialize")]
    pub setup_entry_point: CodePackageEntryPoint,
    #[serde(deserialize_with = "crate::native::untagged::deserialize")]
    pub main_entry_point: CodePackageEntryPoint,
    pub has_setup_entry_point: bool,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub service_package_activation_id: String,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub host_type: HostType,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub host_isolation_mode: HostIsolationMode,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub node_name: String,
    /// Native internal-use service name, retained even though native REST omits it.
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub service_name_internal_use_only: String,
}

impl Query for DeployedCodePackageQueryParameters {
    type Output = Vec<DeployedCodePackage>;
    type Reply = UnpagedListReply<DeployedCodePackage, { result_tags::DEPLOYED_CODE_PACKAGE }>;
    const NAME: &'static str = "GetCodePackageListDeployedOnNode";

    fn validate_parameters(&self) -> Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed code package node name must be 1..=4096 UTF-16 units without NUL",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        query_protocol::{decode_reply, encode_query},
        Error,
    };
    use mssf_transport::{
        message::{Header, Message},
        serialization::{
            serde::{from_slice, to_vec},
            Encoder, MAX_STRING_UNITS,
        },
    };

    #[derive(Clone, Copy)]
    struct Wire {
        package_fields: usize,
        entry_fields: usize,
        statistics_fields: usize,
        deployment_status: i64,
        entry_status: i64,
        host_type: i64,
        host_isolation_mode: i64,
        has_setup: bool,
    }

    impl Default for Wire {
        fn default() -> Self {
            Self {
                package_fields: 13,
                entry_fields: 7,
                statistics_fields: 11,
                deployment_status: 3,
                entry_status: 3,
                host_type: 2,
                host_isolation_mode: 2,
                has_setup: true,
            }
        }
    }

    // ServiceModel/CodePackageEntryPointStatistics.h FABRIC_FIELDS_11.
    fn statistics_fields(e: &mut Encoder, wire: Wire) -> Result<()> {
        for field in 0..wire.statistics_fields {
            match field {
                0 => e.u32(u32::MAX),
                1 => e.i64(-101),
                2 => e.i64(202),
                3 => e.i64(303),
                4 => e.i64(404),
                5 => e.u32(5),
                6 => e.u32(6),
                7 => e.u32(7),
                8 => e.u32(8),
                9 => e.u32(9),
                10 => e.u32(10),
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    // Both entry points are embedded objects, not pointers or nullable Options.
    fn entry_fields(e: &mut Encoder, wire: Wire, setup: bool) -> Result<()> {
        for field in 0..wire.entry_fields {
            match field {
                0 => e.string(if setup {
                    r"C:\节点\..\setup.exe"
                } else {
                    "/node/应用/../main\0opaque"
                })?,
                1 => e.i64(if setup { -1 } else { i64::MIN }),
                2 => e.string(if setup {
                    r"DOMAIN\setup"
                } else {
                    "应用用户"
                })?,
                3 => e.i64(if setup { 0 } else { i64::MAX }),
                4 => e.i64(wire.entry_status),
                5 => e.object(|e| statistics_fields(e, wire))?,
                6 => e.i64(if setup { 0 } else { -123 }),
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    // ServiceModel/DeployedCodePackageQueryResult.h FABRIC_FIELDS_13.
    fn package_fields(e: &mut Encoder, wire: Wire) -> Result<()> {
        for field in 0..wire.package_fields {
            match field {
                0 => e.string("Code_应用")?,
                1 => e.string("v1:opaque")?,
                2 => e.string("Manifest")?,
                3 => e.i64(wire.deployment_status),
                4 => e.u32(u32::MAX),
                5 => e.object(|e| entry_fields(e, wire, true))?,
                6 => e.object(|e| entry_fields(e, wire, false))?,
                7 => e.boolean(wire.has_setup),
                8 => e.string("Activation:Opaque/应用")?,
                9 => e.i64(wire.host_type),
                10 => e.i64(wire.host_isolation_mode),
                11 => e.string(" Node_Ab ")?,
                12 => e.string("fabric:/App/Service?opaque")?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    fn record(wire: Wire) -> Vec<u8> {
        let mut e = Encoder::new();
        e.object(|e| package_fields(e, wire)).unwrap();
        e.finish()
    }

    fn reply(
        kind: i64,
        tag: Option<i32>,
        records: &[Wire],
        error: i64,
        paging: Option<(&[u8], &str)>,
    ) -> Message {
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(kind);
            let tag = tag.map(i32::to_le_bytes);
            e.pointer(tag.as_ref().map(|tag| tag.as_slice()), |e| {
                e.object_array(records, |e, wire| package_fields(e, *wire))?;
                e.pointer(None, |_| Ok(()))
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

    fn parameters() -> DeployedCodePackageQueryParameters {
        DeployedCodePackageQueryParameters::new(
            " Node_Ab ",
            NamingUri::parse("fabric:/App").unwrap(),
        )
    }

    fn assert_request(parameters: &DeployedCodePackageQueryParameters, arguments: &[(&str, &str)]) {
        let mut e = Encoder::new();
        e.object(|e| {
            e.object(|e| {
                e.object_array(arguments, |e, (key, value)| {
                    e.object(|e| e.string(key))?;
                    e.string(value)
                })
            })?;
            e.string("GetCodePackageListDeployedOnNode")
        })
        .unwrap();
        assert_eq!(encode_query(parameters, None).unwrap(), e.finish());
    }

    #[test]
    fn code_query_exact_native_name_argument_order_and_empty_filter_omission() {
        let mut parameters = parameters();
        let required = [
            ("NodeName", " Node_Ab "),
            ("ApplicationName", "fabric:/App"),
        ];
        assert_request(&parameters, &required);
        parameters.service_manifest_name = Some(String::new());
        parameters.code_package_name = Some(String::new());
        assert_request(&parameters, &required);
        parameters.code_package_name = Some("CodeCase/应用".into());
        assert_request(
            &parameters,
            &[
                required[0],
                required[1],
                ("CodePackageName", "CodeCase/应用"),
            ],
        );
        parameters.service_manifest_name = Some(" ManifestCase ".into());
        assert_request(
            &parameters,
            &[
                required[0],
                required[1],
                ("ServiceManifestName", " ManifestCase "),
                ("CodePackageName", "CodeCase/应用"),
            ],
        );
        parameters.code_package_name = None;
        assert_request(
            &parameters,
            &[
                required[0],
                required[1],
                ("ServiceManifestName", " ManifestCase "),
            ],
        );
    }

    #[test]
    fn code_query_bounded_node_and_filter_validation() {
        for name in [
            String::new(),
            "bad\0node".into(),
            "x".repeat(4097),
            "😀".repeat(2049),
            "😀".repeat(2048) + "x",
        ] {
            let mut parameters = parameters();
            parameters.node_name = name;
            assert!(matches!(
                encode_query(&parameters, None),
                Err(Error::InvalidInput(_))
            ));
        }
        for name in [
            "x".repeat(4096),
            "界".repeat(4096),
            "😀".repeat(2048),
            "😀".repeat(2047) + "ab",
        ] {
            let mut parameters = parameters();
            parameters.node_name = name;
            encode_query(&parameters, None).unwrap();
        }
        for service_manifest in [false, true] {
            let mut parameters = parameters();
            let filter = if service_manifest {
                &mut parameters.service_manifest_name
            } else {
                &mut parameters.code_package_name
            };
            *filter = Some("x".repeat(MAX_STRING_UNITS + 1));
            assert!(matches!(
                encode_query(&parameters, None),
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[test]
    fn code_record_complete_native_layout_signed_ids_timestamps_and_statistics() {
        let bytes = record(Wire::default());
        let package: DeployedCodePackage = from_slice(&bytes).unwrap();
        assert_eq!(package.name, "Code_应用");
        assert_eq!(package.version, "v1:opaque");
        assert_eq!(package.service_manifest_name, "Manifest");
        assert_eq!(package.status, DeploymentStatus::Active);
        assert_eq!(package.run_frequency_interval, u32::MAX);
        assert!(package.has_setup_entry_point);
        assert_eq!(
            package.service_package_activation_id,
            "Activation:Opaque/应用"
        );
        assert_eq!(package.host_type, HostType::ContainerHost);
        assert_eq!(package.host_isolation_mode, HostIsolationMode::HyperV);
        assert_eq!(package.node_name, " Node_Ab ");
        assert_eq!(
            package.service_name_internal_use_only,
            "fabric:/App/Service?opaque"
        );
        let setup = &package.setup_entry_point;
        assert_eq!(setup.entry_point_location, r"C:\节点\..\setup.exe");
        assert_eq!(setup.process_id, -1);
        assert_eq!(setup.run_as_user_name, r"DOMAIN\setup");
        assert_eq!(setup.next_activation_time, NodeTimestamp(0));
        assert_eq!(setup.instance_id, 0);
        let main = &package.main_entry_point;
        assert_eq!(main.entry_point_location, "/node/应用/../main\0opaque");
        assert_eq!(main.process_id, i64::MIN);
        assert_eq!(main.run_as_user_name, "应用用户");
        assert_eq!(main.next_activation_time, NodeTimestamp(i64::MAX));
        assert_eq!(main.status, EntryPointStatus::Started);
        assert_eq!(main.instance_id, -123);
        assert_eq!(
            main.statistics,
            CodePackageEntryPointStatistics {
                last_exit_code: u32::MAX,
                last_activation_time: NodeTimestamp(-101),
                last_exit_time: NodeTimestamp(202),
                last_successful_activation_time: NodeTimestamp(303),
                last_successful_exit_time: NodeTimestamp(404),
                activation_failure_count: 5,
                continuous_activation_failure_count: 6,
                exit_failure_count: 7,
                continuous_exit_failure_count: 8,
                activation_count: 9,
                exit_count: 10,
            }
        );
        assert_eq!(setup.statistics, main.statistics);
        assert_eq!(to_vec(&package).unwrap(), bytes);
        let package: DeployedCodePackage = from_slice(&record(Wire {
            has_setup: false,
            ..Wire::default()
        }))
        .unwrap();
        assert!(!package.has_setup_entry_point);
        assert_eq!(package.setup_entry_point.process_id, -1);
    }

    #[test]
    fn code_record_required_prefixes_and_true_native_suffix_defaults() {
        for package_fields in 0..8 {
            assert!(from_slice::<DeployedCodePackage>(&record(Wire {
                package_fields,
                ..Wire::default()
            }))
            .is_err());
        }
        for package_fields in 8..=13 {
            let package: DeployedCodePackage = from_slice(&record(Wire {
                package_fields,
                ..Wire::default()
            }))
            .unwrap();
            assert_eq!(
                package.service_package_activation_id,
                if package_fields > 8 {
                    "Activation:Opaque/应用"
                } else {
                    ""
                }
            );
            assert_eq!(
                package.host_type,
                if package_fields > 9 {
                    HostType::ContainerHost
                } else {
                    HostType::Invalid
                }
            );
            assert_eq!(
                package.host_isolation_mode,
                if package_fields > 10 {
                    HostIsolationMode::HyperV
                } else {
                    HostIsolationMode::None
                }
            );
            assert_eq!(
                package.node_name,
                if package_fields > 11 { " Node_Ab " } else { "" }
            );
            assert_eq!(
                package.service_name_internal_use_only,
                if package_fields > 12 {
                    "fabric:/App/Service?opaque"
                } else {
                    ""
                }
            );
        }
        for entry_fields in 0..6 {
            assert!(from_slice::<DeployedCodePackage>(&record(Wire {
                entry_fields,
                ..Wire::default()
            }))
            .is_err());
        }
        let old: DeployedCodePackage = from_slice(&record(Wire {
            entry_fields: 6,
            ..Wire::default()
        }))
        .unwrap();
        assert_eq!(old.main_entry_point.instance_id, 0);
        assert_eq!(old.setup_entry_point.instance_id, 0);
        for statistics_fields in 0..11 {
            assert!(from_slice::<DeployedCodePackage>(&record(Wire {
                statistics_fields,
                ..Wire::default()
            }))
            .is_err());
        }
    }

    #[test]
    fn code_record_rejects_present_malformed_suffixes_and_all_truncations() {
        for count in 8..13 {
            let mut e = Encoder::new();
            e.object(|e| {
                package_fields(
                    e,
                    Wire {
                        package_fields: count,
                        ..Wire::default()
                    },
                )?;
                e.boolean(false);
                Ok(())
            })
            .unwrap();
            assert!(matches!(
                from_slice::<DeployedCodePackage>(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        let mut e = Encoder::new();
        e.object(|e| {
            entry_fields(
                e,
                Wire {
                    entry_fields: 6,
                    ..Wire::default()
                },
                false,
            )?;
            e.u64(123);
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            from_slice::<CodePackageEntryPoint>(&e.finish()),
            Err(Error::Protocol(_))
        ));
        let bytes = record(Wire::default());
        for end in 0..bytes.len() {
            assert!(from_slice::<DeployedCodePackage>(&bytes[..end]).is_err());
        }
    }

    #[test]
    fn code_record_embedded_objects_require_native_shape_and_empty_tags() {
        for pointer in [false, true] {
            let mut e = Encoder::new();
            e.object(|e| {
                package_fields(
                    e,
                    Wire {
                        package_fields: 5,
                        ..Wire::default()
                    },
                )?;
                if pointer {
                    e.pointer(None, |_| Ok(()))
                } else {
                    e.typed_object(&[1], |e| entry_fields(e, Wire::default(), true))
                }
            })
            .unwrap();
            assert!(matches!(
                from_slice::<DeployedCodePackage>(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        let mut e = Encoder::new();
        e.object(|e| {
            entry_fields(
                e,
                Wire {
                    entry_fields: 5,
                    ..Wire::default()
                },
                false,
            )?;
            e.typed_object(&[1], |e| statistics_fields(e, Wire::default()))
        })
        .unwrap();
        assert!(matches!(
            from_slice::<CodePackageEntryPoint>(&e.finish()),
            Err(Error::Protocol(_))
        ));
        let mut e = Encoder::new();
        e.object(|e| {
            package_fields(
                e,
                Wire {
                    package_fields: 4,
                    ..Wire::default()
                },
            )?;
            e.i64(-1);
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            from_slice::<DeployedCodePackage>(&e.finish()),
            Err(Error::Protocol(_))
        ));
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(-1);
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            from_slice::<CodePackageEntryPointStatistics>(&e.finish()),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn code_record_supported_future_tails_are_skipped_without_weakening_truncation() {
        let mut e = Encoder::new();
        e.object(|e| {
            package_fields(e, Wire::default())?;
            e.string("future:opaque")
        })
        .unwrap();
        let bytes = e.finish();
        let package: DeployedCodePackage = from_slice(&bytes).unwrap();
        assert_eq!(package.name, "Code_应用");
        for end in 0..bytes.len() {
            assert!(from_slice::<DeployedCodePackage>(&bytes[..end]).is_err());
        }
        let mut e = Encoder::new();
        e.object(|e| {
            entry_fields(e, Wire::default(), false)?;
            e.string("future:entry")
        })
        .unwrap();
        let entry: CodePackageEntryPoint = from_slice(&e.finish()).unwrap();
        assert_eq!(entry.instance_id, -123);
        let mut e = Encoder::new();
        e.object(|e| {
            statistics_fields(e, Wire::default())?;
            e.boolean(true);
            Ok(())
        })
        .unwrap();
        let statistics: CodePackageEntryPointStatistics = from_slice(&e.finish()).unwrap();
        assert_eq!(statistics.exit_count, 10);
    }

    #[test]
    fn code_record_all_numeric_enum_codes_and_unknown_rejection() {
        for (deployment_status, expected) in [
            (0, DeploymentStatus::Invalid),
            (1, DeploymentStatus::Downloading),
            (2, DeploymentStatus::Activating),
            (3, DeploymentStatus::Active),
            (4, DeploymentStatus::Upgrading),
            (5, DeploymentStatus::Deactivating),
        ] {
            let package: DeployedCodePackage = from_slice(&record(Wire {
                deployment_status,
                ..Wire::default()
            }))
            .unwrap();
            assert_eq!(package.status, expected);
        }
        for (entry_status, expected) in [
            (0, EntryPointStatus::Invalid),
            (1, EntryPointStatus::Pending),
            (2, EntryPointStatus::Starting),
            (3, EntryPointStatus::Started),
            (4, EntryPointStatus::Stopping),
            (5, EntryPointStatus::Stopped),
        ] {
            let package: DeployedCodePackage = from_slice(&record(Wire {
                entry_status,
                ..Wire::default()
            }))
            .unwrap();
            assert_eq!(package.main_entry_point.status, expected);
            assert_eq!(package.setup_entry_point.status, expected);
        }
        for (host_type, expected) in [
            (0, HostType::Invalid),
            (1, HostType::ExeHost),
            (2, HostType::ContainerHost),
        ] {
            let package: DeployedCodePackage = from_slice(&record(Wire {
                host_type,
                ..Wire::default()
            }))
            .unwrap();
            assert_eq!(package.host_type, expected);
        }
        for (host_isolation_mode, expected) in [
            (0, HostIsolationMode::None),
            (1, HostIsolationMode::Process),
            (2, HostIsolationMode::HyperV),
        ] {
            let package: DeployedCodePackage = from_slice(&record(Wire {
                host_isolation_mode,
                ..Wire::default()
            }))
            .unwrap();
            assert_eq!(package.host_isolation_mode, expected);
        }
        for value in [-1, 6, i64::MAX] {
            for wire in [
                Wire {
                    deployment_status: value,
                    ..Wire::default()
                },
                Wire {
                    entry_status: value,
                    ..Wire::default()
                },
                Wire {
                    host_type: value,
                    ..Wire::default()
                },
                Wire {
                    host_isolation_mode: value,
                    ..Wire::default()
                },
            ] {
                assert!(matches!(
                    from_slice::<DeployedCodePackage>(&record(wire)),
                    Err(Error::Protocol(_))
                ));
            }
        }
        for wire in [
            Wire {
                host_type: 3,
                ..Wire::default()
            },
            Wire {
                host_isolation_mode: 3,
                ..Wire::default()
            },
        ] {
            assert!(from_slice::<DeployedCodePackage>(&record(wire)).is_err());
        }
    }

    #[test]
    fn code_list_exact_tag_empty_list_unpaged_success_and_envelope_validation() {
        for records in [vec![], vec![Wire::default()], vec![Wire::default(); 2]] {
            let packages = decode_reply::<DeployedCodePackageQueryParameters>(&reply(
                2,
                Some(14),
                &records,
                0,
                None,
            ))
            .unwrap();
            assert_eq!(packages.len(), records.len());
        }
        for message in [
            reply(1, Some(14), &[Wire::default()], 0, None),
            reply(99, Some(14), &[Wire::default()], 0, None),
            reply(2, Some(11), &[Wire::default()], 0, None),
            reply(2, None, &[], 0, None),
            reply(2, Some(14), &[Wire::default()], 0, Some((&[], "next"))),
            reply(2, Some(14), &[Wire::default()], 0, Some((&[], ""))),
            reply(2, Some(14), &[Wire::default()], 0, Some((&[1], ""))),
        ] {
            assert!(matches!(
                decode_reply::<DeployedCodePackageQueryParameters>(&message),
                Err(Error::Protocol(_))
            ));
        }
        let mut message = reply(2, Some(14), &[Wire::default()], 0, None);
        message.headers = vec![
            Header::actor(5).unwrap(),
            Header::action("QueryReply").unwrap(),
        ];
        decode_reply::<DeployedCodePackageQueryParameters>(&message).unwrap();
        message.headers[0] = Header::actor(1).unwrap();
        assert!(matches!(
            decode_reply::<DeployedCodePackageQueryParameters>(&message),
            Err(Error::Protocol(_))
        ));
        message.headers[0] = Header::actor(5).unwrap();
        message.headers[1] = Header::action("OtherAction").unwrap();
        assert!(matches!(
            decode_reply::<DeployedCodePackageQueryParameters>(&message),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn code_list_native_errors_do_not_hide_malformed_typed_payloads() {
        for message in [
            reply(2, Some(14), &[Wire::default()], 123, None),
            reply(0, None, &[], 123, None),
        ] {
            assert!(matches!(
                decode_reply::<DeployedCodePackageQueryParameters>(&message),
                Err(Error::Remote { code: 123, .. })
            ));
        }
        for message in [
            reply(2, Some(11), &[Wire::default()], 123, None),
            reply(2, Some(14), &[Wire::default()], 123, Some((&[], ""))),
            reply(
                2,
                Some(14),
                &[Wire {
                    package_fields: 7,
                    ..Wire::default()
                }],
                123,
                None,
            ),
            reply(
                2,
                Some(14),
                &[Wire {
                    entry_status: 99,
                    ..Wire::default()
                }],
                123,
                None,
            ),
            reply(
                2,
                Some(14),
                &[Wire {
                    host_type: 99,
                    ..Wire::default()
                }],
                123,
                None,
            ),
            reply(
                2,
                Some(14),
                &[Wire {
                    host_isolation_mode: 99,
                    ..Wire::default()
                }],
                123,
                None,
            ),
        ] {
            assert!(matches!(
                decode_reply::<DeployedCodePackageQueryParameters>(&message),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn code_public_json_nested_parameters_without_paging_and_numeric_records() {
        let mut request = parameters();
        request.service_manifest_name = Some("Manifest".into());
        request.code_package_name = Some("Code".into());
        let query = DeployedCodePackageQueryDescription::new(request);
        let json = serde_json::to_value(&query).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "parameters": {
                    "NodeName": " Node_Ab ",
                    "ApplicationName": "fabric:/App",
                    "ServiceManifestName": "Manifest",
                    "CodePackageName": "Code"
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<DeployedCodePackageQueryDescription>(json).unwrap(),
            query
        );
        let mut empty = parameters();
        empty.service_manifest_name = Some(String::new());
        empty.code_package_name = Some(String::new());
        assert_eq!(
            serde_json::to_value(&empty).unwrap(),
            serde_json::json!({"NodeName":" Node_Ab ","ApplicationName":"fabric:/App"})
        );
        assert_eq!(
            serde_json::from_str::<DeployedCodePackageQueryParameters>(
                r#"{"NodeName":" Node_Ab ","ApplicationName":"fabric:/App"}"#
            )
            .unwrap(),
            parameters()
        );
        for json in [
            r#"{}"#,
            r#"{"NodeName":"Node"}"#,
            r#"{"ApplicationName":"fabric:/App"}"#,
            r#"{"NodeName":"Node","ApplicationName":"fabric:/"}"#,
            r#"{"NodeName":"Node","ApplicationName":"fabric:/App","Typo":1}"#,
            r#"{"NodeName":"Node","ApplicationName":"fabric:/App","CodePackageName":1}"#,
        ] {
            assert!(serde_json::from_str::<DeployedCodePackageQueryParameters>(json).is_err());
        }
        let package: DeployedCodePackage = from_slice(&record(Wire::default())).unwrap();
        let json = serde_json::to_value(&package).unwrap();
        assert_eq!(json["status"], 3);
        assert_eq!(json["host_type"], 2);
        assert_eq!(json["host_isolation_mode"], 2);
        assert_eq!(json["main_entry_point"]["status"], 3);
        assert_eq!(json["main_entry_point"]["instance_id"], -123);
        assert_eq!(
            json["main_entry_point"]["statistics"]["last_activation_time"],
            -101
        );
        assert_eq!(
            json["main_entry_point"]["statistics"]["last_exit_time"],
            202
        );
        assert_eq!(
            serde_json::from_value::<DeployedCodePackage>(json.clone()).unwrap(),
            package
        );
        for field in [
            "host_type",
            "host_isolation_mode",
            "service_package_activation_id",
            "node_name",
            "service_name_internal_use_only",
        ] {
            for malformed in [serde_json::Value::Null, serde_json::json!(false)] {
                let mut value = json.clone();
                value[field] = malformed;
                assert!(serde_json::from_value::<DeployedCodePackage>(value).is_err());
            }
        }
        let mut old = json;
        let fields = old.as_object_mut().unwrap();
        for field in [
            "service_package_activation_id",
            "host_type",
            "host_isolation_mode",
            "node_name",
            "service_name_internal_use_only",
        ] {
            fields.remove(field);
        }
        for entry in ["setup_entry_point", "main_entry_point"] {
            old[entry].as_object_mut().unwrap().remove("instance_id");
        }
        let old: DeployedCodePackage = serde_json::from_value(old).unwrap();
        assert_eq!(old.host_type, HostType::Invalid);
        assert_eq!(old.host_isolation_mode, HostIsolationMode::None);
        assert!(old.service_package_activation_id.is_empty());
        assert!(old.node_name.is_empty());
        assert!(old.service_name_internal_use_only.is_empty());
        assert_eq!(old.main_entry_point.instance_id, 0);
    }

    #[test]
    fn code_public_json_rejects_missing_required_fields_and_malformed_present_values() {
        let package: DeployedCodePackage = from_slice(&record(Wire::default())).unwrap();
        let json = serde_json::to_value(package).unwrap();
        for field in [
            "name",
            "version",
            "service_manifest_name",
            "status",
            "run_frequency_interval",
            "setup_entry_point",
            "main_entry_point",
            "has_setup_entry_point",
        ] {
            let mut value = json.clone();
            value.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<DeployedCodePackage>(value).is_err());
        }
        for entry in ["setup_entry_point", "main_entry_point"] {
            for field in [
                "entry_point_location",
                "process_id",
                "run_as_user_name",
                "next_activation_time",
                "status",
                "statistics",
            ] {
                let mut value = json.clone();
                value[entry].as_object_mut().unwrap().remove(field);
                assert!(serde_json::from_value::<DeployedCodePackage>(value).is_err());
            }
            for field in json[entry]["statistics"].as_object().unwrap().keys() {
                let mut value = json.clone();
                value[entry]["statistics"]
                    .as_object_mut()
                    .unwrap()
                    .remove(field);
                assert!(serde_json::from_value::<DeployedCodePackage>(value).is_err());
            }
        }
        for pointer in [
            "/status",
            "/host_type",
            "/host_isolation_mode",
            "/setup_entry_point/status",
            "/main_entry_point/status",
        ] {
            for malformed in [
                serde_json::json!(-1),
                serde_json::json!(99),
                serde_json::json!("Started"),
                serde_json::json!(1.5),
                serde_json::Value::Null,
            ] {
                let mut value = json.clone();
                *value.pointer_mut(pointer).unwrap() = malformed;
                assert!(serde_json::from_value::<DeployedCodePackage>(value).is_err());
            }
        }
        for pointer in [
            "/run_frequency_interval",
            "/setup_entry_point/instance_id",
            "/main_entry_point/instance_id",
            "/main_entry_point/statistics/last_exit_code",
        ] {
            for malformed in [
                serde_json::Value::Null,
                serde_json::json!("0"),
                serde_json::json!(1.5),
            ] {
                let mut value = json.clone();
                *value.pointer_mut(pointer).unwrap() = malformed;
                assert!(serde_json::from_value::<DeployedCodePackage>(value).is_err());
            }
        }
    }
}
