//! Owned native node/application-scoped code packages and entry-point observations.

use serde::{Deserialize, Serialize};

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, Query, UnpagedListReply},
    DeploymentStatus, NamingUri, NodeTimestamp, QueryDescription,
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

    fn validate_parameters(&self) -> fubst::Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed code package node name must be 1..=4096 UTF-16 units without NUL",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parameters() -> DeployedCodePackageQueryParameters {
        DeployedCodePackageQueryParameters::new(
            " Node_Ab ",
            NamingUri::parse("fabric:/App").unwrap(),
        )
    }

    fn package() -> DeployedCodePackage {
        let entry = CodePackageEntryPoint {
            entry_point_location: "/entry".into(),
            process_id: 42,
            run_as_user_name: String::new(),
            next_activation_time: NodeTimestamp(0),
            status: EntryPointStatus::Started,
            statistics: CodePackageEntryPointStatistics {
                last_exit_code: 0,
                last_activation_time: NodeTimestamp(-101),
                last_exit_time: NodeTimestamp(202),
                last_successful_activation_time: NodeTimestamp(0),
                last_successful_exit_time: NodeTimestamp(0),
                activation_failure_count: 0,
                continuous_activation_failure_count: 0,
                exit_failure_count: 0,
                continuous_exit_failure_count: 0,
                activation_count: 1,
                exit_count: 0,
            },
            instance_id: -123,
        };
        DeployedCodePackage {
            name: "Code".into(),
            version: "1".into(),
            service_manifest_name: "Manifest".into(),
            status: DeploymentStatus::Active,
            run_frequency_interval: 0,
            setup_entry_point: entry.clone(),
            main_entry_point: entry,
            has_setup_entry_point: false,
            service_package_activation_id: String::new(),
            host_type: HostType::ContainerHost,
            host_isolation_mode: HostIsolationMode::HyperV,
            node_name: "Node".into(),
            service_name_internal_use_only: String::new(),
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
        let package = package();
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
        let package = package();
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
