//! Owned node/application-scoped service packages from the native unpaged query.

use serde::{Deserialize, Serialize};

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, Query, UnpagedListReply},
    DeploymentStatus, NamingUri, QueryDescription,
};

/// Native service-package selectors in facade argument order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DeployedServicePackageQueryParameters {
    /// Required, case-preserving node name: 1..=4096 UTF-16 units without NUL.
    pub node_name: String,
    pub application_name: NamingUri,
    /// Exact service-manifest name; absent or empty omits the filter.
    #[serde(
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_empty"
    )]
    pub service_manifest_name: Option<String>,
}

impl DeployedServicePackageQueryParameters {
    pub fn new(node_name: impl Into<String>, application_name: NamingUri) -> Self {
        Self {
            node_name: node_name.into(),
            application_name,
            service_manifest_name: None,
        }
    }
}

/// One native list request, without paging controls.
pub type DeployedServicePackageQueryDescription =
    QueryDescription<DeployedServicePackageQueryParameters>;

/// Complete native service-package observation, not an application manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployedServicePackage {
    pub service_manifest_name: String,
    pub service_manifest_version: String,
    pub status: DeploymentStatus,
    /// Empty for shared activation or an older record without this suffix.
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub service_package_activation_id: String,
}

impl Query for DeployedServicePackageQueryParameters {
    type Output = Vec<DeployedServicePackage>;
    type Reply =
        UnpagedListReply<DeployedServicePackage, { result_tags::DEPLOYED_SERVICE_PACKAGE }>;
    const NAME: &'static str = "GetServiceManifestListDeployedOnNode";

    fn validate_parameters(&self) -> fubst::Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed service package node name must be 1..=4096 UTF-16 units without NUL",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_package_public_json_and_numeric_status() {
        let query = DeployedServicePackageQueryDescription::new(
            DeployedServicePackageQueryParameters::new("Node_1", "fabric:/App".parse().unwrap()),
        );
        let json = serde_json::to_value(&query).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"parameters": {"NodeName": "Node_1", "ApplicationName": "fabric:/App"}})
        );
        assert_eq!(
            serde_json::from_value::<DeployedServicePackageQueryDescription>(json).unwrap(),
            query
        );
        for json in [
            r#"{"NodeName":"Node_1"}"#,
            r#"{"ApplicationName":"fabric:/App"}"#,
            r#"{"NodeName":"Node_1","ApplicationName":"fabric:/"}"#,
            r#"{"NodeName":"Node_1","ApplicationName":"fabric:/App","MaxResults":1}"#,
        ] {
            assert!(serde_json::from_str::<DeployedServicePackageQueryParameters>(json).is_err());
        }
        let package = DeployedServicePackage {
            service_manifest_name: "Manifest".into(),
            service_manifest_version: "1".into(),
            status: DeploymentStatus::Active,
            service_package_activation_id: String::new(),
        };
        let json = serde_json::to_value(&package).unwrap();
        assert_eq!(json["status"], 3);
        assert_eq!(
            serde_json::from_value::<DeployedServicePackage>(json).unwrap(),
            package
        );
    }
}
