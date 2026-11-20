//! Owned node-scoped deployed applications from the native paged query.

use serde::{Deserialize, Serialize};

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, ListReply, Query},
    HealthState, NamingUri, PagedQuery, QueryPage,
};

/// Native paged deployed-application parameters in facade argument order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DeployedApplicationQueryParameters {
    /// Required, case-preserving node name: 1..=4096 UTF-16 units without NUL.
    pub node_name: String,
    /// Exact application selector. None omits the filter; native normally
    /// excludes fabric:/System unless it is explicitly selected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_name: Option<NamingUri>,
    /// Defaults to false, like the native paged facade. False is sent explicitly
    /// and leaves result health Unknown; true requests aggregate health.
    #[serde(default)]
    pub include_health_state: bool,
}

impl DeployedApplicationQueryParameters {
    pub fn new(node_name: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
            application_name: None,
            include_health_state: false,
        }
    }
}

/// One native node-scoped page request, with independent paging controls.
pub type DeployedApplicationQueryDescription = PagedQuery<DeployedApplicationQueryParameters>;

crate::native::strict_enum! {
    /// Native deployment lifecycle status, distinct from application status/health.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DeploymentStatus {
        Invalid = 0,
        Downloading = 1,
        Activating = 2,
        Active = 3,
        Upgrading = 4,
        Deactivating = 5,
    }
}

/// An owned native deployed-application observation.
///
/// The native name is a string, not a structured NamingUri object. Directory
/// suffixes absent in older records default to empty; absent health defaults to
/// Unknown. Paths are native node paths and are not normalized by the client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployedApplication {
    pub name: String,
    pub type_name: String,
    pub status: DeploymentStatus,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub work_directory: String,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub log_directory: String,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub temp_directory: String,
    #[serde(
        default,
        deserialize_with = "crate::native::trailing_default::deserialize"
    )]
    pub health_state: HealthState,
}

/// One native page, not an atomic deployment snapshot. Byte limits can produce
/// a continuation even when no count limit was requested.
pub type DeployedApplicationListPage = QueryPage<DeployedApplication>;

impl Query for DeployedApplicationQueryParameters {
    type Output = DeployedApplicationListPage;
    type Reply = ListReply<DeployedApplication, { result_tags::DEPLOYED_APPLICATION }>;
    const NAME: &'static str = "GetApplicationPagedListDeployedOnNode";

    fn validate_parameters(&self) -> fubst::Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed application node name must be 1..=4096 UTF-16 units without NUL",
        )
    }

    fn validate_record(
        record: DeployedApplication,
    ) -> std::result::Result<DeployedApplication, &'static str> {
        if matches!(record.health_state, HealthState::Unrecognized(_)) {
            return Err("unsupported deployed application health state");
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Paging;
    use std::num::NonZeroU64;

    #[test]
    fn deployed_public_json_preserves_nested_parameters_and_numeric_results() {
        let query = DeployedApplicationQueryDescription {
            parameters: DeployedApplicationQueryParameters {
                node_name: "Node_1".into(),
                application_name: Some(NamingUri::parse("fabric:/App").unwrap()),
                include_health_state: true,
            },
            paging: Paging {
                continuation_token: Some("next".into()),
                max_results: NonZeroU64::new(2),
            },
        };
        let json = serde_json::to_value(&query).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "parameters": {
                    "NodeName": "Node_1",
                    "ApplicationName": "fabric:/App",
                    "IncludeHealthState": true
                },
                "paging": {"ContinuationToken": "next", "MaxResults": 2}
            })
        );
        assert_eq!(
            serde_json::from_value::<DeployedApplicationQueryDescription>(json).unwrap(),
            query
        );
        let parameters: DeployedApplicationQueryParameters =
            serde_json::from_str(r#"{"NodeName":"Node_1"}"#).unwrap();
        assert_eq!(
            parameters,
            DeployedApplicationQueryParameters::new("Node_1")
        );
        for json in [
            r#"{}"#,
            r#"{"NodeName":"Node_1","Typo":1}"#,
            r#"{"NodeName":"Node_1","IncludeHealthState":"true"}"#,
            r#"{"NodeName":"Node_1","ApplicationName":"fabric:/"}"#,
        ] {
            assert!(serde_json::from_str::<DeployedApplicationQueryParameters>(json).is_err());
        }
        let application: DeployedApplication = serde_json::from_str(
            r#"{"name":"fabric:/App","type_name":"Type","status":3,"health_state":2}"#,
        )
        .unwrap();
        let json = serde_json::to_value(&application).unwrap();
        assert_eq!(json["status"], 3);
        assert_eq!(json["health_state"], 2);
        assert_eq!(
            serde_json::from_value::<DeployedApplication>(json).unwrap(),
            application
        );
        let application: DeployedApplication =
            serde_json::from_str(r#"{"name":"fabric:/App","type_name":"Type","status":3}"#)
                .unwrap();
        assert_eq!(application.health_state, HealthState::Unknown);
        assert!(application.work_directory.is_empty());
        assert!(application.log_directory.is_empty());
        assert!(application.temp_directory.is_empty());
        for health in [
            serde_json::Value::Null,
            serde_json::json!("Unknown"),
            serde_json::json!(1.5),
        ] {
            let mut malformed = serde_json::to_value(&application).unwrap();
            malformed["health_state"] = health;
            assert!(serde_json::from_value::<DeployedApplication>(malformed).is_err());
        }
    }
}
