//! Owned node/application-scoped service packages from the native unpaged query.

use serde::{Deserialize, Serialize};

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, Query, UnpagedListReply},
    DeploymentStatus, NamingUri, QueryDescription, Result,
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

    fn validate_parameters(&self) -> Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed service package node name must be 1..=4096 UTF-16 units without NUL",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        partitions::tests::assert_request,
        query_protocol::{decode_reply, encode_query},
        Error,
    };
    use mssf_transport::{
        message::Message,
        serialization::{
            serde::{from_slice, to_vec},
            Encoder,
        },
    };

    fn fields(e: &mut Encoder, count: usize, status: i64) -> Result<()> {
        for field in 0..count {
            match field {
                0 => e.string("ServiceManifest")?,
                1 => e.string("1.2")?,
                2 => e.i64(status),
                3 => e.string("activation-id")?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    fn record(count: usize, status: i64) -> Vec<u8> {
        let mut e = Encoder::new();
        e.object(|e| fields(e, count, status)).unwrap();
        e.finish()
    }

    fn reply(tag: i32, status: i64, error: i64, paging: Option<&str>) -> Message {
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(2);
            e.pointer(Some(&tag.to_le_bytes()), |e| {
                e.object_array(&[()], |e, _| fields(e, 4, status))?;
                e.pointer(None, |_| Ok(()))
            })?;
            e.i64(error);
            e.pointer(paging.map(|_| &[][..]), |e| e.string(paging.unwrap()))
        })
        .unwrap();
        Message {
            headers: Vec::new(),
            body: e.finish(),
        }
    }

    #[test]
    fn service_package_query_native_order_filters_and_bounded_inputs() {
        let mut parameters =
            DeployedServicePackageQueryParameters::new("Node_1", "fabric:/App".parse().unwrap());
        for filter in [None, Some(String::new())] {
            parameters.service_manifest_name = filter;
            assert_request(
                &parameters,
                None,
                "GetServiceManifestListDeployedOnNode",
                &[("NodeName", "Node_1"), ("ApplicationName", "fabric:/App")],
            );
        }
        parameters.service_manifest_name = Some("Manifest".into());
        assert_request(
            &parameters,
            None,
            "GetServiceManifestListDeployedOnNode",
            &[
                ("NodeName", "Node_1"),
                ("ApplicationName", "fabric:/App"),
                ("ServiceManifestName", "Manifest"),
            ],
        );
        for node in [
            String::new(),
            "bad\0node".into(),
            "x".repeat(4097),
            "\u{1f600}".repeat(2049),
            "x".repeat(100_000),
        ] {
            parameters.node_name = node;
            assert!(matches!(
                encode_query(&parameters, None),
                Err(Error::InvalidInput(_))
            ));
        }
        for node in [
            "x".repeat(4096),
            "\u{754c}".repeat(4096),
            "\u{1f600}".repeat(2048),
        ] {
            parameters.node_name = node;
            encode_query(&parameters, None).unwrap();
        }
        parameters.node_name = "Node_1".into();
        parameters.service_manifest_name = Some("x".repeat(32769));
        assert!(matches!(
            encode_query(&parameters, None),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn service_package_complete_native_layout_and_compatible_suffix() {
        for status in 0..=5 {
            let bytes = record(4, status);
            let package: DeployedServicePackage = from_slice(&bytes).unwrap();
            assert_eq!(package.service_manifest_name, "ServiceManifest");
            assert_eq!(package.service_manifest_version, "1.2");
            assert_eq!(package.service_package_activation_id, "activation-id");
            assert_eq!(serde_json::to_value(package.status).unwrap(), status);
            assert_eq!(to_vec(&package).unwrap(), bytes);
        }
        let package: DeployedServicePackage = from_slice(&record(3, 3)).unwrap();
        assert_eq!(package.status, DeploymentStatus::Active);
        assert!(package.service_package_activation_id.is_empty());
        for count in 0..3 {
            assert!(from_slice::<DeployedServicePackage>(&record(count, 3)).is_err());
        }
        for status in [-1, 6, i64::MAX] {
            assert!(from_slice::<DeployedServicePackage>(&record(4, status)).is_err());
        }
        let mut e = Encoder::new();
        e.object(|e| {
            fields(e, 3, 3)?;
            e.boolean(false);
            Ok(())
        })
        .unwrap();
        assert!(from_slice::<DeployedServicePackage>(&e.finish()).is_err());
        let bytes = record(4, 3);
        for end in 0..bytes.len() {
            assert!(from_slice::<DeployedServicePackage>(&bytes[..end]).is_err());
        }
    }

    #[test]
    fn service_package_unpaged_reply_and_payload_error_precedence() {
        let packages =
            decode_reply::<DeployedServicePackageQueryParameters>(&reply(11, 3, 0, None)).unwrap();
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].status, DeploymentStatus::Active);
        assert!(matches!(
            decode_reply::<DeployedServicePackageQueryParameters>(&reply(11, 3, 123, None)),
            Err(Error::Remote { code: 123, .. })
        ));
        for error in [0, 123] {
            for message in [
                reply(10, 3, error, None),
                reply(11, 99, error, None),
                reply(11, 3, error, Some("next")),
                reply(11, 3, error, Some("")),
            ] {
                assert!(matches!(
                    decode_reply::<DeployedServicePackageQueryParameters>(&message),
                    Err(Error::Protocol(_))
                ));
            }
        }
    }

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
        let package: DeployedServicePackage = from_slice(&record(4, 3)).unwrap();
        let json = serde_json::to_value(&package).unwrap();
        assert_eq!(json["status"], 3);
        assert_eq!(
            serde_json::from_value::<DeployedServicePackage>(json).unwrap(),
            package
        );
    }
}
