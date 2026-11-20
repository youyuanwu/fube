//! Owned deployed replicas and instances from the native unpaged node query.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    query_arguments::validate_node_name,
    query_protocol::{result_tags, Query, UnpagedListReply},
    NamingUri, NodeTimestamp, QueryDescription, ReplicaRole, ReplicaStatus, ServiceKind,
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

    fn validate_parameters(&self) -> fubst::Result<()> {
        validate_node_name(
            &self.node_name,
            "deployed replica node name must be 1..=4096 UTF-16 units without NUL",
        )
    }

    fn validate_record(
        record: DeployedReplica,
    ) -> std::result::Result<DeployedReplica, &'static str> {
        if record.kind == ServiceKind::Invalid {
            return Err("unsupported deployed replica service kind");
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(crate) const ID: &str = "00112233-4455-6677-8899-aabbccddeeff";

    fn parameters() -> DeployedReplicaQueryParameters {
        DeployedReplicaQueryParameters::new("Node_1", NamingUri::parse("fabric:/App").unwrap())
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
