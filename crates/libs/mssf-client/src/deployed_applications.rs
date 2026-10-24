//! Owned node-scoped deployed applications from the native paged query.

use serde::{Deserialize, Serialize};

use crate::{
    query_protocol::{ListReply, Query},
    Error, HealthState, NamingUri, PagedQuery, QueryPage, Result,
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
    type Reply = ListReply<DeployedApplication, 10>;
    const NAME: &'static str = "GetApplicationPagedListDeployedOnNode";

    fn validate_parameters(&self) -> Result<()> {
        let name = &self.node_name;
        if name.is_empty()
            || name.len() > 4096 * 3
            || name.contains('\0')
            || name.encode_utf16().count() > 4096
        {
            return Err(Error::InvalidInput(
                "deployed application node name must be 1..=4096 UTF-16 units without NUL",
            ));
        }
        Ok(())
    }

    fn validate_record(record: DeployedApplication) -> Result<DeployedApplication> {
        if matches!(record.health_state, HealthState::Unrecognized(_)) {
            return Err(Error::Protocol(
                "unsupported deployed application health state",
            ));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        query_protocol::{decode_reply, encode_query},
        Paging,
    };
    use mssf_transport::{
        message::Message,
        serialization::{
            serde::{from_fields, from_slice, to_vec},
            Decoder, Encoder,
        },
    };
    use std::num::NonZeroU64;

    // ServiceModel/DeployedApplicationQueryResult.h FABRIC_FIELDS_07: the
    // application name is std::wstring, unlike ApplicationQueryResult's URI.
    fn fields(e: &mut Encoder, count: usize, status: i64, health: i64) -> Result<()> {
        for field in 0..count {
            match field {
                0 => e.string("fabric:/应用")?,
                1 => e.string("Type")?,
                2 => e.i64(status),
                3 => e.string("/node/应用/work")?,
                4 => e.string("/node/应用/log")?,
                5 => e.string("/node/应用/temp")?,
                6 => e.i64(health),
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    fn record(count: usize, status: i64, health: i64) -> Vec<u8> {
        let mut e = Encoder::new();
        e.object(|e| fields(e, count, status, health)).unwrap();
        e.finish()
    }

    fn decode_record(bytes: &[u8]) -> Result<DeployedApplication> {
        DeployedApplicationQueryParameters::validate_record(from_slice(bytes)?)
    }

    fn reply(
        kind: i64,
        tag: Option<i32>,
        count: usize,
        status: i64,
        health: i64,
        error: i64,
        paging: Option<(&[u8], &str)>,
    ) -> Message {
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(kind);
            let tag = tag.map(i32::to_le_bytes);
            e.pointer(tag.as_ref().map(|tag| tag.as_slice()), |e| {
                e.object_array(&[()], |e, _| fields(e, count, status, health))?;
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

    fn assert_request(
        parameters: &DeployedApplicationQueryParameters,
        paging: Option<&Paging>,
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
            e.string("GetApplicationPagedListDeployedOnNode")
        })
        .unwrap();
        assert_eq!(encode_query(parameters, paging).unwrap(), e.finish());
    }

    #[test]
    fn deployed_query_native_name_order_omission_boolean_and_paging() {
        let mut parameters = DeployedApplicationQueryParameters::new("Node_1");
        assert_request(
            &parameters,
            None,
            &[("NodeName", "Node_1"), ("IncludeHealthState", "false")],
        );
        assert_request(
            &parameters,
            Some(&Paging {
                continuation_token: Some(String::new()),
                max_results: None,
            }),
            &[("NodeName", "Node_1"), ("IncludeHealthState", "false")],
        );
        parameters.application_name = Some(NamingUri::parse("fabric:/App").unwrap());
        parameters.include_health_state = true;
        assert_request(
            &parameters,
            Some(&Paging {
                continuation_token: Some("opaque:/应用".into()),
                max_results: NonZeroU64::new(3),
            }),
            &[
                ("NodeName", "Node_1"),
                ("ApplicationName", "fabric:/App"),
                ("IncludeHealthState", "true"),
                ("ContinuationToken", "opaque:/应用"),
                ("MaxResults", "3"),
            ],
        );
        assert!(matches!(
            encode_query(
                &parameters,
                Some(&Paging {
                    continuation_token: None,
                    max_results: NonZeroU64::new(i64::MAX as u64 + 1),
                })
            ),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn deployed_query_requires_bounded_nonempty_node_name() {
        for name in [
            String::new(),
            "bad\0node".into(),
            "x".repeat(4097),
            "😀".repeat(2049),
        ] {
            assert!(matches!(
                encode_query(&DeployedApplicationQueryParameters::new(name), None),
                Err(Error::InvalidInput(_))
            ));
        }
        for name in ["x".repeat(4096), "😀".repeat(2048), "节点".into()] {
            encode_query(&DeployedApplicationQueryParameters::new(name), None).unwrap();
        }
    }

    #[test]
    fn deployed_record_exact_native_string_layout_and_numeric_enums() {
        for (status, expected) in [
            (0, DeploymentStatus::Invalid),
            (1, DeploymentStatus::Downloading),
            (2, DeploymentStatus::Activating),
            (3, DeploymentStatus::Active),
            (4, DeploymentStatus::Upgrading),
            (5, DeploymentStatus::Deactivating),
        ] {
            let bytes = record(7, status, 2);
            let application = decode_record(&bytes).unwrap();
            assert_eq!(application.name, "fabric:/应用");
            assert_eq!(application.type_name, "Type");
            assert_eq!(application.status, expected);
            assert_eq!(application.work_directory, "/node/应用/work");
            assert_eq!(application.log_directory, "/node/应用/log");
            assert_eq!(application.temp_directory, "/node/应用/temp");
            assert_eq!(application.health_state, HealthState::Warning);
            assert_eq!(to_vec(&application).unwrap(), bytes);
        }
        for health in [0, 1, 2, 3, 65535] {
            assert_eq!(
                decode_record(&record(7, 3, health))
                    .unwrap()
                    .health_state
                    .as_raw(),
                health
            );
        }
        for status in [-1, 6, i64::MAX] {
            assert!(matches!(
                decode_record(&record(7, status, 1)),
                Err(Error::Protocol(_))
            ));
        }
        for health in [-1, 4, i64::MAX] {
            assert!(matches!(
                decode_record(&record(7, 3, health)),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn deployed_record_required_prefix_and_optional_native_suffix_defaults() {
        for count in 0..3 {
            assert!(decode_record(&record(count, 3, 1)).is_err());
        }
        for count in 3..=7 {
            let application = decode_record(&record(count, 3, 1)).unwrap();
            assert_eq!(
                application.work_directory,
                if count > 3 { "/node/应用/work" } else { "" }
            );
            assert_eq!(
                application.log_directory,
                if count > 4 { "/node/应用/log" } else { "" }
            );
            assert_eq!(
                application.temp_directory,
                if count > 5 { "/node/应用/temp" } else { "" }
            );
            assert_eq!(
                application.health_state,
                if count > 6 {
                    HealthState::Ok
                } else {
                    HealthState::Unknown
                }
            );
        }
    }

    #[test]
    fn deployed_record_rejects_malformed_suffix_and_structured_uri_in_string_slot() {
        for count in 3..=6 {
            let mut e = Encoder::new();
            e.object(|e| {
                fields(e, count, 3, 1)?;
                e.boolean(false);
                Ok(())
            })
            .unwrap();
            assert!(matches!(
                decode_record(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
        let mut e = Encoder::new();
        e.object(|e| {
            e.object(|e| {
                e.i64(1);
                e.string("fabric")
            })?;
            e.string("Type")?;
            e.i64(3);
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            decode_record(&e.finish()),
            Err(Error::Protocol(_))
        ));
        let bytes = record(7, 3, 1);
        for end in 0..bytes.len() {
            assert!(decode_record(&bytes[..end]).is_err());
        }
        let mut e = Encoder::new();
        e.object(|e| {
            fields(e, 3, 3, 1)?;
            e.string("tail")
        })
        .unwrap();
        let mut bytes = e.finish();
        let count = bytes.len() - 11;
        assert_eq!(bytes[count - 1], 0x8d);
        bytes[count] = 20;
        assert!(matches!(
            Decoder::new(&bytes).object(from_fields::<DeployedApplication>),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn deployed_list_shared_envelope_paging_errors_and_payload_precedence() {
        let page = decode_reply::<DeployedApplicationQueryParameters>(&reply(
            2,
            Some(10),
            7,
            3,
            1,
            0,
            Some((&[], "opaque-next")),
        ))
        .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].name, "fabric:/应用");
        assert_eq!(page.continuation_token.as_deref(), Some("opaque-next"));
        let page = decode_reply::<DeployedApplicationQueryParameters>(&reply(
            2,
            Some(10),
            3,
            3,
            1,
            0,
            Some((&[], "")),
        ))
        .unwrap();
        assert_eq!(page.items[0].health_state, HealthState::Unknown);
        assert_eq!(page.continuation_token, None);
        for message in [
            reply(1, Some(10), 7, 3, 1, 0, None),
            reply(99, Some(10), 7, 3, 1, 0, None),
            reply(2, Some(4), 7, 3, 1, 0, None),
            reply(2, None, 7, 3, 1, 0, None),
            reply(2, Some(10), 7, 3, 1, 0, Some((&[1], "next"))),
        ] {
            assert!(matches!(
                decode_reply::<DeployedApplicationQueryParameters>(&message),
                Err(Error::Protocol(_))
            ));
        }
        assert!(matches!(
            decode_reply::<DeployedApplicationQueryParameters>(&reply(
                2,
                Some(10),
                7,
                3,
                1,
                123,
                None
            )),
            Err(Error::Remote { code: 123, .. })
        ));
        for message in [
            reply(2, Some(10), 7, 99, 1, 123, None),
            reply(2, Some(10), 7, 3, 99, 123, None),
            reply(2, Some(10), 2, 3, 1, 123, None),
            reply(2, Some(4), 7, 3, 1, 123, None),
        ] {
            assert!(matches!(
                decode_reply::<DeployedApplicationQueryParameters>(&message),
                Err(Error::Protocol(_))
            ));
        }
    }

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
        let application = decode_record(&record(7, 3, 2)).unwrap();
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
