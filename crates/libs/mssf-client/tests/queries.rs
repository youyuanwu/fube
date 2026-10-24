use std::num::NonZeroU64;

use mssf_client::*;
use serde::{de::DeserializeOwned, Serialize};

fn json_round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: &T) {
    let json = serde_json::to_string(value).unwrap();
    assert_eq!(&serde_json::from_str::<T>(&json).unwrap(), value);
}

#[test]
fn scalar_node_enums_use_signed_numeric_serde_and_retain_unknown_values() {
    use mssf_transport::serialization::{
        serde::{from_slice, to_vec},
        Encoder,
    };
    fn check<T>(as_raw: fn(T) -> i64)
    where
        T: Serialize + DeserializeOwned + Copy + PartialEq + std::fmt::Debug,
    {
        for raw in [i64::MIN, -99, 0, 1, 7, 65535, i64::MAX] {
            let json = raw.to_string();
            let value: T = serde_json::from_str(&json).unwrap();
            assert_eq!(as_raw(value), raw);
            assert_eq!(serde_json::to_string(&value).unwrap(), json);
            let mut e = Encoder::new();
            e.i64(raw);
            let bytes = e.finish();
            assert_eq!(to_vec(&value).unwrap(), bytes);
            assert_eq!(from_slice::<T>(&bytes).unwrap(), value);
        }
    }
    check(NodeStatus::as_raw);
    check(HealthState::as_raw);
    check(NodeDeactivationIntent::as_raw);
    check(NodeDeactivationStatus::as_raw);
    check(NodeDeactivationTaskType::as_raw);
    check(SafetyCheckKind::as_raw);
    assert_eq!(
        serde_json::from_str::<HealthState>("1").unwrap(),
        HealthState::Ok
    );
    assert_eq!(
        serde_json::from_str::<HealthState>("-99").unwrap(),
        HealthState::Unrecognized(-99)
    );
    assert!(serde_json::from_str::<HealthState>(r#""Ok""#).is_err());
    assert!(serde_json::from_str::<HealthState>("1.5").is_err());
    assert!(serde_json::from_str::<HealthState>("18446744073709551615").is_err());
}

#[test]
fn descriptions_and_parameters_support_json_without_becoming_native_wire_dtos() {
    let paging = Paging {
        continuation_token: Some(" opaque+/\0\u{1f980} ".into()),
        max_results: NonZeroU64::new(i64::MAX as u64),
    };
    let nodes = NodeQueryDescription {
        parameters: NodeQueryParameters {
            node_name: Some("Exact_Node".into()),
            status_filter: NodeStatusFilter::from_bits_retain(u32::MAX),
        },
        paging: paging.clone(),
    };
    json_round_trip(&nodes);
    json_round_trip(&nodes.parameters);
    json_round_trip(&paging);
    for filter in [
        ApplicationListFilter::All,
        ApplicationListFilter::Name("fabric:/App".parse().unwrap()),
        ApplicationListFilter::TypeName("ExactType".into()),
        ApplicationListFilter::DefinitionKind(ApplicationDefinitionKindFilter::ALL),
    ] {
        let query = ApplicationQueryDescription {
            parameters: ApplicationQueryParameters {
                filter,
                exclude_application_parameters: true,
            },
            paging: paging.clone(),
        };
        json_round_trip(&query);
        json_round_trip(&query.parameters);
    }
    for filter in [
        ServiceListFilter::All,
        ServiceListFilter::Name("fabric:/App/Service".parse().unwrap()),
        ServiceListFilter::TypeName("ExactType".into()),
    ] {
        let query = ServiceQueryDescription {
            parameters: ServiceQueryParameters {
                application_name: "fabric:/App".parse().unwrap(),
                filter,
            },
            paging: paging.clone(),
        };
        json_round_trip(&query);
        json_round_trip(&query.parameters);
    }
    for policy in [None, Some(NodeHealthPolicy::default())] {
        for events_filter in [
            None,
            Some(HealthEventsFilter::DEFAULT),
            Some(HealthEventsFilter::from_bits_retain(u32::MAX)),
        ] {
            let query = NodeHealthQueryDescription::new(NodeHealthQueryParameters {
                node_name: "Exact_Node".into(),
                health_policy: policy,
                events_filter: events_filter.map(Into::into),
            });
            json_round_trip(&query);
            json_round_trip(&query.parameters);
        }
    }
}

#[test]
fn default_query_json_uses_native_names_and_preserves_explicit_health_options() {
    assert_eq!(
        serde_json::to_value(NodeQueryParameters::default()).unwrap(),
        serde_json::json!({})
    );
    assert_eq!(
        serde_json::to_value(ApplicationQueryParameters::default()).unwrap(),
        serde_json::json!({})
    );
    assert_eq!(
        serde_json::to_value(Paging::default()).unwrap(),
        serde_json::json!({})
    );
    assert_eq!(
        serde_json::from_str::<NodeQueryParameters>("{}").unwrap(),
        NodeQueryParameters::default()
    );
    assert_eq!(
        serde_json::from_str::<ApplicationQueryParameters>("{}").unwrap(),
        ApplicationQueryParameters::default()
    );
    let parameters = NodeHealthQueryParameters {
        health_policy: Some(NodeHealthPolicy::default()),
        events_filter: Some(HealthEventsFilter::DEFAULT.into()),
        node_name: "Node".into(),
    };
    assert_eq!(
        serde_json::to_value(&parameters).unwrap(),
        serde_json::json!({
            "ClusterHealthPolicy": { "ConsiderWarningAsError": false },
            "EventsFilter": { "HealthStateFilter": 0 },
            "NodeName": "Node",
        })
    );
    json_round_trip(&parameters);
    for value in [serde_json::json!(0), serde_json::Value::Null] {
        let parameters = serde_json::from_value::<ApplicationQueryParameters>(
            serde_json::json!({ "ApplicationDefinitionKindFilter": value }),
        )
        .unwrap();
        assert_eq!(
            parameters.filter,
            ApplicationListFilter::DefinitionKind(ApplicationDefinitionKindFilter::DEFAULT)
        );
        assert_eq!(
            serde_json::to_value(&parameters).unwrap(),
            serde_json::json!({ "ApplicationDefinitionKindFilter": null })
        );
        json_round_trip(&parameters);
    }
}

#[test]
fn flattened_selectors_reject_conflicts_duplicates_unknowns_and_malformed_values() {
    for json in [
        r#"{"ApplicationName":"fabric:/App","ApplicationTypeName":"Type"}"#,
        r#"{"ApplicationName":"fabric:/App","ApplicationName":"fabric:/Other"}"#,
        r#"{"ApplicationName":null}"#,
        r#"{"ApplicationTypeName":1}"#,
        r#"{"ApplicationDefinitionKindFilter":"COMPOSE"}"#,
        r#"{"Unknown":null}"#,
        r#"{"filter":"All"}"#,
        r#"{"ApplicationName":"fabric:/bad/"}"#,
    ] {
        assert!(
            serde_json::from_str::<ApplicationQueryParameters>(json).is_err(),
            "{json}"
        );
    }
    for json in [
        r#"{"ApplicationName":"fabric:/App","ServiceName":"fabric:/App/S","ServiceTypeName":"Type"}"#,
        r#"{"ApplicationName":"fabric:/App","ServiceName":"fabric:/App/S","ServiceName":"fabric:/App/T"}"#,
        r#"{"ApplicationName":"fabric:/App","ServiceName":null}"#,
        r#"{"ApplicationName":"fabric:/App","Unknown":"value"}"#,
        r#"{"ApplicationName":"fabric:/App","filter":"All"}"#,
        r#"{"ServiceName":"fabric:/App/S"}"#,
    ] {
        assert!(
            serde_json::from_str::<ServiceQueryParameters>(json).is_err(),
            "{json}"
        );
    }
}

#[test]
fn old_or_malformed_query_json_cannot_silently_become_unfiltered_queries() {
    for json in [
        r#"{"node_name":"Node","status_filter":"UP"}"#,
        r#"{"NodeStatusFilter":"UP"}"#,
        r#"{"NodeStatusFilter":-1}"#,
        r#"{"NodeStatusFilter":4294967296}"#,
        r#"{"NodeName":"Node","Unknown":true}"#,
    ] {
        assert!(
            serde_json::from_str::<NodeQueryParameters>(json).is_err(),
            "{json}"
        );
    }
    for json in [
        r#"{"continuation_token":"next","max_results":10}"#,
        r#"{"MaxResults":0}"#,
        r#"{"Unknown":"value"}"#,
    ] {
        assert!(serde_json::from_str::<Paging>(json).is_err(), "{json}");
    }
    for json in [
        r#"{"node_name":"Node"}"#,
        r#"{"NodeName":"Node","EventsFilter":0}"#,
        r#"{"NodeName":"Node","EventsFilter":{"HealthStateFilter":"ERROR"}}"#,
        r#"{"NodeName":"Node","ClusterHealthPolicy":{"consider_warning_as_error":false}}"#,
        r#"{"NodeName":"Node","ClusterHealthPolicy":{"ConsiderWarningAsError":false,"Unknown":0}}"#,
    ] {
        assert!(
            serde_json::from_str::<NodeHealthQueryParameters>(json).is_err(),
            "{json}"
        );
    }
}

#[test]
fn page_and_request_paging_are_separate_and_unpaged_queries_have_no_paging() {
    let query = NodeQueryDescription::default();
    assert_eq!(query.parameters, NodeQueryParameters::default());
    assert_eq!(query.paging, Paging::default());
    let page: NodeListPage = QueryPage {
        items: Vec::new(),
        continuation_token: Some("opaque-next".into()),
    };
    let next = NodeQueryDescription {
        parameters: query.parameters,
        paging: Paging {
            continuation_token: page.continuation_token,
            max_results: NonZeroU64::new(2),
        },
    };
    assert_eq!(
        next.paging.continuation_token.as_deref(),
        Some("opaque-next")
    );
    let health = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("Exact_Node"));
    let json = serde_json::to_value(health).unwrap();
    assert!(json.get("paging").is_none());
    assert_eq!(json["parameters"]["NodeName"], "Exact_Node");
    assert!(
        serde_json::from_str::<Paging>(r#"{"ContinuationToken":null,"MaxResults":0}"#).is_err()
    );
}
