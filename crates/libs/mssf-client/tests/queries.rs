use std::num::NonZeroU64;

use mssf_client::*;
use serde::{de::DeserializeOwned, Serialize};

fn json_round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: &T) {
    let json = serde_json::to_string(value).unwrap();
    assert_eq!(&serde_json::from_str::<T>(&json).unwrap(), value);
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
        ApplicationListFilter::Name("fabric:/App".into()),
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
        ServiceListFilter::Name("fabric:/App/Service".into()),
        ServiceListFilter::TypeName("ExactType".into()),
    ] {
        let query = ServiceQueryDescription {
            parameters: ServiceQueryParameters {
                application_name: "fabric:/App".into(),
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
                events_filter,
            });
            json_round_trip(&query);
            json_round_trip(&query.parameters);
        }
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
    assert_eq!(json["parameters"]["node_name"], "Exact_Node");
    assert!(
        serde_json::from_str::<Paging>(r#"{"continuation_token":null,"max_results":0}"#).is_err()
    );
}
