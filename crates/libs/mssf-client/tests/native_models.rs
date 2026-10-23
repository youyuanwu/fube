use mssf_client::*;
use mssf_transport::serialization::{
    serde::{from_slice, to_vec, ByteArray, ObjectArray, Pointer},
    Encoder, MAX_COLLECTION_COUNT,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

fn uri_fields(e: &mut Encoder, case: usize) -> Result<()> {
    e.i64(if case == 1 { 0 } else { 1 });
    e.string(if case == 2 { "http" } else { "fabric" })?;
    e.string(if case == 3 { "authority" } else { "" })?;
    e.i64(if case == 4 { 1 } else { 0 });
    e.string(if case == 5 { "host" } else { "" })?;
    e.i32(if case == 6 { 19000 } else { -1 });
    e.string(match case {
        7 => "/",
        8 => "App/Child",
        9 => "/App//Child",
        _ => "/App/Child",
    })?;
    e.string(if case == 10 { "query" } else { "" })?;
    e.string(if case == 11 { "fragment" } else { "" })?;
    e.u32(2);
    e.string("App")?;
    e.string(if case == 12 { "Other" } else { "Child" })
}

#[test]
fn naming_uri_has_distinct_native_mapping_and_checked_string_json() {
    let uri: NamingUri = "fabric:/App/Child".parse().unwrap();
    assert_eq!(uri.as_ref(), "fabric:/App/Child");
    assert_eq!(uri.to_string(), "fabric:/App/Child");
    assert_eq!(uri.clone().into_string(), "fabric:/App/Child");
    assert_eq!(NamingUri::try_from(uri.to_string()).unwrap(), uri);
    let mut e = Encoder::new();
    e.object(|e| uri_fields(e, 0)).unwrap();
    let bytes = to_vec(&uri).unwrap();
    assert_eq!(bytes, e.finish());
    assert_ne!(bytes, to_vec(uri.as_str()).unwrap());
    assert_eq!(from_slice::<NamingUri>(&bytes).unwrap(), uri);
    assert_eq!(
        serde_json::to_string(&uri).unwrap(),
        r#""fabric:/App/Child""#
    );
    assert_eq!(
        serde_json::from_str::<NamingUri>(r#""fabric:/App/Child""#).unwrap(),
        uri
    );
    assert_eq!(
        serde_json::to_value(ApplicationListFilter::Name(uri.clone())).unwrap(),
        serde_json::json!({"Name": "fabric:/App/Child"})
    );
    assert_eq!(
        serde_json::to_value(ServiceQueryParameters::new(uri)).unwrap(),
        serde_json::json!({"application_name": "fabric:/App/Child", "filter": "All"})
    );
    for invalid in [
        "fabric:/",
        "fabric:/bad//name",
        "FABRIC:/App",
        "fabric:/App?query",
    ] {
        assert!(matches!(
            NamingUri::parse(invalid),
            Err(Error::InvalidInput(_))
        ));
        assert!(serde_json::from_value::<NamingUri>(serde_json::json!(invalid)).is_err());
    }
    assert!(NamingUri::parse(&format!("fabric:/{}", "a".repeat(4088))).is_ok());
    assert!(NamingUri::parse(&format!("fabric:/{}", "a".repeat(4089))).is_err());
    assert!(serde_json::from_str::<ServiceQueryParameters>(
        r#"{"application_name":"fabric:/bad/","filter":"All"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<ApplicationListFilter>(r#"{"Name":"fabric:/bad/"}"#).is_err());
}

#[test]
fn naming_uri_validates_components_after_structural_decode_in_every_context() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nested {
        name: NamingUri,
    }
    let name = NamingUri::parse("fabric:/App/Child").unwrap();
    let nested = Nested { name: name.clone() };
    assert_eq!(
        from_slice::<Nested>(&to_vec(&nested).unwrap()).unwrap(),
        nested
    );
    let array = ObjectArray(vec![name.clone()]);
    assert_eq!(
        from_slice::<ObjectArray<Vec<NamingUri>>>(&to_vec(&array).unwrap()).unwrap(),
        array
    );
    let pointer = Pointer(Some(name));
    assert_eq!(
        from_slice::<Pointer<NamingUri>>(&to_vec(&pointer).unwrap()).unwrap(),
        pointer
    );
    for case in 1..=13 {
        let mut e = Encoder::new();
        e.typed_object(if case == 13 { &[1] } else { &[] }, |e| uri_fields(e, case))
            .unwrap();
        assert!(
            matches!(
                from_slice::<NamingUri>(&e.finish()),
                Err(Error::Protocol(_))
            ),
            "{case}"
        );
    }
}

#[test]
fn node_ids_and_timestamps_map_directly_without_guid_or_datetime_conversion() {
    let id = NodeId {
        high: 0x0123456789abcdef,
        low: 0xfedcba9876543210,
    };
    let mut e = Encoder::new();
    let mut bytes = Vec::from(id.high.to_le_bytes());
    bytes.extend_from_slice(&id.low.to_le_bytes());
    e.byte_array(&bytes).unwrap();
    assert_eq!(to_vec(&id).unwrap(), e.finish());
    assert_eq!(from_slice::<NodeId>(&to_vec(&id).unwrap()).unwrap(), id);
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(serde_json::from_str::<NodeId>(&json).unwrap(), id);
    for length in [0, 15, 17] {
        assert!(matches!(
            from_slice::<NodeId>(&to_vec(&ByteArray(vec![0; length])).unwrap()),
            Err(Error::Protocol(_))
        ));
    }
    for ticks in [i64::MIN, -1, 0, 1, i64::MAX] {
        let timestamp = NodeTimestamp(ticks);
        let mut e = Encoder::new();
        e.i64(ticks);
        assert_eq!(to_vec(&timestamp).unwrap(), e.finish());
        assert_eq!(
            from_slice::<NodeTimestamp>(&to_vec(&timestamp).unwrap()).unwrap(),
            timestamp
        );
        assert_eq!(
            serde_json::to_string(&timestamp).unwrap(),
            ticks.to_string()
        );
    }
}

#[test]
fn strict_scalar_models_map_explicit_codes_without_duplicate_wire_enums() {
    fn check<T>(values: &[(T, i64)])
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        for (value, code) in values {
            let mut e = Encoder::new();
            e.i64(*code);
            let bytes = e.finish();
            assert_eq!(to_vec(value).unwrap(), bytes);
            assert_eq!(&from_slice::<T>(&bytes).unwrap(), value);
            assert_eq!(serde_json::to_string(value).unwrap(), code.to_string());
        }
        for code in [i64::MIN, -1, 99, i64::MAX] {
            let mut e = Encoder::new();
            e.i64(code);
            assert!(matches!(
                from_slice::<T>(&e.finish()),
                Err(Error::Protocol(_))
            ));
        }
    }
    check(&[
        (ApplicationStatus::Invalid, 0),
        (ApplicationStatus::Ready, 1),
        (ApplicationStatus::Upgrading, 2),
        (ApplicationStatus::Creating, 3),
        (ApplicationStatus::Deleting, 4),
        (ApplicationStatus::Failed, 5),
    ]);
    check(&[
        (
            ApplicationDefinitionKind::ServiceFabricApplicationDescription,
            0,
        ),
        (ApplicationDefinitionKind::Compose, 1),
        (ApplicationDefinitionKind::MeshApplicationDescription, 2),
        (ApplicationDefinitionKind::Invalid, 65535),
    ]);
    check(&[
        (ServiceStatus::Unknown, 0),
        (ServiceStatus::Active, 1),
        (ServiceStatus::Upgrading, 2),
        (ServiceStatus::Deleting, 3),
        (ServiceStatus::Creating, 4),
        (ServiceStatus::Failed, 5),
    ]);
}

fn application_fields(
    e: &mut Encoder,
    parameters: &[(String, String)],
    suffix: usize,
) -> Result<()> {
    e.object(|e| uri_fields(e, 0))?;
    e.string("Type")?;
    e.string("1")?;
    e.i64(2);
    e.i64(-99);
    e.object_array(parameters, |e, (key, value)| {
        e.string(key)?;
        e.string(value)
    })?;
    if suffix >= 1 {
        e.string("legacy")?;
    }
    if suffix >= 2 {
        e.object_array(&[("old", "value")], |e, (key, value)| {
            e.string(key)?;
            e.string(value)
        })?;
    }
    if suffix >= 3 {
        e.i64(1);
    }
    Ok(())
}

#[test]
fn applications_deserialize_native_maps_and_all_trailing_defaults_directly() {
    let parameters = vec![("z".into(), "last".into()), ("a".into(), "first".into())];
    for suffix in 0..=3 {
        let mut e = Encoder::new();
        e.object(|e| application_fields(e, &parameters, suffix))
            .unwrap();
        let application: Application = from_slice(&e.finish()).unwrap();
        assert_eq!(
            application,
            Application {
                name: "fabric:/App/Child".parse().unwrap(),
                type_name: "Type".into(),
                type_version: "1".into(),
                status: ApplicationStatus::Upgrading,
                health_state: HealthState::Unrecognized(-99),
                parameters: parameters.iter().cloned().collect(),
                upgrade_type_version: if suffix >= 1 {
                    "legacy".into()
                } else {
                    String::new()
                },
                upgrade_parameters: if suffix >= 2 {
                    BTreeMap::from([("old".into(), "value".into())])
                } else {
                    BTreeMap::new()
                },
                definition_kind: if suffix == 3 {
                    ApplicationDefinitionKind::Compose
                } else {
                    ApplicationDefinitionKind::Invalid
                },
            }
        );
        let json = serde_json::json!({
            "name": "fabric:/App/Child",
            "type_name": "Type",
            "type_version": "1",
            "status": 2,
            "health_state": -99,
            "parameters": {"z": "last", "a": "first"},
            "upgrade_type_version": application.upgrade_type_version,
            "upgrade_parameters": application.upgrade_parameters,
            "definition_kind": application.definition_kind
        });
        assert_eq!(
            serde_json::from_value::<Application>(json).unwrap(),
            application
        );
    }
}

#[test]
fn application_maps_reject_duplicate_keys_and_retain_collection_bounds() {
    let duplicate: Vec<(String, String)> = vec![("key".into(), "value".into()); 2];
    for upgrade in [false, true] {
        let mut e = Encoder::new();
        e.object(|e| {
            if upgrade {
                application_fields(e, &[], 1)?;
                e.object_array(&duplicate, |e, (key, value)| {
                    e.string(key)?;
                    e.string(value)
                })
            } else {
                application_fields(e, &duplicate, 0)
            }
        })
        .unwrap();
        assert!(matches!(
            from_slice::<Application>(&e.finish()),
            Err(Error::Protocol(_))
        ));
    }
    let parameters: Vec<(String, String)> = (0..MAX_COLLECTION_COUNT)
        .map(|index| (index.to_string(), "value".into()))
        .collect();
    let mut e = Encoder::new();
    e.object(|e| application_fields(e, &parameters, 0)).unwrap();
    let application: Application = from_slice(&e.finish()).unwrap();
    assert_eq!(application.parameters.len(), MAX_COLLECTION_COUNT);
    assert_eq!(
        application.parameters,
        parameters.into_iter().collect::<BTreeMap<_, _>>()
    );
}

fn event() -> HealthEvent {
    HealthEvent {
        source_id: "source".into(),
        property: "property".into(),
        time_to_live_ticks: i64::MAX,
        health_state: HealthState::Warning,
        description: "event".into(),
        sequence_number: i64::MIN,
        source_utc_timestamp: NodeTimestamp(133000000000000001),
        last_modified_utc_timestamp: NodeTimestamp(133000000000000002),
        is_expired: false,
        remove_when_expired: true,
        last_ok_transition_at: NodeTimestamp(0),
        last_warning_transition_at: NodeTimestamp(-1),
        last_error_transition_at: NodeTimestamp(i64::MAX),
    }
}

fn event_fields(e: &mut Encoder) -> Result<()> {
    e.string("source")?;
    e.string("property")?;
    e.i64(i64::MAX);
    e.i64(2);
    e.string("event")?;
    e.i64(i64::MIN);
    e.i64(133000000000000001);
    e.i64(133000000000000002);
    e.boolean(false);
    e.boolean(true);
    e.i64(0);
    e.i64(-1);
    e.i64(i64::MAX);
    Ok(())
}

#[test]
fn health_models_encode_and_decode_directly_with_exact_native_framing() {
    let evaluation = EventHealthEvaluation {
        description: "evaluation".into(),
        unhealthy_event: event(),
        consider_warning_as_error: true,
        aggregated_health_state: HealthState::Error,
    };
    let health = NodeHealth {
        node_name: "Node".into(),
        health_events: vec![event()],
        aggregated_health_state: HealthState::Unrecognized(-99),
        unhealthy_evaluations: vec![HealthEvaluation::Event(evaluation)],
    };
    let mut e = Encoder::new();
    e.object(|e| {
        e.string("Node")?;
        e.object_array(&[()], |e, _| event_fields(e))?;
        e.i64(-99);
        e.object_array(&[()], |e, _| {
            e.pointer(Some(&1i32.to_le_bytes()), |e| {
                e.i64(1);
                e.string("evaluation")?;
                e.object(event_fields)?;
                e.boolean(true);
                e.i64(3);
                Ok(())
            })
        })
    })
    .unwrap();
    let bytes = to_vec(&health).unwrap();
    assert_eq!(bytes, e.finish());
    assert_eq!(from_slice::<NodeHealth>(&bytes).unwrap(), health);
    let json = serde_json::to_string(&health).unwrap();
    assert!(!json.contains("\"value\""));
    assert_eq!(serde_json::from_str::<NodeHealth>(&json).unwrap(), health);
}

#[test]
fn direct_health_collection_mappings_retain_bounds_and_required_pointers() {
    let health = NodeHealth {
        node_name: "Node".into(),
        health_events: Vec::new(),
        aggregated_health_state: HealthState::Ok,
        unhealthy_evaluations: Vec::new(),
    };
    let mut bytes = Encoder::new();
    bytes
        .object(|e| {
            e.string("Node")?;
            e.object_array::<()>(&[], |_, _| Ok(()))?;
            e.i64(1);
            e.object_array(&[()], |e, _| e.pointer(None, |_| Ok(())))
        })
        .unwrap();
    assert!(matches!(
        from_slice::<NodeHealth>(&bytes.finish()),
        Err(Error::Protocol(_))
    ));
    let evaluation = HealthEvaluation::Event(EventHealthEvaluation {
        description: String::new(),
        unhealthy_event: event(),
        consider_warning_as_error: false,
        aggregated_health_state: HealthState::Ok,
    });
    let at_limit = NodeHealth {
        unhealthy_evaluations: vec![evaluation.clone(); MAX_COLLECTION_COUNT],
        ..health
    };
    let bytes = to_vec(&at_limit).unwrap();
    assert_eq!(from_slice::<NodeHealth>(&bytes).unwrap(), at_limit);
    let over_limit = NodeHealth {
        unhealthy_evaluations: vec![evaluation; MAX_COLLECTION_COUNT + 1],
        ..at_limit
    };
    assert!(matches!(to_vec(&over_limit), Err(Error::InvalidInput(_))));
}

#[test]
fn task_and_identity_models_deserialize_directly_with_native_nesting() {
    let id = NodeDeactivationTaskId {
        id: "task".into(),
        task_type: NodeDeactivationTaskType::Client,
    };
    let id_fields = |e: &mut Encoder| {
        e.string("task")?;
        e.i64(3);
        Ok(())
    };
    let mut e = Encoder::new();
    e.object(id_fields).unwrap();
    assert_eq!(
        from_slice::<NodeDeactivationTaskId>(&e.finish()).unwrap(),
        id
    );
    let task = NodeDeactivationTask {
        id,
        intent: NodeDeactivationIntent::Restart,
    };
    let mut e = Encoder::new();
    e.object(|e| {
        e.object(id_fields)?;
        e.i64(2);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        from_slice::<NodeDeactivationTask>(&e.finish()).unwrap(),
        task
    );
    assert_eq!(
        serde_json::from_value::<NodeDeactivationTask>(serde_json::json!({
            "id": {"id": "task", "task_type": 3},
            "intent": 2
        }))
        .unwrap(),
        task
    );
    assert!(
        serde_json::from_value::<NodeDeactivationTask>(serde_json::json!({
            "id": "task",
            "task_type": 3,
            "intent": 2
        }))
        .is_err()
    );
}

#[test]
fn deactivation_models_decode_nested_task_ids_without_a_parallel_record_layer() {
    let mut e = Encoder::new();
    e.object(|e| {
        e.i64(2);
        e.i64(1);
        e.object_array(&[()], |e, _| {
            e.object(|e| {
                e.string("task")?;
                e.i64(3);
                Ok(())
            })?;
            e.i64(2);
            Ok(())
        })?;
        e.object_array(&[()], |e, _| {
            e.pointer(Some(&1i32.to_le_bytes()), |e| {
                e.i64(1);
                Ok(())
            })
        })
    })
    .unwrap();
    assert_eq!(
        from_slice::<NodeDeactivation>(&e.finish()).unwrap(),
        NodeDeactivation {
            effective_intent: NodeDeactivationIntent::Restart,
            status: NodeDeactivationStatus::SafetyCheckInProgress,
            tasks: vec![NodeDeactivationTask {
                id: NodeDeactivationTaskId {
                    id: "task".into(),
                    task_type: NodeDeactivationTaskType::Client,
                },
                intent: NodeDeactivationIntent::Restart,
            }],
            pending_safety_checks: vec![SafetyCheck::EnsureSeedNodeQuorum],
        }
    );
}
