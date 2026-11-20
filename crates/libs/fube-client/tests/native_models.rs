//! Public model JSON contracts, independent of native wire fixtures.

use fube_client::Service;

#[test]
fn service_json_requires_numeric_scalars_and_rejects_malformed_present_suffixes() {
    let base = serde_json::json!({
        "kind": 2,
        "name": "fabric:/App/Child",
        "type_name": "ServiceType",
        "manifest_version": "1.2",
        "has_persisted_state": true,
        "health_state": 1,
        "status": 2,
        "is_service_group": true,
    });
    for (field, malformed) in [
        ("kind", serde_json::json!(-1)),
        ("kind", serde_json::json!(3)),
        ("kind", serde_json::json!("Stateful")),
        (
            "kind",
            serde_json::json!({"Stateful": {"has_persisted_state": true}}),
        ),
        ("status", serde_json::json!(6)),
        ("status", serde_json::json!("Unknown")),
        ("status", serde_json::Value::Null),
        ("is_service_group", serde_json::json!(0)),
        ("is_service_group", serde_json::json!("false")),
        ("is_service_group", serde_json::Value::Null),
        ("has_persisted_state", serde_json::json!("true")),
        ("name", serde_json::json!("fabric:/bad/")),
    ] {
        let mut json = base.clone();
        json[field] = malformed;
        assert!(serde_json::from_value::<Service>(json).is_err(), "{field}");
    }
    for field in ["kind", "has_persisted_state", "status", "is_service_group"] {
        let mut json = base.clone();
        json.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<Service>(json).is_err(), "{field}");
    }
}
