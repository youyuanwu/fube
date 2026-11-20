//! Public model JSON contracts, independent of native wire fixtures.

use fube_client::Service;
use fube_transport::message::{ListenInstance, MessageId};

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

#[test]
fn transport_records_keep_raw_native_order_guid_arrays_in_json() {
    for guid in [[0; 16], std::array::from_fn(|i| (i + 1) as u8)] {
        for index in [0, u32::MAX] {
            let id = MessageId { guid, index };
            let json = serde_json::json!({ "guid": guid, "index": index });
            assert_eq!(serde_json::to_value(id).unwrap(), json);
            assert_eq!(serde_json::from_value::<MessageId>(json).unwrap(), id);
        }
        for address in ["", "node🦀é"] {
            let instance = ListenInstance {
                address: address.into(),
                instance: u64::MAX,
                nonce: guid,
            };
            let json = serde_json::json!({
                "address": address, "instance": u64::MAX, "nonce": guid,
            });
            assert_eq!(serde_json::to_value(&instance).unwrap(), json);
            assert_eq!(
                serde_json::from_value::<ListenInstance>(json).unwrap(),
                instance
            );
        }
    }
    for guid in [
        serde_json::json!(vec![0u8; 15]),
        serde_json::json!(vec![0u8; 17]),
        serde_json::json!(vec![256u16; 16]),
        serde_json::json!("00000000-0000-0000-0000-000000000000"),
    ] {
        assert!(serde_json::from_value::<MessageId>(
            serde_json::json!({ "guid": guid, "index": 0 })
        )
        .is_err());
        assert!(serde_json::from_value::<ListenInstance>(serde_json::json!({
            "address": "", "instance": 0, "nonce": guid,
        }))
        .is_err());
    }
}
