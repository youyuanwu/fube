#[path = "../examples/support/mod.rs"]
mod support;

use fube_ra::protocol::*;
use fube_transport::{
    message::{Header, ACTOR, HIGH_PRIORITY},
    serialization::serde::{from_slice, to_vec, ByteArray, NativeEnum, Pointer},
};
use serde::Serialize;

#[test]
fn native_defaults_are_not_zeroed_rust_layouts() {
    let body = AddInstance::default();
    assert_eq!(body.replica.id, -1);
    assert_eq!(body.replica.current_role, 1);
    assert_eq!(body.replica.state, 2);
    assert!(body.replica.up);
    assert_eq!(body.replica.version.version.value.major, 0);
    assert_eq!(body.service.version.version.value.major, 1);
    assert_eq!(body.service.restart_wait, i64::MIN);
    assert_eq!(body.service.move_cost, 1);
    assert_eq!(body.unit.consistency.kind, 1);
}

#[test]
fn full_nested_nondefault_schema_and_scaling_roundtrip() {
    let request = support::rich_request();
    assert_eq!(request.service.scaling.0.len(), 4);
    assert!(!request.service.correlations.0.is_empty());
    assert!(!request.service.placement.0.is_empty());
    assert_eq!(
        from_slice::<AddInstance>(&to_vec(&request).unwrap()).unwrap(),
        request
    );
}

#[test]
fn errors_and_unknown_scalar_enum_values_are_lossless() {
    for error in [0, -2_147_017_398, -2_147_017_441, i64::MIN, i64::MAX] {
        let mut reply = support::rich_reply(error);
        reply.replica.current_role = 987_654;
        assert_eq!(
            from_slice::<AddInstanceReply>(&to_vec(&reply).unwrap()).unwrap(),
            reply
        );
    }
}

#[test]
fn request_and_reply_preserve_real_header_asymmetry() {
    let body = support::rich_request();
    let generation = support::rich_generation(false);
    let message = add_instance_message(&body, &generation).unwrap();
    assert_eq!(decode_add_instance(&message).unwrap(), (body, generation));
    for owner in [Owner::Fm, Owner::Fmm] {
        let reply = support::rich_reply(-2_147_017_398);
        let message = add_instance_reply_message(&reply, owner).unwrap();
        assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
        assert_eq!(decode_add_instance_reply(&message).unwrap(), (reply, owner));
    }
}

#[test]
fn malformed_missing_duplicate_and_wrong_factory_headers_fail() {
    let original =
        add_instance_message(&AddInstance::default(), &GenerationHeader::default()).unwrap();
    for id in [ACTOR, HIGH_PRIORITY, GENERATION_HEADER] {
        let mut message = original.clone();
        message.headers.retain(|h| h.id != id);
        assert!(decode_add_instance(&message).is_err());
        let mut message = original.clone();
        message
            .headers
            .push(message.headers.iter().find(|h| h.id == id).unwrap().clone());
        assert!(decode_add_instance(&message).is_err());
    }
    let mut message = original.clone();
    message.headers[0] = Header::actor(FM_ACTOR).unwrap();
    assert!(decode_add_instance(&message).is_err());
    let mut message = original;
    message.headers[3].value.truncate(3);
    assert!(decode_add_instance(&message).is_err());
}

#[test]
fn node_id_is_exact_raw_sixteen_bytes_not_a_nested_object() {
    #[derive(Serialize)]
    struct BadNode {
        id: ByteArray,
        instance: u64,
    }
    for len in [0, 15, 17] {
        let bytes = to_vec(&BadNode {
            id: ByteArray(vec![1; len]),
            instance: 1,
        })
        .unwrap();
        assert!(from_slice::<NodeInstance>(&bytes).is_err());
    }
}

#[test]
fn selected_bodies_reject_all_proper_prefixes_and_trailing_garbage() {
    let bytes = to_vec(&support::rich_request()).unwrap();
    for end in 0..bytes.len() {
        assert!(from_slice::<AddInstance>(&bytes[..end]).is_err());
    }
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(from_slice::<AddInstance>(&bad).is_err());
    bad = bytes;
    bad[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(from_slice::<AddInstance>(&bad).is_err());
    let bytes = to_vec(&support::rich_reply(-1)).unwrap();
    for end in 0..bytes.len() {
        assert!(from_slice::<AddInstanceReply>(&bytes[..end]).is_err());
    }
}

#[test]
fn oversized_collections_are_explicit_errors() {
    let mut body = AddInstance::default();
    body.service.initialization.0 = vec![0; 4097];
    assert!(to_vec(&body).is_err());
}

#[test]
fn unknown_polymorphic_tag_and_mismatched_kind_are_rejected() {
    let value = support::rich_request().service.scaling.0[2].trigger.clone();
    let mut bytes = to_vec(&value).unwrap();
    assert_eq!(&bytes[11..15], &1_i32.to_le_bytes());
    bytes[11..15].copy_from_slice(&99_i32.to_le_bytes());
    assert!(from_slice::<Pointer<NativeEnum<ScalingTrigger>>>(&bytes).is_err());
    let mut bytes = to_vec(&value).unwrap();
    assert_eq!(&bytes[15..18], &[0x1f, 0x09, 1]);
    bytes[17] = 2;
    assert!(from_slice::<Pointer<NativeEnum<ScalingTrigger>>>(&bytes).is_err());
}
