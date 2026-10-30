#[path = "../examples/support/mod.rs"]
mod support;

use fube_ra::protocol::*;
use fube_transport::{
    message::{Header, ACTION, ACTOR, HIGH_PRIORITY},
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

#[test]
fn removal_inherits_base_scope_and_has_distinct_action_and_reply() {
    for force in [false, true] {
        let body = support::rich_remove(force);
        let message = remove_instance_message(&body, &support::rich_generation(false)).unwrap();
        assert_eq!(decode_remove_instance(&message).unwrap().0, body);
        assert!(decode_add_instance(&message).is_err());
        for end in 0..message.body.len() {
            assert!(from_slice::<RemoveInstance>(&message.body[..end]).is_err());
        }
        let mut bad = message;
        bad.body.push(0);
        assert!(decode_remove_instance(&bad).is_err());
    }
    let reply = support::rich_reply(0);
    for owner in [Owner::Fm, Owner::Fmm] {
        let message = remove_instance_reply_message(&reply, owner).unwrap();
        assert_eq!(
            decode_remove_instance_reply(&message).unwrap(),
            (reply.clone(), owner)
        );
        assert!(decode_add_instance_reply(&message).is_err());
        assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
    }
}

#[test]
fn replica_up_full_lists_flags_and_reply_factory_are_distinct() {
    for last in [false, true] {
        for from_fmm in [false, true] {
            let body = support::rich_replica_up(last, from_fmm);
            assert_eq!(body.replicas.0.len(), 2);
            assert_eq!(body.dropped.0.len(), 2);
            assert_ne!(body.replicas, body.dropped);
            let message = replica_up_message(&body, Owner::Fm).unwrap();
            assert_eq!(
                decode_replica_up(&message).unwrap(),
                (body.clone(), Owner::Fm)
            );
            let generation = support::rich_generation(false);
            let reply = replica_up_reply_message(&body, &generation).unwrap();
            assert_eq!(decode_replica_up_reply(&reply).unwrap(), (body, generation));
            assert!(decode_replica_up(&reply).is_err());
            let mut bad = reply;
            bad.headers.push(bad.headers[3].clone());
            assert!(decode_replica_up_reply(&bad).is_err());
        }
    }
}

#[test]
fn notifications_preserve_nested_values_partial_replies_and_factory_asymmetry() {
    let body = support::rich_notification();
    assert_eq!(body.infos.0.len(), 2);
    assert_ne!(body.infos.0[0], body.infos.0[1]);
    let partial = support::partial_notification_reply();
    assert_eq!(partial.infos.0.as_slice(), &body.infos.0[1..]);
    for request in [ServiceTypeNotification::default(), body] {
        let message = service_type_notification_message(&request).unwrap();
        assert_eq!(decode_service_type_notification(&message).unwrap(), request);
        assert_eq!(
            message
                .headers
                .iter()
                .find(|h| h.id == ACTOR)
                .unwrap()
                .read(|d| d.i64())
                .unwrap(),
            FM_ACTOR
        );
        assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
        assert!(!message
            .headers
            .iter()
            .find(|h| h.id == HIGH_PRIORITY)
            .unwrap()
            .read(|d| d.boolean())
            .unwrap());
        assert!(decode_service_type_notification_reply(&message).is_err());
    }
    for reply in [ServiceTypeNotificationReply::default(), partial] {
        for from_fmm in [false, true] {
            let generation = support::rich_generation(from_fmm);
            let message = service_type_notification_reply_message(&reply, &generation).unwrap();
            assert_eq!(
                decode_service_type_notification_reply(&message).unwrap(),
                (reply.clone(), generation)
            );
            assert!(!message
                .headers
                .iter()
                .find(|h| h.id == HIGH_PRIORITY)
                .unwrap()
                .read(|d| d.boolean())
                .unwrap());
            assert!(decode_service_type_notification(&message).is_err());
        }
    }
}

#[test]
fn notification_headers_reject_wrong_actions_destinations_and_missing_or_duplicate_values() {
    let request = service_type_notification_message(&support::rich_notification()).unwrap();
    let reply = service_type_notification_reply_message(
        &support::partial_notification_reply(),
        &support::rich_generation(false),
    )
    .unwrap();
    for (original, is_reply) in [(request, false), (reply, true)] {
        let decode = |message: &fube_transport::message::Message| {
            if is_reply {
                decode_service_type_notification_reply(message).map(|_| ())
            } else {
                decode_service_type_notification(message).map(|_| ())
            }
        };
        let mut required = vec![ACTOR, ACTION, HIGH_PRIORITY];
        if is_reply {
            required.push(GENERATION_HEADER);
        }
        for id in required {
            let mut missing = original.clone();
            missing.headers.retain(|h| h.id != id);
            assert!(decode(&missing).is_err());
            let mut duplicate = original.clone();
            duplicate.headers.push(
                original
                    .headers
                    .iter()
                    .find(|h| h.id == id)
                    .unwrap()
                    .clone(),
            );
            assert!(decode(&duplicate).is_err());
            let mut malformed = original.clone();
            malformed
                .headers
                .iter_mut()
                .find(|h| h.id == id)
                .unwrap()
                .value
                .clear();
            assert!(decode(&malformed).is_err());
        }
        for actor in [FMM_ACTOR, if is_reply { FM_ACTOR } else { RA_ACTOR }] {
            let mut wrong = original.clone();
            wrong.headers[0] = Header::actor(actor).unwrap();
            assert!(decode(&wrong).is_err());
        }
        let mut wrong = original.clone();
        wrong.headers[1] = Header::action(if is_reply {
            "ServiceTypeNotification"
        } else {
            "ServiceTypeNotificationReply"
        })
        .unwrap();
        assert!(decode(&wrong).is_err());
        // Priority is factory metadata, not a notification acknowledgment guard.
        let mut high = original;
        high.headers[2] = Header::boolean(HIGH_PRIORITY, true).unwrap();
        assert!(decode(&high).is_ok());
    }
}

#[test]
fn notification_collections_reject_truncation_trailing_data_and_codec_bound_overflow() {
    for body in [
        support::rich_notification(),
        support::partial_notification_reply(),
    ] {
        let bytes = to_vec(&body).unwrap();
        for end in 0..bytes.len() {
            assert!(from_slice::<ServiceTypeNotification>(&bytes[..end]).is_err());
        }
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(from_slice::<ServiceTypeNotification>(&bad).is_err());
        let mut bad = bytes;
        bad[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(from_slice::<ServiceTypeNotification>(&bad).is_err());
    }
    let mut body = ServiceTypeNotification::default();
    body.infos.0 = vec![ServiceTypeInfo::default(); 4097];
    assert!(service_type_notification_message(&body).is_err());
}
