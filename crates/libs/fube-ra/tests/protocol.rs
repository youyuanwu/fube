#[path = "../examples/support/mod.rs"]
mod support;

use fube_ra::protocol::*;
use fube_transport::{
    message::{Header, ACTION, ACTOR, HIGH_PRIORITY},
    serialization::serde::{from_slice, to_vec, ByteArray, NativeEnum, Pointer},
};
use serde::Serialize;

fn generation_owner(high: u64, low: u64) -> [u8; 16] {
    let mut owner = [0; 16];
    owner[..8].copy_from_slice(&high.to_le_bytes());
    owner[8..].copy_from_slice(&low.to_le_bytes());
    owner
}

#[test]
fn change_configuration_preserves_native_configuration_and_unit_reply_schemas() {
    use fube_transport::serialization::serde::ObjectArray;
    let request = support::rich_request();
    let body = Configuration {
        unit: request.unit,
        service: request.service,
        replicas: ObjectArray(vec![request.replica]),
    };
    let generation = support::rich_generation(false);
    let message = change_configuration_message(&body, Owner::Fm).unwrap();
    assert_eq!(
        decode_change_configuration(&message).unwrap(),
        (body.clone(), Owner::Fm)
    );
    assert_eq!(message.properties().unwrap().actor, FM_ACTOR);
    assert!(message.properties().unwrap().high_priority);
    assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
    let reply = FailoverUnitReply {
        unit: body.unit,
        error: 0,
    };
    let ack = change_configuration_reply_message(&reply, &generation).unwrap();
    assert_eq!(
        decode_change_configuration_reply(&ack).unwrap(),
        (reply, generation)
    );
    assert_eq!(ack.properties().unwrap().actor, RA_ACTOR);
    assert!(ack.properties().unwrap().high_priority);
    for id in [ACTOR, ACTION, HIGH_PRIORITY] {
        let mut invalid = message.clone();
        invalid.headers.retain(|h| h.id != id);
        assert!(decode_change_configuration(&invalid).is_err());
    }
    let mut invalid = ack.clone();
    invalid.headers.retain(|h| h.id != GENERATION_HEADER);
    assert!(decode_change_configuration_reply(&invalid).is_err());
    for len in 0..ack.body.len() {
        let mut invalid = ack.clone();
        invalid.body.truncate(len);
        assert!(decode_change_configuration_reply(&invalid).is_err());
    }
}

#[test]
fn lfum_upload_preserves_native_body_generation_and_bodyless_correlated_ack() {
    use fube_transport::serialization::serde::ObjectArray;
    let generation = support::rich_generation(false);
    let upload = LfumUpload {
        generation: generation.number.clone(),
        node: NodeDescription {
            name: "original-node".into(),
            ..NodeDescription::default()
        },
        units: ObjectArray(vec![FailoverUnitInfo {
            service: support::rich_request().service,
            unit: support::rich_request().unit,
            ..FailoverUnitInfo::default()
        }]),
        any_replica_found: true,
    };
    let message = lfum_upload_message(&upload).unwrap();
    assert_eq!(decode_lfum_upload(&message).unwrap(), upload);
    assert!(!message
        .headers
        .iter()
        .any(|header| header.id == GENERATION_HEADER));
    let ack = lfum_upload_reply_message(&generation).unwrap();
    assert!(ack.body.is_empty());
    assert_eq!(decode_lfum_upload_reply(&ack).unwrap(), generation);
    for id in [ACTOR, ACTION, HIGH_PRIORITY] {
        for original in [&message, &ack] {
            let mut bad = original.clone();
            bad.headers.retain(|header| header.id != id);
            assert!(if original.body.is_empty() {
                decode_lfum_upload_reply(&bad).is_err()
            } else {
                decode_lfum_upload(&bad).is_err()
            });
            let mut duplicate = original.clone();
            duplicate.headers.push(
                original
                    .headers
                    .iter()
                    .find(|header| header.id == id)
                    .unwrap()
                    .clone(),
            );
            assert!(if original.body.is_empty() {
                decode_lfum_upload_reply(&duplicate).is_err()
            } else {
                decode_lfum_upload(&duplicate).is_err()
            });
        }
    }
    let mut bad = message.clone();
    bad.headers.push(generation.header().unwrap());
    assert!(decode_lfum_upload(&bad).is_err());
    for length in 0..message.body.len() {
        let mut bad = message.clone();
        bad.body.truncate(length);
        assert!(decode_lfum_upload(&bad).is_err());
    }
    let mut bad = ack.clone();
    bad.body.push(0);
    assert!(decode_lfum_upload_reply(&bad).is_err());
    let mut bad = ack.clone();
    bad.headers.retain(|header| header.id != GENERATION_HEADER);
    assert!(decode_lfum_upload_reply(&bad).is_err());
    let mut fmm = generation.clone();
    fmm.for_fm_replica = true;
    assert!(lfum_upload_reply_message(&fmm).is_err());
    let mut bad = ack;
    bad.headers.retain(|header| header.id != GENERATION_HEADER);
    bad.headers.push(fmm.header().unwrap());
    assert!(decode_lfum_upload_reply(&bad).is_err());
}

#[test]
fn remove_replica_uses_replica_body_not_delete_scopes_and_distinct_native_headers() {
    let body = support::rich_request();
    let generation = support::rich_generation(false);
    let original = remove_replica_message(&body, &generation).unwrap();
    assert_eq!(
        decode_remove_replica(&original).unwrap(),
        (body.clone(), generation)
    );
    assert_eq!(original.body, to_vec(&body).unwrap());
    assert!(original.properties().unwrap().high_priority);
    assert!(decode_add_replica(&original).is_err());
    assert!(decode_remove_instance(&original).is_err());
    assert!(decode_delete_replica(&original).is_err());
    for id in [ACTOR, ACTION, HIGH_PRIORITY, GENERATION_HEADER] {
        let mut missing = original.clone();
        missing.headers.retain(|header| header.id != id);
        assert!(decode_remove_replica(&missing).is_err());
        let mut duplicate = original.clone();
        duplicate.headers.push(
            original
                .headers
                .iter()
                .find(|h| h.id == id)
                .unwrap()
                .clone(),
        );
        assert!(decode_remove_replica(&duplicate).is_err());
    }
    for actor in [FM_ACTOR, FMM_ACTOR] {
        let mut wrong = original.clone();
        wrong.headers[0] = Header::actor(actor).unwrap();
        assert!(decode_remove_replica(&wrong).is_err());
    }
    for n in 0..original.body.len() {
        let mut short = original.clone();
        short.body.truncate(n);
        assert!(decode_remove_replica(&short).is_err());
    }
    for error in [0, -2_147_017_398] {
        let mut reply = support::rich_reply(error);
        reply.replica.current_role = 2;
        reply.replica.state = 3;
        for owner in [Owner::Fm, Owner::Fmm] {
            let message = remove_replica_reply_message(&reply, owner).unwrap();
            assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
            assert_eq!(
                decode_remove_replica_reply(&message).unwrap(),
                (reply.clone(), owner)
            );
            assert!(decode_add_replica_reply(&message).is_err());
            assert!(decode_delete_replica_reply(&message).is_err());
        }
    }
}

#[test]
fn add_replica_is_a_distinct_native_fm_action_with_ready_idle_reply_and_strict_headers() {
    let request = support::rich_request();
    let generation = support::rich_generation(false);
    let original = add_replica_message(&request, &generation).unwrap();
    assert_eq!(
        decode_add_replica(&original).unwrap(),
        (request, generation)
    );
    assert!(decode_add_primary(&original).is_err());
    assert!(decode_create_replica(&original).is_err());
    for id in [ACTOR, ACTION, HIGH_PRIORITY, GENERATION_HEADER] {
        let mut missing = original.clone();
        missing.headers.retain(|header| header.id != id);
        assert!(decode_add_replica(&missing).is_err());
        let mut duplicate = original.clone();
        duplicate.headers.push(
            original
                .headers
                .iter()
                .find(|h| h.id == id)
                .unwrap()
                .clone(),
        );
        assert!(decode_add_replica(&duplicate).is_err());
    }
    for actor in [FM_ACTOR, FMM_ACTOR] {
        let mut wrong = original.clone();
        wrong.headers[0] = Header::actor(actor).unwrap();
        assert!(decode_add_replica(&wrong).is_err());
    }
    for n in 0..original.body.len() {
        let mut short = original.clone();
        short.body.truncate(n);
        assert!(decode_add_replica(&short).is_err());
    }
    for error in [0, -2_147_017_398, i64::MIN, i64::MAX] {
        let mut reply = support::rich_reply(error);
        reply.replica.current_role = 2;
        reply.replica.state = 2;
        for owner in [Owner::Fm, Owner::Fmm] {
            let message = add_replica_reply_message(&reply, owner).unwrap();
            assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
            assert_eq!(
                decode_add_replica_reply(&message).unwrap(),
                (reply.clone(), owner)
            );
            assert!(decode_add_primary_reply(&message).is_err());
            assert!(decode_create_replica_reply(&message).is_err());
        }
    }
}

#[test]
fn peer_creation_preserves_native_scopes_deactivation_and_reply_values() {
    let mut base = support::rich_request();
    base.replica.current_role = 2;
    base.replica.previous_role = 1;
    base.replica.state = 1;
    base.service.stateful = true;
    base.service.persisted = false;
    for (epoch, catchup_lsn) in [
        (Epoch::default(), -1),
        (base.unit.current, 0),
        (
            Epoch {
                configuration: i64::MIN + 17,
                data_loss: i64::MAX - 29,
            },
            i64::MAX,
        ),
    ] {
        let info = ReplicaDeactivationInfo { epoch, catchup_lsn };
        let request = CreateReplica::new(base.clone(), info.clone());
        assert_eq!(request.replica.0, base);
        assert_eq!(request.deactivation, info);
        for fmm in [false, true] {
            let generation = support::rich_generation(fmm);
            let message = create_replica_message(&request, &generation).unwrap();
            assert_eq!(message.properties().unwrap().actor, RA_ACTOR);
            assert!(message.properties().unwrap().high_priority);
            assert_eq!(
                decode_create_replica(&message).unwrap(),
                (request.clone(), generation)
            );
            assert!(decode_add_primary(&message).is_err());
            assert!(decode_delete_replica(&message).is_err());
            assert_eq!(from_slice::<AddInstance>(&message.body).unwrap(), base);
        }
    }
    for error in [0, -2_147_017_398, -2_147_017_441, i64::MIN, i64::MAX] {
        let mut reply = support::rich_reply(error);
        reply.replica.current_role = 2;
        reply.replica.previous_role = 1;
        reply.replica.state = 1;
        reply.replica.replication_endpoint = "127.0.0.2:45678/idle-π".into();
        let generation = support::rich_generation(false);
        let message = create_replica_reply_message(&reply, &generation).unwrap();
        assert_eq!(
            decode_create_replica_reply(&message).unwrap(),
            (reply, generation)
        );
        assert!(decode_add_primary_reply(&message).is_err());
        assert!(decode_delete_replica_reply(&message).is_err());
    }
}

#[test]
fn peer_creation_rejects_bad_headers_shapes_and_every_proper_prefix() {
    use fube_transport::serialization::serde::Inline;
    use fube_transport::{message::Message, Result};
    type Decoder = fn(&Message) -> Result<()>;
    let base = support::rich_request();
    let info = ReplicaDeactivationInfo {
        epoch: base.unit.current,
        catchup_lsn: -0x1234,
    };
    let request = CreateReplica::new(base.clone(), info.clone());
    let generation = support::rich_generation(false);
    let contracts: [(Message, Decoder); 2] = [
        (
            create_replica_message(&request, &generation).unwrap(),
            |m| decode_create_replica(m).map(|_| ()),
        ),
        (
            create_replica_reply_message(&support::rich_reply(0), &generation).unwrap(),
            |m| decode_create_replica_reply(m).map(|_| ()),
        ),
    ];
    for (original, decode) in &contracts {
        for id in [ACTOR, ACTION, HIGH_PRIORITY, GENERATION_HEADER] {
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
        for actor in [FM_ACTOR, FMM_ACTOR] {
            let mut wrong = original.clone();
            wrong.headers[0] = Header::actor(actor).unwrap();
            assert!(decode(&wrong).is_err());
        }
        for action in [
            "AddInstance",
            "AddPrimary",
            "AddReplica",
            "GetLSN",
            "Deactivate",
            "Activate",
            "DeleteReplica",
        ] {
            let mut wrong = original.clone();
            wrong.headers[1] = Header::action(action).unwrap();
            assert!(decode(&wrong).is_err());
        }
        for (other, _) in &contracts {
            if other.properties().unwrap().action != original.properties().unwrap().action {
                let mut wrong = original.clone();
                wrong.headers[1] = other.headers[1].clone();
                assert!(decode(&wrong).is_err());
            }
        }
        for end in 0..original.body.len() {
            let mut truncated = original.clone();
            truncated.body.truncate(end);
            assert!(
                decode(&truncated).is_err(),
                "accepted proper prefix at {end}"
            );
        }
        let mut trailing = original.clone();
        trailing.body.push(0);
        assert!(decode(&trailing).is_err());
        // Priority is a factory default, not semantic admission.
        let mut low = original.clone();
        low.headers[2] = Header::boolean(HIGH_PRIORITY, false).unwrap();
        assert!(decode(&low).is_ok());
    }
    #[derive(Serialize)]
    struct Flattened {
        replica: Inline<AddInstance>,
        info: ReplicaDeactivationInfo,
    }
    #[derive(Serialize)]
    struct Nested {
        replica: AddInstance,
        info: ReplicaDeactivationInfo,
    }
    let mut malformed = contracts[0].0.clone();
    for body in [
        to_vec(&base).unwrap(),
        to_vec(&Flattened {
            replica: Inline(base.clone()),
            info: info.clone(),
        })
        .unwrap(),
        to_vec(&Nested {
            replica: base,
            info,
        })
        .unwrap(),
        to_vec(&DeleteReplica::default()).unwrap(),
    ] {
        malformed.body = body;
        assert!(decode_create_replica(&malformed).is_err());
    }
}

#[test]
fn generation_handshake_preserves_bodyless_proposal_correlated_reply_and_epoch_order() {
    let proposed = GenerationNumber {
        generation: i64::MAX,
        owner: generation_owner(0x8000_0000_0000_0001, u64::MAX),
    };
    let current = GenerationNumber {
        generation: i64::MAX,
        owner: generation_owner(0x8000_0000_0000_0002, 0),
    };
    assert!(proposed.older_than(&current));
    assert!(!current.older_than(&proposed));
    for fmm in [false, true] {
        let generation = GenerationHeader {
            number: proposed.clone(),
            for_fm_replica: fmm,
        };
        let proposal = generation_proposal_message(&generation).unwrap();
        assert!(proposal.body.is_empty());
        assert_eq!(decode_generation_proposal(&proposal).unwrap(), generation);
        for error in [0, -2_147_017_398, i64::MIN, i64::MAX] {
            let reply = GenerationProposalReply {
                proposed: proposed.clone(),
                current: current.clone(),
                error,
            };
            let message = generation_proposal_reply_message(&reply).unwrap();
            assert_eq!(message.properties().unwrap().actor, FM_ACTOR);
            assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
            assert_eq!(decode_generation_proposal_reply(&message).unwrap(), reply);
        }
    }
    let generation = GenerationHeader {
        number: proposed,
        for_fm_replica: false,
    };
    let update = GenerationUpdate {
        fm_service_epoch: Epoch {
            configuration: -0x1234,
            data_loss: 0x5678,
        },
    };
    let message = generation_update_message(&update, &generation).unwrap();
    assert_eq!(
        decode_generation_update(&message).unwrap(),
        (update.clone(), generation.clone())
    );
    #[derive(Serialize)]
    struct NativeEpoch {
        configuration: i64,
        data_loss: i64,
    }
    #[derive(Serialize)]
    struct NativeUpdate {
        epoch: NativeEpoch,
    }
    assert_eq!(
        message.body,
        to_vec(&NativeUpdate {
            epoch: NativeEpoch {
                configuration: -0x1234,
                data_loss: 0x5678,
            },
        })
        .unwrap()
    );
    let reject = generation_update_reject_message(&current).unwrap();
    assert_eq!(reject.body, to_vec(&current).unwrap());
    assert_eq!(decode_generation_update_reject(&reject).unwrap(), current);
    let mut fmm = generation;
    fmm.for_fm_replica = true;
    assert!(generation_update_message(&update, &fmm).is_err());
    let mut unsupported = message;
    unsupported.headers.retain(|h| h.id != GENERATION_HEADER);
    unsupported.headers.push(fmm.header().unwrap());
    assert!(decode_generation_update(&unsupported).is_err());
    unsupported.body.clear();
    assert!(decode_generation_update(&unsupported).is_err());
}

#[test]
fn generation_handshake_rejects_bad_headers_shapes_truncation_and_fmm_update() {
    use fube_transport::{message::Message, Result};
    type Decoder = fn(&Message) -> Result<()>;
    let generation = support::rich_generation(false);
    let reply = GenerationProposalReply {
        proposed: generation.number.clone(),
        current: GenerationNumber::default(),
        error: -2_147_017_398,
    };
    let update = GenerationUpdate {
        fm_service_epoch: Epoch {
            configuration: i64::MIN,
            data_loss: i64::MAX,
        },
    };
    let contracts: Vec<(Message, bool, Decoder)> = vec![
        (
            generation_proposal_message(&generation).unwrap(),
            true,
            |m| decode_generation_proposal(m).map(|_| ()),
        ),
        (
            generation_proposal_reply_message(&reply).unwrap(),
            false,
            |m| decode_generation_proposal_reply(m).map(|_| ()),
        ),
        (
            generation_update_message(&update, &generation).unwrap(),
            true,
            |m| decode_generation_update(m).map(|_| ()),
        ),
        (
            generation_update_reject_message(&generation.number).unwrap(),
            false,
            |m| decode_generation_update_reject(m).map(|_| ()),
        ),
    ];
    for (original, with_generation, decode) in &contracts {
        let properties = original.properties().unwrap();
        assert!(properties.high_priority);
        assert_eq!(
            properties.actor,
            if *with_generation { RA_ACTOR } else { FM_ACTOR }
        );
        for id in [ACTOR, ACTION, HIGH_PRIORITY]
            .into_iter()
            .chain(with_generation.then_some(GENERATION_HEADER))
        {
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
        let mut wrong = original.clone();
        wrong.headers[0] = Header::actor(FMM_ACTOR).unwrap();
        assert!(decode(&wrong).is_err());
        for (other, _, _) in &contracts {
            if other.properties().unwrap().action != properties.action {
                let mut wrong = original.clone();
                wrong.headers[1] = other.headers[1].clone();
                assert!(decode(&wrong).is_err());
            }
        }
        if !with_generation {
            let mut unexpected = original.clone();
            unexpected.headers.push(generation.header().unwrap());
            assert!(decode(&unexpected).is_err());
        }
        for end in 0..original.body.len() {
            let mut truncated = original.clone();
            truncated.body.truncate(end);
            assert!(decode(&truncated).is_err());
        }
        let mut trailing = original.clone();
        trailing.body.push(0);
        assert!(decode(&trailing).is_err());
        let mut shape = original.clone();
        shape.body = to_vec(&generation).unwrap();
        assert!(decode(&shape).is_err());
        // Priority remains a factory default, not semantic negotiation success.
        let mut low = original.clone();
        low.headers[2] = Header::boolean(HIGH_PRIORITY, false).unwrap();
        assert!(decode(&low).is_ok());
    }
    let mut proposal = generation_proposal_message(&generation).unwrap();
    #[derive(Serialize)]
    struct Empty {}
    proposal.body = to_vec(&Empty {}).unwrap();
    assert!(decode_generation_proposal(&proposal).is_err());
    let mut missing_body = generation_update_message(&update, &generation).unwrap();
    missing_body.body.clear();
    assert!(decode_generation_update(&missing_body).is_err());
}

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
fn add_primary_has_distinct_actions_and_native_request_reply_asymmetry() {
    let body = support::rich_request();
    let generation = support::rich_generation(false);
    let message = add_primary_message(&body, &generation).unwrap();
    assert_eq!(decode_add_primary(&message).unwrap(), (body, generation));
    assert!(decode_add_instance(&message).is_err());
    for id in [ACTOR, ACTION, HIGH_PRIORITY, GENERATION_HEADER] {
        let mut missing = message.clone();
        missing.headers.retain(|header| header.id != id);
        assert!(decode_add_primary(&missing).is_err());
    }
    for owner in [Owner::Fm, Owner::Fmm] {
        let body = support::rich_reply(-2_147_017_398);
        let message = add_primary_reply_message(&body, owner).unwrap();
        assert!(!message
            .headers
            .iter()
            .any(|header| header.id == GENERATION_HEADER));
        assert_eq!(decode_add_primary_reply(&message).unwrap(), (body, owner));
        assert!(decode_add_instance_reply(&message).is_err());
    }
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
fn delete_replica_preserves_inherited_body_and_distinct_factory_owner_asymmetry() {
    let mut primary = support::rich_request();
    primary.service.stateful = true;
    primary.service.persisted = false;
    primary.unit.target = 1;
    primary.unit.minimum = 1;
    primary.service.target = 1;
    primary.service.minimum = 1;
    primary.replica.current_role = 4;
    primary.replica.state = 2;
    let body = DeleteReplica::new(primary, false);
    for from_fmm in [false, true] {
        let generation = support::rich_generation(from_fmm);
        let message = delete_replica_message(&body, &generation).unwrap();
        let properties = message.properties().unwrap();
        assert_eq!(properties.actor, RA_ACTOR);
        assert_eq!(properties.action, "DeleteReplica");
        assert!(properties.high_priority);
        assert_eq!(
            decode_delete_replica(&message).unwrap(),
            (body.clone(), generation)
        );
        assert!(decode_remove_instance(&message).is_err());
        assert!(decode_add_primary(&message).is_err());
        assert_eq!(
            message.body,
            remove_instance_message(&body, &GenerationHeader::default())
                .unwrap()
                .body
        );
    }
    for error in [0, -2_147_017_398] {
        let mut reply = support::rich_reply(error);
        reply.replica = Replica {
            node: reply.replica.node,
            id: reply.replica.id,
            instance: reply.replica.instance,
            state: 3,
            up: false,
            ..Replica::default()
        };
        for owner in [Owner::Fm, Owner::Fmm] {
            let message = delete_replica_reply_message(&reply, owner).unwrap();
            let properties = message.properties().unwrap();
            assert_eq!(
                properties.actor,
                if owner == Owner::Fm {
                    FM_ACTOR
                } else {
                    FMM_ACTOR
                }
            );
            assert_eq!(properties.action, "DeleteReplicaReply");
            assert!(properties.high_priority);
            assert!(!message.headers.iter().any(|h| h.id == GENERATION_HEADER));
            assert_eq!(
                decode_delete_replica_reply(&message).unwrap(),
                (reply.clone(), owner)
            );
            assert!(decode_remove_instance_reply(&message).is_err());
            assert!(decode_add_primary_reply(&message).is_err());
        }
    }
}

#[test]
fn delete_replica_decoders_reject_invalid_headers_and_body_framing() {
    let request = delete_replica_message(
        &support::rich_remove(false),
        &support::rich_generation(false),
    )
    .unwrap();
    let reply = delete_replica_reply_message(&support::rich_reply(0), Owner::Fm).unwrap();
    for (original, is_reply) in [(request, false), (reply, true)] {
        let decode = |message: &fube_transport::message::Message| {
            if is_reply {
                decode_delete_replica_reply(message).map(|_| ())
            } else {
                decode_delete_replica(message).map(|_| ())
            }
        };
        let mut required = vec![ACTOR, ACTION, HIGH_PRIORITY];
        if !is_reply {
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
        let mut wrong = original.clone();
        wrong.headers[0] = Header::actor(if is_reply { RA_ACTOR } else { FM_ACTOR }).unwrap();
        assert!(decode(&wrong).is_err());
        wrong.headers[0] = original.headers[0].clone();
        for action in [
            "RemoveInstance",
            "RemoveInstanceReply",
            "AddPrimary",
            "AddPrimaryReply",
        ] {
            wrong.headers[1] = Header::action(action).unwrap();
            assert!(decode(&wrong).is_err());
        }
        for end in 0..original.body.len() {
            let mut truncated = original.clone();
            truncated.body.truncate(end);
            assert!(decode(&truncated).is_err());
        }
        let mut trailing = original.clone();
        trailing.body.push(0);
        assert!(decode(&trailing).is_err());
        let mut bad_size = original.clone();
        bad_size.body[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&bad_size).is_err());
        // Priority is factory metadata, not semantic deletion admission.
        let mut low = original;
        low.headers[2] = Header::boolean(HIGH_PRIORITY, false).unwrap();
        assert!(decode(&low).is_ok());
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

#[test]
fn node_control_sequences_owners_and_full_service_descriptions_are_lossless() {
    for from_fmm in [false, true] {
        let generation = support::rich_generation(from_fmm);
        let owner = if from_fmm { Owner::Fmm } else { Owner::Fm };
        for sequence in [0, -1, i64::MIN + 17, i64::MAX] {
            let body = NodeActivationRequest { sequence, from_fmm };
            let activation = node_activate_request_message(&body, &generation).unwrap();
            let deactivation = node_deactivate_request_message(&body, &generation).unwrap();
            assert_eq!(
                decode_node_activate_request(&activation).unwrap(),
                (body.clone(), generation.clone())
            );
            assert_eq!(
                decode_node_deactivate_request(&deactivation).unwrap(),
                (body, generation.clone())
            );
            assert!(decode_node_activate_request(&deactivation).is_err());
            assert!(decode_node_deactivate_request(&activation).is_err());
            let body = NodeActivationReply { sequence };
            let activation = node_activate_reply_message(&body, owner).unwrap();
            let deactivation = node_deactivate_reply_message(&body, owner).unwrap();
            assert_eq!(
                decode_node_activate_reply(&activation).unwrap(),
                (body.clone(), owner)
            );
            assert_eq!(
                decode_node_deactivate_reply(&deactivation).unwrap(),
                (body, owner)
            );
            assert!(decode_node_activate_reply(&deactivation).is_err());
        }
        let body = NodeUpdateServiceRequest {
            service: support::rich_request().service,
        };
        let message = node_update_service_request_message(&body, &generation).unwrap();
        assert_eq!(
            decode_node_update_service_request(&message).unwrap(),
            (body.clone(), generation)
        );
        let reply = NodeUpdateServiceReply {
            service: body.service.name,
            instance: u64::MAX,
            version: 0x8000_0000_0000_0001,
        };
        let message = node_update_service_reply_message(&reply, owner).unwrap();
        assert_eq!(
            decode_node_update_service_reply(&message).unwrap(),
            (reply, owner)
        );
    }
}

#[test]
fn service_type_status_and_endpoint_actions_do_not_alias_shared_schemas() {
    use fube_transport::serialization::serde::ObjectArray;
    let types = support::rich_notification()
        .infos
        .0
        .into_iter()
        .map(|info| info.versioned.id)
        .collect();
    let body = ServiceTypeUpdate {
        types: ObjectArray(types),
        sequence: u64::MAX,
    };
    let enabled = service_type_enabled_message(&body).unwrap();
    let disabled = service_type_disabled_message(&body).unwrap();
    assert_eq!(decode_service_type_enabled(&enabled).unwrap(), body);
    assert_eq!(decode_service_type_disabled(&disabled).unwrap(), body);
    assert!(decode_service_type_disabled(&enabled).is_err());
    assert!(decode_service_type_enabled(&disabled).is_err());
    assert!(decode_service_type_notification(&enabled).is_err());
    let reply = ServiceTypeUpdateReply {
        sequence: body.sequence,
    };
    for fmm in [false, true] {
        let generation = support::rich_generation(fmm);
        let enabled = service_type_enabled_reply_message(&reply, &generation).unwrap();
        let disabled = service_type_disabled_reply_message(&reply, &generation).unwrap();
        assert_eq!(
            decode_service_type_enabled_reply(&enabled).unwrap(),
            (reply.clone(), generation.clone())
        );
        assert_eq!(
            decode_service_type_disabled_reply(&disabled).unwrap(),
            (reply.clone(), generation.clone())
        );
        assert!(decode_service_type_disabled_reply(&enabled).is_err());
        let body = support::rich_reply(-2_147_017_398);
        let endpoint = replica_endpoint_updated_reply_message(&body, &generation).unwrap();
        assert_eq!(
            decode_replica_endpoint_updated_reply(&endpoint).unwrap(),
            (body, generation)
        );
        assert!(decode_add_instance_reply(&endpoint).is_err());
    }
    for owner in [Owner::Fm, Owner::Fmm] {
        let body = support::rich_request();
        let endpoint = replica_endpoint_updated_message(&body, owner).unwrap();
        assert_eq!(
            decode_replica_endpoint_updated(&endpoint).unwrap(),
            (body, owner)
        );
        assert!(decode_add_instance(&endpoint).is_err());
    }
}

#[test]
fn stage_two_factory_defaults_and_malformed_headers_match_native_asymmetry() {
    use fube_transport::{message::Message, Result};
    type Decoder = fn(&Message) -> Result<()>;
    let generation = support::rich_generation(false);
    let request = NodeActivationRequest {
        sequence: -9,
        from_fmm: false,
    };
    let reply = NodeActivationReply { sequence: -9 };
    let service = NodeUpdateServiceRequest {
        service: support::rich_request().service,
    };
    let updated = NodeUpdateServiceReply {
        service: "fabric:/π🦀".into(),
        instance: u64::MAX,
        version: 9,
    };
    let status = ServiceTypeUpdate::default();
    let status_reply = ServiceTypeUpdateReply { sequence: u64::MAX };
    let contracts: Vec<(&str, Message, bool, Decoder)> = vec![
        (
            "NodeActivateRequest",
            node_activate_request_message(&request, &generation).unwrap(),
            true,
            |m| decode_node_activate_request(m).map(|_| ()),
        ),
        (
            "NodeDeactivateRequest",
            node_deactivate_request_message(&request, &generation).unwrap(),
            true,
            |m| decode_node_deactivate_request(m).map(|_| ()),
        ),
        (
            "NodeActivateReply",
            node_activate_reply_message(&reply, Owner::Fm).unwrap(),
            false,
            |m| decode_node_activate_reply(m).map(|_| ()),
        ),
        (
            "NodeDeactivateReply",
            node_deactivate_reply_message(&reply, Owner::Fm).unwrap(),
            false,
            |m| decode_node_deactivate_reply(m).map(|_| ()),
        ),
        (
            "NodeUpdateServiceRequest",
            node_update_service_request_message(&service, &generation).unwrap(),
            true,
            |m| decode_node_update_service_request(m).map(|_| ()),
        ),
        (
            "NodeUpdateServiceReply",
            node_update_service_reply_message(&updated, Owner::Fm).unwrap(),
            false,
            |m| decode_node_update_service_reply(m).map(|_| ()),
        ),
        (
            "ServiceTypeEnabled",
            service_type_enabled_message(&status).unwrap(),
            false,
            |m| decode_service_type_enabled(m).map(|_| ()),
        ),
        (
            "ServiceTypeDisabled",
            service_type_disabled_message(&status).unwrap(),
            false,
            |m| decode_service_type_disabled(m).map(|_| ()),
        ),
        (
            "ServiceTypeEnabledReply",
            service_type_enabled_reply_message(&status_reply, &generation).unwrap(),
            true,
            |m| decode_service_type_enabled_reply(m).map(|_| ()),
        ),
        (
            "ServiceTypeDisabledReply",
            service_type_disabled_reply_message(&status_reply, &generation).unwrap(),
            true,
            |m| decode_service_type_disabled_reply(m).map(|_| ()),
        ),
        (
            "ReplicaEndpointUpdated",
            replica_endpoint_updated_message(&support::rich_request(), Owner::Fm).unwrap(),
            false,
            |m| decode_replica_endpoint_updated(m).map(|_| ()),
        ),
        (
            "ReplicaEndpointUpdatedReply",
            replica_endpoint_updated_reply_message(&support::rich_reply(0), &generation).unwrap(),
            true,
            |m| decode_replica_endpoint_updated_reply(m).map(|_| ()),
        ),
        (
            "ReportLoad",
            report_load_message(&ReportLoad::default()).unwrap(),
            false,
            |m| decode_report_load(m).map(|_| ()),
        ),
    ];
    for (action, original, incoming, decode) in contracts {
        let properties = original.properties().unwrap();
        assert_eq!(properties.action, action);
        assert_eq!(properties.actor, if incoming { RA_ACTOR } else { FM_ACTOR });
        assert!(
            !properties.high_priority,
            "{action}: native ordinary factory default"
        );
        assert_eq!(
            original.headers.iter().any(|h| h.id == GENERATION_HEADER),
            incoming
        );
        assert!(decode(&original).is_ok());
        let mut required = vec![ACTOR, ACTION, HIGH_PRIORITY];
        if incoming {
            required.push(GENERATION_HEADER);
        }
        for id in required {
            let mut missing = original.clone();
            missing.headers.retain(|h| h.id != id);
            assert!(decode(&missing).is_err(), "{action}: missing {id}");
            let mut duplicate = original.clone();
            duplicate.headers.push(
                original
                    .headers
                    .iter()
                    .find(|h| h.id == id)
                    .unwrap()
                    .clone(),
            );
            assert!(decode(&duplicate).is_err(), "{action}: duplicate {id}");
            let mut malformed = original.clone();
            malformed
                .headers
                .iter_mut()
                .find(|h| h.id == id)
                .unwrap()
                .value
                .clear();
            assert!(decode(&malformed).is_err(), "{action}: malformed {id}");
        }
        let mut wrong = original.clone();
        wrong.headers[0] = Header::actor(if incoming { FM_ACTOR } else { RA_ACTOR }).unwrap();
        assert!(decode(&wrong).is_err(), "{action}: wrong actor");
        let mut wrong = original.clone();
        wrong.headers[1] = Header::action("ServiceBusy").unwrap();
        assert!(decode(&wrong).is_err(), "{action}: wrong action");
        let mut high = original.clone();
        high.headers[2] = Header::boolean(HIGH_PRIORITY, true).unwrap();
        assert!(
            decode(&high).is_ok(),
            "{action}: priority is metadata, not semantic admission"
        );
        for end in 0..original.body.len() {
            let mut truncated = original.clone();
            truncated.body.truncate(end);
            assert!(decode(&truncated).is_err(), "{action}: proper prefix {end}");
        }
        let mut trailing = original;
        trailing.body.push(0);
        assert!(decode(&trailing).is_err(), "{action}: trailing data");
    }
}

#[test]
fn complete_load_legacy_and_modern_map_scopes_roundtrip_and_reject_bad_node_ids() {
    use fube_transport::serialization::serde::{Guid, ObjectArray};
    let stats = LoadMetricStats {
        name: "cpuπ🦀".into(),
        value: u32::MAX,
        count: 17,
        weighted_sum: -123.5,
        weight: 0.125,
        time: i64::MIN + 7,
    };
    let legacy = ObjectArray(vec![LegacyLoadReport {
        unit: PartitionId {
            guid: Guid([0xa5; 16]),
        },
        role: 4,
        metrics: ObjectArray(vec![LoadMetricValue {
            name: "legacy".into(),
            value: u32::MAX,
        }]),
    }]);
    let reports = ObjectArray(vec![LoadDescription {
        partition: Guid([0x5a; 16]),
        service: "fabric:/load".into(),
        stateful: true,
        primary: ObjectArray(vec![stats.clone()]),
        secondary: ObjectArray(vec![stats.clone(), LoadMetricStats::default()]),
        nodes: ObjectArray(vec![NodeLoads {
            node: [0x80; 16],
            loads: ObjectArray(vec![stats.clone()]),
        }]),
    }]);
    for body in [
        ReportLoad::default(),
        ReportLoad {
            legacy: legacy.clone(),
            reports: ObjectArray(vec![]),
            time: -1,
        },
        ReportLoad {
            legacy: ObjectArray(vec![]),
            reports: reports.clone(),
            time: i64::MAX,
        },
        ReportLoad {
            legacy,
            reports,
            time: i64::MIN,
        },
    ] {
        let message = report_load_message(&body).unwrap();
        assert_eq!(decode_report_load(&message).unwrap(), body);
        for end in 0..message.body.len() {
            assert!(from_slice::<ReportLoad>(&message.body[..end]).is_err());
        }
    }
    #[derive(Serialize)]
    struct BadNodeLoads {
        node: ByteArray,
        loads: ObjectArray<Vec<LoadMetricStats>>,
    }
    for len in [0, 15, 17] {
        let bytes = to_vec(&BadNodeLoads {
            node: ByteArray(vec![0; len]),
            loads: ObjectArray(vec![stats.clone()]),
        })
        .unwrap();
        assert!(from_slice::<NodeLoads>(&bytes).is_err());
    }
    let mut overflow = ReportLoad::default();
    overflow.legacy.0 = vec![LegacyLoadReport::default(); 4097];
    assert!(report_load_message(&overflow).is_err());
}
