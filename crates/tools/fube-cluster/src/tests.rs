use super::*;
use crate::fm::{Desired, IdleDeleteTarget, Observed};
use fube_transport::serialization::serde::ObjectArray;
use std::{
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::PermissionsExt,
};

#[test]
fn generation_round_retries_restarts_and_stops_before_upload_completion() {
    use fm::{GenerationHandshake, GenerationPhase, GenerationWork};
    let initial = launches()[0].generation.number.clone();
    let mut round = GenerationHandshake::new(&initial).unwrap();
    let first = round.number.clone();
    let now = Instant::now();
    assert!(matches!(
        round.next(0, now),
        Some(GenerationWork::Propose(_))
    ));
    assert!(round.next(0, now).is_none());
    round
        .proposal_reply(
            0,
            &GenerationProposalReply {
                proposed: first.clone(),
                current: first.clone(),
                error: -1,
            },
        )
        .unwrap();
    assert!(matches!(
        round.next(0, now + Duration::from_secs(5)),
        Some(GenerationWork::Propose(header)) if header.number == first
    ));
    let mut higher = first.clone();
    higher.generation += 3;
    round
        .proposal_reply(
            0,
            &GenerationProposalReply {
                proposed: first.clone(),
                current: higher.clone(),
                error: 0,
            },
        )
        .unwrap();
    assert_eq!(round.number.generation, higher.generation + 1);
    let current = round.number.clone();
    round
        .proposal_reply(
            1,
            &GenerationProposalReply {
                proposed: first.clone(),
                current: first,
                error: 0,
            },
        )
        .unwrap();
    assert_eq!(round.phase, GenerationPhase::Proposing);
    for index in 0..2 {
        round
            .proposal_reply(
                index,
                &GenerationProposalReply {
                    proposed: current.clone(),
                    current: current.clone(),
                    error: 0,
                },
            )
            .unwrap();
    }
    assert_eq!(round.phase, GenerationPhase::AwaitingUpload);
    let now = Instant::now();
    for index in 0..2 {
        assert!(matches!(
            round.next(index, now),
            Some(GenerationWork::Update(header)) if header.number == current
        ));
        assert!(round.next(index, now + Duration::from_secs(4)).is_none());
        assert!(matches!(
            round.next(index, now + Duration::from_secs(5)),
            Some(GenerationWork::Update(_))
        ));
        assert!(round.next(index, now + Duration::from_secs(14)).is_none());
        assert!(round.next(index, now + Duration::from_secs(15)).is_some());
    }
    round.reject(&initial).unwrap();
    assert_eq!(round.number, current);
    round.reject(&current).unwrap();
    assert_eq!(round.number.generation, current.generation + 1);
    assert_eq!(round.phase, GenerationPhase::Proposing);
    assert_eq!(round.updates_sent, [false; 2]);
    let mut exhausted = round.number.clone();
    exhausted.generation = i64::MAX;
    let before = round.number.clone();
    assert!(round.reject(&exhausted).is_err());
    assert_eq!(round.number, before);
}

#[test]
fn generation_rebuild_gates_mutations_without_relabeling_existing_serving_state() {
    let mut ledger = ledger();
    ledger.accept(command("a", "create", Some(1))).unwrap();
    assert!(ledger.begin_generation().is_err());
    ledger
        .reply(0, ready_reply(&ledger.placements[0]), false)
        .unwrap();
    let prior = ledger.result("a");
    ledger.begin_generation().unwrap();
    assert_eq!(ledger.status()["generation_handshake"]["complete"], false);
    assert_eq!(ledger.placements[0].observed, Observed::Running);
    for action in ["create", "delete", "activate", "deactivate"] {
        assert!(ledger.accept(command(action, action, Some(2))).is_err());
    }
    assert!(matches!(
        ledger.accept(command("a", "create", Some(1))).unwrap(),
        Accepted::Duplicate(outcome) if outcome == prior
    ));
    assert!(ledger.begin_generation().is_err());
}

fn launches() -> [Launch; 2] {
    [1, 2].map(|placement| Launch {
        profile: ServiceProfile::Stateless,
        deployment: "00000000-0000-4000-8000-000000000001".into(),
        session: format!("00000000-0000-4000-8000-00000000000{}", placement + 1),
        capability: uuid::Uuid::new_v4().to_string(),
        parent_pid: std::process::id(),
        node: NodeInstance {
            id: [placement; 16],
            instance: 100 + u64::from(placement),
        },
        fm: NodeInstance {
            id: [9; 16],
            instance: 99,
        },
        generation: GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        },
        placement,
        directory: PathBuf::from(format!("/owned/node{placement}")),
        package: PathBuf::from("/owned/package"),
        partition: format!("00000000-0000-4000-8000-00000000001{placement}"),
        replica: i64::from(placement),
        incarnation: 800 + i64::from(placement),
        peer: None,
    })
}
fn profile(launch: &Launch) -> ActualProfile {
    ActualProfile {
        service: SERVICE.into(),
        service_type: fixed_type(),
        version: VersionInstance {
            instance: 12345,
            ..VersionInstance::default()
        },
        code_package: "Code".into(),
        app_pid: 123,
        runtime_root: launch.directory.join("hosting/actual-activation"),
    }
}
fn ledger() -> Ledger {
    let mut ledger = Ledger::new(launches());
    for placement in &mut ledger.placements {
        placement.bootstrap = Bootstrap::Metadata;
        placement.admit_profile(profile(&placement.launch)).unwrap();
    }
    ledger
}
fn stateful_ledger() -> Ledger {
    let mut launches = launches();
    for launch in &mut launches {
        launch.profile = ServiceProfile::StatefulSingleton;
    }
    let mut ledger = Ledger::new(launches);
    for placement in &mut ledger.placements {
        placement.bootstrap = Bootstrap::Metadata;
        let mut actual = profile(&placement.launch);
        actual.service = placement.launch.profile.service().into();
        actual.service_type = fixed_type_for(placement.launch.profile);
        placement.admit_profile(actual).unwrap();
    }
    ledger
}
fn peer_idle_launches() -> [Launch; 2] {
    let mut launches = launches();
    launches[1].partition = launches[0].partition.clone();
    for index in 0..2 {
        let remote = &launches[1 - index];
        launches[index].peer = Some(Peer {
            node: remote.node.clone(),
            session: remote.session.clone(),
            partition: remote.partition.clone(),
            replica: remote.replica,
            incarnation: remote.incarnation,
        });
        launches[index].profile = ServiceProfile::StatefulPeerIdle;
    }
    launches
}

fn peer_idle_ledger() -> Ledger {
    let mut ledger = Ledger::new(peer_idle_launches());
    for placement in &mut ledger.placements {
        placement.bootstrap = Bootstrap::Metadata;
        let mut actual = profile(&placement.launch);
        actual.service = placement.launch.profile.service().into();
        actual.service_type = fixed_type_for(placement.launch.profile);
        placement.admit_profile(actual).unwrap();
    }
    ledger.accept(command("primary", "create", Some(1))).unwrap();
    ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
    ledger
}

#[test]
fn idle_destination_delete_joins_independent_reply_and_disposal_without_clearing_pending_remove() {
    for proof_first in [false, true] {
        for failed in [false, true] {
            let mut ledger = peer_idle_ledger();
            let now = Instant::now();
            assert!(ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).is_err());
            let add = ledger.request_idle(now).unwrap();
            if failed {
                ledger.idle_reply(0, AddReplicaReply { unit: add.unit.clone(),
                    replica: add.replica.clone(), error: -993 }).unwrap();
            }
            let removing = ledger.request_idle_removal(now).unwrap();
            let deleting = ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
            assert_eq!(deleting.replica.0, removing);
            assert!(!deleting.deletion.0.force);
            assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
            let absent = absence_receipt(&ledger.placements[1]).absence.body;
            ledger.reply(1, DeleteReplicaReply { unit: absent.unit,
                replica: absent.replica, error: 0 }, true).unwrap();
            assert_eq!(ledger.idle_deletion_status()["destination_deleted"], false);
            assert_eq!(ledger.request_idle_deletion(now, IdleDeleteTarget::Unobserved).unwrap(), deleting);
            assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
            assert!(ledger.request_idle(now).is_err());
            assert!(ledger.idle_deletion_retry(0, now + Duration::from_secs(30)).is_none());
            assert!(ledger.idle_deletion_retry(1, now + Duration::from_secs(10)).is_none());
            assert_eq!(ledger.idle_deletion_retry(1, now + Duration::from_secs(11)), Some(deleting.clone()));
            assert!(ledger.idle_deletion_retry(1, now + Duration::from_secs(61)).is_none());
            assert_eq!(ledger.idle_deletion_retry(1, now + Duration::from_secs(72)), Some(deleting));
            let reply = remove_reply(&ledger.placements[1]);
            for kind in 0..6 {
                let mut stale = reply.clone();
                match kind {
                    0 => stale.error = -1,
                    1 => stale.replica.instance -= 1,
                    2 => stale.replica.id += 1,
                    3 => stale.replica.node.instance += 1,
                    4 => stale.unit.current.configuration -= 1,
                    _ => stale.unit.id.guid.0[0] ^= 0xff,
                }
                ledger.reply(1, stale, true).unwrap();
            }
            ledger.idle_deletion_reply(0, reply.clone()).unwrap();
            let mut invalid = reply.clone();
            invalid.replica.current_role = 2;
            assert!(ledger.reply(1, invalid, true).is_err());
            assert_eq!(ledger.idle_deletion_status()["destination_deleted"], false);
            let proof = retirement(&ledger.placements[1]);
            let mut foreign = proof.clone();
            foreign.session = ledger.placements[0].launch.session.clone();
            assert!(ledger.retirement(1, foreign, 555).is_err());
            if proof_first {
                ledger.retirement(1, proof.clone(), 555).unwrap();
                assert_eq!(ledger.idle_deletion_status()["disposed"], true);
                assert_eq!(ledger.idle_deletion_status()["destination_deleted"], false);
            } else {
                ledger.reply(1, reply.clone(), true).unwrap();
                assert_eq!(ledger.idle_deletion_status()["destination_deleted"], true);
                assert_eq!(ledger.idle_deletion_status()["disposed"], false);
                assert!(ledger.idle_deletion_retry(1, now + Duration::from_secs(150)).is_none());
            }
            assert_eq!(ledger.idle_deletion_status()["state"], "pending");
            assert_eq!(ledger.idle_removal_status()["pending_remove"], true);
            ledger.retirement(1, proof, 555).unwrap();
            ledger.reply(1, reply, true).unwrap();
            assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
            assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
            assert_eq!(ledger.idle_removal_status()["destination_deleted"], true);
            assert!(!ledger.placements[1].reusable);
            assert!(ledger.accept(command("replacement", "create", Some(2))).is_err());
            ledger.idle_removal_reply(0, RemoveReplicaReply { unit: removing.unit,
                replica: removing.replica, error: 0 }).unwrap();
            assert_eq!(ledger.idle_deletion_status()["pending_remove"], false);
            assert_eq!(ledger.idle_status()["state"], if failed { "failed" } else { "cancelled" });
            assert_eq!(ledger.idle_status()["destination_cleanup_required"], false);
            assert_eq!(ledger.idle_status()["to_be_dropped"], false);
            if failed { assert_eq!(ledger.idle_status()["error"], -993); }
            ledger.lost(1, "later destination loss".into());
            assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
        }
    }
}

#[test]
fn idle_destination_delete_tracks_destination_owner_loss_independently_of_primary_removal() {
    for lost in [0, 1] {
        let mut ledger = peer_idle_ledger();
        let now = Instant::now();
        ledger.request_idle(now).unwrap();
        ledger.request_idle_removal(now).unwrap();
        ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
        ledger.lost(lost, "original session lost".into());
        ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
        ledger.retirement(1, retirement(&ledger.placements[1]), 555).unwrap();
        assert_eq!(ledger.idle_deletion_status()["state"], if lost == 1 { "unknown" } else { "deleted" });
        assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
        assert!(!ledger.placements[1].reusable);
    }
}

#[test]
fn absent_idle_deletion_joins_original_absence_and_native_reply_without_disposal_in_both_orders() {
    for proof_first in [false, true] {
        let mut ledger = peer_idle_ledger();
        let now = Instant::now();
        let add = ledger.request_idle(now).unwrap();
        let removing = ledger.request_idle_removal(now).unwrap();
        assert!(ledger.idle_absence_allowed());
        let deleting = ledger.request_idle_deletion(now, IdleDeleteTarget::Unobserved).unwrap();
        let proof = absence_receipt(&ledger.placements[1]);
        let reply = DeleteReplicaReply { unit: proof.absence.body.unit.clone(),
            replica: proof.absence.body.replica.clone(), error: 0 };
        assert_eq!((reply.unit.target, reply.unit.minimum), (0, 0));
        assert!(!ledger.placements[1].created);
        if proof_first {
            ledger.absence(1, proof.clone(), 555).unwrap();
            assert_eq!(ledger.idle_deletion_status()["never_created"], true);
            assert_eq!(ledger.idle_deletion_status()["destination_deleted"], false);
        } else {
            ledger.reply(1, reply.clone(), true).unwrap();
            assert_eq!(ledger.idle_deletion_status()["destination_deleted"], true);
            assert_eq!(ledger.idle_deletion_status()["never_created"], false);
        }
        assert_eq!(ledger.idle_deletion_status()["state"], "pending");
        assert_eq!(ledger.idle_deletion_status()["disposed"], false);
        assert_eq!(ledger.idle_deletion_status()["completion"], Value::Null);
        assert_eq!(ledger.idle_removal_status()["pending_remove"], true);
        ledger.absence(1, proof.clone(), 555).unwrap();
        ledger.reply(1, reply, true).unwrap();
        assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
        assert_eq!(ledger.idle_deletion_status()["completion"], "never_created");
        assert_eq!(ledger.idle_deletion_status()["disposed"], false);
        assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
        assert!(ledger.placements[1].retirement.is_none());
        assert!(!ledger.placements[1].created && !ledger.placements[1].reusable);
        assert_eq!(ledger.request_idle_removal(now).unwrap(), removing);
        assert_eq!(ledger.idle_deletion_retry(1, now + Duration::from_secs(150)), None);
        ledger.idle_removal_reply(0, RemoveReplicaReply { unit: removing.unit,
            replica: removing.replica, error: 0 }).unwrap();
        assert_eq!(ledger.idle_deletion_status()["pending_remove"], false);
        ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -993 }).unwrap();
        assert_eq!(ledger.idle_status()["state"], "cancelled");
        assert_eq!(ledger.idle_status()["destination_cleanup_required"], false);
        assert!(ledger.retirement(1, retirement(&ledger.placements[1]), 555).is_err());
        ledger.absence(1, proof, 555).unwrap();
        ledger.lost(1, "later session loss".into());
        assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
        assert!(!deleting.deletion.0.force);
    }
}

#[test]
fn absent_idle_cleanup_rejects_foreign_or_opened_proofs_and_retains_unknown_owner_loss() {
    let now = Instant::now();
    for lost in [None, Some(0), Some(1)] {
        let mut ledger = peer_idle_ledger();
        ledger.request_idle(now).unwrap();
        ledger.request_idle_removal(now).unwrap();
        ledger.request_idle_deletion(now, IdleDeleteTarget::Unobserved).unwrap();
        let proof = absence_receipt(&ledger.placements[1]);
        for kind in 0..10 {
            let mut foreign = proof.clone();
            match kind {
                0 => foreign.node.instance += 1,
                1 => foreign.session = ledger.placements[0].launch.session.clone(),
                2 => foreign.generation.number.generation += 1,
                3 => foreign.absence.body.replica.instance += 1,
                4 => foreign.absence.body.unit.current.configuration += 1,
                5 => foreign.absence.body.replica.version = ledger.placements[1].profile.as_ref().unwrap().version.clone(),
                6 => foreign.absence.command = 0,
                7 => foreign.absence.ra_operation = 0,
                8 => foreign.node_pid += 1,
                _ => foreign.app_pid += 1,
            }
            assert!(ledger.absence(1, foreign, 555).is_err());
            assert_eq!(ledger.idle_deletion_status()["never_created"], false);
        }
        if let Some(index) = lost { ledger.lost(index, "original owner loss".into()); }
        let reply = DeleteReplicaReply { unit: proof.absence.body.unit.clone(),
            replica: proof.absence.body.replica.clone(), error: 0 };
        ledger.absence(1, proof, 555).unwrap();
        ledger.reply(1, reply, true).unwrap();
        assert_eq!(ledger.idle_deletion_status()["state"], if lost == Some(1) { "unknown" } else { "deleted" });
        assert_eq!(ledger.idle_deletion_status()["disposed"], false);
    }
    for opened in [false, true] {
        let mut ledger = peer_idle_ledger();
        let add = ledger.request_idle(now).unwrap();
        let mut reply = AddReplicaReply { unit: add.unit, replica: add.replica, error: if opened { 0 } else { -993 } };
        reply.replica.state = 2;
        reply.replica.replication_endpoint = "127.0.0.1:23457".into();
        ledger.idle_reply(0, reply).unwrap();
        ledger.request_idle_removal(now).unwrap();
        assert!(!ledger.idle_absence_allowed());
        assert!(ledger.request_idle_deletion(now, IdleDeleteTarget::Unobserved).is_err());
        assert_eq!(ledger.idle_deletion_status()["state"], "absent");
        ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
        assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
        assert_eq!(ledger.idle_deletion_status()["never_created"], false);
    }
}

#[test]
fn failed_registration_delete_requires_matching_original_failure_and_independent_native_reply() {
    for proof_first in [false, true] {
        let mut ledger = peer_idle_ledger();
        let now = Instant::now();
        let add = ledger.request_idle(now).unwrap();
        assert!(ledger.request_idle_deletion(now, IdleDeleteTarget::FailedRegistration).is_err());
        ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -995 }).unwrap();
        let removal = ledger.request_idle_removal(now).unwrap();
        let deleting = ledger.request_idle_deletion(now, IdleDeleteTarget::FailedRegistration).unwrap();
        let proof = failed_registration_receipt(&ledger.placements[1], -995);
        let reply = remove_reply(&ledger.placements[1]);
        if proof_first { ledger.failed_registration(1, proof.clone(), 555).unwrap(); }
        else { ledger.reply(1, reply.clone(), true).unwrap(); }
        assert_eq!(ledger.idle_deletion_status()["complete"], false);
        assert_eq!(ledger.idle_deletion_status()["completion"], Value::Null);
        assert_eq!(ledger.idle_deletion_status()["disposed"], false);
        ledger.failed_registration(1, proof.clone(), 555).unwrap();
        ledger.reply(1, reply, true).unwrap();
        assert_eq!(ledger.idle_deletion_status()["completion"], "failed_registration");
        assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
        assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
        assert_eq!(ledger.idle_status()["state"], "failed");
        assert_eq!(ledger.idle_status()["error"], -995);
        assert_eq!(ledger.idle_status()["destination_cleanup_required"], false);
        assert!(!ledger.placements[1].created && !ledger.placements[1].reusable);
        assert!(ledger.retirement(1, retirement(&ledger.placements[1]), 555).is_err());
        assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
        assert!(ledger.pre_open_cancellation(1, pre_open_receipt(&ledger.placements[1]), 555).is_err());
        ledger.idle_removal_reply(0, RemoveReplicaReply { unit: removal.unit, replica: removal.replica, error: 0 }).unwrap();
        assert_eq!(ledger.idle_removal_status()["completion"], "failed_creation_retired");
        assert_eq!(ledger.idle_status()["error"], -995);
        assert_eq!(deleting.replica.0.replica.id, proof.deletion.failure.body.replica.id);
        ledger.lost(1, "later owner loss".into());
        assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
    }
}

#[test]
fn failed_registration_receipts_reject_foreign_unknown_and_other_cleanup_origins() {
    let now = Instant::now();
    for lost in [None, Some(0), Some(1)] {
        let mut ledger = peer_idle_ledger();
        let add = ledger.request_idle(now).unwrap();
        ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -995 }).unwrap();
        ledger.request_idle_deletion(now, IdleDeleteTarget::FailedRegistration).unwrap();
        let proof = failed_registration_receipt(&ledger.placements[1], -995);
        for kind in 0..10 {
            let mut foreign = proof.clone();
            match kind {
                0 => foreign.node.instance += 1,
                1 => foreign.session = ledger.placements[0].launch.session.clone(),
                2 => foreign.deletion.failure.error = -994,
                3 => foreign.deletion.failure.body.replica.instance += 1,
                4 => foreign.deletion.failure.body.unit.target = 0,
                5 => foreign.deletion.failure.creation = 0,
                6 => foreign.deletion.failure.ra_operation = foreign.deletion.ra_operation,
                7 => foreign.deletion.command = foreign.deletion.failure.command,
                8 => foreign.node_pid += 1,
                _ => foreign.app_pid += 1,
            }
            assert!(ledger.failed_registration(1, foreign, 555).is_err());
        }
        if let Some(index) = lost { ledger.lost(index, "original session loss".into()); }
        ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
        ledger.failed_registration(1, proof, 555).unwrap();
        assert_eq!(ledger.idle_deletion_status()["state"], if lost == Some(1) { "unknown" } else { "deleted" });
    }
    for target in [IdleDeleteTarget::Runtime, IdleDeleteTarget::PreRegistration, IdleDeleteTarget::Unobserved] {
        let mut ledger = peer_idle_ledger();
        ledger.request_idle(now).unwrap();
        ledger.request_idle_removal(now).unwrap();
        ledger.request_idle_deletion(now, target).unwrap();
        assert!(ledger.failed_registration(1, failed_registration_receipt(&ledger.placements[1], -995), 555).is_err());
    }
}

#[test]
fn pre_registration_delete_joins_distinct_cancellation_and_reply_in_both_orders() {
    for target in [IdleDeleteTarget::PreRegistration, IdleDeleteTarget::Unobserved] {
        for proof_first in [false, true] {
            let mut ledger = peer_idle_ledger();
            let now = Instant::now();
            ledger.request_idle(now).unwrap();
            ledger.request_idle_removal(now).unwrap();
            ledger.request_idle_deletion(now, target).unwrap();
            let proof = pre_open_receipt(&ledger.placements[1]);
            let reply = remove_reply(&ledger.placements[1]);
            if proof_first {
                ledger.pre_open_cancellation(1, proof.clone(), 555).unwrap();
            } else {
                ledger.reply(1, reply.clone(), true).unwrap();
            }
            assert_eq!(ledger.idle_deletion_status()["complete"], false);
            assert_eq!(ledger.idle_deletion_status()["completion"], Value::Null);
            ledger.pre_open_cancellation(1, proof.clone(), 555).unwrap();
            ledger.reply(1, reply, true).unwrap();
            assert_eq!(ledger.idle_deletion_status()["completion"], "pre_open_cancelled");
            assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
            assert_eq!(ledger.idle_deletion_status()["never_created"], false);
            assert_eq!(ledger.idle_deletion_status()["disposed"], false);
            assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
            assert!(!ledger.placements[1].created && !ledger.placements[1].reusable);
            assert!(ledger.placements[1].retirement.is_none());
            assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
            assert!(ledger.retirement(1, retirement(&ledger.placements[1]), 555).is_err());
            ledger.pre_open_cancellation(1, proof, 555).unwrap();
            ledger.lost(1, "later destination loss".into());
            assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
        }
    }
}

#[test]
fn pre_registration_proof_rejects_foreign_lineage_and_cannot_downgrade_runtime_or_unknown_cleanup() {
    let now = Instant::now();
    for lost in [None, Some(0), Some(1)] {
        let mut ledger = peer_idle_ledger();
        ledger.request_idle(now).unwrap();
        ledger.request_idle_removal(now).unwrap();
        ledger.request_idle_deletion(now, IdleDeleteTarget::PreRegistration).unwrap();
        let proof = pre_open_receipt(&ledger.placements[1]);
        for kind in 0..10 {
            let mut foreign = proof.clone();
            match kind {
                0 => foreign.node.instance += 1,
                1 => foreign.session = ledger.placements[0].launch.session.clone(),
                2 => foreign.generation.number.generation += 1,
                3 => foreign.cancellation.body.replica.instance += 1,
                4 => foreign.cancellation.body.unit.target = 0,
                5 => foreign.cancellation.creation = 0,
                6 => foreign.cancellation.creation = foreign.cancellation.ra_operation,
                7 => foreign.cancellation.ra_operation = 0,
                8 => foreign.node_pid += 1,
                _ => foreign.app_pid += 1,
            }
            assert!(ledger.pre_open_cancellation(1, foreign, 555).is_err());
        }
        ledger.request_idle_deletion(now, IdleDeleteTarget::Unobserved).unwrap();
        assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
        if let Some(index) = lost { ledger.lost(index, "original owner loss".into()); }
        ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
        ledger.pre_open_cancellation(1, proof, 555).unwrap();
        assert_eq!(ledger.idle_deletion_status()["state"], if lost == Some(1) { "unknown" } else { "deleted" });
    }
    let mut ledger = peer_idle_ledger();
    ledger.request_idle(now).unwrap();
    ledger.request_idle_removal(now).unwrap();
    ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
    ledger.request_idle_deletion(now, IdleDeleteTarget::PreRegistration).unwrap();
    assert!(ledger.pre_open_cancellation(1, pre_open_receipt(&ledger.placements[1]), 555).is_err());
    assert_eq!(ledger.idle_deletion_status()["pre_open_cancelled"], false);
}

#[test]
fn fm_idle_ledger_requires_semantic_ready_reply_and_fences_failure_loss_and_removal() {
    for outcome in ["ready", "failed", "cancelled", "lost"] {
        let mut ledger = Ledger::new(peer_idle_launches());
        for placement in &mut ledger.placements {
            placement.bootstrap = Bootstrap::Metadata;
            let mut actual = profile(&placement.launch);
            actual.service = placement.launch.profile.service().into();
            actual.service_type = fixed_type_for(placement.launch.profile);
            placement.admit_profile(actual).unwrap();
        }
        assert!(ledger.request_idle(Instant::now()).is_err());
        ledger.accept(command("primary", "create", Some(1))).unwrap();
        ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
        let now = Instant::now();
        let request = ledger.request_idle(now).unwrap();
        assert!(ledger.idle_retry(0, now + Duration::from_secs(10)).is_none());
        assert_eq!(ledger.idle_retry(0, now + Duration::from_secs(11)), Some(request.clone()));
        assert!(ledger.idle_retry(1, now + Duration::from_secs(30)).is_none());
        let mut reply = AddReplicaReply { unit: request.unit.clone(),
            replica: request.replica.clone(), error: 0 };
        reply.replica.state = 2;
        reply.replica.replication_endpoint = "127.0.0.1:23457".into();
        let mut stale = reply.clone();
        stale.replica.instance -= 1;
        ledger.idle_reply(0, stale).unwrap();
        ledger.idle_reply(1, reply.clone()).unwrap();
        assert_eq!(ledger.idle_status()["state"], "pending");
        let mut in_build = reply.clone();
        in_build.replica.state = 1;
        assert!(ledger.idle_reply(0, in_build).is_err());
        match outcome {
            "cancelled" => ledger.idle.as_mut().unwrap().cancelled = true,
            "lost" => ledger.lost(0, "original session lost".into()),
            "failed" => reply.error = -993,
            _ => {}
        }
        ledger.idle_reply(0, reply.clone()).unwrap();
        assert_eq!(ledger.idle_status()["state"], if outcome == "lost" { "unknown" } else { outcome });
        assert_eq!(ledger.idle_status()["ready"], outcome == "ready");
        assert!(ledger.idle_retry(0, now + Duration::from_secs(120)).is_none());
        reply.error = 0;
        ledger.idle_reply(0, reply).unwrap();
        assert_eq!(ledger.idle_status()["ready"], outcome == "ready");
        assert_eq!(ledger.placements[1].observed, Observed::Absent);
        assert!(!ledger.placements[1].created);
    }
}

#[test]
fn fm_idle_removal_keeps_pending_until_matching_reply_and_never_resurrects_add() {
    for lost in [false, true] {
        let mut ledger = Ledger::new(peer_idle_launches());
        for placement in &mut ledger.placements {
            placement.bootstrap = Bootstrap::Metadata;
            let mut actual = profile(&placement.launch);
            actual.service = placement.launch.profile.service().into();
            actual.service_type = fixed_type_for(placement.launch.profile);
            placement.admit_profile(actual).unwrap();
        }
        let now = Instant::now();
        assert!(ledger.request_idle_removal(now).is_err());
        ledger.accept(command("primary", "create", Some(1))).unwrap();
        ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
        let add = ledger.request_idle(now).unwrap();
        let request = ledger.request_idle_removal(now).unwrap();
        assert_eq!(request, add);
        assert_eq!(ledger.idle_status()["state"], "cancelled");
        assert!(ledger.idle_retry(0, now + Duration::from_secs(11)).is_none());
        assert!(ledger.request_idle(now).is_err());
        assert_eq!(ledger.idle_removal_status()["pending_remove"], true);
        let mut reply = RemoveReplicaReply {
            unit: request.unit.clone(), replica: request.replica.clone(), error: 0,
        };
        reply.replica.state = 3;
        reply.replica.replication_endpoint = "127.0.0.1:23457".into();
        let mut failed = reply.clone();
        failed.error = -992;
        ledger.idle_removal_reply(0, failed).unwrap();
        ledger.idle_removal_reply(1, reply.clone()).unwrap();
        for field in 0..4 {
            let mut stale = reply.clone();
            match field {
                0 => stale.replica.instance -= 1,
                1 => stale.replica.node.instance += 1,
                2 => stale.replica.id += 1,
                _ => stale.unit.current.configuration += 1,
            }
            ledger.idle_removal_reply(0, stale).unwrap();
        }
        let mut invalid = reply.clone();
        invalid.replica.state = 2;
        assert!(ledger.idle_removal_reply(0, invalid).is_err());
        assert_eq!(ledger.idle_removal_status()["state"], "pending");
        assert!(ledger.idle_removal_retry(0, now + Duration::from_secs(10)).is_none());
        assert_eq!(ledger.idle_removal_retry(0, now + Duration::from_secs(11)), Some(request.clone()));
        assert!(ledger.idle_removal_retry(1, now + Duration::from_secs(30)).is_none());
        assert!(ledger.idle_removal_retry(0, now + Duration::from_secs(61)).is_none());
        assert_eq!(ledger.idle_removal_retry(0, now + Duration::from_secs(72)), Some(request));
        if lost {
            ledger.lost(0, "original Primary session lost".into());
        }
        ledger.idle_removal_reply(0, reply.clone()).unwrap();
        assert_eq!(ledger.idle_removal_status()["state"], if lost { "unknown" } else { "removed" });
        assert_eq!(ledger.idle_removal_status()["pending_remove"], lost);
        assert!(ledger.idle_removal_retry(0, now + Duration::from_secs(150)).is_none());
        let mut late = reply.clone();
        late.replica.state = 2;
        ledger.idle_reply(0, late).unwrap();
        assert_eq!(ledger.idle_status()["ready"], false);
        assert_eq!(ledger.idle_status()["cleanup_required"], lost);
        ledger.idle_removal_reply(0, reply).unwrap();
        assert_eq!(ledger.placements[1].observed, Observed::Absent);
        assert!(!ledger.placements[1].created);
        if !lost {
            ledger.down(0);
            assert_eq!(ledger.idle_removal_status()["state"], "removed");
        }
    }
}

#[test]
fn fm_in_create_removal_requires_exact_echo_and_keeps_completion_separate_from_open() {
    for lost in [false, true] {
        let mut ledger = Ledger::new(peer_idle_launches());
        for placement in &mut ledger.placements {
            placement.bootstrap = Bootstrap::Metadata;
            let mut actual = profile(&placement.launch);
            actual.service = placement.launch.profile.service().into();
            actual.service_type = fixed_type_for(placement.launch.profile);
            placement.admit_profile(actual).unwrap();
        }
        ledger.accept(command("primary", "create", Some(1))).unwrap();
        ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
        let now = Instant::now();
        let request = ledger.request_idle(now).unwrap();
        let removing = ledger.request_idle_removal(now).unwrap();
        assert_eq!(removing, request);
        assert_eq!(ledger.idle_removal_status()["state"], "pending");
        let reply = RemoveReplicaReply {
            unit: request.unit.clone(), replica: request.replica.clone(), error: 0,
        };
        for field in 0..3 {
            let mut invalid = reply.clone();
            match field {
                0 => invalid.replica.state = 2,
                1 => invalid.replica.replication_endpoint = "unobserved".into(),
                _ => invalid.replica.last_lsn += 1,
            }
            assert!(ledger.idle_removal_reply(0, invalid).is_err());
        }
        ledger.idle_removal_reply(1, reply.clone()).unwrap();
        assert_eq!(ledger.idle_removal_status()["pending_remove"], true);
        assert_eq!(ledger.idle_removal_retry(0, now + Duration::from_secs(11)), Some(removing));
        if lost {
            ledger.lost(0, "original session lost".into());
        }
        ledger.idle_removal_reply(0, reply.clone()).unwrap();
        assert_eq!(ledger.idle_removal_status()["state"], if lost { "unknown" } else { "removed" });
        assert_eq!(ledger.idle_removal_status()["completion"], if lost { Value::Null } else { json!("creation_cancelled") });
        assert_eq!(ledger.idle_removal_status()["destination_deleted"], false);
        let mut late = reply;
        late.replica.state = 2;
        late.replica.replication_endpoint = "127.0.0.1:23457".into();
        ledger.idle_reply(0, late).unwrap();
        assert_eq!(ledger.idle_status()["ready"], false);
        assert!(ledger.idle_retry(0, now + Duration::from_secs(120)).is_none());
        assert!(ledger.idle_removal_retry(0, now + Duration::from_secs(120)).is_none());
        assert_eq!(ledger.placements[1].observed, Observed::Absent);
        assert!(!ledger.placements[1].created);
    }
}

#[test]
fn failed_fm_add_marks_pending_remove_and_preserves_error_after_exact_cleanup_reply() {
    for lost in [false, true] {
        let mut ledger = Ledger::new(peer_idle_launches());
        for placement in &mut ledger.placements {
            placement.bootstrap = Bootstrap::Metadata;
            let mut actual = profile(&placement.launch);
            actual.service = placement.launch.profile.service().into();
            actual.service_type = fixed_type_for(placement.launch.profile);
            placement.admit_profile(actual).unwrap();
        }
        ledger.accept(command("primary", "create", Some(1))).unwrap();
        ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
        let now = Instant::now();
        let request = ledger.request_idle(now).unwrap();
        let mut failure = AddReplicaReply {
            unit: request.unit.clone(), replica: request.replica.clone(), error: -993,
        };
        failure.replica.state = 3;
        failure.replica.up = false;
        failure.replica.version.instance += 1;
        ledger.idle_reply(1, failure.clone()).unwrap();
        assert!(ledger.idle_removal.is_none());
        ledger.idle_reply(0, failure.clone()).unwrap();
        assert_eq!(ledger.idle_status()["state"], "failed");
        assert_eq!(ledger.idle_status()["error"], -993);
        assert_eq!(ledger.idle_status()["to_be_dropped"], true);
        assert_eq!(ledger.idle_status()["cleanup_required"], true);
        assert_eq!(ledger.idle_removal_status()["pending_remove"], true);
        assert_eq!(ledger.idle_removal.as_ref().unwrap().body, request);
        assert_eq!(ledger.idle_removal_retry(0, now + Duration::from_secs(11)), Some(request.clone()));
        assert!(ledger.idle_retry(0, now + Duration::from_secs(11)).is_none());
        let reply = RemoveReplicaReply {
            unit: request.unit.clone(), replica: request.replica.clone(), error: 0,
        };
        let mut rap_success = reply.clone();
        rap_success.replica.state = 3;
        rap_success.replica.replication_endpoint = "127.0.0.1:23457".into();
        assert!(ledger.idle_removal_reply(0, rap_success).is_err());
        let mut stale = reply.clone();
        stale.replica.instance -= 1;
        ledger.idle_removal_reply(0, stale).unwrap();
        let mut error = reply.clone();
        error.error = -992;
        ledger.idle_removal_reply(0, error).unwrap();
        assert_eq!(ledger.idle_removal_status()["state"], "pending");
        if lost {
            ledger.lost(0, "original Primary session lost".into());
        }
        ledger.idle_removal_reply(0, reply.clone()).unwrap();
        assert_eq!(ledger.idle_removal_status()["state"], if lost { "unknown" } else { "removed" });
        assert_eq!(ledger.idle_removal_status()["pending_remove"], lost);
        assert_eq!(ledger.idle_status()["error"], -993);
        assert_eq!(ledger.idle_status()["ready"], false);
        assert_eq!(ledger.idle_status()["cleanup_required"], lost);
        assert_eq!(ledger.idle_status()["destination_cleanup_required"], true);
        assert_eq!(ledger.idle_removal_status()["destination_deleted"], false);
        if !lost {
            assert_eq!(ledger.idle_status()["state"], "failed");
            assert_eq!(ledger.idle_removal_status()["completion"], "failed_creation_retired");
        }
        let original = ledger.status();
        ledger.idle_reply(0, failure.clone()).unwrap();
        failure.error = 0;
        failure.replica.state = 2;
        failure.replica.replication_endpoint = "late".into();
        ledger.idle_reply(0, failure).unwrap();
        ledger.idle_removal_reply(0, reply).unwrap();
        assert_eq!(ledger.status(), original);
        assert!(!ledger.placements[1].created);
    }
}

#[test]
fn peer_idle_profile_binds_one_partition_and_never_admits_a_second_fm_primary() {
    let launches = peer_idle_launches();
    for launch in &launches {
        launch.validate().unwrap();
    }
    let mut ledger = Ledger::new(launches);
    for placement in &mut ledger.placements {
        placement.bootstrap = Bootstrap::Metadata;
        let mut actual = profile(&placement.launch);
        actual.service = placement.launch.profile.service().into();
        actual.service_type = fixed_type_for(placement.launch.profile);
        placement.admit_profile(actual).unwrap();
    }
    assert!(ledger.accept(command("wrong", "create", Some(2))).is_err());
    assert!(ledger.operations.is_empty());
    assert!(matches!(
        ledger
            .accept(command("primary", "create", Some(1)))
            .unwrap(),
        Accepted::Create(0, _)
    ));
    ledger
        .reply(0, ready_reply(&ledger.placements[0]), false)
        .unwrap();
    assert!(ledger.accept(command("delete", "delete", Some(1))).is_err());
    assert!(ledger
        .accept(command("generation", "deactivate", Some(2)))
        .is_err());
    assert!(!ledger.placements[1].created);
    let mut invalid = ledger.placements[0].launch.clone();
    invalid.peer.as_mut().unwrap().replica = invalid.replica;
    assert!(invalid.validate().is_err());
}

fn data_contact(endpoint: &str, request: &[u8]) -> String {
    let mut socket =
        TcpStream::connect_timeout(&endpoint.parse().unwrap(), Duration::from_secs(2)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket.write_all(request).unwrap();
    let mut response = String::new();
    socket.take(1024).read_to_string(&mut response).unwrap();
    response
}

async fn peer_idle_catch<F: std::future::Future>(future: F) -> std::thread::Result<F::Output> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(error) => std::task::Poll::Ready(Err(error)),
        }
    })
    .await
}

#[tokio::test]
async fn peer_idle_real_original_children_open_only_idle_and_keep_primary_serving() {
    let fixture = Fixture::new("peer-idle");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let mut endpoint = None;
    let result = peer_idle_catch(async {
        assert_eq!(
            cluster.children[0].launch.partition,
            cluster.children[1].launch.partition
        );
        assert_ne!(
            cluster.children[0].launch.replica,
            cluster.children[1].launch.replica
        );
        assert_ne!(
            cluster.children[0]
                .metadata
                .as_ref()
                .unwrap()
                .profile
                .as_ref()
                .unwrap()
                .app_pid,
            cluster.children[1]
                .metadata
                .as_ref()
                .unwrap()
                .profile
                .as_ref()
                .unwrap()
                .app_pid,
        );
        for child in &cluster.children {
            assert_eq!(
                child
                    .metadata
                    .as_ref()
                    .unwrap()
                    .profile
                    .as_ref()
                    .unwrap()
                    .version
                    .instance,
                1
            );
        }
        assert!(cluster.create_idle_replica().await.is_err());
        assert!(cluster
            .execute(command("wrong-node", "create", Some(2)))
            .await
            .is_err());
        assert!(cluster.begin_generation_handshake().is_err());
        cluster
            .execute(command("primary", "create", Some(1)))
            .await?;
        until(&mut cluster, |cluster| {
            cluster.ledger.placements[0].observed == Observed::Running
        })
        .await;
        let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
        endpoint = Some(primary.clone());
        let ready_end = Instant::now() + Duration::from_secs(2);
        while !data_contact(&primary, b"read\n").starts_with("OK ") {
            if Instant::now() >= ready_end {
                return Err("public Primary readiness deadline".into());
            }
            cluster.drive().await;
        }
        assert_eq!(cluster.create_idle_replica().await?["state"], "pending");
        until(&mut cluster, |cluster| {
            let status = cluster.status();
            status["peer_idle"]["state"] == "open"
                && status["nodes"][1]["catalog_notified"] == true
                && status["nodes"][1]["retained_work"]["semantic_pending"] == 0
                && status["nodes"][1]["retained_work"]["commands"] == 0
        })
        .await;
        let status = cluster.status();
        let written = data_contact(&primary, b"write\n");
        assert!(
            written.contains("assigned=1 applied=1 committed=1 value=acknowledged"),
            "{written}"
        );
        assert_eq!(data_contact(&primary, b"read\n"), written);
        let remote = status["peer_idle"]["source"]["reply"].clone();
        assert_eq!(remote["error"], 0);
        assert_eq!(remote["replica"]["current_role"], 2);
        assert_eq!(remote["replica"]["state"], 1);
        assert_eq!(remote["replica"]["service_endpoint"], "");
        assert!(!remote["replica"]["replication_endpoint"]
            .as_str()
            .unwrap()
            .is_empty());
        assert_eq!(status["peer_idle"]["built"], false);
        assert_eq!(status["peer_idle"]["serving"], false);
        assert_eq!(status["nodes"][1]["created_lifetime"], false);
        assert_eq!(status["nodes"][1]["observed"], "absent");
        assert_eq!(status["nodes"][1]["catalog_notified"], true);
        assert_eq!(status["nodes"][1]["retained_work"]["semantic_pending"], 0);
        assert_eq!(status["nodes"][1]["retained_work"]["link_alive"], true);
        assert_eq!(status["nodes"][1]["retained_work"]["error"], Value::Null);
        let original = status["nodes"][1]["retained_work"]["local_replica"].clone();
        let primary_replica = &status["nodes"][0]["retained_work"]["local_replica"];
        assert_ne!(
            primary_replica["registration"]["host"],
            original["registration"]["host"]
        );
        assert_ne!(
            primary_replica["registration"]["runtime"],
            original["registration"]["runtime"]
        );
        assert_ne!(
            primary_replica["package_sequence"],
            original["package_sequence"]
        );
        assert!(original["registration"]["acquisition"].as_u64().unwrap() > 0);
        assert_eq!(original["retained_usage"], 1);
        let application = &status["nodes"][1]["retained_work"]["app_observation"];
        assert_eq!(application["opens"], 1);
        assert_eq!(application["provider_acquisitions"], 1);
        assert_eq!(application["idle_access_denied"], true);
        assert_eq!(application["idle_write_denials"], 2);
        cluster.create_idle_replica().await?;
        until(&mut cluster, |cluster| {
            cluster.status()["nodes"][1]["retained_work"]["commands"] == 0
                && cluster.status()["nodes"][0]["retained_work"]["commands"] == 0
        })
        .await;
        let end = Instant::now() + Duration::from_millis(600);
        while Instant::now() < end {
            cluster.drive().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let retried = cluster.status();
        assert_eq!(retried["peer_idle"]["source"]["reply"], remote);
        assert_eq!(
            retried["nodes"][1]["retained_work"]["local_replica"],
            original
        );
        assert_eq!(
            retried["nodes"][1]["retained_work"]["app_observation"],
            *application
        );
        assert_eq!(data_contact(&primary, b"read\n"), written);
        assert!(cluster
            .ledger
            .placements
            .iter()
            .all(|placement| placement.link_alive));
        Ok::<(), String>(())
    })
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    if let Some(endpoint) = endpoint {
        assert!(TcpStream::connect(endpoint).is_err());
    }
    result.unwrap().unwrap();
}
#[tokio::test]
async fn peer_idle_build_copies_nonempty_state_and_continues_replication_without_membership() {
    let fixture = Fixture::new("peer-idle-build");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.build_idle_replica().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
        let end = Instant::now() + Duration::from_secs(2);
        while !data_contact(&primary, b"read\n").starts_with("OK ") {
            assert!(Instant::now() < end);
            cluster.drive().await;
        }
        let write = data_contact(&primary, b"write\n");
        assert!(write.contains("assigned=1 applied=1 committed=1 value=acknowledged"), "{write}");
        cluster.create_idle_replica().await?;
        until(&mut cluster, |cluster| cluster.status()["peer_idle"]["state"] == "open").await;
        let original = cluster.status()["peer_idle"]["target"].clone();
        assert_eq!(cluster.status()["peer_idle"]["built"], false);
        assert_eq!(cluster.build_idle_replica().await?["state"], "pending");
        let end = Instant::now() + Duration::from_secs(12);
        loop {
            cluster.drive().await;
            let status = cluster.status();
            if status["peer_idle"]["built"] == true
                && status["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1
            {
                break;
            }
            if Instant::now() >= end {
                return Err(format!("build deadline; source={}, status={status}", data_contact(&primary, b"state\n")));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let status = cluster.status();
        let copied = &status["nodes"][1]["retained_work"]["app_observation"]["idle_data"];
        assert_eq!(copied["provider"]["values"]["value"], "acknowledged");
        assert_eq!(copied["provider"]["results"]["primary-write"], 1);
        assert_eq!(copied["provider"]["copy_in_progress"], false);
        assert_eq!(copied["role"], "IdleSecondary");
        assert_eq!(copied["ready"], false);
        assert_ne!(copied["read"], "Granted");
        assert_ne!(copied["write"], "Granted");
        assert_eq!(copied["application_acknowledged"], true);
        assert_eq!(status["peer_idle"]["serving"], false);
        assert_eq!(status["peer_idle"]["target"], original);
        assert_eq!(data_contact(&primary, b"read\n"), write);
        let continued = data_contact(&primary, b"write-more\n");
        assert!(continued.contains("assigned=2 applied=2 committed=2 value=continued"), "{continued}");
        until(&mut cluster, |cluster| {
            cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2
                && serde_json::from_str::<Value>(&data_contact(&primary, b"state\n"))
                    .unwrap()["peers"][0]["applied"] == 2
        }).await;
        let source: Value = serde_json::from_str(&data_contact(&primary, b"state\n")).map_err(err)?;
        let status = cluster.status();
        let target = &status["nodes"][1]["retained_work"]["app_observation"]["idle_data"];
        assert_eq!(target["provider"]["values"], source["provider"]["values"]);
        assert_eq!(target["provider"]["results"], source["provider"]["results"]);
        assert_eq!(source["peers"].as_array().unwrap().len(), 1);
        assert_eq!(source["peers"][0]["configured"], false);
        assert_eq!(source["peers"][0]["building"], false);
        assert_eq!(source["peers"][0]["copy_horizon"], 1);
        assert_eq!(source["peers"][0]["applied"], 2);
        assert_eq!(target["ready"], false);
        assert_eq!(cluster.build_idle_replica().await?["built"], true);
        assert_eq!(cluster.status()["peer_idle"]["target"], original);
        assert_eq!(cluster.status()["nodes"][1]["observed"], "absent");
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn peer_idle_removal_retires_only_the_primary_session_before_or_after_copy() {
    for (build, fm) in [(false, false), (true, false), (false, true), (true, true)] {
        let fixture = Fixture::new(if build { "built-idle-remove" } else { "unbuilt-idle-remove" });
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.start().await.unwrap();
        let result = peer_idle_catch(async {
            assert!(cluster.remove_idle_replica().await.is_err());
            assert!(cluster.remove_idle_replica_via_fm().await.is_err());
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
            assert!(cluster.remove_idle_replica().await.is_err());
            assert!(cluster.remove_idle_replica_via_fm().await.is_err());
            let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
            let end = Instant::now() + Duration::from_secs(2);
            while !data_contact(&primary, b"read\n").starts_with("OK ") {
                assert!(Instant::now() < end);
                cluster.drive().await;
            }
            let write = data_contact(&primary, b"write\n");
            assert!(write.contains("assigned=1 applied=1 committed=1 value=acknowledged"), "{write}");
            cluster.create_idle_replica().await?;
            until(&mut cluster, |cluster| cluster.status()["peer_idle"]["state"] == "open").await;
            let original = cluster.status()["peer_idle"]["target"].clone();
            if build {
                cluster.build_idle_replica().await?;
                until(&mut cluster, |cluster| {
                    cluster.status()["peer_idle"]["built"] == true
                        && cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1
                }).await;
            }
            let pending = if fm {
                cluster.remove_idle_replica_via_fm().await?
            } else {
                cluster.remove_idle_replica().await?
            };
            assert_eq!(pending["state"], "pending");
            until(&mut cluster, |cluster| cluster.status()["peer_idle"]["state"] == "removed"
                && (!fm || cluster.ledger.idle_removal_status()["removed"] == true)).await;
            let removed = cluster.status();
            assert_eq!(removed["peer_idle"]["built"], false);
            assert_eq!(removed["peer_idle"]["serving"], false);
            assert_eq!(removed["peer_idle"]["removal"]["result"], 0);
            assert_eq!(removed["peer_idle"]["removal"]["replica"]["state"], 3);
            assert_eq!(removed["peer_idle"]["target"], original);
            assert_eq!(original["open"], true);
            assert_eq!(original["replica"]["state"], 1);
            assert_eq!(original["replica"]["current_role"], 2);
            if fm {
                assert_eq!(cluster.remove_idle_replica_via_fm().await?["state"], "removed");
                assert!(cluster.remove_idle_replica().await.is_err());
                assert_eq!(removed["idle_removal"]["pending_remove"], false);
            } else {
                assert_eq!(cluster.remove_idle_replica().await?["state"], "removed");
            }
            assert_eq!(cluster.create_idle_replica().await?["state"], "removed");
            assert!(cluster.build_idle_replica().await.is_err());
            let source: Value = serde_json::from_str(&data_contact(&primary, b"state\n")).map_err(err)?;
            assert_eq!(source["peers"].as_array().unwrap().len(), 0);
            let continued = data_contact(&primary, b"write-more\n");
            assert!(continued.contains("assigned=2 applied=2 committed=2 value=continued"), "{continued}");
            assert_eq!(data_contact(&primary, b"read\n"), continued);
            for _ in 0..15 {
                cluster.drive().await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let after = cluster.status();
            assert_eq!(after["peer_idle"]["target"], original);
            assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 1);
            assert!(after["nodes"][1]["retained_work"]["app_observation"]["idle_data_error"].is_null());
            if build {
                let target = &after["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"];
                assert_eq!(target["last"], 1);
                assert_eq!(target["values"]["value"], "acknowledged");
                assert_eq!(target["results"]["primary-write"], 1);
                assert!(target["results"]["primary-write-2"].is_null());
            }
            assert_eq!(after["nodes"][1]["observed"], "absent");
            assert!(after["peer_idle"]["command_error"].as_array().unwrap().iter().all(Value::is_null));
            Ok::<(), String>(())
        }).await;
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn fm_add_idle_copies_real_state_and_recovers_lost_reply_without_resurrection_on_remove() {
    for cancel in [false, true] {
        let fixture = Fixture::new(if cancel { "fm-idle-cancel" } else { "fm-idle-build" });
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.start().await.unwrap();
        let result = peer_idle_catch(async {
            assert!(cluster.add_idle_replica().await.is_err());
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
            let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
            let end = Instant::now() + Duration::from_secs(2);
            while !data_contact(&primary, b"read\n").starts_with("OK ") {
                assert!(Instant::now() < end);
                cluster.drive().await;
            }
            assert!(data_contact(&primary, b"write\n").contains("committed=1 value=acknowledged"));
            assert_eq!(cluster.add_idle_replica().await?["state"], "pending");
            let request = cluster.ledger.idle.as_ref().unwrap().body.clone();
            let end = Instant::now() + Duration::from_secs(12);
            let dropped = loop {
                let mut dropped = None;
                for index in 0..2 {
                    let incoming = cluster.children[index].link.as_ref().unwrap()
                        .receive(Instant::now() + Duration::from_millis(2)).await?;
                    if let Some(mut incoming) = incoming {
                        if incoming.message.properties().map_err(err)?.action == "AddReplicaReply" {
                            let (body, owner) = decode_add_replica_reply(&incoming.message).map_err(err)?;
                            assert_eq!(owner, Owner::Fm);
                            assert_eq!(index, 0);
                            if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                            dropped = Some(body);
                        } else {
                            cluster.receive_message(index, incoming).await?;
                        }
                    }
                }
                if let Some(body) = dropped { break body; }
                assert!(Instant::now() < end, "lost-reply deadline: {}", cluster.status());
            };
            assert_eq!(dropped.replica.state, 2);
            assert_eq!(dropped.replica.current_role, 2);
            assert_eq!(cluster.ledger.idle_status()["state"], "pending");
            assert_eq!(cluster.ledger.placements[1].observed, Observed::Absent);
            until(&mut cluster, |cluster| {
                cluster.status()["peer_idle"]["ready"] == true
                    && cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1
            }).await;
            assert_eq!(cluster.ledger.idle_status()["state"], "pending");
            if cancel {
                cluster.send_idle(&request).await?;
                assert_eq!(cluster.remove_idle_replica_via_fm().await?["state"], "pending");
                let remove_request = cluster.ledger.idle_removal.as_ref().unwrap().body.clone();
                let end = Instant::now() + Duration::from_secs(12);
                let removed = loop {
                    let mut dropped = None;
                    for index in 0..2 {
                        let incoming = cluster.children[index].link.as_ref().unwrap()
                            .receive(Instant::now() + Duration::from_millis(2)).await?;
                        if let Some(mut incoming) = incoming {
                            if incoming.message.properties().map_err(err)?.action == "RemoveReplicaReply" {
                                let (body, owner) = decode_remove_replica_reply(&incoming.message).map_err(err)?;
                                assert_eq!(owner, Owner::Fm);
                                assert_eq!(index, 0);
                                if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                                dropped = Some(body);
                            } else {
                                cluster.receive_message(index, incoming).await?;
                            }
                        }
                    }
                    if let Some(body) = dropped { break body; }
                    assert!(Instant::now() < end, "lost removal reply: {}", cluster.status());
                };
                assert_eq!(removed.error, 0);
                assert_eq!(removed.replica.state, 3);
                until(&mut cluster, |cluster| cluster.status()["peer_idle"]["state"] == "removed").await;
                assert_eq!(cluster.ledger.idle_removal_status()["state"], "pending");
                assert_eq!(cluster.ledger.idle_removal_status()["pending_remove"], true);
                let retry = cluster.ledger.idle_removal_retry(0, Instant::now() + Duration::from_secs(11))
                    .ok_or("missing FM removal retry")?;
                assert_eq!(retry, remove_request);
                cluster.send_idle_removal(&retry).await?;
                until(&mut cluster, |cluster| cluster.ledger.idle_removal_status()["removed"] == true).await;
                assert_eq!(cluster.ledger.idle_removal.as_ref().unwrap().reply.as_ref(), Some(&removed));
                assert_eq!(cluster.remove_idle_replica_via_fm().await?["state"], "removed");
                assert_eq!(cluster.ledger.idle_status()["state"], "cancelled");
                cluster.ledger.idle_reply(0, dropped)?;
                assert_eq!(cluster.ledger.idle_status()["ready"], false);
                cluster.send_idle(&request).await?;
                cluster.send_idle_removal(&retry).await?;
            } else {
                let retry = cluster.ledger.idle_retry(0, Instant::now() + Duration::from_secs(11))
                    .ok_or("missing FM semantic retry")?;
                assert_eq!(retry, request);
                cluster.send_idle(&retry).await?;
                until(&mut cluster, |cluster| cluster.ledger.idle_status()["ready"] == true).await;
                assert_eq!(cluster.add_idle_replica().await?["state"], "ready");
            }
            assert!(cluster.create_idle_replica().await.is_err());
            assert!(cluster.build_idle_replica().await.is_err());
            let write = data_contact(&primary, b"write-more\n");
            assert!(write.contains("committed=2 value=continued"), "{write}");
            assert_eq!(data_contact(&primary, b"read\n"), write);
            if !cancel {
                until(&mut cluster, |cluster| {
                    cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2
                }).await;
            }
            let status = cluster.status();
            let target = &status["peer_idle"]["target"];
            assert_eq!(target["open"], true);
            assert_eq!(target["replica"]["current_role"], 2);
            assert_eq!(target["replica"]["state"], 1);
            assert_eq!(target["replica"]["service_endpoint"], "");
            assert_eq!(status["peer_idle"]["serving"], false);
            assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 1);
            let data = &status["nodes"][1]["retained_work"]["app_observation"]["idle_data"];
            assert_eq!(data["ready"], false);
            assert_eq!(data["provider"]["results"]["primary-write"], 1);
            assert_eq!(data["provider"]["last"], if cancel { 1 } else { 2 });
            let source: Value = serde_json::from_str(&data_contact(&primary, b"state\n")).map_err(err)?;
            if cancel {
                assert!(source["peers"].as_array().unwrap().is_empty());
            } else {
                assert_eq!(source["peers"][0]["configured"], false);
                assert_eq!(source["peers"][0]["building"], false);
                assert_eq!(status["peer_idle"]["ready"], true);
                assert_eq!(cluster.remove_idle_replica_via_fm().await?["state"], "pending");
                until(&mut cluster, |cluster| {
                    cluster.ledger.idle_removal_status()["removed"] == true
                        && cluster.status()["peer_idle"]["state"] == "removed"
                        && cluster.status()["peer_idle"]["ready"] == false
                }).await;
                let after = cluster.status();
                assert_eq!(after["peer_idle"]["target"], *target);
                assert_eq!(after["peer_idle"]["built"], false);
                assert_eq!(after["peer_idle"]["ready"], false);
                assert_eq!(after["peer_idle"]["fm"]["ready"], false);
                assert_eq!(after["idle_removal"]["pending_remove"], false);
                assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"], data["provider"]);
                assert_eq!(data_contact(&primary, b"read\n"), write);
            }
            Ok::<(), String>(())
        }).await;
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn fm_can_cancel_original_pending_add_without_waiting_for_open_telemetry() {
    let fixture = Fixture::new("fm-pending-create-cancel");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.remove_idle_replica_via_fm().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        assert_eq!(cluster.add_idle_replica().await?["state"], "pending");
        let add = cluster.ledger.idle.as_ref().unwrap().body.clone();
        assert_eq!(cluster.remove_idle_replica_via_fm().await?["state"], "pending");
        assert_eq!(cluster.ledger.idle_status()["state"], "cancelled");
        until(&mut cluster, |cluster| {
            cluster.ledger.idle_removal_status()["removed"] == true
                && cluster.status()["peer_idle"]["target"]["open"] == true
        }).await;
        let removed = cluster.ledger.idle_removal_status();
        assert!(matches!(removed["completion"].as_str(), Some("creation_cancelled" | "replicator_removed")));
        assert_eq!(removed["destination_deleted"], false);
        assert_eq!(removed["pending_remove"], false);
        cluster.send_idle(&add).await?;
        for _ in 0..15 { cluster.drive().await; }
        assert_eq!(cluster.ledger.idle_status()["ready"], false);
        assert_eq!(cluster.remove_idle_replica_via_fm().await?["state"], "removed");
        let status = cluster.status();
        assert_eq!(status["peer_idle"]["serving"], false);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(status["peer_idle"]["target"]["replica"]["current_role"], 2);
        assert_eq!(status["peer_idle"]["target"]["replica"]["state"], 1);
        Ok::<(), String>(())
    }).await;
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap().unwrap();
}

#[tokio::test]
async fn fm_in_create_cancel_fences_actual_peer_open_before_and_after_completion() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::Pending,
        node::report_tests::PeerCreationOutcome::Opened,
    ] {
        let fixture = Fixture::new("peer-idle-create-cancel");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [
            cluster.ledger.placements[0].launch.clone(),
            cluster.ledger.placements[1].launch.clone(),
        ];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn failed_peer_creation_cleanup_replays_lost_reply_without_adopting_late_success() {
    let fixture = Fixture::new("peer-idle-failed-create");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let launches = [
        cluster.ledger.placements[0].launch.clone(),
        cluster.ledger.placements[1].launch.clone(),
    ];
    let result = node::report_tests::run_peer_creation_removal(
        &launches, node::report_tests::PeerCreationOutcome::Failed,
    ).await;
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap().unwrap();
}

#[tokio::test]
async fn opened_idle_destination_delete_joins_disposal_and_replays_lost_reply_independently_of_remove() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DeleteProvider,
        node::report_tests::PeerCreationOutcome::DeleteService,
    ] {
        let fixture = Fixture::new("peer-idle-destination-delete");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [
            cluster.ledger.placements[0].launch.clone(),
            cluster.ledger.placements[1].launch.clone(),
        ];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn registered_open_idle_delete_drains_actual_open_and_disposal_before_reply_replay() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DeleteOpeningProvider,
        node::report_tests::PeerCreationOutcome::DeleteOpeningService,
    ] {
        let fixture = Fixture::new("peer-idle-opening-delete");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [
            cluster.ledger.placements[0].launch.clone(),
            cluster.ledger.placements[1].launch.clone(),
        ];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn failed_registration_delete_uses_actual_lookup_failure_and_replays_lost_reply_without_erasing_add_error() {
    let fixture = Fixture::new("peer-idle-failed-registration-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
    let result = node::report_tests::run_peer_creation_removal(
        &launches, node::report_tests::PeerCreationOutcome::DeleteFailedRegistration,
    ).await;
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap().unwrap();
}

#[tokio::test]
async fn pre_registration_delete_drains_actual_late_acquisition_before_publishing_cancellation() {
    let fixture = Fixture::new("peer-idle-prereg-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let launches = [
        cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone(),
    ];
    let result = node::report_tests::run_peer_creation_removal(
        &launches, node::report_tests::PeerCreationOutcome::DeletePreRegistration,
    ).await;
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap().unwrap();
}

#[tokio::test]
async fn absent_idle_delete_replays_lost_reply_and_fences_held_original_peer_create() {
    let fixture = Fixture::new("peer-idle-absent-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let launches = [
        cluster.ledger.placements[0].launch.clone(),
        cluster.ledger.placements[1].launch.clone(),
    ];
    let result = node::report_tests::run_peer_creation_removal(
        &launches, node::report_tests::PeerCreationOutcome::DeleteAbsent,
    ).await;
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap().unwrap();
}

#[tokio::test]
async fn fm_deletes_pre_registration_idle_with_distinct_child_cancellation_not_disposal() {
    let fixture = Fixture::new("fm-prereg-idle-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let code = fixture.root.join("deployment/package/LocalService/Code");
    let app = code.join("hosting-local-app");
    fs::rename(&app, code.join("hosting-local-app-real")).unwrap();
    fs::write(&app, b"#!/bin/sh\ncase \"$*\" in\n*--stateful-lifecycle*) exec \"${0%/*}/hosting-local-app-real\" \"$@\" --block-registration;;\n*) exec \"${0%/*}/hosting-local-app-real\" \"$@\";;\nesac\n").unwrap();
    fs::set_permissions(&app, fs::Permissions::from_mode(0o755)).unwrap();
    for index in 0..2 { cluster.spawn(index).await.unwrap(); }
    let address = cluster.children[1].metadata.as_ref().and_then(|metadata| metadata.peer_address).unwrap();
    let child = &mut cluster.children[0];
    child.process.as_mut().unwrap().command(
        format!("peer {} {} {address}\n", child.launch.session, child.launch.capability).as_bytes(),
    ).unwrap();
    let profile = cluster.children[1].metadata.as_ref().unwrap().profile.as_ref().unwrap();
    let work = profile.runtime_root.join(format!("deployment/{}_App1/work",
        profile.service_type.package.application.type_name));
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running
            && work.join("local-registration.held").exists()).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |cluster| cluster.status()["peer_idle"]["target"]["phase"] == "Opening"
            && cluster.status()["peer_idle"]["target"]["registration"].is_null()
            && cluster.status()["peer_idle"]["target"]["retained_usage"] == 0).await;
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        cluster.remove_idle_replica_via_fm().await?;
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "pending");
        until(&mut cluster, |cluster| cluster.ledger.idle_deletion_status()["complete"] == true
            && cluster.ledger.idle_removal_status()["removed"] == true
            && cluster.status()["peer_idle"]["target"]["phase"] == "Closed").await;
        let deleted = cluster.status();
        assert_eq!(deleted["idle_deletion"]["completion"], "pre_open_cancelled");
        assert_eq!(deleted["idle_deletion"]["never_created"], false);
        assert_eq!(deleted["idle_deletion"]["disposed"], false);
        assert_eq!(deleted["nodes"][1]["created_lifetime"], false);
        assert_eq!(deleted["peer_idle"]["target"]["retained_usage"], 0);
        assert_eq!(deleted["nodes"][1]["retained_work"]["app_observation"]["opens"], 0);
        let path = cluster.children[1].launch.directory.join("pre-open-cancellation.json");
        let proof: PreOpenCancellationReceipt = read_json(&path)?;
        assert_ne!(proof.cancellation.creation, 0);
        assert_ne!(proof.cancellation.creation, proof.cancellation.ra_operation);
        assert!(!cluster.children[1].launch.directory.join("retirement.json").exists());
        assert!(!cluster.children[1].launch.directory.join("absence.json").exists());
        fs::write(work.join("local-registration.release"), b"release original registration").unwrap();
        until(&mut cluster, |_| work.join("local-registration.complete").exists()).await;
        for _ in 0..20 { cluster.drive().await; }
        let after = cluster.status();
        assert_eq!(after["idle_deletion"], deleted["idle_deletion"]);
        assert_eq!(after["peer_idle"]["target"]["phase"], "Closed");
        assert_eq!(after["peer_idle"]["target"]["retained_usage"], 0);
        assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["opens"], 0);
        assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 0);
        assert_eq!(after["peer_idle"]["command_error"], json!([null, null]));
        assert_eq!(read_json::<Value>(&path)?, serde_json::to_value(proof).map_err(err)?);
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "deleted");
        assert!(cluster.execute(command("replacement", "create", Some(2))).await.is_err());
        let continued = data_contact(&endpoint, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        assert_eq!(data_contact(&endpoint, b"read\n"), continued);
        Ok::<(), String>(())
    }).await;
    fs::write(work.join("local-registration.release"), b"release original registration").unwrap();
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn fm_deletes_registered_open_idle_through_original_child_adapters_without_absence() {
    let fixture = Fixture::new("fm-opening-idle-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let code = fixture.root.join("deployment/package/LocalService/Code");
    let app = code.join("hosting-local-app");
    fs::rename(&app, code.join("hosting-local-app-real")).unwrap();
    fs::write(&app, b"#!/bin/sh\ncase \"$*\" in\n*--stateful-lifecycle*) exec \"${0%/*}/hosting-local-app-real\" \"$@\" --block-open --block-dispose;;\n*) exec \"${0%/*}/hosting-local-app-real\" \"$@\";;\nesac\n").unwrap();
    fs::set_permissions(&app, fs::Permissions::from_mode(0o755)).unwrap();
    for index in 0..2 { cluster.spawn(index).await.unwrap(); }
    let address = cluster.children[1].metadata.as_ref().and_then(|metadata| metadata.peer_address).unwrap();
    let child = &mut cluster.children[0];
    child.process.as_mut().unwrap().command(
        format!("peer {} {} {address}\n", child.launch.session, child.launch.capability).as_bytes(),
    ).unwrap();
    let profile = cluster.children[1].metadata.as_ref().unwrap().profile.as_ref().unwrap();
    let work = profile.runtime_root.join(format!("deployment/{}_App1/work",
        profile.service_type.package.application.type_name));
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |cluster| work.join("local-open.held").exists()
            && cluster.status()["peer_idle"]["target"]["phase"] == "Opening"
            && cluster.status()["peer_idle"]["target"]["registration"].is_object()
            && cluster.status()["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"] == 1).await;
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        cluster.remove_idle_replica_via_fm().await?;
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "pending");
        until(&mut cluster, |cluster| work.join("local-open.cancelled").exists()
            && cluster.status()["peer_idle"]["target"]["phase"] == "Closing").await;
        let held = cluster.status();
        assert_eq!(held["peer_idle"]["target"]["retained_usage"], 1);
        assert_eq!(held["idle_deletion"]["destination_deleted"], false);
        assert_eq!(held["idle_deletion"]["disposed"], false);
        assert_eq!(held["idle_deletion"]["never_created"], false);
        assert_eq!(held["idle_deletion"]["completion"], Value::Null);
        assert_eq!(held["nodes"][1]["created_lifetime"], true);
        assert!(!cluster.children[1].launch.directory.join("absence.json").exists());
        assert!(!cluster.children[1].launch.directory.join("retirement.json").exists());
        let continued = data_contact(&endpoint, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        fs::write(work.join("local-open.release"), b"release original Open").unwrap();
        until(&mut cluster, |_| work.join("local-instance.resource").exists()
            && !work.join("local-provider.resource").exists()).await;
        for _ in 0..10 { cluster.drive().await; }
        assert_eq!(cluster.status()["peer_idle"]["target"]["retained_usage"], 1);
        assert_eq!(cluster.ledger.idle_deletion_status()["complete"], false);
        assert!(cluster.ledger.placements[1].retirement.is_none());
        fs::write(work.join("local-dispose.release"), b"release original service").unwrap();
        until_with_budget(&mut cluster, Duration::from_secs(20), |cluster| cluster.ledger.idle_deletion_status()["complete"] == true
            && cluster.ledger.idle_removal_status()["removed"] == true
            && cluster.status()["peer_idle"]["target"]["retained_usage"] == 0).await;
        let after = cluster.status();
        assert_eq!(after["idle_deletion"]["completion"], "disposed");
        assert_eq!(after["idle_deletion"]["never_created"], false);
        assert_eq!(after["peer_idle"]["target"]["open"], false);
        assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 1);
        assert_eq!(after["peer_idle"]["source"]["reply"], Value::Null);
        assert_eq!(after["peer_idle"]["source"]["build"], Value::Null);
        assert_eq!(after["peer_idle"]["command_error"], json!([null, null]));
        assert!(cluster.children[1].launch.directory.join("retirement.json").exists());
        assert!(!cluster.children[1].launch.directory.join("absence.json").exists());
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "deleted");
        assert!(cluster.execute(command("replacement", "create", Some(2))).await.is_err());
        assert_eq!(data_contact(&endpoint, b"read\n"), continued);
        Ok::<(), String>(())
    }).await;
    fs::write(work.join("local-open.release"), b"release original Open").unwrap();
    fs::write(work.join("local-dispose.release"), b"release original service").unwrap();
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn fm_deletes_never_created_idle_before_peer_binding_and_publishes_separate_absence() {
    let fixture = Fixture::new("fm-never-created-idle-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    for index in 0..2 { cluster.spawn(index).await.unwrap(); }
    let result = peer_idle_catch(async {
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&primary, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&primary, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |cluster| cluster.status()["peer_idle"]["source"]["replica"].is_object()).await;
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        cluster.remove_idle_replica_via_fm().await?;
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "pending");
        until(&mut cluster, |cluster| cluster.ledger.idle_deletion_status()["complete"] == true
            && cluster.status()["peer_idle"]["target"]["open_token"] == 0).await;
        let before = cluster.status();
        assert_eq!(before["idle_deletion"]["pending_remove"], true);
        assert_eq!(before["idle_deletion"]["completion"], "never_created");
        assert_eq!(before["idle_deletion"]["never_created"], true);
        assert_eq!(before["idle_deletion"]["disposed"], false);
        assert_eq!(before["nodes"][1]["retirement"], Value::Null);
        assert_eq!(before["nodes"][1]["created_lifetime"], false);
        assert_eq!(before["nodes"][1]["retained_work"]["app_observation"]["opens"], 0);
        assert_eq!(before["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 0);
        assert!(!cluster.children[1].launch.directory.join("retirement.json").exists());
        assert!(cluster.children[1].launch.directory.join("absence.json").exists());
        let address = cluster.children[1].metadata.as_ref().and_then(|metadata| metadata.peer_address)
            .ok_or("missing original target peer listener")?;
        let child = &mut cluster.children[0];
        child.process.as_mut().ok_or("missing original Primary owner")?.command(
            format!("peer {} {} {address}\n", child.launch.session, child.launch.capability).as_bytes(),
        ).map_err(err)?;
        until(&mut cluster, |cluster| cluster.status()["peer_idle"]["peer_link_alive"] == json!([true, true])
            && cluster.ledger.idle_removal_status()["removed"] == true
            && cluster.status()["nodes"][1]["retained_work"]["commands"] == 0).await;
        for _ in 0..20 { cluster.drive().await; }
        let after = cluster.status();
        let mut completed = before["idle_deletion"].clone();
        completed["pending_remove"] = json!(false);
        assert_eq!(after["idle_deletion"], completed);
        assert_eq!(after["peer_idle"]["peer_error"], json!([null, null]));
        assert_eq!(after["peer_idle"]["command_error"], json!([null, null]));
        assert_eq!(after["peer_idle"]["target"]["open_token"], 0);
        assert_eq!(after["peer_idle"]["target"]["retained_usage"], 0);
        assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["opens"], 0);
        assert_eq!(after["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 0);
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "deleted");
        assert!(cluster.execute(command("replacement", "create", Some(2))).await.is_err());
        assert!(cluster.add_idle_replica().await.is_err());
        let continued = data_contact(&primary, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        assert_eq!(data_contact(&primary, b"read\n"), continued);
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn fm_deletes_original_built_idle_destination_without_replacing_or_stopping_primary() {
    let fixture = Fixture::new("fm-idle-destination-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&primary, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&primary, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |cluster| cluster.ledger.idle_status()["ready"] == true
            && cluster.status()["peer_idle"]["state"] == "open").await;
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        let before = cluster.status()["peer_idle"]["target"].clone();
        cluster.remove_idle_replica_via_fm().await?;
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "pending");
        until(&mut cluster, |cluster| cluster.ledger.idle_deletion_status()["complete"] == true
            && cluster.ledger.idle_removal_status()["removed"] == true
            && cluster.status()["peer_idle"]["target"]["open"] == false).await;
        let status = cluster.status();
        assert_eq!(status["peer_idle"]["fm_deletion"]["state"], "deleted");
        assert_eq!(status["peer_idle"]["fm_removal"]["destination_deleted"], true);
        assert_eq!(status["peer_idle"]["fm"]["destination_cleanup_required"], false);
        assert_eq!(status["peer_idle"]["target"]["open"], false);
        assert_eq!(status["peer_idle"]["target"]["open_token"], before["open_token"]);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 1);
        let proof = cluster.ledger.placements[1].retirement.clone().ok_or("missing original destination receipt")?;
        assert_eq!(proof.disposal.body.replica.id, cluster.ledger.placements[1].replica);
        assert_eq!(proof.disposal.body.replica.instance, cluster.ledger.placements[1].incarnation);
        assert!(!cluster.ledger.placements[1].reusable);
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "deleted");
        assert!(cluster.execute(command("replacement", "create", Some(2))).await.is_err());
        assert!(cluster.add_idle_replica().await.is_err());
        assert!(cluster.build_idle_replica().await.is_err());
        let write = data_contact(&primary, b"write-more\n");
        assert!(write.contains("committed=2 value=continued"), "{write}");
        assert_eq!(data_contact(&primary, b"read\n"), write);
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn peer_idle_stale_native_requests_cannot_reopen_and_peer_loss_preserves_fm() {
    let fixture = Fixture::new("peer-idle-stale");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let launches = [
        cluster.ledger.placements[0].launch.clone(),
        cluster.ledger.placements[1].launch.clone(),
    ];
    let result = node::report_tests::run_peer_idle_stale(&launches).await;
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap().unwrap();
}

fn command(op: &str, action: &str, node: Option<u8>) -> Command {
    Command {
        op: op.into(),
        action: action.into(),
        node,
    }
}
fn ready_reply(placement: &fm::Placement) -> AddInstanceReply {
    let body = placement.body().unwrap();
    AddInstanceReply {
        unit: body.unit,
        replica: Replica {
            state: 2,
            service_endpoint: "127.0.0.1:23456".into(),
            replication_endpoint: if body.service.stateful {
                "127.0.0.1:23457".into()
            } else {
                String::new()
            },
            ..body.replica
        },
        error: 0,
    }
}
fn remove_reply(placement: &fm::Placement) -> AddInstanceReply {
    let mut reply = ready_reply(placement);
    reply.replica = Replica {
        node: reply.replica.node,
        id: reply.replica.id,
        instance: reply.replica.instance,
        up: false,
        state: 3,
        ..Replica::default()
    };
    reply
}
fn retirement(placement: &fm::Placement) -> RetirementReceipt {
    RetirementReceipt {
        deployment: placement.launch.deployment.clone(),
        session: placement.launch.session.clone(),
        node: placement.launch.node.clone(),
        fm: placement.launch.fm.clone(),
        generation: placement.launch.generation.clone(),
        node_pid: 555,
        app_pid: placement.profile.as_ref().unwrap().app_pid,
        runtime_root: placement.profile.as_ref().unwrap().runtime_root.clone(),
        disposal: fube_node::local::InstanceDisposal {
            body: placement.body().unwrap(),
            host: "original-host".into(),
            runtime: "original-runtime".into(),
            command: 8,
            ra_operation: 19,
            lookup: 3,
            acquisition: 42,
        },
    }
}

fn pre_open_receipt(placement: &fm::Placement) -> PreOpenCancellationReceipt {
    PreOpenCancellationReceipt {
        deployment: placement.launch.deployment.clone(), session: placement.launch.session.clone(),
        node: placement.launch.node.clone(), fm: placement.launch.fm.clone(), generation: placement.launch.generation.clone(),
        node_pid: 555, app_pid: placement.profile.as_ref().unwrap().app_pid,
        runtime_root: placement.profile.as_ref().unwrap().runtime_root.clone(),
        cancellation: fube_node::local::PreOpenCancellation {
            body: pre_open_cancelled_body(&placement.body().unwrap()), creation: 1, command: 8, ra_operation: 19,
        },
    }
}

fn failed_registration_receipt(placement: &fm::Placement, error: i64) -> FailedRegistrationReceipt {
    FailedRegistrationReceipt {
        deployment: placement.launch.deployment.clone(), session: placement.launch.session.clone(),
        node: placement.launch.node.clone(), fm: placement.launch.fm.clone(), generation: placement.launch.generation.clone(),
        node_pid: 555, app_pid: placement.profile.as_ref().unwrap().app_pid,
        runtime_root: placement.profile.as_ref().unwrap().runtime_root.clone(),
        deletion: fube_node::local::FailedRegistrationDeletion {
            failure: fube_node::local::PreOpenRegistrationFailure {
                body: pre_open_cancelled_body(&placement.body().unwrap()), creation: 1, lookup: 2,
                command: 4, ra_operation: 3, error,
            },
            command: 8, ra_operation: 19,
        },
    }
}

fn absence_receipt(placement: &fm::Placement) -> AbsenceReceipt {
    let mut body = placement.body().unwrap();
    body.unit.previous = Epoch::default();
    body.unit.target = 0;
    body.unit.minimum = 0;
    body.replica = Replica { node: placement.launch.node.clone(), id: placement.replica,
        instance: placement.incarnation, up: false, state: 3, ..Replica::default() };
    AbsenceReceipt {
        deployment: placement.launch.deployment.clone(), session: placement.launch.session.clone(),
        node: placement.launch.node.clone(), fm: placement.launch.fm.clone(), generation: placement.launch.generation.clone(),
        node_pid: 555, app_pid: placement.profile.as_ref().unwrap().app_pid,
        runtime_root: placement.profile.as_ref().unwrap().runtime_root.clone(),
        absence: fube_node::local::ReplicaAbsence { body, command: 8, ra_operation: 19 },
    }
}

#[test]
fn serialized_retirement_join_rejects_missing_old_and_cross_owner_proofs_in_both_orders() {
    let parent = support::parent();
    let root = parent.join(format!("receipt-{}", uuid::Uuid::new_v4()));
    private_dir(&root).unwrap();
    let path = root.join("retirement.json");
    for proof_first in [false, true] {
        let mut ledger = ledger();
        ledger.accept(command("a", "create", Some(1))).unwrap();
        let first = ready_reply(&ledger.placements[0]);
        ledger.reply(0, first.clone(), false).unwrap();
        let first_result = ledger.result("a");
        ledger.accept(command("d", "delete", Some(1))).unwrap();
        let old_proof = retirement(&ledger.placements[0]);
        let removed = remove_reply(&ledger.placements[0]);
        if proof_first {
            write_json(&path, &old_proof).unwrap();
            ledger.read_retirement(0, &path, 555).unwrap();
            assert_eq!(ledger.result("d")["state"], "pending");
            assert!(ledger.accept(command("early", "create", Some(1))).is_err());
            ledger.reply(0, removed, true).unwrap();
        } else {
            let _ = fs::remove_file(&path);
            ledger.reply(0, removed, true).unwrap();
            ledger.read_retirement(0, &path, 555).unwrap();
            assert_eq!(ledger.result("d")["state"], "pending");
            assert!(ledger.accept(command("early", "create", Some(1))).is_err());
            write_json(&path, &old_proof).unwrap();
            ledger.read_retirement(0, &path, 555).unwrap();
        }
        assert_eq!(ledger.result("d")["cleanup"], "verified_original_instance");
        let deleted = ledger.result("d");
        ledger.accept(command("b", "create", Some(1))).unwrap();
        assert_eq!(ledger.placements[0].replica, first.replica.id + 1);
        assert_eq!(ledger.placements[0].incarnation, first.replica.instance + 1);
        ledger
            .reply(0, ready_reply(&ledger.placements[0]), false)
            .unwrap();
        ledger.accept(command("e", "delete", Some(1))).unwrap();
        ledger
            .reply(0, remove_reply(&ledger.placements[0]), true)
            .unwrap();
        // The serialized, otherwise valid old proof cannot discharge B.
        write_json(&path, &old_proof).unwrap();
        ledger.read_retirement(0, &path, 555).unwrap();
        assert_eq!(ledger.result("e")["state"], "pending");
        let identity = (
            ledger.placements[0].replica,
            ledger.placements[0].incarnation,
        );
        let operations = ledger.operations.len();
        assert!(ledger
            .accept(command("premature", "create", Some(1)))
            .is_err());
        assert_eq!(ledger.operations.len(), operations);
        assert_eq!(
            (
                ledger.placements[0].replica,
                ledger.placements[0].incarnation
            ),
            identity
        );
        let current = retirement(&ledger.placements[0]);
        let mut sibling = current.clone();
        sibling.session = ledger.placements[1].launch.session.clone();
        write_json(&path, &sibling).unwrap();
        assert!(ledger.read_retirement(0, &path, 555).is_err());
        assert_eq!(ledger.result("e")["state"], "pending");
        write_json(&path, &current).unwrap();
        ledger.read_retirement(0, &path, 555).unwrap();
        assert_eq!(ledger.result("e")["state"], "complete");
        ledger.accept(command("c", "create", Some(1))).unwrap();
        ledger.lost(0, "current link lost".into());
        assert_eq!(
            ledger.result("a"),
            first_result,
            "predecessor snapshot is frozen"
        );
        assert_eq!(ledger.result("d"), deleted);
        assert!(
            matches!(ledger.accept(command("a", "create", Some(1))).unwrap(),
                Accepted::Duplicate(value) if value == first_result)
        );
        assert!(ledger.accept(command("a", "create", Some(2))).is_err());
        assert!(ledger
            .accept(command("after-loss", "create", Some(1)))
            .is_err());
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn retired_native_frames_do_not_mutate_successor_and_identity_overflow_does_not_consume_history() {
    let mut ledger = ledger();
    ledger.accept(command("a", "create", Some(1))).unwrap();
    let old = ready_reply(&ledger.placements[0]);
    ledger.reply(0, old.clone(), false).unwrap();
    ledger.accept(command("d", "delete", Some(1))).unwrap();
    let removed = remove_reply(&ledger.placements[0]);
    ledger.reply(0, removed.clone(), true).unwrap();
    ledger
        .retirement(0, retirement(&ledger.placements[0]), 555)
        .unwrap();
    ledger.accept(command("b", "create", Some(1))).unwrap();
    let current = ready_reply(&ledger.placements[0]);
    ledger.reply(0, current.clone(), false).unwrap();
    let status = ledger.status();
    ledger.reply(0, old, false).unwrap();
    ledger.reply(0, removed.clone(), true).unwrap();
    let report = ReplicaUp {
        dropped: ObjectArray(vec![FailoverUnitInfo {
            unit: removed.unit,
            service: ledger.placements[0].body().unwrap().service,
            replicas: ObjectArray(vec![ReplicaInfo {
                description: removed.replica,
                intermediate_role: 1,
            }]),
            ..FailoverUnitInfo::default()
        }]),
        ..ReplicaUp::default()
    };
    assert!(ledger.placements[0].report(&report).unwrap().is_none());
    assert_eq!(ledger.status(), status);
    assert!(ledger.placements[0].link_alive);
    ledger.accept(command("e", "delete", Some(1))).unwrap();
    ledger
        .reply(0, remove_reply(&ledger.placements[0]), true)
        .unwrap();
    ledger
        .retirement(0, retirement(&ledger.placements[0]), 555)
        .unwrap();
    ledger.placements[0].incarnation = i64::MAX;
    let prior = ledger.operations.len();
    let replica = ledger.placements[0].replica;
    assert!(ledger
        .accept(command("overflow", "create", Some(1)))
        .is_err());
    assert_eq!(ledger.operations.len(), prior);
    assert_eq!(ledger.placements[0].replica, replica);
}

#[test]
fn private_launch_metadata_roundtrip_does_not_use_native_byte_array_json_visitors() {
    let launch = launches()[0].clone();
    let bytes = serde_json::to_vec(&launch).unwrap();
    let decoded: Launch = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.node.id, [1; 16]);
    assert_eq!(decoded.node.instance, 101);
    assert_eq!(decoded.fm.id, [9; 16]);
    assert_eq!(decoded.fm.instance, 99);
    assert_eq!(decoded.generation.number.owner, [9; 16]);
    assert_eq!(decoded.generation.number.generation, 7);
    assert!(!decoded.generation.for_fm_replica);
    let metadata = Metadata {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node,
        pid: 555,
        address: "127.0.0.1:23456".parse().unwrap(),
        peer_address: None,
        profile: Some(profile(&decoded)),
    };
    let bytes = serde_json::to_vec(&metadata).unwrap();
    let decoded: Metadata = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.pid, 555);
    assert_eq!(decoded.node.instance, 101);
    assert_eq!(decoded.profile.unwrap().version.instance, 12345);
}

#[test]
fn semantic_completion_is_exact_and_writes_never_complete_an_operation() {
    let mut ledger = ledger();
    assert!(matches!(
        ledger.accept(command("a", "create", Some(1))).unwrap(),
        Accepted::Create(0, _)
    ));
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    assert_eq!(ledger.result("a")["state"], "pending");
    let reply = ready_reply(&ledger.placements[0]);
    let mut stale = reply.clone();
    stale.replica.node.instance -= 1;
    assert!(ledger.reply(0, stale, false).is_err());
    let mut stale = reply.clone();
    stale.unit.current.configuration += 1;
    assert!(ledger.reply(0, stale, false).is_err());
    let mut stale = reply.clone();
    stale.replica.instance -= 1;
    assert!(ledger.reply(0, stale, false).is_err());
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    ledger.reply(0, reply.clone(), false).unwrap();
    assert_eq!(ledger.placements[0].observed, Observed::Running);
    let count = ledger.operations.len();
    assert!(matches!(
        ledger.accept(command("a", "create", Some(1))).unwrap(),
        Accepted::Duplicate(_)
    ));
    assert_eq!(ledger.operations.len(), count);
    assert!(ledger.accept(command("a", "create", Some(2))).is_err());
    ledger.accept(command("d", "delete", Some(1))).unwrap();
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    assert_eq!(ledger.placements[0].desired, Desired::Absent);
    assert!(ledger.reply(0, reply.clone(), true).is_err());
    let mut disposed = reply;
    disposed.replica.state = 3;
    disposed.replica.up = false;
    disposed.replica.version = VersionInstance::invalid();
    disposed.replica.service_endpoint.clear();
    ledger.reply(0, disposed, true).unwrap();
    assert_eq!(ledger.placements[0].observed, Observed::Pending);
    assert_eq!(ledger.result("d")["state"], "pending");
    assert!(ledger.accept(command("new", "create", Some(1))).is_err());
}

#[test]
fn notification_catalog_uses_generated_version_and_unsupported_work_is_not_echoed() {
    let mut ledger = ledger();
    let placement = &ledger.placements[0];
    let body = ServiceTypeNotification {
        infos: ObjectArray(vec![placement.profile.as_ref().unwrap().info()]),
    };
    assert_eq!(placement.notification(&body).unwrap(), body);
    let mut wrong = body.clone();
    wrong.infos.0[0].versioned.version.instance += 1;
    assert!(placement.notification(&wrong).is_err());
    wrong = body.clone();
    wrong.infos.0[0].code_package = "Other".into();
    assert!(placement.notification(&wrong).is_err());
    let final_upload = ReplicaUp {
        last: true,
        ..ReplicaUp::default()
    };
    assert!(placement.report(&final_upload).is_err());
    ledger.placements[0].bootstrap = Bootstrap::NodeUp;
    assert!(ledger.placements[0].report(&final_upload).is_err());
    let launch = ledger.placements[0].launch.clone();
    assert!(ledger.placements[0]
        .admit_profile(profile(&launch))
        .is_err());
    ledger.placements[0].bootstrap = Bootstrap::FinalUpload;
    assert_eq!(
        ledger.placements[0].report(&final_upload).unwrap(),
        Some(final_upload.clone())
    );
    let mut unsupported = final_upload;
    unsupported.from_fmm = true;
    assert!(ledger.placements[0].report(&unsupported).is_err());
}

#[test]
fn finite_identities_are_not_evicted_and_uncertainty_retains_authority() {
    let mut ledger = ledger();
    ledger.accept(command("create", "create", Some(1))).unwrap();
    ledger.lost(0, "link lost".into());
    assert_eq!(ledger.placements[0].desired, Desired::Running);
    assert_eq!(ledger.placements[0].observed, Observed::Unknown);
    assert_eq!(ledger.result("create")["state"], "unknown");
    assert!(ledger
        .accept(command("replace", "create", Some(1)))
        .is_err());
    assert!(ledger.accept(command("remove", "delete", Some(1))).is_err());
    for index in 1..64 {
        ledger
            .accept(command(&format!("status{index}"), "status", None))
            .unwrap();
    }
    assert!(ledger.accept(command("overflow", "status", None)).is_err());
    assert!(matches!(
        ledger.accept(command("create", "create", Some(1))).unwrap(),
        Accepted::Duplicate(_)
    ));
    assert_eq!(ledger.operations.len(), 64);
    assert!(serde_json::from_str::<Command>(
        r#"{"op":"x","action":"create","node":1,"force":true}"#
    )
    .is_err());
    assert!(command("x", "create", Some(3)).validate().is_err());

    let mut repeated = self::ledger();
    for cycle in 0..2 {
        repeated
            .accept(command(&format!("c{cycle}"), "create", Some(1)))
            .unwrap();
        repeated
            .reply(0, ready_reply(&repeated.placements[0]), false)
            .unwrap();
        repeated
            .accept(command(&format!("d{cycle}"), "delete", Some(1)))
            .unwrap();
        repeated
            .reply(0, remove_reply(&repeated.placements[0]), true)
            .unwrap();
        repeated
            .retirement(0, retirement(&repeated.placements[0]), 555)
            .unwrap();
    }
    let old = repeated.result("c0");
    let replica = repeated.placements[0].replica;
    for index in 4..64 {
        repeated
            .accept(command(&format!("s{index}"), "status", None))
            .unwrap();
    }
    assert!(repeated
        .accept(command("capacity", "create", Some(1)))
        .is_err());
    assert_eq!(repeated.placements[0].replica, replica);
    assert_eq!(repeated.operations.len(), 64);
    assert!(
        matches!(repeated.accept(command("c0", "create", Some(1))).unwrap(),
        Accepted::Duplicate(value) if value == old)
    );
    assert!(repeated.accept(command("c0", "create", Some(2))).is_err());
}

#[test]
fn semantic_remove_then_disconnect_cannot_be_repaired_by_a_late_local_receipt() {
    let mut ledger = ledger();
    ledger.accept(command("c", "create", Some(1))).unwrap();
    ledger
        .reply(0, ready_reply(&ledger.placements[0]), false)
        .unwrap();
    ledger.accept(command("d", "delete", Some(1))).unwrap();
    let proof = retirement(&ledger.placements[0]);
    ledger
        .reply(0, remove_reply(&ledger.placements[0]), true)
        .unwrap();
    assert_eq!(ledger.result("d")["state"], "pending");
    ledger.lost(0, "original channel lost before proof".into());
    ledger.retirement(0, proof, 555).unwrap();
    assert_eq!(ledger.result("d")["state"], "unknown");
    assert!(!ledger.placements[0].reusable);
    assert!(ledger.accept(command("unsafe", "create", Some(1))).is_err());
}

#[path = "../tests/support/mod.rs"]
mod support;

struct Fixture {
    root: PathBuf,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let parent = support::parent();
        assert!(parent.is_absolute() && parent.is_dir());
        let root = parent.join(format!("{label}-{}", uuid::Uuid::new_v4()));
        Self { root }
    }
    fn config(&self) -> Config {
        Config {
            profile: ServiceProfile::Stateless,
            root: self.root.clone(),
            app: support::binary("cluster-stateless-app", "FUBE_CLUSTER_APP"),
            executable: support::binary("fube-cluster", "FUBE_CLUSTER_NODE_EXE"),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.root.join("handoff.json").exists()
            && read_json::<Value>(&self.root.join("handoff.json")).unwrap()["clean"] == true
        {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
}
async fn until(cluster: &mut Cluster, predicate: impl Fn(&Cluster) -> bool) {
    until_with_budget(cluster, Duration::from_secs(12), predicate).await;
}
async fn until_with_budget(
    cluster: &mut Cluster,
    budget: Duration,
    predicate: impl Fn(&Cluster) -> bool,
) {
    let end = Instant::now() + budget;
    loop {
        cluster.drive().await;
        if predicate(cluster) {
            break;
        }
        assert!(
            Instant::now() < end,
            "progress deadline: {}",
            cluster.status()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
fn contact(endpoint: &str) -> Value {
    let mut socket =
        TcpStream::connect_timeout(&endpoint.parse().unwrap(), Duration::from_secs(1)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket.write_all(b"identity\n").unwrap();
    let mut bytes = vec![];
    socket.take(4097).read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() <= 4096 && bytes.last() == Some(&b'\n'));
    serde_json::from_slice(&bytes).unwrap()
}
fn assert_identity(actual: &Value, launch: &Launch, node_pid: u32, app_pid: u32) {
    // Expected identities come from original launch authority and actual owned
    // process metadata, captured BEFORE reading any FM endpoint/result.
    assert_eq!(actual["profile"], "fube-cluster-stateless-v1");
    assert_eq!(actual["node"], node_hex(&launch.node));
    assert_eq!(actual["node_instance"], launch.node.instance);
    assert_eq!(actual["node_pid"], node_pid);
    assert_eq!(actual["app_pid"], app_pid);
    assert_eq!(actual["service"], SERVICE);
    assert_eq!(actual["service_type"], TYPE);
    assert_eq!(actual["partition"], launch.partition);
    assert_eq!(actual["replica"], launch.replica);
    assert_eq!(actual["incarnation"], launch.incarnation as u64);
}
#[tokio::test]
async fn real_generation_handshake_uses_correlated_replies_and_keeps_upload_gated() {
    let fixture = Fixture::new("generation-handshake");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    let result: Result<()> = async {
        let link = cluster.children[0].link.as_ref().unwrap();
        let initial = cluster.children[0].launch.generation.clone();
        let mut high = initial.clone();
        high.number.generation += 10;
        link.send(
            generation_update_message(
                &GenerationUpdate {
                    fm_service_epoch: Epoch::default(),
                },
                &high,
            )
            .map_err(err)?,
        )
        .await?;
        let mut rejected = link
            .receive(wire::deadline())
            .await?
            .ok_or("missing native GenerationUpdateReject")?;
        let proposed = decode_generation_update_reject(&rejected.message).map_err(err)?;
        assert_eq!(proposed, initial.number);
        if let Some(handoff) = rejected.handoff.as_mut() {
            handoff.acknowledge();
        }
        let reply = link
            .request(generation_proposal_message(&high).map_err(err)?)
            .await?;
        let reply = decode_generation_proposal_reply(&reply).map_err(err)?;
        assert_eq!(reply.proposed, high.number);
        assert_eq!(reply.current, high.number);
        assert_eq!(reply.error, 0);
        cluster
            .execute(command("sibling", "create", Some(2)))
            .await?;
        until(&mut cluster, |c| {
            c.ledger.placements[1].observed == Observed::Running
        })
        .await;
        let sibling = cluster.ledger.placements[1].endpoint.clone().unwrap();
        let started = cluster.begin_generation_handshake()?;
        assert_eq!(started["complete"], false);
        until(&mut cluster, |c| {
            let Some(round) = &c.ledger.generation else {
                return false;
            };
            round.number.generation > high.number.generation
                && round.updates_sent == [true; 2]
                && c.status()["nodes"].as_array().unwrap().iter().all(|node| {
                    node["retained_work"]["generations"]["upload_pending"] == true
                        && node["retained_work"]["generations"]["receive"] == json!(round.number)
                })
        })
        .await;
        let status = cluster.status();
        assert_eq!(status["generation_handshake"]["phase"], "awaiting_upload");
        assert_eq!(status["generation_handshake"]["complete"], false);
        for node in status["nodes"].as_array().unwrap() {
            assert_eq!(
                node["retained_work"]["generations"]["send"],
                json!(initial.number)
            );
            assert_eq!(node["link_alive"], true);
        }
        assert_eq!(contact(&sibling)["service"], SERVICE);
        assert!(!cluster.ledger.placements[0].created);
        assert!(cluster
            .execute(command("blocked", "create", Some(1)))
            .await
            .is_err());
        Ok(())
    }
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap();
}

#[tokio::test]
async fn stateful_add_primary_lost_reply_retries_the_original_native_lifetime() {
    let fixture = Fixture::new("primary-retry");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulSingleton;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    cluster
        .execute(command("primary", "create", Some(1)))
        .await
        .unwrap();
    let end = Instant::now() + Duration::from_secs(8);
    let original = loop {
        let link = cluster.children[0].link.as_ref().unwrap();
        if let Some(mut incoming) = link
            .receive(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
        {
            let action = incoming.message.properties().unwrap().action;
            let reply = match action.as_str() {
                "AddPrimaryReply" => Some(decode_add_primary_reply(&incoming.message).unwrap().0),
                "ServiceTypeNotification" => {
                    let body = decode_service_type_notification(&incoming.message).unwrap();
                    let admitted = cluster.ledger.placements[0].notification(&body).unwrap();
                    link.send(
                        service_type_notification_reply_message(
                            &admitted,
                            &cluster.children[0].launch.generation,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                    None
                }
                other => panic!("unexpected original Primary ingress {other}"),
            };
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
            if let Some(reply) = reply {
                break reply;
            } // Intentionally lose semantic processing, not the live link.
        }
        assert!(Instant::now() < end, "initial Primary reply not observed");
    };
    assert_eq!(cluster.ledger.placements[0].observed, Observed::Pending);
    assert!(cluster.ledger.placements[0].endpoint.is_none());
    let app_pid = cluster.children[0]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    until_with_budget(&mut cluster, Duration::from_secs(15), |cluster| {
        cluster.ledger.placements[0].observed == Observed::Running
    })
    .await;
    let placement = &cluster.ledger.placements[0];
    assert_eq!(
        (placement.replica, placement.incarnation),
        (original.replica.id, original.replica.instance)
    );
    assert_eq!(
        placement.endpoint.as_ref().unwrap(),
        &original.replica.service_endpoint
    );
    let endpoint = placement.endpoint.clone().unwrap();
    let mut socket = TcpStream::connect(&endpoint).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket.write_all(b"write\n").unwrap();
    let mut response = String::new();
    socket.take(512).read_to_string(&mut response).unwrap();
    assert!(
        response.contains("assigned=1 applied=1 committed=1 value=acknowledged"),
        "{response}"
    );
    assert!(cluster
        .ledger
        .primary_retry(0, Instant::now() + Duration::from_secs(100))
        .unwrap()
        .is_none());
    assert!(cluster.shutdown().await["clean"] == true);
    assert!(gone(app_pid));
    assert!(TcpStream::connect(&endpoint).is_err());
}

#[test]
fn stateful_primary_retry_preserves_native_minimum_and_maximum_action_intervals() {
    let mut ledger = stateful_ledger();
    ledger.accept(command("create", "create", Some(1))).unwrap();
    let initial = ledger.placements[0].body().unwrap();
    let now = Instant::now();
    assert!(ledger
        .primary_retry(0, now + Duration::from_secs(9))
        .unwrap()
        .is_none());
    assert_eq!(
        ledger
            .primary_retry(0, now + Duration::from_secs(11))
            .unwrap()
            .unwrap(),
        initial
    );
    assert!(ledger
        .primary_retry(0, now + Duration::from_secs(61))
        .unwrap()
        .is_none());
    assert_eq!(
        ledger
            .primary_retry(0, now + Duration::from_secs(72))
            .unwrap()
            .unwrap(),
        initial
    );
    assert!(ledger.accept(command("delete", "delete", Some(1))).is_err());
    assert!(ledger
        .accept(command("replacement", "create", Some(1)))
        .is_err());
}

#[test]
fn stateful_delete_requires_both_original_inputs_before_explicit_fresh_recreation() {
    for proof_first in [false, true] {
        let mut ledger = stateful_ledger();
        assert!(ledger
            .accept(command("missing", "delete", Some(1)))
            .is_err());
        ledger.accept(command("create", "create", Some(1))).unwrap();
        assert!(ledger
            .accept(command("opening", "delete", Some(1)))
            .is_err());
        ledger
            .reply(0, ready_reply(&ledger.placements[0]), false)
            .unwrap();
        let original = ledger.placements[0].body().unwrap();
        let Accepted::Delete(_, request) =
            ledger.accept(command("delete", "delete", Some(1))).unwrap()
        else {
            panic!("normal stateful delete expected");
        };
        assert_eq!(*request, DeleteReplica::new(original.clone(), false));
        assert!(ledger
            .primary_retry(0, Instant::now() + Duration::from_secs(100))
            .unwrap()
            .is_none());
        let now = Instant::now();
        assert!(ledger
            .deletion_retry(0, now + Duration::from_secs(9))
            .unwrap()
            .is_none());
        assert_eq!(
            ledger
                .deletion_retry(0, now + Duration::from_secs(11))
                .unwrap()
                .unwrap(),
            *request
        );
        assert!(ledger
            .deletion_retry(0, now + Duration::from_secs(61))
            .unwrap()
            .is_none());
        assert_eq!(
            ledger
                .deletion_retry(0, now + Duration::from_secs(72))
                .unwrap()
                .unwrap(),
            *request
        );
        let correct = remove_reply(&ledger.placements[0]);
        for forged in 0..4 {
            let mut reply = correct.clone();
            match forged {
                0 => reply.replica.current_role = 4,
                1 => reply.replica.up = true,
                2 => reply.replica.replication_endpoint = "127.0.0.1:23457".into(),
                _ => reply.replica.instance += 1,
            }
            assert!(ledger.reply(0, reply, true).is_err());
            assert_eq!(ledger.result("delete")["state"], "pending");
        }
        let mut foreign = retirement(&ledger.placements[0]);
        foreign.disposal.acquisition = 0;
        assert!(ledger.retirement(0, foreign, 555).is_err());
        if proof_first {
            ledger
                .retirement(0, retirement(&ledger.placements[0]), 555)
                .unwrap();
        } else {
            ledger.reply(0, correct.clone(), true).unwrap();
            assert!(ledger
                .deletion_retry(0, now + Duration::from_secs(200))
                .unwrap()
                .is_none());
        }
        assert_eq!(ledger.result("delete")["state"], "pending");
        assert_eq!(ledger.placements[0].observed, Observed::Pending);
        assert!(ledger
            .accept(command("premature", "create", Some(1)))
            .is_err());
        if proof_first {
            ledger.reply(0, correct.clone(), true).unwrap();
        } else {
            ledger
                .retirement(0, retirement(&ledger.placements[0]), 555)
                .unwrap();
        }
        assert_eq!(ledger.result("delete")["state"], "complete");
        assert_eq!(ledger.placements[0].observed, Observed::Absent);
        assert!(ledger.placements[0].reusable);
        assert_eq!(
            ledger.placements[0].retired,
            [(original.replica.id, original.replica.instance)]
        );
        let old_proof = retirement(&ledger.placements[0]);
        ledger.reply(0, correct.clone(), true).unwrap();
        ledger
            .retirement(0, retirement(&ledger.placements[0]), 555)
            .unwrap();
        assert!(matches!(
            ledger.accept(command("delete", "delete", Some(1))).unwrap(),
            Accepted::Duplicate(_)
        ));
        assert!(ledger
            .deletion_retry(0, now + Duration::from_secs(200))
            .unwrap()
            .is_none());
        ledger
            .accept(command("replacement", "create", Some(1)))
            .unwrap();
        assert_eq!(ledger.placements[0].replica, original.replica.id + 1);
        assert_eq!(
            ledger.placements[0].incarnation,
            original.replica.instance + 1
        );
        let fresh = ledger.status();
        ledger.reply(0, correct, true).unwrap();
        ledger.retirement(0, old_proof, 555).unwrap();
        assert_eq!(ledger.status(), fresh);
        assert_eq!(ledger.result("replacement")["state"], "pending");
    }
}

#[tokio::test]
async fn stateful_missing_delete_is_metadata_only_and_fresh_open_has_its_own_disposal() {
    let fixture = Fixture::new("stateful-tombstone");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulSingleton;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.prepare().unwrap();
    let result = node::report_tests::run_stateful_tombstone(&cluster.ledger.placements[0].launch)
        .await
        .unwrap();
    assert_eq!(cluster.shutdown().await["clean"], true);
    result.unwrap();
}

#[tokio::test]
async fn stateful_delete_lost_reply_retries_original_lifetime_after_independent_write_readback() {
    let fixture = Fixture::new("stateful-delete");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulSingleton;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    for node in 1..=2 {
        cluster
            .execute(command(&format!("primary-{node}"), "create", Some(node)))
            .await
            .unwrap();
    }
    until(&mut cluster, |c| {
        c.ledger
            .placements
            .iter()
            .all(|p| p.observed == Observed::Running)
    })
    .await;
    let profile = cluster.children[0]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .clone();
    let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
    let sibling = cluster.ledger.placements[1].endpoint.clone().unwrap();
    let request = |endpoint: &str, command: &[u8]| {
        let mut socket = TcpStream::connect(endpoint).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(command).unwrap();
        let mut response = String::new();
        socket.take(512).read_to_string(&mut response).unwrap();
        response
    };
    until(&mut cluster, |_| {
        request(&endpoint, b"read\n").starts_with("OK ")
    })
    .await;
    let written = request(&endpoint, b"write\n");
    assert!(
        written.contains("assigned=1 applied=1 committed=1 value=acknowledged"),
        "{written}"
    );
    assert_eq!(request(&endpoint, b"read\n"), written);
    let original = cluster.ledger.placements[0].body().unwrap();
    cluster
        .execute(command("delete", "delete", Some(1)))
        .await
        .unwrap();
    let end = Instant::now() + Duration::from_secs(8);
    loop {
        let link = cluster.children[0].link.as_ref().unwrap();
        if let Some(mut incoming) = link
            .receive(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
        {
            let action = incoming.message.properties().unwrap().action;
            let lost = match action.as_str() {
                "DeleteReplicaReply" => {
                    let reply = decode_delete_replica_reply(&incoming.message).unwrap().0;
                    assert_eq!(
                        (reply.replica.id, reply.replica.instance),
                        (original.replica.id, original.replica.instance)
                    );
                    true
                }
                "ServiceTypeNotification" => {
                    let body = decode_service_type_notification(&incoming.message).unwrap();
                    link.send(
                        service_type_notification_reply_message(
                            &body,
                            &cluster.children[0].launch.generation,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                    false
                }
                other => panic!("unexpected DeleteReplica ingress {other}"),
            };
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
            if lost {
                break;
            }
        }
        assert!(
            Instant::now() < end,
            "initial DeleteReplicaReply not observed"
        );
    }
    // Physical/service retirement by itself must not complete the lost FM reply.
    until(&mut cluster, |c| {
        c.ledger.placements[0].retirement.is_some()
    })
    .await;
    assert_eq!(cluster.ledger.result("delete")["state"], "pending");
    assert!(TcpStream::connect(&endpoint).is_err());
    assert!(Path::new(&format!("/proc/{}", profile.app_pid)).exists());
    assert!(request(&sibling, b"read\n").starts_with("OK "));
    until_with_budget(&mut cluster, Duration::from_secs(15), |c| {
        c.ledger.placements[0].observed == Observed::Absent
    })
    .await;
    assert_eq!(cluster.ledger.result("delete")["state"], "complete");
    assert_eq!(cluster.ledger.placements[0].replica, original.replica.id);
    assert!(cluster.ledger.placements[0].reusable);
    assert_eq!(
        cluster
            .execute(command("delete", "delete", Some(1)))
            .await
            .unwrap()["state"],
        "complete"
    );
    let old_proof = cluster.ledger.placements[0].retirement.clone().unwrap();
    cluster
        .execute(command("recreate", "create", Some(1)))
        .await
        .unwrap();
    until(&mut cluster, |c| {
        c.ledger.placements[0].observed == Observed::Running
    })
    .await;
    let fresh_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
    until(&mut cluster, |_| {
        request(&fresh_endpoint, b"read\n").starts_with("OK ")
    })
    .await;
    let empty = request(&fresh_endpoint, b"read\n");
    assert!(
        empty.contains("assigned=0 applied=0 committed=0 value=\n"),
        "{empty}"
    );
    let fresh_written = request(&fresh_endpoint, b"write\n");
    assert!(fresh_written.contains("assigned=1 applied=1 committed=1 value=acknowledged"));
    assert_eq!(request(&fresh_endpoint, b"read\n"), fresh_written);
    assert_eq!(
        cluster.ledger.placements[0].replica,
        original.replica.id + 1
    );
    assert_eq!(
        cluster.ledger.placements[0].incarnation,
        original.replica.instance + 1
    );
    let fresh = cluster.ledger.placements[0].body().unwrap();
    let mut old_placement = cluster.ledger.placements[0].clone();
    old_placement.replica = original.replica.id;
    old_placement.incarnation = original.replica.instance;
    let stale_ack = stateful_loss_report(&old_placement);
    let link = cluster.children[0].link.as_ref().unwrap();
    let generation = &cluster.children[0].launch.generation;
    for message in [
        add_primary_message(&original, generation).unwrap(),
        delete_replica_message(&DeleteReplica::new(original.clone(), false), generation).unwrap(),
        replica_up_reply_message(&stale_ack, generation).unwrap(),
        add_primary_message(&fresh, generation).unwrap(),
    ] {
        link.send(message).await.unwrap();
    }
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(mut incoming) = link
            .receive(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
        {
            let action = incoming.message.properties().unwrap().action;
            let barrier = match action.as_str() {
                "AddPrimaryReply" => {
                    let reply = decode_add_primary_reply(&incoming.message).unwrap().0;
                    assert_eq!(
                        (reply.replica.id, reply.replica.instance),
                        (fresh.replica.id, fresh.replica.instance)
                    );
                    assert_eq!(reply.error, 0);
                    true
                }
                "ServiceTypeNotification" => {
                    let body = decode_service_type_notification(&incoming.message).unwrap();
                    link.send(service_type_notification_reply_message(&body, generation).unwrap())
                        .await
                        .unwrap();
                    false
                }
                other => panic!("stale frames emitted unexpected successor work {other}"),
            };
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
            if barrier {
                break;
            }
        }
        assert!(
            Instant::now() < end,
            "same-session successor reply barrier did not complete"
        );
    }
    assert_eq!(request(&fresh_endpoint, b"read\n"), fresh_written);
    cluster
        .execute(command("delete-fresh", "delete", Some(1)))
        .await
        .unwrap();
    until(&mut cluster, |c| {
        c.ledger.placements[0].observed == Observed::Absent
    })
    .await;
    let new_proof = cluster.ledger.placements[0].retirement.as_ref().unwrap();
    assert_ne!(old_proof.disposal.lookup, new_proof.disposal.lookup);
    assert_ne!(
        old_proof.disposal.acquisition,
        new_proof.disposal.acquisition
    );
    assert_eq!(cluster.ledger.placements[0].retired.len(), 2);
    assert!(TcpStream::connect(&fresh_endpoint).is_err());
    assert!(request(&sibling, b"read\n").starts_with("OK "));
    assert_eq!(
        cluster.children[0]
            .metadata
            .as_ref()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .app_pid,
        profile.app_pid
    );
    assert_eq!(cluster.shutdown().await["clean"], true);
    assert!(gone(profile.app_pid));
    assert!(!profile.runtime_root.exists());
}

fn stateful_loss_report(placement: &fm::Placement) -> ReplicaUp {
    let body = placement.body().unwrap();
    ReplicaUp {
        dropped: ObjectArray(vec![FailoverUnitInfo {
            unit: body.unit,
            service: body.service,
            intermediate: Epoch::default(),
            report_from_primary: false,
            replicas: ObjectArray(vec![ReplicaInfo {
                description: remove_reply(placement).replica,
                intermediate_role: 1,
            }]),
            local_deleted: false,
        }]),
        ..ReplicaUp::default()
    }
}

#[test]
fn stateful_loss_is_processed_not_echoed_and_native_deletion_never_supplies_disposal_proof() {
    for deleting in [false, true] {
        let mut ledger = stateful_ledger();
        ledger.accept(command("create", "create", Some(1))).unwrap();
        ledger
            .reply(0, ready_reply(&ledger.placements[0]), false)
            .unwrap();
        let original = ledger.placements[0].body().unwrap();
        if deleting {
            ledger.accept(command("delete", "delete", Some(1))).unwrap();
        }
        let report = stateful_loss_report(&ledger.placements[0]);
        for forged in 0..5 {
            let mut invalid = report.clone();
            let info = &mut invalid.dropped.0[0];
            match forged {
                0 => info.replicas.0[0].description.up = true,
                1 => info.replicas.0[0].description.instance -= 1,
                2 => info.replicas.0[0].description.current_role = 4,
                3 => info.local_deleted = true,
                _ => info.unit.current.configuration += 1,
            }
            assert!(ledger.report(0, &invalid).is_err());
            assert!(!ledger.placements[0].loss_reported);
        }
        let ack = ledger.report(0, &report).unwrap().unwrap();
        assert_eq!(ack.replicas, report.dropped);
        assert!(ack.dropped.0.is_empty() && !ack.last && !ack.from_fmm);
        assert_eq!(ledger.placements[0].observed, Observed::Lost);
        assert_eq!(
            ledger.placements[0].desired,
            if deleting {
                Desired::Absent
            } else {
                Desired::Running
            }
        );
        assert!(ledger.placements[0].link_alive && ledger.placements[0].endpoint.is_none());
        assert!(ledger.placements[0].retirement.is_none() && !ledger.placements[0].reusable);
        assert_eq!(ledger.result("create")["state"], "lost");
        let now = Instant::now();
        assert!(ledger
            .deletion_retry(0, now + Duration::from_secs(9))
            .unwrap()
            .is_none());
        assert_eq!(
            ledger
                .deletion_retry(0, now + Duration::from_secs(11))
                .unwrap()
                .unwrap(),
            DeleteReplica::new(original, false)
        );
        ledger
            .reply(0, ready_reply(&ledger.placements[0]), false)
            .unwrap();
        assert_eq!(ledger.placements[0].observed, Observed::Lost);
        assert_eq!(ledger.report(0, &report).unwrap(), Some(ack));
        let mut failed = ready_reply(&ledger.placements[0]);
        failed.error = -7;
        ledger.reply(0, failed, true).unwrap();
        assert!(ledger.placements[0].error.as_ref().unwrap().contains("-7"));
        assert_eq!(
            ledger.placements[0].status()["native_replica_deleted"],
            false
        );
        ledger
            .reply(0, remove_reply(&ledger.placements[0]), true)
            .unwrap();
        ledger
            .reply(0, remove_reply(&ledger.placements[0]), true)
            .unwrap();
        assert_eq!(
            ledger.placements[0].status()["native_replica_deleted"],
            true
        );
        assert!(ledger
            .deletion_retry(0, now + Duration::from_secs(200))
            .unwrap()
            .is_none());
        let before = ledger.status();
        ledger
            .retirement(0, retirement(&ledger.placements[0]), 555)
            .unwrap();
        assert_eq!(ledger.status(), before);
        let mut foreign = retirement(&ledger.placements[0]);
        foreign.node.instance += 1;
        assert!(ledger.retirement(0, foreign, 555).is_err());
        assert_eq!(ledger.placements[0].observed, Observed::Lost);
        assert!(!ledger.placements[0].reusable && ledger.placements[0].retirement.is_none());
        assert!(ledger.placements[0].retired.is_empty());
        assert!(ledger
            .accept(command("replacement", "create", Some(1)))
            .is_err());
        assert!(ledger
            .accept(command("fresh-delete", "delete", Some(1)))
            .is_err());
        if deleting {
            assert_eq!(ledger.result("delete")["state"], "lost");
        }
    }
}

#[tokio::test]
async fn stateful_app_terminal_and_lost_ack_retire_only_original_reporting_without_touching_sibling(
) {
    use node::report_tests::StatefulDown;
    for mode in [
        StatefulDown::Crash,
        StatefulDown::LeaseLoss,
        StatefulDown::CloseProvider,
        StatefulDown::CloseService,
        StatefulDown::DropProvider,
        StatefulDown::DropService,
        StatefulDown::AbortProvider,
        StatefulDown::AbortService,
        StatefulDown::ForceAbortProvider,
        StatefulDown::ForceAbortService,
        StatefulDown::ForceDelete,
    ] {
        let fixture = Fixture::new("stateful-loss");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulSingleton;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        cluster.spawn(0).await.unwrap();
        cluster
            .execute(command("sibling", "create", Some(1)))
            .await
            .unwrap();
        until(&mut cluster, |c| {
            c.ledger.placements[0].observed == Observed::Running
        })
        .await;
        let sibling = cluster.ledger.placements[0].endpoint.clone().unwrap();
        let sibling_pid = cluster.children[0]
            .metadata
            .as_ref()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .app_pid;
        let request = |command: &[u8]| {
            let mut socket = TcpStream::connect(&sibling).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            socket.write_all(command).unwrap();
            let mut response = String::new();
            socket.take(512).read_to_string(&mut response).unwrap();
            response
        };
        until(&mut cluster, |_| request(b"read\n").starts_with("OK ")).await;
        let written = request(b"write\n");
        assert!(
            written.contains("assigned=1 applied=1 committed=1 value=acknowledged"),
            "{written}"
        );
        let launch = cluster.ledger.placements[1].launch.clone();
        let result =
            node::report_tests::run_stateful_down(&launch, &mut cluster.ledger, mode).await;
        assert_eq!(request(b"read\n"), written);
        cluster.children[0]
            .process
            .as_mut()
            .unwrap()
            .check_alive()
            .unwrap();
        assert_eq!(
            cluster.children[0]
                .metadata
                .as_ref()
                .unwrap()
                .profile
                .as_ref()
                .unwrap()
                .app_pid,
            sibling_pid
        );
        let loss = cluster.ledger.placements[1].status();
        assert_eq!(loss["observed"], "lost");
        assert_eq!(loss["loss_reported"], true);
        assert_eq!(loss["native_replica_deleted"], true);
        assert_eq!(loss["reusable"], false);
        assert!(loss["retirement"].is_null());
        assert_eq!(cluster.shutdown().await["clean"], true);
        assert!(gone(sibling_pid));
        result.unwrap().unwrap();
    }
}

fn gone(pid: u32) -> bool {
    !Path::new(&format!("/proc/{pid}")).exists()
}

#[test]
fn node_intent_supersedes_pending_work_without_accepting_stale_completion_or_creation() {
    use fm::Accepted;
    let mut ledger = ledger();
    let Accepted::NodeControl(_, false, first) = ledger
        .accept(command("deactivate", "deactivate", Some(1)))
        .unwrap()
    else {
        panic!("deactivation");
    };
    assert!(ledger
        .accept(command("premature-create", "create", Some(1)))
        .is_err());
    let now = Instant::now();
    assert!(ledger.node_control_retry(0, now).is_none());
    let retry = ledger
        .node_control_retry(0, now + Duration::from_secs(6))
        .unwrap();
    assert!(!retry.0 && retry.1 == first);
    let Accepted::NodeControl(_, true, next) = ledger
        .accept(command("activate", "activate", Some(1)))
        .unwrap()
    else {
        panic!("activation");
    };
    assert!(next.sequence > first.sequence);
    assert_eq!(ledger.result("deactivate")["state"], "unknown");
    ledger
        .node_control_reply(
            0,
            false,
            NodeActivationReply {
                sequence: first.sequence,
            },
        )
        .unwrap();
    assert_eq!(ledger.result("activate")["state"], "pending");
    ledger
        .node_control_reply(
            0,
            true,
            NodeActivationReply {
                sequence: next.sequence,
            },
        )
        .unwrap();
    assert_eq!(ledger.result("activate")["state"], "complete");
    assert!(ledger
        .node_control_retry(0, now + Duration::from_secs(60))
        .is_none());
    ledger
        .accept(command("deactivate-again", "deactivate", Some(1)))
        .unwrap();
    ledger.lost(0, "original session lost".into());
    assert_eq!(ledger.result("deactivate-again")["state"], "unknown");
}

#[test]
fn service_update_resends_same_description_and_ignores_old_or_duplicate_replies() {
    let mut ledger = ledger();
    let mut service = ledger.placements[0].body().unwrap().service;
    service.update_version = 2;
    ledger.placements[0].service_update = Some(service.clone());
    let now = Instant::now();
    ledger.placements[0].service_retry_at = Some(now + Duration::from_secs(5));
    assert!(ledger.service_update_retry(0, now).is_none());
    assert_eq!(
        ledger
            .service_update_retry(0, now + Duration::from_secs(6))
            .unwrap()
            .service,
        service
    );
    let mut reply = NodeUpdateServiceReply {
        service: service.name.clone(),
        instance: service.instance,
        version: 1,
    };
    ledger.service_reply(0, reply.clone()).unwrap();
    assert!(ledger.placements[0].service_update.is_some());
    reply.version = 2;
    ledger.service_reply(0, reply.clone()).unwrap();
    ledger.service_reply(0, reply).unwrap();
    assert!(ledger
        .service_update_retry(0, now + Duration::from_secs(60))
        .is_none());
    assert_eq!(ledger.placements[0].service_description, Some(service));
}

#[tokio::test]
async fn real_node_coordination_and_public_load_preserve_original_disposal_and_sibling() {
    use node::report_tests::NodeClose;
    for mode in [
        NodeClose::Provider,
        NodeClose::Service,
        NodeClose::Timeout,
        NodeClose::Cancel,
    ] {
        let fixture = Fixture::new("node-coordination");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulSingleton;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        cluster.spawn(0).await.unwrap();
        cluster
            .execute(command("sibling", "create", Some(1)))
            .await
            .unwrap();
        until(&mut cluster, |c| {
            c.ledger.placements[0].observed == Observed::Running
        })
        .await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        let pid = cluster.children[0]
            .metadata
            .as_ref()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .app_pid;
        until(&mut cluster, |_| {
            let mut socket = TcpStream::connect(&endpoint).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            socket.write_all(b"read\n").unwrap();
            let mut response = String::new();
            socket.take(512).read_to_string(&mut response).unwrap();
            response.starts_with("OK ")
        })
        .await;
        let launch = cluster.ledger.placements[1].launch.clone();
        let result = node::report_tests::run_node_coordination(&launch, mode).await;
        cluster.children[0]
            .process
            .as_mut()
            .unwrap()
            .check_alive()
            .unwrap();
        let mut socket = TcpStream::connect(endpoint).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(b"read\n").unwrap();
        let mut response = String::new();
        socket.take(512).read_to_string(&mut response).unwrap();
        assert!(response.starts_with("OK "), "{response}");
        assert_eq!(cluster.shutdown().await["clean"], true);
        assert!(gone(pid));
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn executable_nodes_preserve_uncertainty_sibling_contact_and_clean_handoff() {
    let fixture = Fixture::new("uncertain");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    let expected = cluster
        .children
        .iter()
        .map(|child| {
            (
                child.launch.clone(),
                child.process.as_ref().unwrap().pid(),
                child
                    .metadata
                    .as_ref()
                    .unwrap()
                    .profile
                    .as_ref()
                    .unwrap()
                    .app_pid,
            )
        })
        .collect::<Vec<_>>();
    assert_ne!(expected[0].1, expected[1].1);
    assert_ne!(expected[0].1, std::process::id());
    assert_ne!(expected[0].2, expected[1].2);
    for node in 1..=2 {
        let outcome = cluster
            .execute(command(&format!("c{node}"), "create", Some(node)))
            .await
            .unwrap();
        assert_eq!(outcome["state"], "pending");
    }
    // Native notification eligibility starts at monotonic zero. A fresh runner
    // may need the full type interval plus a retry before its first notification.
    let settings = fube_ra::entity::Settings::default();
    let notification_budget =
        Duration::from_millis(settings.notification_type_ms + settings.notification_retry_ms)
            + Duration::from_secs(12);
    until_with_budget(&mut cluster, notification_budget, |cluster| {
        cluster
            .ledger
            .placements
            .iter()
            .all(|p| p.observed == Observed::Running && p.catalog_notified)
    })
    .await;
    let endpoints = cluster
        .ledger
        .placements
        .iter()
        .map(|p| p.endpoint.clone().unwrap())
        .collect::<Vec<_>>();
    for index in 0..2 {
        assert_identity(
            &contact(&endpoints[index]),
            &expected[index].0,
            expected[index].1,
            expected[index].2,
        );
    }
    assert_ne!(
        contact(&endpoints[1])["node"],
        node_hex(&expected[0].0.node),
        "swapped sibling endpoint must not satisfy the placement oracle"
    );
    assert_eq!(
        cluster
            .execute(command("c1", "create", Some(1)))
            .await
            .unwrap()["state"],
        "complete"
    );
    assert!(cluster
        .execute(command("c1", "create", Some(2)))
        .await
        .is_err());
    cluster.children[0].link.as_ref().unwrap().channel.dispose();
    until(&mut cluster, |cluster| {
        cluster.ledger.placements[0].observed == Observed::Unknown
    })
    .await;
    assert_eq!(cluster.ledger.placements[0].desired, Desired::Running);
    assert_identity(
        &contact(&endpoints[0]),
        &expected[0].0,
        expected[0].1,
        expected[0].2,
    );
    assert_identity(
        &contact(&endpoints[1]),
        &expected[1].0,
        expected[1].1,
        expected[1].2,
    );
    assert!(cluster
        .execute(command("replacement", "create", Some(1)))
        .await
        .is_err());
    cluster
        .execute(command("d2", "delete", Some(2)))
        .await
        .unwrap();
    until(&mut cluster, |cluster| {
        cluster.ledger.placements[1].observed == Observed::Absent
    })
    .await;
    assert!(TcpStream::connect(&endpoints[1]).is_err());
    assert_identity(
        &contact(&endpoints[0]),
        &expected[0].0,
        expected[0].1,
        expected[0].2,
    );
    let prior = cluster.interlock.deployment.clone();
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
    for (_, node, app) in expected {
        assert!(gone(node));
        assert!(gone(app));
    }
    assert!(!fixture.root.join("deployment").exists());
    drop(cluster);
    let mut successor = Cluster::new(fixture.config()).unwrap();
    assert_ne!(successor.interlock.deployment, prior);
    successor.start().await.unwrap();
    let result = successor.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
}

#[tokio::test]
async fn real_stale_native_generation_is_not_admitted_and_sibling_remains_usable() {
    let fixture = Fixture::new("stale");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    let launch = cluster.children[1].launch.clone();
    let pid = cluster.children[1].process.as_ref().unwrap().pid();
    let app = cluster.children[1]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    let body = cluster.ledger.placements[0].body().unwrap();
    let mut stale = cluster.children[0].launch.generation.clone();
    stale.number.generation -= 1;
    cluster.children[0]
        .link
        .as_ref()
        .unwrap()
        .send(add_instance_message(&body, &stale).unwrap())
        .await
        .unwrap();
    until(&mut cluster, |c| {
        c.ledger.placements[0].observed == Observed::Unknown
    })
    .await;
    assert!(!cluster.ledger.placements[0].created);
    assert!(cluster.ledger.placements[0].endpoint.is_none());
    cluster
        .execute(command("sibling", "create", Some(2)))
        .await
        .unwrap();
    until(&mut cluster, |c| {
        c.ledger.placements[1].observed == Observed::Running
    })
    .await;
    assert_identity(
        &contact(cluster.ledger.placements[1].endpoint.as_ref().unwrap()),
        &launch,
        pid,
        app,
    );
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
}

#[tokio::test]
async fn partial_startup_rolls_back_every_acquired_original_child() {
    let fixture = Fixture::new("partial");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.prepare().unwrap();
    cluster.spawn(0).await.unwrap();
    let pid = cluster.children[0].process.as_ref().unwrap().pid();
    let app = cluster.children[0]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    // Only this test-owned second launch loses its immutable package source;
    // the first original owner and its rollback are the production code path.
    cluster.ledger.placements[1].launch.package = fixture.root.join("missing-package");
    assert!(cluster.spawn(1).await.is_err());
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], true, "{result}");
    assert!(gone(pid) && gone(app));
    assert!(!fixture.root.join("deployment").exists());
}

#[tokio::test]
async fn active_original_node_owner_loss_cannot_fabricate_receipt_or_clean_relaunch() {
    let fixture = Fixture::new("lost-owner");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.start().await.unwrap();
    for node in 1..=2 {
        cluster
            .execute(command(&format!("c{node}"), "create", Some(node)))
            .await
            .unwrap();
    }
    until(&mut cluster, |c| {
        c.ledger
            .placements
            .iter()
            .all(|p| p.observed == Observed::Running)
    })
    .await;
    let sibling_endpoint = cluster.ledger.placements[1].endpoint.clone().unwrap();
    let lost_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
    let lost_app = cluster.children[0]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    let sibling_pid = cluster.children[1].process.as_ref().unwrap().pid();
    let sibling_app = cluster.children[1]
        .metadata
        .as_ref()
        .unwrap()
        .profile
        .as_ref()
        .unwrap()
        .app_pid;
    // Fault through the retained ORIGINAL child/group capability, never a
    // process-name signal or a PID reconstructed into ownership.
    cluster.children[0]
        .process
        .as_mut()
        .unwrap()
        .cleanup()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let terminated = match fs::read_to_string(format!("/proc/{lost_app}/stat")) {
            Ok(stat) => stat
                .rsplit_once(')')
                .is_some_and(|(_, rest)| matches!(rest.split_whitespace().next(), Some("Z" | "X"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => panic!("independent lost-app observation failed: {error}"),
        };
        if terminated {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "parent-death containment stranded the lost app"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(TcpStream::connect(&lost_endpoint).is_err());
    until(&mut cluster, |c| {
        c.ledger.placements[0].observed == Observed::Unknown
    })
    .await;
    assert_eq!(contact(&sibling_endpoint)["app_pid"], sibling_app);
    let receipt_path = cluster.children[0].launch.directory.join("cleanup.json");
    assert!(!receipt_path.exists());
    let result = cluster.shutdown().await;
    assert_eq!(result["clean"], false, "{result}");
    assert!(gone(sibling_pid) && gone(sibling_app));
    assert!(TcpStream::connect(&sibling_endpoint).is_err());
    assert_eq!(
        read_json::<Value>(&fixture.root.join("handoff.json")).unwrap()["clean"],
        false
    );
    drop(cluster);
    let error = Cluster::new(fixture.config())
        .err()
        .expect("dirty root must refuse fresh authority");
    assert!(error.contains("dirty predecessor"), "{error}");
    // Preserve this small failed-authority domain and its dirty marker as
    // evidence. No force/reset or claim to the lost app capability is made.
}

#[test]
fn profile_root_lock_and_missing_handoff_fail_closed() {
    let fixture = Fixture::new("interlock");
    private_dir(&fixture.root).unwrap();
    let first = interlock::Interlock::acquire(&fixture.root).unwrap();
    assert!(interlock::Interlock::acquire(&fixture.root).is_err());
    first.publish_clean(&[]).unwrap();
    drop(first);
    let second = interlock::Interlock::acquire(&fixture.root).unwrap();
    second.publish_clean(&[]).unwrap();
    drop(second);
    fs::remove_file(fixture.root.join("handoff.json")).unwrap();
    fs::write(fixture.root.join("unproved-resource"), b"owned").unwrap();
    assert!(interlock::Interlock::acquire(&fixture.root).is_err());
    // This fixture never acquired processes, so exact test-owned input cleanup
    // is safe; this is not a reset path exposed by the product.
    fs::remove_dir_all(&fixture.root).unwrap();
    private_dir(&fixture.root).unwrap();
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(interlock::Interlock::acquire(&fixture.root).is_err());
    fs::remove_dir_all(&fixture.root).unwrap();
}
