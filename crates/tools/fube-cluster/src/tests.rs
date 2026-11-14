use super::*;
use crate::fm::{Desired, IdleDeleteTarget, Observed};
use fube_transport::{
    channel::IncomingMessage, native_channel::GatewayTarget,
    serialization::serde::ObjectArray,
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::PermissionsExt,
};

#[test]
fn generation_round_retries_restarts_and_stops_before_upload_completion() {
    use fm::{GenerationHandshake, GenerationPhase, GenerationWork};
    let initial = launches()[0].generation.number.clone();
    let mut round = GenerationHandshake::new(&initial, 2).unwrap();
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
fn replicated_lfum_reconciles_every_original_node_and_rejects_partial_or_stale_membership() {
    let mut ledger = configured_ledger(3, 2);
    for (i, placement) in ledger.placements.iter_mut().enumerate() {
        placement.node_description = Some(NodeDescription {
            name: format!("node{i}"),
            ..NodeDescription::default()
        });
    }
    ledger.begin_generation().unwrap();
    let generation = ledger.generation.as_ref().unwrap().number.clone();
    for index in 0..3 {
        ledger.generation.as_mut().unwrap().proposal_reply(index, &GenerationProposalReply {
            proposed: generation.clone(), current: generation.clone(), error: 0,
        }).unwrap();
    }
    let configuration = ledger.configuration.as_ref().unwrap().reply.as_ref().unwrap().clone();
    for index in 0..3 {
        let upload = LfumUpload {
            generation: generation.clone(),
            node: ledger.placements[index].node_description.clone().unwrap(),
            units: ObjectArray(vec![FailoverUnitInfo {
                service: ledger.placements[index].body().unwrap().service,
                unit: configuration.unit.clone(),
                report_from_primary: index == 0,
                replicas: ObjectArray(configuration.replicas.0.iter().cloned().map(|description| ReplicaInfo {
                    description, intermediate_role: 1,
                }).collect()),
                ..FailoverUnitInfo::default()
            }]),
            any_replica_found: true,
        };
        let mut stale = upload.clone();
        stale.generation.generation -= 1;
        ledger.lfum_upload(index, stale).unwrap();
        assert!(ledger.generation.as_ref().unwrap().uploads[index].is_none());
        for mutation in 0..8 {
            let mut bad = upload.clone();
            let info = &mut bad.units.0[0];
            match mutation {
                0 => { info.replicas.0.pop(); }
                1 => info.replicas.0[1] = info.replicas.0[0].clone(),
                2 => info.replicas.0[1].description.instance += 1,
                3 => info.replicas.0[1].description.current_role = 2,
                4 => info.replicas.0[1].description.last_lsn = 123,
                5 => info.unit.current.configuration += 1,
                6 => info.report_from_primary = !info.report_from_primary,
                _ => bad.node.name.push_str("-foreign"),
            }
            assert!(ledger.lfum_upload(index, bad).is_err());
            assert!(ledger.generation.as_ref().unwrap().uploads[index].is_none());
        }
        ledger.lfum_upload(index, upload.clone()).unwrap();
        assert_eq!(ledger.generation_pending(), index != 2);
        let mut duplicate = upload.clone();
        duplicate.units.0.clear();
        ledger.lfum_upload(index, duplicate).unwrap();
        assert_eq!(ledger.generation.as_ref().unwrap().uploads[index], Some(upload));
    }
    assert!(!ledger.generation_pending());
}

#[test]
fn admitting_lfum_validates_sf_phase_roles_without_completing_configuration() {
    let mut ledger = configured_ledger(3, 2);
    ledger.configuration.as_mut().unwrap().reply = None;
    let request = ledger.configuration.as_ref().unwrap().body.configuration.clone();
    for (index, placement) in ledger.placements.iter_mut().enumerate() {
        placement.node_description = Some(NodeDescription { name: format!("node{index}"), ..NodeDescription::default() });
    }
    ledger.begin_generation().unwrap();
    let generation = ledger.generation.as_ref().unwrap().number.clone();
    for index in 0..3 {
        ledger.generation.as_mut().unwrap().proposal_reply(index, &GenerationProposalReply {
            proposed: generation.clone(), current: generation.clone(), error: 0,
        }).unwrap();
    }
    for index in 0..3 {
        let body = LfumUpload {
            generation: generation.clone(),
            node: ledger.placements[index].node_description.clone().unwrap(),
            any_replica_found: true,
            units: ObjectArray(vec![FailoverUnitInfo {
                service: request.service.clone(), unit: request.unit.clone(),
                intermediate: if index == 0 { request.unit.current } else { Epoch::default() },
                report_from_primary: index == 0,
                replicas: ObjectArray(request.replicas.0.iter().cloned().map(|mut description| {
                    description.first_lsn = -1; description.last_lsn = -1;
                    ReplicaInfo { intermediate_role: if index == 0 { description.current_role } else { 1 }, description }
                }).collect()),
                local_deleted: false,
            }]),
        };
        let mut bad = body.clone();
        if index == 0 {
            bad.units.0[0].replicas.0[1].intermediate_role = 1;
        } else {
            bad.units.0[0].intermediate = request.unit.current;
        }
        assert!(ledger.lfum_upload(index, bad).is_err());
        ledger.lfum_upload(index, body).unwrap();
        assert!(ledger.configuration.as_ref().unwrap().reply.is_none());
    }
    assert!(!ledger.generation_pending());
    assert_eq!(ledger.configuration_status()["ready"], false);
}

#[test]
fn building_lfum_validates_original_idle_states_without_finishing_build_or_configuration() {
    let mut ledger = initial_configured_ledger(3, 2);
    for (index, placement) in ledger.placements.iter_mut().enumerate() {
        placement.node_description = Some(NodeDescription { name: format!("node{index}"), ..NodeDescription::default() });
    }
    assert!(ledger.begin_generation().is_err());
    let builds: Vec<_> = (1..3).map(|destination|
        ledger.request_idle_on(destination, Instant::now()).unwrap()).collect();
    ledger.begin_generation().unwrap();
    let generation = ledger.generation.as_ref().unwrap().number.clone();
    for index in 0..3 {
        ledger.generation.as_mut().unwrap().proposal_reply(index, &GenerationProposalReply {
            proposed: generation.clone(), current: generation.clone(), error: 0,
        }).unwrap();
    }
    assert!(ledger.request_configuration(Instant::now()).is_err());
    let primary = ledger.placements[0].ready_replica.clone().unwrap();
    let mut replicas = vec![primary];
    replicas.extend(builds.iter().map(|build| build.replica.clone()));
    for index in 0..3 {
        let body = LfumUpload {
            generation: generation.clone(),
            node: ledger.placements[index].node_description.clone().unwrap(),
            any_replica_found: true,
            units: ObjectArray(vec![FailoverUnitInfo {
                service: builds[0].service.clone(), unit: builds[0].unit.clone(),
                report_from_primary: index == 0,
                replicas: ObjectArray(replicas.iter().filter(|r|
                    index == 0 || r.node == ledger.placements[index].launch.node
                ).cloned().map(|description| ReplicaInfo { description, intermediate_role: 1 }).collect()),
                ..FailoverUnitInfo::default()
            }]),
        };
        for mutation in 0..7 {
            let mut bad = body.clone();
            let info = &mut bad.units.0[0];
            match mutation {
                0 => info.replicas.0[0].description.instance += 1,
                1 => info.replicas.0[0].description.last_lsn = 12,
                2 => info.replicas.0[0].description.current_role = 3,
                3 => info.replicas.0[0].description.state = 3,
                4 => info.replicas.0[0].intermediate_role = 2,
                5 => info.report_from_primary = !info.report_from_primary,
                _ => info.unit.current.configuration += 1,
            }
            assert!(ledger.lfum_upload(index, bad).is_err());
        }
        ledger.lfum_upload(index, body).unwrap();
    }
    assert!(!ledger.generation_pending());
    assert!(ledger.configuration.is_none());
    assert!(ledger.idles.iter().all(|idle| idle.reply.is_none()));
    for build in builds {
        let mut replica = build.replica;
        replica.state = 2;
        replica.replication_endpoint = "127.0.0.1:12346".into();
        ledger.idle_reply(0, AddReplicaReply { unit: build.unit, replica, error: 0 }).unwrap();
    }
    assert!(ledger.request_configuration(Instant::now()).is_ok());
}

#[test]
fn failover_lfum_validates_original_phase_views_without_finishing_promotion() {
    let mut ledger = configured_ledger(3, 2);
    for (index, placement) in ledger.placements.iter_mut().enumerate() {
        placement.node_description = Some(NodeDescription { name: format!("node{index}"), ..NodeDescription::default() });
    }
    let original = ledger.configuration.as_ref().unwrap().reply.clone().unwrap();
    ledger.report(0, &stateful_loss_report(&ledger.placements[0])).unwrap();
    let intent = ledger.start_primary_failover(Instant::now()).unwrap().unwrap().configuration;
    ledger.begin_generation().unwrap();
    for activating in [false, true] {
        let generation = ledger.generation.as_ref().unwrap().number.clone();
        for index in 0..3 {
            ledger.generation.as_mut().unwrap().proposal_reply(index, &GenerationProposalReply {
                proposed: generation.clone(), current: generation.clone(), error: 0,
            }).unwrap();
        }
        for index in 0..3 {
            let mut config = intent.clone();
            if !activating && index == 2 {
                config.replicas = original.replicas.clone();
                for r in &mut config.replicas.0 { r.previous_role = r.current_role; }
            }
            let mut body = LfumUpload {
                generation: generation.clone(),
                node: ledger.placements[index].node_description.clone().unwrap(),
                any_replica_found: true,
                units: ObjectArray(vec![FailoverUnitInfo {
                    service: config.service, unit: config.unit.clone(),
                    intermediate: if activating && index == 1 { config.unit.current } else { Epoch::default() },
                    report_from_primary: index == 1,
                    replicas: ObjectArray(config.replicas.0.into_iter().map(|mut description| {
                        description.first_lsn = -1; description.last_lsn = -1;
                        ReplicaInfo { intermediate_role: if activating && index == 1 { description.current_role } else { 1 }, description }
                    }).collect()), local_deleted: false,
                }]),
            };
            if index == 0 {
                body.units.0.clear();
                if !activating {
                    body.units.0.push(FailoverUnitInfo {
                        service: intent.service.clone(), unit: original.unit.clone(),
                        replicas: ObjectArray(vec![ReplicaInfo {
                            description: crate::profile::dropped_replica(&original.replicas.0[0]), intermediate_role: 1,
                        }]), ..FailoverUnitInfo::default()
                    });
                }
            } else {
                for mutation in 0..5 {
                    let mut bad = body.clone();
                    match mutation {
                        0 => bad.units.0[0].replicas.0[0].description.instance += 1,
                        1 => bad.units.0[0].replicas.0[0].description.last_lsn = 12,
                        2 => bad.units.0[0].replicas.0[0].description.previous_role = 2,
                        3 => bad.units.0[0].report_from_primary = !bad.units.0[0].report_from_primary,
                        _ => bad.units.0[0].unit.current.configuration += 1,
                    }
                    assert!(ledger.lfum_upload(index, bad).is_err());
                }
            }
            ledger.lfum_upload(index, body).unwrap();
            assert!(ledger.swap.as_ref().unwrap().reply.is_none());
        }
        assert!(!ledger.generation_pending());
        if !activating {
            ledger.begin_generation().unwrap();
            assert_eq!(ledger.generation.as_ref().unwrap().number.generation, generation.generation + 1);
        }
    }
    assert_eq!(ledger.configuration_status()["ready"], false);
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

#[test]
fn lfum_reconciliation_requires_both_exact_inventories_and_retains_duplicate_receipts() {
    let mut ledger = ledger();
    ledger.accept(command("a", "create", Some(1))).unwrap();
    ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
    ledger.begin_generation().unwrap();
    let generation = ledger.generation.as_ref().unwrap().number.clone();
    for index in 0..2 {
        ledger.generation.as_mut().unwrap().proposal_reply(index, &GenerationProposalReply {
            proposed: generation.clone(), current: generation.clone(), error: 0,
        }).unwrap();
    }
    let original = ledger.placements[0].body().unwrap();
    let upload = LfumUpload {
        generation: generation.clone(),
        node: ledger.placements[0].node_description.clone().unwrap(),
        units: ObjectArray(vec![FailoverUnitInfo {
            service: original.service, unit: original.unit,
            replicas: ObjectArray(vec![ReplicaInfo { description: ledger.placements[0].ready_replica.clone().unwrap(), intermediate_role: 1 }]),
            ..FailoverUnitInfo::default()
        }]),
        any_replica_found: true,
    };
    let mut stale = upload.clone();
    stale.generation.generation -= 1;
    assert_eq!(ledger.lfum_upload(0, stale.clone()).unwrap().number, stale.generation);
    assert!(ledger.generation.as_ref().unwrap().uploads[0].is_none());
    for mutation in 0..6 {
        let mut bad = upload.clone();
        match mutation {
            0 => bad.node.name.push_str("-foreign"),
            1 => bad.units.0.clear(),
            2 => {
                let extra = bad.units.0[0].replicas.0[0].clone();
                bad.units.0[0].replicas.0.push(extra);
            }
            3 => bad.units.0[0].replicas.0[0].description.instance += 1,
            4 => bad.units.0[0].unit.current.configuration += 1,
            _ => bad.any_replica_found = false,
        }
        assert!(ledger.lfum_upload(0, bad).is_err());
        assert!(ledger.generation.as_ref().unwrap().uploads[0].is_none());
    }
    assert_eq!(ledger.lfum_upload(0, upload.clone()).unwrap().number, generation);
    assert!(ledger.generation_pending());
    assert!(ledger.accept(command("blocked", "create", Some(2))).is_err());
    let mut duplicate = upload.clone();
    duplicate.units.0.clear();
    ledger.lfum_upload(0, duplicate).unwrap();
    assert_eq!(ledger.generation.as_ref().unwrap().uploads[0], Some(upload));
    assert!(ledger.generation.as_mut().unwrap().next(0, Instant::now() + Duration::from_secs(60)).is_none());
    ledger.lfum_upload(1, LfumUpload {
        generation: generation.clone(),
        node: ledger.placements[1].node_description.clone().unwrap(),
        ..LfumUpload::default()
    }).unwrap();
    assert!(!ledger.generation_pending());
    assert_eq!(ledger.status()["generation_handshake"]["complete"], true);
    assert_eq!(ledger.placements[0].observed, Observed::Running);
    ledger.generation.as_mut().unwrap().reject(&generation).unwrap();
    assert!(!ledger.generation_pending());
    ledger.accept(command("after", "create", Some(2))).unwrap();
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
        peers: vec![],
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
        placement.node_description = Some(NodeDescription { name: placement.launch.name(), ..NodeDescription::default() });
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
        placement.node_description = Some(NodeDescription { name: placement.launch.name(), ..NodeDescription::default() });
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
        launches[index].peers = vec![Peer {
            node: remote.node.clone(),
            session: remote.session.clone(),
            partition: remote.partition.clone(),
            replica: remote.replica,
            incarnation: remote.incarnation,
        }];
        launches[index].profile = ServiceProfile::StatefulPeerIdle;
    }
    launches
}

fn three_replica_ledger() -> Ledger {
    let pair = peer_idle_launches();
    let mut third = pair[1].clone();
    third.placement = 3;
    third.node.id = [3; 16];
    third.node.instance += 1;
    third.replica = 3;
    third.incarnation += 1;
    third.session = "00000000-0000-4000-8000-000000000033".into();
    third.directory = PathBuf::from("/owned/node3");
    let mut launches = vec![pair[0].clone(), pair[1].clone(), third];
    let peers: Vec<_> = launches.iter().map(|launch| Peer {
        node: launch.node.clone(), session: launch.session.clone(),
        partition: launch.partition.clone(), replica: launch.replica, incarnation: launch.incarnation,
    }).collect();
    for (i, launch) in launches.iter_mut().enumerate() {
        launch.profile = ServiceProfile::StatefulThree;
        launch.peers = peers.iter().enumerate().filter(|(index, _)| *index != i).map(|(_, p)| p.clone()).collect();
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
    ledger.accept(command("primary", "create", Some(1))).unwrap();
    ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
    ledger
}

fn configured_three_ledger() -> Ledger {
    let mut ledger = three_replica_ledger();
    for destination in 1..3 {
        let body = ledger.request_idle_on(destination, Instant::now()).unwrap();
        let mut replica = body.replica;
        replica.state = 2;
        replica.replication_endpoint = format!("127.0.0.1:{}", 12346 + destination);
        ledger.idle_reply(0, AddReplicaReply { unit: body.unit, replica, error: 0 }).unwrap();
    }
    let body = ledger.request_configuration(Instant::now()).unwrap();
    let mut reply = ConfigurationReply {
        unit: body.configuration.unit, replicas: body.configuration.replicas, error: 0,
    };
    reply.unit.previous = Epoch::default();
    for r in &mut reply.replicas.0 { r.previous_role = 1; }
    ledger.configuration_reply(0, reply).unwrap();
    ledger
}

fn initial_configured_ledger(target: i32, minimum: i32) -> Ledger {
    let template = launches()[0].clone();
    let mut launches: Vec<_> = (1..=target as u8).map(|placement| {
        let mut launch = template.clone();
        launch.profile = ServiceProfile::Stateful { target, minimum };
        launch.placement = placement;
        launch.node.id = u128::from(placement).to_le_bytes();
        launch.node.instance += u64::from(placement);
        launch.session = uuid::Uuid::new_v4().to_string();
        launch.replica = i64::from(placement);
        launch.incarnation += i64::from(placement);
        launch.directory = PathBuf::from(format!("/owned/node{placement}"));
        launch
    }).collect();
    let peers: Vec<_> = launches.iter().map(|l| Peer {
        node: l.node.clone(), session: l.session.clone(), partition: l.partition.clone(),
        replica: l.replica, incarnation: l.incarnation,
    }).collect();
    for (i, launch) in launches.iter_mut().enumerate() {
        launch.peers = peers.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, p)| p.clone()).collect();
        launch.validate().unwrap();
    }
    let mut ledger = Ledger::new(launches);
    for p in &mut ledger.placements {
        p.bootstrap = Bootstrap::Metadata;
        let mut actual = profile(&p.launch);
        actual.service = p.launch.profile.service().into();
        actual.service_type = fixed_type_for(p.launch.profile);
        p.admit_profile(actual).unwrap();
    }
    ledger.accept(command("primary", "create", Some(1))).unwrap();
    ledger.reply(0, ready_reply(&ledger.placements[0]), false).unwrap();
    ledger
}

fn configured_ledger(target: i32, minimum: i32) -> Ledger {
    let mut ledger = initial_configured_ledger(target, minimum);
    for destination in 1..target as usize {
        let body = ledger.request_idle_on(destination, Instant::now()).unwrap();
        let mut replica = body.replica;
        replica.state = 2;
        replica.replication_endpoint = format!("127.0.0.1:{}", 12346 + destination);
        ledger.idle_reply(0, AddReplicaReply { unit: body.unit, replica, error: 0 }).unwrap();
    }
    let body = ledger.request_configuration(Instant::now()).unwrap();
    let mut reply = ConfigurationReply {
        unit: body.configuration.unit, replicas: body.configuration.replicas, error: 0,
    };
    reply.unit.previous = Epoch::default();
    for r in &mut reply.replicas.0 { r.previous_role = 1; }
    ledger.configuration_reply(0, reply).unwrap();
    ledger
}

#[test]
fn configured_failover_redirects_only_exact_native_progress_and_replays_legacy_reply() {
    let mut ledger = configured_ledger(5, 3);
    let service = ledger.placements[0].body().unwrap().service;
    let lost_secondary = stateful_loss_report(&ledger.placements[4]);
    ledger.report(4, &lost_secondary).unwrap();
    let loss = stateful_loss_report(&ledger.placements[0]);
    ledger.report(0, &loss).unwrap();
    let intent = ledger.start_primary_failover(Instant::now()).unwrap().unwrap();
    let mut change = intent.configuration.clone();
    let selected = change.replicas.0.iter_mut().find(|r| r.node == ledger.placements[2].launch.node).unwrap();
    selected.first_lsn = 1;
    selected.last_lsn = 9;
    for invalid in 0..5 {
        let mut bad = change.clone();
        match invalid {
            0 => bad.replicas.0[2].instance += 1,
            1 => bad.unit.current.configuration += 1,
            2 => { bad.replicas.0[1].first_lsn = 1; bad.replicas.0[1].last_lsn = 10; },
            3 => bad.service.minimum = 1,
            _ => { bad.replicas.0[2].first_lsn = -1; bad.replicas.0[2].last_lsn = -1; },
        }
        assert!(ledger.change_configuration(1, bad, Instant::now()).unwrap().is_none());
    }
    assert!(ledger.change_configuration(2, change.clone(), Instant::now()).unwrap().is_none());
    let ack = ledger.change_configuration(1, change.clone(), Instant::now()).unwrap().unwrap();
    assert_eq!(ledger.primary_index(), 2);
    assert_eq!(ledger.swap.as_ref().unwrap().body.configuration.service, service);
    assert_eq!(ack.unit.previous, intent.configuration.unit.previous);
    assert!(ack.unit.current.configuration > intent.configuration.unit.current.configuration);
    assert_eq!(ledger.change_configuration(1, change, Instant::now()).unwrap(), Some(ack));
    assert_eq!(ledger.configuration_status()["candidate_redirections"].as_array().unwrap().len(), 1);
    ledger.report(4, &lost_secondary).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "pending");
    let next = ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).unwrap();
    let mut reply = ConfigurationReply { unit: next.configuration.unit, replicas: next.configuration.replicas, error: 0 };
    reply.unit.previous = Epoch::default();
    reply.replicas.0.retain(|r| r.current_role >= 3);
    for r in &mut reply.replicas.0 {
        r.previous_role = 1;
        r.first_lsn = 1;
        r.last_lsn = 9;
        r.service_endpoint = if r.current_role == 4 { "127.0.0.1:12349".into() } else { String::new() };
    }
    ledger.configuration_reply(1, reply.clone()).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], false);
    ledger.configuration_reply(2, reply).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], true);
    assert!(ledger.placements[0].endpoint.is_none());
    assert!(ledger.placements[4].endpoint.is_none());
}

#[test]
fn primary_loss_supersedes_only_unreplaced_secondary_removal_and_preserves_pc_cc() {
    for (target, minimum) in [(3,2),(4,2),(5,3),(64,32)] {
        for blocked in 0..6 {
            let mut ledger = configured_ledger(target,minimum);
            let original = ledger.placements[1].body().unwrap();
            ledger.report(1,&stateful_loss_report(&ledger.placements[1])).unwrap();
            ledger.reply(1,AddInstanceReply {
                unit:original.unit,replica:dropped_replica(&original.replica),error:0,
            },true).unwrap();
            let removal = ledger.begin_secondary_repair(1,Instant::now()).unwrap().unwrap();
            let mut late = ConfigurationReply {
                unit:removal.configuration.unit.clone(),replicas:removal.configuration.replicas.clone(),error:0,
            };
            late.unit.previous = Epoch::default();
            late.replicas.0.retain(|r| r.current_role >= 3);
            for r in &mut late.replicas.0 { r.previous_role = 1; }
            match blocked {
                1 => ledger.repair.as_mut().unwrap().host_requested = true,
                2 => ledger.configuration_reply(0,late.clone()).unwrap(),
                3 => ledger.lost(0,"FM link lost, not app-host loss".into()),
                4 => { ledger.report(2,&stateful_loss_report(&ledger.placements[2])).unwrap(); },
                _ => {}
            }
            if blocked != 3 {
                ledger.report(0,&stateful_loss_report(&ledger.placements[0])).unwrap();
            }
            if blocked == 5 {
                ledger.placements[2].activated = false;
            }
            let intent = ledger.start_primary_failover(Instant::now()).unwrap();
            if blocked != 0 || target == 3 {
                assert!(intent.is_none(),"unexpected supersession {target}/{minimum} case={blocked}");
                assert!(ledger.repair.as_ref().unwrap().profile.is_none());
                continue;
            }
            let intent = intent.unwrap();
            assert!(ledger.repair.is_none());
            assert!(ledger.repair_history.last().unwrap().unknown);
            assert_eq!(ledger.repair_status()["state"],"superseded");
            assert_eq!(ledger.repair_status()["ready"],false);
            assert_eq!(ledger.primary_index(),2);
            assert_eq!(ledger.failed_primary_index(),Some(0));
            assert_eq!(intent.configuration.unit.previous,removal.configuration.unit.previous);
            assert_eq!(intent.configuration.unit.current.configuration,removal.configuration.unit.current.configuration+0x1_0000_0001);
            assert_eq!(intent.configuration.service,removal.configuration.service);
            for expected in &removal.configuration.replicas.0 {
                let actual = intent.configuration.replicas.0.iter().find(|r| r.node == expected.node).unwrap();
                assert_eq!(actual.previous_role,expected.previous_role);
                assert_eq!(actual.current_role >= 3,expected.current_role >= 3);
                assert_eq!((actual.id,actual.instance),(expected.id,expected.instance));
            }
            let before = ledger.status();
            ledger.configuration_reply(0,late).unwrap();
            assert_eq!(ledger.status(),before);
            assert!(ledger.start_primary_failover(Instant::now()).unwrap().is_none());
            assert_eq!(ledger.configuration_retry(Instant::now()+Duration::from_secs(11)),Some(intent.clone()));
            let mut reply = ConfigurationReply {
                unit:intent.configuration.unit,replicas:intent.configuration.replicas,error:0,
            };
            reply.unit.previous = Epoch::default();
            reply.replicas.0.retain(|r| r.current_role >= 3);
            for r in &mut reply.replicas.0 {
                r.previous_role = 1;
                if r.up { r.first_lsn=1; r.last_lsn=8; r.service_endpoint.clear(); }
                if r.current_role == 4 { r.service_endpoint="127.0.0.1:12349".into(); }
            }
            ledger.configuration_reply(2,reply).unwrap();
            assert_eq!(ledger.configuration_status()["ready"],true);
            assert!(ledger.begin_primary_repair(Instant::now()).is_err());
            assert!(ledger.begin_secondary_repair(1,Instant::now()).is_err());
        }
    }
}

#[test]
fn sequential_restoration_keeps_partial_membership_exact_and_fences_additional_losses() {
    for (target,minimum) in [(4,2),(5,3),(64,32)] {
        for lose_at in 0..7 {
            let mut ledger=configured_ledger(target,minimum);
            let secondary=ledger.placements[1].body().unwrap();
            ledger.report(1,&stateful_loss_report(&ledger.placements[1])).unwrap();
            ledger.reply(1,AddInstanceReply { unit:secondary.unit,replica:dropped_replica(&secondary.replica),error:0 },true).unwrap();
            ledger.begin_secondary_repair(1,Instant::now()).unwrap();
            ledger.report(0,&stateful_loss_report(&ledger.placements[0])).unwrap();
            let failover=ledger.start_primary_failover(Instant::now()).unwrap().unwrap();
            assert!(ledger.begin_secondary_repair(0,Instant::now()).is_err());
            let mut completed=ConfigurationReply { unit:failover.configuration.unit,replicas:failover.configuration.replicas,error:0 };
            completed.unit.previous=Epoch::default();
            completed.replicas.0.retain(|r| r.current_role>=3);
            for r in &mut completed.replicas.0 {
                r.previous_role=1;
                if r.up { r.first_lsn=1;r.last_lsn=8;r.service_endpoint.clear(); }
                if r.current_role==4 {r.service_endpoint="127.0.0.1:12349".into();}
            }
            ledger.configuration_reply(2,completed).unwrap();
            assert!(ledger.begin_secondary_repair(0,Instant::now()).is_err());
            let primary=ledger.placements[0].body().unwrap();
            ledger.reply(0,AddInstanceReply {unit:primary.unit,replica:dropped_replica(&primary.replica),error:0},true).unwrap();
            let service=ledger.placements[2].body().unwrap().service;
            if lose_at==0 {
                ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();
                assert!(ledger.begin_secondary_repair(0,Instant::now()).is_err());
                continue;
            }
            assert!(ledger.begin_secondary_repair(1,Instant::now()).is_err());
            for destination in [0,1] {
                let removal=ledger.begin_secondary_repair(destination,Instant::now()).unwrap();
                assert!(ledger.begin_secondary_repair(1-destination,Instant::now()).is_err());
                if let Some(removal)=removal {
                    if lose_at==1 {ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();}
                    let mut reply=ConfigurationReply {unit:removal.configuration.unit,replicas:removal.configuration.replicas,error:0};
                    reply.unit.previous=Epoch::default();
                    reply.replicas.0.retain(|r|r.current_role>=3);
                    for r in &mut reply.replicas.0 {r.previous_role=1;}
                    ledger.configuration_reply(2,reply).unwrap();
                    if lose_at==1 {
                        assert_eq!(ledger.repair_status()["removed"],false);
                        assert_eq!(ledger.repair_status()["state"],"unknown");
                        break;
                    }
                }
                let repair=ledger.repair.as_ref().unwrap();
                let mut profile=repair.old_profile.clone();
                profile.app_pid+=100;
                profile.runtime_root=profile.runtime_root.parent().unwrap().join("fresh-host");
                let receipt=HostReplacementReceipt {
                    deployment:ledger.placements[destination].launch.deployment.clone(),
                    session:ledger.placements[destination].launch.session.clone(),node_pid:123,
                    predecessor:repair.old_profile.clone(),profile,
                    old_host:uuid::Uuid::new_v4().to_string(),new_host:uuid::Uuid::new_v4().to_string(),
                };
                assert!(ledger.repaired_host(&receipt,123).unwrap());
                if matches!(lose_at,2|6) {
                    let lost=if lose_at==6 {destination} else {3};
                    ledger.report(lost,&stateful_loss_report(&ledger.placements[lost])).unwrap();
                }
                let build=ledger.repair.as_ref().unwrap().build.body.clone();
                let mut replica=build.replica;
                replica.state=2;
                replica.replication_endpoint="127.0.0.1:12350".into();
                ledger.idle_reply(2,AddReplicaReply {unit:build.unit,replica,error:0}).unwrap();
                if matches!(lose_at,2|6) {
                    assert_eq!(ledger.repair_status()["built"],false);
                    assert!(ledger.start_repair_configuration(Instant::now()).unwrap().is_none());
                    break;
                }
                let admitted=ledger.start_repair_configuration(Instant::now()).unwrap().unwrap();
                assert_eq!(admitted.configuration.service,service);
                assert_eq!(admitted.configuration.replicas.0.len(),target as usize-1+destination);
                if lose_at==3 {ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();}
                let mut reply=ConfigurationReply {unit:admitted.configuration.unit,replicas:admitted.configuration.replicas,error:0};
                reply.unit.previous=Epoch::default();
                for r in &mut reply.replicas.0 {r.previous_role=1;}
                ledger.configuration_reply(2,reply).unwrap();
                if lose_at==3 {
                    assert_eq!(ledger.repair_status()["ready"],false);
                    break;
                }
                assert_eq!(ledger.repair_status()["ready"],true);
                assert_eq!(ledger.repair_status()["target_restored"],destination==1);
                assert_eq!(ledger.placements[1].loss_reported,destination==0);
                if lose_at==4 && destination==0 {
                    ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();
                    assert!(ledger.begin_secondary_repair(1,Instant::now()).is_err());
                    break;
                }
            }
            if lose_at!=5 {
                assert_eq!(ledger.repair_status()["state"],"unknown");
                assert!(ledger.start_primary_failover(Instant::now()).unwrap().is_none());
            } else {
                assert!(ledger.placements.iter().all(|p| !p.loss_reported && p.observed==Observed::Running));
            }
        }
    }
}

#[test]
fn minimum_restoration_builds_removed_placement_before_combined_removal_admission() {
    for (target, minimum) in [(4,3),(6,5),(64,63)] {
        for lose_at in 0..7 {
            let mut ledger=configured_ledger(target,minimum);
            let secondary=ledger.placements[1].body().unwrap();
            ledger.report(1,&stateful_loss_report(&ledger.placements[1])).unwrap();
            ledger.reply(1,AddInstanceReply {unit:secondary.unit,replica:dropped_replica(&secondary.replica),error:0},true).unwrap();
            ledger.begin_secondary_repair(1,Instant::now()).unwrap();
            ledger.report(0,&stateful_loss_report(&ledger.placements[0])).unwrap();
            let failover=ledger.start_primary_failover(Instant::now()).unwrap().unwrap();
            let mut completed=ConfigurationReply {unit:failover.configuration.unit,replicas:failover.configuration.replicas,error:0};
            completed.unit.previous=Epoch::default();
            completed.replicas.0.retain(|r|r.current_role>=3);
            for r in &mut completed.replicas.0 {
                r.previous_role=1;
                if r.up {r.first_lsn=1;r.last_lsn=8;r.service_endpoint.clear();}
                if r.current_role==4 {r.service_endpoint="127.0.0.1:12349".into();}
            }
            let base=completed.clone();
            ledger.configuration_reply(2,completed).unwrap();
            let primary=ledger.placements[0].body().unwrap();
            ledger.reply(0,AddInstanceReply {unit:primary.unit,replica:dropped_replica(&primary.replica),error:0},true).unwrap();
            assert!(ledger.begin_secondary_repair(0,Instant::now()).unwrap_err().contains("minimum"));
            if lose_at==0 {
                ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();
                assert!(ledger.begin_secondary_repair(1,Instant::now()).is_err());
                continue;
            }
            for (step,destination) in [1,0].into_iter().enumerate() {
                assert!(ledger.begin_secondary_repair(destination,Instant::now()).unwrap().is_none());
                assert!(ledger.begin_secondary_repair(1-destination,Instant::now()).is_err());
                assert!(ledger.start_repair_configuration(Instant::now()).unwrap().is_none());
                assert!(ledger.configuration_retry(Instant::now()+Duration::from_secs(11)).is_none());
                let repair=ledger.repair.as_ref().unwrap();
                let mut profile=repair.old_profile.clone();
                profile.app_pid+=100;
                profile.runtime_root=profile.runtime_root.parent().unwrap().join("fresh-host");
                let receipt=HostReplacementReceipt {
                    deployment:ledger.placements[destination].launch.deployment.clone(),
                    session:ledger.placements[destination].launch.session.clone(),node_pid:123,
                    predecessor:repair.old_profile.clone(),profile,
                    old_host:uuid::Uuid::new_v4().to_string(),new_host:uuid::Uuid::new_v4().to_string(),
                };
                assert!(ledger.repaired_host(&receipt,123).unwrap());
                if matches!(lose_at,1|6) {
                    let lost=if lose_at==6 {destination} else {3};
                    ledger.report(lost,&stateful_loss_report(&ledger.placements[lost])).unwrap();
                }
                let build=ledger.repair.as_ref().unwrap().build.body.clone();
                let mut replica=build.replica;
                replica.state=2;
                replica.replication_endpoint="127.0.0.1:12350".into();
                let built=AddReplicaReply {unit:build.unit,replica,error:0};
                let mut stale=built.clone();
                stale.replica.instance-=1;
                ledger.idle_reply(2,stale).unwrap();
                assert_eq!(ledger.repair_status()["built"],false);
                ledger.idle_reply(2,built).unwrap();
                if matches!(lose_at,1|6) {
                    assert!(ledger.start_repair_configuration(Instant::now()).unwrap().is_none());
                    break;
                }
                if lose_at==2 {ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();}
                let request=ledger.start_repair_configuration(Instant::now()).unwrap();
                if lose_at==2 {assert!(request.is_none());break;}
                let request=request.unwrap();
                assert_eq!(request.configuration.service,secondary.service);
                assert_eq!(request.configuration.replicas.0.iter().filter(|r|r.current_role>=3).count(),target as usize-1+step);
                if step==0 {
                    assert_eq!(request.configuration.unit.previous,base.unit.current);
                    assert_eq!(request.configuration.unit.current.configuration,base.unit.current.configuration+1);
                    for old in &base.replicas.0 {
                        let actual=request.configuration.replicas.0.iter().find(|r|r.node==old.node).unwrap();
                        assert_eq!((actual.id,actual.instance,actual.previous_role),(old.id,old.instance,old.current_role));
                        assert_eq!(actual.current_role,if old.up {old.current_role} else {1});
                    }
                    assert_eq!(ledger.placements[0].observed,Observed::Lost);
                }
                let mut reply=ConfigurationReply {unit:request.configuration.unit,replicas:request.configuration.replicas,error:0};
                reply.unit.previous=Epoch::default();
                reply.replicas.0.retain(|r|r.current_role>=3);
                for r in &mut reply.replicas.0 {r.previous_role=1;}
                let mut stale=reply.clone();
                stale.unit.current.configuration-=1;
                ledger.configuration_reply(2,stale).unwrap();
                assert_eq!(ledger.repair_status()["ready"],false);
                if lose_at==3 {ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();}
                ledger.configuration_reply(2,reply).unwrap();
                if lose_at==3 {assert_eq!(ledger.repair_status()["ready"],false);break;}
                assert_eq!(ledger.repair_status()["ready"],true);
                assert_eq!(ledger.repair_status()["target_restored"],step==1);
                assert_eq!(ledger.placements[0].loss_reported,step==0);
                if lose_at==4 && step==0 {
                    ledger.report(3,&stateful_loss_report(&ledger.placements[3])).unwrap();
                    assert!(ledger.begin_secondary_repair(0,Instant::now()).is_err());
                    break;
                }
            }
            if lose_at==5 {
                assert!(ledger.placements.iter().all(|p|!p.loss_reported&&p.observed==Observed::Running));
            } else {
                assert_eq!(ledger.repair_status()["state"],"unknown");
                assert!(ledger.start_primary_failover(Instant::now()).unwrap().is_none());
            }
        }
    }
}

#[test]
fn secondary_repair_requires_exact_loss_minimum_and_completed_removal_before_host_admission() {
    for (target,minimum) in [(2,1),(4,2),(4,3),(4,4),(5,3),(64,32)] {
        let mut ledger = configured_ledger(target,minimum);
        assert!(ledger.begin_secondary_repair(1,Instant::now()).is_err());
        let original = ledger.placements[1].body().unwrap();
        let report = stateful_loss_report(&ledger.placements[1]);
        ledger.report(1,&report).unwrap();
        assert!(ledger.begin_secondary_repair(1,Instant::now()).is_err());
        ledger.reply(1,AddInstanceReply { unit: original.unit, replica: dropped_replica(&original.replica), error: 0 },true).unwrap();
        if target == minimum {
            assert!(ledger.begin_secondary_repair(1,Instant::now()).unwrap_err().contains("minimum"));
            assert!(ledger.repair.is_none());
            continue;
        }
        let removal = ledger.begin_secondary_repair(1,Instant::now()).unwrap().unwrap();
        assert!(ledger.begin_secondary_repair(1,Instant::now()).unwrap().is_none());
        assert_eq!(ledger.primary_index(),0);
        let repair = ledger.repair.as_ref().unwrap();
        let mut profile = repair.old_profile.clone();
        profile.app_pid += 100;
        profile.runtime_root = profile.runtime_root.parent().unwrap().join("fresh-host");
        let receipt = HostReplacementReceipt {
            deployment: ledger.placements[1].launch.deployment.clone(),
            session: ledger.placements[1].launch.session.clone(), node_pid:123,
            predecessor: repair.old_profile.clone(), profile,
            old_host: uuid::Uuid::new_v4().to_string(),new_host:uuid::Uuid::new_v4().to_string(),
        };
        assert!(ledger.repaired_host(&receipt,123).is_err());
        let mut reply = ConfigurationReply {
            unit: removal.configuration.unit.clone(), replicas: removal.configuration.replicas.clone(),error:0,
        };
        reply.unit.previous = Epoch::default();
        reply.replicas.0.retain(|r| r.current_role >= 3);
        for r in &mut reply.replicas.0 { r.previous_role = 1; }
        ledger.configuration_reply(1,reply.clone()).unwrap();
        let mut stale = reply.clone();
        stale.unit.current.configuration -= 1;
        ledger.configuration_reply(0,stale).unwrap();
        assert_eq!(ledger.repair_status()["removed"],false);
        assert_eq!(ledger.configuration_retry(Instant::now()+Duration::from_secs(11)),Some(removal));
        ledger.configuration_reply(0,reply).unwrap();
        assert_eq!(ledger.repair_status()["removed"],true);
        assert!(ledger.repaired_host(&receipt,123).unwrap());
        assert!(ledger.start_repair_configuration(Instant::now()).unwrap().is_none());
        let build = ledger.repair.as_ref().unwrap().build.body.clone();
        let mut replica = build.replica;
        replica.state = 2;
        replica.replication_endpoint = "127.0.0.1:12350".into();
        ledger.idle_reply(0,AddReplicaReply { unit:build.unit, replica,error:0 }).unwrap();
        let admitted = ledger.start_repair_configuration(Instant::now()).unwrap().unwrap();
        assert_eq!(admitted.configuration.service,original.service);
        assert_eq!(admitted.configuration.replicas.0.len(),target as usize);
        let mut reply = ConfigurationReply { unit:admitted.configuration.unit, replicas:admitted.configuration.replicas,error:0 };
        reply.unit.previous = Epoch::default();
        for r in &mut reply.replicas.0 { r.previous_role = 1; }
        ledger.configuration_reply(0,reply).unwrap();
        assert_eq!(ledger.repair_status()["ready"],true);
    }
}

#[test]
fn primary_repair_requires_fresh_host_owned_build_and_exact_final_membership() {
    for sizes in [None, Some((2, 1)), Some((4, 2)), Some((4, 4)), Some((5, 3)), Some((64, 32))] {
    for lose_survivor in [false, true] {
        let mut ledger = sizes.map_or_else(configured_three_ledger, |(target, minimum)| configured_ledger(target, minimum));
        let service = ledger.placements[0].body().unwrap().service;
        assert!(ledger.begin_primary_repair(Instant::now()).is_err());
        let loss = stateful_loss_report(&ledger.placements[0]);
        ledger.report(0, &loss).unwrap();
        let body = ledger.start_primary_failover(Instant::now()).unwrap().unwrap();
        let mut reply = ConfigurationReply {
            unit: body.configuration.unit, replicas: body.configuration.replicas, error: 0,
        };
        reply.unit.previous = Epoch::default();
        reply.replicas.0.retain(|r| r.current_role >= 3);
        for r in &mut reply.replicas.0 {
            r.previous_role = 1;
            r.first_lsn = 1;
            r.last_lsn = 8;
            if r.current_role == 4 { r.service_endpoint = "127.0.0.1:12349".into(); }
        }
        ledger.configuration_reply(1, reply).unwrap();
        assert!(ledger.begin_primary_repair(Instant::now()).is_err());
        let original = ledger.placements[0].body().unwrap();
        ledger.reply(0, AddInstanceReply {
            unit: original.unit, replica: dropped_replica(&original.replica), error: 0,
        }, true).unwrap();
        assert!(ledger.begin_primary_repair(Instant::now()).unwrap());
        assert!(!ledger.begin_primary_repair(Instant::now()).unwrap());
        let repair = ledger.repair.as_ref().unwrap();
        let build = repair.build.body.clone();
        assert!(ledger.placements.iter().all(|p| build.replica.id > p.replica));
        assert!(build.replica.instance > ledger.placements[0].incarnation);
        assert_eq!(build.unit, ledger.placements[1].body().unwrap().unit);
        let mut fresh = repair.old_profile.clone();
        fresh.app_pid += 100;
        fresh.runtime_root = fresh.runtime_root.parent().unwrap().join("fresh-host");
        let receipt = HostReplacementReceipt {
            deployment: ledger.placements[0].launch.deployment.clone(),
            session: ledger.placements[0].launch.session.clone(), node_pid: 123,
            predecessor: repair.old_profile.clone(), profile: fresh,
            old_host: uuid::Uuid::new_v4().to_string(), new_host: uuid::Uuid::new_v4().to_string(),
        };
        let mut reused = receipt.clone();
        reused.profile.app_pid = receipt.predecessor.app_pid;
        assert!(ledger.repaired_host(&reused, 123).is_err());
        assert!(ledger.repaired_host(&receipt, 124).is_err());
        assert!(ledger.repaired_host(&receipt, 123).unwrap());
        assert!(!ledger.repaired_host(&receipt, 123).unwrap());
        assert!(ledger.start_repair_configuration(Instant::now()).unwrap().is_none());
        let mut built = AddReplicaReply { unit: build.unit, replica: build.replica, error: 0 };
        built.replica.state = 2;
        built.replica.replication_endpoint = "127.0.0.1:12350".into();
        ledger.idle_reply(0, built.clone()).unwrap();
        let mut stale = built.clone();
        stale.replica.instance -= 1;
        ledger.idle_reply(1, stale).unwrap();
        assert_eq!(ledger.repair_status()["built"], false);
        ledger.idle_reply(1, built).unwrap();
        let configuration = ledger.start_repair_configuration(Instant::now()).unwrap().unwrap();
        assert_eq!(configuration.configuration.service, service);
        assert_eq!(configuration.configuration.replicas.0.len(), ledger.placements.len());
        let mut final_reply = ConfigurationReply {
            unit: configuration.configuration.unit.clone(),
            replicas: configuration.configuration.replicas.clone(), error: 0,
        };
        final_reply.unit.previous = Epoch::default();
        for r in &mut final_reply.replicas.0 { r.previous_role = 1; }
        let mut partial = final_reply.clone();
        partial.replicas.0.pop();
        ledger.configuration_reply(1, partial).unwrap();
        ledger.configuration_reply(0, final_reply.clone()).unwrap();
        assert_eq!(ledger.repair_status()["ready"], false);
        assert_eq!(ledger.configuration_retry(Instant::now() + Duration::from_secs(11)), Some(configuration));
        assert!(ledger.report(0, &loss).unwrap().is_none());
        if lose_survivor {
            ledger.lost(ledger.placements.len() - 1, "survivor lost while repairing".into());
        }
        ledger.configuration_reply(1, final_reply.clone()).unwrap();
        assert_eq!(ledger.repair_status()["ready"], !lose_survivor);
        assert_eq!(ledger.configuration_status()["ready"], !lose_survivor);
        if !lose_survivor {
            let current = ledger.placements[1].body().unwrap();
            let next_loss = stateful_loss_report(&ledger.placements[1]);
            ledger.report(1, &next_loss).unwrap();
            let second = ledger.start_primary_failover(Instant::now()).unwrap().unwrap();
            assert_eq!(ledger.primary_index(), 0);
            assert_eq!(ledger.failed_primary_index(), Some(1));
            assert_eq!(ledger.repair_history.len(), 1);
            assert_eq!(second.configuration.unit.previous, final_reply.unit.current);
            assert!(second.configuration.replicas.0.iter().any(|r| {
                r.node == ledger.placements[0].launch.node && r.id == ledger.placements[0].replica
                    && r.previous_role == 3 && r.current_role == 4
            }));
            ledger.configuration_reply(1, final_reply).unwrap();
            assert_eq!(ledger.configuration_status()["ready"], false);
            assert!(ledger.begin_primary_repair(Instant::now()).is_err());
            let mut reply = ConfigurationReply {
                unit: second.configuration.unit, replicas: second.configuration.replicas, error: 0,
            };
            reply.unit.previous = Epoch::default();
            reply.replicas.0.retain(|r| r.current_role >= 3);
            for r in &mut reply.replicas.0 {
                r.previous_role = 1;
                r.first_lsn = 1;
                r.last_lsn = 8;
                r.service_endpoint = if r.current_role == 4 { "127.0.0.1:12351".into() } else { String::new() };
            }
            ledger.configuration_reply(0, reply).unwrap();
            ledger.reply(1, AddInstanceReply {
                unit: current.unit, replica: dropped_replica(&current.replica), error: 0,
            }, true).unwrap();
            assert!(ledger.begin_primary_repair(Instant::now()).unwrap());
            let new_build = &ledger.repair.as_ref().unwrap().build.body;
            assert!(ledger.placements.iter().all(|p| new_build.replica.id > p.replica && new_build.replica.instance > p.incarnation));
            assert!(ledger.report(0, &loss).unwrap().is_none());
        }
    }
    }
}

#[test]
fn primary_failover_requires_matched_loss_and_both_survivors_and_ignores_old_loss_replay() {
    let mut disconnected = configured_three_ledger();
    disconnected.lost(0, "FM channel disconnected without a replica report".into());
    assert!(disconnected.start_primary_failover(Instant::now()).unwrap().is_none());
    assert!(!disconnected.placements[0].loss_reported);
    for survivor in [1, 2] {
        let mut missing = configured_three_ledger();
        missing.lost(survivor, "survivor original session lost".into());
        let loss = stateful_loss_report(&missing.placements[0]);
        missing.report(0, &loss).unwrap();
        assert!(missing.start_primary_failover(Instant::now()).unwrap().is_none());
    }
    let mut ledger = configured_three_ledger();
    let loss = stateful_loss_report(&ledger.placements[0]);
    let mut stale = loss.clone();
    stale.dropped.0[0].replicas.0[0].description.instance -= 1;
    assert!(ledger.report(0, &stale).is_err());
    assert!(ledger.start_primary_failover(Instant::now()).unwrap().is_none());
    ledger.report(0, &loss).unwrap();
    let body = ledger.start_primary_failover(Instant::now()).unwrap().unwrap();
    assert_eq!(body.configuration.unit.previous.data_loss, body.configuration.unit.current.data_loss);
    assert_eq!(body.configuration.replicas.0.iter().filter(|r| r.previous_role >= 3).count(), 3);
    assert_eq!(body.configuration.replicas.0.iter().filter(|r| r.current_role >= 3).count(), 2);
    ledger.report(0, &loss).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "pending");
    assert_eq!(ledger.configuration_retry(Instant::now() + Duration::from_secs(11)), Some(body.clone()));
    assert!(ledger.start_primary_failover(Instant::now()).unwrap().is_none());
    let mut reply = ConfigurationReply {
        unit: body.configuration.unit.clone(), replicas: body.configuration.replicas.clone(), error: 0,
    };
    reply.unit.previous = Epoch::default();
    reply.replicas.0.retain(|r| r.current_role >= 3);
    for r in &mut reply.replicas.0 {
        r.previous_role = 1;
        r.first_lsn = 1;
        r.last_lsn = 8;
        if r.current_role == 4 { r.service_endpoint = "127.0.0.1:12349".into(); }
    }
    let mut partial = reply.clone();
    partial.replicas.0.pop();
    ledger.configuration_reply(1, partial).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], false);
    ledger.configuration_reply(0, reply.clone()).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], false);
    ledger.configuration_reply(1, reply).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], true);
    assert_eq!(ledger.placements[1].endpoint.as_deref(), Some("127.0.0.1:12349"));
    assert!(ledger.placements[0].endpoint.is_none());
    ledger.report(0, &loss).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], true);
    assert_eq!(ledger.placements[1].endpoint.as_deref(), Some("127.0.0.1:12349"));
    ledger.lost(2, "last original Secondary lost".into());
    assert_eq!(ledger.configuration_status()["state"], "unknown");
    assert!(ledger.configuration_retry(Instant::now() + Duration::from_secs(30)).is_none());
}

#[test]
fn three_replica_fm_keeps_build_identity_retries_and_final_completion_independent() {
    let mut ledger = three_replica_ledger();
    assert!(ledger.begin_generation().is_err());
    assert!(ledger.request_configuration(Instant::now()).is_err());
    let first = ledger.request_idle_on(1, Instant::now()).unwrap();
    let second = ledger.request_idle_on(2, Instant::now()).unwrap();
    let ready = |request: &AddReplica| {
        let mut replica = request.replica.clone();
        replica.state = 2;
        replica.replication_endpoint = format!("127.0.0.1:{}", 12345 + replica.id);
        AddReplicaReply { unit: request.unit.clone(), replica, error: 0 }
    };
    let first_reply = ready(&first);
    let second_reply = ready(&second);
    ledger.idle_reply(1, first_reply.clone()).unwrap();
    assert!(ledger.idles.iter().all(|i| i.reply.is_none()));
    ledger.idle_reply(0, second_reply).unwrap();
    let mut stale = first_reply.clone();
    stale.replica.instance -= 1;
    ledger.idle_reply(0, stale).unwrap();
    assert_eq!(ledger.idle_status()["ready"], false);
    assert!(ledger.request_configuration(Instant::now()).is_err());
    assert_eq!(ledger.idle_retry(0, Instant::now() + Duration::from_secs(11)), Some(first.clone()));
    ledger.idle_reply(0, first_reply).unwrap();
    assert_eq!(ledger.idle_status()["ready"], true);
    assert_eq!(ledger.request_idle_on(2, Instant::now()).unwrap(), second);
    let config = ledger.request_configuration(Instant::now()).unwrap();
    assert_eq!(config.configuration.replicas.0.len(), 3);
    assert_eq!((config.configuration.unit.target, config.configuration.unit.minimum), (3, 2));
    let mut reply = ConfigurationReply {
        unit: config.configuration.unit.clone(), replicas: config.configuration.replicas.clone(), error: 0,
    };
    reply.unit.previous = Epoch::default();
    for r in &mut reply.replicas.0 { r.previous_role = 1; }
    let mut incomplete = reply.clone();
    incomplete.replicas.0.pop();
    ledger.configuration_reply(0, incomplete).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], false);
    ledger.configuration_reply(1, reply.clone()).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], false);
    ledger.configuration_reply(0, reply.clone()).unwrap();
    ledger.configuration_reply(0, reply).unwrap();
    assert_eq!(ledger.configuration_status()["ready"], true);
    assert!(ledger.placements.iter().all(|p| p.observed == Observed::Running));
    ledger.lost(2, "original node session lost".into());
    assert_eq!(ledger.configuration_status()["state"], "unknown");
    assert_eq!(ledger.idle_status()["state"], "unknown");

    let mut failed = three_replica_ledger();
    let request = failed.request_idle_on(2, Instant::now()).unwrap();
    let mut failure = ready(&request);
    failure.error = -991;
    failed.idle_reply(0, failure).unwrap();
    assert_eq!(failed.idle_status()["state"], "failed");
    assert!(failed.request_configuration(Instant::now()).is_err());
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
fn same_primary_configuration_requires_semantic_build_and_exact_current_reply() {
    let mut ledger = peer_idle_ledger();
    let now = Instant::now();
    let original_primary_reply = ready_reply(&ledger.placements[0]);
    assert!(ledger.request_configuration(now).is_err());
    let add = ledger.request_idle(now).unwrap();
    assert!(ledger.request_configuration(now).is_err());
    let mut replica = add.replica.clone();
    replica.state = 2;
    replica.replication_endpoint = "127.0.0.1:12346".into();
    ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica, error: 0 }).unwrap();
    let body = ledger.request_configuration(now).unwrap();
    assert_eq!(ledger.request_configuration(now).unwrap(), body);
    assert!(ledger.begin_generation().is_err());
    assert!(ledger.request_idle_removal(now).is_err());
    assert!(ledger.request_idle_replacement(now).is_err());
    let mut reply = ConfigurationReply { unit: body.configuration.unit.clone(),
        replicas: body.configuration.replicas.clone(), error: 0 };
    reply.unit.previous = Epoch::default();
    for replica in &mut reply.replicas.0 { replica.previous_role = 1; }
    let mut stale = reply.clone();
    stale.replicas.0[1].instance -= 1;
    ledger.configuration_reply(0, stale).unwrap();
    ledger.configuration_reply(1, reply.clone()).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "pending");
    let retry = ledger.configuration_retry(now + Duration::from_secs(11)).unwrap();
    assert_eq!(retry, body);
    ledger.configuration_reply(0, reply.clone()).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "ready");
    ledger.reply(0, original_primary_reply, false).unwrap();
    assert_eq!(ledger.placements[1].observed, Observed::Running);
    assert!(ledger.configuration_retry(now + Duration::from_secs(100)).is_none());
    ledger.lost(1, "original Secondary node owner lost".into());
    ledger.configuration_reply(0, reply).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "unknown");
    assert!(ledger.request_configuration(now).is_err());
}

#[test]
fn planned_primary_transfer_requires_exact_native_completion_and_does_not_publish_early_endpoint() {
    let mut ledger = peer_idle_ledger();
    let now = Instant::now();
    assert!(ledger.request_primary_transfer(now).is_err());
    let add = ledger.request_idle(now).unwrap();
    let mut secondary = add.replica.clone();
    secondary.state = 2;
    secondary.replication_endpoint = "127.0.0.1:12346".into();
    ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: secondary, error: 0 }).unwrap();
    let active = ledger.request_configuration(now).unwrap();
    assert!(ledger.request_primary_transfer(now).is_err());
    let mut reply = ConfigurationReply { unit: active.configuration.unit, replicas: active.configuration.replicas, error: 0 };
    reply.unit.previous = Epoch::default();
    for r in &mut reply.replicas.0 { r.previous_role = 1; }
    ledger.configuration_reply(0, reply.clone()).unwrap();
    let swap = ledger.request_primary_transfer(now).unwrap();
    assert_eq!(ledger.request_primary_transfer(now).unwrap(), swap);
    ledger.configuration_reply(0, reply).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "pending");
    let mut endpoint = AddInstance { unit: swap.configuration.unit.clone(), service: swap.configuration.service.clone(), replica: swap.configuration.replicas.0[1].clone() };
    endpoint.replica.service_endpoint = "127.0.0.1:23456".into();
    assert!(ledger.endpoint_updated(0, endpoint.clone()).unwrap().is_none());
    assert!(ledger.endpoint_updated(1, endpoint.clone()).unwrap().is_some());
    assert!(ledger.placements.iter().all(|p| p.endpoint.is_none()));
    assert_eq!(ledger.configuration_status()["state"], "pending");
    let mut reply = ConfigurationReply { unit: swap.configuration.unit, replicas: swap.configuration.replicas, error: 0 };
    reply.unit.previous = Epoch::default();
    for r in &mut reply.replicas.0 {
        r.previous_role = 1; r.first_lsn = 0; r.last_lsn = 1;
        r.service_endpoint = if r.current_role == 4 { "127.0.0.1:23456".into() } else { String::new() };
    }
    let mut duplicate = reply.clone();
    duplicate.replicas.0[1] = duplicate.replicas.0[0].clone();
    ledger.configuration_reply(1, duplicate).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "pending");
    ledger.configuration_reply(0, reply.clone()).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "pending");
    ledger.configuration_reply(1, reply).unwrap();
    assert_eq!(ledger.configuration_status()["state"], "ready");
    assert_eq!(ledger.placements[1].endpoint.as_deref(), Some("127.0.0.1:23456"));
    ledger.lost(1, "new Primary owner lost".into());
    assert_eq!(ledger.configuration_status()["state"], "unknown");
    assert!(ledger.request_primary_transfer(now).is_err());
}

#[test]
fn configured_secondary_replacement_requires_retirement_delete_disposal_and_live_owners() {
    for missing in 0..6 {
        let mut ledger = peer_idle_ledger();
        let now = Instant::now();
        let add = ledger.request_idle(now).unwrap();
        let mut replica = add.replica.clone();
        replica.state = 2;
        replica.replication_endpoint = "127.0.0.1:12346".into();
        ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica, error: 0 }).unwrap();
        let activate = ledger.request_configuration(now).unwrap();
        let mut reply = ConfigurationReply { unit: activate.configuration.unit.clone(), replicas: activate.configuration.replicas.clone(), error: 0 };
        reply.unit.previous = Epoch::default();
        for r in &mut reply.replicas.0 { r.previous_role = 1; }
        ledger.configuration_reply(0, reply.clone()).unwrap();
        let retire = ledger.request_secondary_retirement(now).unwrap();
        assert_eq!(ledger.request_secondary_retirement(now).unwrap(), retire);
        assert!(ledger.request_idle_replacement(now).is_err());
        ledger.configuration_reply(0, reply).unwrap();
        assert_eq!(ledger.configuration_status()["state"], "pending");
        let mut proof = retirement(&ledger.placements[1]);
        proof.disposal.body.replica.state = 3;
        ledger.retirement(1, proof.clone(), 555).unwrap();
        assert!(ledger.placements[1].retirement.is_none());
        if missing == 0 { continue; }
        let mut reply = ConfigurationReply { unit: retire.configuration.unit, replicas: retire.configuration.replicas, error: 0 };
        reply.unit.previous = Epoch::default();
        reply.replicas.0.retain(|r| r.current_role >= 3);
        for r in &mut reply.replicas.0 { r.previous_role = 1; }
        ledger.configuration_reply(0, reply).unwrap();
        assert!(ledger.request_idle_deletion_with_force(now, IdleDeleteTarget::Runtime, true).is_err());
        ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
        if missing != 1 {
            let deletion = ledger.idle_deletion.as_ref().unwrap();
            ledger.idle_deletion_reply(1, DeleteReplicaReply {
                unit: deletion.body.replica.0.unit.clone(),
                replica: dropped_replica(&deletion.body.replica.0.replica), error: 0,
            }).unwrap();
        }
        if missing != 2 { ledger.retirement(1, proof, 555).unwrap(); }
        if missing == 3 { ledger.lost(0, "Primary owner lost".into()); }
        if missing == 4 { ledger.lost(1, "Secondary owner lost".into()); }
        if missing < 5 {
            let before = ledger.status();
            assert!(ledger.request_idle_replacement(now).is_err());
            assert_eq!(ledger.status(), before);
        } else {
            let next = ledger.request_idle_replacement(now).unwrap();
            assert!(next.replica.id > add.replica.id && next.replica.instance > add.replica.instance);
            assert_eq!(next.replica.current_role, 2);
            assert_eq!(next.replica.state, 1);
            assert!(next.replica.replication_endpoint.is_empty());
        }
    }
}

#[test]
fn idle_replacement_requires_both_semantic_barriers_and_original_disposal() {
    for missing in 0..9 {
        let mut ledger = peer_idle_ledger();
        let now = Instant::now();
        let add = ledger.request_idle(now).unwrap();
        let mut added = AddReplicaReply {
            unit: add.unit.clone(),
            replica: add.replica.clone(),
            error: 0,
        };
        added.replica.state = 2;
        added.replica.replication_endpoint = "127.0.0.1:12346".into();
        if missing != 0 { ledger.idle_reply(0, added.clone()).unwrap(); }
        let remove = ledger.request_idle_removal(now).unwrap();
        ledger.request_idle_deletion_with_force(now, IdleDeleteTarget::Runtime, missing == 6).unwrap();
        let mut removed = RemoveReplicaReply { unit: remove.unit, replica: remove.replica, error: 0 };
        if !removed.replica.replication_endpoint.is_empty() { removed.replica.state = 3; }
        if missing != 1 { ledger.idle_removal_reply(0, removed.clone()).unwrap(); }
        let deleted = remove_reply(&ledger.placements[1]);
        if missing != 2 { ledger.reply(1, deleted.clone(), true).unwrap(); }
        let proof = retirement(&ledger.placements[1]);
        if missing != 3 { ledger.retirement(1, proof.clone(), 555).unwrap(); }
        if missing == 4 { ledger.lost(0, "Primary owner lost".into()); }
        if missing == 5 { ledger.lost(1, "destination owner lost".into()); }
        if missing == 7 {
            assert!(ledger.begin_generation().is_err());
            ledger.generation = Some(fm::GenerationHandshake::new(
                &ledger.placements[0].launch.generation.number, ledger.placements.len(),
            ).unwrap());
        }
        if missing < 8 {
            let before = ledger.status();
            assert!(ledger.request_idle_replacement(now).is_err(), "missing gate {missing}");
            assert_eq!(ledger.status(), before);
            continue;
        }
        let original = ledger.status();
        let next = ledger.request_idle_replacement(now).unwrap();
        assert!(next.replica.id > add.replica.id && next.replica.instance > add.replica.instance);
        assert!(next.replica.id > ledger.placements[0].replica);
        assert_eq!(next.replica.id, next.replica.instance);
        assert_eq!(next.unit, add.unit);
        assert_eq!(next.service, add.service);
        assert_eq!(ledger.request_idle_replacement(now).unwrap(), next);
        assert_eq!(ledger.status()["idle_predecessor"]["build"], original["idle_build"]);
        assert_eq!(ledger.status()["idle_predecessor"]["deletion"], original["idle_deletion"]);
        ledger.idle_reply(0, added).unwrap();
        ledger.idle_removal_reply(0, removed).unwrap();
        ledger.reply(1, deleted, true).unwrap();
        ledger.retirement(1, proof, 555).unwrap();
        assert_eq!(ledger.idle_status()["state"], "pending");
        assert_eq!(ledger.idle_removal_status()["state"], "absent");
        assert_eq!(ledger.idle_deletion_status()["state"], "absent");
        assert!(!ledger.placements[1].reusable && ledger.placements[1].retirement.is_none());
    }
}

#[test]
fn peer_host_loss_processes_only_owned_creation_and_never_completes_primary_remove_or_disposal() {
    for add_acknowledged in [false, true] {
        for removing in [false, true] {
            let mut ledger = peer_idle_ledger();
            let now = Instant::now();
            let report = stateful_loss_report(&ledger.placements[1]);
            assert!(ledger.report(1, &report).is_err());
            let add = ledger.request_idle(now).unwrap();
            let mut added = AddReplicaReply { unit: add.unit.clone(), replica: add.replica.clone(), error: 0 };
            added.replica.state = 2;
            added.replica.replication_endpoint = "127.0.0.1:23457".into();
            if add_acknowledged { ledger.idle_reply(0, added.clone()).unwrap(); }
            let removal = removing.then(|| ledger.request_idle_removal(now).unwrap());
            let before_add = ledger.idles.first().unwrap().reply.clone();
            let reply = ledger.report(1, &report).unwrap().unwrap();
            assert_eq!(reply.replicas, report.dropped);
            assert!(reply.dropped.0.is_empty() && !reply.last && !reply.from_fmm);
            assert_eq!(ledger.placements[0].observed, Observed::Running);
            assert_eq!(ledger.placements[1].observed, Observed::Lost);
            assert_eq!(ledger.idle_status()["state"], "lost");
            assert_eq!(ledger.idle_status()["ready"], false);
            assert_eq!(ledger.idle_status()["destination_lost"], true);
            assert_eq!(ledger.idle_removal_status()["pending_remove"], removing);
            assert!(ledger.placements[1].retirement.is_none() && !ledger.placements[1].reusable);
            assert!(ledger.request_idle(now).is_err());
            assert!(ledger.idle_retry(0, now + Duration::from_secs(11)).is_none());
            if !removing {
                assert!(ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).is_err());
            }
            assert!(ledger.idle_deletion.is_none());
            assert!(ledger.deletion_retry(1, Instant::now() + Duration::from_secs(9)).unwrap().is_none());
            assert_eq!(ledger.deletion_retry(1, Instant::now() + Duration::from_secs(11)).unwrap(), Some(DeleteReplica::new(add, false)));
            ledger.idle_reply(0, added).unwrap();
            assert_eq!(ledger.idles.first().unwrap().reply, before_add);
            assert_eq!(ledger.report(1, &report).unwrap(), Some(reply));
            ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
            assert_eq!(ledger.placements[1].status()["native_replica_deleted"], true);
            assert_eq!(ledger.placements[1].observed, Observed::Lost);
            assert_eq!(ledger.idle_status()["destination_cleanup_required"], true);
            assert_eq!(ledger.idle_deletion_status()["complete"], false);
            assert_eq!(ledger.idle_deletion_status()["disposed"], false);
            assert!(ledger.deletion_retry(1, now + Duration::from_secs(200)).unwrap().is_none());
            let before = ledger.status();
            ledger.retirement(1, retirement(&ledger.placements[1]), 555).unwrap();
            assert_eq!(ledger.status(), before);
            assert_eq!(ledger.idle_removal_status()["pending_remove"], removing);
            if let Some(removal) = removal {
                let mut replica = removal.replica;
                if add_acknowledged { replica.state = 3; }
                ledger.idle_removal_reply(0, RemoveReplicaReply { unit: removal.unit, replica, error: 0 }).unwrap();
                assert_eq!(ledger.idle_removal_status()["pending_remove"], false);
                assert_eq!(ledger.idle_status()["state"], "lost");
            }
        }
    }
}

#[test]
fn peer_host_loss_rejects_foreign_reports_and_requires_the_original_live_destination_session() {
    let now = Instant::now();
    for lost in [None, Some(0), Some(1)] {
        let mut ledger = peer_idle_ledger();
        ledger.request_idle(now).unwrap();
        let report = stateful_loss_report(&ledger.placements[1]);
        for kind in 0..7 {
            let mut foreign = report.clone();
            let info = &mut foreign.dropped.0[0];
            match kind {
                0 => info.replicas.0[0].description.up = true,
                1 => info.replicas.0[0].description.instance -= 1,
                2 => info.replicas.0[0].description.node.instance += 1,
                3 => info.unit.current.configuration += 1,
                4 => info.local_deleted = true,
                5 => info.replicas.0[0].description.current_role = 2,
                _ => foreign.from_fmm = true,
            }
            assert!(ledger.report(1, &foreign).is_err());
            assert!(!ledger.placements[1].loss_reported);
        }
        if let Some(index) = lost { ledger.lost(index, "original session lost".into()); }
        if lost == Some(1) { assert!(ledger.report(1, &report).is_err()); }
        else {
            assert!(ledger.report(1, &report).unwrap().is_some());
            assert_eq!(ledger.placements[1].observed, Observed::Lost);
            assert!(ledger.placements[1].retirement.is_none() && !ledger.placements[1].reusable);
        }
    }
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
fn idle_deletion_joins_original_host_loss_without_fabricating_disposal_or_primary_remove() {
    for (force, target) in [
        (false, IdleDeleteTarget::Runtime), (true, IdleDeleteTarget::Runtime),
        (true, IdleDeleteTarget::PreRegistration), (true, IdleDeleteTarget::Unobserved),
    ] {
        for proof_first in [false, true] {
            let mut ledger = peer_idle_ledger();
            let now = Instant::now();
            ledger.request_idle(now).unwrap();
            ledger.request_idle_removal(now).unwrap();
            let request = ledger.request_idle_deletion_with_force(now, target, force).unwrap();
            let placement = &ledger.placements[1];
            let profile = placement.profile.as_ref().unwrap();
            let proof = HostLossReceipt {
                deployment: placement.launch.deployment.clone(), session: placement.launch.session.clone(),
                node: placement.launch.node.clone(), fm: placement.launch.fm.clone(), generation: placement.launch.generation.clone(),
                node_pid: 555, app_pid: profile.app_pid, runtime_root: profile.runtime_root.clone(),
                loss: fube_node::local::ReplicaLoss {
                    body: pre_open_cancelled_body(&add_body(&placement.launch, profile)),
                    creation: 11, operation: 17, host: "original-host".into(), app_pid: profile.app_pid,
                },
            };
            for kind in 0..9 {
                let mut foreign = proof.clone();
                match kind {
                    0 => foreign.session.push_str("-foreign"),
                    1 => foreign.node.instance += 1,
                    2 => foreign.generation.number.generation += 1,
                    3 => foreign.loss.body.replica.id += 1,
                    4 => foreign.loss.creation = 0,
                    5 => foreign.loss.operation = foreign.loss.creation,
                    6 => foreign.loss.app_pid += 1,
                    7 => foreign.runtime_root.push("foreign"),
                    _ => foreign.node_pid += 1,
                }
                assert!(ledger.host_loss(1, foreign, 555).is_err());
            }
            let reply = remove_reply(&ledger.placements[1]);
            if proof_first { ledger.host_loss(1, proof.clone(), 555).unwrap(); }
            else { ledger.reply(1, reply.clone(), true).unwrap(); }
            assert_eq!(ledger.idle_deletion_status()["complete"], false);
            assert_eq!(ledger.idle_deletion_status()["disposed"], false);
            ledger.host_loss(1, proof, 555).unwrap();
            ledger.reply(1, reply, true).unwrap();
            assert_eq!(ledger.idle_deletion_status()["completion"], "host_lost");
            assert_eq!(ledger.idle_deletion_status()["complete"], true);
            assert_eq!(ledger.idle_deletion_status()["disposed"], false);
            assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
            assert!(!ledger.placements[1].reusable);
            assert_eq!(request.deletion.0.force, force);
            assert!(ledger.retirement(1, retirement(&ledger.placements[1]), 555).is_err());
        }
    }
}

#[test]
fn idle_delete_after_terminal_report_requires_explicit_intent_and_original_cleanup_proof() {
    let mut ledger = peer_idle_ledger();
    let now = Instant::now();
    ledger.request_idle(now).unwrap();
    ledger.request_idle_removal(now).unwrap();
    let report = stateful_loss_report(&ledger.placements[1]);
    ledger.report(1, &report).unwrap();
    let proof = retirement(&ledger.placements[1]);
    ledger.retirement(1, proof.clone(), 555).unwrap();
    assert!(ledger.placements[1].retirement.is_none());
    ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
    ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
    assert_eq!(ledger.idle_deletion_status()["complete"], false);
    ledger.retirement(1, proof, 555).unwrap();
    assert_eq!(ledger.idle_deletion_status()["completion"], "disposed");
    assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
    assert!(!ledger.placements[1].reusable);
}

#[test]
fn idle_force_escalation_preserves_original_descriptor_and_retry_mode() {
    let mut ledger = peer_idle_ledger();
    let now = Instant::now();
    ledger.request_idle(now).unwrap();
    ledger.request_idle_removal(now).unwrap();
    let normal = ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap();
    let forced = ledger.request_idle_deletion_with_force(now, IdleDeleteTarget::Runtime, true).unwrap();
    assert_eq!(normal.replica, forced.replica);
    assert!(!normal.deletion.0.force && forced.deletion.0.force);
    assert_eq!(ledger.deletion_retry(1, now + Duration::from_secs(11)).unwrap(), Some(forced.clone()));
    assert_eq!(ledger.request_idle_deletion(now, IdleDeleteTarget::Runtime).unwrap(), forced);
    ledger.lost(1, "original destination Node owner lost".into());
    ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
    assert_eq!(ledger.idle_deletion_status()["state"], "unknown");
    assert_eq!(ledger.idle_deletion_status()["complete"], false);
}

#[test]
fn idle_delete_accepts_actual_failure_winning_a_pending_creation_race_without_inventing_add_error() {
    for target in [IdleDeleteTarget::Runtime, IdleDeleteTarget::PreRegistration, IdleDeleteTarget::Unobserved] {
        for runtime_failure in [false, true] {
            let mut ledger = peer_idle_ledger();
            let now = Instant::now();
            ledger.request_idle(now).unwrap();
            ledger.request_idle_removal(now).unwrap();
            ledger.request_idle_deletion(now, target).unwrap();
            let accepted = if runtime_failure {
                ledger.failed_open(1, failed_open_receipt(&ledger.placements[1], -995), 555)
            } else {
                ledger.failed_registration(1, failed_registration_receipt(&ledger.placements[1], -994), 555)
            };
            if !runtime_failure && target == IdleDeleteTarget::Runtime {
                assert!(accepted.is_err());
                assert_eq!(ledger.idle_deletion_status()["complete"], false);
                continue;
            }
            accepted.unwrap();
            assert_eq!(ledger.idle_deletion_status()["complete"], false);
            ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
            assert_eq!(ledger.idle_deletion_status()["completion"], if runtime_failure { "failed_open" } else { "failed_registration" });
            assert_eq!(ledger.idle_status()["error"], Value::Null);
            assert_eq!(ledger.idle_status()["ready"], false);
            assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
        }
    }
}

#[test]
fn failed_open_delete_joins_disposal_and_native_reply_without_clearing_primary_removal() {
    for proof_first in [false, true] {
        let mut ledger = peer_idle_ledger();
        let now = Instant::now();
        let add = ledger.request_idle(now).unwrap();
        assert!(ledger.request_idle_deletion(now, IdleDeleteTarget::FailedOpen).is_err());
        ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -995 }).unwrap();
        let removal = ledger.request_idle_removal(now).unwrap();
        ledger.request_idle_deletion(now, IdleDeleteTarget::FailedOpen).unwrap();
        let proof = failed_open_receipt(&ledger.placements[1], -995);
        let reply = remove_reply(&ledger.placements[1]);
        if proof_first { ledger.failed_open(1, proof.clone(), 555).unwrap(); }
        else { ledger.reply(1, reply.clone(), true).unwrap(); }
        assert_eq!(ledger.idle_deletion_status()["complete"], false);
        assert_eq!(ledger.idle_deletion_status()["completion"], Value::Null);
        ledger.failed_open(1, proof, 555).unwrap();
        ledger.reply(1, reply, true).unwrap();
        assert_eq!(ledger.idle_deletion_status()["completion"], "failed_open");
        assert_eq!(ledger.idle_deletion_status()["disposed"], true);
        assert_eq!(ledger.idle_deletion_status()["pending_remove"], true);
        assert_eq!(ledger.idle_status()["state"], "failed");
        assert_eq!(ledger.idle_status()["error"], -995);
        assert_eq!(ledger.idle_status()["destination_cleanup_required"], false);
        assert!(!ledger.placements[1].reusable);
        assert!(ledger.retirement(1, retirement(&ledger.placements[1]), 555).is_err());
        assert!(ledger.absence(1, absence_receipt(&ledger.placements[1]), 555).is_err());
        assert!(ledger.pre_open_cancellation(1, pre_open_receipt(&ledger.placements[1]), 555).is_err());
        assert!(ledger.failed_registration(1, failed_registration_receipt(&ledger.placements[1], -995), 555).is_err());
        ledger.idle_removal_reply(0, RemoveReplicaReply { unit: removal.unit, replica: removal.replica, error: 0 }).unwrap();
        assert_eq!(ledger.idle_removal_status()["completion"], "failed_creation_retired");
        assert_eq!(ledger.idle_status()["error"], -995);
        ledger.lost(1, "later owner loss".into());
        assert_eq!(ledger.idle_deletion_status()["state"], "deleted");
    }
}

#[test]
fn failed_open_proofs_reject_foreign_lineage_and_cannot_replace_other_cleanup_origins() {
    let now = Instant::now();
    for lost in [None, Some(0), Some(1)] {
        let mut ledger = peer_idle_ledger();
        let add = ledger.request_idle(now).unwrap();
        ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -995 }).unwrap();
        ledger.request_idle_deletion(now, IdleDeleteTarget::FailedOpen).unwrap();
        let proof = failed_open_receipt(&ledger.placements[1], -995);
        for kind in 0..15 {
            let mut foreign = proof.clone();
            match kind {
                0 => foreign.node.instance += 1,
                1 => foreign.session = ledger.placements[0].launch.session.clone(),
                2 => foreign.deletion.failure.error = -994,
                3 => foreign.deletion.failure.disposal.body.replica.instance += 1,
                4 => foreign.deletion.failure.disposal.body.unit.target = 0,
                5 => foreign.deletion.failure.creation = 0,
                6 => foreign.deletion.failure.disposal.ra_operation = foreign.deletion.ra_operation,
                7 => foreign.deletion.command = foreign.deletion.failure.disposal.command,
                8 => foreign.node_pid += 1,
                9 => foreign.app_pid += 1,
                10 => foreign.deletion.failure.disposal.lookup = 0,
                11 => foreign.deletion.failure.disposal.acquisition = 0,
                12 => foreign.deletion.failure.disposal.host.clear(),
                13 => foreign.deletion.failure.disposal.runtime.clear(),
                _ => foreign.deletion.failure.error = fube_ra::entity::APPLICATION_HOST_CRASH,
            }
            assert!(ledger.failed_open(1, foreign, 555).is_err());
        }
        if let Some(index) = lost { ledger.lost(index, "original session loss".into()); }
        ledger.reply(1, remove_reply(&ledger.placements[1]), true).unwrap();
        ledger.failed_open(1, proof, 555).unwrap();
        assert_eq!(ledger.idle_deletion_status()["state"], if lost == Some(1) { "unknown" } else { "deleted" });
    }
    let mut ledger = peer_idle_ledger();
    let add = ledger.request_idle(now).unwrap();
    ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -995 }).unwrap();
    ledger.request_idle_removal(now).unwrap();
    ledger.request_idle_deletion(now, IdleDeleteTarget::FailedRegistration).unwrap();
    assert!(ledger.failed_open(1, failed_open_receipt(&ledger.placements[1], -995), 555).is_err());
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
    for target in [IdleDeleteTarget::Runtime, IdleDeleteTarget::FailedOpen] {
        let mut ledger = peer_idle_ledger();
        let add = ledger.request_idle(now).unwrap();
        if target == IdleDeleteTarget::FailedOpen {
            ledger.idle_reply(0, AddReplicaReply { unit: add.unit, replica: add.replica, error: -995 }).unwrap();
        }
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
            "cancelled" => ledger.idles.first_mut().unwrap().cancelled = true,
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
    invalid.peers.first_mut().unwrap().replica = invalid.replica;
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

async fn hold_peer_loss_frame(
    cluster: &mut Cluster,
    node: usize,
    action: &str,
    budget: Duration,
) -> Result<fube_transport::message::Message> {
    let end = Instant::now() + budget;
    loop {
        for index in 0..cluster.children.len() {
            if let Some(mut incoming) = cluster.children[index].link.as_ref().unwrap()
                .receive(Instant::now() + Duration::from_millis(2)).await?
            {
                if index == node && incoming.message.properties().map_err(err)?.action == action {
                    if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                    return Ok(incoming.message);
                }
                cluster.receive_message(index, incoming).await?;
            }
        }
        cluster.advance_initial_membership().await?;
        cluster.start_failover().await;
        cluster.advance_repair().await?;
        for index in 0..cluster.children.len() {
            if let Some(body) = cluster.ledger.idle_retry(index, Instant::now()) {
                cluster.send_idle(&body).await?;
            }
        }
        if Instant::now() >= end { return Err(format!("held {action} deadline: {}", cluster.status())); }
    }
}

#[tokio::test]
async fn peer_explicit_delete_finishes_host_loss_and_force_obliteration_without_disposal_or_reuse() {
    for force in [false, true] {
        let fixture = Fixture::new("peer-delete-host-loss-force");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.start().await.unwrap();
        let result = peer_idle_catch(async {
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
            let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
            cluster.add_idle_replica().await?;
            until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true
                && c.status()["peer_idle"]["target"]["open"] == true).await;
            let original = cluster.ledger.idles.first().unwrap().body.clone();
            let app_pid = cluster.children[1].metadata.as_ref().unwrap().profile.as_ref().unwrap().app_pid;
            cluster.remove_idle_replica_via_fm().await?;
            let removed = hold_peer_loss_frame(&mut cluster, 0, "RemoveReplicaReply", Duration::from_secs(12)).await?;
            let (removed, _) = decode_remove_replica_reply(&removed).map_err(err)?;
            if force {
                assert_eq!(cluster.delete_idle_replica_via_fm_with_force(true).await?["force"], true);
            } else {
                assert!(std::process::Command::new("kill").args(["-KILL", &app_pid.to_string()])
                    .status().map_err(err)?.success());
            }
            let report = hold_peer_loss_frame(&mut cluster, 1, "ReplicaUp", Duration::from_secs(12)).await?;
            let (report, _) = decode_replica_up(&report).map_err(err)?;
            let ack = cluster.ledger.report(1, &report)?.ok_or("missing native processed ACK")?;
            let generation = cluster.children[1].launch.generation.clone();
            cluster.children[1].link.as_ref().unwrap().send(replica_up_reply_message(&ack, &generation).map_err(err)?).await?;
            let first = hold_peer_loss_frame(&mut cluster, 1, "DeleteReplicaReply", Duration::from_secs(12)).await?;
            let (first, _) = decode_delete_replica_reply(&first).map_err(err)?;
            if !force { cluster.delete_idle_replica_via_fm().await?; }
            assert_eq!(cluster.ledger.idle_deletion_status()["complete"], false);
            let retry = cluster.ledger.deletion_retry(1, Instant::now() + Duration::from_secs(11))?
                .ok_or("missing original Delete retry")?;
            assert_eq!(retry, DeleteReplica::new(cluster.ledger.idle_removal.as_ref().unwrap().body.clone(), force));
            cluster.send_idle_deletion(&retry).await?;
            let replay = hold_peer_loss_frame(&mut cluster, 1, "DeleteReplicaReply", Duration::from_secs(12)).await?;
            assert_eq!(decode_delete_replica_reply(&replay).map_err(err)?.0, first);
            cluster.ledger.reply(1, first, true)?;
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    cluster.read_retirement(1)?;
                    if cluster.ledger.idle_deletion_status()["complete"] == true { break; }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok::<_, String>(())
            }).await.map_err(err)??;
            assert_eq!(cluster.ledger.idle_deletion_status()["completion"], "host_lost");
            assert_eq!(cluster.ledger.idle_deletion_status()["disposed"], false);
            assert_eq!(cluster.ledger.idle_deletion_status()["pending_remove"], true);
            assert!(cluster.ledger.placements[1].retirement.is_none() && !cluster.ledger.placements[1].reusable);
            assert!(cluster.children[1].launch.directory.join("host-loss.json").exists());
            assert!(!cluster.children[1].launch.directory.join("retirement.json").exists());
            cluster.ledger.idle_removal_reply(0, removed)?;
            assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2 value=continued"));
            cluster.send_idle(&original).await?;
            until(&mut cluster, |c| c.ledger.idle_deletion_status()["complete"] == true
                && c.ledger.idle_removal_status()["pending_remove"] == false).await;
            assert_eq!(cluster.status()["peer_idle"]["ready"], false);
            assert_eq!(cluster.ledger.idle_deletion_status()["completion"], "host_lost");
            Ok::<_, String>(())
        }).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_idle_app_host_loss_rejects_stale_ack_and_replays_metadata_without_completing_primary_remove() {
    let fixture = Fixture::new("peer-idle-host-loss");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |cluster| cluster.ledger.idle_status()["ready"] == true
            && cluster.status()["peer_idle"]["state"] == "open"
            && cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1).await;
        let add = cluster.ledger.idles.first().unwrap().body.clone();
        let added = cluster.ledger.idles.first().unwrap().reply.clone();
        let original_token = cluster.status()["peer_idle"]["target"]["open_token"].clone();
        cluster.remove_idle_replica_via_fm().await?;
        let removed = hold_peer_loss_frame(&mut cluster, 0, "RemoveReplicaReply", Duration::from_secs(12)).await?;
        let (removed, owner) = decode_remove_replica_reply(&removed).map_err(err)?;
        assert_eq!(owner, Owner::Fm);
        assert_eq!(cluster.ledger.idle_removal_status()["pending_remove"], true);
        let profile = cluster.children[1].metadata.as_ref().unwrap().profile.as_ref().unwrap().clone();
        assert!(std::process::Command::new("kill").args(["-KILL", &profile.app_pid.to_string()])
            .status().map_err(err)?.success());
        let first = hold_peer_loss_frame(&mut cluster, 1, "ReplicaUp", Duration::from_secs(12)).await?;
        let (report, owner) = decode_replica_up(&first).map_err(err)?;
        assert_eq!(owner, Owner::Fm);
        assert_eq!(report, stateful_loss_report(&cluster.ledger.placements[1]));
        let reply = cluster.ledger.report(1, &report)?.ok_or("missing original Idle loss ACK")?;
        assert_eq!(reply.replicas, report.dropped);
        assert!(reply.dropped.0.is_empty());
        assert_eq!(cluster.ledger.placements[1].observed, Observed::Lost);
        assert_eq!(cluster.ledger.placements[0].observed, Observed::Running);
        assert_eq!(cluster.status()["peer_idle"]["ready"], false);
        assert_eq!(cluster.ledger.idle_removal_status()["pending_remove"], true);
        let mut stale = reply.clone();
        stale.replicas.0[0].replicas.0[0].description.instance -= 1;
        cluster.children[1].link.as_ref().unwrap().send(
            replica_up_reply_message(&stale, &cluster.children[1].launch.generation).map_err(err)?).await?;
        let end = Instant::now() + Duration::from_secs(3);
        while cluster.status()["peer_idle"]["target"]["fm_stage"] != "ReplicaDown" {
            assert!(Instant::now() < end, "original Idle report observation deadline");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(cluster.ledger.report(1, &report)?, Some(reply.clone()));
        assert_eq!(cluster.status()["peer_idle"]["target"]["fm_stage"], "ReplicaDown");
        assert_eq!(cluster.status()["peer_idle"]["target"]["retained_usage"], 0);
        let generation = cluster.children[1].launch.generation.clone();
        cluster.children[1].link.as_ref().unwrap().send(replica_up_reply_message(&reply, &generation).map_err(err)?).await?;
        let deleted = hold_peer_loss_frame(&mut cluster, 1, "DeleteReplicaReply", Duration::from_secs(12)).await?;
        let (deleted, owner) = decode_delete_replica_reply(&deleted).map_err(err)?;
        assert_eq!(owner, Owner::Fm);
        assert_eq!(cluster.ledger.placements[1].status()["native_replica_deleted"], false);
        let retry = cluster.ledger.deletion_retry(1, Instant::now() + Duration::from_secs(11))?.ok_or("missing lost metadata Delete retry")?;
        assert_eq!(retry, DeleteReplica::new(add.clone(), false));
        cluster.children[1].link.as_ref().unwrap().send(delete_replica_message(&retry, &generation).map_err(err)?).await?;
        let replay = hold_peer_loss_frame(&mut cluster, 1, "DeleteReplicaReply", Duration::from_secs(12)).await?;
        assert_eq!(decode_delete_replica_reply(&replay).map_err(err)?.0, deleted);
        cluster.ledger.reply(1, deleted, true)?;
        let evidence = cluster.children[1].launch.directory.join("host-loss.json");
        let end = Instant::now() + Duration::from_secs(8);
        while !evidence.try_exists().map_err(err)? {
            for index in 0..2 {
                if let Some(mut incoming) = cluster.children[index].link.as_ref().unwrap()
                    .receive(Instant::now() + Duration::from_millis(2)).await?
                {
                    if index == 0 && incoming.message.properties().map_err(err)?.action == "RemoveReplicaReply" {
                        assert_eq!(decode_remove_replica_reply(&incoming.message).map_err(err)?.0, removed);
                        if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                    } else {
                        cluster.receive_message(index, incoming).await?;
                    }
                }
            }
            if Instant::now() >= end {
                return Err(format!("original host-loss evidence deadline: {}", cluster.status()));
            }
        }
        cluster.read_retirement(1)?;
        let loss: HostLossReceipt = read_json(&evidence)?;
        assert!(cluster.ledger.idle_deletion.is_none());
        assert_eq!(cluster.ledger.placements[1].observed, Observed::Lost);
        assert_eq!(cluster.ledger.placements[1].status()["native_replica_deleted"], true);
        assert_eq!(cluster.ledger.idle_removal_status()["pending_remove"], true);
        assert_eq!(cluster.ledger.idle_status()["state"], "lost");
        assert_eq!(cluster.ledger.idle_status()["destination_cleanup_required"], true);
        assert_eq!(cluster.ledger.idle_deletion_status()["complete"], false);
        assert_eq!(cluster.ledger.idle_deletion_status()["disposed"], false);
        assert!(cluster.ledger.placements[1].retirement.is_none() && !cluster.ledger.placements[1].reusable);
        assert_eq!(json!(loss.loss.creation), original_token);
        for name in ["retirement.json", "absence.json", "pre-open-cancellation.json",
            "failed-registration-deletion.json", "failed-open-deletion.json"]
        {
            assert!(!cluster.children[1].launch.directory.join(name).exists(), "{name}");
        }
        cluster.ledger.idle_removal_reply(0, removed)?;
        assert_eq!(cluster.ledger.idle_removal_status()["pending_remove"], false);
        cluster.send_idle(&add).await?;
        for _ in 0..15 { cluster.drive().await; }
        assert_eq!(cluster.ledger.idles.first().unwrap().reply, added);
        assert_eq!(cluster.status()["peer_idle"]["state"], "lost");
        assert!(cluster.add_idle_replica().await.is_err());
        assert_eq!(cluster.delete_idle_replica_via_fm().await?["state"], "pending");
        until(&mut cluster, |c| c.ledger.idle_deletion_status()["complete"] == true).await;
        assert_eq!(cluster.ledger.idle_deletion_status()["completion"], "host_lost");
        assert!(cluster.execute(command("replacement", "create", Some(2))).await.is_err());
        let continued = data_contact(&endpoint, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        assert_eq!(data_contact(&endpoint, b"read\n"), continued);
        for child in &mut cluster.children { child.process.as_mut().unwrap().check_alive().map_err(err)?; }
        let terminated = match fs::read_to_string(format!("/proc/{}/stat", profile.app_pid)) {
            Ok(stat) => stat.rsplit_once(')').is_some_and(|(_, rest)|
                matches!(rest.split_whitespace().next(), Some("Z" | "X"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(err(error)),
        };
        assert!(terminated);
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
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
            let request = cluster.ledger.idles.first().unwrap().body.clone();
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
async fn fm_can_remove_ready_idle_without_waiting_for_open_telemetry() {
    let fixture = Fixture::new("fm-ready-idle-remove");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    let mut cluster = Cluster::new(config).unwrap();
    cluster.ledger = peer_idle_ledger();
    cluster.peer_idle_requested = true;
    for (index, placement) in cluster.ledger.placements.iter_mut().enumerate() {
        placement.launch.directory = fixture.root.join(format!("node{index}"));
    }
    let launch = cluster.ledger.placements[0].launch.clone();
    let (fm, node) = node::report_tests::links(&launch).await.unwrap();
    for placement in &cluster.ledger.placements {
        cluster.children.push(ChildOwner {
            launch: placement.launch.clone(),
            process: None,
            link: (placement.launch.node == launch.node).then(|| fm.clone()),
            metadata: None,
        });
    }
    let add = cluster.ledger.request_idle(Instant::now()).unwrap();
    let mut replica = add.replica.clone();
    replica.state = 2;
    replica.replication_endpoint = "127.0.0.1:12346".into();
    cluster.ledger.idle_reply(0, AddReplicaReply {
        unit: add.unit, replica: replica.clone(), error: 0,
    }).unwrap();
    assert_eq!(cluster.ledger.idle_status()["ready"], true);
    assert_eq!(cluster.status()["peer_idle"]["state"], "pending");
    let result = cluster.remove_idle_replica_via_fm().await;
    if result.is_ok() {
        let mut incoming = node.receive(wire::deadline()).await.unwrap().unwrap();
        let (body, _) = decode_remove_replica(&incoming.message).unwrap();
        assert_eq!(body.replica, replica);
        if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
        assert_eq!(cluster.ledger.idle_removal_status()["pending_remove"], true);
    }
    node.close().await.unwrap();
    assert_eq!(cluster.shutdown().await["clean"], true);
    assert_eq!(result.unwrap()["state"], "pending");
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
        let add = cluster.ledger.idles.first().unwrap().body.clone();
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
async fn peer_graceful_close_waits_for_real_disposal_and_retries_report_without_authorizing_replacement() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::GracefulCloseProvider,
        node::report_tests::PeerCreationOutcome::GracefulCloseService,
    ] {
        let fixture = Fixture::new("peer-idle-graceful-close");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_drop_waits_for_real_disposal_and_retries_dropped_stage_without_authorizing_replacement() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DropProvider,
        node::report_tests::PeerCreationOutcome::DropService,
    ] {
        let fixture = Fixture::new("peer-idle-drop");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_abort_waits_for_real_disposal_and_retries_dropped_stage_without_authorizing_replacement() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::AbortProvider,
        node::report_tests::PeerCreationOutcome::AbortService,
    ] {
        let fixture = Fixture::new("peer-idle-abort");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_normal_delete_during_or_after_close_drop_abort_joins_actual_disposal_and_metadata() {
    for mode in [fube_ra::entity::CloseMode::Close, fube_ra::entity::CloseMode::Drop, fube_ra::entity::CloseMode::Abort] {
        for (provider, during) in [(true, true), (false, false)] {
            let fixture = Fixture::new("peer-delete-terminal-matrix");
            let mut config = fixture.config();
            config.profile = ServiceProfile::StatefulPeerIdle;
            config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
            let mut cluster = Cluster::new(config).unwrap();
            cluster.prepare().unwrap();
            let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
            let result = node::report_tests::run_peer_creation_removal(&launches,
                node::report_tests::PeerCreationOutcome::DeleteTerminal { mode, provider, during }).await;
            assert_eq!(cluster.shutdown().await["clean"], true);
            result.unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn peer_force_delete_before_runtime_uses_absence_or_cancelled_creation_not_disposal() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DeleteAbsentForce,
        node::report_tests::PeerCreationOutcome::DeletePreRegistrationForce,
    ] {
        let fixture = Fixture::new("peer-force-delete-before-runtime");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_force_delete_supersedes_held_normal_disposal_and_requires_original_loss_and_metadata() {
    for provider in [false, true] {
        let fixture = Fixture::new("peer-delete-force-disposal");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches,
            node::report_tests::PeerCreationOutcome::DeleteForceDuringDisposal { provider }).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_delete_keeps_original_applied_cleanup_facts_after_app_exit_without_adopting_an_owner() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DeleteAbsentAfterHostExit,
        node::report_tests::PeerCreationOutcome::DeletePreRegistrationAfterHostExit,
        node::report_tests::PeerCreationOutcome::DeleteFailedRegistrationAfterHostExit,
        node::report_tests::PeerCreationOutcome::DeleteFailedOpenAfterHostExit,
        node::report_tests::PeerCreationOutcome::DeleteProviderAfterHostExit,
    ] {
        let fixture = Fixture::new("peer-delete-after-app-exit");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_normal_close_host_loss_with_held_provider_or_service_never_fabricates_disposal() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::CloseLossProvider,
        node::report_tests::PeerCreationOutcome::CloseLossService,
    ] {
        let fixture = Fixture::new("peer-idle-closing-host-loss");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn peer_pending_drop_host_loss_with_held_provider_or_service_never_fabricates_disposal() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DropLossProvider,
        node::report_tests::PeerCreationOutcome::DropLossService,
    ] {
        let fixture = Fixture::new("peer-idle-dropping-host-loss");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
        let result = node::report_tests::run_peer_creation_removal(&launches, outcome).await;
        assert_eq!(cluster.shutdown().await["clean"], true);
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn failed_open_delete_waits_for_actual_abort_and_disposal_then_replays_lost_reply() {
    for outcome in [
        node::report_tests::PeerCreationOutcome::DeleteFailedOpenProvider,
        node::report_tests::PeerCreationOutcome::DeleteFailedOpenService,
    ] {
        let fixture = Fixture::new("peer-idle-failed-open-delete");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulPeerIdle;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster.prepare().unwrap();
        let launches = [cluster.ledger.placements[0].launch.clone(), cluster.ledger.placements[1].launch.clone()];
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
async fn immutable_configured_service_builds_initial_membership_without_resize() {
    for (target, minimum) in [(1, 1), (2, 1), (3, 2), (4, 2), (4, 3), (5, 3), (4, 4)] {
        let fixture = Fixture::new("immutable-sizes");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful { target, minimum };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("configured startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            let creation = cluster.ledger.placements[0].body()?;
            let service = creation.service.clone();
            assert_eq!((service.target, service.minimum), (target, minimum));
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
            let endpoint = cluster.ledger.placements[0].endpoint.clone().ok_or("missing endpoint")?;
            if target > 1 {
                assert_eq!(cluster.ledger.result("primary")["state"], "pending");
                assert!(!cluster.ledger.configuration_status()["ready"].as_bool().unwrap());
            }
            if minimum > 1 {
                assert!(!data_contact(&endpoint, b"write\n").starts_with("OK "));
            }
            if target > 1 {
                let lost = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(30)).await?;
                assert_eq!(cluster.ledger.result("primary")["state"], "pending");
                assert_eq!(cluster.ledger.configuration_status()["ready"], false);
                let request = cluster.ledger.configuration.as_ref().unwrap().body.clone();
                assert_eq!(request.configuration.service, service);
                assert_eq!(request.configuration.replicas.0.len(), target as usize);
                for idle in &cluster.ledger.idles {
                    assert_eq!(idle.body.service, service);
                    assert_eq!((idle.body.unit.target, idle.body.unit.minimum), (target, minimum));
                }
                let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).ok_or("missing configuration retry")?;
                assert_eq!(retry, request);
                cluster.send_configuration(&retry).await?;
                until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
                let (reply, _) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
                assert_eq!(cluster.ledger.configuration.as_ref().unwrap().reply.as_ref(), Some(&reply));
            }
            until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
            until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
            let written = data_contact(&endpoint, b"write\n");
            assert!(written.contains("committed=1 value=acknowledged"), "{target}/{minimum}: {written}");
            until(&mut cluster, |c| (1..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
            until(&mut cluster, |c| (0..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["opens"] == 1
                && c.status()["nodes"][i]["retained_work"]["local_replica"]["replica"]["current_role"] == if i == 0 { 4 } else { 3 })).await;
            assert!(cluster.transfer_primary().await.is_err());
            assert!(cluster.retire_secondary_replica().await.is_err());
            assert!(cluster.repair_primary_replica().await.is_err());
            for placement in &cluster.ledger.placements {
                assert_eq!(placement.body()?.service, service);
                let mut resize = placement.body()?;
                resize.service.minimum = if minimum == 1 { 2 } else { 1 };
                assert!(placement.launch.profile.control_profile().check_service_sizes(&resize.unit, &resize.service).is_err());
            }
            let status = cluster.status();
            for (i, node) in status["nodes"].as_array().unwrap().iter().enumerate() {
                assert_eq!(node["retained_work"]["app_observation"]["opens"], 1);
                assert_eq!(node["retained_work"]["local_replica"]["replica"]["current_role"], if i == 0 { 4 } else { 3 });
            }
            if target == 4 && (minimum == 2 || minimum == 4) {
                let lost: &[usize] = if minimum == 2 { &[2, 3] } else { &[3] };
                for &index in lost {
                    let pid = cluster.ledger.placements[index].profile.as_ref().unwrap().app_pid;
                    assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
                }
                until(&mut cluster, |c| lost.iter().all(|index| c.ledger.placements[*index].loss_reported)).await;
                assert_eq!(cluster.ledger.configuration_status()["state"], "unknown");
                assert!(cluster.ledger.start_primary_failover(Instant::now())?.is_none());
                if minimum == 4 {
                    assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2"));
                } else {
                    let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
                    pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
                    pending.write_all(b"write-more\n").map_err(err)?;
                    until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_admitted"] == 2).await;
                    let mut bytes = [0; 1024];
                    assert!(matches!(pending.read(&mut bytes), Err(ref e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
                    assert_eq!(cluster.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_acknowledged"], 1);
                }
            } else if target == 2 {
                let mut resize = cluster.ledger.configuration.as_ref().unwrap().body.clone();
                resize.configuration.unit.previous = resize.configuration.unit.current;
                resize.configuration.unit.current.configuration += 1;
                resize.configuration.unit.target = 1;
                resize.configuration.service.target = 1;
                resize.configuration.service.update_version += 1;
                for replica in &mut resize.configuration.replicas.0 {
                    replica.previous_role = replica.current_role;
                    if replica.current_role == 3 { replica.current_role = 1; }
                }
                cluster.send_configuration(&resize).await?;
                until(&mut cluster, |c| !c.ledger.placements[0].link_alive).await;
                assert_eq!(cluster.ledger.configuration_status()["state"], "unknown");
            }
            Ok::<(), String>(())
        }).await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("CONFIGURED_STATUS {}", cluster.status());
            for child in &cluster.children { eprintln!("CONFIGURED_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[test]
fn configured_sizes_reject_invalid_bounds_before_acquiring_a_deployment() {
    for (target, minimum) in [(0, 0), (1, 0), (2, 3), (65, 1), (-1, 1)] {
        let fixture = Fixture::new("invalid-sizes");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful { target, minimum };
        assert!(Cluster::new(config).is_err());
        assert!(!fixture.root.exists());
    }
}

#[tokio::test]
async fn configured_primary_failover_preserves_sizes_data_and_quorum_with_unavailable_secondaries() {
    for (target, minimum, secondary_losses, expected) in [
        (2, 1, 0, true), (4, 2, 1, true), (4, 3, 1, true), (4, 4, 0, true),
        (5, 3, 1, true), (5, 1, 3, false),
    ] {
        let fixture = Fixture::new("configured-failover");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful { target, minimum };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("configured failover startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
            let service = cluster.ledger.placements[0].body()?.service;
            let old_endpoint = cluster.ledger.placements[0].endpoint.clone().ok_or("missing endpoint")?;
            until(&mut cluster, |_| data_contact(&old_endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&old_endpoint, b"write\n").contains("committed=1"));
            until(&mut cluster, |c| (1..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
            let owners = cluster.status()["nodes"].as_array().unwrap().iter()
                .map(|n| (n["node_pid"].clone(), n["app_pid"].clone(), n["runtime_root"].clone())).collect::<Vec<_>>();
            let losses: Vec<_> = (target as usize - secondary_losses..target as usize).collect();
            for &index in &losses {
                let pid = cluster.ledger.placements[index].profile.as_ref().unwrap().app_pid;
                assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
            }
            until(&mut cluster, |c| losses.iter().all(|i| c.ledger.placements[*i].loss_reported)).await;
            let pid = cluster.ledger.placements[0].profile.as_ref().unwrap().app_pid;
            assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
            if !expected {
                until(&mut cluster, |c| c.ledger.placements[0].loss_reported).await;
                for _ in 0..20 { cluster.drive().await; }
                assert!(cluster.ledger.swap.is_none());
                assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
                assert_eq!(cluster.status()["nodes"][1]["retained_work"]["local_replica"]["replica"]["current_role"], 3);
                return Ok(());
            }
            let lost = hold_peer_loss_frame(&mut cluster, 1, "DoReconfigurationReply", Duration::from_secs(30)).await?;
            assert_eq!(cluster.ledger.configuration_status()["ready"], false);
            assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
            let request = cluster.ledger.swap.as_ref().unwrap().body.clone();
            assert_eq!(request.configuration.service, service);
            assert_eq!(request.configuration.replicas.0.iter().filter(|r| r.previous_role >= 3).count(), target as usize);
            let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).ok_or("missing failover retry")?;
            assert_eq!(retry, request);
            cluster.send_configuration(&retry).await?;
            until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
            let (reply, _) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
            assert_eq!(cluster.ledger.swap.as_ref().unwrap().reply.as_ref(), Some(&reply));
            let endpoint = cluster.ledger.placements[1].endpoint.clone().ok_or("missing new Primary endpoint")?;
            if minimum as usize > reply.replicas.0.len() {
                assert!(!data_contact(&endpoint, b"write-more\n").starts_with("OK "));
                assert_eq!(cluster.ledger.placements[1].body()?.service, service);
                return Ok(());
            }
            assert!(data_contact(&endpoint, b"read\n").contains("committed=1 value=acknowledged"));
            let written = data_contact(&endpoint, b"write-more\n");
            assert!(written.contains("committed=2 value=continued"), "{target}/{minimum}: {written}");
            until(&mut cluster, |c| (2..target as usize).filter(|i| !losses.contains(i)).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2)).await;
            until(&mut cluster, |c| (1..target as usize).filter(|i| !losses.contains(i)).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["opens"] == 1)).await;
            let status = cluster.status();
            for i in (1..target as usize).filter(|i| !losses.contains(i)) {
                assert_eq!(owners[i], (status["nodes"][i]["node_pid"].clone(), status["nodes"][i]["app_pid"].clone(), status["nodes"][i]["runtime_root"].clone()));
                assert_eq!(status["nodes"][i]["retained_work"]["peer_command_error"], Value::Null);
                assert_eq!(status["nodes"][i]["retained_work"]["peer_error"], Value::Null);
                assert_eq!(cluster.ledger.placements[i].body()?.service, service);
            }
            if secondary_losses > 0 {
                assert!(cluster.repair_primary_replica().await.is_err());
            }
            Ok::<(), String>(())
        }).await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("CONFIGURED_FAILOVER_STATUS {}", cluster.status());
            for child in &cluster.children { eprintln!("CONFIGURED_FAILOVER_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn configured_candidate_redirection_routes_new_epoch_and_rejects_deprecated_reply() {
    let fixture = Fixture::new("configured-redirection");
    let mut config = fixture.config();
    config.profile = ServiceProfile::Stateful { target: 4, minimum: 2 };
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    if let Err(error) = cluster.start().await {
        let cleanup = cluster.shutdown().await;
        panic!("redirection startup: {error}; cleanup={cleanup}");
    }
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
        until(&mut cluster, |c| (1..4).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
        let pid = cluster.ledger.placements[0].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
        let lost = hold_peer_loss_frame(&mut cluster, 0, "ReplicaUp", Duration::from_secs(12)).await?;
        let (report, _) = decode_replica_up(&lost).map_err(err)?;
        let response = cluster.ledger.report(0, &report)?.ok_or("missing loss ACK")?;
        cluster.children[0].link.as_ref().unwrap().send(replica_up_reply_message(&response, &cluster.children[0].generation()).map_err(err)?).await?;
        let intent = cluster.ledger.start_primary_failover(Instant::now())?.ok_or("missing failover intent")?;
        // Inject a native FM input; RA's actual history selection is tested in its model suite.
        let mut change = intent.configuration;
        let selected = change.replicas.0.iter_mut().find(|r| r.node == cluster.children[2].launch.node).unwrap();
        selected.first_lsn = 1;
        selected.last_lsn = 1;
        let legacy = cluster.ledger.change_configuration(1, change.clone(), Instant::now())?.ok_or("redirection rejected")?;
        cluster.children[1].link.as_ref().unwrap().send(change_configuration_reply_message(&legacy, &cluster.children[1].generation()).map_err(err)?).await?;
        cluster.send_configuration(&cluster.ledger.swap.as_ref().unwrap().body).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true
            && c.status()["nodes"][1]["retained_work"]["deprecated_change_configuration_reply"]["state"] == "rejected").await;
        assert_eq!(cluster.ledger.primary_index(), 2);
        assert!(cluster.ledger.placements[1].link_alive);
        let endpoint = cluster.ledger.placements[2].endpoint.clone().ok_or("missing redirected endpoint")?;
        assert!(data_contact(&endpoint, b"read\n").contains("committed=1 value=acknowledged"));
        assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2 value=continued"));
        until(&mut cluster, |c| [1, 3].iter().all(|i|
            c.status()["nodes"][*i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2)).await;
        for i in 1..4 {
            assert_eq!(cluster.status()["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(cluster.status()["nodes"][i]["retained_work"]["peer_command_error"], Value::Null);
        }
        until(&mut cluster, |c| c.ledger.placements[0].status()["native_replica_deleted"] == true).await;
        cluster.repair_primary_replica().await?;
        until(&mut cluster, |c| c.ledger.repair_status()["ready"] == true
            && c.status()["nodes"][0]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        assert_eq!(cluster.ledger.primary_index(), 2);
        assert_eq!(cluster.ledger.repair.as_ref().unwrap().configuration.as_ref().unwrap().reply.as_ref().unwrap().replicas.0.len(), 4);
        assert!(data_contact(&endpoint, b"write-final\n").contains("committed=3 value=final"));
        until(&mut cluster, |c| [0, 1, 3].iter().all(|i|
            c.status()["nodes"][*i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 3)).await;
        let pid = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| c.ledger.failed_primary_index() == Some(2)
            && c.ledger.swap.as_ref().is_some_and(|s| s.reply.is_some())).await;
        assert!(cluster.ledger.configuration_status()["candidate_redirections"].as_array().unwrap().is_empty());
        assert!(cluster.ledger.change_configuration(1, change, Instant::now())?.is_none());
        let endpoint = cluster.ledger.placements[0].endpoint.clone().ok_or("missing rebuilt Primary endpoint")?;
        assert!(data_contact(&endpoint, b"read\n").contains("committed=3 value=final"));
        Ok::<(), String>(())
    }).await;
    if !matches!(&result, Ok(Ok(()))) {
        eprintln!("REDIRECTION_STATUS {}", cluster.status());
        for child in &cluster.children { eprintln!("REDIRECTION_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn primary_loss_during_secondary_removal_recovers_data_without_replacement_or_quorum_reduction() {
    for (target, minimum, checkpoint) in [(5,3,0),(5,3,1),(5,3,2),(3,2,2)] {
        let fixture=Fixture::new("removal-primary-loss");
        let mut config=fixture.config();
        config.profile=ServiceProfile::Stateful { target,minimum };
        config.app=support::binary("hosting-local-app","FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster=Cluster::new(config).unwrap();
        if let Err(error)=cluster.start().await {
            let cleanup=cluster.shutdown().await;
            panic!("removal supersession startup: {error}; cleanup={cleanup}");
        }
        let mut stopped_node=None;
        let result=peer_idle_catch(async {
            cluster.execute(command("primary","create",Some(1))).await?;
            until(&mut cluster,|c| c.ledger.result("primary")["state"]=="complete").await;
            let endpoint=cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster,|_| data_contact(&endpoint,b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&endpoint,b"write\n").contains("committed=1 value=acknowledged"));
            until(&mut cluster,|c| (1..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"]==1)).await;
            let service=cluster.ledger.placements[0].body()?.service;
            let node_pids=cluster.status()["nodes"].as_array().unwrap().iter().map(|n| n["node_pid"].clone()).collect::<Vec<_>>();
            let profiles=cluster.ledger.placements.iter().map(|p| p.profile.clone().unwrap()).collect::<Vec<_>>();
            let old_secondary=cluster.ledger.placements[1].body()?;
            assert!(std::process::Command::new("kill").args(["-KILL",&profiles[1].app_pid.to_string()]).status().map_err(err)?.success());
            until(&mut cluster,|c| c.ledger.placements[1].loss_reported
                && c.ledger.placements[1].status()["native_replica_deleted"]==true).await;
            let removal=cluster.ledger.begin_secondary_repair(1,Instant::now())?.ok_or("missing removal")?;
            let held=if checkpoint==2 {
                cluster.send_configuration(&removal).await?;
                Some(hold_peer_loss_frame(&mut cluster,0,"DoReconfigurationReply",Duration::from_secs(30)).await?)
            } else {
                if checkpoint==1 {
                    let pid=cluster.children[4].process.as_ref().unwrap().pid();
                    assert!(std::process::Command::new("kill").args(["-STOP",&pid.to_string()]).status().map_err(err)?.success());
                    stopped_node=Some(pid);
                    cluster.send_configuration(&removal).await?;
                    until(&mut cluster,|c| c.status()["nodes"][0]["retained_work"]["local_replica"]["configuration"]["stage"]=="Deactivate"
                        && c.status()["nodes"][2]["retained_work"]["local_replica"]["configuration"]["stage"]=="Deactivated").await;
                }
                None
            };
            assert!(!cluster.ledger.repair.as_ref().unwrap().host_requested);
            assert!(std::process::Command::new("kill").args(["-KILL",&profiles[0].app_pid.to_string()]).status().map_err(err)?.success());
            if let Some(pid)=stopped_node.take() {
                assert!(std::process::Command::new("kill").args(["-CONT",&pid.to_string()]).status().map_err(err)?.success());
            }
            until(&mut cluster,|c| c.ledger.placements[0].loss_reported
                && c.ledger.placements[0].status()["native_replica_deleted"]==true).await;
            if target==3 {
                assert!(cluster.ledger.swap.is_none());
                assert_eq!(cluster.ledger.configuration_status()["state"],"unknown");
                assert!(cluster.ledger.start_primary_failover(Instant::now())?.is_none());
                if let Some(held)=held {
                    cluster.ledger.configuration_reply(0,decode_do_reconfiguration_reply(&held).map_err(err)?.0)?;
                }
                for _ in 0..30 { cluster.drive().await; }
                assert!(cluster.ledger.swap.is_none());
                assert!(!cluster.ledger.repair.as_ref().unwrap().host_requested);
                assert!(cluster.ledger.placements[2].endpoint.is_none());
                assert_eq!(cluster.status()["nodes"][2]["retained_work"]["local_replica"]["replica"]["current_role"],3);
            } else {
                until(&mut cluster,|c| c.ledger.configuration_status()["ready"]==true
                    && c.ledger.failed_primary_index()==Some(0)).await;
                let intent=cluster.ledger.swap.as_ref().unwrap().body.clone();
                assert_eq!(cluster.ledger.primary_index(),2);
                assert_eq!(intent.configuration.unit.previous,removal.configuration.unit.previous);
                assert_eq!(intent.configuration.unit.current.configuration,removal.configuration.unit.current.configuration+0x1_0000_0001);
                assert_eq!(intent.configuration.service,service);
                assert_eq!(intent.configuration.replicas.0.iter().filter(|r| r.previous_role>=3).count(),5);
                assert_eq!(intent.configuration.replicas.0.iter().filter(|r| r.current_role>=3).count(),4);
                assert_eq!(intent.configuration.replicas.0.iter().filter(|r| r.up).count(),3);
                assert!(cluster.ledger.repair.is_none());
                assert!(!cluster.ledger.repair_history.last().unwrap().host_requested);
                let endpoint=cluster.ledger.placements[2].endpoint.clone().ok_or("missing superseding Primary endpoint")?;
                assert!(data_contact(&endpoint,b"read\n").contains("committed=1 value=acknowledged"));
                if let Some(held)=held {
                    cluster.ledger.configuration_reply(0,decode_do_reconfiguration_reply(&held).map_err(err)?.0)?;
                }
                cluster.send_configuration(&removal).await?;
                cluster.send_configuration(&intent).await?;
                for _ in 0..30 { cluster.drive().await; }
                assert_eq!(cluster.ledger.configuration_status()["ready"],true);
                assert!(cluster.ledger.repair.is_none());
                assert!(cluster.repair_primary_replica().await.is_err());
                assert!(cluster.repair_secondary_replica(2).await.is_err());
                assert!(data_contact(&endpoint,b"write-more\n").contains("committed=2 value=continued"));
                until(&mut cluster,|c| (3..target as usize).all(|i|
                    c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"]==2)).await;
            }
            assert!(!cluster.children[1].launch.host_replacement_path(old_secondary.replica.id,old_secondary.replica.instance).exists());
            for (i,p) in profiles.iter().enumerate() {
                assert_eq!(cluster.ledger.placements[i].profile.as_ref(),Some(p));
                assert_eq!(cluster.ledger.placements[i].body()?.service,service);
                assert_eq!(&cluster.status()["nodes"][i]["node_pid"],&node_pids[i]);
                if i>=2 { assert_eq!(cluster.status()["nodes"][i]["retained_work"]["app_observation"]["opens"],1); }
            }
            assert!(cluster.ledger.placements.iter().all(|p| p.link_alive));
            Ok::<(),String>(())
        }).await;
        if let Some(pid)=stopped_node {
            assert!(std::process::Command::new("kill").args(["-CONT",&pid.to_string()]).status().unwrap().success());
        }
        if !matches!(&result,Ok(Ok(()))) {
            eprintln!("REMOVAL_SUPERSESSION_STATUS {}",cluster.status());
            for child in &cluster.children { eprintln!("REMOVAL_SUPERSESSION_LOG {:?}",bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup=cluster.shutdown().await;
        assert_eq!(cleanup["clean"],true,"{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn sequential_repair_after_removal_supersession_restores_target_and_later_failover() {
    for (target,minimum,interrupt_phase3) in [(5,3,false),(5,3,true),(4,3,false),(4,3,true)] {
    let ready_idle_first=target-1==minimum;
    let fixture=Fixture::new("sequential-supersession-repair");
    let mut config=fixture.config();
    config.profile=ServiceProfile::Stateful { target,minimum };
    config.app=support::binary("hosting-local-app","FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster=Cluster::new(config).unwrap();
    if let Err(error)=cluster.start().await {
        let cleanup=cluster.shutdown().await;
        panic!("sequential restoration startup: {error}; cleanup={cleanup}");
    }
    let mut stopped_node=None;
    let result=peer_idle_catch(async {
        cluster.execute(command("primary","create",Some(1))).await?;
        until(&mut cluster,|c| c.ledger.result("primary")["state"]=="complete").await;
        let endpoint=cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster,|_| data_contact(&endpoint,b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint,b"write\n").contains("committed=1"));
        until(&mut cluster,|c| (1..target as usize).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"]==1)).await;
        let service=cluster.ledger.placements[0].body()?.service;
        let profiles=cluster.ledger.placements.iter().map(|p| p.profile.clone().unwrap()).collect::<Vec<_>>();
        let node_pids=cluster.status()["nodes"].as_array().unwrap().iter().map(|n| n["node_pid"].clone()).collect::<Vec<_>>();
        let secondary=cluster.ledger.placements[1].body()?;
        let secondary_report=stateful_loss_report(&cluster.ledger.placements[1]);
        assert!(std::process::Command::new("kill").args(["-KILL",&profiles[1].app_pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster,|c| c.ledger.placements[1].status()["native_replica_deleted"]==true).await;
        if interrupt_phase3 {
            let pid=cluster.children[target as usize-1].process.as_ref().unwrap().pid();
            assert!(std::process::Command::new("kill").args(["-STOP",&pid.to_string()]).status().map_err(err)?.success());
            stopped_node=Some(pid);
        }
        cluster.repair_secondary_replica(2).await?;
        let held=if interrupt_phase3 {
            until(&mut cluster,|c| c.status()["nodes"][0]["retained_work"]["local_replica"]["configuration"]["stage"]=="Deactivate"
                && c.status()["nodes"][2]["retained_work"]["local_replica"]["configuration"]["stage"]=="Deactivated").await;
            None
        } else {Some(hold_peer_loss_frame(&mut cluster,0,"DoReconfigurationReply",Duration::from_secs(30)).await?)};
        assert!(std::process::Command::new("kill").args(["-KILL",&profiles[0].app_pid.to_string()]).status().map_err(err)?.success());
        if interrupt_phase3 {
            until(&mut cluster,|c| c.ledger.placements[0].loss_reported).await;
            let pid=stopped_node.take().unwrap();
            assert!(std::process::Command::new("kill").args(["-CONT",&pid.to_string()]).status().map_err(err)?.success());
        }
        until(&mut cluster,|c| c.ledger.configuration_status()["ready"]==true
            && c.ledger.failed_primary_index()==Some(0)
            && c.ledger.placements[0].status()["native_replica_deleted"]==true).await;
        let supersession=cluster.ledger.swap.as_ref().unwrap().body.clone();
        let endpoint=cluster.ledger.placements[2].endpoint.clone().unwrap();
        assert!(data_contact(&endpoint,b"read\n").contains("committed=1"));
        let destinations=if ready_idle_first {
            assert!(data_contact(&endpoint,b"read\n").contains("committed=1 value=acknowledged"));
            assert!(cluster.repair_secondary_replica(1).await.unwrap_err().contains("minimum"));
            [1,0]
        } else {
            assert!(cluster.repair_secondary_replica(2).await.unwrap_err().contains("current-configuration"));
            [0,1]
        };
        for (step,destination) in destinations.into_iter().enumerate() {
            let old=cluster.ledger.placements[destination].body()?;
            cluster.repair_secondary_replica(destination as u8+1).await?;
            assert!(cluster.repair_secondary_replica(if destination==0 {2} else {1}).await.is_err());
            let late_removal=if !ready_idle_first && destination==0 {
                let lost=hold_peer_loss_frame(&mut cluster,2,"DoReconfigurationReply",Duration::from_secs(30)).await?;
                assert_eq!(cluster.ledger.repair_status()["removed"],false);
                let retry=cluster.ledger.configuration_retry(Instant::now()+Duration::from_secs(11)).ok_or("missing restoration removal retry")?;
                assert_eq!(retry.configuration.replicas.0.iter().filter(|r| r.current_role>=3).count(),3);
                cluster.send_configuration(&retry).await?;
                Some(decode_do_reconfiguration_reply(&lost).map_err(err)?.0)
            } else {None};
            let lost=hold_peer_loss_frame(&mut cluster,2,"AddReplicaReply",Duration::from_secs(30)).await?;
            let (built,_)=decode_add_replica_reply(&lost).map_err(err)?;
            assert!(built.replica.id>old.replica.id && built.replica.instance>old.replica.instance);
            assert_eq!(cluster.ledger.repair_status()["built"],false);
            if ready_idle_first && step==0 {
                assert!(cluster.ledger.repair.as_ref().unwrap().configuration.is_none());
                assert!(cluster.ledger.repair.as_ref().unwrap().removal.is_none());
                assert_eq!(cluster.ledger.swap.as_ref().unwrap().reply.as_ref().unwrap().replicas.0.len(),3);
                assert!(data_contact(&endpoint,b"write-more\n").contains("committed=2 value=continued"));
                assert!(data_contact(&endpoint,b"read\n").contains("committed=2 value=continued"));
            }
            let retry=cluster.ledger.idle_retry(2,Instant::now()+Duration::from_secs(11)).ok_or("missing restoration build retry")?;
            cluster.send_idle(&retry).await?;
            let lost=hold_peer_loss_frame(&mut cluster,2,"DoReconfigurationReply",Duration::from_secs(30)).await?;
            let (admitted,_)=decode_do_reconfiguration_reply(&lost).map_err(err)?;
            assert_eq!(admitted.replicas.0.len(),target as usize-1+step);
            if ready_idle_first && step==0 {
                let request=&cluster.ledger.repair.as_ref().unwrap().configuration.as_ref().unwrap().body.configuration;
                let down=request.replicas.0.iter().find(|r|r.node==cluster.children[0].launch.node).unwrap();
                assert_eq!((down.previous_role,down.current_role,down.up),(3,1,false));
                assert_eq!(request.unit.previous,supersession.configuration.unit.current);
                assert_eq!(request.unit.current.configuration,supersession.configuration.unit.current.configuration+1);
            }
            assert_eq!(cluster.ledger.repair_status()["ready"],false);
            let retry=cluster.ledger.configuration_retry(Instant::now()+Duration::from_secs(11)).ok_or("missing restoration admission retry")?;
            cluster.send_configuration(&retry).await?;
            until(&mut cluster,|c| c.ledger.repair_status()["ready"]==true
                && c.status()["nodes"][destination]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"]==if ready_idle_first {2} else {1}).await;
            assert_eq!(cluster.ledger.repair_status()["target_restored"],step==1);
            assert_eq!(cluster.repair_secondary_replica(destination as u8+1).await?["ready"],true);
            if let Some(reply)=late_removal {
                cluster.ledger.configuration_reply(2,reply)?;
                assert_eq!(cluster.ledger.repair_status()["ready"],true);
            }
            assert_eq!(cluster.ledger.primary_index(),2);
            assert_eq!(cluster.ledger.placements[2].endpoint.as_ref(),Some(&endpoint));
            assert!(!profiles[destination].runtime_root.exists());
            assert!(!Path::new(&format!("/proc/{}",profiles[destination].app_pid)).exists());
            if let Some(held)=&held {
                cluster.ledger.configuration_reply(0,decode_do_reconfiguration_reply(held).map_err(err)?.0)?;
            }
            cluster.send_configuration(&supersession).await?;
            let link=cluster.children[destination].link.as_ref().unwrap();
            link.send(delete_replica_message(&DeleteReplica::new(old,false),&link.generation()).map_err(err)?).await?;
            assert!(cluster.ledger.report(1,&secondary_report)?.is_none());
            for _ in 0..20 { cluster.drive().await; }
            assert_eq!(cluster.ledger.repair_status()["ready"],true);
            if ready_idle_first && step==0 {
                assert!(cluster.ledger.placements[0].loss_reported);
                assert_eq!(cluster.ledger.placements[0].observed,Observed::Lost);
                assert!(data_contact(&endpoint,b"read\n").contains("committed=2 value=continued"));
                until(&mut cluster,|c| [1,3].iter().all(|i|
                    c.status()["nodes"][*i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"]==2)).await;
            }
        }
        if !ready_idle_first {assert!(data_contact(&endpoint,b"write-more\n").contains("committed=2"));}
        until(&mut cluster,|c| (0..target as usize).filter(|i|*i!=2).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"]==2)).await;
        for (i,p) in profiles.iter().enumerate().skip(2) { assert_eq!(cluster.ledger.placements[i].profile.as_ref(),Some(p)); }
        assert!(std::process::Command::new("kill").args(["-KILL",&profiles[2].app_pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster,|c| c.ledger.failed_primary_index()==Some(2)
            && c.ledger.swap.as_ref().is_some_and(|s| s.reply.is_some())
            && c.ledger.placements[2].status()["native_replica_deleted"]==true).await;
        assert_eq!(cluster.ledger.primary_index(),0);
        let endpoint=cluster.ledger.placements[0].endpoint.clone().unwrap();
        assert!(data_contact(&endpoint,b"read\n").contains("committed=2"));
        cluster.repair_primary_replica().await?;
        until(&mut cluster,|c| c.ledger.repair_status()["ready"]==true).await;
        assert!(data_contact(&endpoint,b"write-final\n").contains("committed=3"));
        for (i,pid) in node_pids.iter().enumerate() {
            assert_eq!(&cluster.status()["nodes"][i]["node_pid"],pid);
            assert_eq!(cluster.ledger.placements[i].body()?.service,service);
            assert_eq!(cluster.status()["nodes"][i]["retained_work"]["app_observation"]["opens"],1);
        }
        assert!(cluster.ledger.placements.iter().all(|p| p.link_alive && !p.loss_reported));
        assert_ne!(cluster.ledger.placements[1].replica,secondary.replica.id);
        Ok::<(),String>(())
    }).await;
    if let Some(pid)=stopped_node {
        assert!(std::process::Command::new("kill").args(["-CONT",&pid.to_string()]).status().unwrap().success());
    }
    if !matches!(&result,Ok(Ok(()))) {
        eprintln!("SEQUENTIAL_RESTORATION_STATUS {}",cluster.status());
        for child in &cluster.children { eprintln!("SEQUENTIAL_RESTORATION_LOG {:?}",bounded_logs(child.process.as_ref().unwrap())); }
    }
    let cleanup=cluster.shutdown().await;
    assert_eq!(cleanup["clean"],true,"{cleanup}");
    result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn secondary_loss_repair_preserves_primary_repeats_and_then_survives_primary_failover_2_1() {
    secondary_loss_repair(2, 1).await;
}

#[tokio::test]
async fn secondary_loss_repair_preserves_primary_repeats_and_then_survives_primary_failover_4_2() {
    secondary_loss_repair(4, 2).await;
}

#[tokio::test]
async fn secondary_loss_repair_preserves_primary_repeats_and_then_survives_primary_failover_4_3() {
    secondary_loss_repair(4, 3).await;
}

#[tokio::test]
async fn secondary_loss_repair_preserves_primary_repeats_and_then_survives_primary_failover_5_3() {
    secondary_loss_repair(5, 3).await;
}

async fn secondary_loss_repair(target: i32, minimum: i32) {
        let fixture = Fixture::new("secondary-loss-repair");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful { target, minimum };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("Secondary repair startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            assert!(cluster.repair_secondary_replica(2).await.is_err());
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
            let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
            until(&mut cluster, |c| (1..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
            let service = cluster.ledger.placements[0].body()?.service;
            let primary = cluster.ledger.placements[0].profile.clone().unwrap();
            let node_pids = cluster.status()["nodes"].as_array().unwrap().iter().map(|n| n["node_pid"].clone()).collect::<Vec<_>>();
            let mut predecessors = Vec::new();
            for cycle in 0..2 {
                let old = cluster.ledger.placements[1].body()?;
                let report = stateful_loss_report(&cluster.ledger.placements[1]);
                let profile = cluster.ledger.placements[1].profile.clone().unwrap();
                let peers: Vec<_> = cluster.ledger.placements.iter().enumerate().filter(|(i,_)| *i != 1)
                    .map(|(i,p)| (i,p.profile.clone().unwrap())).collect();
                assert!(std::process::Command::new("kill").args(["-KILL",&profile.app_pid.to_string()]).status().map_err(err)?.success());
                until(&mut cluster, |c| c.ledger.placements[1].loss_reported
                    && c.ledger.placements[1].status()["native_replica_deleted"] == true).await;
                assert!(cluster.repair_secondary_replica(1).await.is_err());
                assert_eq!(cluster.repair_secondary_replica(2).await?["state"], "pending");
                assert_eq!(cluster.repair_secondary_replica(2).await?["state"], "pending");
                let lost = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(30)).await?;
                assert_eq!(cluster.ledger.repair_status()["removed"], false);
                assert!(!cluster.ledger.repair.as_ref().unwrap().host_requested);
                assert_eq!(cluster.ledger.placements[1].profile.as_ref(), Some(&profile));
                let (reply,_) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
                assert_eq!(reply.replicas.0.len(), target as usize - 1);
                let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).ok_or("missing Secondary removal retry")?;
                assert_eq!(retry.configuration.service, service);
                assert_eq!(retry.configuration.unit.current.data_loss, old.unit.current.data_loss);
                assert_eq!(retry.configuration.unit.current.configuration, old.unit.current.configuration + 1);
                cluster.send_configuration(&retry).await?;
                let lost = hold_peer_loss_frame(&mut cluster, 0, "AddReplicaReply", Duration::from_secs(30)).await?;
                assert_eq!(cluster.ledger.repair_status()["removed"], true);
                assert_eq!(cluster.ledger.repair_status()["built"], false);
                let (built,_) = decode_add_replica_reply(&lost).map_err(err)?;
                assert!(built.replica.id > old.replica.id && built.replica.instance > old.replica.instance);
                let retry = cluster.ledger.idle_retry(0, Instant::now() + Duration::from_secs(11)).ok_or("missing Secondary build retry")?;
                cluster.send_idle(&retry).await?;
                let lost = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(30)).await?;
                assert_eq!(cluster.ledger.repair_status()["ready"], false);
                let (reply,_) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
                assert_eq!(reply.replicas.0.len(), target as usize);
                let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).ok_or("missing Secondary admission retry")?;
                cluster.send_configuration(&retry).await?;
                until(&mut cluster, |c| c.ledger.repair_status()["ready"] == true
                    && c.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == if cycle == 0 { 1 } else { 2 }).await;
                assert_eq!(cluster.ledger.primary_index(), 0);
                assert_eq!(cluster.ledger.placements[0].endpoint.as_ref(), Some(&endpoint));
                assert_eq!(cluster.ledger.placements[0].profile.as_ref(), Some(&primary));
                assert_eq!(cluster.repair_secondary_replica(2).await?["state"], "ready");
                for (i,p) in peers { assert_eq!(cluster.ledger.placements[i].profile.as_ref(), Some(&p)); }
                assert!(!Path::new(&format!("/proc/{}",profile.app_pid)).exists());
                assert!(!profile.runtime_root.exists());
                predecessors.push((old, report));
                for (old, report) in &predecessors {
                    assert!(cluster.ledger.report(1, report)?.is_none());
                    let link = cluster.children[1].link.as_ref().unwrap();
                    link.send(delete_replica_message(&DeleteReplica::new(old.clone(), false), &link.generation()).map_err(err)?).await?;
                }
                for _ in 0..20 { cluster.drive().await; }
                assert!(cluster.ledger.placements.iter().all(|p| p.link_alive && !p.loss_reported));
                for (i,pid) in node_pids.iter().enumerate() {
                    assert_eq!(cluster.ledger.placements[i].body()?.service, service);
                    assert_eq!(&cluster.status()["nodes"][i]["node_pid"], pid);
                    assert_eq!(cluster.status()["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
                }
                if cycle == 0 {
                    assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2 value=continued"));
                    until(&mut cluster, |c| (1..target as usize).all(|i|
                        c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2)).await;
                }
            }
            assert!(std::process::Command::new("kill").args(["-KILL",&primary.app_pid.to_string()]).status().map_err(err)?.success());
            until(&mut cluster, |c| c.ledger.failed_primary_index() == Some(0)
                && c.ledger.swap.as_ref().is_some_and(|s| s.reply.is_some())
                && c.ledger.placements[0].status()["native_replica_deleted"] == true).await;
            assert_eq!(cluster.ledger.primary_index(), 1);
            let endpoint = cluster.ledger.placements[1].endpoint.clone().ok_or("missing rebuilt Primary endpoint")?;
            assert!(data_contact(&endpoint, b"read\n").contains("committed=2 value=continued"));
            cluster.repair_primary_replica().await?;
            until(&mut cluster, |c| c.ledger.repair_status()["ready"] == true).await;
            assert!(data_contact(&endpoint, b"write-final\n").contains("committed=3 value=final"));
            Ok::<(),String>(())
        }).await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("SECONDARY_REPAIR_STATUS {}", cluster.status());
            for child in &cluster.children { eprintln!("SECONDARY_REPAIR_LOG {:?}",bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"],true,"{cleanup}");
        result.unwrap().unwrap();
}

#[tokio::test]
async fn secondary_repair_blocks_at_minimum_without_quorum_and_after_another_loss() {
    for (target, minimum, losses, interrupted) in [(4,4,1,false),(4,2,2,false),(5,3,3,false),(4,2,1,true)] {
        let fixture = Fixture::new("secondary-repair-blocked");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful { target, minimum };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("blocked Secondary repair startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
            let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
            until(&mut cluster, |c| (1..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
            let old = cluster.ledger.placements[1].body()?;
            let primary = cluster.ledger.placements[0].profile.clone().unwrap();
            for i in 1..=losses {
                let pid = cluster.ledger.placements[i].profile.as_ref().unwrap().app_pid;
                assert!(std::process::Command::new("kill").args(["-KILL",&pid.to_string()]).status().map_err(err)?.success());
            }
            until(&mut cluster, |c| (1..=losses).all(|i| c.ledger.placements[i].loss_reported
                && c.ledger.placements[i].status()["native_replica_deleted"] == true)).await;
            if interrupted {
                cluster.repair_secondary_replica(2).await?;
                let held = hold_peer_loss_frame(&mut cluster,0,"DoReconfigurationReply",Duration::from_secs(30)).await?;
                let pid = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
                assert!(std::process::Command::new("kill").args(["-KILL",&pid.to_string()]).status().map_err(err)?.success());
                until(&mut cluster, |c| c.ledger.placements[2].loss_reported).await;
                cluster.ledger.configuration_reply(0,decode_do_reconfiguration_reply(&held).map_err(err)?.0)?;
                assert_eq!(cluster.ledger.repair_status()["state"],"unknown");
                assert!(!cluster.ledger.repair.as_ref().unwrap().host_requested);
                assert!(cluster.ledger.repair.as_ref().unwrap().profile.is_none());
            } else {
                let error = cluster.repair_secondary_replica(2).await.unwrap_err();
                if target == minimum { assert!(error.contains("minimum"),"{error}"); }
                assert!(cluster.ledger.repair.is_none());
            }
            assert!(!cluster.children[1].launch.host_replacement_path(old.replica.id,old.replica.instance).exists());
            assert_eq!(cluster.ledger.placements[0].profile.as_ref(),Some(&primary));
            assert_eq!(cluster.ledger.placements[0].endpoint.as_ref(),Some(&endpoint));
            assert!(cluster.ledger.swap.is_none());
            assert!(cluster.ledger.placements.iter().all(|p| p.link_alive));
            assert_eq!(cluster.status()["nodes"][0]["retained_work"]["app_observation"]["opens"],1);
            assert!(data_contact(&endpoint,b"read\n").contains("committed=1"));
            if target == minimum || interrupted {
                assert!(data_contact(&endpoint,b"write-more\n").contains("committed=2"));
            } else {
                let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
                pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
                pending.write_all(b"write-more\n").map_err(err)?;
                until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_admitted"] == 2).await;
                let mut bytes = [0;1024];
                assert!(matches!(pending.read(&mut bytes),Err(ref e) if matches!(e.kind(),std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
                assert_eq!(cluster.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_acknowledged"],1);
            }
            Ok::<(),String>(())
        }).await;
        if !matches!(&result,Ok(Ok(()))) {
            eprintln!("SECONDARY_BLOCKED_STATUS {}",cluster.status());
            for child in &cluster.children { eprintln!("SECONDARY_BLOCKED_LOG {:?}",bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"],true,"{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn configured_primary_repair_repeats_fresh_owner_cycles_without_resizing() {
    for (target, minimum, interrupted) in [(2, 1, false), (4, 2, false), (4, 4, false), (5, 3, false), (4, 3, true)] {
        let fixture = Fixture::new("configured-repair");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful { target, minimum };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("configured repair startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            assert!(cluster.repair_primary_replica().await.is_err());
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
            let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
            until(&mut cluster, |c| (1..target as usize).all(|i|
                c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
            let service = cluster.ledger.placements[0].body()?.service;
            let node_pids = cluster.status()["nodes"].as_array().unwrap().iter()
                .map(|n| n["node_pid"].clone()).collect::<Vec<_>>();
            let mut predecessors = Vec::new();
            let mut old_requests: Vec<(usize, DoReconfiguration)> = Vec::new();
            let mut epoch = cluster.ledger.placements[0].body()?.unit.current;
            for cycle in 0..3 {
                let lost = cluster.ledger.primary_index();
                let original = cluster.ledger.placements[lost].body()?;
                let loss = stateful_loss_report(&cluster.ledger.placements[lost]);
                let profile = cluster.ledger.placements[lost].profile.clone().unwrap();
                let survivors = cluster.ledger.placements.iter().enumerate()
                    .filter(|(i, _)| *i != lost)
                    .map(|(i, p)| (i, p.profile.clone().unwrap())).collect::<Vec<_>>();
                assert!(std::process::Command::new("kill").args(["-KILL", &profile.app_pid.to_string()])
                    .status().map_err(err)?.success());
                if cycle == 2 {
                    let end = Instant::now() + Duration::from_secs(8);
                    while cluster.status()["nodes"][lost]["retained_work"]["local_replica"]["phase"] != "Closed" {
                        assert!(Instant::now() < end, "late reconfiguration loss deadline");
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    for (index, body) in old_requests.iter().filter(|(index, _)| *index == lost) {
                        let link = cluster.children[*index].link.as_ref().unwrap();
                        link.send(do_reconfiguration_message(body, &link.generation()).map_err(err)?).await?;
                    }
                }
                until(&mut cluster, |c| c.ledger.failed_primary_index() == Some(lost)
                    && c.ledger.swap.as_ref().is_some_and(|s| s.reply.is_some())
                    && c.ledger.placements[lost].status()["native_replica_deleted"] == true
                    && c.ledger.repair.is_none()).await;
                let primary = cluster.ledger.primary_index();
                let endpoint = cluster.ledger.placements[primary].endpoint.clone().ok_or("missing repaired-cycle endpoint")?;
                assert!(data_contact(&endpoint, b"read\n").contains(&format!("committed={}", if cycle == 0 { 1 } else { 2 })));
                if target == minimum {
                    assert!(!data_contact(&endpoint, if cycle == 0 { b"write-more\n" } else { b"write-final\n" }).starts_with("OK "));
                }
                let failover = cluster.ledger.swap.as_ref().unwrap().body.clone();
                assert_eq!(failover.configuration.service, service);
                assert_eq!(failover.configuration.unit.previous, epoch);
                assert_eq!(failover.configuration.unit.current.data_loss, epoch.data_loss);
                old_requests.push((primary, failover));
                assert_eq!(cluster.repair_primary_replica().await?["state"], "pending");
                assert_eq!(cluster.repair_primary_replica().await?["state"], "pending");
                if interrupted && cycle == 1 {
                    let path = cluster.children[lost].launch.host_replacement_path(original.replica.id, original.replica.instance);
                    let end = Instant::now() + Duration::from_secs(10);
                    while !path.exists() {
                        assert!(Instant::now() < end, "configured replacement startup deadline");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    assert_eq!(cluster.ledger.repair_status()["host_ready"], false);
                    return Ok(());
                }
                if cycle == 1 {
                    let lost_reply = hold_peer_loss_frame(&mut cluster, primary, "AddReplicaReply", Duration::from_secs(30)).await?;
                    let (built, _) = decode_add_replica_reply(&lost_reply).map_err(err)?;
                    assert_eq!(cluster.ledger.repair_status()["built"], false);
                    let retry = cluster.ledger.idle_retry(primary, Instant::now() + Duration::from_secs(11))
                        .ok_or("missing configured repair build retry")?;
                    cluster.send_idle(&retry).await?;
                    let lost_reply = hold_peer_loss_frame(&mut cluster, primary, "DoReconfigurationReply", Duration::from_secs(30)).await?;
                    let (completed, _) = decode_do_reconfiguration_reply(&lost_reply).map_err(err)?;
                    assert_eq!(cluster.ledger.repair.as_ref().unwrap().build.reply.as_ref(), Some(&built));
                    assert_eq!(cluster.ledger.repair_status()["ready"], false);
                    assert_eq!(completed.replicas.0.len(), target as usize);
                    let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11))
                        .ok_or("missing configured repair configuration retry")?;
                    assert_eq!(retry.configuration.service, service);
                    assert_eq!(retry.configuration.replicas.0.iter().filter(|r| r.previous_role >= 3).count(), target as usize - 1);
                    cluster.send_configuration(&retry).await?;
                }
                until(&mut cluster, |c| c.ledger.repair_status()["ready"] == true
                    && c.status()["nodes"][lost]["retained_work"]["local_replica"]["replica"]["current_role"] == 3
                    && c.status()["nodes"][lost]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == if cycle == 0 { 1 } else { 2 }).await;
                let repair = cluster.ledger.repair.as_ref().unwrap();
                assert_eq!(repair.build.body.service, service);
                let repaired = repair.configuration.as_ref().unwrap().reply.as_ref().unwrap();
                assert_eq!(repaired.replicas.0.len(), target as usize);
                assert_eq!(repaired.unit.previous, Epoch::default());
                assert!(epoch.older_than(repaired.unit.current));
                epoch = repaired.unit.current;
                let fresh = cluster.ledger.placements[lost].profile.as_ref().unwrap();
                assert_ne!(fresh.app_pid, profile.app_pid);
                assert_ne!(fresh.runtime_root, profile.runtime_root);
                assert!(!profile.runtime_root.exists());
                assert!(!Path::new(&format!("/proc/{}", profile.app_pid)).exists());
                assert!(cluster.ledger.placements[lost].replica > original.replica.id);
                assert!(cluster.ledger.placements[lost].incarnation > original.replica.instance);
                for (i, owner) in survivors {
                    assert_eq!(cluster.ledger.placements[i].profile.as_ref(), Some(&owner));
                }
                predecessors.push((lost, original, loss));
                for (i, body, report) in &predecessors {
                    assert!(cluster.ledger.report(*i, report)?.is_none());
                    cluster.ledger.reply(*i, AddInstanceReply {
                        unit: body.unit.clone(), replica: dropped_replica(&body.replica), error: 0,
                    }, true)?;
                    let link = cluster.children[*i].link.as_ref().unwrap();
                    link.send(delete_replica_message(&DeleteReplica::new(body.clone(), false), &link.generation()).map_err(err)?).await?;
                    let child = &mut cluster.children[*i];
                    child.process.as_mut().unwrap().command(format!(
                        "replace-host {} {} {} {}\n", child.launch.session, child.launch.capability,
                        body.replica.id, body.replica.instance,
                    ).as_bytes()).map_err(err)?;
                }
                for (i, body) in &old_requests {
                    let link = cluster.children[*i].link.as_ref().unwrap();
                    link.send(do_reconfiguration_message(body, &link.generation()).map_err(err)?).await?;
                }
                for _ in 0..20 { cluster.drive().await; }
                assert_eq!(cluster.repair_primary_replica().await?["state"], "ready");
                assert!(cluster.ledger.placements.iter().all(|p| p.link_alive && !p.loss_reported));
                let status = cluster.status();
                for i in 0..target as usize {
                    assert_eq!(status["nodes"][i]["node_pid"], node_pids[i]);
                    assert_eq!(status["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
                    assert_eq!(cluster.ledger.placements[i].body()?.service, service);
                    assert_eq!(status["nodes"][i]["retained_work"]["peer_error"], Value::Null);
                    assert_eq!(status["nodes"][i]["retained_work"]["peer_command_error"], Value::Null);
                }
                if cycle == 0 {
                    assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2 value=continued"));
                    until(&mut cluster, |c| (0..target as usize).filter(|i| *i != primary).all(|i|
                        c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2)).await;
                }
            }
            assert_eq!(cluster.ledger.repair_history.len(), 2);
            assert_eq!(cluster.ledger.placements[0].retired.len(), 2);
            let primary = cluster.ledger.primary_index();
            let endpoint = cluster.ledger.placements[primary].endpoint.clone().unwrap();
            if target == 2 || minimum == target {
                assert!(data_contact(&endpoint, b"write-final\n").contains("committed=3 value=final"));
            } else {
                let acknowledged = cluster.status()["nodes"][primary]["retained_work"]["app_observation"]["primary_last_acknowledged"].clone();
                let losses: Vec<_> = (0..target as usize).filter(|i| *i != primary)
                    .take((target as usize).div_ceil(2)).collect();
                for &i in &losses {
                    let pid = cluster.ledger.placements[i].profile.as_ref().unwrap().app_pid;
                    assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()])
                        .status().map_err(err)?.success());
                }
                until(&mut cluster, |c| losses.iter().all(|i| c.ledger.placements[*i].loss_reported)).await;
                let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
                pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
                pending.write_all(b"write-final\n").map_err(err)?;
                until(&mut cluster, |c| c.status()["nodes"][primary]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
                let mut bytes = [0; 1024];
                assert!(matches!(pending.read(&mut bytes), Err(ref e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
                assert_eq!(cluster.status()["nodes"][primary]["retained_work"]["app_observation"]["primary_last_acknowledged"], acknowledged);
            }
            Ok::<(), String>(())
        }).await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("CONFIGURED_REPAIR_STATUS {}", cluster.status());
            for child in &cluster.children { eprintln!("CONFIGURED_REPAIR_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn three_replica_primary_repair_recopies_fresh_owner_and_restores_majority() {
    for mode in 0..3 {
    let fixture = Fixture::new("primary-repair");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulThree;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    if let Err(error) = cluster.start().await {
        let cleanup = cluster.shutdown().await;
        panic!("repair startup: {error}; cleanup={cleanup}");
    }
    let result = peer_idle_catch(async {
        assert!(cluster.repair_primary_replica().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let old_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&old_endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&old_endpoint, b"write\n").contains("committed=1"));
        cluster.add_idle_replica_on(2).await?;
        cluster.add_idle_replica_on(3).await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        cluster.activate_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let old = cluster.ledger.placements[0].profile.clone().unwrap();
        let original = cluster.ledger.placements[0].body()?;
        let old_report = stateful_loss_report(&cluster.ledger.placements[0]);
        let owners = cluster.status()["nodes"].as_array().unwrap().iter()
            .map(|n| (n["node_pid"].clone(), n["app_pid"].clone(), n["runtime_root"].clone())).collect::<Vec<_>>();
        assert!(cluster.repair_primary_replica().await.is_err());
        assert!(std::process::Command::new("kill").args(["-KILL", &old.app_pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| c.ledger.swap.as_ref().is_some_and(|s| s.reply.is_some())
            && c.ledger.placements[0].status()["native_replica_deleted"] == true).await;
        let endpoint = cluster.ledger.placements[1].endpoint.clone().ok_or("missing promoted endpoint")?;
        assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2"));
        assert_eq!(cluster.repair_primary_replica().await?["state"], "pending");
        if mode == 2 {
            let path = cluster.children[0].launch.host_replacement_path(original.replica.id, original.replica.instance);
            let end = Instant::now() + Duration::from_secs(10);
            while !path.exists() {
                assert!(Instant::now() < end, "replacement host startup deadline");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(cluster.ledger.repair_status()["host_ready"], false);
            return Ok(());
        }
        let lost_build = hold_peer_loss_frame(&mut cluster, 1, "AddReplicaReply", Duration::from_secs(30)).await?;
        let (built_reply, _) = decode_add_replica_reply(&lost_build).map_err(err)?;
        assert_eq!(cluster.ledger.repair_status()["built"], false);
        let retry = cluster.ledger.idle_retry(1, Instant::now() + Duration::from_secs(11))
            .ok_or("missing original repair build retry")?;
        assert_eq!(retry, cluster.ledger.repair.as_ref().unwrap().build.body);
        cluster.send_idle(&retry).await?;
        let lost = hold_peer_loss_frame(&mut cluster, 1, "DoReconfigurationReply", Duration::from_secs(30)).await?;
        let (reply, _) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
        assert_eq!(cluster.ledger.repair_status()["built"], true);
        assert_eq!(cluster.ledger.repair.as_ref().unwrap().build.reply.as_ref(), Some(&built_reply));
        assert_eq!(cluster.ledger.repair_status()["ready"], false);
        let request = cluster.ledger.repair.as_ref().unwrap().configuration.as_ref().unwrap().body.clone();
        assert_eq!(request.configuration.service.target, 3);
        assert_eq!(request.configuration.service.minimum, 2);
        assert_eq!(request.configuration.replicas.0.iter().filter(|r| r.previous_role >= 3).count(), 2);
        assert_eq!(request.configuration.replicas.0.iter().filter(|r| r.current_role >= 3).count(), 3);
        assert_eq!(request.configuration.unit.previous.data_loss, request.configuration.unit.current.data_loss);
        let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).ok_or("missing repair retry")?;
        assert_eq!(retry, request);
        cluster.send_configuration(&retry).await?;
        until(&mut cluster, |c| c.ledger.repair_status()["ready"] == true
            && c.status()["nodes"][0]["retained_work"]["local_replica"]["replica"]["current_role"] == 3
            && c.status()["nodes"][0]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        assert_eq!(cluster.ledger.repair.as_ref().unwrap().configuration.as_ref().unwrap().reply.as_ref(), Some(&reply));
        let fresh = cluster.ledger.placements[0].profile.clone().unwrap();
        assert_ne!(fresh.app_pid, old.app_pid);
        assert_ne!(fresh.runtime_root, old.runtime_root);
        assert!(!old.runtime_root.exists());
        assert!(!Path::new(&format!("/proc/{}", old.app_pid)).exists());
        assert!(cluster.ledger.placements[0].replica > original.replica.id);
        assert!(cluster.ledger.placements[0].incarnation > original.replica.instance);
        let status = cluster.status();
        assert_eq!(owners[0].0, status["nodes"][0]["node_pid"]);
        for i in 1..3 {
            assert_eq!(owners[i], (status["nodes"][i]["node_pid"].clone(), status["nodes"][i]["app_pid"].clone(), status["nodes"][i]["runtime_root"].clone()));
            assert_eq!(status["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
        }
        assert_eq!(status["nodes"][0]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(status["nodes"][0]["retained_work"]["local_replica"]["replica"]["current_role"], 3);
        assert!(cluster.ledger.report(0, &old_report)?.is_none());
        cluster.ledger.reply(0, AddInstanceReply {
            unit: original.unit.clone(), replica: dropped_replica(&original.replica), error: 0,
        }, true)?;
        let link = cluster.children[0].link.as_ref().unwrap();
        link.send(delete_replica_message(&DeleteReplica::new(original.clone(), false), &link.generation()).map_err(err)?).await?;
        assert_eq!(cluster.repair_primary_replica().await?["state"], "ready");
        let pid = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| c.ledger.placements[2].loss_reported).await;
        if mode == 1 {
            assert!(std::process::Command::new("kill").args(["-KILL", &fresh.app_pid.to_string()]).status().map_err(err)?.success());
            until(&mut cluster, |c| c.ledger.placements[0].loss_reported).await;
            let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
            pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
            pending.write_all(b"write-final\n").map_err(err)?;
            until(&mut cluster, |c| c.status()["nodes"][1]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
            let mut bytes = [0; 1024];
            assert!(matches!(pending.read(&mut bytes), Err(ref e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
            assert_eq!(cluster.status()["nodes"][1]["retained_work"]["app_observation"]["primary_last_acknowledged"], 2);
        } else {
            let written = data_contact(&endpoint, b"write-final\n");
            assert!(written.contains("committed=3 value=final"), "{written}");
            until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 3).await;
        }
        assert!(TcpStream::connect_timeout(&old_endpoint.parse().map_err(err)?, Duration::from_millis(250)).is_err());
        Ok::<(), String>(())
    }).await;
    if !matches!(&result, Ok(Ok(()))) {
        eprintln!("REPAIR_STATUS {}", cluster.status());
        for child in &cluster.children { eprintln!("REPAIR_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn repeated_primary_failover_and_repair_preserve_current_owners_and_data() {
    for interrupted in [false, true] {
        let fixture = Fixture::new("repeated-primary-repair");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulThree;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("repeat startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
            let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
            for destination in [2, 3] { cluster.add_idle_replica_on(destination).await?; }
            until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
            cluster.activate_idle_replica().await?;
            until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
            let node_pids = cluster.status()["nodes"].as_array().unwrap().iter()
                .map(|n| n["node_pid"].clone()).collect::<Vec<_>>();
            let mut predecessors = Vec::new();
            let mut old_requests = Vec::new();
            let mut epoch = cluster.ledger.placements[0].body()?.unit.current;
            for cycle in 0..3 {
                let lost = cluster.ledger.primary_index();
                let original = cluster.ledger.placements[lost].body()?;
                let loss = stateful_loss_report(&cluster.ledger.placements[lost]);
                let profile = cluster.ledger.placements[lost].profile.clone().unwrap();
                let survivors = cluster.ledger.placements.iter().enumerate()
                    .filter(|(i, _)| *i != lost)
                    .map(|(i, p)| (i, p.profile.clone().unwrap())).collect::<Vec<_>>();
                assert!(std::process::Command::new("kill").args(["-KILL", &profile.app_pid.to_string()])
                    .status().map_err(err)?.success());
                until(&mut cluster, |c| c.ledger.failed_primary_index() == Some(lost)
                    && c.ledger.swap.as_ref().is_some_and(|s| s.reply.is_some())
                    && c.ledger.placements[lost].status()["native_replica_deleted"] == true
                    && c.ledger.repair.is_none()).await;
                let primary = cluster.ledger.primary_index();
                assert_ne!(primary, lost);
                let endpoint = cluster.ledger.placements[primary].endpoint.clone().ok_or("missing current Primary endpoint")?;
                assert!(data_contact(&endpoint, b"read\n").contains(&format!("committed={}", if cycle == 0 { 1 } else { 3.min(cycle + 1) })));
                if cycle < 2 {
                    let written = data_contact(&endpoint, if cycle == 0 { b"write-more\n" } else { b"write-final\n" });
                    assert!(written.contains(&format!("committed={}", cycle + 2)), "{written}");
                }
                let failover = cluster.ledger.swap.as_ref().unwrap().body.clone();
                assert_eq!(failover.configuration.unit.previous, epoch);
                assert_eq!(failover.configuration.unit.current.data_loss, epoch.data_loss);
                assert!(epoch.older_than(failover.configuration.unit.current));
                old_requests.push((primary, failover));
                assert_eq!(cluster.repair_primary_replica().await?["state"], "pending");
                if interrupted && cycle == 1 {
                    let path = cluster.children[lost].launch.host_replacement_path(original.replica.id, original.replica.instance);
                    let end = Instant::now() + Duration::from_secs(10);
                    while !path.exists() {
                        assert!(Instant::now() < end, "second replacement startup deadline");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    assert_eq!(cluster.ledger.repair_status()["host_ready"], false);
                    return Ok(());
                }
                if cycle == 1 {
                    let lost_reply = hold_peer_loss_frame(&mut cluster, primary, "AddReplicaReply", Duration::from_secs(30)).await?;
                    let (built, _) = decode_add_replica_reply(&lost_reply).map_err(err)?;
                    assert_eq!(cluster.ledger.repair_status()["built"], false);
                    assert!(cluster.ledger.idle_retry(lost, Instant::now() + Duration::from_secs(11)).is_none());
                    let retry = cluster.ledger.idle_retry(primary, Instant::now() + Duration::from_secs(11))
                        .ok_or("missing repeated build retry")?;
                    cluster.send_idle(&retry).await?;
                    let lost_reply = hold_peer_loss_frame(&mut cluster, primary, "DoReconfigurationReply", Duration::from_secs(30)).await?;
                    let (completed, _) = decode_do_reconfiguration_reply(&lost_reply).map_err(err)?;
                    assert_eq!(cluster.ledger.repair.as_ref().unwrap().build.reply.as_ref(), Some(&built));
                    assert_eq!(cluster.ledger.repair_status()["ready"], false);
                    assert_eq!(completed.replicas.0.len(), 3);
                    let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11))
                        .ok_or("missing repeated configuration retry")?;
                    cluster.send_configuration(&retry).await?;
                }
                until(&mut cluster, |c| c.ledger.repair_status()["ready"] == true
                    && c.status()["nodes"][lost]["retained_work"]["local_replica"]["replica"]["current_role"] == 3
                    && c.status()["nodes"][lost]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 3.min(cycle + 2)).await;
                epoch = cluster.ledger.placements[primary].body()?.unit.current;
                let fresh = cluster.ledger.placements[lost].profile.as_ref().unwrap();
                assert_ne!(fresh.app_pid, profile.app_pid);
                assert_ne!(fresh.runtime_root, profile.runtime_root);
                assert!(!profile.runtime_root.exists());
                assert!(!Path::new(&format!("/proc/{}", profile.app_pid)).exists());
                assert!(cluster.ledger.placements[lost].replica > original.replica.id);
                assert!(cluster.ledger.placements[lost].incarnation > original.replica.instance);
                for (i, owner) in survivors {
                    assert_eq!(cluster.ledger.placements[i].profile.as_ref(), Some(&owner));
                }
                predecessors.push((lost, original, loss));
                for (i, body, report) in &predecessors {
                    assert!(cluster.ledger.report(*i, report)?.is_none());
                    cluster.ledger.reply(*i, AddInstanceReply {
                        unit: body.unit.clone(), replica: dropped_replica(&body.replica), error: 0,
                    }, true)?;
                    let link = cluster.children[*i].link.as_ref().unwrap();
                    link.send(delete_replica_message(&DeleteReplica::new(body.clone(), false), &link.generation()).map_err(err)?).await?;
                    let child = &mut cluster.children[*i];
                    child.process.as_mut().unwrap().command(format!(
                        "replace-host {} {} {} {}\n", child.launch.session, child.launch.capability,
                        body.replica.id, body.replica.instance,
                    ).as_bytes()).map_err(err)?;
                }
                for (i, body) in &old_requests {
                    let link = cluster.children[*i].link.as_ref().unwrap();
                    link.send(do_reconfiguration_message(body, &link.generation()).map_err(err)?).await?;
                }
                for _ in 0..20 { cluster.drive().await; }
                assert!(cluster.ledger.placements.iter().all(|p| p.link_alive && !p.loss_reported));
                assert_eq!(cluster.ledger.repair_status()["ready"], true);
                let status = cluster.status();
                for i in 0..3 {
                    assert_eq!(status["nodes"][i]["node_pid"], node_pids[i]);
                    assert_eq!(status["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
                }
                assert!(data_contact(&endpoint, b"read\n").contains(&format!("committed={}", 3.min(cycle + 2))));
            }
            assert_eq!(cluster.ledger.repair_history.len(), 2);
            assert_eq!(cluster.ledger.placements[0].retired.len(), 2);
            let primary = cluster.ledger.primary_index();
            let survivor = (0..3).find(|i| *i != primary).unwrap();
            let last = (0..3).find(|i| *i != primary && *i != survivor).unwrap();
            let completed_epoch = cluster.ledger.swap.as_ref().unwrap().body.configuration.unit.current;
            for i in [survivor, primary] {
                let pid = cluster.ledger.placements[i].profile.as_ref().unwrap().app_pid;
                assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()])
                    .status().map_err(err)?.success());
                until(&mut cluster, |c| c.ledger.placements[i].loss_reported).await;
            }
            for _ in 0..20 { cluster.drive().await; }
            assert_eq!(cluster.ledger.swap.as_ref().unwrap().body.configuration.unit.current, completed_epoch);
            assert_eq!(cluster.ledger.repair_history.len(), 2);
            assert_eq!(cluster.status()["nodes"][last]["retained_work"]["local_replica"]["replica"]["current_role"], 3);
            assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
            Ok::<(), String>(())
        }).await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("REPEAT_STATUS {}", cluster.status());
            for child in &cluster.children { eprintln!("REPEAT_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn three_replica_primary_failover_preserves_data_and_never_promotes_without_survivors() {
    for missing_survivor in [false, true] {
        let fixture = Fixture::new("primary-failover");
        let mut config = fixture.config();
        config.profile = ServiceProfile::StatefulThree;
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        if let Err(error) = cluster.start().await {
            let cleanup = cluster.shutdown().await;
            panic!("failover startup: {error}; cleanup={cleanup}");
        }
        let result = peer_idle_catch(async {
            cluster.execute(command("primary", "create", Some(1))).await?;
            until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
            let old_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
            until(&mut cluster, |_| data_contact(&old_endpoint, b"read\n").starts_with("OK ")).await;
            assert!(data_contact(&old_endpoint, b"write\n").contains("committed=1"));
            cluster.add_idle_replica_on(2).await?;
            cluster.add_idle_replica_on(3).await?;
            until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
            cluster.activate_idle_replica().await?;
            until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
            let owners = cluster.status()["nodes"].as_array().unwrap().iter()
                .map(|n| (n["node_pid"].clone(), n["app_pid"].clone(), n["runtime_root"].clone())).collect::<Vec<_>>();
            if missing_survivor {
                let pid = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
                assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
                until(&mut cluster, |c| c.ledger.placements[2].loss_reported).await;
            }
            let pid = cluster.ledger.placements[0].profile.as_ref().unwrap().app_pid;
            assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
            if missing_survivor {
                until(&mut cluster, |c| c.ledger.placements[0].loss_reported).await;
                for _ in 0..20 { cluster.drive().await; }
                assert!(cluster.ledger.swap.is_none());
                assert_eq!(cluster.ledger.configuration_status()["state"], "unknown");
                assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
                assert_eq!(cluster.status()["nodes"][1]["retained_work"]["local_replica"]["replica"]["current_role"], 3);
                return Ok(());
            }
            let lost = hold_peer_loss_frame(&mut cluster, 1, "DoReconfigurationReply", Duration::from_secs(20)).await?;
            let (reply, owner) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
            assert_eq!(owner, Owner::Fm);
            assert!(cluster.ledger.placements[0].loss_reported);
            assert_eq!(cluster.ledger.configuration_status()["kind"], "primary_failover");
            assert_eq!(cluster.ledger.configuration_status()["ready"], false);
            assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
            let request = cluster.ledger.swap.as_ref().unwrap().body.clone();
            assert_eq!(request.configuration.unit.previous.data_loss, request.configuration.unit.current.data_loss);
            assert_eq!(request.configuration.replicas.0.iter().filter(|r| r.previous_role >= 3).count(), 3);
            assert_eq!(request.configuration.replicas.0.iter().filter(|r| r.current_role >= 3).count(), 2);
            assert!(request.configuration.replicas.0.iter().any(|r| r.previous_role == 4 && r.current_role == 1 && !r.up && r.state == 3));
            let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11))
                .ok_or("missing native failover retry")?;
            assert_eq!(retry, request);
            cluster.send_configuration(&retry).await?;
            until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
            assert_eq!(cluster.ledger.swap.as_ref().unwrap().reply.as_ref(), Some(&reply));
            let endpoint = cluster.ledger.placements[1].endpoint.clone().ok_or("missing promoted endpoint")?;
            assert_ne!(endpoint, old_endpoint);
            assert!(TcpStream::connect_timeout(&old_endpoint.parse().map_err(err)?, Duration::from_millis(250)).is_err());
            assert!(data_contact(&endpoint, b"read\n").contains("committed=1 value=acknowledged"));
            let written = data_contact(&endpoint, b"write-more\n");
            assert!(written.contains("committed=2 value=continued"), "{written}");
            until(&mut cluster, |c| c.status()["nodes"][2]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
            let status = cluster.status();
            for i in 1..3 {
                assert_eq!(owners[i], (status["nodes"][i]["node_pid"].clone(), status["nodes"][i]["app_pid"].clone(), status["nodes"][i]["runtime_root"].clone()));
                assert_eq!(status["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
                assert_eq!(status["nodes"][i]["retained_work"]["peer_command_error"], Value::Null);
                assert_eq!(status["nodes"][i]["retained_work"]["peer_error"], Value::Null);
            }
            assert_eq!(status["nodes"][2]["retained_work"]["app_observation"]["idle_data"]["provider"]["data_loss_calls"], 0);
            let pid = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
            assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
            until(&mut cluster, |c| c.ledger.configuration_status()["state"] == "unknown").await;
            let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
            pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
            pending.write_all(b"write-final\n").map_err(err)?;
            until(&mut cluster, |c| c.status()["nodes"][1]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
            let mut bytes = [0; 1024];
            assert!(matches!(pending.read(&mut bytes), Err(ref e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
                "failover must not shrink quorum after losing the last Secondary");
            assert_eq!(cluster.status()["nodes"][1]["retained_work"]["app_observation"]["primary_last_acknowledged"], 2);
            assert!(cluster.ledger.placements[0].endpoint.is_none());
            Ok::<(), String>(())
        }).await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("FAILOVER_STATUS {}", cluster.status());
            for child in &cluster.children { eprintln!("FAILOVER_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn three_replica_service_copies_each_peer_replays_replies_and_preserves_majority() {
    for lose_majority in [false, true] {
    let fixture = Fixture::new("three-replica-service");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulThree;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    let started = cluster.start().await;
    if let Err(error) = started {
        let cleanup = cluster.shutdown().await;
        panic!("three-node startup: {error}; cleanup={cleanup}");
    }
    let result = peer_idle_catch(async {
        assert_eq!(cluster.children.len(), 3);
        assert!(cluster.activate_idle_replica().await.is_err());
        assert!(cluster.begin_generation_handshake().is_err());
        assert!(cluster.execute(command("wrong-primary", "create", Some(2))).await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
        let owners = cluster.status()["nodes"].as_array().unwrap().iter()
            .map(|n| (n["node_pid"].clone(), n["app_pid"].clone(), n["runtime_root"].clone())).collect::<Vec<_>>();
        cluster.add_idle_replica_on(3).await?;
        let frozen = cluster.ledger.idles[0].body.clone();
        let lost = hold_peer_loss_frame(&mut cluster, 0, "AddReplicaReply", Duration::from_secs(12)).await?;
        let (lost, _) = decode_add_replica_reply(&lost).map_err(err)?;
        assert_eq!(lost.replica.node, cluster.children[2].launch.node);
        assert_eq!(cluster.ledger.idle_status()["ready"], false);
        assert!(cluster.activate_idle_replica().await.is_err());
        cluster.add_idle_replica_on(2).await?;
        until(&mut cluster, |c| c.ledger.idles.len() == 2 && c.ledger.idles[1].reply.is_some()).await;
        assert_eq!(cluster.ledger.idle_status()["ready"], false);
        assert!(cluster.activate_idle_replica().await.is_err());
        let retry = cluster.ledger.idle_retry(0, Instant::now() + Duration::from_secs(11))
            .ok_or("missing per-target AddReplica retry")?;
        assert_eq!(retry, frozen);
        cluster.send_idle(&retry).await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        assert_eq!(cluster.ledger.idles[0].reply.as_ref(), Some(&lost));
        cluster.activate_idle_replica().await?;
        let config = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        assert_eq!(config.configuration.unit.target, 3);
        assert_eq!(config.configuration.unit.minimum, 2);
        assert_eq!(config.configuration.replicas.0.len(), 3);
        let lost = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(12)).await?;
        assert_eq!(cluster.ledger.configuration_status()["ready"], false);
        let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11))
            .ok_or("missing three-member reconfiguration retry")?;
        assert_eq!(retry, config);
        cluster.send_configuration(&retry).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let (lost, _) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
        assert_eq!(cluster.ledger.configuration.as_ref().unwrap().reply.as_ref(), Some(&lost));
        if !lose_majority {
            assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2"));
        }
        let last = if lose_majority { 1 } else { 2 };
        until(&mut cluster, |c| (1..3).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == last
            && c.status()["nodes"][i]["retained_work"]["local_replica"]["configuration"]["stage"] == "Complete")).await;
        let status = cluster.status();
        for (i, owner) in owners.into_iter().enumerate() {
            assert_eq!(owner, (status["nodes"][i]["node_pid"].clone(), status["nodes"][i]["app_pid"].clone(), status["nodes"][i]["runtime_root"].clone()));
            let observation = &status["nodes"][i]["retained_work"];
            assert_eq!(observation["app_observation"]["opens"], 1);
            assert_eq!(observation["peer_error"], Value::Null);
            assert_eq!(observation["peer_command_error"], Value::Null);
            if i != 0 {
                assert_eq!(observation["local_replica"]["replica"]["current_role"], 3);
                if !lose_majority {
                    assert_eq!(observation["app_observation"]["idle_data"]["role"], "ActiveSecondary");
                }
                assert_ne!(observation["app_observation"]["idle_data"]["write"], "Granted");
                assert_eq!(observation["app_observation"]["idle_data"]["provider"]["values"]["value"],
                    if lose_majority { "acknowledged" } else { "continued" });
            }
        }
        assert!(cluster.transfer_primary().await.is_err());
        assert!(cluster.retire_secondary_replica().await.is_err());
        if !lose_majority {
            return Ok(());
        }
        let destination = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &destination.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| c.ledger.configuration_status()["state"] == "unknown").await;
        assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2"));
        until(&mut cluster, |c| c.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        let destination = cluster.ledger.placements[1].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &destination.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| c.ledger.placements[1].loss_reported).await;
        // A distinct mutation must not complete from the historical singleton vote.
        let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
        pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
        pending.write_all(b"write-final\n").map_err(err)?;
        until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
        let mut bytes = [0; 1024];
        let read = pending.read(&mut bytes);
        assert!(matches!(read, Err(ref e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
            "lost majority must not acknowledge: {read:?}, bytes={bytes:?}");
        assert!(cluster.activate_idle_replica().await.is_err());
        assert_eq!(cluster.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_acknowledged"], 2);
        assert_eq!(cluster.ledger.configuration.as_ref().unwrap().body.configuration.replicas.0.len(), 3);
        assert_eq!(cluster.ledger.placements[0].endpoint.as_deref(), Some(endpoint.as_str()));
        Ok::<(), String>(())
    }).await;
    if !matches!(&result, Ok(Ok(()))) {
        eprintln!("THREE_STATUS {}", cluster.status());
        for child in &cluster.children { eprintln!("THREE_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn same_primary_configuration_lost_final_reply_retries_without_reapplying_roles() {
    let fixture = Fixture::new("active-secondary-reply-retry");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        cluster.activate_idle_replica().await?;
        let frozen = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        let lost = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(12)).await?;
        let (reply, owner) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
        assert_eq!(owner, Owner::Fm);
        assert_eq!(cluster.ledger.configuration_status()["state"], "pending");
        assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2"));
        let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11))
            .ok_or("missing native DoReconfiguration retry")?;
        assert_eq!(retry, frozen);
        cluster.send_configuration(&retry).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true
            && c.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        assert_eq!(cluster.ledger.configuration.as_ref().unwrap().reply.as_ref(), Some(&reply));
        let mut stale = frozen;
        stale.configuration.unit.current.configuration -= 1;
        cluster.send_configuration(&stale).await?;
        for _ in 0..10 { cluster.drive().await; }
        let status = cluster.status();
        assert_eq!(status["configuration"]["state"], "ready");
        assert_eq!(status["nodes"][0]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(status["peer_idle"]["command_error"], json!([null, null]));
        assert!(cluster.ledger.begin_generation().is_err());
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn same_primary_configuration_activates_secondary_and_never_acks_without_its_quorum() {
    let fixture = Fixture::new("active-secondary");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.activate_idle_replica().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        assert_eq!(cluster.activate_idle_replica().await?["state"], "pending");
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true
            && c.status()["peer_idle"]["target"]["configuration"]["stage"] == "Complete").await;
        let status = cluster.status();
        assert_eq!(status["peer_idle"]["target"]["replica"]["current_role"], 3);
        assert_eq!(status["peer_idle"]["target"]["configuration"]["stage"], "Complete");
        assert_eq!(status["configuration"]["request"]["configuration"]["replicas"][0]["previous_role"], 4);
        assert_eq!(status["configuration"]["request"]["configuration"]["replicas"][1]["previous_role"], 2);
        assert_eq!(cluster.activate_idle_replica().await?["state"], "ready");
        assert!(cluster.remove_idle_replica_via_fm().await.is_err());
        assert!(cluster.replace_idle_replica().await.is_err());
        let write = data_contact(&endpoint, b"write-more\n");
        assert!(write.contains("committed=2"), "{write}");
        until(&mut cluster, |c| c.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        let secondary = cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"].clone();
        assert_eq!(secondary["role"], "ActiveSecondary");
        assert_ne!(secondary["write"], "Granted");
        let destination = cluster.ledger.placements[1].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &destination.to_string()]).status().unwrap().success());
        until(&mut cluster, |c| c.ledger.configuration_status()["state"] == "unknown").await;
        assert!(cluster.retire_secondary_replica().await.is_err());
        let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
        pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
        pending.write_all(b"write-final\n").map_err(err)?;
        until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
        let mut bytes = [0; 1024];
        let read = pending.read(&mut bytes);
        assert!(matches!(read, Err(ref e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
            "lost Secondary must not ACK from the old singleton quorum: {read:?}");
        assert_eq!(cluster.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_acknowledged"], 2);
        assert_eq!(cluster.ledger.placements[0].endpoint.as_deref(), Some(endpoint.as_str()));
        Ok::<(), String>(())
    }).await;
    if !matches!(&result, Ok(Ok(()))) {
        eprintln!("CONFIGURATION_NODE_LOGS {:?}", bounded_logs(cluster.children[0].process.as_ref().unwrap()));
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn planned_primary_transfer_new_primary_loss_never_reactivates_old_writer() {
    let fixture = Fixture::new("planned-primary-loss");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let old_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&old_endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&old_endpoint, b"write\n").contains("committed=1"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        cluster.activate_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        cluster.transfer_primary().await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let new_endpoint = cluster.ledger.placements[1].endpoint.clone().unwrap();
        assert!(data_contact(&new_endpoint, b"write-more\n").contains("committed=2"));
        let new_primary = cluster.ledger.placements[1].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &new_primary.to_string()]).status().unwrap().success());
        until(&mut cluster, |c| c.ledger.configuration_status()["state"] == "unknown").await;
        let denied = data_contact(&old_endpoint, b"write-final\n");
        assert!(denied.contains("NotPrimary"), "{denied}");
        assert!(cluster.transfer_primary().await.is_err());
        assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn planned_primary_transfer_fences_old_writer_replays_lost_reply_and_preserves_data() {
    let fixture = Fixture::new("planned-primary-transfer");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.transfer_primary().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let old_endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&old_endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&old_endpoint, b"write\n").contains("committed=1"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        cluster.activate_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let old_configuration = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        let owners = cluster.status()["nodes"].as_array().unwrap().iter()
            .map(|n| (n["node_pid"].clone(), n["app_pid"].clone(), n["runtime_root"].clone())).collect::<Vec<_>>();
        assert_eq!(cluster.transfer_primary().await?["state"], "pending");
        let intent = cluster.ledger.swap.as_ref().unwrap().body.clone();
        assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
        let lost = hold_peer_loss_frame(&mut cluster, 1, "DoReconfigurationReply", Duration::from_secs(15)).await?;
        let (reply, _) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
        assert_eq!(cluster.ledger.configuration_status()["state"], "pending");
        assert!(cluster.ledger.placements.iter().all(|p| p.endpoint.is_none()));
        let old_write = data_contact(&old_endpoint, b"write-more\n");
        assert!(old_write.contains("NotPrimary"), "{old_write}");
        let retry = cluster.ledger.configuration_retry(Instant::now() + Duration::from_secs(11)).ok_or("missing swap retry")?;
        assert_eq!(retry, intent);
        cluster.send_configuration(&retry).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        assert_eq!(cluster.ledger.swap.as_ref().unwrap().reply.as_ref(), Some(&reply));
        let new_endpoint = cluster.ledger.placements[1].endpoint.clone().ok_or("missing new Primary endpoint")?;
        assert_ne!(new_endpoint, old_endpoint);
        assert!(data_contact(&new_endpoint, b"read\n").contains("committed=1"));
        assert!(data_contact(&new_endpoint, b"write-more\n").contains("committed=2"));
        until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        cluster.send_configuration(&old_configuration).await?;
        assert!(data_contact(&new_endpoint, b"write-final\n").contains("committed=3"));
        until(&mut cluster, |c| c.status()["nodes"][0]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 3).await;
        let status = cluster.status();
        assert_eq!(status["peer_idle"]["command_error"], json!([null, null]));
        assert_eq!(status["peer_idle"]["peer_error"], json!([null, null]));
        for (i, owner) in owners.into_iter().enumerate() {
            assert_eq!(owner, (status["nodes"][i]["node_pid"].clone(), status["nodes"][i]["app_pid"].clone(), status["nodes"][i]["runtime_root"].clone()));
            assert_eq!(status["nodes"][i]["retained_work"]["app_observation"]["opens"], 1);
        }
        assert_eq!(cluster.transfer_primary().await?["state"], "ready");
        assert!(cluster.activate_idle_replica().await.is_err());
        assert!(cluster.retire_secondary_replica().await.is_err());
        Ok::<(), String>(())
    }).await;
    if !matches!(&result, Ok(Ok(()))) {
        eprintln!("SWAP_STATUS {}", cluster.status());
        for child in &cluster.children { eprintln!("SWAP_LOG {:?}", bounded_logs(child.process.as_ref().unwrap())); }
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn configured_secondary_retirement_replaces_and_reactivates_real_data() {
    let fixture = Fixture::new("configured-secondary-replacement");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.retire_secondary_replica().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        let predecessor_add = cluster.ledger.idles.first().unwrap().body.clone();
        cluster.activate_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let predecessor_configuration = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        let owners = cluster.status()["nodes"].as_array().unwrap().iter()
            .map(|n| (n["node_pid"].clone(), n["app_pid"].clone(), n["runtime_root"].clone())).collect::<Vec<_>>();
        assert!(cluster.delete_idle_replica_via_fm().await.is_err());
        cluster.retire_secondary_replica().await?;
        assert!(cluster.replace_idle_replica().await.is_err());
        let retirement = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        let lost = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(12)).await?;
        let (reply, _) = decode_do_reconfiguration_reply(&lost).map_err(err)?;
        assert_eq!(reply.replicas.0.len(), 1);
        assert_eq!(cluster.ledger.configuration_status()["state"], "pending");
        assert!(cluster.replace_idle_replica().await.is_err());
        cluster.send_configuration(&retirement).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true
            && c.status()["peer_idle"]["target"]["phase"] == "Closed").await;
        assert_eq!(cluster.ledger.configuration.as_ref().unwrap().reply.as_ref(), Some(&reply));
        assert_eq!(cluster.status()["peer_idle"]["active_secondary"], false);
        assert_eq!(cluster.status()["peer_idle"]["state"], "retired_secondary");
        assert!(cluster.ledger.placements.iter().all(|p| !p.loss_reported && p.link_alive));
        assert!(cluster.replace_idle_replica().await.is_err());
        assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2"));
        cluster.delete_idle_replica_via_fm().await?;
        let predecessor_delete = cluster.ledger.idle_deletion.as_ref().unwrap().body.clone();
        until(&mut cluster, |c| c.ledger.idle_deletion_status()["complete"] == true).await;
        assert_eq!(cluster.replace_idle_replica().await?["state"], "pending");
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true
            && c.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        let successor = cluster.ledger.idles.first().unwrap().body.clone();
        assert!(successor.replica.id > predecessor_add.replica.id);
        assert!(successor.replica.instance > predecessor_add.replica.instance);
        cluster.activate_idle_replica().await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true
            && c.status()["peer_idle"]["target"]["configuration"]["stage"] == "Complete").await;
        cluster.send_configuration(&predecessor_configuration).await?;
        cluster.send_configuration(&retirement).await?;
        cluster.send_idle(&predecessor_add).await?;
        cluster.send_idle_deletion(&predecessor_delete).await?;
        assert!(data_contact(&endpoint, b"write-final\n").contains("committed=3"));
        until(&mut cluster, |c| c.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 3).await;
        let status = cluster.status();
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["role"], "ActiveSecondary");
        assert_eq!(status["peer_idle"]["command_error"], json!([null, null]));
        assert_eq!(status["peer_idle"]["peer_error"], json!([null, null]));
        assert_eq!(status["nodes"][0]["retained_work"]["app_observation"]["opens"], 1);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["opens"], 2);
        assert_eq!(status["configuration"]["history"].as_array().unwrap().len(), 2);
        assert_eq!(cluster.ledger.placements[0].endpoint.as_deref(), Some(endpoint.as_str()));
        for (i, owner) in owners.into_iter().enumerate() {
            assert_eq!(owner, (status["nodes"][i]["node_pid"].clone(), status["nodes"][i]["app_pid"].clone(), status["nodes"][i]["runtime_root"].clone()));
        }
        assert!(cluster.retire_secondary_replica().await.is_err());
        Ok::<(), String>(())
    }).await;
    if !matches!(&result, Ok(Ok(()))) {
        eprintln!("RETIREMENT_STATUS {}", cluster.status());
        for child in &cluster.children {
            eprintln!("RETIREMENT_LOG {:?}", bounded_logs(child.process.as_ref().unwrap()));
        }
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn fm_idle_replacement_rebuilds_real_data_retries_lost_reply_and_fences_old_lifetime() {
    let fixture = Fixture::new("fm-idle-replacement");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulPeerIdle;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        assert!(cluster.replace_idle_replica().await.is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |cluster| cluster.ledger.placements[0].observed == Observed::Running).await;
        let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&primary, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&primary, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.add_idle_replica().await?;
        until(&mut cluster, |cluster| cluster.ledger.idle_status()["ready"] == true
            && cluster.status()["peer_idle"]["state"] == "open").await;
        let original = cluster.ledger.idles.first().unwrap().body.clone();
        let owners = cluster.status()["nodes"].as_array().unwrap().iter()
            .map(|node| (node["node_pid"].clone(), node["app_pid"].clone(), node["runtime_root"].clone()))
            .collect::<Vec<_>>();
        assert!(cluster.replace_idle_replica().await.is_err());
        cluster.remove_idle_replica_via_fm().await?;
        let removing = cluster.ledger.idle_removal.as_ref().unwrap().body.clone();
        assert!(cluster.replace_idle_replica().await.is_err());
        cluster.delete_idle_replica_via_fm().await?;
        let deleting = cluster.ledger.idle_deletion.as_ref().unwrap().body.clone();
        until(&mut cluster, |cluster| cluster.ledger.idle_deletion_status()["complete"] == true
            && cluster.ledger.idle_removal_status()["removed"] == true).await;
        let old_proof = cluster.ledger.placements[1].retirement.clone().unwrap();
        assert_eq!(cluster.replace_idle_replica().await?["state"], "pending");
        let successor = cluster.ledger.idles.first().unwrap().body.clone();
        assert!(successor.replica.id > original.replica.id);
        assert!(successor.replica.instance > original.replica.instance);
        let end = Instant::now() + Duration::from_secs(12);
        let dropped = loop {
            let mut dropped = None;
            for index in 0..2 {
                let incoming = cluster.children[index].link.as_ref().unwrap()
                    .receive(Instant::now() + Duration::from_millis(2)).await?;
                if let Some(mut incoming) = incoming {
                    if incoming.message.properties().map_err(err)?.action == "AddReplicaReply" {
                        let (body, _) = decode_add_replica_reply(&incoming.message).map_err(err)?;
                        assert_eq!(body.replica.id, successor.replica.id);
                        if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                        dropped = Some(body);
                    } else {
                        cluster.receive_message(index, incoming).await?;
                    }
                }
            }
            if let Some(body) = dropped { break body; }
            assert!(Instant::now() < end, "successor Add reply deadline: {}", cluster.status());
        };
        assert_eq!(cluster.ledger.idle_status()["state"], "pending");
        let retry = cluster.ledger.idle_retry(0, Instant::now() + Duration::from_secs(11))
            .ok_or("missing successor Add retry")?;
        assert_eq!(retry, successor);
        cluster.send_idle(&retry).await?;
        until(&mut cluster, |cluster| cluster.ledger.idle_status()["ready"] == true
            && cluster.status()["peer_idle"]["target"]["replica"]["id"] == successor.replica.id
            && cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1).await;
        assert_eq!(cluster.ledger.idles.first().unwrap().reply.as_ref(), Some(&dropped));
        assert_eq!(cluster.replace_idle_replica().await?["state"], "ready");
        let history = cluster.status()["idle_predecessor"].clone();
        assert_eq!(history["deletion"]["completion"], "disposed");
        assert_eq!(history["disposal"], serde_json::to_value(&old_proof).map_err(err)?);
        cluster.send_idle(&original).await?;
        cluster.send_idle_removal(&removing).await?;
        cluster.send_idle_deletion(&deleting).await?;
        let write = data_contact(&primary, b"write-more\n");
        assert!(write.contains("committed=2 value=continued"), "{write}");
        until(&mut cluster, |cluster| cluster.status()["nodes"][1]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        let status = cluster.status();
        assert_eq!(status["idle_predecessor"], history);
        assert_eq!(status["peer_idle"]["target"]["replica"]["current_role"], 2);
        assert_eq!(status["peer_idle"]["target"]["replica"]["state"], 1);
        assert_eq!(status["peer_idle"]["serving"], false);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["opens"], 2);
        assert_eq!(status["nodes"][1]["retained_work"]["app_observation"]["provider_acquisitions"], 2);
        assert_eq!(status["peer_idle"]["command_error"], json!([null, null]));
        assert_eq!(status["peer_idle"]["peer_error"], json!([null, null]));
        for (index, owner) in owners.into_iter().enumerate() {
            assert_eq!(owner, (status["nodes"][index]["node_pid"].clone(),
                status["nodes"][index]["app_pid"].clone(), status["nodes"][index]["runtime_root"].clone()));
        }
        assert_eq!(data_contact(&primary, b"read\n"), write);
        assert!(cluster.ledger.placements.iter().all(|placement| placement.link_alive),
            "replacement links: {}", cluster.status());
        cluster.remove_idle_replica_via_fm().await?;
        cluster.delete_idle_replica_via_fm().await?;
        until(&mut cluster, |cluster| cluster.ledger.idle_deletion_status()["complete"] == true
            && cluster.ledger.idle_removal_status()["removed"] == true).await;
        assert_eq!(cluster.ledger.placements[1].retirement.as_ref().unwrap().disposal.body.replica.id,
            successor.replica.id);
        assert!(cluster.replace_idle_replica().await.is_err());
        assert_eq!(cluster.status()["idle_predecessor"], history);
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

fn failed_open_receipt(placement: &fm::Placement, error: i64) -> FailedOpenReceipt {
    let mut disposal = retirement(placement).disposal;
    disposal.body.replica.current_role = 2;
    FailedOpenReceipt {
        deployment: placement.launch.deployment.clone(), session: placement.launch.session.clone(),
        node: placement.launch.node.clone(), fm: placement.launch.fm.clone(), generation: placement.launch.generation.clone(),
        node_pid: 555, app_pid: placement.profile.as_ref().unwrap().app_pid,
        runtime_root: placement.profile.as_ref().unwrap().runtime_root.clone(),
        deletion: fube_node::local::FailedOpenDeletion {
            failure: fube_node::local::RuntimeOpenFailure {
                disposal, creation: 1, error,
            },
            command: 9, ra_operation: 20,
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
        peer_addresses: vec![],
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
async fn real_generation_upload_uses_correlated_replies_and_resumes_new_generation_control() {
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
                && round.phase == fm::GenerationPhase::Complete
                && c.status()["nodes"].as_array().unwrap().iter().all(|node| {
                    node["retained_work"]["generations"]["upload_pending"] == false
                        && node["retained_work"]["generations"]["send"] == json!(round.number)
                })
        })
        .await;
        let status = cluster.status();
        assert_eq!(status["generation_handshake"]["phase"], "complete");
        assert_eq!(status["generation_handshake"]["complete"], true);
        let round = cluster.ledger.generation.as_ref().unwrap();
        assert!(!round.uploads[0].as_ref().unwrap().any_replica_found);
        assert!(round.uploads[0].as_ref().unwrap().units.0.is_empty());
        assert!(round.uploads[1].as_ref().unwrap().any_replica_found);
        assert_eq!(round.uploads[1].as_ref().unwrap().units.0.len(), 1);
        for node in status["nodes"].as_array().unwrap() {
            assert_eq!(node["link_alive"], true);
        }
        assert_eq!(contact(&sibling)["service"], SERVICE);
        assert!(!cluster.ledger.placements[0].created);
        cluster.execute(command("after-upload", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        assert_eq!(cluster.ledger.placements[1].endpoint.as_ref(), Some(&sibling));
        let mut service = cluster.ledger.placements[1].body()?.service;
        service.update_version += 1;
        cluster.update_service(1, service.clone()).await?;
        until(&mut cluster, |c| c.ledger.placements[1].service_description.as_ref() == Some(&service)).await;
        cluster.execute(command("delete-after-upload", "delete", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Absent).await;
        Ok(())
    }
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap();
}

#[tokio::test]
async fn lfum_overlapping_updates_keep_independent_retries_and_resume_backpressure() {
    let fixture = Fixture::new("lfum-lost-reply");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.prepare().unwrap();
    let result = node::report_tests::run_lfum_retry(&cluster.ledger.placements[0].launch).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn real_lfum_during_primary_failover_preserves_sf_phases_data_and_original_owners() {
    let fixture = Fixture::new("failover-lfum");
    let mut config = fixture.config();
    config.profile = ServiceProfile::Stateful { target: 3, minimum: 2 };
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let mut releases = vec![];
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        until(&mut cluster, |c| (1..3).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
        let owners = cluster.status();
        let mut work = vec![];
        for (index, barrier) in [(1, "local-primary-role"), (1, "local-current-configuration")] {
            let profile = cluster.children[index].metadata.as_ref().unwrap().profile.as_ref().unwrap();
            let path = profile.runtime_root.join("deployment/HostingLocalStatefulApp_App1/work");
            fs::write(path.join(format!("{barrier}.block")), b"hold actual failover role").map_err(err)?;
            releases.push(path.join(format!("{barrier}.release")));
            work.push(path.join(format!("{barrier}.held")));
        }
        let pid = cluster.ledger.placements[0].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| work[0].exists()
            && c.status()["nodes"][1]["retained_work"]["local_replica"]["swap"]["stage"] == "Catchup").await;
        let intent = cluster.ledger.swap.as_ref().unwrap().body.clone();
        assert!(TcpStream::connect_timeout(&endpoint.parse().map_err(err)?, Duration::from_millis(250)).is_err());
        cluster.begin_generation_handshake()?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let old = hold_lfum_uploads(&mut cluster).await?;
        let frozen = old.iter().map(|incoming| decode_lfum_upload(&incoming.message))
            .collect::<std::result::Result<Vec<_>, _>>().map_err(err)?;
        assert_eq!(frozen[1].units.0[0].unit, intent.configuration.unit);
        assert_eq!(frozen[1].units.0[0].intermediate, Epoch::default());
        let queried = &frozen[2].units.0[0];
        assert_eq!(queried.intermediate, Epoch::default());
        assert!(queried.replicas.0.iter().any(|r| r.description.node == cluster.children[0].launch.node
            && r.description.current_role == 4 && r.description.previous_role == 4 && r.description.up));
        cluster.ledger.generation.as_mut().unwrap().reject(&frozen[0].generation)?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let current = hold_lfum_uploads(&mut cluster).await?;
        for (index, mut incoming) in old.into_iter().enumerate() {
            incoming.reply.take().ok_or("missing failover LFUM reply")?.reply(
                cluster.children[index].link.as_ref().unwrap().seal(lfum_upload_reply_message(&GenerationHeader {
                    number: frozen[index].generation.clone(), for_fm_replica: false,
                }).map_err(err)?)?, wire::deadline(),
            ).await.map_err(err)?;
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while cluster.status()["nodes"].as_array().unwrap().iter().any(|n|
            n["retained_work"]["generations"]["pending_uploads"] != 1)
        {
            assert!(Instant::now() < deadline, "stale failover LFUM completion: {}", cluster.status());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for node in cluster.status()["nodes"].as_array().unwrap() {
            assert_eq!(node["retained_work"]["generations"]["send"],
                owners["nodes"][0]["retained_work"]["generations"]["send"]);
        }
        cluster.send_configuration(&intent).await?;
        for (index, incoming) in current.into_iter().enumerate() {
            assert_eq!(decode_lfum_upload(&incoming.message).map_err(err)?.units, frozen[index].units);
            cluster.receive_message(index, incoming).await?;
        }
        until(&mut cluster, |c| !c.ledger.generation_pending()
            && c.status()["nodes"].as_array().unwrap().iter().all(|n|
                n["retained_work"]["generations"]["upload_pending"] == false)).await;
        assert!(cluster.ledger.swap.as_ref().unwrap().reply.is_none());
        fs::write(&releases[0], b"finish Primary role").map_err(err)?;
        until(&mut cluster, |c| work[1].exists()
            && c.status()["nodes"][1]["retained_work"]["local_replica"]["swap"]["stage"] == "End").await;
        cluster.begin_generation_handshake()?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let activating = hold_lfum_uploads(&mut cluster).await?;
        for (index, incoming) in activating.into_iter().enumerate() {
            let body = decode_lfum_upload(&incoming.message).map_err(err)?;
            if index != 0 {
                let info = &body.units.0[0];
                let mut expected = intent.configuration.unit.clone();
                if index == 2 { expected.previous = Epoch::default(); }
                assert_eq!(info.unit, expected);
                assert_eq!(info.intermediate, if index == 1 { intent.configuration.unit.current } else { Epoch::default() });
                assert_eq!(info.report_from_primary, index == 1);
            }
            cluster.receive_message(index, incoming).await?;
        }
        until(&mut cluster, |c| !c.ledger.generation_pending()
            && c.status()["nodes"].as_array().unwrap().iter().all(|n|
                n["retained_work"]["generations"]["upload_pending"] == false)).await;
        assert!(cluster.ledger.swap.as_ref().unwrap().reply.is_none());
        fs::write(&releases[1], b"finish final RAP configuration").map_err(err)?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        let promoted = cluster.ledger.placements[1].endpoint.clone().unwrap();
        assert!(data_contact(&promoted, b"read\n").contains("committed=1 value=acknowledged"));
        assert!(data_contact(&promoted, b"write-more\n").contains("committed=2 value=continued"));
        until(&mut cluster, |c|
            c.status()["nodes"][2]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2).await;
        let after = cluster.status();
        for index in 1..3 {
            for field in ["node_pid", "app_pid", "runtime_root"] {
                assert_eq!(after["nodes"][index][field], owners["nodes"][index][field]);
            }
            assert_eq!(after["nodes"][index]["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(after["nodes"][index]["retained_work"]["app_observation"]["provider_acquisitions"], 1);
            assert_eq!(after["nodes"][index]["retained_work"]["peer_command_error"], Value::Null);
        }
        let pid = cluster.ledger.placements[2].profile.as_ref().unwrap().app_pid;
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success());
        until(&mut cluster, |c| c.ledger.placements[2].loss_reported).await;
        let mut pending = TcpStream::connect(&promoted).map_err(err)?;
        pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
        pending.write_all(b"write-final\n").map_err(err)?;
        until(&mut cluster, |c|
            c.status()["nodes"][1]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
        let mut bytes = [0; 1024];
        assert!(matches!(pending.read(&mut bytes), Err(ref error)
            if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
        assert_eq!(cluster.status()["nodes"][1]["retained_work"]["app_observation"]["primary_last_acknowledged"], 2);
        Ok::<(), String>(())
    }).await;
    for path in &releases { fs::write(path, b"release failover role").unwrap(); }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn real_lfum_during_idle_build_preserves_sf_inventory_copy_and_original_owners() {
    let fixture = Fixture::new("building-lfum");
    let mut config = fixture.config();
    config.profile = ServiceProfile::Stateful { target: 3, minimum: 2 };
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let mut barriers = vec![];
    let result = peer_idle_catch(async {
        for index in 1..3 {
            let profile = cluster.children[index].metadata.as_ref().unwrap().profile.as_ref().unwrap();
            let work = profile.runtime_root.join("deployment/HostingLocalStatefulApp_App1/work");
            fs::write(work.join("local-copy.block"), b"hold actual copy").map_err(err)?;
            barriers.push(work);
        }
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running
            && barriers[0].join("local-copy.held").exists()).await;
        cluster.add_idle_replica_on(3).await?;
        until(&mut cluster, |_| barriers.iter().all(|work| work.join("local-copy.held").exists())).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        let authority = data_contact(&endpoint, b"read\n");
        let owners = cluster.status();
        let requests: Vec<_> = cluster.ledger.idles.iter().map(|idle| idle.body.clone()).collect();
        assert!(cluster.ledger.idles.iter().all(|idle| idle.reply.is_none()));
        cluster.begin_generation_handshake()?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let held = hold_lfum_uploads(&mut cluster).await?;
        let frozen_bodies = held.iter().map(|incoming| decode_lfum_upload(&incoming.message))
            .collect::<std::result::Result<Vec<_>, _>>().map_err(err)?;
        let frozen = frozen_bodies[0].clone();
        assert_eq!(frozen.units.0[0].replicas.0.len(), 3);
        for (index, incoming) in held.iter().enumerate() {
            let body = decode_lfum_upload(&incoming.message).map_err(err)?;
            let info = &body.units.0[0];
            assert_eq!(info.intermediate, Epoch::default());
            assert_eq!(info.report_from_primary, index == 0);
            assert!(info.replicas.0.iter().all(|r| r.intermediate_role == 1
                && r.description.first_lsn == -1 && r.description.last_lsn == -1));
            if index != 0 {
                assert_eq!(info.replicas.0.len(), 1);
                let local = &info.replicas.0[0].description;
                assert_eq!((local.current_role, local.state), (2, 1));
                assert!(frozen.units.0[0].replicas.0.iter().any(|r| r.description == *local));
            }
        }
        cluster.ledger.generation.as_mut().unwrap().reject(&frozen.generation)?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let current = hold_lfum_uploads(&mut cluster).await?;
        for (index, mut incoming) in held.into_iter().enumerate() {
            incoming.reply.take().ok_or("missing build LFUM reply")?.reply(
                cluster.children[index].link.as_ref().unwrap().seal(lfum_upload_reply_message(&GenerationHeader {
                    number: frozen.generation.clone(), for_fm_replica: false,
                }).map_err(err)?)?, wire::deadline(),
            ).await.map_err(err)?;
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while cluster.status()["nodes"].as_array().unwrap().iter().any(|n|
            n["retained_work"]["generations"]["pending_uploads"] != 1)
        {
            assert!(Instant::now() < deadline, "stale build LFUM reply: {}", cluster.status());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for node in cluster.status()["nodes"].as_array().unwrap() {
            assert_eq!(node["retained_work"]["generations"]["send"],
                owners["nodes"][0]["retained_work"]["generations"]["send"]);
        }
        for request in &requests { cluster.send_idle(request).await?; }
        assert!(cluster.activate_idle_replica().await.is_err());
        fs::write(barriers[0].join("local-copy.release"), b"finish first copy").map_err(err)?;
        until(&mut cluster, |c| c.ledger.idles.iter().filter(|idle| idle.reply.is_some()).count() == 1).await;
        assert!(cluster.ledger.configuration.is_none());
        fs::write(barriers[1].join("local-copy.release"), b"finish second copy").map_err(err)?;
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        assert!(cluster.ledger.configuration.is_none());
        assert_eq!(data_contact(&endpoint, b"read\n"), authority);
        cluster.activate_idle_replica().await?;
        let admission = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        until(&mut cluster, |c|
            c.status()["nodes"][0]["retained_work"]["local_replica"]["configuration"]["stage"] == "Activate").await;
        assert!(cluster.ledger.configuration.as_ref().unwrap().reply.is_none());
        assert_eq!(cluster.status()["nodes"][0]["retained_work"]["generations"]["upload_pending"], true);
        for (index, incoming) in current.into_iter().enumerate() {
            assert_eq!(decode_lfum_upload(&incoming.message).map_err(err)?.units,
                frozen_bodies[index].units);
            cluster.receive_message(index, incoming).await?;
        }
        until(&mut cluster, |c| !c.ledger.generation_pending()
            && c.status()["nodes"].as_array().unwrap().iter().all(|n|
                n["retained_work"]["generations"]["upload_pending"] == false)).await;
        cluster.send_configuration(&admission).await?;
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        until(&mut cluster, |c| (1..3).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 1)).await;
        let after = cluster.status();
        for (old, new) in owners["nodes"].as_array().unwrap().iter().zip(after["nodes"].as_array().unwrap()) {
            for field in ["node_pid", "app_pid", "runtime_root"] { assert_eq!(old[field], new[field]); }
            assert_eq!(new["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(new["retained_work"]["app_observation"]["provider_acquisitions"], 1);
        }
        Ok::<(), String>(())
    }).await;
    for work in &barriers { fs::write(work.join("local-copy.release"), b"release copy").unwrap(); }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn real_lfum_during_secondary_admission_preserves_sf_inventory_and_original_owners() {
    let fixture = Fixture::new("admitting-lfum");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulThree;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let mut barriers = vec![];
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        for destination in [2, 3] { cluster.add_idle_replica_on(destination).await?; }
        until(&mut cluster, |c| c.ledger.idle_status()["ready"] == true).await;
        let owners = cluster.status();
        for index in 1..3 {
            let profile = cluster.children[index].metadata.as_ref().unwrap().profile.as_ref().unwrap();
            let work = profile.runtime_root.join("deployment/HostingLocalStatefulApp_App1/work");
            fs::write(work.join("local-secondary-role.block"), b"hold role").map_err(err)?;
            barriers.push(work);
        }
        cluster.activate_idle_replica().await?;
        until(&mut cluster, |c| barriers.iter().all(|work| work.join("local-secondary-role.held").exists())
            && c.status()["nodes"][0]["retained_work"]["local_replica"]["configuration"]["stage"] == "Activate").await;
        let configuration = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        let authority = data_contact(&endpoint, b"read\n");
        cluster.begin_generation_handshake()?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let held = hold_lfum_uploads(&mut cluster).await?;
        let frozen = decode_lfum_upload(&held[0].message).map_err(err)?;
        assert_eq!(frozen.units.0[0].unit, configuration.configuration.unit);
        assert_eq!(frozen.units.0[0].intermediate, configuration.configuration.unit.current);
        for (index, incoming) in held.iter().enumerate() {
            let body = decode_lfum_upload(&incoming.message).map_err(err)?;
            assert_eq!(body.units.0[0].unit, configuration.configuration.unit);
            assert_eq!(body.units.0[0].intermediate, if index == 0 {
                configuration.configuration.unit.current
            } else { Epoch::default() });
        }
        cluster.ledger.generation.as_mut().unwrap().reject(&frozen.generation)?;
        for _ in 0..2 { for index in 0..3 { cluster.retry_generation(index).await?; } }
        let current = hold_lfum_uploads(&mut cluster).await?;
        for (index, mut incoming) in held.into_iter().enumerate() {
            incoming.reply.take().ok_or("missing held upload reply")?.reply(
                cluster.children[index].link.as_ref().unwrap().seal(lfum_upload_reply_message(&GenerationHeader {
                    number: frozen.generation.clone(), for_fm_replica: false,
                }).map_err(err)?)?, wire::deadline(),
            ).await.map_err(err)?;
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while cluster.status()["nodes"].as_array().unwrap().iter().any(|n|
            n["retained_work"]["generations"]["pending_uploads"] != 1)
        {
            assert!(Instant::now() < deadline, "stale LFUM completion: {}", cluster.status());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for node in cluster.status()["nodes"].as_array().unwrap() {
            assert_eq!(node["retained_work"]["generations"]["send"],
                owners["nodes"][0]["retained_work"]["generations"]["send"]);
        }
        for (index, incoming) in current.into_iter().enumerate() {
            cluster.receive_message(index, incoming).await?;
        }
        until(&mut cluster, |c| !c.ledger.generation_pending()
            && c.status()["nodes"].as_array().unwrap().iter().all(|n|
                n["retained_work"]["generations"]["upload_pending"] == false)).await;
        assert_eq!(cluster.ledger.configuration_status()["ready"], false);
        assert_eq!(data_contact(&endpoint, b"read\n"), authority);
        for work in &barriers { fs::write(work.join("local-secondary-role.release"), b"release role").map_err(err)?; }
        until(&mut cluster, |c| c.ledger.configuration_status()["ready"] == true).await;
        assert!(data_contact(&endpoint, b"write-more\n").contains("committed=2 value=continued"));
        until(&mut cluster, |c| (1..3).all(|i|
            c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]["provider"]["last"] == 2)).await;
        let after = cluster.status();
        for (old, new) in owners["nodes"].as_array().unwrap().iter().zip(after["nodes"].as_array().unwrap()) {
            for field in ["node_pid", "app_pid", "runtime_root"] { assert_eq!(old[field], new[field]); }
            assert_eq!(new["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(new["retained_work"]["app_observation"]["provider_acquisitions"], 1);
        }
        let replay = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        cluster.send_configuration(&replay).await?;
        let reply = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(8)).await?;
        assert_eq!(decode_do_reconfiguration_reply(&reply).map_err(err)?.0,
            *cluster.ledger.configuration.as_ref().unwrap().reply.as_ref().unwrap());
        for index in 1..3 {
            let pid = cluster.ledger.placements[index].profile.as_ref().unwrap().app_pid;
            assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()])
                .status().map_err(err)?.success());
        }
        until(&mut cluster, |c| (1..3).all(|i| c.ledger.placements[i].loss_reported)).await;
        let mut pending = TcpStream::connect(&endpoint).map_err(err)?;
        pending.set_read_timeout(Some(Duration::from_millis(250))).map_err(err)?;
        pending.write_all(b"write-final\n").map_err(err)?;
        until(&mut cluster, |c|
            c.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_admitted"] == 3).await;
        let mut bytes = [0; 1024];
        assert!(matches!(pending.read(&mut bytes), Err(ref error)
            if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
        assert_eq!(cluster.status()["nodes"][0]["retained_work"]["app_observation"]["primary_last_acknowledged"], 2);
        Ok::<(), String>(())
    }).await;
    for work in &barriers {
        fs::write(work.join("local-secondary-role.release"), b"release role").unwrap();
    }
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

async fn hold_lfum_uploads(cluster: &mut Cluster) -> Result<Vec<IncomingMessage<GatewayTarget>>> {
    let mut held = vec![];
    for index in 0..cluster.children.len() {
        loop {
            let mut incoming = cluster.children[index].link.as_ref().unwrap().receive(wire::deadline()).await?
                .ok_or("missing replicated LFUM upload")?;
            if incoming.message.properties().map_err(err)?.action == "LFUMUpload" {
                if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                held.push(incoming);
                break;
            }
            cluster.receive_message(index, incoming).await?;
        }
    }
    Ok(held)
}

#[tokio::test]
async fn real_replicated_lfum_preserves_membership_data_owners_and_new_generation_control() {
    let fixture = Fixture::new("replicated-lfum");
    let mut config = fixture.config();
    config.profile = ServiceProfile::Stateful { target: 3, minimum: 2 };
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    if let Err(error) = cluster.start().await {
        let cleanup = cluster.shutdown().await;
        panic!("replicated LFUM startup: {error}; cleanup={cleanup}");
    }
    let result = peer_idle_catch(async {
        assert!(cluster.begin_generation_handshake().is_err());
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.result("primary")["state"] == "complete").await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().ok_or("missing Primary endpoint")?;
        until(&mut cluster, |_| data_contact(&endpoint, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        until(&mut cluster, |c| c.status()["nodes"].as_array().unwrap().iter().all(|n|
            n["retained_work"]["local_replica"]["configuration"]["stage"] == "Complete")).await;
        let before = cluster.status();
        let members = cluster.ledger.configuration.as_ref().unwrap().reply.clone().unwrap();
        cluster.begin_generation_handshake()?;
        for _ in 0..2 {
            for index in 0..3 { cluster.retry_generation(index).await?; }
        }
        let held = hold_lfum_uploads(&mut cluster).await?;
        let frozen = decode_lfum_upload(&held[0].message).map_err(err)?;
        assert_eq!(frozen.units.0[0].replicas.0.len(), 3);
        // Withhold the first semantic reply, supersede the round, then deliver
        // that old correlated reply. It cannot acknowledge the new inventory.
        cluster.ledger.generation.as_mut().unwrap().reject(&frozen.generation)?;
        for _ in 0..2 {
            for index in 0..3 { cluster.retry_generation(index).await?; }
        }
        let next = cluster.ledger.generation.as_ref().unwrap().number.clone();
        let deadline = Instant::now() + Duration::from_secs(3);
        while cluster.status()["nodes"][0]["retained_work"]["generations"]["receive"] != json!(next) {
            assert!(Instant::now() < deadline, "new receive generation: {}", cluster.status());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for (index, mut incoming) in held.into_iter().enumerate() {
            incoming.reply.take().ok_or("missing held LFUM reply authority")?.reply(
                cluster.children[index].link.as_ref().unwrap().seal(lfum_upload_reply_message(&GenerationHeader {
                    number: frozen.generation.clone(), for_fm_replica: false,
                }).map_err(err)?)?, wire::deadline(),
            ).await.map_err(err)?;
        }
        assert!(cluster.ledger.generation.as_ref().unwrap().uploads[0].is_none());
        assert_eq!(cluster.status()["nodes"][0]["retained_work"]["generations"]["send"],
            before["nodes"][0]["retained_work"]["generations"]["send"]);
        until(&mut cluster, |c| !c.ledger.generation_pending()
            && c.status()["nodes"].as_array().unwrap().iter().all(|n|
                n["retained_work"]["generations"]["upload_pending"] == false)).await;
        let round = cluster.ledger.generation.as_ref().unwrap();
        assert_eq!(round.uploads.len(), 3);
        assert_eq!(round.uploads[0].as_ref().unwrap().units, frozen.units);
        for (index, upload) in round.uploads.iter().enumerate() {
            let upload = upload.as_ref().unwrap();
            assert!(upload.any_replica_found);
            assert_eq!(upload.units.0[0].unit, members.unit);
            assert_eq!(upload.units.0[0].replicas.0.len(), 3);
            assert_eq!(upload.units.0[0].report_from_primary, index == 0);
        }
        // A replay under the new generation exercises shared control admission,
        // without granting new membership or invoking another application Open.
        let replay = cluster.ledger.configuration.as_ref().unwrap().body.clone();
        cluster.send_configuration(&replay).await?;
        let message = hold_peer_loss_frame(&mut cluster, 0, "DoReconfigurationReply", Duration::from_secs(8)).await?;
        let (reply, owner) = decode_do_reconfiguration_reply(&message).map_err(err)?;
        assert_eq!(reply, members);
        assert_eq!(owner, Owner::Fm);
        let continued = data_contact(&endpoint, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        assert_eq!(data_contact(&endpoint, b"read\n"), continued);
        let after = cluster.status();
        for (old, new) in before["nodes"].as_array().unwrap().iter().zip(after["nodes"].as_array().unwrap()) {
            for field in ["node_pid", "app_pid", "runtime_root"] {
                assert_eq!(new[field], old[field]);
            }
            assert_eq!(new["retained_work"]["app_observation"]["opens"], 1);
            assert_eq!(new["retained_work"]["local_replica"], old["retained_work"]["local_replica"]);
        }
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn real_stateful_lfum_upload_preserves_original_primary_data_and_runtime_owners() {
    let fixture = Fixture::new("stateful-lfum");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulSingleton;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result: Result<()> = async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let primary = cluster.ledger.placements[0].endpoint.clone().unwrap();
        until(&mut cluster, |_| data_contact(&primary, b"read\n").starts_with("OK ")).await;
        assert!(data_contact(&primary, b"write\n").contains("committed=1 value=acknowledged"));
        let owners = cluster.children.iter().map(|child| {
            let profile = child.metadata.as_ref().unwrap().profile.as_ref().unwrap();
            (profile.app_pid, profile.runtime_root.clone())
        }).collect::<Vec<_>>();
        let original = cluster.ledger.placements[0].ready_replica.clone().unwrap();
        cluster.begin_generation_handshake()?;
        until(&mut cluster, |c| !c.ledger.generation_pending()
            && c.status()["nodes"].as_array().unwrap().iter().all(|node|
                node["retained_work"]["generations"]["upload_pending"] == false)).await;
        let upload = cluster.ledger.generation.as_ref().unwrap().uploads[0].as_ref().unwrap();
        assert!(upload.units.0[0].report_from_primary);
        assert_eq!(upload.units.0[0].replicas.0[0].description, original);
        assert_eq!(cluster.ledger.placements[0].endpoint.as_ref(), Some(&primary));
        let continued = data_contact(&primary, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        for (child, owner) in cluster.children.iter().zip(owners) {
            let profile = child.metadata.as_ref().unwrap().profile.as_ref().unwrap();
            assert_eq!((profile.app_pid, profile.runtime_root.clone()), owner);
        }
        Ok(())
    }.await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap();
}

#[tokio::test]
async fn real_lfum_channel_loss_preserves_original_primary_without_upload_success() {
    let fixture = Fixture::new("lfum-channel-loss");
    let mut config = fixture.config();
    config.profile = ServiceProfile::StatefulSingleton;
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::new(config).unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        cluster.execute(command("primary", "create", Some(1))).await?;
        until(&mut cluster, |c| c.ledger.placements[0].observed == Observed::Running).await;
        let endpoint = cluster.ledger.placements[0].endpoint.clone().unwrap();
        let initial = cluster.children[0].launch.generation.number.clone();
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1 value=acknowledged"));
        cluster.begin_generation_handshake()?;
        for index in 0..2 { cluster.retry_generation(index).await?; }
        for index in 0..2 { cluster.retry_generation(index).await?; }
        loop {
            let mut incoming = cluster.children[0].link.as_ref().unwrap()
                .receive(wire::deadline()).await?.ok_or("missing owned LFUM request")?;
            if incoming.message.properties().map_err(err)?.action == "LFUMUpload" {
                if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                break;
            }
            cluster.receive_message(0, incoming).await?;
        }
        cluster.children[0].link.as_ref().unwrap().channel.dispose();
        until(&mut cluster, |c| {
            let retained = &c.status()["nodes"][0]["retained_work"];
            retained["link_alive"] == false && retained["error"].is_string()
                && retained["generations"]["upload_pending"] == true
        }).await;
        assert_eq!(cluster.status()["nodes"][0]["retained_work"]["generations"]["send"], json!(initial));
        assert!(cluster.ledger.generation.as_ref().unwrap().uploads[0].is_none());
        assert!(cluster.ledger.generation_pending());
        let continued = data_contact(&endpoint, b"write-more\n");
        assert!(continued.contains("committed=2 value=continued"), "{continued}");
        Ok::<(), String>(())
    }).await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
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
            && request(&sibling, b"read\n").starts_with("OK ")
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
