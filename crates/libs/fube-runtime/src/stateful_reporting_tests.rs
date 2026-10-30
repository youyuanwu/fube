use super::super::reporting_tests::{
    assert_fault, assert_health, assert_load, health_reply, information, without_advancing_time,
    GUID_BYTES,
};
use super::*;
use crate::{ErrorKind, HealthInformation, HealthReportSendOption, LoadMetric, MoveCost};

async fn opened(mode: u8, role: i64) -> (Harness, Arc<StatefulCalls>, ProxyRequest) {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 17, mode);
    open.unit.id.id = Guid(GUID_BYTES);
    open.unit.consistency.id.id = Guid(GUID_BYTES);
    open.local.current_role = role;
    open.service.metrics.0.push(Metric {
        name: "Work".into(),
        weight: 1,
        primary: 0,
        secondary: 0,
    });
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle(GUID_BYTES).await;
    (h, calls, open)
}

#[tokio::test]
async fn stateful_reporting_requested_role_does_not_replace_completed_private_role() {
    let (mut h, calls, open) = opened(21, 4).await;
    let integration = calls.partitions.lock().unwrap()[0].clone();
    let mut local = open.local.clone();
    local.current_role = 3;
    local.previous_role = 4;
    let mut primary = control_tests::peer(&local, 2);
    primary.current_role = 4;
    let mut demote =
        control_tests::control(&open, "UpdateConfiguration", vec![local.clone(), primary]);
    demote.local = local;
    demote.unit.current.configuration += 1 << 32;
    demote.unit.previous = open.unit.current.clone();
    demote.flags = 2;
    h.send("UpdateConfiguration", &demote).await;
    control_tests::wait_event(&calls, "waitAll", 1).await;
    let status = integration.status().unwrap();
    assert_eq!(status.current_role, ReplicaRole::ActiveSecondary);
    assert_eq!(status.service_role, ReplicaRole::Primary);
    for (cost, value) in [
        (MoveCost::Zero, 0),
        (MoveCost::Low, 1),
        (MoveCost::Medium, 2),
        (MoveCost::High, 3),
    ] {
        let (partition, _) = integration.observe_report().unwrap();
        let call = h
            .engine
            .shared
            .execution
            .try_reserve()
            .unwrap()
            .run_owned(move || partition.report_move_cost(cost));
        let (result, message) = tokio::join!(call, h.receive("RAPReportLoad"));
        result.unwrap();
        assert_load(&message, true, true, "_MoveCost_", value);
    }
    calls.catch_up_release.notify_one();
    control_tests::wait_event(&calls, "waitAll", 2).await;
    calls.catch_up_release.notify_one();
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    h.engine.shared.stateful.wait_idle(GUID_BYTES).await;
    assert_eq!(
        integration.status().unwrap().service_role,
        ReplicaRole::ActiveSecondary
    );
    let partition = calls.sdk_partitions.lock().unwrap()[0].clone();
    let call = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            partition.report_load(&[LoadMetric {
                name: "Work".into(),
                value: 731,
            }])
        });
    let (result, message) = tokio::join!(call, h.receive("RAPReportLoad"));
    result.unwrap();
    assert_load(&message, true, false, "Work", 731);
}

#[tokio::test]
async fn stateful_reporting_private_completed_primary_secondary_idle_roles_and_native_arrays() {
    for initial_role in [4, 2] {
        let (mut h, calls, open) = opened(20, initial_role).await;
        let integration = calls.partitions.lock().unwrap()[0].clone();
        let mut diagnostics = integration.status().unwrap();
        diagnostics.service_role = ReplicaRole::None;
        diagnostics.current_role = ReplicaRole::Unknown;
        let mut identity = integration.identity().clone();
        identity.replica_id = -1;
        identity.incarnation = -1;
        identity.partition_id = uuid::Uuid::nil();
        // Edited copies cannot select a role, partition or replica ID.
        assert_ne!(identity, *integration.identity());
        assert_ne!(
            diagnostics.service_role,
            integration.status().unwrap().service_role
        );
        for role in [initial_role, 3] {
            if role == 3 {
                let mut local = open.local.clone();
                local.current_role = 3;
                local.previous_role = initial_role;
                let mut primary = control_tests::peer(&local, 2);
                primary.current_role = 4;
                let mut demote = control_tests::control(
                    &open,
                    "UpdateConfiguration",
                    vec![local.clone(), primary],
                );
                demote.local = local;
                demote.unit.current.configuration += 1 << 32;
                demote.unit.previous = open.unit.current.clone();
                demote.flags = 2;
                h.send("UpdateConfiguration", &demote).await;
                let reply: ProxyReply =
                    from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
                assert_eq!(reply.error.error, 0);
                h.engine.shared.stateful.wait_idle(GUID_BYTES).await;
                assert_eq!(
                    integration.status().unwrap().service_role,
                    ReplicaRole::ActiveSecondary
                );
            }
            for (name, value) in [("Work", 731), ("_MoveCost_", 3)] {
                let (partition, observation) = integration.observe_report().unwrap();
                let call = h
                    .engine
                    .shared
                    .execution
                    .try_reserve()
                    .unwrap()
                    .run_owned(move || {
                        let result: Result<()> = if name == "Work" {
                            partition.report_load(&[LoadMetric {
                                name: name.into(),
                                value,
                            }])
                        } else {
                            partition.report_move_cost(MoveCost::High)
                        };
                        result
                    });
                let (result, message) = tokio::join!(call, h.receive("RAPReportLoad"));
                result.unwrap();
                assert_load(&message, true, role == 4, name, value);
                assert!(observation
                    .result()
                    .unwrap()
                    .unwrap()
                    .health_sequence
                    .is_none());
            }
        }
        for metrics in [
            vec![],
            vec![LoadMetric {
                name: "Other".into(),
                value: 1,
            }],
            vec![LoadMetric {
                name: "Work".into(),
                value: i32::MAX as u32,
            }],
            vec![
                LoadMetric {
                    name: "Work".into(),
                    value: 1
                };
                2
            ],
        ] {
            let (partition, observation) = integration.observe_report().unwrap();
            let error = h
                .engine
                .shared
                .execution
                .try_reserve()
                .unwrap()
                .run_owned(move || partition.report_load(&metrics))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(observation.result().unwrap().is_err());
        }
        h.no_reply().await;
    }
}

#[tokio::test]
async fn stateful_reporting_all_health_targets_options_exact_results_and_stale_generation() {
    let (mut h, calls, mut open) = opened(20, 4).await;
    let integration = calls.partitions.lock().unwrap()[0].clone();
    let mut records = Vec::new();
    for kind in [1, 3] {
        for option in [None, Some(false), Some(true)] {
            let (partition, record) = integration.observe_report().unwrap();
            let call = h
                .engine
                .shared
                .execution
                .try_reserve()
                .unwrap()
                .run_owned(move || {
                    let result: Result<()> = match (kind, option) {
                        (1, None) => partition.report_replica_health(&information()),
                        (3, None) => partition.report_partition_health(&information()),
                        (1, Some(immediate)) => partition.report_replica_health2(
                            &information(),
                            Some(&HealthReportSendOption { immediate }),
                        ),
                        (3, Some(immediate)) => partition.report_partition_health2(
                            &information(),
                            Some(&HealthReportSendOption { immediate }),
                        ),
                        _ => unreachable!(),
                    };
                    result
                });
            tokio::pin!(call);
            tokio::time::pause();
            let before = tokio::time::Instant::now();
            let message = without_advancing_time(h.receive("ReportHealth")).await;
            assert_eq!(tokio::time::Instant::now(), before);
            assert_health(&message, kind, i64::MAX, Some(45));
            poll_pending_once(call.as_mut()).await;
            assert!(record.result().is_none());
            health_reply(&mut h, &message, kind, 45, None).await;
            without_advancing_time(call).await.unwrap();
            tokio::time::resume();
            records.push(record);
        }
        // Each identity component/result must match exactly, independently.
        for wrong in 0..7 {
            let partition = calls.sdk_partitions.lock().unwrap()[0].clone();
            let call = h
                .engine
                .shared
                .execution
                .try_reserve()
                .unwrap()
                .run_owned(move || {
                    if kind == 1 {
                        partition.report_replica_health(&information())
                    } else {
                        partition.report_partition_health(&information())
                    }
                });
            let message = h.receive("ReportHealth").await;
            assert_health(&message, kind, i64::MAX, Some(45));
            health_reply(&mut h, &message, kind, 45, Some(wrong)).await;
            let error = call.await.unwrap_err();
            assert_eq!(
                error.kind(),
                if wrong == 6 {
                    ErrorKind::Native
                } else {
                    ErrorKind::Transport
                }
            );
            if wrong == 6 {
                assert_eq!(error.native_code(), Some(-123));
            }
        }
    }
    assert!(records
        .iter()
        .all(|r| r.result().unwrap().unwrap().health_sequence == Some(45)));
    let (partition, fault_record) = integration.observe_report().unwrap();
    let call = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || partition.report_fault(FaultType::Permanent));
    let (result, message) = tokio::join!(call, h.receive("RAReportFault"));
    result.unwrap();
    assert_fault(&message, (1 << 32) + 1, 1, 17, 2);
    assert_eq!(
        fault_record
            .result()
            .unwrap()
            .unwrap()
            .fault
            .unwrap()
            .attempts,
        1
    );
    let stale = calls.sdk_partitions.lock().unwrap()[0].clone();
    let (observed_stale, stale_record) = integration.observe_report().unwrap();
    h.send("ReplicaClose", &open).await;
    h.receive("ReplicaCloseReply").await;
    open.local.instance += 1;
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle(GUID_BYTES).await;
    h.engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            assert_eq!(
                stale
                    .report_load(&[LoadMetric {
                        name: "Work".into(),
                        value: 1
                    }])
                    .unwrap_err()
                    .kind(),
                ErrorKind::ObjectClosed
            );
            assert_eq!(
                stale.report_move_cost(MoveCost::High).unwrap_err().kind(),
                ErrorKind::ObjectClosed
            );
            assert_eq!(
                stale.report_fault(FaultType::Permanent).unwrap_err().kind(),
                ErrorKind::ObjectClosed
            );
            assert_eq!(
                stale
                    .report_replica_health(&information())
                    .unwrap_err()
                    .kind(),
                ErrorKind::ObjectClosed
            );
            assert_eq!(
                observed_stale
                    .report_partition_health(&information())
                    .unwrap_err()
                    .kind(),
                ErrorKind::ObjectClosed
            );
            Ok(())
        })
        .await
        .unwrap();
    assert!(stale_record.result().unwrap().is_err());
    h.no_reply().await;
}

#[tokio::test]
async fn stateful_reporting_verified_ttl_sequence_validation_wrong_kind_and_uncertain_timeout() {
    let (mut h, calls, _) = opened(20, 4).await;
    let integration = calls.partitions.lock().unwrap()[0].clone();
    for ttl in [0, 3] {
        let (partition, record) = integration.observe_report().unwrap();
        let call = h
            .engine
            .shared
            .execution
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                let health = HealthInformation {
                    time_to_live_seconds: ttl,
                    sequence_number: 0,
                    ..information()
                };
                partition.report_replica_health2(&health, None)
            });
        let message = h.receive("ReportHealth").await;
        let sequence = assert_health(
            &message,
            1,
            if ttl == 0 { i64::MAX } else { 30_000_000 },
            None,
        );
        health_reply(&mut h, &message, 1, sequence, None).await;
        call.await.unwrap();
        assert_eq!(
            record.result().unwrap().unwrap().health_sequence,
            Some(sequence)
        );
    }
    for invalid in 0..6 {
        let (partition, record) = integration.observe_report().unwrap();
        let error = h
            .engine
            .shared
            .execution
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                let mut health = information();
                match invalid {
                    0 => health.sequence_number = -1,
                    1 => health.state = crate::HealthState::Invalid,
                    2 => health.source_id = "System.Forged".into(),
                    3 => health.property = "DeleteProperty".into(),
                    4 => health.description = "x".repeat(4097),
                    _ => health.source_id = "bad\0source".into(),
                }
                partition.report_partition_health(&health)
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(record.result().unwrap().is_err());
    }
    let reporting = integration.reporting_for_test();
    assert!(reporting
        .health(&health_report(), crate::reporting::HealthTarget::Instance)
        .await
        .is_err());
    // Independent core timer reaches the existing 30s health deadline. No reply,
    // no retry; the sync record reports uncertainty, never success or rollback.
    let (partition, record) = integration.observe_report().unwrap();
    let call = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || partition.report_replica_health(&information()));
    h.receive("ReportHealth").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    let error = without_advancing_time(call).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert!(error.is_uncertain());
    tokio::time::resume();
    assert!(record.result().unwrap().is_err());
    h.no_reply().await;
}

#[tokio::test]
async fn stateful_reporting_unknown_role_is_not_invented_primary_before_completed_change_role() {
    // Hold service ChangeRole(Primary), after native request says Primary.
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 17, 30);
    open.service.metrics.0.push(Metric {
        name: "Work".into(),
        weight: 1,
        primary: 0,
        secondary: 0,
    });
    h.send("ReplicaOpen", &open).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !calls.events.lock().unwrap().contains(&"servicePrimary") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let integration = calls.partitions.lock().unwrap()[0].clone();
    let (partition, _) = integration.observe_report().unwrap();
    let error = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || partition.report_move_cost(MoveCost::High))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidState);
    let reporting = integration.reporting_for_test();
    reporting.complete_service_role(ReplicaRole::None);
    assert_eq!(
        reporting
            .load(&[LoadMetric {
                name: "Work".into(),
                value: 1
            }])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidState
    );
    reporting.complete_service_role(ReplicaRole::Unknown);
    h.no_reply().await;
    calls.release.notify_one();
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let (partition, _) = integration.observe_report().unwrap();
    let call = h
        .engine
        .shared
        .execution
        .try_reserve()
        .unwrap()
        .run_owned(move || partition.report_move_cost(MoveCost::Low));
    let (result, message) = tokio::join!(call, h.receive("RAPReportLoad"));
    result.unwrap();
    let sent: LoadBody = from_slice(&message.body).unwrap();
    assert!(sent.reports.0[0].stateful);
    assert_eq!(sent.reports.0[0].primary.0[0].value, 1);
}
