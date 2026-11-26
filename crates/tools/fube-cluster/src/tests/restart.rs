use super::*;

#[tokio::test]
async fn manual_spare_replacement_uses_public_controls_and_semantic_waits() {
    let fixture = Fixture::new("manual-spare-replacement");
    let mut config = fixture.config();
    config.profile = ServiceProfile::Stateful {
        target: 3,
        minimum: 3,
    };
    config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
    let mut cluster = Cluster::with_node_pool(config, 4).unwrap();
    cluster
        .set_recovery_mode(crate::RecoveryMode::Manual)
        .unwrap();
    cluster
        .start_with_fm(&support::binary("fube-fm", "FUBE_FM_EXE"))
        .await
        .unwrap();
    let result = peer_idle_catch(async {
        let partition = cluster.status()["nodes"][0]["partition"]
            .as_str()
            .unwrap()
            .to_string();
        cluster.create_partition("create", &partition, 1).await?;
        cluster
            .wait_operation("create", Duration::from_secs(30))
            .await?;
        let original = cluster.status();
        let endpoint = original["nodes"][0]["endpoint"]
            .as_str()
            .unwrap()
            .to_string();
        until(&mut cluster, |_| {
            data_contact(&endpoint, b"read\n").starts_with("OK ")
        })
        .await;
        assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
        cluster.kill_node("kill-member", 3).await?;
        cluster
            .execute(command("down-member", "node-down", Some(3)))
            .await?;
        let predecessor: crate::ReplicaRef =
            serde_json::from_value(cluster.status()["node_failures"][0]["predecessor"].clone())
                .map_err(err)?;
        cluster
            .reconfigure_partition("reconfigure", &partition, predecessor.clone())
            .await?;
        cluster
            .wait_operation("reconfigure", Duration::from_secs(30))
            .await?;
        assert_eq!(cluster.status()["nodes"][3]["observed"], "absent");
        cluster
            .rebuild_replica("replace", &partition, predecessor, 4)
            .await?;
        cluster
            .wait_operation("replace", Duration::from_secs(30))
            .await?;
        let restored = cluster.status();
        assert_eq!(restored["replica_set"]["minimum"], 3);
        for node in [0, 1, 3] {
            assert_eq!(
                restored["nodes"][node]["node_pid"],
                original["nodes"][node]["node_pid"]
            );
            assert_eq!(
                restored["nodes"][node]["app_pid"],
                original["nodes"][node]["app_pid"]
            );
        }
        cluster.kill_node("kill-secondary", 2).await?;
        assert!(data_contact(&endpoint, b"write 2\n").contains("committed=2"));
        cluster
            .execute(command("down-secondary", "node-down", Some(2)))
            .await?;
        Ok::<(), String>(())
    })
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn owned_stop_and_kill_controls_never_retarget_a_restarted_node() {
    let fixture = Fixture::new("owned-node-controls");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster
        .set_recovery_mode(crate::RecoveryMode::Manual)
        .unwrap();
    cluster.start().await.unwrap();
    let result = peer_idle_catch(async {
        let original = cluster.status()["nodes"][0].clone();
        let partition = original["partition"].as_str().unwrap();
        let pid = cluster.manager.children[0].node_pid;
        cluster.manager.children[0].node_pid += 1;
        assert!(cluster.kill_node("foreign-owner", 1).await.is_err());
        cluster.manager.children[0].node_pid = pid;
        assert!(cluster.kill_node("zero-node", 0).await.is_err());
        cluster.create_partition("create", partition, 1).await?;
        cluster
            .wait_operation("create", Duration::from_secs(12))
            .await?;
        let stop = cluster.stop_node("stop", 1).await?;
        assert_eq!(stop["forced"], false);
        assert_eq!(stop["node_down"], "not_reported");
        assert!(cluster.status()["node_failures"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(cluster.wait_operation("stop", Duration::ZERO).await?, stop);
        cluster
            .execute(command("down", "node-down", Some(1)))
            .await?;
        assert_eq!(
            cluster.wait_operation("down", Duration::ZERO).await?["state"],
            "down"
        );
        cluster
            .execute(command("restart", "restart-node", Some(1)))
            .await?;
        let restarted = cluster.status()["nodes"][0].clone();
        assert_ne!(restarted["node_instance"], original["node_instance"]);
        assert_eq!(cluster.stop_node("stop", 1).await?, stop);
        assert!(cluster.kill_node("stop", 1).await.is_err());
        cluster
            .create_partition("after-replay", partition, 1)
            .await?;
        cluster
            .wait_operation("after-replay", Duration::from_secs(12))
            .await?;
        assert_eq!(
            cluster.status()["nodes"][0]["node_pid"],
            restarted["node_pid"]
        );
        let kill = cluster.kill_node("kill", 1).await?;
        assert_eq!(kill["forced"], true);
        assert_eq!(kill["node_down"], "not_reported");
        cluster
            .execute(command("down-again", "node-down", Some(1)))
            .await?;
        Ok::<(), String>(())
    })
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn operation_wait_deadline_does_not_cancel_or_complete_pending_work() {
    let fixture = Fixture::new("operation-wait-deadline");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster.manager.ledger.operations.push(fm::Operation {
        command: command("pending", "create", Some(1)).request().unwrap(),
        outcome: crate::OperationOutcome::new("pending", crate::OperationState::Pending),
    });
    assert!(cluster
        .wait_operation("pending", Duration::from_millis(10))
        .await
        .unwrap_err()
        .contains("does not cancel"));
    assert_eq!(cluster.manager.ledger.result("pending")["state"], "pending");
    assert!(cluster
        .wait_operation("missing", Duration::ZERO)
        .await
        .unwrap_err()
        .contains("not retained"));
    assert!(cluster
        .wait_operation("pending", Duration::MAX)
        .await
        .is_err());
    cluster.manager.ledger.complete(
        "pending",
        crate::OperationOutcome::new("pending", crate::OperationState::Complete),
    );
    assert_eq!(
        cluster
            .wait_operation("pending", Duration::ZERO)
            .await
            .unwrap()["state"],
        "complete"
    );
    assert_eq!(cluster.manager.ledger.operations.len(), 1);
    assert_eq!(cluster.shutdown().await["clean"], true);
}

#[tokio::test]
async fn operation_wait_reports_original_failure_uncertainty_and_cancellation() {
    let fixture = Fixture::new("operation-wait-failure");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    for state in [
        crate::OperationState::Failed,
        crate::OperationState::Unknown,
        crate::OperationState::Cancelled,
        crate::OperationState::Rejected,
    ] {
        let name = serde_json::to_value(state).unwrap();
        let name = name.as_str().unwrap();
        cluster.manager.ledger.operations.push(fm::Operation {
            command: command(name, "create", Some(1)).request().unwrap(),
            outcome: crate::OperationOutcome::new(name, state).with_error("original owner ended"),
        });
        let error = cluster
            .wait_operation(name, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(
            error.contains(name) && error.contains("original owner ended"),
            "{error}"
        );
    }
    assert_eq!(cluster.shutdown().await["clean"], true);
}

fn proof(ledger: &Ledger, index: usize) -> NodeDownReceipt {
    let p = &ledger.placements[index];
    NodeDownReceipt {
        deployment: p.launch.deployment.clone(),
        session: p.launch.session.clone(),
        node: p.launch.node.clone(),
        node_pid: 555,
        profile: p.profile.clone(),
        node_group_drained: true,
        app_group_drained: true,
    }
}

fn completed(body: &DoReconfiguration) -> ConfigurationReply {
    let mut reply = ConfigurationReply {
        unit: body.configuration.unit.clone(),
        replicas: body.configuration.replicas.clone(),
        error: 0,
    };
    reply.unit.previous = Epoch::default();
    reply.replicas.0.retain(|r| r.current_role >= 3);
    for replica in &mut reply.replicas.0 {
        replica.previous_role = 1;
        if body.configuration.unit.current.configuration & !0xffff_ffff
            != body.configuration.unit.previous.configuration & !0xffff_ffff
        {
            replica.first_lsn = 0;
            replica.last_lsn = 0;
        }
        replica.service_endpoint = if replica.current_role == 4 {
            "127.0.0.1:23459".into()
        } else {
            String::new()
        };
    }
    reply
}

#[test]
fn confirmed_node_down_reconfigures_without_the_dead_link_and_rejoin_is_incarnation_bound() {
    for lost in [0, 2] {
        let mut ledger = configured_ledger(3, 2);
        assert!(ledger
            .accept(command("unproved-down", "node-down", Some(1)))
            .is_err());
        let proof = proof(&ledger, lost);
        let reference = replica_ref(&ledger, lost);
        let partition = ledger.placements[lost].launch.partition.clone();
        ledger.lost(lost, "transport closure is only uncertainty".into());
        assert_eq!(ledger.placements[lost].observed, Observed::Unknown);
        assert!(ledger
            .start_primary_failover(Instant::now())
            .unwrap()
            .is_none());
        assert!(ledger
            .record_node_down(lost, proof.clone())
            .unwrap()
            .is_none());
        let Accepted::Reconfigure(request) = ledger
            .accept(Command {
                op: "reconfigure".into(),
                action: "reconfigure-partition".into(),
                partition: Some(partition),
                predecessor: Some(reference),
                ..Command::default()
            })
            .unwrap()
        else {
            panic!("explicit Node reconfiguration required");
        };
        assert_eq!(ledger.placements[lost].observed, Observed::Down);
        assert!(!ledger.placements[lost].link_alive);
        assert!(ledger.prepare_node_restart(lost).is_err());
        assert!(
            !request
                .configuration
                .replicas
                .0
                .iter()
                .find(|r| r.node == proof.node)
                .unwrap()
                .up
        );
        let primary = ledger.configuration_target_index();
        assert_ne!(primary, lost);
        ledger
            .configuration_reply(primary, completed(&request))
            .unwrap();
        assert_eq!(ledger.configuration_status()["ready"], true);
        let launch = ledger.prepare_node_restart(lost).unwrap();
        assert_eq!(launch.node.id, proof.node.id);
        assert!(launch.node.instance > proof.node.instance);
        let previous = ledger.placements[lost].launch.clone();
        assert_ne!(launch.directory, previous.directory);
        assert_ne!(launch.session, previous.session);
        ledger.install_node_restart(lost, launch.clone()).unwrap();
        let mut actual = profile(&launch);
        actual.service = launch.profile.service().into();
        actual.service_type = fixed_type_for(launch.profile);
        ledger.placements[lost].bootstrap = Bootstrap::Metadata;
        ledger.placements[lost].link_alive = true;
        ledger.placements[lost].admit_profile(actual).unwrap();
        assert!(ledger
            .record_node_down(lost, proof.clone())
            .unwrap()
            .is_none());
        assert_eq!(ledger.placements[lost].launch.node, launch.node);
        assert!(ledger.placements[lost].link_alive);
        assert!(ledger.record_node_down((lost + 1) % 3, proof).is_err());
        assert!(ledger.begin_node_replica_rebuild(lost, lost).unwrap());
        let build = ledger.repair.as_ref().unwrap().build.body.clone();
        let mut replica = build.replica.clone();
        replica.state = 2;
        replica.first_lsn = 0;
        replica.last_lsn = 0;
        replica.replication_endpoint = "127.0.0.1:23460".into();
        ledger
            .idle_reply(
                primary,
                AddReplicaReply {
                    unit: build.unit,
                    replica,
                    error: 0,
                },
            )
            .unwrap();
        let restore = ledger
            .start_repair_configuration(Instant::now())
            .unwrap()
            .unwrap();
        ledger
            .configuration_reply(primary, completed(&restore))
            .unwrap();
        assert_eq!(ledger.configuration_status()["ready"], true);
        assert_eq!(ledger.placements[lost].observed, Observed::Running);
        assert!(ledger.placements[lost].ready_replica.as_ref().unwrap().node == launch.node);
    }
}

#[test]
fn node_down_requires_explicit_metadata_and_never_fabricates_surviving_quorum() {
    let mut ledger = configured_ledger(3, 3);
    let first_proof = proof(&ledger, 0);
    assert!(ledger.record_node_down(0, first_proof).unwrap().is_none());
    assert_eq!(ledger.placements[0].observed, Observed::Down);
    assert_eq!(ledger.configuration_status()["ready"], false);
    assert!(ledger.prepare_node_restart(0).is_err());
    let request = ledger.reconfigure_node_down(0).unwrap().unwrap();
    assert_eq!(request.configuration.replicas.0.len(), 3);
    assert!(request
        .configuration
        .replicas
        .0
        .iter()
        .any(|replica| !replica.up));
    assert_eq!(
        ledger
            .configuration
            .as_ref()
            .unwrap()
            .body
            .configuration
            .unit
            .minimum,
        3
    );
    let mut ledger = configured_ledger(5, 3);
    let proof = proof(&ledger, 0);
    ledger.lost(1, "transport loss is not confirmed node death".into());
    assert!(ledger.record_node_down(0, proof).unwrap().is_none());
    assert!(ledger.reconfigure_node_down(0).is_err());
    assert_eq!(ledger.placements[1].observed, Observed::Unknown);
    assert!(ledger.placements[1].node_down.is_none());
    assert!(ledger.node_failures[0]
        .recovery_error
        .as_ref()
        .unwrap()
        .contains("every other member live"));
}

#[test]
fn unused_spare_node_failure_does_not_invalidate_ready_membership() {
    let mut ledger = configured_ledger_in_pool(3, 2, 4);
    let proof = proof(&ledger, 3);
    assert!(ledger.record_node_down(3, proof).unwrap().is_none());
    assert_eq!(ledger.configuration_status()["ready"], true);
    let successor = ledger.prepare_node_restart(3).unwrap();
    ledger.install_node_restart(3, successor).unwrap();
    assert_eq!(ledger.restart_desired(3).unwrap(), fm::Desired::Absent);
}

#[test]
fn scoped_control_schema_rejects_mixed_fields_and_unproved_failure() {
    let mut ledger = configured_ledger(3, 3);
    ledger.recovery_mode = crate::RecoveryMode::Manual;
    let predecessor = replica_ref(&ledger, 0);
    let partition = ledger.placements[0].launch.partition.clone();
    let request = Command {
        op: "rebuild".into(),
        action: "rebuild-replica".into(),
        node: Some(1),
        partition: Some(partition),
        predecessor: Some(predecessor),
        ..Command::default()
    };
    let bytes = serde_json::to_vec(&request).unwrap();
    assert_eq!(serde_json::from_slice::<Command>(&bytes).unwrap(), request);
    let mut mixed = request.clone();
    mixed.action = "node-down".into();
    assert!(mixed.validate().is_err());
    let mut missing = request.clone();
    missing.predecessor = None;
    assert!(missing.validate().is_err());
    ledger.lost(0, "socket closure is not a NodeDown receipt".into());
    assert!(ledger.accept(request).is_err());
    assert!(ledger.repair.is_none());
}

#[test]
fn manual_node_recovery_is_scoped_retained_and_preserves_minimum() {
    for lost in [0, 2] {
        let mut ledger = configured_ledger(3, 3);
        ledger.recovery_mode = crate::RecoveryMode::Manual;
        let partition = ledger.placements[lost].launch.partition.clone();
        let predecessor = replica_ref(&ledger, lost);
        let fence = proof(&ledger, lost);
        assert!(ledger
            .record_node_down(lost, fence.clone())
            .unwrap()
            .is_none());
        assert!(ledger.swap.is_none());
        assert!(ledger.repair.is_none());
        assert!(ledger.prepare_node_restart(lost).is_err());
        let reconfigure = Command {
            op: "reconfigure".into(),
            action: "reconfigure-partition".into(),
            partition: Some(partition.clone()),
            predecessor: Some(predecessor.clone()),
            ..Command::default()
        };
        let mut foreign = reconfigure.clone();
        foreign.partition = Some(uuid::Uuid::new_v4().to_string());
        assert!(ledger.accept(foreign).is_err());
        let mut stale = reconfigure.clone();
        stale.predecessor.as_mut().unwrap().node_instance += 1;
        assert!(ledger.accept(stale).is_err());
        let Accepted::Reconfigure(request) = ledger.accept(reconfigure.clone()).unwrap() else {
            panic!("explicit survivor reconfiguration required");
        };
        assert_eq!(request.configuration.unit.minimum, 3);
        assert_eq!(
            request
                .configuration
                .replicas
                .0
                .iter()
                .filter(|r| r.current_role >= 3)
                .count(),
            3
        );
        assert_eq!(
            request
                .configuration
                .replicas
                .0
                .iter()
                .filter(|r| r.up)
                .count(),
            2
        );
        assert!(matches!(
            ledger.accept(reconfigure.clone()).unwrap(),
            Accepted::Duplicate(_)
        ));
        let primary = ledger.configuration_target_index();
        ledger
            .configuration_reply(primary, completed(&request))
            .unwrap();
        ledger.refresh_scenario_operations();
        assert_eq!(ledger.result("reconfigure")["state"], "complete");
        let launch = ledger.prepare_node_restart(lost).unwrap();
        ledger.install_node_restart(lost, launch.clone()).unwrap();
        ledger.placements[lost].bootstrap = Bootstrap::Metadata;
        ledger.placements[lost].link_alive = true;
        let mut actual = profile(&launch);
        actual.service = launch.profile.service().into();
        actual.service_type = fixed_type_for(launch.profile);
        ledger.placements[lost].admit_profile(actual).unwrap();
        assert!(ledger.repair.is_none());
        let rebuild = Command {
            op: "rebuild".into(),
            action: "rebuild-replica".into(),
            node: Some((lost + 1) as u8),
            partition: Some(partition),
            predecessor: Some(predecessor),
            ..Command::default()
        };
        assert!(matches!(
            ledger.accept(rebuild.clone()).unwrap(),
            Accepted::Build
        ));
        let owner = ledger.repair.as_ref().unwrap().build.body.clone();
        assert!(matches!(
            ledger.accept(rebuild.clone()).unwrap(),
            Accepted::Duplicate(_)
        ));
        assert_eq!(ledger.repair.as_ref().unwrap().build.body, owner);
        let mut changed = rebuild.clone();
        changed.node = Some(((lost + 1) % 3 + 1) as u8);
        assert!(ledger.accept(changed).is_err());
        let mut replica = owner.replica.clone();
        replica.state = 2;
        replica.replication_endpoint = "127.0.0.1:23460".into();
        ledger
            .idle_reply(
                primary,
                AddReplicaReply {
                    unit: owner.unit,
                    replica,
                    error: 0,
                },
            )
            .unwrap();
        let admission = ledger
            .start_repair_configuration(Instant::now())
            .unwrap()
            .unwrap();
        assert_eq!(admission.configuration.unit.minimum, 3);
        assert_eq!(
            admission
                .configuration
                .replicas
                .0
                .iter()
                .filter(|r| r.current_role >= 3)
                .count(),
            3
        );
        ledger
            .configuration_reply(primary, completed(&admission))
            .unwrap();
        ledger.refresh_scenario_operations();
        assert_eq!(ledger.result("rebuild")["state"], "complete");
        assert_eq!(ledger.configuration_status()["ready"], true);
        let mut another = rebuild;
        another.op = "stale-rebuild".into();
        assert!(ledger.accept(another).is_err());
    }
}

#[test]
fn scoped_supersession_binds_pending_removal_and_failed_primary_without_reviving_lost_authority() {
    for lose_authority in [false, true] {
        let mut ledger = configured_ledger(5, 3);
        ledger.recovery_mode = crate::RecoveryMode::Manual;
        let partition = ledger.placements[0].launch.partition.clone();
        let secondary = replica_ref(&ledger, 1);
        let primary = replica_ref(&ledger, 0);
        for index in [1, 0] {
            let old = ledger.placements[index].body().unwrap();
            ledger
                .report(index, &stateful_loss_report(&ledger.placements[index]))
                .unwrap();
            ledger
                .reply(
                    index,
                    AddInstanceReply {
                        unit: old.unit,
                        replica: dropped_replica(&old.replica),
                        error: 0,
                    },
                    true,
                )
                .unwrap();
            if index == 1 {
                assert!(matches!(
                    ledger
                        .accept(Command {
                            op: "remove-secondary".into(),
                            action: "rebuild-replica".into(),
                            partition: Some(partition.clone()),
                            predecessor: Some(secondary.clone()),
                            node: Some(2),
                            ..Command::default()
                        })
                        .unwrap(),
                    Accepted::Reconfigure(_)
                ));
            }
        }
        ledger.refresh_scenario_operations();
        assert_eq!(ledger.result("remove-secondary")["state"], "unknown");
        let reconfigure = Command {
            op: "supersede-primary".into(),
            action: "reconfigure-partition".into(),
            partition: Some(partition),
            predecessor: Some(primary),
            operation: Some("remove-secondary".into()),
            ..Command::default()
        };
        let mut unbound = reconfigure.clone();
        unbound.operation = None;
        assert!(ledger.accept(unbound).is_err());
        let mut wrong = reconfigure.clone();
        wrong.predecessor = Some(secondary);
        assert!(ledger.accept(wrong).is_err());
        let mut missing = reconfigure.clone();
        missing.operation = Some("missing".into());
        assert!(ledger.accept(missing).is_err());
        assert!(ledger.swap.is_none());
        assert!(!ledger.repair.as_ref().unwrap().host_requested);
        if lose_authority {
            ledger.lost(4, "original control authority lost".into());
            assert!(ledger.accept(reconfigure).is_err());
            assert!(ledger.swap.is_none());
        } else {
            assert!(matches!(
                ledger.accept(reconfigure.clone()).unwrap(),
                Accepted::Reconfigure(_)
            ));
            assert_eq!(ledger.result("remove-secondary")["state"], "superseded");
            assert_eq!(
                ledger.result("remove-secondary")["superseded_by"],
                "supersede-primary"
            );
            assert!(matches!(
                ledger.accept(reconfigure).unwrap(),
                Accepted::Duplicate(_)
            ));
            assert_eq!(ledger.result("supersede-primary")["state"], "pending");
            assert!(!ledger.repair_history.last().unwrap().host_requested);
        }
    }
}

#[test]
fn scoped_cancellation_owns_the_exact_pending_rebuild() {
    let mut ledger = configured_ledger(3, 2);
    ledger.recovery_mode = crate::RecoveryMode::Manual;
    let predecessor = replica_ref(&ledger, 0);
    let partition = ledger.placements[0].launch.partition.clone();
    let old = ledger.placements[0].body().unwrap();
    ledger
        .report(0, &stateful_loss_report(&ledger.placements[0]))
        .unwrap();
    ledger
        .reply(
            0,
            AddInstanceReply {
                unit: old.unit,
                replica: dropped_replica(&old.replica),
                error: 0,
            },
            true,
        )
        .unwrap();
    let Accepted::Reconfigure(failover) = ledger
        .accept(Command {
            op: "failover".into(),
            action: "reconfigure-partition".into(),
            partition: Some(partition.clone()),
            predecessor: Some(predecessor.clone()),
            ..Command::default()
        })
        .unwrap()
    else {
        panic!("explicit failover must own native survivor reconfiguration");
    };
    let primary = ledger.configuration_target_index();
    ledger
        .configuration_reply(primary, completed(&failover))
        .unwrap();
    let rebuild = Command {
        op: "rebuild".into(),
        action: "rebuild-replica".into(),
        node: Some(1),
        partition: Some(partition),
        predecessor: Some(predecessor),
        ..Command::default()
    };
    assert!(matches!(ledger.accept(rebuild).unwrap(), Accepted::Build));
    let repair = ledger.repair.as_mut().unwrap();
    repair.profile = Some(repair.old_profile.clone());
    repair.host_requested = true;
    ledger.placements[0].loss_reported = false;
    let cancel = Command {
        op: "cancel".into(),
        action: "cancel-rebuild".into(),
        operation: Some("rebuild".into()),
        ..Command::default()
    };
    let mut wrong = cancel.clone();
    wrong.operation = Some("primary".into());
    assert!(ledger.accept(wrong).is_err());
    ledger
        .repair
        .as_mut()
        .unwrap()
        .build
        .body
        .replica
        .node
        .instance += 1;
    assert!(ledger.accept(cancel.clone()).is_err());
    ledger
        .repair
        .as_mut()
        .unwrap()
        .build
        .body
        .replica
        .node
        .instance -= 1;
    let Accepted::Cancel(removal) = ledger.accept(cancel.clone()).unwrap() else {
        panic!("native removal must own cancellation");
    };
    assert!(matches!(
        ledger.accept(cancel).unwrap(),
        Accepted::Duplicate(_)
    ));
    let mut removed = removal.replica;
    removed.state = 3;
    removed.replication_endpoint = "127.0.0.1:23460".into();
    ledger
        .idle_removal_reply(
            primary,
            RemoveReplicaReply {
                unit: removal.unit,
                replica: removed,
                error: 0,
            },
        )
        .unwrap();
    ledger.refresh_scenario_operations();
    assert_eq!(ledger.result("cancel")["state"], "complete");
    assert_eq!(ledger.result("rebuild")["state"], "cancelled");
    assert_eq!(ledger.repair_status()["destination_deleted"], false);
    assert_eq!(ledger.repair_status()["disposed"], false);
}

#[tokio::test]
async fn manual_stateless_rejoin_waits_for_partition_scoped_recreation() {
    let fixture = Fixture::new("manual-stateless-rejoin");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    cluster
        .set_recovery_mode(crate::RecoveryMode::Manual)
        .unwrap();
    cluster
        .start_with_fm(&support::binary("fube-fm", "FUBE_FM_EXE"))
        .await
        .unwrap();
    let result = peer_idle_catch(async {
        let partition = cluster.status()["nodes"][0]["partition"]
            .as_str()
            .unwrap()
            .to_string();
        cluster.create_partition("create", &partition, 1).await?;
        cluster
            .wait_operation("create", Duration::from_secs(12))
            .await?;
        let stopped = cluster.stop_node("stop", 1).await?;
        assert_eq!(stopped["forced"], false);
        assert_eq!(stopped["node_down"], "not_reported");
        assert!(cluster.status()["node_failures"]
            .as_array()
            .unwrap()
            .is_empty());
        cluster
            .execute(command("down", "node-down", Some(1)))
            .await?;
        let predecessor: crate::ReplicaRef =
            serde_json::from_value(cluster.status()["node_failures"][0]["predecessor"].clone())
                .map_err(err)?;
        let restart = cluster
            .execute(command("restart", "restart-node", Some(1)))
            .await?;
        assert_eq!(restart["state"], "complete");
        for _ in 0..10 {
            cluster.drive().await;
        }
        assert_eq!(cluster.status()["nodes"][0]["observed"], "absent");
        let other_partition = cluster.status()["nodes"][1]["partition"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(cluster
            .rebuild_replica("wrong-partition", &other_partition, predecessor.clone(), 1)
            .await
            .is_err());
        cluster
            .rebuild_replica("rebuild", &partition, predecessor.clone(), 1)
            .await?;
        cluster
            .wait_operation("rebuild", Duration::from_secs(12))
            .await?;
        assert_eq!(
            cluster
                .rebuild_replica("rebuild", &partition, predecessor, 1)
                .await?["state"],
            "complete"
        );
        assert!(cluster
            .execute(command("rebuild", "node-down", Some(1)))
            .await
            .is_err());
        Ok::<(), String>(())
    })
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn manual_minimum_sized_node_recovery_requires_each_control_and_preserves_data() {
    for (target, lost, separate_fm) in [(3, 2, false), (3, 0, true), (4, 1, false), (4, 0, true)] {
        let fixture = Fixture::new("manual-three-of-three-rejoin");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful {
            target,
            minimum: target,
        };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::new(config).unwrap();
        cluster
            .set_recovery_mode(crate::RecoveryMode::Manual)
            .unwrap();
        if separate_fm {
            cluster
                .start_with_fm(&support::binary("fube-fm", "FUBE_FM_EXE"))
                .await
                .unwrap();
        } else {
            cluster.start().await.unwrap();
        }
        let result = peer_idle_catch(async {
            assert!(cluster
                .set_recovery_mode(crate::RecoveryMode::Manual)
                .is_err());
            assert_eq!(cluster.status()["recovery_mode"], "manual");
            let partition = cluster.status()["nodes"][0]["partition"]
                .as_str()
                .unwrap()
                .to_string();
            cluster.create_partition("create", &partition, 1).await?;
            cluster
                .wait_operation("create", Duration::from_secs(30))
                .await?;
            let original = cluster.status();
            let endpoint = original["nodes"][0]["endpoint"]
                .as_str()
                .unwrap()
                .to_string();
            until(&mut cluster, |_| {
                data_contact(&endpoint, b"read\n").starts_with("OK ")
            })
            .await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
            until(&mut cluster, |c| {
                (1..target as usize).all(|i| {
                    c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]
                        ["provider"]["last"]
                        == 1
                })
            })
            .await;
            let node = (lost + 1) as u8;
            let killed = cluster.kill_node("kill", node).await?;
            assert_eq!(killed["node_down"], "not_reported");
            assert!(cluster.status()["node_failures"]
                .as_array()
                .unwrap()
                .is_empty());
            cluster
                .execute(command("down", "node-down", Some((lost + 1) as u8)))
                .await?;
            let predecessor: crate::ReplicaRef = serde_json::from_value(
                cluster.status()["node_failures"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|failure| {
                        failure["node"] == node && failure["instance"] == killed["node_instance"]
                    })
                    .unwrap()["predecessor"]
                    .clone(),
            )
            .map_err(err)?;
            for _ in 0..10 {
                cluster.drive().await;
            }
            assert_eq!(cluster.status()["repair"]["state"], "absent");
            assert_eq!(cluster.status()["configuration"]["ready"], false);
            cluster
                .reconfigure_partition("reconfigure", &partition, predecessor.clone())
                .await?;
            cluster
                .wait_operation("reconfigure", Duration::from_secs(30))
                .await?;
            let status = cluster.status();
            let primary = status["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .position(|node| node["endpoint"].as_str().is_some())
                .unwrap();
            let endpoint = status["nodes"][primary]["endpoint"]
                .as_str()
                .unwrap()
                .to_string();
            assert!(data_contact(&endpoint, b"read\n").contains("committed=1"));
            assert!(data_contact(&endpoint, b"write 2\n").contains("committed=2"));
            let restart = cluster
                .execute(command("restart", "restart-node", Some((lost + 1) as u8)))
                .await?;
            assert_eq!(restart["state"], "complete");
            assert_eq!(restart["replica_recovery"], "not_requested");
            for _ in 0..10 {
                cluster.drive().await;
            }
            assert_eq!(cluster.status()["repair"]["state"], "absent");
            assert_eq!(cluster.status()["nodes"][lost]["observed"], "absent");
            let rebuild = cluster
                .rebuild_replica("rebuild", &partition, predecessor.clone(), (lost + 1) as u8)
                .await?;
            assert_eq!(rebuild["state"], "pending");
            assert_eq!(
                cluster
                    .rebuild_replica("rebuild", &partition, predecessor, (lost + 1) as u8)
                    .await?["replica"],
                rebuild["replica"]
            );
            cluster
                .wait_operation("rebuild", Duration::from_secs(30))
                .await?;
            let status = cluster.status();
            assert_eq!(status["replica_set"]["minimum"], target);
            assert_eq!(
                status["operations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|op| op["op"] == "rebuild")
                    .unwrap()["state"],
                "complete"
            );
            assert!(data_contact(&endpoint, b"write 3\n").contains("committed=3"));
            if target == 3 && separate_fm {
                let bundled = cluster.restart_node(node).await?;
                cluster
                    .wait_operation(
                        bundled["rebuild_operation"]
                            .as_str()
                            .ok_or("bundled restart did not retain a rebuild operation")?,
                        Duration::from_secs(30),
                    )
                    .await?;
                let restarted = cluster.status()["nodes"][lost]["node_instance"].clone();
                assert_eq!(
                    cluster.kill_node("kill", node).await?["node_instance"],
                    killed["node_instance"]
                );
                assert_eq!(cluster.status()["nodes"][lost]["node_instance"], restarted);
                assert!(data_contact(&endpoint, b"read\n").contains("committed=3"));
            }
            for node in 0..target as usize {
                if node != lost {
                    assert_eq!(
                        status["nodes"][node]["node_pid"],
                        original["nodes"][node]["node_pid"]
                    );
                    assert_eq!(
                        status["nodes"][node]["app_pid"],
                        original["nodes"][node]["app_pid"]
                    );
                }
            }
            Ok::<(), String>(())
        })
        .await;
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn explicit_stateless_node_controls_preserve_fm_and_use_fresh_owners() {
    let fixture = Fixture::new("explicit-stateless-node-restart");
    let mut cluster = Cluster::new(fixture.config()).unwrap();
    let fm_executable = support::binary("fube-fm", "FUBE_FM_EXE");
    cluster.start_with_fm(&fm_executable).await.unwrap();
    let result = peer_idle_catch(async {
        cluster
            .execute(command("create-one", "create", Some(1)))
            .await?;
        until(&mut cluster, |c| {
            c.status()["nodes"][0]["observed"] == "running"
        })
        .await;
        let original = cluster.status();
        assert!(cluster
            .execute(command("create-one", "restart-node", Some(1)))
            .await
            .is_err());
        assert!(cluster
            .remote
            .as_mut()
            .unwrap()
            .request(management::Operation::FinishNodeRestart { index: usize::MAX })
            .await
            .is_err());
        assert_eq!(
            cluster.status()["nodes"][0]["node_pid"],
            original["nodes"][0]["node_pid"]
        );
        let child = &cluster.manager.children[0];
        let forged_live_fence = NodeDownReceipt {
            deployment: child.launch.deployment.clone(),
            session: child.launch.session.clone(),
            node: child.launch.node.clone(),
            node_pid: child.node_pid,
            profile: child.metadata.as_ref().unwrap().profile.clone(),
            node_group_drained: true,
            app_group_drained: true,
        };
        assert!(cluster
            .remote
            .as_mut()
            .unwrap()
            .request(management::Operation::NodeDown {
                index: 0,
                receipt: forged_live_fence
            })
            .await
            .is_err());
        assert_eq!(cluster.status()["nodes"][0]["observed"], "running");
        assert!(cluster
            .execute(command("live-down", "node-down", Some(1)))
            .await
            .is_err());
        cluster.processes[0]
            .as_mut()
            .unwrap()
            .cleanup()
            .map_err(err)?;
        until(&mut cluster, |c| {
            c.status()["nodes"][0]["observed"] == "unknown"
        })
        .await;
        let down = cluster
            .execute(command("down-one", "node-down", Some(1)))
            .await?;
        assert_eq!(down["state"], "down");
        assert_eq!(cluster.status()["nodes"][0]["observed"], "down");
        let stale_proof = cluster.fenced_nodes[0].clone();
        let partition = original["nodes"][0]["partition"]
            .as_str()
            .unwrap()
            .to_string();
        let predecessor =
            serde_json::from_value(cluster.status()["node_failures"][0]["predecessor"].clone())
                .map_err(err)?;
        assert_eq!(
            cluster
                .execute(command("down-one", "node-down", Some(1)))
                .await?,
            down
        );
        let restart = cluster
            .execute(command("restart-one", "restart-node", Some(1)))
            .await?;
        assert_eq!(cluster.status()["nodes"][0]["observed"], "absent");
        cluster
            .rebuild_replica("rebuild-one", &partition, predecessor, 1)
            .await?;
        cluster
            .wait_operation("rebuild-one", Duration::from_secs(30))
            .await?;
        let current = cluster.status();
        assert_eq!(current["fm"]["pid"], original["fm"]["pid"]);
        assert_ne!(
            current["nodes"][0]["node_pid"],
            original["nodes"][0]["node_pid"]
        );
        assert_ne!(
            current["nodes"][0]["app_pid"],
            original["nodes"][0]["app_pid"]
        );
        assert_eq!(
            current["nodes"][1]["node_pid"],
            original["nodes"][1]["node_pid"]
        );
        assert_eq!(restart["state"], "complete");
        assert_eq!(restart["replica_recovery"], "not_requested");
        assert_eq!(
            cluster
                .execute(command("restart-one", "restart-node", Some(1)))
                .await?["state"],
            "complete"
        );
        let replay = cluster
            .remote
            .as_mut()
            .unwrap()
            .request(management::Operation::NodeDown {
                index: 0,
                receipt: stale_proof.clone(),
            })
            .await?;
        assert_eq!(replay["duplicate"], true);
        assert_eq!(cluster.status()["nodes"][0]["observed"], "running");
        let mut foreign = stale_proof;
        foreign.session = uuid::Uuid::new_v4().to_string();
        assert!(cluster
            .remote
            .as_mut()
            .unwrap()
            .request(management::Operation::NodeDown {
                index: 0,
                receipt: foreign
            })
            .await
            .is_err());
        assert_eq!(
            cluster.status()["nodes"][0]["node_pid"],
            current["nodes"][0]["node_pid"]
        );
        assert!(cluster
            .execute(command("restart-one", "node-down", Some(1)))
            .await
            .is_err());
        assert!(cluster
            .execute(command("unfenced-restart", "restart-node", Some(1)))
            .await
            .is_err());
        cluster.stop_node("stop-live", 1).await?;
        cluster
            .execute(command("down-live", "node-down", Some(1)))
            .await?;
        cluster
            .execute(command("restart-live", "restart-node", Some(1)))
            .await?;
        let predecessor = serde_json::from_value(
            cluster.status()["node_failures"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()["predecessor"]
                .clone(),
        )
        .map_err(err)?;
        cluster
            .rebuild_replica("rebuild-live", &partition, predecessor, 1)
            .await?;
        cluster
            .wait_operation("rebuild-live", Duration::from_secs(30))
            .await?;
        assert_ne!(
            cluster.status()["nodes"][0]["node_pid"],
            current["nodes"][0]["node_pid"]
        );
        assert_eq!(cluster.status()["fm"]["pid"], original["fm"]["pid"]);
        Ok::<(), String>(())
    })
    .await;
    let cleanup = cluster.shutdown().await;
    assert_eq!(cleanup["clean"], true, "{cleanup}");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn explicit_primary_and_secondary_node_restarts_rebuild_data_without_restarting_survivors() {
    for (lost, separate_fm) in [(2, false), (0, true)] {
        let fixture = Fixture::new("explicit-replicated-node-restart");
        let mut config = fixture.config();
        config.profile = ServiceProfile::Stateful {
            target: 3,
            minimum: 2,
        };
        config.app = support::binary("hosting-local-app", "FUBE_CLUSTER_STATEFUL_APP");
        let mut cluster = Cluster::with_node_pool(config, if separate_fm { 3 } else { 4 }).unwrap();
        if separate_fm {
            cluster
                .start_with_fm(&support::binary("fube-fm", "FUBE_FM_EXE"))
                .await
                .unwrap();
        } else {
            cluster.start().await.unwrap();
        }
        let result = peer_idle_catch(async {
            cluster
                .execute(command("primary", "create", Some(1)))
                .await?;
            until(&mut cluster, |c| {
                c.status()["configuration"]["ready"] == true
            })
            .await;
            let original = cluster.status();
            let endpoint = original["nodes"][0]["endpoint"]
                .as_str()
                .unwrap()
                .to_string();
            until(&mut cluster, |_| {
                data_contact(&endpoint, b"read\n").starts_with("OK ")
            })
            .await;
            assert!(data_contact(&endpoint, b"write\n").contains("committed=1"));
            until(&mut cluster, |c| {
                (1..3).all(|i| {
                    c.status()["nodes"][i]["retained_work"]["app_observation"]["idle_data"]
                        ["provider"]["last"]
                        == 1
                })
            })
            .await;
            let old = cluster.manager.children[lost].launch.clone();
            cluster.processes[lost]
                .as_mut()
                .unwrap()
                .cleanup()
                .map_err(err)?;
            let down = cluster.node_down((lost + 1) as u8).await?;
            assert_eq!(down["state"], "down");
            let partition = original["nodes"][lost]["partition"]
                .as_str()
                .unwrap()
                .to_string();
            let predecessor =
                serde_json::from_value(cluster.status()["node_failures"][0]["predecessor"].clone())
                    .map_err(err)?;
            cluster
                .reconfigure_partition("reconfigure", &partition, predecessor)
                .await?;
            cluster
                .wait_operation("reconfigure", Duration::from_secs(30))
                .await?;
            let status = cluster.status();
            let primary = status["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .position(|n| n["endpoint"].as_str().is_some())
                .unwrap();
            assert_ne!(primary, lost);
            let endpoint = status["nodes"][primary]["endpoint"]
                .as_str()
                .unwrap()
                .to_string();
            assert!(data_contact(&endpoint, b"read\n").contains("committed=1"));
            assert!(data_contact(&endpoint, b"write 2\n").contains("committed=2"));
            let restarted = cluster.restart_node((lost + 1) as u8).await?;
            cluster
                .wait_operation(
                    restarted["rebuild_operation"].as_str().unwrap(),
                    Duration::from_secs(30),
                )
                .await?;
            let current = cluster.status();
            assert_eq!(current["configuration"]["ready"], true);
            assert_eq!(current["fm"], original["fm"]);
            for i in 0..3 {
                if i == lost {
                    assert_ne!(
                        current["nodes"][i]["node_pid"],
                        original["nodes"][i]["node_pid"]
                    );
                    assert_ne!(
                        current["nodes"][i]["app_pid"],
                        original["nodes"][i]["app_pid"]
                    );
                } else {
                    assert_eq!(
                        current["nodes"][i]["node_pid"],
                        original["nodes"][i]["node_pid"]
                    );
                    assert_eq!(
                        current["nodes"][i]["app_pid"],
                        original["nodes"][i]["app_pid"]
                    );
                }
            }
            assert!(cluster.manager.children[lost].launch.node.instance > old.node.instance);
            assert!(data_contact(&endpoint, b"write 3\n").contains("committed=3"));
            if !separate_fm {
                let first_successor = cluster.manager.children[lost].node_pid;
                cluster.restart_node((lost + 1) as u8).await?;
                until_with_budget(&mut cluster, Duration::from_secs(30), |c| {
                    c.status()["repair"]["ready"] == true
                })
                .await;
                assert_ne!(cluster.manager.children[lost].node_pid, first_successor);
                assert!(data_contact(&endpoint, b"write 4\n").contains("committed=4"));
                let spare = cluster.manager.children[3].node_pid;
                cluster.restart_node(4).await?;
                assert_ne!(cluster.manager.children[3].node_pid, spare);
                assert_eq!(cluster.status()["configuration"]["ready"], true);
                assert_eq!(cluster.status()["nodes"][3]["observed"], "absent");
                assert!(data_contact(&endpoint, b"write 5\n").contains("committed=5"));
            }
            Ok::<(), String>(())
        })
        .await;
        if !matches!(&result, Ok(Ok(()))) {
            eprintln!("NODE_RESTART_STATUS {}", cluster.status());
            for owner in cluster.processes.iter().flatten() {
                eprintln!("NODE_RESTART_LOG {:?}", bounded_logs(owner));
            }
        }
        let cleanup = cluster.shutdown().await;
        assert_eq!(cleanup["clean"], true, "{cleanup}");
        result.unwrap().unwrap();
    }
}
