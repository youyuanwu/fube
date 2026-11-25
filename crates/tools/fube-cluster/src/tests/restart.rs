use super::*;

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
        ledger.lost(lost, "transport closure is only uncertainty".into());
        assert_eq!(ledger.placements[lost].observed, Observed::Unknown);
        assert!(ledger
            .start_primary_failover(Instant::now())
            .unwrap()
            .is_none());
        let request = ledger
            .record_node_down(lost, proof.clone())
            .unwrap()
            .unwrap();
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
        assert!(ledger.begin_rejoined_replica(lost).unwrap());
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
fn node_down_without_surviving_minimum_records_failure_but_never_fabricates_recovery() {
    let mut ledger = configured_ledger(3, 3);
    let first_proof = proof(&ledger, 0);
    assert!(ledger.record_node_down(0, first_proof).unwrap().is_none());
    assert_eq!(ledger.placements[0].observed, Observed::Down);
    assert_eq!(ledger.configuration_status()["ready"], false);
    assert!(ledger.node_failures[0]
        .recovery_error
        .as_ref()
        .unwrap()
        .contains("quorum/minimum"));
    assert!(ledger.prepare_node_restart(0).is_err());
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
        assert_eq!(
            cluster
                .execute(command("down-one", "node-down", Some(1)))
                .await?,
            down
        );
        let restart = cluster
            .execute(command("restart-one", "restart-node", Some(1)))
            .await?;
        until(&mut cluster, |c| {
            c.status()["nodes"][0]["observed"] == "running"
        })
        .await;
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
        assert_eq!(restart["state"], "pending");
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
        cluster
            .execute(command("restart-live", "restart-node", Some(1)))
            .await?;
        until(&mut cluster, |c| {
            c.status()["nodes"][0]["observed"] == "running"
        })
        .await;
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
            until(&mut cluster, |c| {
                c.status()["configuration"]["ready"] == true
            })
            .await;
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
            cluster.restart_node((lost + 1) as u8).await?;
            until_with_budget(&mut cluster, Duration::from_secs(30), |c| {
                c.status()["repair"]["ready"] == true
            })
            .await;
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
