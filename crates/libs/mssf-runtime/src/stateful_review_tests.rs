use super::*;

async fn caught_up_with_built_peer(
    h: &mut Harness,
    mode: u8,
) -> (Arc<StatefulCalls>, ProxyRequest, ProxyRequest) {
    let calls = register(h);
    let open = request(h, 2, 1, mode);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 2);
    target.previous_role = 1;
    let mut idle = target.clone();
    idle.current_role = 2;
    let build = control(&open, "ReplicatorBuildIdleReplica", vec![idle]);
    h.send("ReplicatorBuildIdleReplica", &build).await;
    h.receive("ReplicatorBuildIdleReplicaReply").await;
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    let mut update = control(
        &open,
        "UpdateConfiguration",
        vec![open.local.clone(), target],
    );
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    h.receive("UpdateConfigurationReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    (calls, open, update)
}

#[tokio::test]
async fn successful_remove_revokes_cached_configuration_proof_without_shrinking_membership() {
    let mut h = Harness::new().await;
    let (calls, open, mut update) = caught_up_with_built_peer(&mut h, 36).await;
    let mut removed = update.remote.0[1].clone();
    removed.current_role = 2;
    let remove = control(&update, "ReplicatorRemoveIdleReplica", vec![removed]);
    h.send("ReplicatorRemoveIdleReplica", &remove).await;
    let reply: ProxyReply =
        from_slice(&h.receive("ReplicatorRemoveIdleReplicaReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    calls.configuration_failure.store(true, Ordering::Release);
    h.send("UpdateConfiguration", &update).await;
    let failed: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(failed.error.error, -994);
    assert_eq!(calls.configurations.lock().unwrap().len(), 2);
    assert_eq!(
        calls.partitions.lock().unwrap()[0]
            .status()
            .unwrap()
            .configuration
            .len(),
        1
    );
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    calls.configuration_failure.store(false, Ordering::Release);
    update.remote.0[1].instance += 1;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    let new: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(new.error.error, 0);
    assert_eq!(new.unit.id.id, open.unit.id.id);
}

#[tokio::test]
async fn unchanged_projected_proof_does_not_hide_changed_native_observations_and_access() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    h.receive("UpdateConfigurationReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut dropped = peer(&open.local, 2);
    dropped.up = false;
    dropped.state = 3;
    dropped.previous_role = 1;
    update.remote.0.push(dropped);
    h.send("UpdateConfiguration", &update).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    let snapshot = calls.partitions.lock().unwrap()[0].status().unwrap();
    assert_eq!(snapshot.configuration.len(), 1);
    assert_eq!(snapshot.access().1, crate::AccessStatus::NoWriteQuorum);
    assert_eq!(calls.configurations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn late_configuration_during_remove_cannot_reinstall_pre_removal_receipt() {
    let mut h = Harness::new().await;
    let (calls, _, update) = caught_up_with_built_peer(&mut h, 37).await;
    let mut removed = update.remote.0[1].clone();
    removed.current_role = 2;
    let remove = control(&update, "ReplicatorRemoveIdleReplica", vec![removed]);
    h.send("ReplicatorRemoveIdleReplica", &remove).await;
    wait_event(&calls, "remove", 1).await;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "catchupConfiguration", 2).await;
    calls.remove_release.notify_one();
    h.receive("ReplicatorRemoveIdleReplicaReply").await;
    calls.configuration_release.notify_one();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.no_reply().await;
    calls.configuration_failure.store(true, Ordering::Release);
    h.send("UpdateConfiguration", &update).await;
    let failed: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(failed.error.error, -994);
    assert_eq!(calls.configurations.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn failed_configuration_observation_is_not_a_cached_success_receipt() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 36);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    h.receive("UpdateConfigurationReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    update.remote.0.push(peer(&open.local, 2));
    calls.configuration_failure.store(true, Ordering::Release);
    for _ in 0..2 {
        h.send("UpdateConfiguration", &update).await;
        let failed: ProxyReply =
            from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
        assert_eq!(failed.error.error, -994);
        h.engine.shared.stateful.wait_idle([2; 16]).await;
    }
    assert_eq!(calls.configurations.lock().unwrap().len(), 3);
    calls.configuration_failure.store(false, Ordering::Release);
    h.send("UpdateConfiguration", &update).await;
    let verified: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(verified.error.error, 0);
    assert_eq!(calls.configurations.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn same_epoch_demotion_retry_never_regrants_revoked_write_access() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 34);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 2);
    target.current_role = 4;
    let mut local = open.local.clone();
    local.current_role = 3;
    local.previous_role = 4;
    let mut demote = control(&open, "UpdateConfiguration", vec![local.clone(), target]);
    demote.local = local;
    demote.unit.current.configuration += 1 << 32;
    demote.unit.previous = open.unit.current.clone();
    demote.flags = 2;
    h.send("UpdateConfiguration", &demote).await;
    let failed: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(failed.error.error, -996);
    assert_eq!((failed.error.api.api, failed.error.api.interface), (6, 4));
    let partition = calls.partitions.lock().unwrap()[0].clone();
    assert_eq!(partition.access_status().unwrap(), (G, T));
    let before = calls.access_events.lock().unwrap().len();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("UpdateConfiguration", &demote).await;
    let retried: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(retried.error.error, 0);
    assert_eq!(partition.access_status().unwrap(), (N, N));
    assert!(calls.access_events.lock().unwrap()[before..]
        .iter()
        .all(|(_, (_, write))| *write != G));
}

#[tokio::test]
async fn cancellation_drains_overlapping_configuration_and_suppresses_its_late_success() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let sibling = h.request(1, 1, 0);
    h.send("ReplicaOpen", &sibling).await;
    h.receive("ReplicaOpenReply").await;
    let open = request(&h, 2, 1, 35);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "waitQuorum", 1).await;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "catchupConfiguration", 2).await;
    assert!(h.engine.shared.stateful.configuring([2; 16]));
    let mut cancel = control(&update, "CancelCatchupReplicaSet", vec![]);
    cancel.runtime = open.runtime.clone();
    h.send("CancelCatchupReplicaSet", &cancel).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !h.engine.shared.stateful.catchup_is_cancelled([2; 16]) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.engine.shared.stateful.wait_idle([2; 16]).await; // Wait callback truly drained.
    h.no_reply().await;
    h.send("CancelCatchupReplicaSet", &cancel).await;
    h.no_reply().await; // Configuration callback still owns its operation.
    assert!(h.engine.shared.stateful.configuring([2; 16]));
    calls.configuration_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.engine.shared.stateful.configuring([2; 16]) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.no_reply().await; // No successful UC reply after cancellation.
    h.send("CancelCatchupReplicaSet", &cancel).await;
    let drained: ProxyReply =
        from_slice(&h.receive("CancelCatchupReplicaSetReply").await.body).unwrap();
    assert_eq!(drained.error.error, 0);
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
}
