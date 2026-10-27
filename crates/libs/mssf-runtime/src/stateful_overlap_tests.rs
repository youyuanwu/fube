use super::*;

#[tokio::test]
async fn cancel_before_catchup_install_survives_blocked_configuration_callback() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 23);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "catchupConfiguration", 1).await;
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
    calls.configuration_release.notify_one();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.no_reply().await;
    assert!(!calls.events.lock().unwrap().contains(&"waitQuorum"));
    h.send("CancelCatchupReplicaSet", &cancel).await;
    h.receive("CancelCatchupReplicaSetReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert!(calls.events.lock().unwrap().contains(&"waitQuorum"));
}

#[tokio::test]
async fn newer_configuration_while_waiting_does_not_authenticate_old_catchup_completion() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 21);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "waitQuorum", 1).await;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    let overlap: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!((overlap.error.error, overlap.flags), (0, 0));
    calls.catch_up_release.notify_one();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.no_reply().await;
    assert_eq!(
        calls.partitions.lock().unwrap()[0]
            .status()
            .unwrap()
            .catch_up,
        crate::CatchUpResult::NotStarted
    );
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "waitQuorum", 2).await;
    calls.catch_up_release.notify_one();
    let current: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!((current.error.error, current.flags), (0, 2));
}

#[tokio::test]
async fn primary_epoch_change_suppresses_old_build_reply_without_retiring_sibling() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let sibling = h.request(1, 1, 0);
    h.send("ReplicaOpen", &sibling).await;
    h.receive("ReplicaOpenReply").await;
    let open = request(&h, 2, 1, 22);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 2);
    target.current_role = 2;
    let build = control(&open, "ReplicatorBuildIdleReplica", vec![target]);
    h.send("ReplicatorBuildIdleReplica", &build).await;
    wait_event(&calls, "build", 1).await;
    let mut newer = control(&open, "ReplicatorUpdateEpochAndGetStatus", vec![]);
    newer.unit.current.configuration += 1 << 32;
    h.send("ReplicatorUpdateEpochAndGetStatus", &newer).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.engine.shared.stateful.revisions([2; 16]) != (1, 1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.no_reply().await; // UpdateEpoch API is incompatible with pending Build.
    assert_eq!(h.engine.shared.stateful.revisions([2; 16]), (1, 1));
    calls.build_release.notify_one();
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    h.no_reply().await; // The old primary epoch's build cannot publish success.
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
    h.send("ReplicatorUpdateEpochAndGetStatus", &newer).await;
    let reply: ProxyReply = from_slice(
        &h.receive("ReplicatorUpdateEpochAndGetStatusReply")
            .await
            .body,
    )
    .unwrap();
    assert_eq!(reply.error.error, 0);
}

#[tokio::test]
async fn abort_cancels_and_drains_build_before_per_replica_disposal_ack() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let sibling = h.request(1, 1, 0);
    h.send("ReplicaOpen", &sibling).await;
    h.receive("ReplicaOpenReply").await;
    let open = request(&h, 2, 1, 22);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 2);
    target.current_role = 2;
    let build = control(&open, "ReplicatorBuildIdleReplica", vec![target]);
    h.send("ReplicatorBuildIdleReplica", &build).await;
    wait_event(&calls, "build", 1).await;
    let mut close = open.clone();
    close.flags = 4;
    close.local.current_role = 1;
    h.send("ReplicaClose", &close).await;
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    h.no_reply().await;
    h.send("ReplicaClose", &close).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaCloseReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
    assert!(calls.partitions.lock().unwrap()[0].ensure_valid().is_err());
    assert_eq!(
        &calls.events.lock().unwrap()[6..],
        [
            "replicatorNone",
            "serviceNone",
            "replicatorAbort",
            "serviceAbort"
        ]
    );
}

#[tokio::test]
async fn query_observes_blocked_role_callback_without_protocol_locks_or_access_grant() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 1, 30);
    open.local.current_role = 2;
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut promote = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    promote.local.current_role = 4;
    promote.remote.0[0].current_role = 4;
    promote.unit.current.configuration += 1 << 32;
    promote.unit.previous = open.unit.current.clone();
    promote.flags = 2;
    h.send("UpdateConfiguration", &promote).await;
    wait_event(&calls, "servicePrimary", 1).await;
    let mut query = control(&promote, "RAPQuery", vec![]);
    query.runtime = open.runtime.clone();
    h.send("RAPQuery", &query).await;
    let active: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(
        (
            active.error,
            active.detail.operation,
            active.detail.replicator_operation
        ),
        (0, 4, 1)
    );
    assert_eq!(
        (active.detail.read_status, active.detail.write_status),
        (2, 2)
    );
    calls.release.notify_one();
    let mut notification = None;
    for _ in 0..2 {
        let frame = tokio::time::timeout(Duration::from_secs(2), read_message(&mut h.peer))
            .await
            .unwrap()
            .unwrap();
        if frame.properties().unwrap().action == "ProxyReplicaEndpointUpdated" {
            notification = Some(from_slice::<ReplicaMessage>(&frame.body).unwrap());
        } else {
            assert_eq!(
                frame.properties().unwrap().action,
                "UpdateConfigurationReply"
            );
        }
    }
    let mut ack = query;
    ack.local = notification.unwrap().local;
    h.send("ProxyReplicaEndpointUpdatedReply", &ack).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.engine.shared.stateful.endpoint_is_pending([2; 16]) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn query_observes_service_close_after_replicator_close_without_locking_callback() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 31);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("ReplicaClose", &open).await;
    wait_event(&calls, "serviceClose", 1).await;
    let query = control(&open, "RAPQuery", vec![]);
    h.send("RAPQuery", &query).await;
    let closing: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(
        (
            closing.error,
            closing.detail.operation,
            closing.detail.replicator_operation
        ),
        (0, 8, 1)
    );
    assert_eq!(
        (closing.detail.read_status, closing.detail.write_status),
        (3, 3)
    );
    calls.release.notify_one();
    h.receive("ReplicaCloseReply").await;
}

#[tokio::test]
async fn cancellation_is_not_acknowledged_until_an_uncooperative_future_actually_completes() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 33);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = 2;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "waitQuorum", 1).await;
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
    h.no_reply().await; // No dropping the wait future and calling that drain.
    calls.catch_up_release.notify_one();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.no_reply().await;
    h.send("CancelCatchupReplicaSet", &cancel).await;
    let drained: ProxyReply =
        from_slice(&h.receive("CancelCatchupReplicaSetReply").await.body).unwrap();
    assert_eq!(drained.error.error, 0);
}
