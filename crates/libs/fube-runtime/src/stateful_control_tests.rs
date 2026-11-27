use super::*;
use fube_core::protocol::NativeApiName;
use fube_core::protocol::NativeInterfaceName;
use fube_core::protocol::NativeProxyFlags;
use fube_core::protocol::{NativeReplicaRole, NativeReplicaState};
#[path = "stateful_binding_tests.rs"]
mod binding_tests;
#[path = "stateful_overlap_tests.rs"]
mod overlap_tests;
#[path = "stateful_review_tests.rs"]
mod review_tests;
use crate::ServicePartitionAccessStatus::{
    Granted as G, NotPrimary as N, ReconfigurationPending as T,
};

#[tokio::test]
async fn native_proxy_flags_round_trip_without_widening_action_admission() {
    #[derive(serde::Serialize)]
    struct RawRequest<'a> {
        runtime: &'a str,
        unit: &'a FailoverUnit,
        local: &'a Replica,
        remote: &'a ObjectArray<Vec<Replica>>,
        service: &'a Service,
        flags: i64,
    }
    let h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    for action in [
        RapAction::ReplicaOpen,
        RapAction::ReplicaClose,
        RapAction::UpdateService,
        RapAction::UpdateConfiguration,
        RapAction::BuildIdleReplica,
        RapAction::RemoveIdleReplica,
        RapAction::GetStatus,
        RapAction::UpdateEpochAndGetStatus,
        RapAction::CancelCatchupReplicaSet,
        RapAction::EndpointUpdatedReply,
    ] {
        let request = match action {
            RapAction::ReplicaOpen | RapAction::ReplicaClose | RapAction::UpdateService => {
                open.clone()
            }
            _ => control(
                &open,
                action.action(),
                if matches!(
                    action,
                    RapAction::UpdateConfiguration
                        | RapAction::BuildIdleReplica
                        | RapAction::RemoveIdleReplica
                ) {
                    vec![open.local.clone()]
                } else {
                    vec![]
                },
            ),
        };
        for flags in [16, 18, i64::MIN, i64::MIN | 4, -1] {
            let bytes = to_vec(&RawRequest {
                runtime: &request.runtime,
                unit: &request.unit,
                local: &request.local,
                remote: &request.remote,
                service: &request.service,
                flags,
            })
            .unwrap();
            let decoded: ProxyRequest = from_slice(&bytes).unwrap();
            assert_eq!(decoded.flags.bits(), flags);
            assert_eq!(to_vec(&decoded).unwrap(), bytes);
            assert!(
                matches!(
                    h.engine.shared.stateful.dispatch(action, decoded).await,
                    Err(Error::Protocol(_))
                ),
                "{}: flags {flags}",
                action.action()
            );
        }
    }
    for (flags, admitted) in [
        (NativeProxyFlags::NONE, true),
        (NativeProxyFlags::END_RECONFIGURATION, true),
        (NativeProxyFlags::CATCH_UP, true),
        (
            NativeProxyFlags::END_RECONFIGURATION | NativeProxyFlags::CATCH_UP,
            true,
        ),
        (NativeProxyFlags::ABORT, false),
        (NativeProxyFlags::DROP, false),
    ] {
        let mut request = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
        request.flags = flags;
        let result = h
            .engine
            .shared
            .stateful
            .dispatch(RapAction::UpdateConfiguration, request)
            .await;
        assert_eq!(result.is_ok(), admitted, "{flags:?}");
    }
    let mut query = open;
    query.flags = NativeProxyFlags::from_bits_retain(i64::MIN);
    assert!(matches!(
        h.engine.shared.stateful.query_reply(&query),
        Err(Error::Protocol(_))
    ));
    assert!(calls.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn configuration_allows_only_fenced_old_node_and_fresh_replacement_across_pc_cc() {
    for invalid in [
        None,
        Some("live-predecessor"),
        Some("same-node-instance"),
        Some("multiple-predecessors"),
        Some("unrecognized-role"),
        Some("unrecognized-state"),
    ] {
        let mut h = Harness::new().await;
        let _calls = register(&h);
        let open = request(&h, 2, 1, 20);
        h.send("ReplicaOpen", &open).await;
        h.receive("ReplicaOpenReply").await;
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        let mut old = peer(&open.local, 3);
        old.current_role = NativeReplicaRole::None;
        old.state = NativeReplicaState::Dropped;
        old.up = false;
        let mut fresh = old.clone();
        fresh.node.instance += 1;
        fresh.id += 1;
        fresh.instance += 1;
        fresh.previous_role = NativeReplicaRole::Idle;
        fresh.current_role = NativeReplicaRole::Secondary;
        fresh.state = NativeReplicaState::Ready;
        fresh.up = true;
        if invalid == Some("live-predecessor") {
            old.up = true;
            old.state = NativeReplicaState::Ready;
        } else if invalid == Some("same-node-instance") {
            fresh.node.instance = old.node.instance;
        } else if invalid == Some("multiple-predecessors") {
            fresh.node.instance += 1;
            fresh.id += 1;
            fresh.instance += 1;
        } else if invalid == Some("unrecognized-role") {
            fresh.current_role = NativeReplicaRole::Unrecognized(99);
        } else if invalid == Some("unrecognized-state") {
            fresh.state = NativeReplicaState::Unrecognized(99);
        }
        let mut configuration = control(
            &open,
            "UpdateConfiguration",
            vec![open.local.clone(), peer(&open.local, 4), old.clone(), fresh],
        );
        if invalid == Some("multiple-predecessors") {
            old.node.instance += 1;
            old.id += 1;
            old.instance += 1;
            configuration.remote.0.push(old);
        }
        configuration.unit.previous = open.unit.current.clone();
        configuration.unit.current.configuration += 1;
        configuration.flags = NativeProxyFlags::CATCH_UP;
        h.send("UpdateConfiguration", &configuration).await;
        let reply: ProxyReply =
            from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
        assert_eq!(
            reply.error.error == 0,
            invalid.is_none(),
            "{invalid:?}: {reply:?}"
        );
    }
}

pub(super) fn control(request: &ProxyRequest, action: &str, remotes: Vec<Replica>) -> ProxyRequest {
    let mut result = request.clone();
    result.service = native_default_service(result.service);
    if matches!(
        action,
        "UpdateConfiguration" | "ReplicatorBuildIdleReplica" | "ReplicatorRemoveIdleReplica"
    ) {
        result.runtime.clear();
    }
    result.remote = ObjectArray(remotes);
    result.flags = NativeProxyFlags::NONE;
    result
}
pub(super) fn peer(local: &Replica, node: u8) -> Replica {
    let mut replica = local.clone();
    replica.node.id.0 = vec![node; 16];
    replica.id = node as i64 * 100;
    replica.instance = 1;
    replica.current_role = NativeReplicaRole::Secondary;
    replica.previous_role = NativeReplicaRole::Secondary;
    replica.state = NativeReplicaState::Ready;
    replica.first_lsn = 0;
    replica.last_lsn = 0;
    replica.replication_endpoint = format!("model://peer/{node}");
    replica
}
pub(super) async fn wait_event(calls: &StatefulCalls, event: &str, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if calls
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == event)
                .count()
                >= count
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn default_metadata_configuration_projection_and_separate_access_quorums() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut dropped = peer(&open.local, 3);
    dropped.state = NativeReplicaState::Dropped;
    dropped.up = false;
    let mut other = peer(&open.local, 2);
    other.state = NativeReplicaState::Dropped;
    other.up = false;
    let mut current = control(
        &open,
        "UpdateConfiguration",
        vec![open.local.clone(), other, dropped],
    );
    current.unit.current.configuration += 1;
    current.unit.previous = open.unit.current.clone();
    current.flags = NativeProxyFlags::CATCH_UP;
    h.send("UpdateConfiguration", &current).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(
        (reply.error.error, reply.flags),
        (0, NativeProxyFlags::CATCH_UP)
    );
    let configurations = calls.configurations.lock().unwrap();
    let (cc, pc) = &configurations[0];
    assert_eq!(cc.write_quorum, 1); // Dropped clamps replicator configuration.
    assert!(cc.local_member);
    assert!(cc.replicas.is_empty());
    assert_eq!(pc.as_ref().unwrap().write_quorum, 0); // No PC local role was supplied.
    assert!(!pc.as_ref().unwrap().local_member);
    drop(configurations);
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (G, crate::ServicePartitionAccessStatus::NoWriteQuorum)
    ); // Access majority is NOT clamped.
    assert_eq!(
        calls
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "waitQuorum")
            .count(),
        1
    );
}

#[tokio::test]
async fn same_role_new_primary_epoch_and_status_have_real_initialized_progress() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut status = control(&open, "ReplicatorUpdateEpochAndGetStatus", vec![]);
    status.unit.current.configuration += 1 << 32;
    status.unit.previous = open.unit.current.clone();
    h.send("ReplicatorUpdateEpochAndGetStatus", &status).await;
    let reply: ProxyReply = from_slice(
        &h.receive("ReplicatorUpdateEpochAndGetStatusReply")
            .await
            .body,
    )
    .unwrap();
    assert_eq!(reply.error.error, 0);
    assert_eq!((reply.local.first_lsn, reply.local.last_lsn), (0, 0));
    assert_eq!(
        *calls.epochs.lock().unwrap(),
        vec![
            crate::Epoch {
                configuration_number: 1 << 32,
                data_loss_number: 1
            },
            crate::Epoch {
                configuration_number: 2 << 32,
                data_loss_number: 1
            },
        ]
    );
    assert_eq!(&calls.events.lock().unwrap()[5..], ["epoch", "progress"]);
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("ReplicatorUpdateEpochAndGetStatus", &status).await;
    h.receive("ReplicatorUpdateEpochAndGetStatusReply").await;
    assert_eq!(
        calls
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "epoch")
            .count(),
        1
    );
}

#[tokio::test]
async fn demotion_orders_pre_revoke_all_epoch_all_role_and_progress() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 2);
    target.current_role = NativeReplicaRole::Primary;
    let mut local = open.local.clone();
    local.current_role = NativeReplicaRole::Secondary;
    local.previous_role = NativeReplicaRole::Primary;
    let mut demote = control(&open, "UpdateConfiguration", vec![local.clone(), target]);
    demote.local = local;
    demote.unit.current.configuration += 1 << 32;
    demote.unit.previous = open.unit.current.clone();
    demote.flags = NativeProxyFlags::CATCH_UP;
    h.send("UpdateConfiguration", &demote).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert_eq!((reply.local.first_lsn, reply.local.last_lsn), (0, 0));
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        [
            "catchupConfiguration",
            "waitAll",
            "epoch",
            "catchupConfiguration",
            "waitAll",
            "replicatorSecondary",
            "serviceSecondary",
            "progress"
        ]
    );
    {
        let access = calls.access_events.lock().unwrap();
        let waits: Vec<_> = access
            .iter()
            .filter(|(event, _)| *event == "waitAll")
            .map(|(_, access)| *access)
            .collect();
        assert_eq!(waits, vec![(G, G), (G, T)]);
        assert!(access
            .iter()
            .any(|(event, access)| *event == "replicatorSecondary" && *access == (N, N)));
    }
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (N, N)
    );
    {
        let configs = calls.configurations.lock().unwrap();
        assert!(configs[0].0.replicas[0].must_catch_up);
        assert_eq!(configs[0].0.replicas[0].role, ReplicaRole::ActiveSecondary);
    }
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let cancel = control(&demote, "CancelCatchupReplicaSet", vec![]);
    // The full-runtime status/cancel constructors do not use UC's empty runtime.
    let mut cancel = cancel;
    cancel.runtime = open.runtime.clone();
    h.send("CancelCatchupReplicaSet", &cancel).await;
    let reply: ProxyReply =
        from_slice(&h.receive("CancelCatchupReplicaSetReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0x80071d24u32 as i32 as i64);
    assert_eq!(
        (reply.error.api.api, reply.error.api.interface),
        (NativeApiName::Invalid, NativeInterfaceName::Invalid)
    );
}

#[tokio::test]
async fn demotion_retries_during_catchup_do_not_acknowledge_an_unapplied_role() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 21);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 3);
    target.current_role = NativeReplicaRole::Primary;
    let mut local = open.local.clone();
    local.current_role = NativeReplicaRole::Secondary;
    local.previous_role = NativeReplicaRole::Primary;
    let mut demote = control(&open, "UpdateConfiguration", vec![local.clone(), target]);
    demote.local = local;
    demote.unit.current.configuration += 1 << 32;
    demote.unit.previous = open.unit.current.clone();
    demote.flags = NativeProxyFlags::CATCH_UP;
    h.send("UpdateConfiguration", &demote).await;
    wait_event(&calls, "waitAll", 1).await;
    h.send("UpdateConfiguration", &demote).await;
    let first = tokio::time::timeout(Duration::from_millis(30), h.peer.receive()).await;
    calls.catch_up_release.notify_one();
    wait_event(&calls, "waitAll", 2).await;
    h.send("UpdateConfiguration", &demote).await;
    let second = tokio::time::timeout(Duration::from_millis(30), h.peer.receive()).await;
    calls.catch_up_release.notify_one();
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.engine.quiesce().await.unwrap();
    assert!(
        first.is_err() && second.is_err(),
        "demotion was acknowledged during catch-up"
    );
    assert_eq!(
        (reply.error.error, reply.flags),
        (0, NativeProxyFlags::CATCH_UP)
    );
    let events = calls.events.lock().unwrap();
    assert!(events.contains(&"replicatorSecondary") && events.contains(&"serviceSecondary"));
}

#[tokio::test]
async fn configuration_can_overlap_catchup_but_cancel_drains_before_ack_and_new_epoch_reopens() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 21);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = NativeProxyFlags::CATCH_UP;
    update.unit.current.configuration += 1;
    update.unit.previous = open.unit.current.clone();
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "waitQuorum", 1).await;
    h.send("UpdateConfiguration", &update).await;
    let overlapping: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(
        (overlapping.error.error, overlapping.flags),
        (0, NativeProxyFlags::NONE)
    );
    assert_eq!(
        calls
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "waitQuorum")
            .count(),
        1
    );
    let mut cancel = control(&update, "CancelCatchupReplicaSet", vec![]);
    cancel.runtime = open.runtime.clone();
    h.send("CancelCatchupReplicaSet", &cancel).await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.no_reply().await; // Cancellation is not proof of completed work.
    h.send("CancelCatchupReplicaSet", &cancel).await;
    let cancelled: ProxyReply =
        from_slice(&h.receive("CancelCatchupReplicaSetReply").await.body).unwrap();
    assert_eq!(cancelled.error.error, 0);
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("UpdateConfiguration", &update).await;
    h.no_reply().await;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    wait_event(&calls, "waitQuorum", 2).await;
    calls.catch_up_release.notify_one();
    let resumed: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(
        (resumed.error.error, resumed.flags),
        (0, NativeProxyFlags::CATCH_UP)
    );
}

#[tokio::test]
async fn concurrent_builds_remove_cancel_and_tombstones_preserve_remote_incarnations() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 22);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut a = peer(&open.local, 2);
    a.current_role = NativeReplicaRole::Idle;
    let mut b = peer(&open.local, 3);
    b.current_role = NativeReplicaRole::Idle;
    let build_a = control(&open, "ReplicatorBuildIdleReplica", vec![a.clone()]);
    let build_b = control(&open, "ReplicatorBuildIdleReplica", vec![b]);
    h.send("ReplicatorBuildIdleReplica", &build_a).await;
    h.send("ReplicatorBuildIdleReplica", &build_b).await;
    wait_event(&calls, "build", 2).await;
    let query = control(&open, "RAPQuery", vec![]);
    h.send("RAPQuery", &query).await;
    let active: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(active.detail.operation, NativeServiceOperation::None);
    assert_eq!(
        active.detail.replicator_operation,
        NativeReplicatorOperation::BUILD
    );
    let remove = control(&open, "ReplicatorRemoveIdleReplica", vec![a]);
    h.send("ReplicatorRemoveIdleReplica", &remove).await;
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    h.no_reply().await;
    h.send("ReplicatorRemoveIdleReplica", &remove).await;
    let removed: ProxyReply =
        from_slice(&h.receive("ReplicatorRemoveIdleReplicaReply").await.body).unwrap();
    assert_eq!(removed.error.error, 0);
    assert_eq!(
        (removed.remote.0[0].id, removed.remote.0[0].instance),
        (200, 1)
    );
    assert_eq!(
        *calls.removals.lock().unwrap(),
        [crate::ReplicaRemoval::Idle]
    );
    calls.build_release.notify_waiters();
    let built: ProxyReply =
        from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
    assert_eq!((built.error.error, built.remote.0[0].id), (0, 300));
    h.send("ReplicatorBuildIdleReplica", &build_a).await;
    h.no_reply().await;
    assert_eq!(calls.builds.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn idle_removal_before_build_is_idempotent_and_suppresses_delayed_build() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut idle = peer(&open.local, 2);
    idle.current_role = NativeReplicaRole::Idle;
    idle.state = NativeReplicaState::Dropped;
    let remove = control(&open, "ReplicatorRemoveIdleReplica", vec![idle.clone()]);
    for _ in 0..2 {
        h.send("ReplicatorRemoveIdleReplica", &remove).await;
        let reply: ProxyReply =
            from_slice(&h.receive("ReplicatorRemoveIdleReplicaReply").await.body).unwrap();
        assert_eq!(reply.error.error, 0);
        assert_eq!(reply.remote.0.len(), 1);
        assert_eq!(
            (
                reply.remote.0[0].id,
                reply.remote.0[0].instance,
                reply.remote.0[0].state
            ),
            (idle.id, idle.instance, idle.state),
        );
        h.engine
            .shared
            .stateful
            .wait_remote_idle([2; 16], [2; 16])
            .await;
    }
    idle.state = NativeReplicaState::InBuild;
    let build = control(&open, "ReplicatorBuildIdleReplica", vec![idle]);
    h.send("ReplicatorBuildIdleReplica", &build).await;
    h.no_reply().await;
    assert!(calls.builds.lock().unwrap().is_empty());
    assert!(calls.removals.lock().unwrap().is_empty());
}

#[tokio::test]
async fn captured_retry_cleanup_cannot_repeat_or_remove_a_rebuilt_remote() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 43);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut idle = peer(&open.local, 2);
    idle.current_role = NativeReplicaRole::Idle;
    let build = control(&open, "ReplicatorBuildIdleReplica", vec![idle]);
    h.send("ReplicatorBuildIdleReplica", &build).await;
    let first: ProxyReply =
        from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
    assert_eq!(first.error.error, -993);
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    let hook = h.engine.shared.stateful.pause_build_cleanup();
    let shared = h.engine.shared.clone();
    let captured = build.clone();
    let pending =
        tokio::spawn(async move { shared.stateful.run_build_retry([2; 16], captured).await });
    tokio::time::timeout(Duration::from_secs(1), hook.reached.notified())
        .await
        .unwrap();
    h.send("ReplicatorBuildIdleReplica", &build).await;
    wait_event(&calls, "remove", 1).await;
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    assert_eq!(
        *calls.removals.lock().unwrap(),
        [crate::ReplicaRemoval::BuildRetryCleanup]
    );
    h.send("ReplicatorBuildIdleReplica", &build).await;
    let rebuilt: ProxyReply =
        from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
    assert_eq!(rebuilt.error.error, 0);
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    hook.release.notify_one();
    pending.await.unwrap().unwrap();
    assert_eq!(
        *calls.removals.lock().unwrap(),
        [crate::ReplicaRemoval::BuildRetryCleanup]
    );
    assert_eq!(calls.builds.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn replicator_absence_completes_retry_cleanup_and_terminal_idle_removal() {
    for mode in [43, 44] {
        let mut h = Harness::new().await;
        let calls = register(&h);
        let open = request(&h, 2, 1, mode);
        h.send("ReplicaOpen", &open).await;
        h.receive("ReplicaOpenReply").await;
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        let mut idle = peer(&open.local, 2);
        idle.current_role = NativeReplicaRole::Idle;
        let build = control(&open, "ReplicatorBuildIdleReplica", vec![idle.clone()]);
        h.send("ReplicatorBuildIdleReplica", &build).await;
        let first: ProxyReply =
            from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
        assert_eq!(first.error.error, if mode == 43 { -993 } else { 0 });
        h.engine
            .shared
            .stateful
            .wait_remote_idle([2; 16], [2; 16])
            .await;
        if mode == 43 {
            h.send("ReplicatorBuildIdleReplica", &build).await;
            h.engine
                .shared
                .stateful
                .wait_remote_idle([2; 16], [2; 16])
                .await;
            h.no_reply().await;
            assert_eq!(
                *calls.removals.lock().unwrap(),
                [crate::ReplicaRemoval::BuildRetryCleanup]
            );
            h.send("ReplicatorBuildIdleReplica", &build).await;
            let rebuilt: ProxyReply =
                from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
            assert_eq!(rebuilt.error.error, 0);
            h.engine
                .shared
                .stateful
                .wait_remote_idle([2; 16], [2; 16])
                .await;
        }
        idle.state = NativeReplicaState::Dropped;
        let remove = control(&open, "ReplicatorRemoveIdleReplica", vec![idle]);
        if mode == 44 {
            calls.uncertain_removal.store(true, Ordering::Release);
            h.send("ReplicatorRemoveIdleReplica", &remove).await;
            let reply: ProxyReply =
                from_slice(&h.receive("ReplicatorRemoveIdleReplicaReply").await.body).unwrap();
            assert_eq!(reply.error.error, 0x80071be8u32 as i32 as i64);
            h.engine
                .shared
                .stateful
                .wait_remote_idle([2; 16], [2; 16])
                .await;
            calls.uncertain_removal.store(false, Ordering::Release);
        }
        for _ in 0..2 {
            h.send("ReplicatorRemoveIdleReplica", &remove).await;
            let reply: ProxyReply =
                from_slice(&h.receive("ReplicatorRemoveIdleReplicaReply").await.body).unwrap();
            assert_eq!(reply.error.error, 0);
            h.engine
                .shared
                .stateful
                .wait_remote_idle([2; 16], [2; 16])
                .await;
        }
        assert_eq!(
            calls.removals.lock().unwrap().last(),
            Some(&crate::ReplicaRemoval::Idle)
        );
        assert_eq!(calls.removals.lock().unwrap().len(), 2);
        h.send("ReplicatorBuildIdleReplica", &build).await;
        h.no_reply().await;
        assert_eq!(
            calls.builds.lock().unwrap().len(),
            if mode == 43 { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn newer_built_incarnation_removes_old_resource_then_retries_and_fences_old_remove() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut target = peer(&open.local, 2);
    target.current_role = NativeReplicaRole::Idle;
    let old = control(&open, "ReplicatorBuildIdleReplica", vec![target]);
    h.send("ReplicatorBuildIdleReplica", &old).await;
    h.receive("ReplicatorBuildIdleReplicaReply").await;
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    let mut newer = old.clone();
    newer.remote.0[0].instance = 2;
    h.send("ReplicatorBuildIdleReplica", &newer).await;
    wait_event(&calls, "remove", 1).await;
    h.engine
        .shared
        .stateful
        .wait_remote_idle([2; 16], [2; 16])
        .await;
    h.no_reply().await;
    h.send("ReplicatorBuildIdleReplica", &newer).await;
    let built: ProxyReply =
        from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
    assert_eq!((built.error.error, built.remote.0[0].instance), (0, 2));
    h.send("ReplicatorRemoveIdleReplica", &old).await;
    h.no_reply().await;
    assert_eq!(calls.builds.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn promotion_notifies_original_endpoint_until_exact_ack_then_current_configuration() {
    promotion_endpoint_scenario(None).await;
}

async fn endpoint_secondary_fixture() -> (Harness, ProxyRequest, ProxyRequest) {
    let mut h = Harness::new().await;
    register(&h);
    let mut open = request(&h, 2, 1, 20);
    open.local.current_role = NativeReplicaRole::Idle;
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut secondary = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    secondary.local.current_role = NativeReplicaRole::Secondary;
    secondary.remote.0[0].current_role = NativeReplicaRole::Secondary;
    secondary.unit.current.configuration += 1;
    secondary.unit.previous = open.unit.current.clone();
    h.send("UpdateConfiguration", &secondary).await;
    h.receive("UpdateConfigurationReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    (h, open, secondary)
}

async fn receive_endpoint_notification(h: &mut Harness) -> Message {
    let mut notification = None;
    for _ in 0..2 {
        let frame = tokio::time::timeout(Duration::from_secs(2), h.peer.receive())
            .await
            .unwrap()
            .unwrap();
        match frame.metadata().unwrap().action.as_str() {
            "ProxyReplicaEndpointUpdated" => notification = Some(frame),
            "UpdateConfigurationReply" => assert_eq!(
                from_slice::<ProxyReply>(&frame.body).unwrap().error.error,
                0
            ),
            other => panic!("unexpected {other}"),
        }
    }
    notification.unwrap()
}

#[tokio::test]
async fn endpoint_retry_is_superseded_only_after_preparation_and_owner_admission_succeed() {
    enum Failure {
        Encoding,
        Capacity,
    }
    for failure in [Some(Failure::Encoding), Some(Failure::Capacity), None] {
        let (mut h, open, mut promote) = endpoint_secondary_fixture().await;
        promote.local.current_role = NativeReplicaRole::Primary;
        promote.remote.0[0].current_role = NativeReplicaRole::Primary;
        promote.unit.current.configuration += 1 << 32;
        promote.unit.previous = open.unit.current.clone();
        promote.flags = NativeProxyFlags::CATCH_UP;
        h.send("UpdateConfiguration", &promote).await;
        let original = receive_endpoint_notification(&mut h).await;
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        let original_cancel = h
            .engine
            .shared
            .stateful
            .endpoint_cancellation([2; 16])
            .unwrap();
        match failure {
            Some(Failure::Encoding) => h
                .engine
                .shared
                .stateful
                .fail_next_endpoint_preparation(Error::ResourceLimit("injected endpoint encoding")),
            Some(Failure::Capacity) => h.engine.shared.stateful.hold_endpoint_retry_capacity(),
            None => {}
        }
        let result = h.engine.shared.stateful.republish_endpoint([2; 16]).await;
        let expected = if failure.is_some() {
            assert!(matches!(result, Err(Error::ResourceLimit(_))));
            assert!(!original_cancel.is_cancelled());
            h.no_reply().await;
            original
        } else {
            result.unwrap();
            let notification = h.receive("ProxyReplicaEndpointUpdated").await;
            assert!(original_cancel.is_cancelled());
            assert_eq!(notification.body, original.body);
            assert_ne!(
                notification.metadata().unwrap().id,
                original.metadata().unwrap().id
            );
            notification
        };
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        let current_cancel = h
            .engine
            .shared
            .stateful
            .endpoint_cancellation([2; 16])
            .unwrap();
        assert!(!current_cancel.is_cancelled());

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(30)).await;
        let retry = h.receive("ProxyReplicaEndpointUpdated").await;
        assert_eq!(retry.body, expected.body);
        assert_ne!(
            retry.metadata().unwrap().id,
            expected.metadata().unwrap().id
        );
        let body: ReplicaMessage = from_slice(&retry.body).unwrap();
        let mut ack = control(&promote, "ProxyReplicaEndpointUpdatedReply", vec![]);
        ack.runtime = open.runtime.clone();
        ack.local = body.local;
        h.send("ProxyReplicaEndpointUpdatedReply", &ack).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(1), async {
            while h.engine.shared.stateful.endpoint_is_pending([2; 16]) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(original_cancel.is_cancelled());
        assert!(current_cancel.is_cancelled());
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(30)).await;
        h.no_reply().await;
        tokio::time::resume();
        h.engine.quiesce().await.unwrap();
        crate::execution::shutdown(&h.engine.shared.execution, &h.engine.shared.authority)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn failed_endpoint_admission_without_a_previous_retry_does_not_install_a_phantom_owner() {
    for capacity in [false, true] {
        let (mut h, open, mut promote) = endpoint_secondary_fixture().await;
        if capacity {
            h.engine.shared.stateful.hold_endpoint_retry_capacity();
        } else {
            h.engine
                .shared
                .stateful
                .fail_next_endpoint_preparation(Error::ResourceLimit("injected endpoint encoding"));
        }
        promote.local.current_role = NativeReplicaRole::Primary;
        promote.remote.0[0].current_role = NativeReplicaRole::Primary;
        promote.unit.current.configuration += 1 << 32;
        promote.unit.previous = open.unit.current.clone();
        promote.flags = NativeProxyFlags::CATCH_UP;
        h.send("UpdateConfiguration", &promote).await;
        let reply: ProxyReply =
            from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
        assert_eq!(reply.error.error, 0x8007000eu32 as i32 as i64);
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        assert!(!h.engine.shared.stateful.endpoint_is_pending([2; 16]));
        h.no_reply().await;
        h.engine.quiesce().await.unwrap();
        crate::execution::shutdown(&h.engine.shared.execution, &h.engine.shared.authority)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn native_endpoint_notification_retry_and_quiescence_do_not_need_application_scheduler_progress(
) {
    let (handle, handle_rx) = std::sync::mpsc::sync_channel(1);
    let (release, stalled) = std::sync::mpsc::sync_channel(1);
    let application = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        handle.send(runtime.handle().clone()).unwrap();
        // The application runtime is deliberately never driven. Only its
        // scheduler/driver handle is supplied to user callback workers.
        stalled.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    struct JoinApplication(
        Option<std::sync::mpsc::SyncSender<()>>,
        Option<std::thread::JoinHandle<()>>,
    );
    impl Drop for JoinApplication {
        fn drop(&mut self) {
            self.0.take().unwrap().send(()).unwrap();
            self.1.take().unwrap().join().unwrap();
        }
    }
    let _application = JoinApplication(Some(release), Some(application));
    promotion_endpoint_scenario(Some(
        handle_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
    ))
    .await;
}

async fn promotion_endpoint_scenario(application: Option<tokio::runtime::Handle>) {
    let application_stalled = application.is_some();
    let mut h = Harness::new_with_application(application).await;
    let calls = register(&h);
    let mut open = request(&h, 2, 1, 20);
    open.local.current_role = NativeReplicaRole::Idle;
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut secondary = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    secondary.local.current_role = NativeReplicaRole::Secondary;
    secondary.remote.0[0].current_role = NativeReplicaRole::Secondary;
    secondary.unit.current.configuration += 1;
    secondary.unit.previous = open.unit.current.clone();
    h.send("UpdateConfiguration", &secondary).await;
    h.receive("UpdateConfigurationReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut promote = secondary.clone();
    promote.local.current_role = NativeReplicaRole::Primary;
    promote.remote.0[0].current_role = NativeReplicaRole::Primary;
    promote.unit.current.configuration += 1 << 32;
    promote.unit.previous = secondary.unit.current.clone();
    promote.flags = NativeProxyFlags::CATCH_UP;
    h.send("UpdateConfiguration", &promote).await;
    let notification = receive_endpoint_notification(&mut h).await;
    let body: ReplicaMessage = from_slice(&notification.body).unwrap();
    assert_eq!(body.local.service_endpoint, "model://stateful/client");
    assert_eq!(body.local.replication_endpoint, "model://stateful/control");
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (G, T)
    );
    if application_stalled {
        // Leave the retry pending and prove native Close/quiescence observes
        // its cancellation without ever driving the application scheduler.
        // The companion same-clock fixture below exercises the actual30s retry.
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        let mut close = open.clone();
        close.unit = promote.unit.clone();
        close.local = promote.local.clone();
        h.send("ReplicaClose", &close).await;
        h.receive("ReplicaCloseReply").await;
        h.engine.shared.stateful.wait_idle([2; 16]).await;
        h.engine.quiesce().await.unwrap();
        crate::execution::shutdown(&h.engine.shared.execution, &h.engine.shared.authority)
            .await
            .unwrap();
        return;
    }
    let mut ack = control(&promote, "ProxyReplicaEndpointUpdatedReply", vec![]);
    ack.runtime = open.runtime.clone();
    ack.local = body.local.clone();
    ack.local.service_endpoint = "wrong".into();
    h.send("ProxyReplicaEndpointUpdatedReply", &ack).await;
    tokio::task::yield_now().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    let retry = h.receive("ProxyReplicaEndpointUpdated").await;
    assert_eq!(retry.body, notification.body);
    assert_ne!(
        retry.metadata().unwrap().id,
        notification.metadata().unwrap().id
    );
    ack.local = body.local;
    h.send("ProxyReplicaEndpointUpdatedReply", &ack).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.engine.shared.stateful.endpoint_is_pending([2; 16]) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("UpdateConfiguration", &promote).await;
    let duplicate: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(
        (duplicate.error.error, duplicate.flags),
        (0, NativeProxyFlags::CATCH_UP)
    );
    assert_eq!(
        calls
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "waitQuorum")
            .count(),
        1
    );
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (G, T)
    );
    let mut end = promote;
    end.flags = NativeProxyFlags::END_RECONFIGURATION;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("UpdateConfiguration", &end).await;
    h.receive("UpdateConfigurationReply").await;
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (G, G)
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    h.no_reply().await;
    tokio::time::resume();
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.engine.quiesce().await.unwrap();
    crate::execution::shutdown(&h.engine.shared.execution, &h.engine.shared.authority)
        .await
        .unwrap();
}

#[tokio::test]
async fn data_loss_state_change_is_a_control_outcome_not_a_failed_open_cleanup() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 1, 25);
    open.unit.previous = Epoch {
        data_loss: 0,
        configuration: 1 << 32,
    };
    h.send("ReplicaOpen", &open).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0x80071d28u32 as i32 as i64);
    assert_eq!(
        (reply.error.api.api, reply.error.api.interface),
        (NativeApiName::Invalid, NativeInterfaceName::Invalid)
    );
    assert_eq!((reply.local.first_lsn, reply.local.last_lsn), (0, 0));
    assert!(calls.partitions.lock().unwrap()[0].validate().is_ok());
    assert!(!calls.events.lock().unwrap().contains(&"serviceAbort"));
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut update = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    update.flags = NativeProxyFlags::CATCH_UP;
    update.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &update).await;
    let reported: ProxyReply =
        from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reported.error.error, 0);
    assert_eq!(
        calls
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "dataLoss")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_open_can_retry_same_native_incarnation_without_reviving_old_local_owner() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 28);
    h.send("ReplicaOpen", &open).await;
    let failed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(failed.error.error, -456);
    let old = calls.partitions.lock().unwrap()[0].clone();
    assert!(old.validate().is_err());
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("ReplicaOpen", &open).await;
    let retried: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(retried.error.error, 0);
    let new = calls.partitions.lock().unwrap()[1].clone();
    assert_eq!(old.identity(), new.identity());
    assert!(old.validate().is_err());
    assert!(new.validate().is_ok());
}

#[tokio::test]
async fn native_last_open_retry_abort_flag_does_not_reopen_disposed_tombstone() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 1, 1);
    open.flags = NativeProxyFlags::ABORT;
    h.send("ReplicaOpen", &open).await;
    let failed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(
        (failed.error.error, failed.flags),
        (-456, NativeProxyFlags::ABORT)
    );
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    open.flags = NativeProxyFlags::NONE;
    open.service.initialization.0 = vec![20];
    h.send("ReplicaOpen", &open).await;
    let closed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_ne!(closed.error.error, 0);
    assert_eq!(calls.partitions.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn progress_error_is_not_initialized_success_and_uses_replicator_api_metadata() {
    let mut h = Harness::new().await;
    register(&h);
    let open = request(&h, 2, 1, 24);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let status = control(&open, "ReplicatorGetStatus", vec![]);
    h.send("ReplicatorGetStatus", &status).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicatorGetStatusReply").await.body).unwrap();
    assert_eq!(reply.error.error, -999);
    assert_eq!(
        (reply.error.api.api, reply.error.api.interface),
        (NativeApiName::GetStatus, NativeInterfaceName::Replicator)
    );
    assert_eq!((reply.local.first_lsn, reply.local.last_lsn), (-1, -1));
}

#[tokio::test]
async fn data_loss_callback_failure_reports_transient_fault_before_failed_open_cleanup() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 1, 29);
    open.unit.previous = Epoch {
        data_loss: 0,
        configuration: 1 << 32,
    };
    h.send("ReplicaOpen", &open).await;
    let fault: FaultBody = from_slice(&h.receive("RAReportFault").await.body).unwrap();
    assert_eq!(
        (fault.fault, fault.local.id, fault.local.instance),
        (NativeFaultType::Transient, 100, 1)
    );
    let failed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(failed.error.error, -995);
    assert_eq!(
        (failed.error.api.api, failed.error.api.interface),
        (NativeApiName::OnDataLoss, NativeInterfaceName::Replicator)
    );
    assert!(calls.partitions.lock().unwrap()[0].validate().is_err());
    assert!(calls.events.lock().unwrap().contains(&"replicatorAbort"));
    assert!(calls.events.lock().unwrap().contains(&"serviceAbort"));
}
