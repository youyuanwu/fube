use super::*;

#[tokio::test]
async fn retry_cleanup_allows_verified_same_incarnation_rebuild_but_native_remove_does_not() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (_peer, _, address) = replica(2, 200, 1, ReplicaRole::IdleSecondary, server()).await;
    let info = information(2, 200, 1, address, ReplicaRole::IdleSecondary);
    let mut unreachable = info.clone();
    unreachable.address = "http://127.0.0.1:1/unreachable".into();
    assert!(primary
        .build_replica(unreachable, Cancellation::default())
        .await
        .is_err());
    primary
        .remove_replica(
            info.clone(),
            ReplicaRemoval::BuildRetryCleanup,
            Cancellation::default(),
        )
        .await
        .unwrap();
    primary
        .build_replica(info.clone(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(primary.state.lock().unwrap().built.len(), 1);
    primary
        .remove_replica(info.clone(), ReplicaRemoval::Idle, Cancellation::default())
        .await
        .unwrap();
    assert!(primary
        .build_replica(info, Cancellation::default())
        .await
        .is_err());
}

#[tokio::test]
async fn retry_cleanup_and_rebuild_cannot_accept_a_late_pre_cleanup_peer_receipt() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (peer, _, address) = replica(2, 200, 1, ReplicaRole::IdleSecondary, server()).await;
    let body = peer.peer_body().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let delayed_address = format!("http://{}/old", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let wire = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        e.notify_one();
        r.notified().await;
        response(&mut stream, 200, &body).await.unwrap();
    });
    let p = primary.clone();
    let old = tokio::spawn(async move {
        p.build_replica(
            information(2, 200, 1, delayed_address, ReplicaRole::IdleSecondary),
            Cancellation::default(),
        )
        .await
    });
    entered.notified().await;
    let info = information(2, 200, 1, address, ReplicaRole::IdleSecondary);
    primary
        .remove_replica(
            info.clone(),
            ReplicaRemoval::BuildRetryCleanup,
            Cancellation::default(),
        )
        .await
        .unwrap();
    primary
        .build_replica(info, Cancellation::default())
        .await
        .unwrap();
    release.notify_one();
    wire.await.unwrap();
    assert!(old.await.unwrap().is_err());
    assert_eq!(primary.state.lock().unwrap().built.len(), 1);
}

#[tokio::test]
async fn both_native_demotion_all_checks_accept_only_the_authorized_previous_epoch() {
    let (primary, view, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (_peer, _, address) = replica(2, 200, 1, ReplicaRole::ActiveSecondary, server()).await;
    let old = epoch();
    let new = Epoch {
        data_loss_number: old.data_loss_number,
        configuration_number: old.configuration_number + (1 << 32),
    };
    {
        let mut status = view.status.lock().unwrap();
        status.current_role = ReplicaRole::ActiveSecondary;
        status.current_epoch = new;
        status.previous_epoch = old;
        status.configuration_stage = ConfigurationStage::PreWriteRevoke;
    }
    let mut current = config(
        vec![information(
            2,
            200,
            1,
            address,
            ReplicaRole::ActiveSecondary,
        )],
        2,
        true,
    );
    current.epoch = new;
    let mut previous = current.clone();
    previous.epoch = old;
    primary
        .update_catch_up_configuration(current.clone(), previous.clone(), Cancellation::default())
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .unwrap();
    // Native demotes before promoting the target. Updating our epoch does not
    // advance the peer: the second ALL must still prove that real old peer.
    view.status.lock().unwrap().configuration_stage = ConfigurationStage::CatchUp;
    primary
        .update_epoch(new, Cancellation::default())
        .await
        .unwrap();
    primary
        .update_catch_up_configuration(current, previous, Cancellation::default())
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .unwrap();
    assert!(primary
        .wait_for_catch_up(CatchUpMode::Write, Cancellation::default())
        .await
        .is_err());
    view.status.lock().unwrap().current_role = ReplicaRole::Primary;
    assert!(primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .is_err());
}

#[tokio::test]
async fn removed_incarnation_cannot_prove_pending_or_future_all_catchup() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (target, view, real_address) =
        replica(2, 200, 1, ReplicaRole::IdleSecondary, server()).await;
    primary
        .build_replica(
            information(2, 200, 1, real_address, ReplicaRole::IdleSecondary),
            Cancellation::default(),
        )
        .await
        .unwrap();
    {
        let mut status = view.status.lock().unwrap();
        status.current_role = ReplicaRole::ActiveSecondary;
        status.service_role = ReplicaRole::ActiveSecondary;
        status.replicator_role = ReplicaRole::ActiveSecondary;
    }
    target
        .change_role(
            epoch(),
            ReplicaRole::ActiveSecondary,
            Cancellation::default(),
        )
        .await
        .unwrap();
    let body = target.peer_body().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/removedreceipt", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let wire = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        e.notify_one();
        r.notified().await;
        response(&mut stream, 200, &body).await.unwrap();
    });
    let info = information(2, 200, 1, address, ReplicaRole::ActiveSecondary);
    primary
        .update_catch_up_configuration(
            config(vec![info.clone()], 2, true),
            no_previous(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    let p = primary.clone();
    let pending = tokio::spawn(async move {
        p.wait_for_catch_up(CatchUpMode::All, Cancellation::default())
            .await
    });
    entered.notified().await;
    primary
        .remove_replica(info, ReplicaRemoval::Idle, Cancellation::default())
        .await
        .unwrap();
    release.notify_one();
    wire.await.unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .is_err());
    assert_eq!(
        primary
            .state
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .write_quorum,
        2
    );
    assert_eq!(
        primary
            .state
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .replicas
            .len(),
        1
    );
    let (_replacement, _, address) =
        replica(2, 200, 2, ReplicaRole::ActiveSecondary, server()).await;
    primary
        .update_catch_up_configuration(
            config(
                vec![information(
                    2,
                    200,
                    2,
                    address,
                    ReplicaRole::ActiveSecondary,
                )],
                2,
                true,
            ),
            no_previous(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn remove_during_pending_build_fences_its_late_successful_response() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (target, _, _) = replica(2, 200, 1, ReplicaRole::IdleSecondary, server()).await;
    let body = target.peer_body().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/latebuild", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        e.notify_one();
        r.notified().await;
        response(&mut stream, 200, &body).await.unwrap();
    });
    let info = information(2, 200, 1, address, ReplicaRole::IdleSecondary);
    let p = primary.clone();
    let target = info.clone();
    let build = tokio::spawn(async move { p.build_replica(target, Cancellation::default()).await });
    entered.notified().await;
    primary
        .remove_replica(info, ReplicaRemoval::Idle, Cancellation::default())
        .await
        .unwrap();
    release.notify_one();
    peer.await.unwrap();
    assert!(build.await.unwrap().is_err());
    assert!(primary.state.lock().unwrap().built.is_empty());
}

#[tokio::test]
async fn nonempty_frontier_and_client_address_cannot_masquerade_as_replication_readiness() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (peer, _, real_address) = replica(2, 200, 1, ReplicaRole::IdleSecondary, server()).await;
    let client_address = real_address.replace("/empty/", "/client/");
    assert!(primary
        .build_replica(
            information(2, 200, 1, client_address, ReplicaRole::IdleSecondary),
            Cancellation::default()
        )
        .await
        .is_err());
    let mut body: serde_json::Value = serde_json::from_str(&peer.peer_body().unwrap()).unwrap();
    body["last"] = serde_json::json!(99);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/badfrontier", listener.local_addr().unwrap());
    let fake = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        response(&mut stream, 200, &body.to_string()).await.unwrap();
    });
    assert!(primary
        .build_replica(
            information(2, 200, 1, address, ReplicaRole::IdleSecondary),
            Cancellation::default()
        )
        .await
        .is_err());
    fake.await.unwrap();
    assert!(primary.state.lock().unwrap().built.is_empty());
}

#[tokio::test]
async fn completed_catchup_cannot_authorize_unverified_new_current_membership() {
    let (primary, view, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    primary
        .update_catch_up_configuration(
            config(vec![], 1, true),
            no_previous(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::Write, Cancellation::default())
        .await
        .unwrap();
    view.status.lock().unwrap().catch_up = CatchUpResult::Completed;
    let (gone, _, address) = replica(2, 200, 1, ReplicaRole::ActiveSecondary, server()).await;
    gone.abort();
    let new = config(
        vec![information(
            2,
            200,
            1,
            address,
            ReplicaRole::ActiveSecondary,
        )],
        2,
        true,
    );
    assert!(primary
        .update_current_configuration(new.clone(), Cancellation::default())
        .await
        .is_err());
    assert!(primary
        .update_catch_up_configuration(new, no_previous(), Cancellation::default())
        .await
        .is_err());
}

#[tokio::test]
async fn overlapping_configuration_invalidates_a_pending_old_peer_receipt() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (old_peer, _, _) = replica(2, 200, 1, ReplicaRole::ActiveSecondary, server()).await;
    let body = old_peer.peer_body().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_address = format!("http://{}/oldreceipt", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        e.notify_one();
        r.notified().await;
        response(&mut stream, 200, &body).await.unwrap();
    });
    primary
        .update_catch_up_configuration(
            config(
                vec![information(
                    2,
                    200,
                    1,
                    old_address,
                    ReplicaRole::ActiveSecondary,
                )],
                2,
                true,
            ),
            no_previous(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    let p = primary.clone();
    let wait = tokio::spawn(async move {
        p.wait_for_catch_up(CatchUpMode::Write, Cancellation::default())
            .await
    });
    entered.notified().await;
    let (gone, _, missing) = replica(3, 300, 1, ReplicaRole::ActiveSecondary, server()).await;
    gone.abort();
    primary
        .update_catch_up_configuration(
            config(
                vec![information(
                    3,
                    300,
                    1,
                    missing,
                    ReplicaRole::ActiveSecondary,
                )],
                2,
                true,
            ),
            no_previous(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    release.notify_one();
    peer.await.unwrap();
    assert!(wait.await.unwrap().is_err());
}

#[tokio::test]
async fn abort_drains_an_owned_network_probe_instead_of_claiming_dropped_work_complete() {
    let shared = server();
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, shared.clone()).await;
    let (_sibling, _, sibling_address) =
        replica(3, 300, 1, ReplicaRole::IdleSecondary, shared).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/pending", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        e.notify_one();
        r.notified().await;
    });
    let p = primary.clone();
    let operation = tokio::spawn(async move {
        p.build_replica(
            information(2, 200, 1, address, ReplicaRole::IdleSecondary),
            Cancellation::default(),
        )
        .await
    });
    entered.notified().await;
    let p = primary.clone();
    tokio::task::spawn_blocking(move || p.abort())
        .await
        .unwrap();
    assert!(operation.await.unwrap().is_err());
    assert_eq!(primary.active.load(Ordering::Acquire), 0);
    assert_eq!(http_peer(&sibling_address).await.unwrap()["replica"], 300);
    release.notify_one();
    server.await.unwrap();
}
