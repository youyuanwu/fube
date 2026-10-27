use super::*;

#[tokio::test]
async fn remove_during_pending_build_fences_its_late_successful_response() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (target, _, _) = replica(2, 200, 1, ReplicaRole::Idle, server()).await;
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
    let info = information(2, 200, 1, address, ReplicaRole::Idle);
    let p = primary.clone();
    let target = info.clone();
    let build = tokio::spawn(async move { p.build_replica(target, Cancellation::default()).await });
    entered.notified().await;
    primary
        .remove_replica(info, Cancellation::default())
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
    let (peer, _, real_address) = replica(2, 200, 1, ReplicaRole::Idle, server()).await;
    let client_address = real_address.replace("/empty/", "/client/");
    assert!(primary
        .build_replica(
            information(2, 200, 1, client_address, ReplicaRole::Idle),
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
            information(2, 200, 1, address, ReplicaRole::Idle),
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
        .wait_for_catch_up(CatchUpMode::WriteQuorum, Cancellation::default())
        .await
        .unwrap();
    view.status.lock().unwrap().catch_up = CatchUpResult::Completed;
    let (gone, _, address) = replica(2, 200, 1, ReplicaRole::Secondary, server()).await;
    gone.abort();
    let new = config(
        vec![information(2, 200, 1, address, ReplicaRole::Secondary)],
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
    let (old_peer, _, _) = replica(2, 200, 1, ReplicaRole::Secondary, server()).await;
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
                vec![information(2, 200, 1, old_address, ReplicaRole::Secondary)],
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
        p.wait_for_catch_up(CatchUpMode::WriteQuorum, Cancellation::default())
            .await
    });
    entered.notified().await;
    let (gone, _, missing) = replica(3, 300, 1, ReplicaRole::Secondary, server()).await;
    gone.abort();
    primary
        .update_catch_up_configuration(
            config(
                vec![information(3, 300, 1, missing, ReplicaRole::Secondary)],
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
    let (_sibling, _, sibling_address) = replica(3, 300, 1, ReplicaRole::Idle, shared).await;
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
            information(2, 200, 1, address, ReplicaRole::Idle),
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
