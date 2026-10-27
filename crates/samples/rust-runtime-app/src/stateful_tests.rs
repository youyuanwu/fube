use super::*;
#[path = "stateful_peer_tests.rs"]
mod peer_tests;
use mssf_runtime::CatchUpResult;

struct NativeModel {
    alive: AtomicBool,
    status: Mutex<ReplicaStatus>,
}
impl View for NativeModel {
    fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        Ok(self.status.lock().unwrap().clone())
    }
}
fn epoch() -> Epoch {
    Epoch {
        data_loss: 1,
        configuration: 1 << 32,
    }
}
async fn replica(
    node: u8,
    id: i64,
    incarnation: i64,
    role: ReplicaRole,
    server: Arc<PeerServer>,
) -> (Arc<EmptyReplicator>, Arc<NativeModel>, String) {
    let view = Arc::new(NativeModel {
        alive: AtomicBool::new(true),
        status: Mutex::new(ReplicaStatus {
            lifecycle: ReplicaLifecycle::Open,
            service_role: role,
            replicator_role: role,
            replicator_open: true,
            current_role: role,
            current_epoch: Epoch {
                configuration: (1 << 32) + 1,
                data_loss: 1,
            },
            previous_epoch: Epoch::INVALID,
            configuration_stage: ConfigurationStage::Current,
            catch_up: CatchUpResult::NotStarted,
            minimum_replica_set_size: 1,
            configuration: vec![],
            service_address: String::new(),
            replication_address: String::new(),
            progress: EMPTY,
        }),
    });
    let identity = ReplicaIdentity {
        service_name: "fabric:/Test/Stateful".into(),
        service_type: "RustStatefulRuntimeType".into(),
        partition_id: "02020202-0202-0202-0202-020202020202".parse().unwrap(),
        replica_id: id,
        incarnation,
        node_id: [node; 16],
        node_instance: 1,
    };
    let result = EmptyReplicator::new(
        view.clone(),
        identity,
        format!("/fixture/{id}/{incarnation}"),
        server,
    );
    result.wait_millis.store(35, Ordering::Release);
    let address = result.open(Cancellation::default()).await.unwrap();
    result
        .change_role(epoch(), role, Cancellation::default())
        .await
        .unwrap();
    (result, view, address)
}
fn information(
    node: u8,
    id: i64,
    incarnation: i64,
    address: String,
    role: ReplicaRole,
) -> ReplicaInformation {
    ReplicaInformation {
        node_id: [node; 16],
        node_instance: 1,
        replica_id: id,
        incarnation,
        role,
        previous_role: ReplicaRole::Secondary,
        up: true,
        ready: role != ReplicaRole::Idle,
        address,
        progress: EMPTY,
        must_catch_up: false,
    }
}
fn config(
    replicas: Vec<ReplicaInformation>,
    quorum: usize,
    local: bool,
) -> ReplicaSetConfiguration {
    ReplicaSetConfiguration {
        epoch: epoch(),
        replicas,
        write_quorum: quorum,
        local_member: local,
    }
}
fn no_previous() -> ReplicaSetConfiguration {
    ReplicaSetConfiguration {
        epoch: Epoch::INVALID,
        replicas: vec![],
        write_quorum: 0,
        local_member: false,
    }
}
fn server() -> Arc<PeerServer> {
    PeerServer::new("127.0.0.1".into(), 0)
}

#[tokio::test]
async fn actual_peer_build_and_empty_quorum_progress_are_not_control_counters() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (secondary, view, address) = replica(2, 200, 1, ReplicaRole::Idle, server()).await;
    let target = information(2, 200, 1, address.clone(), ReplicaRole::Idle);
    primary
        .build_replica(target, Cancellation::default())
        .await
        .unwrap();
    assert_eq!(primary.state.lock().unwrap().built.len(), 1);
    {
        let mut status = view.status.lock().unwrap();
        status.current_role = ReplicaRole::Secondary;
        status.service_role = ReplicaRole::Secondary;
        status.replicator_role = ReplicaRole::Secondary;
    }
    secondary
        .change_role(epoch(), ReplicaRole::Secondary, Cancellation::default())
        .await
        .unwrap();
    let cc = config(
        vec![information(2, 200, 1, address, ReplicaRole::Secondary)],
        2,
        true,
    );
    primary
        .update_catch_up_configuration(cc.clone(), no_previous(), Cancellation::default())
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::WriteQuorum, Cancellation::default())
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .unwrap();
    primary
        .update_current_configuration(cc, Cancellation::default())
        .await
        .unwrap();
    assert_eq!(primary.progress().unwrap(), EMPTY);
    assert!(!primary.on_data_loss(Cancellation::default()).await.unwrap());
    assert!(primary.probes.load(Ordering::Acquire) >= 3);
}

#[tokio::test]
async fn foreign_incarnation_role_and_epoch_peers_never_build_successfully() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (_wrong_role_peer, _, address) = replica(2, 200, 1, ReplicaRole::Secondary, server()).await;
    let wrong_role = information(2, 200, 1, address, ReplicaRole::Idle);
    assert!(primary
        .build_replica(wrong_role, Cancellation::default())
        .await
        .is_err());
    let (peer, view, address) = replica(2, 200, 1, ReplicaRole::Idle, server()).await;
    let wrong_incarnation = information(2, 200, 2, address.clone(), ReplicaRole::Idle);
    assert!(primary
        .build_replica(wrong_incarnation, Cancellation::default())
        .await
        .is_err());
    let foreign = information(3, 300, 1, address.clone(), ReplicaRole::Idle);
    assert!(primary
        .build_replica(foreign, Cancellation::default())
        .await
        .is_err());
    let next = Epoch {
        data_loss: 1,
        configuration: 2 << 32,
    };
    view.status.lock().unwrap().current_epoch.configuration = (2 << 32) + 1;
    peer.update_epoch(next, Cancellation::default())
        .await
        .unwrap();
    assert!(primary
        .build_replica(
            information(2, 200, 1, address, ReplicaRole::Idle),
            Cancellation::default()
        )
        .await
        .is_err());
    assert!(primary.state.lock().unwrap().built.is_empty());
}

#[tokio::test]
async fn quorum_all_and_required_target_have_distinct_real_peer_obligations() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (_good_peer, _, good) = replica(2, 200, 1, ReplicaRole::Secondary, server()).await;
    let (unavailable, _, missing) = replica(3, 300, 1, ReplicaRole::Secondary, server()).await;
    unavailable.abort();
    let a = information(2, 200, 1, good, ReplicaRole::Secondary);
    let b = information(3, 300, 1, missing, ReplicaRole::Secondary);
    let cc = config(vec![a.clone(), b.clone()], 2, true);
    primary
        .update_catch_up_configuration(cc, no_previous(), Cancellation::default())
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::WriteQuorum, Cancellation::default())
        .await
        .unwrap();
    assert!(primary
        .wait_for_catch_up(CatchUpMode::All, Cancellation::default())
        .await
        .is_err());
    let mut required = b;
    required.must_catch_up = true;
    primary
        .update_catch_up_configuration(
            config(vec![a, required], 2, true),
            no_previous(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(primary
        .wait_for_catch_up(CatchUpMode::WriteQuorum, Cancellation::default())
        .await
        .is_err());
}

#[tokio::test]
async fn previous_configuration_cannot_invent_a_local_vote() {
    let (primary, view, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    view.status.lock().unwrap().previous_epoch = epoch();
    let (_good_peer, _, good) = replica(2, 200, 1, ReplicaRole::Secondary, server()).await;
    let (gone, _, missing) = replica(3, 300, 1, ReplicaRole::Secondary, server()).await;
    gone.abort();
    let pc = config(
        vec![
            information(2, 200, 1, good, ReplicaRole::Secondary),
            information(3, 300, 1, missing, ReplicaRole::Secondary),
        ],
        2,
        false,
    );
    primary
        .update_catch_up_configuration(config(vec![], 1, true), pc, Cancellation::default())
        .await
        .unwrap();
    assert!(primary
        .wait_for_catch_up(CatchUpMode::WriteQuorum, Cancellation::default())
        .await
        .is_err());
}

#[tokio::test]
async fn epochs_only_advance_under_native_selection_and_never_advance_the_empty_frontier() {
    let (primary, view, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let next = Epoch {
        data_loss: 2,
        configuration: 2 << 32,
    };
    assert!(primary
        .update_epoch(next, Cancellation::default())
        .await
        .is_err());
    view.status.lock().unwrap().current_epoch = Epoch {
        data_loss: 2,
        configuration: (2 << 32) + 9,
    };
    primary
        .update_epoch(next, Cancellation::default())
        .await
        .unwrap();
    primary
        .change_role(next, ReplicaRole::Primary, Cancellation::default())
        .await
        .unwrap();
    assert_eq!(primary.progress().unwrap(), EMPTY);
    assert!(!primary.on_data_loss(Cancellation::default()).await.unwrap());
    assert!(primary
        .update_epoch(epoch(), Cancellation::default())
        .await
        .is_err());
}

#[tokio::test]
async fn remove_tombstone_fences_old_instance_but_not_new_instance() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let (old, _, address) = replica(2, 200, 1, ReplicaRole::Idle, server()).await;
    let old_info = information(2, 200, 1, address.clone(), ReplicaRole::Idle);
    primary
        .remove_replica(
            old_info.clone(),
            ReplicaRemoval::Idle,
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(primary
        .build_replica(old_info, Cancellation::default())
        .await
        .is_err());
    let (_new_peer, _, new_address) = replica(2, 200, 2, ReplicaRole::Idle, server()).await;
    assert!(primary
        .build_replica(
            information(2, 200, 2, address, ReplicaRole::Idle),
            Cancellation::default()
        )
        .await
        .is_err());
    primary
        .build_replica(
            information(2, 200, 2, new_address, ReplicaRole::Idle),
            Cancellation::default(),
        )
        .await
        .unwrap();
    old.abort();
    assert_eq!(primary.state.lock().unwrap().built.len(), 1);
}

#[tokio::test]
async fn cancellation_and_close_drain_real_pending_network_work() {
    let (primary, _, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/blackhole", listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let blackhole = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_header(&mut stream).await.unwrap();
        e.notify_one();
        r.notified().await;
    });
    let cancel = Cancellation::default();
    let token = cancel.clone();
    let p = primary.clone();
    let work = tokio::spawn(async move {
        p.build_replica(information(2, 200, 1, address, ReplicaRole::Idle), token)
            .await
    });
    entered.notified().await;
    cancel.cancel();
    assert!(work.await.unwrap().is_err());
    assert_eq!(primary.active.load(Ordering::Acquire), 0);
    primary.close(Cancellation::default()).await.unwrap();
    assert!(primary.progress().is_err());
    release.notify_one();
    blackhole.await.unwrap();
}

#[tokio::test]
async fn close_and_abort_remove_only_owned_control_routes_from_shared_listener() {
    let shared = server();
    let (a, _, address_a) = replica(2, 200, 1, ReplicaRole::Idle, shared.clone()).await;
    let (b, _, address_b) = replica(3, 300, 1, ReplicaRole::Idle, shared).await;
    assert_eq!(http_peer(&address_a).await.unwrap()["replica"], 200);
    assert_eq!(http_peer(&address_b).await.unwrap()["replica"], 300);
    a.close(Cancellation::default()).await.unwrap();
    a.abort();
    assert!(http_peer(&address_a).await.is_err());
    assert_eq!(http_peer(&address_b).await.unwrap()["replica"], 300);
    b.abort();
    assert!(http_peer(&address_b).await.is_err());
}

#[tokio::test]
async fn service_route_requires_current_primary_access_and_live_lifetime() {
    let (_, view, _) = replica(1, 100, 1, ReplicaRole::Primary, server()).await;
    let mut snapshot = view.snapshot().unwrap();
    assert!(readable(&snapshot));
    snapshot.current_role = ReplicaRole::Secondary;
    assert!(!readable(&snapshot)); // Never role==Primary alone.
    snapshot.current_role = ReplicaRole::Primary;
    snapshot.lifecycle = ReplicaLifecycle::Closing;
    assert!(!readable(&snapshot));
    view.alive.store(false, Ordering::Release);
    assert!(view.snapshot().is_err());
}
