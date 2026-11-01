use super::*;
use fube_ra_store::{MemoryBackend, Settings as StoreSettings, Store};
use fube_transport::serialization::serde::{ObjectArray, Pointer};

#[tokio::test]
async fn poisoned_metadata_still_joins_and_transfers_shutdown_ownership() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let ra = Arc::new(
        ra::Executor::new(
            store.clone(),
            ra::Admission {
                local_node: protocol::NodeInstance::default(),
                fm_source: protocol::NodeInstance::default(),
                generation: protocol::GenerationNumber::default(),
                owner_ready: true,
                node_open: true,
                node_activated: true,
                fabric_upgrading: false,
            },
            ra::Settings::default(),
        )
        .unwrap(),
    );
    let node_context = NodeContext {
        node_id: "1".into(),
        node_pid: 1,
        lease_handle: 0,
        lease_duration: 0,
        error: 0,
        node_instance: 1,
        node_name: "local".into(),
        node_type: "local".into(),
        client_address: "127.0.0.1:19000".into(),
        deployment: "/tmp/prepared".into(),
        ip: "127.0.0.1".into(),
        work: "/tmp".into(),
        initial_ttl: 0,
        application_directories: ObjectArray(vec![]),
        node_directories: ObjectArray(vec![]),
        application_log: Pointer(None),
        system_log: Pointer(None),
    };
    let cfg = Config::registration(hosting::ipc::Config {
        node: node_context,
        core_limits: Default::default(),
        max_admissions: 1,
        scratch_bytes: 8 * 1024 * 1024,
        decode_bytes: 4096,
        projection_bytes: 4096,
        reply_bytes: 4096,
        lease_duration: LOCAL_LEASE_DURATION,
    });
    let mut node = Node::bind("127.0.0.1:0".parse().unwrap(), cfg, ra.clone()).unwrap();
    let handle = node.handle();
    let shared = handle.shared.clone();
    assert!(std::thread::spawn(move || {
        let _guard = shared.meta.lock().unwrap();
        panic!("explicit qualification metadata poison");
    })
    .join()
    .is_err());
    assert!(matches!(
        handle.submit(Input::HealthSources),
        Err(Error::Closed)
    ));
    let started = Instant::now();
    assert!(node.stop().is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(node.thread.is_none());
    let residual = handle
        .take_shutdown()
        .expect("owned residual survives poison");
    assert!(residual.owner.health_sources().unwrap().is_empty());
    assert!(handle.recover_commands().is_empty());
    ra.close(Duration::from_secs(3)).await.unwrap();
    store.close().await.unwrap();
}
