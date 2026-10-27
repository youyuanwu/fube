//! Adapter tests use controlled callbacks, not a purported empty-state policy.
//! The actual sample custom replicator has its own controlled-peer test gate.
use super::*;
use crate::{
    CatchUpMode, ReplicaInformation, ReplicaRole, ReplicaSetConfiguration, ReplicationProgress,
    Replicator, StatefulFactory, StatefulPartition, StatefulService, StatefulServiceContext,
};

#[derive(Default)]
struct StatefulCalls {
    events: Mutex<Vec<&'static str>>,
    partitions: Mutex<Vec<StatefulPartition>>,
    release: Notify,
}
impl StatefulCalls {
    fn record(&self, event: &'static str) {
        // A callback must be able to inspect its published snapshot.
        for partition in self.partitions.lock().unwrap().iter() {
            let _ = partition.status();
        }
        self.events.lock().unwrap().push(event);
    }
}
struct StatefulFactoryFixture(Arc<StatefulCalls>);
struct StatefulFixture {
    calls: Arc<StatefulCalls>,
    mode: u8,
}
struct ReplicatorFixture(Arc<StatefulCalls>);
impl StatefulFactory for StatefulFactoryFixture {
    fn create(
        &self,
        context: StatefulServiceContext,
    ) -> ServiceFuture<'_, Arc<dyn StatefulService>> {
        Box::pin(async move {
            assert_eq!(context.identity.replica_id, 100);
            self.0.record("create");
            Ok(Arc::new(StatefulFixture {
                calls: self.0.clone(),
                mode: context.initialization_data.first().copied().unwrap_or(0),
            }) as Arc<dyn StatefulService>)
        })
    }
}
impl StatefulService for StatefulFixture {
    fn open(
        &self,
        partition: StatefulPartition,
        _: Cancellation,
    ) -> ServiceFuture<'_, Arc<dyn Replicator>> {
        Box::pin(async move {
            self.calls.record("serviceOpen");
            self.calls.partitions.lock().unwrap().push(partition);
            match self.mode {
                1 => {
                    return Err(Error::Remote {
                        code: -456,
                        message: "controlled open failure".into(),
                    })
                }
                8 => self.calls.release.notified().await,
                _ => {}
            }
            Ok(Arc::new(ReplicatorFixture(self.calls.clone())) as Arc<dyn Replicator>)
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.calls.record(match role {
                ReplicaRole::Primary => "servicePrimary",
                ReplicaRole::Idle => "serviceIdle",
                ReplicaRole::Secondary => "serviceSecondary",
                ReplicaRole::None => "serviceNone",
                ReplicaRole::Unknown => return Err(Error::Protocol("unexpected unknown role")),
            });
            Ok(if role == ReplicaRole::Primary {
                "model://stateful/client"
            } else {
                ""
            }
            .into())
        })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.calls.record("serviceClose");
            if self.mode == 2 {
                return Err(Error::Protocol("controlled close failure"));
            }
            Ok(())
        })
    }
    fn abort(&self) {
        self.calls.record("serviceAbort");
        assert_ne!(self.mode, 11, "controlled stateful abort panic");
    }
}
impl Replicator for ReplicatorFixture {
    fn open(&self, _: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.0.record("replicatorOpen");
            Ok("model://stateful/control".into())
        })
    }
    fn change_role(
        &self,
        _: crate::Epoch,
        role: ReplicaRole,
        _: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.0.record(match role {
                ReplicaRole::Primary => "replicatorPrimary",
                ReplicaRole::Idle => "replicatorIdle",
                ReplicaRole::Secondary => "replicatorSecondary",
                ReplicaRole::None => "replicatorNone",
                ReplicaRole::Unknown => return Err(Error::Protocol("unexpected unknown role")),
            });
            Ok(())
        })
    }
    fn update_epoch(&self, _: crate::Epoch, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async {
            Err(Error::Protocol(
                "unexpected epoch operation in lifecycle fixture",
            ))
        })
    }
    fn progress(&self) -> Result<ReplicationProgress> {
        Ok(ReplicationProgress::EMPTY)
    }
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            self.0.record("dataLoss");
            Ok(false)
        })
    }
    fn update_catch_up_configuration(
        &self,
        _: ReplicaSetConfiguration,
        _: ReplicaSetConfiguration,
        _: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async {
            Err(Error::Protocol(
                "unexpected configuration in lifecycle fixture",
            ))
        })
    }
    fn update_current_configuration(
        &self,
        _: ReplicaSetConfiguration,
        _: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async {
            Err(Error::Protocol(
                "unexpected current configuration in lifecycle fixture",
            ))
        })
    }
    fn wait_for_catch_up(&self, _: CatchUpMode, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("unexpected catchup in lifecycle fixture")) })
    }
    fn build_replica(&self, _: ReplicaInformation, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("unexpected build in lifecycle fixture")) })
    }
    fn remove_replica(&self, _: ReplicaInformation, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("unexpected remove in lifecycle fixture")) })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.0.record("replicatorClose");
            Ok(())
        })
    }
    fn abort(&self) {
        self.0.record("replicatorAbort");
    }
}
fn register(h: &Harness) -> Arc<StatefulCalls> {
    let calls = Arc::new(StatefulCalls::default());
    h.engine
        .insert_stateful_factory(
            "ModelStatefulType".into(),
            Arc::new(StatefulFactoryFixture(calls.clone())),
        )
        .unwrap();
    calls
}
fn request(h: &Harness, partition: u8, incarnation: i64, mode: u8) -> ProxyRequest {
    let mut request = h.request(partition, incarnation, mode);
    request.service.stateful = true;
    request.service.service_type.name = "ModelStatefulType".into();
    request.unit.current.configuration = (1 << 32) + 1;
    request
}

#[tokio::test]
async fn stateful_open_order_endpoints_access_duplicate_and_graceful_isolation() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let stateless = h.request(1, 1, 0);
    h.send("ReplicaOpen", &stateless).await;
    h.receive("ReplicaOpenReply").await;
    let request = request(&h, 2, 1, 0);
    h.send("ReplicaOpen", &request).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert_eq!(reply.local.service_endpoint, "model://stateful/client");
    assert_eq!(reply.local.replication_endpoint, "model://stateful/control");
    assert_eq!(
        *calls.events.lock().unwrap(),
        vec![
            "create",
            "serviceOpen",
            "replicatorOpen",
            "replicatorPrimary",
            "servicePrimary"
        ]
    );
    let partition = calls.partitions.lock().unwrap()[0].clone();
    assert_eq!(
        partition.access_status().unwrap(),
        (crate::AccessStatus::Granted, crate::AccessStatus::Granted)
    );
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert_eq!(calls.events.lock().unwrap().len(), 5);
    h.send("RAPQuery", &request).await;
    let query: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(
        (
            query.detail.kind,
            query.detail.replica_id,
            query.detail.instance_id
        ),
        (2, 100, -1)
    );
    assert_eq!(
        (query.detail.read_status, query.detail.write_status),
        (1, 1)
    );
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert!(matches!(partition.status(), Err(Error::Closed)));
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        ["replicatorClose", "serviceClose"]
    );
}

#[tokio::test]
async fn stateful_idle_open_never_grants_primary_access_and_drop_orders_none_before_close() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut request = request(&h, 3, 1, 0);
    request.local.current_role = 2;
    h.send("ReplicaOpen", &request).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert!(reply.local.service_endpoint.is_empty());
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (
            crate::AccessStatus::NotPrimary,
            crate::AccessStatus::NotPrimary
        )
    );
    request.flags = 8;
    request.local.current_role = 1; // Native Close-only reconstructed descriptor.
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        [
            "replicatorNone",
            "serviceNone",
            "replicatorClose",
            "serviceClose"
        ]
    );
}

#[tokio::test]
async fn stateful_failed_open_fences_handle_and_abort_preserves_stateless_sibling() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let stateless = h.request(1, 1, 0);
    h.send("ReplicaOpen", &stateless).await;
    h.receive("ReplicaOpenReply").await;
    let failed = request(&h, 2, 1, 1);
    h.send("ReplicaOpen", &failed).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, -456);
    assert!(calls.partitions.lock().unwrap()[0].status().is_err());
    assert_eq!(
        *calls.events.lock().unwrap(),
        ["create", "serviceOpen", "serviceAbort"]
    );
    let mut next = request(&h, 2, 2, 0);
    h.send("ReplicaOpen", &next).await;
    h.receive("ReplicaOpenReply").await;
    next.flags = 4;
    next.local.current_role = 1;
    h.send("ReplicaClose", &next).await;
    h.receive("ReplicaCloseReply").await;
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
    assert_eq!(
        &calls.events.lock().unwrap()[8..],
        [
            "replicatorNone",
            "serviceNone",
            "replicatorAbort",
            "serviceAbort"
        ]
    );
    assert_eq!(h.engine.metrics().snapshot().abort_callbacks, 2);
}

#[tokio::test]
async fn stateful_abort_cancels_open_owner_and_fences_old_incarnation() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut request = request(&h, 2, 1, 8);
    h.send("ReplicaOpen", &request).await;
    for _ in 0..100 {
        if !calls.partitions.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    request.flags = 4;
    h.send("ReplicaClose", &request).await;
    let failed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_ne!(failed.error.error, 0);
    assert!(calls.partitions.lock().unwrap()[0].status().is_err());
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    request.flags = 0;
    request.local.instance = 2;
    request.service.initialization.0 = vec![0];
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let old = calls.partitions.lock().unwrap()[0].clone();
    let new = calls.partitions.lock().unwrap()[1].clone();
    assert!(old.ensure_valid().is_err());
    assert!(new.ensure_valid().is_ok());
    request.local.instance = 1;
    h.send("ReplicaClose", &request).await;
    h.no_reply().await;
    assert!(new.ensure_valid().is_ok());
}

#[tokio::test]
async fn stateful_type_and_partition_cross_kind_takeover_are_rejected() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    assert!(h
        .engine
        .insert_stateful_factory("ModelType".into(), Arc::new(StatefulFactoryFixture(calls)))
        .is_err());
    assert!(h
        .engine
        .insert_factory(
            "ModelStatefulType".into(),
            Arc::new(Factory(h.calls.clone()))
        )
        .is_err());
    let stateless = h.request(2, 1, 0);
    h.send("ReplicaOpen", &stateless).await;
    h.receive("ReplicaOpenReply").await;
    let stateful = request(&h, 2, 2, 0);
    h.send("ReplicaOpen", &stateful).await;
    h.engine.shared.connection.closed().await;
    assert!(h.engine.shared.authority.check().is_err());
}

#[tokio::test]
async fn stateful_close_error_is_normalized_only_after_abort_and_panic_never_acks() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let request = request(&h, 2, 1, 2);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        [
            "replicatorClose",
            "serviceClose",
            "replicatorAbort",
            "serviceAbort"
        ]
    );
    let mut request = request;
    request.local.instance = 2;
    request.service.initialization.0 = vec![11];
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    request.flags = 4;
    h.send("ReplicaClose", &request).await;
    h.engine.shared.connection.closed().await;
    assert!(h.engine.shared.authority.check().is_err());
}

#[tokio::test]
async fn stateful_close_before_open_tombstone_and_newer_owner_are_isolated() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut request = request(&h, 2, 1, 0);
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    h.send("ReplicaOpen", &request).await;
    let closed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_ne!(closed.error.error, 0);
    assert!(calls.events.lock().unwrap().is_empty());
    request.local.instance = 2;
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert!(calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
}

#[tokio::test]
async fn active_stateful_callback_prevents_successful_host_quiescence() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let request = request(&h, 2, 1, 8);
    h.send("ReplicaOpen", &request).await;
    for _ in 0..100 {
        if !calls.partitions.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!calls.partitions.lock().unwrap().is_empty());
    h.engine.shared.authority.retire(); // Runtime::shutdown's first action.
    assert!(h.engine.quiesce().await.is_err());
    assert!(calls.partitions.lock().unwrap()[0].ensure_valid().is_err());
    calls.release.notify_waiters();
}
