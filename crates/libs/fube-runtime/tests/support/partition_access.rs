use super::*;
use fube_runtime::{
    HealthInformation, HealthReportSendOption, LoadMetric, MoveCost,
    ServicePartitionAccessStatus as Access, ServicePartitionInformation,
};
use std::{collections::VecDeque, sync::atomic::AtomicUsize};

pub(super) struct AccessControl {
    values: Mutex<VecDeque<Result<Access>>>,
    reads: AtomicUsize,
}
impl AccessControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            values: Mutex::new(VecDeque::from([Ok(Access::Granted)])),
            reads: 0.into(),
        })
    }
    fn set(&self, values: impl IntoIterator<Item = Result<Access>>) {
        *self.values.lock().unwrap() = values.into_iter().collect();
        self.reads.store(0, Ordering::SeqCst);
    }
    fn get(&self) -> Result<Access> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut values = self.values.lock().unwrap();
        if values.len() > 1 {
            values.pop_front().unwrap()
        } else {
            values.front().unwrap().clone()
        }
    }
}
pub(super) struct AccessPartition {
    pub inner: Arc<dyn IStatefulServicePartition>,
    pub access: Arc<AccessControl>,
}
impl IStatefulServicePartition for AccessPartition {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation> {
        self.inner.get_partition_information()
    }
    fn get_read_status(&self) -> Result<Access> {
        self.inner.get_read_status()
    }
    fn get_write_status(&self) -> Result<Access> {
        self.access.get()
    }
    fn report_load(&self, metrics: &[LoadMetric]) -> Result<()> {
        self.inner.report_load(metrics)
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.inner.report_fault(fault)
    }
    fn report_move_cost(&self, cost: MoveCost) -> Result<()> {
        self.inner.report_move_cost(cost)
    }
    fn report_partition_health(&self, health: &HealthInformation) -> Result<()> {
        self.inner.report_partition_health(health)
    }
    fn report_replica_health(&self, health: &HealthInformation) -> Result<()> {
        self.inner.report_replica_health(health)
    }
    fn report_partition_health2(
        &self,
        health: &HealthInformation,
        options: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.inner.report_partition_health2(health, options)
    }
    fn report_replica_health2(
        &self,
        health: &HealthInformation,
        options: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.inner.report_replica_health2(health, options)
    }
}

async fn fixture() -> (Fixture, Arc<AccessControl>) {
    let access = AccessControl::new();
    let supplied_access = access.clone();
    let provider = MemoryProvider::new();
    let supplied = provider.clone();
    let (created, received) = oneshot::channel();
    let host = rap::Host::start(
        uuid::Uuid::new_v4(),
        1,
        ReplicaRole::Primary,
        move |native| {
            Box::new(Factory {
                forwarding: false,
                access: Some(supplied_access),
                native,
                provider: supplied,
                settings: Settings::default(),
                created: Arc::new(Mutex::new(Some(created))),
                opening_cancel: Arc::default(),
                sample_open_progress: false,
                retry_open_progress: false,
            })
        },
    )
    .await;
    let (engine, native) = received.await.unwrap();
    host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    until(|| engine.snapshot().ready).await;
    (
        Fixture {
            engine,
            provider,
            native,
            host,
        },
        access,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_primary_independent_access_denials_and_getter_errors_reject_without_lsn() {
    let (p, access) = fixture().await;
    for (status, kind, code) in [
        (Ok(Access::NotPrimary), ErrorKind::NotPrimary, 0x80071bc6u32),
        (Ok(Access::Invalid), ErrorKind::NotPrimary, 0x80071bc6),
        (
            Ok(Access::ReconfigurationPending),
            ErrorKind::ReconfigurationPending,
            0x80071bca,
        ),
        (
            Ok(Access::NoWriteQuorum),
            ErrorKind::NoWriteQuorum,
            0x80071bc5,
        ),
        (
            Err(Error::InvalidInput("failed getter")),
            ErrorKind::NotPrimary,
            0x80071bc6,
        ),
    ] {
        for recheck in [false, true] {
            let before = p.engine.snapshot().progress.assigned;
            access.set(if recheck {
                vec![Ok(Access::Granted), status.clone()]
            } else {
                vec![status.clone()]
            });
            let error = p
                .engine
                .control
                .get_state_replicator()
                .replicate(
                    OperationData::new(vec![vec![1]]).unwrap(),
                    Cancellation::default(),
                )
                .await
                .err()
                .expect("access denial");
            assert_eq!(error.kind(), kind);
            assert_eq!(error.native_result_code(), code as i32 as i64);
            assert!(!error.is_uncertain());
            assert_eq!(
                access.reads.load(Ordering::SeqCst),
                if recheck { 2 } else { 1 }
            );
            let after = p.engine.snapshot();
            assert!(
                after.ready,
                "denial must not be simulated by lower readiness"
            );
            assert_eq!(after.role, ReplicaRole::Primary);
            assert_eq!(after.progress.assigned, before);
            assert_eq!(after.owned_operations, 0);
        }
    }
    access.set([Ok(Access::Granted)]);
    p.write("first-allowed").await;
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_partition_revocation_is_rechecked_before_assignment() {
    let (p, access) = fixture().await;
    let (entered, received) = std_mpsc::sync_channel(1);
    let (release, blocked) = std_mpsc::sync_channel(1);
    *p.engine.shared.write_assignment_pause.lock().unwrap() = Some((entered, blocked));
    let state = p.engine.control.get_state_replicator();
    let write = tokio::spawn(async move {
        state
            .replicate(
                OperationData::new(vec![vec![1]]).unwrap(),
                Cancellation::default(),
            )
            .await
    });
    received
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert_eq!(access.reads.load(Ordering::SeqCst), 1);
    access.set([Ok(Access::NoWriteQuorum)]);
    release.send(()).unwrap();
    assert_eq!(
        write.await.unwrap().err().unwrap().kind(),
        ErrorKind::NoWriteQuorum
    );
    assert_eq!(p.engine.snapshot().progress.assigned, 0);
    assert!(p.engine.snapshot().ready);
    access.set([Ok(Access::Granted)]);
    p.write("after-queued-denial").await;
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_lower_continuation_survives_later_partition_access_revocation() {
    let (p, access) = fixture().await;
    let (network, _incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let mut peer = p.descriptor(ReplicaRole::ActiveSecondary);
    peer.replica_id = 2;
    peer.node_id = [2; 16];
    peer.incarnation = 12;
    peer.address = Endpoint {
        actor: Actor {
            partition: p.native.identity().partition_id,
            replica: 2,
            incarnation: uuid::Uuid::new_v4(),
        },
        address: network.address().to_owned(),
        transport_id: None,
    }
    .to_string();
    p.host
        .configure(config(EPOCH, vec![peer], 2))
        .await
        .unwrap();
    let data = Mutation {
        id: "accepted".into(),
        key: "key".into(),
        value: "value".into(),
    }
    .operation()
    .unwrap();
    let mut accepted = p
        .engine
        .control
        .get_state_replicator()
        .replicate(data.clone(), Cancellation::default())
        .await
        .unwrap();
    p.provider
        .apply_replication(accepted.sequence_number, &data)
        .unwrap();
    access.set([Ok(Access::NotPrimary)]);
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(30),
        accepted.completion.wait(Cancellation::default())
    )
    .await
    .is_err());
    p.host.configure(config(EPOCH, vec![], 1)).await.unwrap();
    assert_eq!(
        accepted
            .completion
            .wait(Cancellation::default())
            .await
            .unwrap(),
        1
    );
    assert_eq!(p.engine.snapshot().progress.committed, 1);
    let error = p
        .engine
        .control
        .get_state_replicator()
        .replicate(data, Cancellation::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::NotPrimary);
    assert_eq!(p.engine.snapshot().progress.assigned, 1);
    network.shutdown_checked().unwrap();
}
