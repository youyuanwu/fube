use super::*;
use fube_runtime::{
    FaultType, HealthInformation, HealthReportSendOption, LoadMetric, MoveCost,
    ServicePartitionAccessStatus as Access, ServicePartitionInformation,
};

pub(super) struct WriteAccess(pub Mutex<Access>);
pub(super) struct AccessPartition {
    pub inner: Arc<dyn IStatefulServicePartition>,
    pub access: Arc<WriteAccess>,
}
impl IStatefulServicePartition for AccessPartition {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation> {
        self.inner.get_partition_information()
    }
    fn get_read_status(&self) -> Result<Access> {
        self.inner.get_read_status()
    }
    fn get_write_status(&self) -> Result<Access> {
        Ok(*self.access.0.lock().unwrap())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_transaction_keeps_transient_first_cache_but_lower_denies_actual_admission() {
    for delegating in [false, true] {
        let access = Arc::new(WriteAccess(Mutex::new(Access::Granted)));
        let fixture = Fixture::start_using_access(
            uuid::Uuid::new_v4(),
            1,
            ReplicaRole::Primary,
            delegating,
            Settings::default(),
            Some(access.clone()),
        )
        .await;
        fixture.configure(vec![]).await;
        fixture.add("fabric:/access").await;
        let root = fixture.owner.transactional_replicator();
        let provider = fixture.provider("fabric:/access");
        for (status, expected) in [
            (
                Access::ReconfigurationPending,
                fube_runtime::ErrorKind::ReconfigurationPending,
            ),
            (
                Access::NoWriteQuorum,
                fube_runtime::ErrorKind::NoWriteQuorum,
            ),
        ] {
            let tx = root.create_transaction().unwrap();
            let input = tx.clone();
            let access = access.clone();
            let control = fixture.control.clone();
            let id = provider.id;
            fixture
                .native
                .execution()
                .try_reserve()
                .unwrap()
                .run_owned(move || {
                    *access.0.lock().unwrap() = status;
                    let before = control.get_current_progress()?;
                    // StateManager permits transient preflight for the cached first
                    // operation; the ordinary lower denies the second's real write.
                    input.add_operation(
                        None,
                        None,
                        Some(OperationData::new(vec![vec![1]])?),
                        id,
                        None,
                    )?;
                    assert_eq!(control.get_current_progress()?, before);
                    let error = input
                        .add_operation(
                            None,
                            None,
                            Some(OperationData::new(vec![vec![2]])?),
                            id,
                            None,
                        )
                        .unwrap_err();
                    assert_eq!(error.kind(), expected);
                    assert_eq!(control.get_current_progress()?, before);
                    *access.0.lock().unwrap() = Access::Granted;
                    Ok(())
                })
                .await
                .unwrap();
            tx.commit().await.unwrap();
            tx.dispose().unwrap();
        }
    }
}
