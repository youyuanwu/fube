use crate::{lease::Authority, Error, Result};
pub use fube_core::sdk::{
    ApiFuture, Cancellation, FaultType, IStatelessServiceFactory, IStatelessServiceInstance,
    IStatelessServicePartition, Int64PartitionInformation, NamedPartitionInformation,
    PartitionKind, ServiceIdentity, ServicePartitionInformation, SingletonPartitionInformation,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Generation-bound partition. Public identity is observational; changing it
/// never changes the private native report routing identity.
#[derive(Debug, Clone)]
pub(crate) struct Partition {
    pub identity: ServiceIdentity,
    pub kind: PartitionKind,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) authority: Arc<Authority>,
    pub(crate) reporting: Arc<crate::reporting::Reporting>,
}
impl Partition {
    /// Remaining process authority for observation, not lease renewal.
    pub fn authority_remaining(&self) -> Result<std::time::Duration> {
        self.ensure_valid()?;
        self.authority.remaining()
    }
}
impl IStatelessServicePartition for Partition {
    fn get_partition_info(&self) -> Result<ServicePartitionInformation> {
        self.ensure_valid()?;
        Ok(self.kind.information(self.identity.partition_id))
    }
    fn report_load(&self, metrics: &[crate::LoadMetric]) -> Result<()> {
        self.ensure_valid()?;
        let reporting = self.reporting.clone();
        let metrics = metrics.to_vec();
        self.reporting
            .complete(async move { reporting.load(&metrics).await })
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.ensure_valid()?;
        let reporting = self.reporting.clone();
        self.reporting
            .complete(async move { reporting.fault(fault).await.map(|_| ()) })
    }
    fn report_move_cost(&self, cost: crate::MoveCost) -> Result<()> {
        self.ensure_valid()?;
        let reporting = self.reporting.clone();
        self.reporting
            .complete(async move { reporting.move_cost(cost).await })
    }
    fn report_partition_health(&self, health: &crate::HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Partition)
            .map(|_| ())
    }
    fn report_instance_health(&self, health: &crate::HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Instance)
            .map(|_| ())
    }
    fn report_partition_health2(
        &self,
        health: &crate::HealthInformation,
        _: Option<&crate::HealthReportSendOption>,
    ) -> Result<()> {
        IStatelessServicePartition::report_partition_health(self, health)
    }
    fn report_instance_health2(
        &self,
        health: &crate::HealthInformation,
        _: Option<&crate::HealthReportSendOption>,
    ) -> Result<()> {
        IStatelessServicePartition::report_instance_health(self, health)
    }
}
impl Partition {
    pub(crate) fn health(
        &self,
        health: &crate::HealthInformation,
        target: crate::reporting::HealthTarget,
    ) -> Result<crate::reporting::HealthReceipt> {
        self.ensure_valid()?;
        let health = crate::HealthReport::try_from(health)?;
        let reporting = self.reporting.clone();
        self.reporting
            .complete(async move { reporting.health(&health, target).await })
    }
    pub fn ensure_valid(&self) -> Result<()> {
        self.authority.check()?;
        if self.valid.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }
    /// Await local complete-frame write, NOT native acknowledgment/persistence.
    /// No automatic load retry. Unsent obsolete work is cancelled at the writer.
    /// Once the writer passes its validity boundary, delivery is uncertain on
    /// timeout/cancellation. Native load/move retention has no incarnation fence.
    #[cfg(test)]
    pub async fn report_load(&self, metrics: &[crate::LoadMetric]) -> Result<()> {
        self.ensure_valid()?;
        self.reporting.load(metrics).await
    }
    /// Await one matched HM per-report result. No hidden retries; timeout does
    /// not prove the report was never applied. Reuse an explicit sequence when
    /// retrying an uncertain result, rather than inventing a newer update.
    #[cfg(test)]
    pub async fn report_health(
        &self,
        report: &crate::HealthReport,
    ) -> Result<crate::HealthReceipt> {
        self.ensure_valid()?;
        self.reporting
            .health(report, crate::reporting::HealthTarget::Instance)
            .await
    }
    /// Await initial local write (or explicitly coalesce a pending stage).
    /// Retries retain the bound native identity until closure. This is not a
    /// native fault acknowledgment or a promise of replacement.
    /// An initial write error does not remove the bounded pending fault stage:
    /// inspect `fault_delivery()` for retry outcomes. Pending transient waiters
    /// fail explicitly if permanent supersedes them before initial completion.
    /// Initial completion never changes after a retry; the snapshot may change.
    #[cfg(test)]
    pub async fn report_fault(&self, fault: FaultType) -> Result<crate::FaultDelivery> {
        self.ensure_valid()?;
        self.reporting.fault(fault).await
    }
    /// Latest bounded fault-stage status, including retry error/cancellation.
    pub fn fault_delivery(&self) -> Option<crate::FaultDelivery> {
        self.reporting.fault_status()
    }
    /// Same one-way completion/retention semantics as load, for `_MoveCost_`.
    #[cfg(test)]
    pub async fn report_move_cost(&self, cost: crate::MoveCost) -> Result<()> {
        self.ensure_valid()?;
        self.reporting.move_cost(cost).await
    }
}
