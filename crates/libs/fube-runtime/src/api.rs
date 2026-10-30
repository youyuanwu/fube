use crate::{lease::Authority, Error, Result};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Notify;

/// Object-safe boxed future with the shared typed SDK error. Cancellation is
/// advisory, not rollback or proof of disposal of accepted work.
pub type ApiFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Native replica ID is the public stateless instance ID; incarnation is the
/// separate RAP replica-instance ID and must never be conflated with it.
#[derive(Debug, Clone)]
pub struct ServiceIdentity {
    pub service_name: String,
    pub service_type: String,
    pub partition_id: uuid::Uuid,
    pub instance_id: i64,
    pub incarnation: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKind {
    Singleton,
    Int64Range { low: i64, high: i64 },
    Named(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonPartitionInformation {
    pub id: uuid::Uuid,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Int64PartitionInformation {
    pub id: uuid::Uuid,
    pub low_key: i64,
    pub high_key: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedPartitionInformation {
    pub id: uuid::Uuid,
    pub name: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServicePartitionInformation {
    Invalid,
    Singleton(SingletonPartitionInformation),
    Int64Range(Int64PartitionInformation),
    Named(NamedPartitionInformation),
}
impl PartitionKind {
    pub(crate) fn information(&self, id: uuid::Uuid) -> ServicePartitionInformation {
        match self {
            Self::Singleton => {
                ServicePartitionInformation::Singleton(SingletonPartitionInformation { id })
            }
            Self::Int64Range { low, high } => {
                ServicePartitionInformation::Int64Range(Int64PartitionInformation {
                    id,
                    low_key: *low,
                    high_key: *high,
                })
            }
            Self::Named(name) => ServicePartitionInformation::Named(NamedPartitionInformation {
                id,
                name: name.clone(),
            }),
        }
    }
}

/// UTF-8/UUID and boxed-future representations are not COM/source compatible.
/// Construction is synchronous and fallible on a supervised native worker.
pub trait IStatelessServiceFactory: Send + Sync + 'static {
    fn create_instance(
        &self,
        service_type: String,
        service_name: String,
        initialization_data: &[u8],
        partition_id: uuid::Uuid,
        instance_id: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>>;
}
pub trait IStatelessServiceInstance: Send + Sync + 'static {
    /// Return the actual opaque endpoint only after the listener is ready.
    fn open(
        &self,
        partition: Arc<dyn IStatelessServicePartition>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, String>;
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn abort(&self);
}
pub trait IStatelessServicePartition: Send + Sync + 'static {
    fn get_partition_info(&self) -> Result<ServicePartitionInformation>;
    /// Local frame-write completion, not native acknowledgment or durability.
    fn report_load(&self, metrics: &[crate::LoadMetric]) -> Result<()>;
    fn report_fault(&self, fault: FaultType) -> Result<()>;
    fn report_move_cost(&self, cost: crate::MoveCost) -> Result<()>;
    fn report_partition_health(&self, health: &crate::HealthInformation) -> Result<()>;
    /// Exact HM-result wait, unlike SDK default admission/batching.
    fn report_instance_health(&self, health: &crate::HealthInformation) -> Result<()>;
    fn report_partition_health2(
        &self,
        health: &crate::HealthInformation,
        options: Option<&crate::HealthReportSendOption>,
    ) -> Result<()>;
    fn report_instance_health2(
        &self,
        health: &crate::HealthInformation,
        options: Option<&crate::HealthReportSendOption>,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
struct CancelState {
    value: AtomicBool,
    changed: Notify,
}
#[derive(Debug, Clone, Default)]
pub struct Cancellation(Arc<CancelState>);
impl Cancellation {
    pub fn is_cancelled(&self) -> bool {
        self.0.value.load(Ordering::Acquire)
    }
    pub async fn cancelled(&self) {
        loop {
            let wake = self.0.changed.notified();
            if self.is_cancelled() {
                return;
            }
            wake.await;
        }
    }
    /// Signal cancellation of locally owned work. This never grants native
    /// authority and is not proof that the operation has completed or cleaned up.
    pub fn cancel(&self) {
        self.0.value.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FaultType {
    Invalid = 0,
    Permanent = 1,
    Transient = 2,
}
impl Partition {
    /// Remaining process authority (send-based node lease and any shorter
    /// callback fence), for application observability only.
    /// It does not grant a new lease or permit reconnect/revival.
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
    fn report_partition_health(&self, _: &crate::HealthInformation) -> Result<()> {
        self.ensure_valid()?;
        Err(Error::new(
            crate::ErrorKind::Unsupported,
            "partition health routing is not enabled",
        ))
    }
    fn report_instance_health(&self, health: &crate::HealthInformation) -> Result<()> {
        self.ensure_valid()?;
        let health = crate::HealthReport::try_from(health)?;
        let reporting = self.reporting.clone();
        self.reporting
            .complete(async move { reporting.health(&health).await.map(|_| ()) })
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
        self.reporting.health(report).await
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
