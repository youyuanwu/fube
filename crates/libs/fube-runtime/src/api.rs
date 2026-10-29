use crate::{lease::Authority, ActivationContext};
use fube_transport::{Error, Result};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Notify;

pub type ServiceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

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
#[derive(Debug, Clone)]
pub struct ServiceContext {
    pub identity: ServiceIdentity,
    pub initialization_data: Vec<u8>,
    pub activation: ActivationContext,
}

/// Factories and service objects are owned for their registered runtime/unit.
/// Callbacks must not block the executor; cancellation is advisory. Abort must
/// synchronously dispose application resources before it returns.
pub trait StatelessFactory: Send + Sync + 'static {
    fn create(&self, context: ServiceContext) -> ServiceFuture<'_, Arc<dyn StatelessService>>;
}
pub trait StatelessService: Send + Sync + 'static {
    /// Return the actual opaque endpoint only after the listener is ready.
    fn open(&self, partition: Partition, cancellation: Cancellation) -> ServiceFuture<'_, String>;
    fn close(&self, cancellation: Cancellation) -> ServiceFuture<'_, ()>;
    fn abort(&self);
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
pub struct Partition {
    pub identity: ServiceIdentity,
    pub kind: PartitionKind,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) authority: Arc<Authority>,
    pub(crate) reporting: Arc<crate::reporting::Reporting>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultType {
    Transient,
    Permanent,
}
impl Partition {
    /// Remaining process authority (send-based node lease and any shorter
    /// callback fence), for application observability only.
    /// It does not grant a new lease or permit reconnect/revival.
    pub fn authority_remaining(&self) -> Result<std::time::Duration> {
        self.ensure_valid()?;
        self.authority.remaining()
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
    pub async fn report_load(&self, metrics: &[crate::LoadMetric]) -> Result<()> {
        self.ensure_valid()?;
        self.reporting.load(metrics).await
    }
    /// Await one matched HM per-report result. No hidden retries; timeout does
    /// not prove the report was never applied. Reuse an explicit sequence when
    /// retrying an uncertain result, rather than inventing a newer update.
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
    pub async fn report_fault(&self, fault: FaultType) -> Result<crate::FaultDelivery> {
        self.ensure_valid()?;
        self.reporting.fault(fault).await
    }
    /// Latest bounded fault-stage status, including retry error/cancellation.
    pub fn fault_delivery(&self) -> Option<crate::FaultDelivery> {
        self.reporting.fault_status()
    }
    /// Same one-way completion/retention semantics as load, for `_MoveCost_`.
    pub async fn report_move_cost(&self, cost: crate::MoveCost) -> Result<()> {
        self.ensure_valid()?;
        self.reporting.move_cost(cost).await
    }
}
