use crate::{lease::Authority, ActivationContext};
use mssf_transport::{Error, Result};
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
    pub(crate) fn cancel(&self) {
        self.0.value.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}

/// Generation-bound partition. Reporting is explicitly unsupported by this
/// initial profile; calls never log success or send empty placeholder reports.
#[derive(Debug, Clone)]
pub struct Partition {
    pub identity: ServiceIdentity,
    pub kind: PartitionKind,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) authority: Arc<Authority>,
}
#[derive(Debug, Clone, Copy)]
pub enum FaultType {
    Transient,
    Permanent,
}
impl Partition {
    /// Remaining send-based authority, for application observability only.
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
    pub fn report_load(&self, _metrics: &[(String, u32)]) -> Result<()> {
        self.ensure_valid()?;
        Err(Error::InvalidInput(
            "load reporting unsupported by runtime profile",
        ))
    }
    pub fn report_health(&self, _source: &str, _property: &str, _description: &str) -> Result<()> {
        self.ensure_valid()?;
        Err(Error::InvalidInput(
            "health reporting unsupported by runtime profile",
        ))
    }
    pub fn report_fault(&self, _fault: FaultType) -> Result<()> {
        self.ensure_valid()?;
        Err(Error::InvalidInput(
            "fault reporting unsupported by runtime profile",
        ))
    }
    pub fn report_move_cost(&self, _cost: u32) -> Result<()> {
        self.ensure_valid()?;
        Err(Error::InvalidInput(
            "move-cost reporting unsupported by runtime profile",
        ))
    }
}
