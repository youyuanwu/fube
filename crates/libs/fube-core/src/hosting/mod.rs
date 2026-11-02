//! Rust host ownership adapters, not Service Fabric SDK authority interfaces.
//! Backends retain their real deadline/fencing policy; context is transported
//! opaquely and never interpreted as permission by this module.

mod execution;
mod standalone;

pub use execution::*;
use std::{sync::Arc, time::Instant};

use crate::{ApiFuture, Result};
use standalone::StandaloneAuthority;

/// A reserved backend deadline. Drop releases the same reservation; arming and
/// child-budget changes must affect the host's actual independent fence.
pub trait ExecutionDeadline: Send + Sync {
    fn arm_until(&self, until: Instant);
    fn set_effective_until(&self, until: Instant);
}

/// An accounted OS thread. Joining must not block an async scheduler.
pub trait ExecutionThread: Send + Sync {
    fn join_finished(&self) -> ApiFuture<'_, ()>;
}

/// The host's terminal authority, not a replication role or proof issuer.
/// Retirement is monotonic, including the original deadline of late cleanup.
pub trait ExecutionAuthority: Send + Sync {
    fn check(&self) -> Result<()>;
    fn retire(&self);
    fn retire_at(&self, until: Instant);
    fn reserve_callback_deadline(
        self: Arc<Self>,
        cleanup: bool,
    ) -> Result<Box<dyn ExecutionDeadline>>;
    fn own_thread(&self, thread: std::thread::JoinHandle<()>) -> Arc<dyn ExecutionThread>;
}

/// Explicit full/primary epoch-domain conversion, not an inherent SDK method.
pub trait EpochDomains {
    fn primary(self) -> crate::Epoch;
}
impl EpochDomains for crate::Epoch {
    fn primary(self) -> Self {
        if self == Self::INVALID {
            return self;
        }
        Self {
            data_loss_number: self.data_loss_number,
            configuration_number: self.configuration_number & !0xffff_ffff,
        }
    }
}
