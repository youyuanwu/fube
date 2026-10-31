//! Finite core ownership; provider-specific payload/heap policy is separate.
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub(crate) struct Limits {
    owners: Arc<Semaphore>,
    cached: Arc<Semaphore>,
    locks: Arc<Semaphore>,
}
impl Limits {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            owners: Arc::new(Semaphore::new(16)),
            cached: Arc::new(Semaphore::new(4 * 1024 * 1024)),
            locks: Arc::new(Semaphore::new(64)),
        })
    }
    pub(crate) fn owner(&self) -> Result<OwnedSemaphorePermit> {
        self.owners
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("live transaction ownership"))
    }
    pub(crate) fn cached(&self, bytes: usize) -> Result<Arc<OwnedSemaphorePermit>> {
        self.cached
            .clone()
            .try_acquire_many_owned(
                u32::try_from(bytes).map_err(|_| Error::ResourceLimit("cached operation size"))?,
            )
            .map(Arc::new)
            .map_err(|_| Error::ResourceLimit("cached transaction data"))
    }
    pub(crate) fn lock(&self) -> Result<OwnedSemaphorePermit> {
        self.locks
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("transaction lock contexts"))
    }
}
