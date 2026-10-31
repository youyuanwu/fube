use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const EVENT_CAPACITY: usize = 64;

/// Best-effort fube diagnostics, not SF callbacks or native authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeEvent {
    /// The reply was written, not necessarily received by the native caller.
    QueryReplySent {
        request_id: fube_transport::message::MessageId,
    },
    /// Dispatch tasks have drained; service disposal/unregistration may remain.
    DispatchStopped,
}

/// Bounded observational counters, not registration or compatibility evidence.
/// Native acceptance must corroborate these through independent node/RPC data.
#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    pub(crate) opens: AtomicU64,
    pub(crate) closes: AtomicU64,
    pub(crate) aborts: AtomicU64,
    pub(crate) queries: AtomicU64,
    pub(crate) updates: AtomicU64,
    pub(crate) open_failures: AtomicU64,
    pub(crate) close_failures: AtomicU64,
    pub(crate) abort_callbacks: AtomicU64,
}
#[derive(Debug, Clone, Copy)]
pub struct RuntimeCounters {
    pub open_commands: u64,
    pub close_commands: u64,
    pub abort_commands: u64,
    pub queries: u64,
    pub service_updates: u64,
    pub open_failures: u64,
    pub close_failures: u64,
    pub abort_callbacks: u64,
}
impl RuntimeMetrics {
    pub fn snapshot(&self) -> RuntimeCounters {
        RuntimeCounters {
            open_commands: self.opens.load(Ordering::Relaxed),
            close_commands: self.closes.load(Ordering::Relaxed),
            abort_commands: self.aborts.load(Ordering::Relaxed),
            queries: self.queries.load(Ordering::Relaxed),
            service_updates: self.updates.load(Ordering::Relaxed),
            open_failures: self.open_failures.load(Ordering::Relaxed),
            close_failures: self.close_failures.load(Ordering::Relaxed),
            abort_callbacks: self.abort_callbacks.load(Ordering::Relaxed),
        }
    }
}
