use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::watch;

pub type Result<T> = std::result::Result<T, Error>;
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    InvalidInput,
    Conflict,
    NotFound,
    Capacity,
    Closed,
    Fenced,
    Busy,
    Io,
    Corrupt,
    Incompatible,
    Sqlite,
    Unknown,
    NotExecuted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for Error {}

/// Exact UTF-8 namespace; no case folding or native enum ordinal encoding.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecordId {
    pub record_type: String,
    pub id: String,
}

impl RecordId {
    pub fn new(record_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            record_type: record_type.into(),
            id: id.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub key: RecordId,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub enum Request {
    Insert(Record),
    Update(Record),
    Delete(RecordId),
    Enumerate(String),
}

impl Request {
    pub(crate) fn key(&self) -> Option<&RecordId> {
        match self {
            Self::Insert(r) | Self::Update(r) => Some(&r.key),
            Self::Delete(k) => Some(k),
            Self::Enumerate(_) => None,
        }
    }

    pub(crate) fn charge(&self, limits: &Settings) -> Result<usize> {
        let invalid = |message| Error::new(ErrorKind::InvalidInput, message);
        let bounded_text = |text: &str, max| {
            if text.is_empty() || text.len() > max || text.contains('\0') {
                Err(invalid("empty, NUL-containing or oversized type/ID"))
            } else {
                Ok(())
            }
        };
        let mut bytes = REQUEST_OVERHEAD;
        if let Some(key) = self.key() {
            bounded_text(&key.record_type, limits.max_type_bytes)?;
            bounded_text(&key.id, limits.max_id_bytes)?;
            bytes = bytes
                .checked_add(key.record_type.capacity())
                .and_then(|b| b.checked_add(key.id.capacity()))
                .ok_or_else(|| invalid("request accounting overflow"))?;
        }
        match self {
            Self::Insert(r) | Self::Update(r) => {
                if r.value.len() > limits.max_value_bytes {
                    return Err(invalid("oversized value"));
                }
                bytes = bytes
                    .checked_add(r.value.capacity())
                    .ok_or_else(|| invalid("request accounting overflow"))?;
            }
            Self::Enumerate(t) => {
                bounded_text(t, limits.max_type_bytes)?;
                bytes = bytes
                    .checked_add(t.capacity())
                    .ok_or_else(|| invalid("request accounting overflow"))?;
            }
            Self::Delete(_) => {}
        }
        Ok(bytes)
    }
}

pub(crate) const REQUEST_OVERHEAD: usize = 512;
pub(crate) const RECORD_OVERHEAD: usize = 128;
pub(crate) fn record_bytes(record: &Record) -> usize {
    RECORD_OVERHEAD + record.key.record_type.len() + record.key.id.len() + record.value.len()
}

#[derive(Debug, Eq, PartialEq)]
pub enum Output {
    Written,
    Snapshot(Vec<Record>),
}

#[derive(Clone, Debug)]
pub enum Status {
    Pending,
    /// Supervision expired, not proof of rollback. May later become Complete.
    Unknown(Error),
    Complete(Result<Arc<Output>>),
}

/// Caller-owned observation, not ownership of the underlying operation.
#[derive(Clone)]
pub struct Operation {
    pub id: u64,
    pub(crate) rx: watch::Receiver<Status>,
}

impl Operation {
    pub fn status(&self) -> Status {
        self.rx.borrow().clone()
    }

    /// Wait for a known result or the supervisor's finite unknown-outcome report.
    pub async fn wait(&self) -> Result<Arc<Output>> {
        let mut rx = self.rx.clone();
        loop {
            match rx.borrow().clone() {
                Status::Complete(result) => return result,
                Status::Unknown(error) => return Err(error),
                Status::Pending => {}
            }
            rx.changed()
                .await
                .map_err(|_| Error::new(ErrorKind::Unknown, "supervisor lost"))?;
        }
    }

    /// Wait finitely for late resolution after an Unknown status.
    pub async fn resolve(&self, budget: Duration) -> Result<Arc<Output>> {
        let mut rx = self.rx.clone();
        let wait = async {
            loop {
                if let Status::Complete(result) = rx.borrow().clone() {
                    return result;
                }
                rx.changed()
                    .await
                    .map_err(|_| Error::new(ErrorKind::Unknown, "supervisor lost"))?;
            }
        };
        tokio::time::timeout(budget, wait)
            .await
            .map_err(|_| Error::new(ErrorKind::Unknown, "resolution still pending"))?
    }
}

/// Finite policy budgets. Byte accounting is logical, not an allocator/RSS quota.
#[derive(Clone, Debug)]
pub struct Settings {
    pub max_type_bytes: usize,
    pub max_id_bytes: usize,
    pub max_value_bytes: usize,
    pub max_records: usize,
    pub max_stored_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub max_outstanding: usize,
    pub max_queued_bytes: usize,
    pub operation_timeout: Duration,
    pub close_timeout: Duration,
    pub busy_timeout: Duration,
    pub max_database_bytes: u64,
    pub max_wal_bytes: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_type_bytes: 64,
            max_id_bytes: 512,
            max_value_bytes: 1024 * 1024,
            max_records: 4096,
            max_stored_bytes: 32 * 1024 * 1024,
            max_snapshot_bytes: 8 * 1024 * 1024,
            max_outstanding: 64,
            max_queued_bytes: 8 * 1024 * 1024,
            operation_timeout: Duration::from_secs(5),
            close_timeout: Duration::from_secs(10),
            busy_timeout: Duration::from_millis(100),
            max_database_bytes: 64 * 1024 * 1024,
            max_wal_bytes: 8 * 1024 * 1024,
        }
    }
}

impl Settings {
    pub(crate) fn validate(&self) -> Result<()> {
        let sizes = [
            self.max_type_bytes,
            self.max_id_bytes,
            self.max_value_bytes,
            self.max_records,
            self.max_stored_bytes,
            self.max_snapshot_bytes,
            self.max_outstanding,
            self.max_queued_bytes,
        ];
        let timers = [
            self.operation_timeout,
            self.close_timeout,
            self.busy_timeout,
        ];
        if sizes.iter().any(|&v| v == 0 || v > i32::MAX as usize)
            || self.max_outstanding > 65536
            || timers
                .iter()
                .any(|t| t.is_zero() || *t > Duration::from_secs(3600))
            || self.max_database_bytes < 16384
            || self.max_database_bytes > i32::MAX as u64
            || self.max_wal_bytes < 4096
            || self.max_wal_bytes > i32::MAX as u64
            || self
                .max_type_bytes
                .checked_add(self.max_id_bytes)
                .and_then(|v| v.checked_add(self.max_value_bytes))
                .is_none_or(|v| v > i32::MAX as usize - 4096)
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid finite storage budgets",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    Starting,
    Open,
    Closing,
    Fenced,
    Closed,
}

#[derive(Clone, Debug)]
pub struct Stats {
    pub lifecycle: Lifecycle,
    pub outstanding: usize,
    pub queued_bytes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct CloseReport {
    pub accepted: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub not_executed: u64,
    pub first_failure: Option<Error>,
}

impl CloseReport {
    pub fn is_clean(&self) -> bool {
        self.first_failure.is_none()
    }
}

/// Shared logical contract; synchronous fail-fast admission, asynchronous results.
pub trait KeyValueStore: Send + Sync {
    fn submit(&self, request: Request) -> Result<Operation>;
    fn stats(&self) -> Stats;
    fn close(&self) -> StoreFuture<'_, CloseReport>;
}
