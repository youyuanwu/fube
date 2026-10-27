// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use mssf_runtime::{Cancellation, Epoch, ServiceFuture};
use mssf_transport::{Error, Result};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

/// Finite budgets, validated before a listener is opened.
#[derive(Clone, Debug)]
pub struct Settings {
    pub operation_bytes: usize,
    pub operation_segments: usize,
    pub queue_operations: usize,
    pub queue_bytes: usize,
    pub control_messages: usize,
    pub control_bytes: usize,
    pub connections: usize,
    pub provider_operations: usize,
    pub copies: usize,
    pub retry: Duration,
    pub callback_timeout: Duration,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            operation_bytes: 1024 * 1024,
            operation_segments: 1024,
            queue_operations: 1024,
            queue_bytes: 16 * 1024 * 1024,
            control_messages: 64,
            control_bytes: 1024 * 1024,
            connections: 64,
            provider_operations: 64,
            copies: 64,
            retry: Duration::from_millis(250),
            callback_timeout: Duration::from_secs(8),
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<()> {
        if self.operation_bytes == 0
            || self.operation_bytes > 1024 * 1024
            || self.operation_segments == 0
            || self.operation_segments > 1024
            || self.queue_operations == 0
            || self.queue_operations > 1024
            || self.queue_bytes < self.operation_bytes
            || self.queue_bytes > 16 * 1024 * 1024
            || self.control_messages == 0
            || self.control_messages > 64
            || self.control_bytes == 0
            || self.control_bytes > 1024 * 1024
            || self.connections == 0
            || self.connections > 64
            || self.provider_operations == 0
            || self.provider_operations > 64
            || self.copies == 0
            || self.copies > 64
            || self.retry < Duration::from_millis(10)
            || self.retry > Duration::from_secs(2)
            || self.callback_timeout.is_zero()
            || self.callback_timeout > Duration::from_secs(30)
        {
            return Err(Error::InvalidInput(
                "inconsistent classic replication limits",
            ));
        }
        Ok(())
    }
}

/// Immutable owned provider segments; clones share the payload allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationData {
    segments: Arc<[Arc<[u8]>]>,
    bytes: usize,
}
impl OperationData {
    pub fn new(segments: Vec<Vec<u8>>) -> Result<Self> {
        Self::with_settings(segments, &Settings::default())
    }
    pub fn with_settings(segments: Vec<Vec<u8>>, settings: &Settings) -> Result<Self> {
        settings.validate()?;
        if segments.len() > settings.operation_segments {
            return Err(Error::ResourceLimit("operation segments"));
        }
        let bytes = segments.iter().try_fold(0usize, |n, s| {
            n.checked_add(s.len())
                .ok_or(Error::InvalidInput("operation length overflow"))
        })?;
        if bytes > settings.operation_bytes {
            return Err(Error::ResourceLimit("operation bytes"));
        }
        Ok(Self {
            segments: segments.into_iter().map(Arc::from).collect(),
            bytes,
        })
    }
    pub fn segments(&self) -> &[Arc<[u8]>] {
        &self.segments
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    /// Includes descriptor overhead so empty-buffer floods cannot evade budgets.
    pub fn retained_bytes(&self) -> usize {
        self.bytes + self.segments.len() * std::mem::size_of::<Arc<[u8]>>() + 128
    }
    pub fn to_segments(&self) -> Vec<Vec<u8>> {
        self.segments.iter().map(|s| s.to_vec()).collect()
    }
}

/// Provider enumeration is distinct from the secondary acknowledged operation
/// stream. `None` is EOS; an empty operation is still data.
pub trait OperationDataStream: Send + Sync + 'static {
    fn next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>>;
}

/// Author-owned state contract. Calls may fail; no callback runs under a
/// transport/engine lock. Volatile native builds pass `None` for copy context.
pub trait StateProvider: Send + Sync + 'static {
    fn last_committed(&self) -> Result<i64>;
    /// Reconcile authoritative state to the supplied prior-epoch boundary;
    /// ambiguous tail mutations may survive only in the selected history.
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous_epoch_last: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool>;
    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>>;
    /// Return coherent state AND request-result history at exactly `up_to`.
    /// Unavailable history is an error, never a silently different snapshot.
    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>>;
}

/// An explicit, once-only acknowledgment. Drop/poll does not invoke it.
/// A retired owner rejects late acknowledgments without reviving progress.
pub struct Acknowledgment {
    acknowledged: AtomicBool,
    action: Box<dyn Fn() -> Result<()> + Send + Sync>,
}
impl std::fmt::Debug for Acknowledgment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acknowledgment")
            .field("acknowledged", &self.acknowledged.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl Acknowledgment {
    pub fn new(action: impl Fn() -> Result<()> + Send + Sync + 'static) -> Self {
        Self {
            acknowledged: AtomicBool::new(false),
            action: Box::new(action),
        }
    }
    pub fn acknowledge(&self) -> Result<()> {
        if self
            .acknowledged
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::InvalidInput("operation already acknowledged"));
        }
        (self.action)()
    }
}

#[derive(Debug)]
pub struct Operation {
    pub sequence: i64,
    pub epoch: Epoch,
    pub data: OperationData,
    pub acknowledgment: Acknowledgment,
}
impl Operation {
    pub fn acknowledge(&self) -> Result<()> {
        self.acknowledgment.acknowledge()
    }
}

/// A bounded, single-consumer service stream. Cancellation stops only that poll;
/// it does not acknowledge the operation or dispose its owning replica.
pub struct OperationStream {
    receiver: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Operation>>,
}
impl OperationStream {
    /// Wrap an already-bounded channel owned by an engine/adapter.
    pub fn new(receiver: tokio::sync::mpsc::Receiver<Operation>) -> Self {
        Self {
            receiver: tokio::sync::Mutex::new(receiver),
        }
    }
    pub async fn next(&self, cancellation: Cancellation) -> Result<Option<Operation>> {
        let mut receiver = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::Closed),
            receiver = self.receiver.lock() => receiver,
        };
        tokio::select! {
            _ = cancellation.cancelled() => Err(Error::Closed),
            operation = receiver.recv() => Ok(operation),
        }
    }
}

/// Observable engine boundaries; secondary `received` is native current
/// progress and is NOT its application ACK nor a global quorum proof.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Progress {
    pub assigned: i64,
    pub received: i64,
    pub applied: i64,
    pub committed: i64,
    pub client_completed: i64,
    pub completed: i64,
    pub first_retained: i64,
    pub retained_operations: usize,
    pub retained_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_ack_only_once_and_drop_does_not_ack() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let make = || {
            let calls = calls.clone();
            Acknowledgment::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        };
        drop(make());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let ack = make();
        ack.acknowledge().unwrap();
        assert!(ack.acknowledge().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn settings_and_empty_segment_metadata_are_bounded() {
        let s = Settings {
            provider_operations: 65,
            ..Settings::default()
        };
        assert!(s.validate().is_err());
        assert!(OperationData::new(vec![vec![]; 1025]).is_err());
        let data = OperationData::new(vec![vec![], vec![1], vec![]]).unwrap();
        assert_eq!(data.bytes(), 1);
        assert_eq!(data.segments().len(), 3);
        assert!(data.retained_bytes() > 1);
    }
    #[tokio::test]
    async fn cancellation_is_not_ack_or_stream_consumption() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let stream = OperationStream::new(receiver);
        let cancelled = Cancellation::default();
        cancelled.cancel();
        assert!(stream.next(cancelled).await.is_err());
        drop(sender);
        assert!(stream
            .next(Cancellation::default())
            .await
            .unwrap()
            .is_none());
    }
}
