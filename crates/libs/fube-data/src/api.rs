// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use fube_runtime::{ApiFuture as ServiceFuture, Cancellation, Epoch, Error, Result};
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
            return Err(Error::new(
                fube_runtime::ErrorKind::ReplicationOperationTooLarge,
                "operation segments",
            ));
        }
        let bytes = segments.iter().try_fold(0usize, |n, s| {
            n.checked_add(s.len())
                .ok_or(Error::InvalidInput("operation length overflow"))
        })?;
        if bytes > settings.operation_bytes {
            return Err(Error::new(
                fube_runtime::ErrorKind::ReplicationOperationTooLarge,
                "operation bytes",
            ));
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
/// Boxed futures and owned immutable segments are Rust representations, not a
/// COM ABI. In the built-in engine, construction/polls/final owned captures run
/// on the bounded provider/cleanup lanes under their original deadlines.
pub trait IOperationDataStream: Send + Sync + 'static {
    /// Async payload enumeration: Some(empty) is data, None is EOS. No service
    /// ACK exists on this provider stream; a caller owns direct invocations.
    fn get_next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>>;
}

/// Author-owned state contract. Calls may fail; no callback runs under a
/// transport/engine lock. Volatile native builds pass `None` for copy context.
/// Native/managed callback responsibilities and synchrony are preserved; Rust
/// uses fallible scalar getters, owned stream Arcs and object-safe futures.
/// There is deliberately no replay supertrait or discovery hook. The engine
/// reserves callback/capture ownership before invoking application code; an
/// application that calls its provider directly owns that invocation itself.
pub trait IStateProvider: Send + Sync + 'static {
    /// Synchronous fallible provider progress, queried during lower construction
    /// before its later Open. Initialize the provider before constructing it.
    /// MemoryProvider reports local application; a logging provider reports its
    /// actual log tail, not stable/visible transaction progress or durable commit.
    fn get_last_committed_sequence_number(&self) -> Result<i64>;
    /// Apply the provider's original-operation/primary epoch boundary. A logging
    /// provider recognizes its represented duplicate before checking tail equality;
    /// genuine new incompatible epochs must still fail. The lower preserves the
    /// callback result without querying/equating independent upper and lower tails.
    /// Secondary admission-only controls do not invoke this callback.
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous_epoch_last: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    /// True means authoritative provider state changed, not merely an epoch
    /// notification. The actual bool reaches canonical primary control.
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool>;
    /// Synchronous payload-stream factory. This volatile engine does not send
    /// a bidirectional/persisted context to get_copy_state; it supplies None.
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>>;
    /// Return coherent copy state for the inclusive requested `up_to`; the included
    /// MemoryProvider copies exact history AND its request-result ledger. A native
    /// staged upper-log provider may extend its checkpoint horizon internally and
    /// must preserve its copy metadata/overlap semantics; lower progress is separate.
    /// Custom providers own their data, retry and reconciliation policies.
    /// Unavailable history is an error, never a silently different snapshot.
    /// Construction is synchronous but the engine calls it on the provider lane
    /// only after provider progress reaches at least the requested horizon.
    fn get_copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>>;
}

/// Supported native operation kind. Atomic groups and synthetic application EOS
/// operations are not supported by this volatile, RequireServiceAck profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum OperationType {
    Normal = 0x0001,
}

/// Native metadata responsibilities (not origin/epoch or diagnostic authority).
/// Only normal operations with invalid atomic-group ID (-1) are delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationMetadata {
    pub operation_type: OperationType,
    pub sequence_number: i64,
    pub atomic_group_id: i64,
}

/// Destination copy AND replication items require explicit application-before-
/// ACK. Poll, drop and cancellation never acknowledge; late ACKs are fenced.
/// Original operation epoch is available only through the explicit origin
/// stream capability in [`crate::extensions`], not native metadata.
pub trait IOperation: Send + Sync + 'static {
    /// Immediate native type/sequence/atomic-group fields. Normal only, group -1;
    /// original epoch is an explicit extension, not native operation metadata.
    fn get_metadata(&self) -> Result<OperationMetadata>;
    /// Immutable bounded segmented bytes; empty data does not indicate EOS.
    fn get_data(&self) -> Result<OperationData>;
    /// Once-only fenced service ACK after application apply, for BOTH copy and
    /// replication. This advances local protocol progress, not disk durability
    /// or universal/global network commitment. Drop never acknowledges.
    fn acknowledge(&self) -> Result<()>;
}

/// Bounded single-consumer destination stream. None is EOS, including internal
/// legacy-null EOS retirement; no synthetic application operation/ACK is added.
/// Internal EOS retirement cannot bridge an unapplied data gap.
pub trait IOperationStream: Send + Sync + 'static {
    /// Async operation receipt, distinct from application apply and explicit ACK.
    fn get_operation(
        &self,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Option<Arc<dyn IOperation>>>;
}

/// Application-facing SDK data boundary. Admission yields an early LSN and a
/// separate quorum observation, NOT a single quorum-delayed LSN future.
/// The application must retain accepted admission and locally apply it BEFORE
/// awaiting completion. This differs from managed prepare/quorum/local-commit
/// guidance. Dropping/canceling an observer cannot roll back accepted work or
/// take over the application's local-apply responsibility. Finite limits reject
/// before assignment; cancellation of a completion wait is uncertain.
pub trait IStateReplicator: Send + Sync + 'static {
    /// Async bounded admission. Rejection before admission consumes no LSN;
    /// after admission disconnect/cancel/drop is ambiguous, not rollback.
    fn replicate(
        &self,
        data: OperationData,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ReplicationAdmission>;
    /// Immediate fallible getter; destination copy items require service ACK.
    fn get_copy_stream(&self) -> Result<Arc<dyn IOperationStream>>;
    /// Immediate fallible getter; shares the owner's bounded single consumer.
    fn get_replication_stream(&self) -> Result<Arc<dyn IOperationStream>>;
}

/// Accepted sequence identity, separate from later quorum completion. The
/// application retains admission/local-apply ownership across client disconnect.
pub struct ReplicationAdmission {
    pub sequence_number: i64,
    pub completion: ReplicationCompletion,
}

/// Advisory observation only: dropping this value never undoes admission.
pub struct ReplicationCompletion {
    pub(crate) receiver: tokio::sync::watch::Receiver<Option<Result<i64>>>,
}
impl ReplicationCompletion {
    /// A canceled wait is uncertain and leaves the engine's accepted ownership
    /// and required application local apply unchanged.
    pub async fn wait(&mut self, cancellation: Cancellation) -> Result<i64> {
        loop {
            if let Some(result) = self.receiver.borrow_and_update().clone() {
                return result;
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::new(fube_runtime::ErrorKind::Canceled, "replication completion wait canceled").uncertain()),
                result = self.receiver.changed() => result.map_err(|_| Error::Closed)?,
            }
        }
    }
}

/// An explicit, once-only acknowledgment. Drop/poll does not invoke it.
/// A retired owner rejects late acknowledgments without reviving progress.
pub(crate) struct Acknowledgment {
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
pub(crate) struct Operation {
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
impl IOperation for Operation {
    fn get_metadata(&self) -> Result<OperationMetadata> {
        Ok(OperationMetadata {
            operation_type: OperationType::Normal,
            sequence_number: self.sequence,
            atomic_group_id: -1,
        })
    }
    fn get_data(&self) -> Result<OperationData> {
        Ok(self.data.clone())
    }
    fn acknowledge(&self) -> Result<()> {
        Operation::acknowledge(self)
    }
}

/// A bounded, single-consumer role-generation view. Successful promotion closes
/// dispatch after eligible records, before waiting for service ACKs. A later
/// secondary role needs freshly acquired views; old views never revive.
/// Cancellation stops only that poll, not operation/replica ownership or ACKs.
pub(crate) struct OperationStream {
    receiver: std::sync::Mutex<tokio::sync::mpsc::Receiver<Operation>>,
    poller: tokio::sync::Semaphore,
    stopped: Cancellation,
}
impl IOperationStream for OperationStream {
    fn get_operation(
        &self,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Option<Arc<dyn IOperation>>> {
        Box::pin(async move {
            Ok(self
                .next(cancellation)
                .await?
                .map(|operation| Arc::new(operation) as Arc<dyn IOperation>))
        })
    }
}
impl OperationStream {
    /// Wrap an already-bounded channel owned by an engine/adapter.
    pub fn new(receiver: tokio::sync::mpsc::Receiver<Operation>) -> Self {
        Self {
            receiver: std::sync::Mutex::new(receiver),
            poller: tokio::sync::Semaphore::new(1),
            stopped: Cancellation::default(),
        }
    }
    pub async fn next(&self, cancellation: Cancellation) -> Result<Option<Operation>> {
        // mpsc records only one receive waker. Serialize polls without keeping
        // the receiver locked while waiting, including behind another poll.
        let _poller = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Closed),
            _ = self.stopped.cancelled() => return Ok(None),
            permit = self.poller.acquire() => permit.map_err(|_| Error::Closed)?,
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Closed),
            _ = self.stopped.cancelled() => Ok(None),
            operation = std::future::poll_fn(|cx| {
                let mut receiver = self.receiver.lock().unwrap();
                if self.stopped.is_cancelled() {
                    return std::task::Poll::Ready(None);
                }
                // Dequeue/publication and terminal disposal share this lock.
                // A dequeued capture is application-owned even if shutdown
                // follows before the caller resumes.
                receiver.poll_recv(cx)
            }) => Ok(operation),
        }
    }

    /// Dispose only buffered engine ownership, never application-held captures.
    pub(crate) fn shutdown(&self) {
        let mut receiver = self.receiver.lock().unwrap();
        receiver.close();
        self.stopped.cancel();
        while receiver.try_recv().is_ok() {}
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
