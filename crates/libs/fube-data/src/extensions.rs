//! Explicit non-SDK capabilities. None grants native membership/access authority
//! or adds discovery methods to SDK traits.
//! Replay, operation origin and epoch-checked admission reorganize preexisting
//! fube behavior; they are not ported SDK callbacks. Execution/native-binding/
//! cleanup integration is newly written fube infrastructure using standard Tokio.
use crate::{Cancellation, Epoch, IOperation, OperationData, Result};
use fube_runtime::ApiFuture;
use std::sync::Arc;

pub use crate::api::Progress;
pub use crate::engine::{EngineSnapshot, PeerSnapshot};
pub use fube_runtime::extensions::EpochDomains;

/// Best-effort fube diagnostics, not native control or compatibility evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationEvent {
    /// A cooldown was scheduled after snapshot publication and admission release.
    /// Both instants belong to this process's monotonic clock.
    ReplayRetryScheduled {
        sequence_number: i64,
        published_at: std::time::Instant,
        retry_at: std::time::Instant,
    },
}

/// Authentic operation at exactly the requested sequence. Never synthesize a
/// snapshot mutation, stamp a new epoch, assign an LSN or alter segment layout.
#[derive(Clone, Debug)]
pub struct ReplayOperation {
    pub epoch: Epoch,
    pub data: OperationData,
}

/// Separately optional provider capability, explicitly supplied at construction.
/// A standard-only provider receives no replay callback. Unknown original
/// metadata/history returns None; the engine retains bounded retry pacing.
/// Without this capability, a no-write rebind that needs unavailable original
/// metadata waits for ordinary protocol progress; no fake operation is sent.
pub trait ReplayCheckpointProvider: Send + Sync + 'static {
    fn replication_checkpoint(
        &self,
        sequence: i64,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, Option<ReplayOperation>>;
}

/// Original operation epoch alongside, not inside, canonical native metadata.
pub struct OperationWithOrigin {
    pub operation: Arc<dyn IOperation>,
    pub epoch: Epoch,
}

/// Explicit opt-in origin capability for applications that preserve replay
/// history. Shares the canonical stream's single consumer and bounded captures.
pub trait OperationOriginStream: Send + Sync + 'static {
    fn get_operation_with_origin(
        &self,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, Option<OperationWithOrigin>>;
}

impl OperationOriginStream for crate::api::OperationStream {
    fn get_operation_with_origin(
        &self,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, Option<OperationWithOrigin>> {
        Box::pin(async move {
            Ok(self.next(cancellation).await?.map(|operation| {
                let epoch = operation.epoch;
                OperationWithOrigin {
                    operation: Arc::new(operation),
                    epoch,
                }
            }))
        })
    }
}

/// Listener selection and transport actor incarnation, NOT native incarnation.
/// Native authority is separately runtime/partition-issued with private fields,
/// not constructible from these options or diagnostics. This is not a claim of
/// cryptographic authentication on trusted clear IPC or global unforgeability.
#[derive(Clone, Debug)]
pub struct ReplicationEndpointOptions {
    pub listen_address: String,
    pub actor_incarnation: uuid::Uuid,
}

/// Explicit construction capability, equivalent to the native provider's
/// SupportsCopyUntilLatestLsn marker. Never SDK trait/type/peer discovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateProviderCapabilities {
    /// Copy receives the inclusive assigned horizon; replication starts at
    /// assigned+1. The default classic horizon is max(committed, previous quorum).
    pub supports_copy_until_latest_lsn: bool,
}

/// Exact-primary-epoch admission, not a second canonical contract or legacy
/// submit adapter. Preserves application payload origin across role races.
pub trait EpochCheckedReplication: Send + Sync {
    fn replicate_in_epoch(
        &self,
        expected_epoch: Epoch,
        data: OperationData,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, crate::ReplicationAdmission>;
}

/// Explicit engine observations and safe canonical-sync scheduling capability.
/// Snapshots are never native control or application-access grants.
pub trait ReplicatorDiagnostics {
    fn snapshot(&self) -> EngineSnapshot;
    fn endpoint(&self) -> crate::wire::Endpoint;
    fn execution_runtime(&self) -> fube_runtime::extensions::ExecutionRuntime;
    fn get_copy_stream_with_origin(&self) -> Result<Arc<dyn OperationOriginStream>>;
    fn get_replication_stream_with_origin(&self) -> Result<Arc<dyn OperationOriginStream>>;
    #[cfg(feature = "qualification")]
    fn qualification_transport_snapshot(&self) -> serde_json::Value;
    #[cfg(feature = "qualification")]
    fn qualification_probe(
        &self,
        kind: &str,
        target: Option<i64>,
    ) -> ApiFuture<'_, serde_json::Value>;
}
impl ReplicatorDiagnostics for crate::FabricReplicator {
    fn snapshot(&self) -> EngineSnapshot {
        self.snapshot()
    }
    fn endpoint(&self) -> crate::wire::Endpoint {
        self.endpoint()
    }
    fn execution_runtime(&self) -> fube_runtime::extensions::ExecutionRuntime {
        self.execution()
    }
    fn get_copy_stream_with_origin(&self) -> Result<Arc<dyn OperationOriginStream>> {
        self.copy_with_origin()
    }
    fn get_replication_stream_with_origin(&self) -> Result<Arc<dyn OperationOriginStream>> {
        self.replication_with_origin()
    }
    #[cfg(feature = "qualification")]
    fn qualification_transport_snapshot(&self) -> serde_json::Value {
        self.transport_snapshot()
    }
    #[cfg(feature = "qualification")]
    fn qualification_probe(
        &self,
        kind: &str,
        target: Option<i64>,
    ) -> ApiFuture<'_, serde_json::Value> {
        self.probe(kind, target)
    }
}
