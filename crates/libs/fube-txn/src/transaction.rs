// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{
    ApiFuture, Cancellation, OperationData, Result, SequenceNumber, StateProviderId, TransactionId,
    TransactionState,
};
use std::{any::Any, sync::Arc, time::Duration};

/// Opaque native common/OperationContext.h provider-owned capture. Any allows
/// provider-specific downcasts; the replicator must not interpret its payload.
/// Retain the same capture through explicit Unlock, not just future observation.
pub trait OperationContext: Any + Send + Sync + 'static {}

/// Trait projection of native common/LockContext.h, not a lock implementation.
pub trait LockContext: OperationContext {
    fn is_tracking_context(&self) -> bool;
    fn unlock(&self) -> Result<()>;
}

/// Required instance surface of native common/TransactionBase.h, projected as
/// a trait. Static ID/recovery/retry helpers and operators are not ported here.
/// Dispose is explicit and synchronous; dropping a Rust handle is not its proof.
pub trait TransactionBase: Send + Sync + 'static {
    fn get_is_primary_transaction(&self) -> bool;
    fn get_transaction_id(&self) -> TransactionId;
    fn get_is_atomic_operation(&self) -> bool;
    fn get_is_valid_transaction(&self) -> bool;
    fn get_state(&self) -> TransactionState;
    fn get_commit_sequence_number(&self) -> SequenceNumber;
    /// Native set_CommitSequenceNumber is a one-time assignment of a positive LSN.
    fn set_commit_sequence_number(&self, value: SequenceNumber) -> Result<()>;
    fn add_lock_context(&self, lock_context: Arc<dyn LockContext>) -> Result<()>;
    fn dispose(&self) -> Result<()>;
}

/// Native common/ITransaction.h: both CommitAsync overloads and synchronous
/// Abort. Successful Abort schedules owned async abort work; it is not a drain.
/// Timeout/cancellation restores Active only after the source-classified retry
/// path, not by interrupting already accepted work or promising rollback.
pub trait ITransaction: Send + Sync + 'static {
    /// Native parameterless overload: infinite timeout, no cancellation.
    fn commit(&self) -> ApiFuture<'_, ()>;
    fn commit_with_options(
        &self,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    fn abort(&self) -> Result<()>;
}

/// Application/provider surface of native common/Transaction.h. AddOperation
/// is synchronous (including first-operation caching); it is not a new quorum
/// wait or serial admission fence. Core-only abort helpers/stopwatch are excluded.
pub trait Transaction: TransactionBase + ITransaction {
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;
    fn get_visibility_sequence_number(&self) -> ApiFuture<'_, SequenceNumber>;
}

/// Trait projection of native common/AtomicOperation.h. One operation per
/// instance; unlike AtomicRedoOperation this signature includes undo data.
pub trait AtomicOperation: TransactionBase {
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, ()>;
}

/// Trait projection of native common/AtomicRedoOperation.h. No undo input:
/// native false-progress handling requires full copy rather than provider undo.
pub trait AtomicRedoOperation: TransactionBase {
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, ()>;
}
