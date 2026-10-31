// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{
    ApiFuture, AtomicOperation, AtomicRedoOperation, BackupInfo, BackupOption, Cancellation, Epoch,
    GetOrAddResult, IStateProvider2, IStateProviderEnumerator, IStatefulServicePartition,
    ITransaction, OperationContext, OperationData, ReplicaRole, RestorePolicy, Result,
    SequenceNumber, StateProviderId, Transaction, TransactionId, TryRemoveVersionResult,
};
use std::{path::PathBuf, sync::Arc, time::Duration};

/// Native common/ITransactionVersionManager.h snapshot visibility registration.
pub trait ITransactionVersionManager: Send + Sync + 'static {
    fn register(&self) -> ApiFuture<'_, SequenceNumber>;
    fn unregister(&self, visibility_version_number: SequenceNumber) -> Result<()>;
}

/// Native common/IVersionManager.h, inherited by ITransactionalReplicator even
/// though it is for provider/internal version retention rather than app writes.
pub trait IVersionManager: Send + Sync + 'static {
    fn try_remove_checkpoint(
        &self,
        checkpoint_lsn_to_be_removed: SequenceNumber,
        next_checkpoint_lsn: SequenceNumber,
    ) -> ApiFuture<'_, ()>;
    fn try_remove_version(
        &self,
        state_provider_id: StateProviderId,
        commit_lsn: SequenceNumber,
        next_commit_lsn: SequenceNumber,
    ) -> Result<TryRemoveVersionResult>;
}

/// Trait projection of native common/EnumerationCompletionResult.h's
/// visibility/completion observations. No cancellation argument is native.
pub trait EnumerationCompletionResult: Send + Sync + 'static {
    fn get_visibility_sequence_number(&self) -> SequenceNumber;
    fn get_notification(&self) -> ApiFuture<'_, SequenceNumber>;
    fn get_is_notification_completed(&self) -> bool;
}

/// Native common/ITransactionManager.h, preserving synchronous Begin/Add and
/// separate async LSN-bearing paths. These methods are declarations, not new
/// transaction admission, retry or quorum algorithms.
pub trait ITransactionManager: ITransactionVersionManager {
    fn get_role(&self) -> ReplicaRole;
    fn create_transaction(&self) -> Result<Arc<dyn Transaction>>;
    fn create_atomic_operation(&self) -> Result<Arc<dyn AtomicOperation>>;
    fn create_atomic_redo_operation(&self) -> Result<Arc<dyn AtomicRedoOperation>>;
    fn single_operation_transaction_abort_unlock(
        &self,
        state_provider_id: StateProviderId,
        operation_context: &dyn OperationContext,
    ) -> Result<()>;
    fn error_if_state_provider_is_not_registered(
        &self,
        state_provider_id: StateProviderId,
        transaction_id: TransactionId,
    ) -> Result<()>;
    fn begin_transaction(
        &self,
        transaction: &dyn Transaction,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;
    fn begin_transaction_async(
        &self,
        transaction: Arc<dyn Transaction>,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
    fn add_operation(
        &self,
        transaction: &dyn Transaction,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;
    /// Native AddOperationAsync overload taking AtomicOperation and undo.
    fn add_atomic_operation(
        &self,
        atomic_operation: Arc<dyn AtomicOperation>,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
    /// Native AddOperationAsync overload taking AtomicRedoOperation, no undo.
    fn add_atomic_redo_operation(
        &self,
        atomic_redo_operation: Arc<dyn AtomicRedoOperation>,
        metadata: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
    fn commit_transaction(
        &self,
        transaction: Arc<dyn Transaction>,
    ) -> ApiFuture<'_, SequenceNumber>;
    fn abort_transaction(&self, transaction: Arc<dyn Transaction>)
        -> ApiFuture<'_, SequenceNumber>;
}

/// Native common/IStateProviderMap.h transactional registry operations.
/// Missing/existing names remain errors where native specifies errors, not
/// success-shaped None. Mutation participates in the supplied transaction.
pub trait IStateProviderMap: Send + Sync + 'static {
    fn get(&self, state_provider_name: &str) -> Result<Arc<dyn IStateProvider2>>;
    fn add(
        &self,
        transaction: Arc<dyn Transaction>,
        state_provider_name: String,
        state_provider_type_name: String,
        initialization_parameters: Option<OperationData>,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    fn remove(
        &self,
        transaction: Arc<dyn Transaction>,
        state_provider_name: String,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    fn create_enumerator(&self, parents_only: bool) -> Result<Arc<dyn IStateProviderEnumerator>>;
    /// Native GetOrAddAsync retains its repeatable read/transactional locking
    /// semantics; the boolean records whether the provider already existed.
    fn get_or_add(
        &self,
        transaction: Arc<dyn Transaction>,
        state_provider_name: String,
        state_provider_type_name: String,
        initialization_parameters: Option<OperationData>,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, GetOrAddResult>;
}

/// Native common/ITransactionChangeHandler.h synchronous callback.
pub trait ITransactionChangeHandler: Send + Sync + 'static {
    fn on_transaction_committed(
        &self,
        source: &dyn ITransactionalReplicator,
        transaction: &dyn ITransaction,
    ) -> Result<()>;
}

/// Native common/IStateManagerChangeHandler.h synchronous notifications.
pub trait IStateManagerChangeHandler: Send + Sync + 'static {
    fn on_rebuilt(
        &self,
        source: &dyn ITransactionalReplicator,
        state_providers: &dyn IStateProviderEnumerator,
    ) -> Result<()>;
    fn on_added(
        &self,
        source: &dyn ITransactionalReplicator,
        transaction: &dyn ITransaction,
        state_provider_name: &str,
        state_provider: &dyn IStateProvider2,
    ) -> Result<()>;
    fn on_removed(
        &self,
        source: &dyn ITransactionalReplicator,
        transaction: &dyn ITransaction,
        state_provider_name: &str,
        state_provider: &dyn IStateProvider2,
    ) -> Result<()>;
}

/// Native common/IBackupCallbackHandler.h upload decision; false is a native
/// callback result, distinct from an exception represented by Err.
pub trait IBackupCallbackHandler: Send + Sync + 'static {
    fn upload_backup(
        &self,
        backup_info: BackupInfo,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, bool>;
}

/// Native common/IDataLossHandler.h authoritative state-changed callback.
pub trait IDataLossHandler: Send + Sync + 'static {
    fn data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool>;
}

/// Native common/ITransactionalReplicator.h public surface and inheritance.
/// Native test hooks and internal log/control interfaces are excluded. This is
/// not the runtime-facing IPrimaryReplicator and grants no membership authority.
pub trait ITransactionalReplicator:
    IStateProviderMap + ITransactionManager + IVersionManager
{
    fn get_is_readable(&self) -> bool;
    /// Existing fube partition representation for native Utilities::IStatefulPartition.
    fn get_stateful_partition(&self) -> Arc<dyn IStatefulServicePartition>;
    fn get_has_persisted_state(&self) -> bool;
    fn get_last_stable_sequence_number(&self) -> Result<SequenceNumber>;
    /// Native logging API reports actual CurrentLogTailLsn, not last visible
    /// transaction commit, stable LSN or lower quorum/release progress.
    fn get_last_committed_sequence_number(&self) -> Result<SequenceNumber>;
    fn get_current_epoch(&self) -> Result<Epoch>;
    fn register_transaction_change_handler(
        &self,
        handler: Arc<dyn ITransactionChangeHandler>,
    ) -> Result<()>;
    fn unregister_transaction_change_handler(&self) -> Result<()>;
    fn register_state_manager_change_handler(
        &self,
        handler: Arc<dyn IStateManagerChangeHandler>,
    ) -> Result<()>;
    fn unregister_state_manager_change_handler(&self) -> Result<()>;
    /// Native default BackupAsync overload; defaults belong to its implementation.
    fn backup(&self, handler: Arc<dyn IBackupCallbackHandler>) -> ApiFuture<'_, BackupInfo>;
    fn backup_with_options(
        &self,
        handler: Arc<dyn IBackupCallbackHandler>,
        backup_option: BackupOption,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, BackupInfo>;
    fn restore(&self, backup_folder: PathBuf) -> ApiFuture<'_, ()>;
    fn restore_with_options(
        &self,
        backup_folder: PathBuf,
        restore_policy: RestorePolicy,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
}
