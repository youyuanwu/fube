// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{
    ApiFuture, ApplyContext, Cancellation, FactoryArguments, ITransactionalReplicator,
    OperationContext, OperationData, ReplicaRole, Result, SequenceNumber, StateProviderEntry,
    StateProviderInfo, TransactionBase,
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

/// Native common/IOperationDataStream.h and OperationDataStream.h awaitable
/// projection. None is EOS; Some(empty) is data. No provider payload ACK exists.
/// Native IDisposable is explicit, unlike the classic fube-data stream trait.
pub trait IOperationDataStream: Send + Sync + 'static {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>>;
    fn dispose(&self) -> Result<()>;
}

/// Native Data::IEnumerator specialized to state-provider name/object pairs.
/// Single-consumer cursor; Current is valid only after successful MoveNext.
/// Potential native exceptions become Result, not an empty enumeration.
pub trait IStateProviderEnumerator: Send + Sync + 'static {
    fn current(&self) -> Result<StateProviderEntry>;
    fn move_next(&self) -> Result<bool>;
}

/// Native common/IStateProvider2.h, distinct from the lower V1 IStateProvider.
/// All callbacks are declared, including future persisted profiles; declaration
/// alone does not implement or qualify recovery, checkpoint, backup or restore.
/// Native throwing void callbacks are represented by fallible Rust results.
pub trait IStateProvider2: Send + Sync + 'static {
    fn get_name(&self) -> &str;
    fn get_children(&self, root_name: &str) -> Result<Vec<StateProviderInfo>>;
    /// Native weak replicator ownership prevents a replicator/provider cycle.
    /// None preserves absent children, distinct from a supplied empty list.
    fn initialize(
        &self,
        transactional_replicator: Weak<dyn ITransactionalReplicator>,
        work_folder: &Path,
        children: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()>;

    fn open(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn change_role(&self, new_role: ReplicaRole, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Native noexcept Abort; future implementations must own terminal cleanup.
    fn abort(&self);

    fn prepare_checkpoint(&self, checkpoint_lsn: SequenceNumber) -> Result<()>;
    fn perform_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn complete_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn recover_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn backup_checkpoint(
        &self,
        backup_directory: PathBuf,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Installs checkpoint files; does not itself recover their contents.
    fn restore_checkpoint(
        &self,
        backup_directory: PathBuf,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    fn remove_state(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    /// Native permits a null stream when this provider has no state to copy.
    fn get_current_state(&self) -> Result<Option<Arc<dyn IOperationDataStream>>>;
    /// Native BeginSettingCurrentStateAsync has no cancellation parameter.
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()>;
    fn set_current_state(
        &self,
        state_record_number: i64,
        operation_data: OperationData,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Not called when copy is aborted partway through installation.
    fn end_setting_current_state(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    fn prepare_for_remove(
        &self,
        transaction: Arc<dyn TransactionBase>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// The operation LSN is distinct from transaction's commit LSN. Apply may
    /// precede quorum; provider lock/version contexts govern visibility. Native
    /// Apply has no cancellation argument. None context is a valid success.
    fn apply(
        &self,
        logical_sequence_number: SequenceNumber,
        transaction: Arc<dyn TransactionBase>,
        apply_context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>>;
    /// Explicit native synchronous Unlock, not Drop or an async callback.
    /// Its failure must reach native service-fault handling, not be swallowed
    /// or relabeled as proof that a successful commit rolled back.
    fn unlock(&self, operation_context: &dyn OperationContext) -> Result<()>;
}

/// Native statemanager/IStateProvider2Factory.h synchronous Create callback.
pub trait IStateProvider2Factory: Send + Sync + 'static {
    fn create(&self, arguments: &FactoryArguments) -> Result<Arc<dyn IStateProvider2>>;
}
