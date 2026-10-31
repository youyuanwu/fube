// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native aggregate contracts, deliberately not part of the public SDK.
//! These are forward declarations, not executing state/log managers. Owned
//! inputs allow future implementations to retain accepted work; lazy futures,
//! cancellation and observer drop do not implement native owned completion.

#![allow(
    dead_code,
    reason = "crate-internal forward contracts have no concrete V2 kernels yet"
)]
#![deny(missing_docs)]

use std::{
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::{
    ApiFuture, ApplyContext, AtomicOperation, AtomicRedoOperation, BackupInfo, BackupOption,
    Cancellation, Epoch, IBackupCallbackHandler, IOperationDataStream, IStateManagerChangeHandler,
    IStateProviderMap, ITransactionChangeHandler, ITransactionManager, ITransactionalReplicator,
    IVersionManager, OperationContext, OperationData, ReplicaRole, RestorePolicy, Result,
    SequenceNumber, StateProviderId, Transaction, TransactionBase, TransactionId,
};

/// Log-open instructions consumed by state-manager open and subsequent replay.
///
/// Fields project native getters, not its overloaded constructors. In particular
/// the two-bool constructor's implementation sets completion-needed from its
/// first argument despite the header's misleading skip-recovery parameter name.
/// [Native type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/RecoveryInformation.h#L16-L51);
/// [implementation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/RecoveryInformation.cpp#L10-L37);
/// [recovery decisions](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/RecoveryManager.cpp#L117-L346);
/// [flag consumption](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L257-L334).
pub(crate) struct RecoveryInformation {
    /// Log a complete-checkpoint record after recovery of an incomplete checkpoint.
    pub(crate) log_complete_checkpoint_after_recovery: bool,
    /// Clean local state left by interrupted restore before ordinary recovery.
    pub(crate) should_remove_local_state_due_to_incomplete_restore: bool,
    /// Skip replay when the removing-state tail indicates interrupted role None.
    pub(crate) should_skip_recovery_due_to_incomplete_change_role_none: bool,
}

/// Owned projection of the native async state-manager role delegate.
///
/// Logging pauses dispatch around `delegate(ActiveSecondary, None-token)`.
/// Captured owners must be cloned into the static returned future by future
/// implementations; this type alone promises neither eager start nor serialization.
/// [Signature](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ILoggingReplicator.h#L10-L74);
/// [invocation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L871-L918).
pub(crate) type StateManagerBecomeSecondaryDelegate =
    Arc<dyn Fn(ReplicaRole, Cancellation) -> ApiFuture<'static, ()> + Send + Sync>;

/// Logging-to-aggregate-state-manager apply/checkpoint/copy contract.
///
/// Not an alias of a single provider: native routing envelopes carry provider IDs,
/// copy includes registry state, and aggregate set/end have no cancellation input.
/// Native status/throwing callbacks become Rust results; no codec is supplied.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateProviderManager.h#L10-L61);
/// [routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2796-L2860).
pub(crate) trait IStateProviderManager: Send + Sync + 'static {
    /// Apply at the operation LSN with the transaction's separately stored commit LSN.
    ///
    /// Single-operation/atomic records can have equal operation and commit LSNs;
    /// multi-operation commits use the end-record LSN for every operation's commit.
    ///
    /// Header metadata is nullable, but concrete StateManager requires a nonnull
    /// named envelope; enclosed user metadata may be absent. Success may have no
    /// capture (provider null result or stale skip); otherwise retain routed capture
    /// for explicit unlock. None does not discharge an already retained admission
    /// capture. Transaction origin/apply role must satisfy the manager's
    /// primary/nonprimary checks.
    /// Apply can precede quorum and has no cancellation token.
    /// [Dispatch](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1290-L1345);
    /// [nullable routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2796-L2860);
    /// [single-record apply](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L682-L766);
    /// [commit apply](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L768-L966).
    fn apply(
        &self,
        logical_sequence_number: SequenceNumber,
        transaction: Arc<dyn TransactionBase>,
        apply_context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>>;

    /// Release a nonnull aggregate named capture by provider ID, synchronously.
    ///
    /// Local registry captures take the local path; provider exceptions become
    /// status failures and logging records a service error (faulting outside recovery).
    /// Primary invokes unlock
    /// after apply and replication waits even on failure, not only successful commit.
    /// [Routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1347-L1387);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527);
    /// [unlock failure](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L365-L616).
    /// [recovery fault gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L969-L990).
    fn unlock(&self, operation_context: &dyn OperationContext) -> Result<()>;

    /// Snapshot registry metadata, then synchronously prepare providers at nonnegative LSN.
    ///
    /// This captures prepared state, not installed checkpoint files. Native logging
    /// calls after log flush; preparation failure fails the checkpoint path.
    /// [Prepare](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1389-L1430);
    /// [caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L368-L452).
    fn prepare_checkpoint(&self, checkpoint_lsn: SequenceNumber) -> Result<()>;

    /// Await prepared providers, then perform aggregate checkpoint metadata work.
    ///
    /// Native logging serializes checkpoint APIs and waits stability permission;
    /// its normal call supplies no cancellation. Failures fault checkpoint service.
    /// [Perform](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1432-L1467);
    /// [caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1393-L1552).
    fn perform_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    /// Await provider completion before aggregate file replacement/metadata cleanup.
    ///
    /// Logging records and flushes completion only afterward; cancellation is forwarded,
    /// not proof of rollback. Recovery may finish an interrupted checkpoint.
    /// [Complete](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1469-L1512);
    /// [caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1100-L1142).
    fn complete_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    /// Back up registry checkpoint first, then active provider checkpoints to owned path.
    ///
    /// Token reaches provider/file work; errors stop successful aggregate backup.
    /// This does not back up logs, upload externally or complete a replicated backup.
    /// [Backup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1518-L1550).
    fn backup_checkpoint(
        &self,
        backup_directory: PathBuf,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;

    /// Replace registry/providers and reset role Unknown; restore registry first,
    /// then open, restore and recover providers.
    ///
    /// Stronger than per-provider file installation; coordinator subsequently
    /// recovers logging and restores role. Owned path/token may outlive observation;
    /// failure does not guarantee restoration of pre-call state.
    /// [Restore](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1558-L1701);
    /// [coordinator](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L418-L566).
    fn restore_checkpoint(
        &self,
        backup_directory: PathBuf,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;

    /// Create an aggregate copy stream for the target replica, synchronously.
    ///
    /// Native success always creates a registry-bearing named stream; failed null
    /// out-pointer is Err, not successful None. Null individual provider streams
    /// are skipped. Target ID is not a per-provider callback input. Caller must
    /// explicitly dispose the upper stream; creating it is not enumeration/install.
    /// [Factory](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5634-L5655);
    /// [capture](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/CheckpointManager.cpp#L537-L648);
    /// [stream disposal](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/NamedOperationDataStream.cpp#L107-L140).
    fn get_current_state(&self, target_replica_id: i64) -> Result<Arc<dyn IOperationDataStream>>;

    /// Begin aggregate installation by retaining stale providers and rebuilding registry.
    ///
    /// No native cancellation input. Individual provider begin callbacks happen
    /// later as copied metadata initializes them, not all at this boundary.
    /// [Begin](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5657-L5670);
    /// [registry rebuild](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3592-L3663);
    /// [later provider begin](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3665-L3910).
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()>;

    /// Route required named copy data to registry or provider after begin.
    ///
    /// Preserve signed global record number (not necessarily contiguous per provider).
    /// Aggregate has no token and passes native None to providers. Failure is not
    /// EOS or successful installation; owned data must survive accepted work.
    /// [Set](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5657-L5775).
    fn set_current_state(&self, state_record_number: i64, data: OperationData)
        -> ApiFuture<'_, ()>;

    /// Finish valid state stage: local rebuild notification, then only begun providers.
    ///
    /// No token; clear tracked progress on success. Native caller skips this on
    /// aborted partial copy/EOS. This is not completion of overlapping log replay
    /// or publication of the copied checkpoint.
    /// [End](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5657-L5775);
    /// [local notification](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3912-L3924);
    /// [caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L974-L1100).
    fn end_setting_current_state(&self) -> ApiFuture<'_, ()>;

    /// Await provider role transition; publish manager role only after success.
    ///
    /// Must not race apply or another transition. None removes state; repeat of a
    /// successfully completed role is a no-op. Native throwing `Awaitable<void>`
    /// becomes a fallible unit future, with owned advisory token representation.
    /// This one method also represents IStateManager's repeated native declaration.
    /// [Role](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L103-L179).
    fn change_role(&self, new_role: ReplicaRole, cancellation: Cancellation) -> ApiFuture<'_, ()>;
}

/// State-manager routing/admission seam, not the full transaction manager.
///
/// Only registration, cached-operation cleanup and begin/add overloads belong
/// here. Provider IDs envelope optional user payloads/captures; no commit, abort-log,
/// role or version interface is inherited. Sync success admits work, not quorum.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ITransactionStateManager.h#L12-L67);
/// [routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L970-L1287).
pub(crate) trait ITransactionStateManager: Send + Sync + 'static {
    /// Unlock original nonnull cached first-operation capture without an abort log.
    ///
    /// Reserved state-manager ID takes aggregate path; other IDs route to provider.
    /// Provider exception faults partition and returns status failure.
    /// [Cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4032-L4063).
    fn single_operation_transaction_abort_unlock(
        &self,
        state_provider_id: StateProviderId,
        operation_context: &dyn OperationContext,
    ) -> Result<()>;

    /// Validate ID before first-operation caching, rejecting own transient deletion.
    ///
    /// Reserved registry ID is accepted; missing ID is invalid-parameter, own delete
    /// is object-closed. This is neither name lookup nor visibility/access check.
    /// [Entry](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4065-L4072);
    /// [validation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4566-L4620).
    fn error_if_state_provider_is_not_registered(
        &self,
        state_provider_id: StateProviderId,
        transaction_id: TransactionId,
    ) -> Result<()>;

    /// Envelope validated provider input for optimized single-operation commit.
    ///
    /// Not an async equivalent of ordinary begin: successful result is commit LSN
    /// after apply/replication and unlock invocation. Own transaction and optional
    /// data/context; no cancellation/deadline input is native at this layer.
    /// [Single begin](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L970-L1035);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn begin_transaction_async(
        &self,
        transaction: Arc<dyn Transaction>,
        state_provider_id: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;

    /// Validate/envelope first ordinary operation and synchronously submit begin.
    ///
    /// ID precedes nullable user payloads/capture. Logging retains accepted work
    /// and starts completion observation; no LSN, quorum wait or commit decision.
    /// [Begin](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1037-L1095);
    /// [log admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L328-L353);
    /// [owned completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn begin_transaction(
        &self,
        transaction: &dyn Transaction,
        state_provider_id: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;

    /// Validate/envelope a subsequent ordinary operation after accepted begin.
    ///
    /// Synchronous admission can fail while prior records remain accepted;
    /// optional capture/payloads are retained, not applied/committed by this return.
    /// [Add](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1097-L1155);
    /// [log admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L235-L294);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn add_operation(
        &self,
        transaction: &dyn Transaction,
        state_provider_id: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;

    /// Route undo-capable atomic overload and await single-record completion.
    ///
    /// Provider validation/envelope precedes logging; successful commit LSN follows
    /// apply/replication waits and unlock invocation. Undo is an actual native input.
    /// Own operation/captures; failures are not reclassified into ordinary admission.
    /// [Atomic](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1157-L1222);
    /// [record admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L262-L294);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn add_atomic_operation(
        &self,
        atomic_operation: Arc<dyn AtomicOperation>,
        state_provider_id: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;

    /// Route redo-only atomic overload, with no undo input, to logging completion.
    ///
    /// Successful result is commit LSN after apply/replication and unlock invocation;
    /// owned optional metadata/redo/capture preserve null versus empty values.
    /// [Redo atomic](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1224-L1287);
    /// [record admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L296-L326);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn add_atomic_redo_operation(
        &self,
        atomic_redo_operation: Arc<dyn AtomicRedoOperation>,
        state_provider_id: StateProviderId,
        metadata: Option<OperationData>,
        redo: Option<OperationData>,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
}

/// Aggregate registry, transaction-routing and provider lifecycle interface.
///
/// Native hierarchy combines all three supertraits. Its repeated ChangeRole
/// declaration is represented once by inherited IStateProviderManager::change_role.
/// No native lifecycle Abort belongs to this interface. Native concrete service
/// handles failures/close; these declarations supply no lifecycle engine.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/IStateManager.h#L12-L40);
/// [open ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L257-L334).
pub(crate) trait IStateManager:
    IStateProviderMap + ITransactionStateManager + IStateProviderManager
{
    /// Capture live replicator owners weakly before open, avoiding ownership cycles.
    ///
    /// Native takes references and immediately obtains weak references; Rust takes
    /// explicit nonoptional Weak handles. Future implementations require live
    /// targets during initialization/lifecycle, not successful missing-owner setup.
    /// Native void becomes Result; no open/recovery or strong retention is implied.
    /// [Initialize](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L50-L69).
    fn initialize(
        &self,
        transactional_replicator: Weak<dyn ITransactionalReplicator>,
        logging_replicator: Weak<dyn ILoggingReplicator>,
    ) -> Result<()>;

    /// Open after log-open instructions, before logging replay.
    ///
    /// Flags request interrupted checkpoint completion/restore cleanup; persistent
    /// path rebuilds registry, opens/completes/recovers providers. Volatile bypasses
    /// persisted recovery. Actual native entry ignores outer token and workers use
    /// None; retain signature without promising prompt interruption. Failure blocks open.
    /// [Entry](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L71-L101);
    /// [worker](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2982-L3102).
    fn open(
        &self,
        complete_checkpoint: bool,
        cleanup_restore: bool,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;

    /// Await provider close and metadata lock-manager disposal, terminally.
    ///
    /// Native outer token is unused; worker passes None. This is not an alias
    /// for synchronous Dispose/Abort, nor evidence that arbitrary callback work
    /// has drained. Provider cleanup fallback follows dispatcher lifecycle rules.
    /// [Entry](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L181-L203);
    /// [worker](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2982-L3102);
    /// [provider cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3301-L3379).
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    /// Retain/replace the single shared registry-change handler synchronously.
    ///
    /// No immediate replay; callbacks later borrow source/transaction/provider
    /// and root-only entries. Callback failures follow apply/rebuild paths.
    /// [Cache](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L205-L216);
    /// [callbacks](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872).
    fn register_state_manager_change_handler(
        &self,
        handler: Arc<dyn IStateManagerChangeHandler>,
    ) -> Result<()>;

    /// Clear handler cache synchronously, without draining already snapped callbacks.
    ///
    /// A borrowed invocation can still retain its snapped shared handler afterward;
    /// unregister does not undo registry changes.
    /// [Cache](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L205-L216);
    /// [invoke](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872).
    fn unregister_state_manager_change_handler(&self) -> Result<()>;
}

/// Internal logging composition: transaction/version management and disposal.
///
/// Native IDisposable is folded into explicit dispose, not a fifth trait.
/// Inherited creation, registration-check and cached-abort-unlock entry points
/// are structurally present but concrete native logging asserts/not-implements
/// them: callers use the public wrapper/state-manager routing instead.
/// Recovery, role/copy/checkpoint work require ordered owned lifecycle operations;
/// declaring futures is not an implementation of those obligations.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ILoggingReplicator.h#L10-L74);
/// [prohibited entry points](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.h#L131-L200).
pub(crate) trait ILoggingReplicator: ITransactionManager + IVersionManager {
    /// Observe construction persistence capability, not a durable commit or qualification.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L235-L238).
    fn get_has_persisted_state(&self) -> bool;

    /// Open log and return recovery instructions before state-provider recovery/replay.
    ///
    /// No cancellation input. Incomplete restore/removing-state detection affects
    /// subsequent state-manager open; Err is not a successful default instruction set.
    /// [Open](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L235-L309);
    /// [recovery instructions](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/RecoveryManager.cpp#L117-L346).
    fn open(&self) -> ApiFuture<'_, RecoveryInformation>;

    /// Replay log after state-manager open/recovery, using owned open instructions.
    ///
    /// Native has no token. Recovery rebuilds/dispatches records and updates managers;
    /// apply/unlock failure prevents successful open rather than assuming ReportFault
    /// alone handles pre-open errors. Skip-removing-state decision is made by caller.
    /// [Recovery](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L311-L370);
    /// [replay and errors](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/RecoveryManager.cpp#L349-L648);
    /// [ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L257-L334).
    fn perform_recovery(&self, recovery_information: RecoveryInformation) -> ApiFuture<'_, ()>;

    /// Drain copy/replication, transition role, cancel checkpoints and close/flush log.
    ///
    /// Log work is skipped if role None/removing-state or restore made it unavailable.
    /// No token; explicit synchronous disposal is separate. Wrapper closes recovered
    /// logging before its API activity drain to avoid pending-commit deadlock;
    /// it logs lower close errors before disposal, not necessarily propagating them.
    /// [Close](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L381-L420);
    /// [transition](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1434-L1557);
    /// [wrapper](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L336-L386).
    fn close(&self) -> ApiFuture<'_, ()>;

    /// Transition active-secondary with owned state-manager role callback.
    ///
    /// Prior idle role drains copy; other predecessors drain copy/replication and
    /// start secondary build. Pause/drain dispatcher, await delegate(ActiveSecondary,
    /// native None-token), then resume even on delegate failure and report service
    /// error. Return is not completed full copy or arbitrary concurrent apply authority.
    /// [Transition](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L871-L918);
    /// [previous-role drains](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1434-L1557).
    fn become_active_secondary(
        &self,
        state_manager_change_role: StateManagerBecomeSecondaryDelegate,
    ) -> ApiFuture<'_, ()>;

    /// Prepare idle-secondary and start owned secondary build.
    ///
    /// No delegate/token; success is transition/build start, not completed full copy.
    /// Previous-role drains remain ordered. Despite the entry's defensive build
    /// exclusion, native active-secondary to idle-secondary transition asserts;
    /// this declaration does not establish that path as supported.
    /// [Idle](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L920-L946);
    /// [transition restriction](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1434-L1557);
    /// [owned build](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L147-L254).
    fn become_idle_secondary(&self) -> ApiFuture<'_, ()>;

    /// Drain streams/readers, mark closing/removing-state, flush and abort checkpoints.
    ///
    /// Flush/process is skipped if removing-state was already recorded. Native logs
    /// backup-state deletion failure but returns success on that branch.
    /// Provider None/removal and log deletion are separate ordered wrapper steps.
    /// [None](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L948-L987);
    /// [closing/removing-state transition](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1434-L1557);
    /// [wrapper ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L388-L492).
    fn become_none(&self) -> ApiFuture<'_, ()>;

    /// Quiesce prior activity, mark elected primary and start owned abort/barrier work.
    ///
    /// is_for_restore selects restore transition. Return does not await pending
    /// transaction aborts/barrier task completion; demotion/close cancels and joins
    /// that owned task. Future implementations must retain work independently.
    /// [Promotion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L989-L1078);
    /// [owned task](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1110-L1250);
    /// [join](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1080-L1108);
    /// [join before demotion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1434-L1557).
    fn become_primary(&self, is_for_restore: bool) -> ApiFuture<'_, ()>;

    /// Delete log asynchronously after normal wrapper's state-manager None succeeds.
    ///
    /// Separate from become_none and provider removal; failure prevents successful
    /// composite removal, not automatically a restored pre-call log.
    /// [Delete](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1252-L1261);
    /// [ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L388-L492).
    fn delete_log(&self) -> ApiFuture<'_, ()>;

    /// Observe earliest retained log/full-copy horizon, or zero when none.
    ///
    /// Cleanup uses delete LSN (or stale provider's create LSN when delete is absent)
    /// strictly below this value.
    /// It is not stable/commit LSN or proof of present role/removal authority.
    /// [Horizon](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1662-L1681);
    /// [cleanup comparison](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5301-L5425).
    fn get_safe_lsn_to_remove_state_provider(&self) -> Result<SequenceNumber>;

    /// Observe primary partition read-granted or nonprimary barrier-consistency threshold.
    ///
    /// No wait/role mutation; registry reads apply additional access/role checks.
    /// [Readability](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456);
    /// [registry checks](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4622-L4709).
    fn is_readable(&self) -> bool;

    /// Observe checkpoint stable field, updated by native successful barrier rules.
    ///
    /// Primary uses barrier LSN after apply/replication; secondary uses its propagated
    /// stable field. Getter initiates no wait, flush or snapshot registration.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456);
    /// [primary completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L864-L881);
    /// [barriers](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1554-L1630).
    fn get_last_stable_sequence_number(&self) -> Result<SequenceNumber>;

    /// Observe current log tail despite native name "last committed".
    ///
    /// Includes control and pending transaction records; not last successful commit,
    /// stable/quorum frontier or visible snapshot number. No completion wait.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456);
    /// [tail advancement](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp#L638-L777).
    fn get_last_committed_sequence_number(&self) -> Result<SequenceNumber>;

    /// Observe current tail's data-loss/configuration epoch; no reconfiguration authority.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456).
    fn get_current_epoch(&self) -> Result<Epoch>;

    /// Full backup with infinite timeout/no token, requiring persisted primary profile.
    ///
    /// Owned upload handler is retained; success includes stabilized capture/upload
    /// and backup record work, not just checkpoint copies. False upload is not Err
    /// from the callback but enclosing backup rejects it.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L458-L522);
    /// [default](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L137-L149);
    /// [backup sequence](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L151-L407);
    /// [upload result](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1430-L1472).
    fn backup(&self, handler: Arc<dyn IBackupCallbackHandler>) -> ApiFuture<'_, BackupInfo>;

    /// Back up full checkpoint/logs or incremental chain and invoke owned upload handler.
    ///
    /// Persisted/primary gates and incremental chain/epoch constraints apply.
    /// None timeout is infinite; deadline/token affect selected locks/provider/log/
    /// callback paths, not a universal whole-call cancellation fence. Failure can
    /// leave local files; success is not a new interoperability qualification.
    /// [Gates](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L458-L522);
    /// [backup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L151-L407);
    /// [chain constraints](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L707-L785);
    /// [selected waits](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L899-L978).
    fn backup_with_options(
        &self,
        handler: Arc<dyn IBackupCallbackHandler>,
        backup_option: BackupOption,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, BackupInfo>;

    /// Safe restore with infinite timeout/no token, only in persisted data-loss callback.
    ///
    /// Owned folder identifies backup chain; restore replaces providers/log, recovers,
    /// restarts primary and removes restore token. Not provider file install alone.
    /// [Persisted gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L524-L575);
    /// [restore](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L409-L566).
    fn restore(&self, backup_folder: PathBuf) -> ApiFuture<'_, ()>;

    /// Restore analyzed chain with explicit Safe/Force and owned folder/token.
    ///
    /// Force bypasses safety comparison only; data-loss/persisted/restore-entry
    /// eligibility still applies. Timeout covers selected waits, not whole-call
    /// deadline. Failure may leave restore token/partially replaced state for next open.
    /// [Persisted gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L524-L575);
    /// [restore](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L418-L566);
    /// [entry wait](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1758-L1789);
    /// [safety](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1840-L1905).
    fn restore_with_options(
        &self,
        backup_folder: PathBuf,
        restore_policy: RestorePolicy,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;

    /// Retain/replace transaction handler in early cache or live operation processor.
    ///
    /// No replay or drain promise; early cache transfers on processor creation.
    /// Callback is for ordinary committed transaction record unlock, not atomics.
    /// [Cache](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1930-L2048);
    /// [notification eligibility](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L365-L636).
    fn register_transaction_change_handler(
        &self,
        handler: Arc<dyn ITransactionChangeHandler>,
    ) -> Result<()>;

    /// Clear transaction handler without joining already snapped callback references.
    ///
    /// No safe arbitrary pre-open unregister contract is established: native
    /// pre-processor branch subsequently dereferences processor. Do not normalize
    /// that source limitation into a new Rust runtime error policy.
    /// [Unregister](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1930-L2048);
    /// [snapped handler](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L618-L636).
    fn unregister_transaction_change_handler(&self) -> Result<()>;

    /// Explicit synchronous idempotent disposal: break provider cycle, dispose log/reader.
    ///
    /// Native void is projected as Result per crate convention. Not close/drain,
    /// accepted-work cancellation or a Drop-as-success contract; wrapper closes first.
    /// [Dispose](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1263-L1277).
    fn dispose(&self) -> Result<()>;
}

#[cfg(test)]
mod tests;
