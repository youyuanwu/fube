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
///
/// Reader registrations retain provider versions; duplicate VSNs share a
/// completion source but require individual unregisters. Not transaction allocation.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ITransactionVersionManager.h#L10-L17);
/// [implementation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L276-L354).
pub trait ITransactionVersionManager: Send + Sync + 'static {
    /// Await captured dispatching barrier, then register sampled last-applied-barrier VSN.
    ///
    /// Registration lock protects sampling/append; duplicates share notifications.
    /// No token/deadline or transaction commit LSN input; dropping observer is not
    /// unregister. Future implementations must preserve accepted registration ownership.
    /// [Register](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L276-L319).
    /// [sampled version](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L577-L588).
    fn register(&self) -> ApiFuture<'_, SequenceNumber>;
    /// Remove one existing reader registration, not all equal VSNs.
    ///
    /// Native asserts absent registration; last duplicate completes notification
    /// outside lock. Does not itself delete versions/checkpoints or cancel observers.
    /// [Unregister](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L321-L354).
    fn unregister(&self, visibility_version_number: SequenceNumber) -> Result<()>;
}

/// Native common/IVersionManager.h, inherited by ITransactionalReplicator even
/// though it is for provider/internal version retention rather than app writes.
/// Native registration/removal coordination is represented by results/futures,
/// not a provider storage/deletion algorithm.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IVersionManager.h#L10-L46);
/// [implementation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L133-L274).
pub trait IVersionManager: Send + Sync + 'static {
    /// Wait until readers permit old checkpoint removal; caller deletes files separately.
    ///
    /// Native requires next LSN > old + 1 (intervening barrier). Immediately
    /// completes if removable; otherwise awaits captured retaining-reader
    /// notifications. No cancellation input or new checkpoint deletion policy.
    /// [Removal wait](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L133-L185).
    fn try_remove_checkpoint(
        &self,
        checkpoint_lsn_to_be_removed: SequenceNumber,
        next_checkpoint_lsn: SequenceNumber,
    ) -> ApiFuture<'_, ()>;
    /// Test version retention with next commit LSN strictly greater than commit LSN.
    ///
    /// Removable result has absent sets; otherwise returns retaining VSNs and only
    /// newly registered (provider, VSN) notifications. No new notifications is not
    /// removability. Native range test is [commit,next), but collection can
    /// conservatively include boundary-equal next VSN after an earlier retainer.
    /// Caller owns deletion after notification; this is a synchronous test/registration.
    /// [Test/register](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L187-L274);
    /// [range collection](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L378-L472).
    fn try_remove_version(
        &self,
        state_provider_id: StateProviderId,
        commit_lsn: SequenceNumber,
        next_commit_lsn: SequenceNumber,
    ) -> Result<TryRemoveVersionResult>;
}

/// Trait projection of native common/EnumerationCompletionResult.h's
/// visibility/completion observations. No cancellation argument is native.
/// Native equality/deduplication is by stored VSN; completion does not delete data.
/// [Type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/EnumerationCompletionResult.h#L10-L42);
/// [getters](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/EnumerationCompletionResult.cpp#L30-L68).
pub trait EnumerationCompletionResult: Send + Sync + 'static {
    /// Observe stored reader VSN, not transaction ID, commit LSN or operation LSN.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/EnumerationCompletionResult.cpp#L30-L68).
    fn get_visibility_sequence_number(&self) -> SequenceNumber;
    /// Observe shared completion, yielding VSN after last duplicate reader unregisters.
    ///
    /// No token; dropping this wait does not unregister or prove provider deletion.
    /// [Wait](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/EnumerationCompletionResult.cpp#L30-L68);
    /// [signal](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L321-L354).
    fn get_notification(&self) -> ApiFuture<'_, SequenceNumber>;
    /// Observe completion-source state synchronously, not completed version deletion.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/EnumerationCompletionResult.cpp#L30-L68).
    fn get_is_notification_completed(&self) -> bool;
}

/// Native common/ITransactionManager.h, preserving synchronous Begin/Add and
/// separate async LSN-bearing paths. These methods are declarations, not new
/// transaction admission, retry or quorum algorithms.
///
/// Public wrapper routes provider envelopes through StateManager; logging consumes
/// record admission. Its concrete lower implementation prohibits inherited
/// creation/registration-check/cached-unlock paths, despite structural inheritance.
/// Owned async handles/payloads must survive accepted work independently of observer.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ITransactionManager.h#L10-L81);
/// [forwarders](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L637-L770).
pub trait ITransactionManager: ITransactionVersionManager {
    /// Observe logging role, not transaction's creation-role snapshot or permission to write.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L588-L591);
    /// [inline role getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.h#L131-L135).
    fn get_role(&self) -> ReplicaRole;
    /// Allocate ordinary transaction under wrapper activity gate; capture role/weak manager.
    ///
    /// Nonprimary creation can succeed, but subsequent writes reject that origin.
    /// Allocation errors are not successful null transactions; no log record is created.
    /// [Creation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L588-L624);
    /// [role capture](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L24-L79).
    fn create_transaction(&self) -> Result<Arc<dyn Transaction>>;
    /// Allocate undo-capable atomic with negative identity and creation-role snapshot.
    ///
    /// Wrapper allocation/activity failures propagate; no add/commit is performed.
    /// This entry is prohibited on concrete lower logging manager.
    /// [Creation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L588-L624);
    /// [atomic identity/role](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/AtomicOperation.cpp#L31-L48);
    /// [lower restriction](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.h#L131-L200).
    fn create_atomic_operation(&self) -> Result<Arc<dyn AtomicOperation>>;
    /// Allocate redo-only atomic, not undo-capable or ordinary transaction.
    ///
    /// Captures current role/weak owner; later add checks write eligibility.
    /// Success is object allocation, not log completion.
    /// [Creation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L615-L624);
    /// [redo identity/role](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/AtomicRedoOperation.cpp#L31-L48).
    fn create_atomic_redo_operation(&self) -> Result<Arc<dyn AtomicRedoOperation>>;
    /// Release original nonnull cached first-write capture by ID without abort log.
    ///
    /// StateManager reports service fault/status on provider exception, but native
    /// public wrapper discards inner status after activity gate and returns success.
    /// Thus successful return alone is not proof of clean provider unlock/drain.
    /// [Wrapper](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L626-L635);
    /// [cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4032-L4063).
    fn single_operation_transaction_abort_unlock(
        &self,
        state_provider_id: StateProviderId,
        operation_context: &dyn OperationContext,
    ) -> Result<()>;
    /// Validate registered ID before caching, including own transient-delete rejection.
    ///
    /// Reserved registry ID succeeds; missing ID is invalid-parameter and own
    /// transient deletion is object-closed. Not name/visibility/read-access validation.
    /// [Check](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4566-L4620).
    fn error_if_state_provider_is_not_registered(
        &self,
        state_provider_id: StateProviderId,
        transaction_id: TransactionId,
    ) -> Result<()>;
    /// Synchronously submit ordinary begin/first write after provider-ID validation.
    ///
    /// Optional user metadata/context are enveloped for routing; logging starts
    /// owned apply/replication/unlock observation after first-primary barrier.
    /// Result is admission status, not commit LSN or quorum wait.
    /// [Routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1037-L1095);
    /// [first-primary barrier](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L608-L706);
    /// [admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L328-L353).
    fn begin_transaction(
        &self,
        transaction: &dyn Transaction,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;
    /// Optimized single-write begin/commit, not async equivalent of ordinary begin.
    ///
    /// Own transaction/nullable payloads/capture, validate and envelope provider ID.
    /// Successful result is commit LSN after apply/replication waits and unlock
    /// invocation; no token/timeout at this layer. Unlock failure faults separately.
    /// [Provider routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L970-L1035);
    /// [Single record](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L355-L388);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn begin_transaction_async(
        &self,
        transaction: Arc<dyn Transaction>,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
    /// Admit subsequent ordinary operation synchronously after begin, preserving routing ID.
    ///
    /// Logging retains payload/capture and observes completion separately; no
    /// per-operation commit decision/quorum wait. Failure need not revoke earlier
    /// accepted transaction records; native normal transaction handles retry/state.
    /// [Routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1097-L1155);
    /// [record admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L236-L260).
    fn add_operation(
        &self,
        transaction: &dyn Transaction,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;
    /// Await undo-capable atomic overload; own operation/nullable metadata/undo/redo/capture.
    ///
    /// Validated provider routing precedes single record admission. Successful
    /// result is record/commit LSN after apply/replication and unlock invocation,
    /// with no ordinary transaction commit call or native token/deadline input.
    /// [Provider routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1157-L1222);
    /// [Atomic](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L262-L294);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn add_atomic_operation(
        &self,
        atomic_operation: Arc<dyn AtomicOperation>,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
    /// Await redo-only atomic overload with no undo input.
    ///
    /// Own operation/nullable metadata/redo/capture; provider routing validates
    /// identity. Successful result is commit LSN after apply/replication and unlock
    /// invocation; false-progress compatibility differs from undo-capable atomics.
    /// [Provider routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1224-L1287);
    /// [Redo overload](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L296-L326);
    /// [copy restriction](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/CopyModeResult.cpp#L75-L94).
    fn add_atomic_redo_operation(
        &self,
        atomic_redo_operation: Arc<dyn AtomicRedoOperation>,
        metadata: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber>;
    /// Submit multi-write end-commit record and await logging completion.
    ///
    /// Own transaction; success returns end-record commit LSN after apply/replication
    /// and unlock invocation. Normal transaction handles retries/state above this
    /// layer; no timeout/token input or read-only/single-write optimization here.
    /// [End commit](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L390-L478);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn commit_transaction(
        &self,
        transaction: Arc<dyn Transaction>,
    ) -> ApiFuture<'_, SequenceNumber>;
    /// Submit abort end record and await its LSN, not a transaction commit LSN.
    ///
    /// Own transaction; abort does not redo ordinary writes, while completion
    /// unlocks admitted captures. Synchronous Transaction abort's owned worker
    /// manages retry above this layer; observer drop is not completed abort.
    /// [Abort](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L220-L234);
    /// [end record](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L390-L478).
    fn abort_transaction(&self, transaction: Arc<dyn Transaction>)
        -> ApiFuture<'_, SequenceNumber>;
}

/// Native common/IStateProviderMap.h transactional registry operations.
/// Missing/existing names remain errors where native specifies errors, not
/// success-shaped None. Mutation participates in the supplied transaction.
///
/// Registry names project valid nonreserved native scheme-bearing URIs to UTF-8,
/// not a key/value store. Async names/transaction/parameters own accepted inputs;
/// None timeout projects native infinite. Parent-ID routing is explicit metadata.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateProviderMap.h#L10-L77);
/// [access checks](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4622-L4752).
pub trait IStateProviderMap: Send + Sync + 'static {
    /// Lookup provider synchronously after name/read-access/secondary-consistency checks.
    ///
    /// Hides transient creation; missing name is NAME_DOES_NOT_EXIST, not None.
    /// Returned shared lifetime does not prove continued registry/write access.
    /// [Lookup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L218-L258);
    /// [filter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/MetadataManager.cpp#L271-L323).
    fn get(&self, state_provider_name: &str) -> Result<Arc<dyn IStateProvider2>>;
    /// Validate name/write access, construct children/provider and admit registry adds.
    ///
    /// Children precede parent; transaction retains write locks/transient metadata.
    /// Completion is admission, not transaction commit/physical persistence.
    /// Duplicate is NAME_ALREADY_EXISTS; timeout/token apply to lock/retry stages,
    /// not rollback of accepted work. Optional initialization is not empty payload.
    /// [Add sequence](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L661-L863);
    /// [transient/duplicate admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4074-L4253);
    /// [retry admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4456-L4530).
    fn add(
        &self,
        transaction: Arc<dyn Transaction>,
        state_provider_name: String,
        state_provider_type_name: String,
        initialization_parameters: Option<OperationData>,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Validate name/write access and admit parent-first then child registry removal.
    ///
    /// Missing name fails; lock/recheck sets transient deletion before provider
    /// PrepareForRemove and delete admission. Own transaction/name; timeout/token
    /// affect lock/retry work. Completion is neither commit nor physical removal;
    /// retained providers await safe log/copy cleanup horizon.
    /// [Remove](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L865-L968);
    /// [prepare/admit](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4255-L4454).
    fn remove(
        &self,
        transaction: Arc<dyn Transaction>,
        state_provider_name: String,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Materialize filtered active-provider cursor after read-access checks.
    ///
    /// parents_only excludes child IDs. No atomic snapshot across concurrent
    /// registry mutation or snapshot-VSN registration is promised; errors fail
    /// enumeration factory, and a usable cursor owns name/provider references.
    /// Rust success requires nonnull Arc; native dictionary cursor allocation can
    /// return null without this factory translating it into failed status.
    /// [Access gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L260-L272);
    /// [nullable native allocation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/utilities/ConcurrentDictionary.h#L197-L209);
    /// [Factory](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2872-L2939).
    fn create_enumerator(&self, parents_only: bool) -> Result<Arc<dyn IStateProviderEnumerator>>;
    /// Repeatably read existing provider or admit creation, retaining transaction locks.
    ///
    /// Read lock existing path returns already_exists=true; missing releases read,
    /// acquires write and rechecks before creation/children/admission. False means
    /// newly admitted, not committed. Existing provider type/initialization are not
    /// equivalence-checked. Owned inputs/token/timeout follow add lock/retry boundaries.
    /// [Read path](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L274-L459);
    /// [write recheck](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L502-L651).
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
///
/// Ordinary transaction commit-record notification from unlock, not atomic callback
/// or transport ACK. Native throwing void becomes Result; callback error recovery
/// policy is not established merely by that representation.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ITransactionChangeHandler.h#L10-L17);
/// [invocation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L618-L636).
pub trait ITransactionChangeHandler: Send + Sync + 'static {
    /// Borrow source/ordinary transaction after capture-release loop in commit unlock path.
    ///
    /// Not an atomic event; native sites do not independently require successful
    /// apply/replication at every notification, so callback is not proof that user
    /// Commit succeeded. No isolated execution/rollback/fallible recovery promise;
    /// retain owned handles separately if needed beyond this synchronous borrow.
    /// [Unlock switch](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L366-L636).
    fn on_transaction_committed(
        &self,
        source: &dyn ITransactionalReplicator,
        transaction: &dyn ITransaction,
    ) -> Result<()>;
}

/// Native common/IStateManagerChangeHandler.h synchronous notifications.
/// Root-only registry apply/rebuild notifications differ from later transaction
/// notification. Shared handler is retained; invocation arguments are borrowed.
/// Native callback exceptions become caller status errors, not isolated callbacks.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateManagerChangeHandler.h#L10-L29);
/// [invocation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872).
pub trait IStateManagerChangeHandler: Send + Sync + 'static {
    /// Borrow parents-only cursor after rebuild/recovery or local copy-end.
    ///
    /// Not incremental mutation event; provider End/log completion may still be
    /// pending. Exceptions/Err fail caller's rebuild path; enumeration must be
    /// consumed synchronously or independently retained, not saved as borrowed view.
    /// [Notify](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872);
    /// [copy local end](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3912-L3924).
    fn on_rebuilt(
        &self,
        source: &dyn ITransactionalReplicator,
        state_providers: &dyn IStateProviderEnumerator,
    ) -> Result<()>;
    /// Borrow root provider/name/transaction on registry insert Apply before Unlock.
    ///
    /// Not an after-quorum or completed-Commit callback. Callback exceptions
    /// become apply-path errors; child entries are omitted. No rollback isolation
    /// is supplied by Result; borrow lifetime is only this invocation.
    /// [Insert](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1880-L1916);
    /// [notification](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872).
    fn on_added(
        &self,
        source: &dyn ITransactionalReplicator,
        transaction: &dyn ITransaction,
        state_provider_name: &str,
        state_provider: &dyn IStateProvider2,
    ) -> Result<()>;
    /// Borrow root provider/name/transaction on logical remove Apply before Unlock.
    ///
    /// Callback failure becomes apply-path error, not physical removal completion.
    /// Provider can remain retained for replay/copy/deferred cleanup after registry
    /// removal; child entries are omitted and borrow does not transfer ownership.
    /// [Delete](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1917-L1956);
    /// [notification](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872).
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
/// Executes after local backup capture, separately from checkpoint/restore APIs.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IBackupCallbackHandler.h#L10-L17);
/// [caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1430-L1472).
pub trait IBackupCallbackHandler: Send + Sync + 'static {
    /// Upload owned BackupInfo after capture/stabilization, using supplied token.
    ///
    /// True permits outer backup-record completion but is not sufficient for backup
    /// success. False is successful callback value converted by backup to
    /// INVALID_OPERATION; Err/exception is distinct. Native primary transition can
    /// drain this callback; observation cancellation is not a completed upload/drain.
    /// [Upload](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1430-L1472);
    /// [stabilization before callback](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L318-L364);
    /// [drain](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1796-L1837).
    fn upload_backup(
        &self,
        backup_info: BackupInfo,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, bool>;
}

/// Native common/IDataLossHandler.h authoritative state-changed callback.
/// Restore eligibility is scoped to this callback, not any arbitrary lifecycle API.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IDataLossHandler.h#L10-L15);
/// [caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1559-L1592).
pub trait IDataLossHandler: Send + Sync + 'static {
    /// Report state-changed boolean while native caller enables restore for callback scope.
    ///
    /// Native logging supplies None-token and disables eligibility in finally.
    /// False is unchanged state, not error; exceptions/Err fail data-loss call.
    /// Completion does not itself prove provider/log recovery or promotion finished.
    /// [Data loss](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1559-L1592).
    fn data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool>;
}

/// Native common/ITransactionalReplicator.h public surface and inheritance.
/// Native test hooks and internal log/control interfaces are excluded. This is
/// not the runtime-facing IPrimaryReplicator and grants no membership authority.
///
/// Combines transactional registry, transaction/snapshot management and version
/// retention, with owned async backup/restore inputs. Native composition opens
/// logging, opens/recovers providers, then replays log; declarations implement
/// none of those kernels or persistence qualification.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ITransactionalReplicator.h#L12-L64);
/// [open ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L257-L334).
pub trait ITransactionalReplicator:
    IStateProviderMap + ITransactionManager + IVersionManager
{
    /// Observe logging primary read-granted/nonprimary applied-barrier consistency.
    ///
    /// No wait or role authority; registry Get adds its own access/role validation,
    /// and getter alone does not establish provider isolation or registration.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L858-L893);
    /// [underlying](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456).
    fn get_is_readable(&self) -> bool;
    /// Obtain shared fube partition representation for native partition utility.
    ///
    /// Getter grants no new epoch/control authority; Arc represents retained
    /// interface lifetime, not continued read/write access.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L858-L893).
    fn get_stateful_partition(&self) -> Arc<dyn IStatefulServicePartition>;
    /// Observe configured persistence capability, not checkpoint/durable-commit proof.
    ///
    /// Does not enable persistence in this API-only crate.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L858-L893).
    fn get_has_persisted_state(&self) -> bool;
    /// API-activity-gated observation of logging checkpoint stable LSN.
    ///
    /// Primary barrier apply/replication advances stable; secondary uses propagated
    /// barrier stable number. Getter initiates no flush/quorum wait or snapshot registration.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L858-L893);
    /// [stable getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L435-L439);
    /// [primary barrier completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L864-L881);
    /// [barrier rules](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1554-L1630).
    fn get_last_stable_sequence_number(&self) -> Result<SequenceNumber>;
    /// Native logging API reports actual CurrentLogTailLsn, not last visible
    /// transaction commit, stable LSN or lower quorum/release progress.
    /// API activity may fail; observation does not wait for pending/control records
    /// at the tail to become application-visible.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L858-L893);
    /// [tail getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456).
    fn get_last_committed_sequence_number(&self) -> Result<SequenceNumber>;
    /// API-activity-gated observation of tail epoch's data-loss/configuration numbers.
    ///
    /// No UpdateEpoch/reconfiguration permission or global atomic progress snapshot.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L858-L893);
    /// [tail epoch](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L456).
    fn get_current_epoch(&self) -> Result<Epoch>;
    /// Retain/replace shared transaction handler in logging cache/processor.
    ///
    /// Registration performs no replay or callback serialization/drain; errors
    /// follow native registration path, not an invented activity gate or callback recovery.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L167-L240);
    /// [cache](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1930-L2048).
    fn register_transaction_change_handler(
        &self,
        handler: Arc<dyn ITransactionChangeHandler>,
    ) -> Result<()>;
    /// Clear transaction callback cache, without draining already snapped handlers.
    ///
    /// Do not assume arbitrary pre-open safety: native lower pre-processor branch
    /// still dereferences processor afterward. Result does not invent a safe error
    /// mapping for that source limitation.
    /// [Unregister](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1930-L2048).
    fn unregister_transaction_change_handler(&self) -> Result<()>;
    /// Retain/replace shared registry handler; no immediate rebuild replay.
    ///
    /// StateManager invokes borrowed root-only events on later apply/rebuild paths,
    /// not automatically at completed transaction/quorum boundary.
    /// [Cache](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L205-L216);
    /// [forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L167-L240).
    fn register_state_manager_change_handler(
        &self,
        handler: Arc<dyn IStateManagerChangeHandler>,
    ) -> Result<()>;
    /// Clear registry handler cache synchronously; snapped references can still invoke.
    ///
    /// Not a callback drain, registry mutation or transaction rollback.
    /// [Cache](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L205-L216);
    /// [invocation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5785-L5872).
    fn unregister_state_manager_change_handler(&self) -> Result<()>;
    /// Default full backup: infinite timeout/no token, requiring persisted primary profile.
    ///
    /// Retain owned upload handler; success includes capture/stabilization, upload
    /// and replicated backup record work, not checkpoint copies alone. Callback
    /// false rejects backup separately from callback Err.
    /// [Default](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L137-L149);
    /// [capture/upload/marker](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L151-L407);
    /// [gates](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L458-L522).
    fn backup(&self, handler: Arc<dyn IBackupCallbackHandler>) -> ApiFuture<'_, BackupInfo>;
    /// Full checkpoint/log or incremental chain backup with owned upload handler/token.
    ///
    /// Persisted/primary and incremental prior-chain/epoch gates apply. None timeout
    /// is infinite; token/deadline cover selected locks/provider/log/callback paths,
    /// not whole-call interruption. Full parent ID=self, incremental=previous backup.
    /// Upload true permits subsequent backup-record work; false becomes invalid-operation.
    /// Files may remain on failure. Native backup marker status-folding differs from
    /// ordinary transaction completion; no uniform lossless error mapping is claimed.
    /// [Backup sequence](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L151-L407);
    /// [incremental epoch/chain gates](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L707-L785);
    /// [selected lock timeout/token](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L905-L978);
    /// [marker completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1477-L1583).
    fn backup_with_options(
        &self,
        handler: Arc<dyn IBackupCallbackHandler>,
        backup_option: BackupOption,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, BackupInfo>;
    /// Default Safe restore with infinite timeout/no token, within persisted data-loss callback.
    ///
    /// Owned folder identifies backup chain; replaces registry/providers/log,
    /// recovers and restarts primary before deleting restore token. Not provider
    /// checkpoint installation alone or automatically retryable after interruption.
    /// [Default/restore](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L409-L566).
    /// [persisted gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L524-L575).
    fn restore(&self, backup_folder: PathBuf) -> ApiFuture<'_, ()>;
    /// Restore analyzed backup chain with explicit policy and owned folder/token.
    ///
    /// Safe requires backup epoch/tail strictly ahead; Force bypasses only this
    /// comparison, not persisted/data-loss/entry eligibility. Timeout covers some
    /// waits; token reaches analysis/providers, not a full-call rollback fence.
    /// Failure can leave restore token/partial state for next-open cleanup; accepted
    /// restore lifecycle must not depend on observation lifetime.
    /// [Restore](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L418-L566);
    /// [persisted gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L524-L575);
    /// [entry timeout](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1758-L1789);
    /// [Safe gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/BackupManager.cpp#L1840-L1905).
    fn restore_with_options(
        &self,
        backup_folder: PathBuf,
        restore_policy: RestorePolicy,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
}
