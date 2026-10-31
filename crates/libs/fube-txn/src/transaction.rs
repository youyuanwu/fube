// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{
    ApiFuture, Cancellation, OperationData, Result, SequenceNumber, StateProviderId, TransactionId,
    TransactionState,
};
use std::{any::Any, sync::Arc, time::Duration};

/// Opaque provider-owned capture; Any permits provider-specific downcasts.
///
/// Logging does not interpret user contents; aggregate wrappers add provider-ID
/// routing. Logging retains an admitted capture unless a nonnull Apply result
/// replaces it; a null result leaves it retained for explicit Unlock.
/// No Drop-as-unlock is implied.
/// [Type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/OperationContext.h#L10-L17);
/// [routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2796-L2860).
/// [capture replacement](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L682-L766).
/// [inline retained-slot setter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/BeginTransactionOperationLogRecord.h#L64-L68).
pub trait OperationContext: Any + Send + Sync + 'static {}

/// Transaction-retained lock-release capture, distinct from logged operation capture.
///
/// Native transaction disposal releases nontracking lock contexts; this projection
/// implements no lock acquisition. Native throwing callbacks become Rust results.
/// [Type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/LockContext.h#L10-L34);
/// [disposal](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L161-L179).
pub trait LockContext: OperationContext {
    /// Observe whether transaction disposal skips this capture (native default false).
    ///
    /// Does not acquire/release a lock or establish lock-manager ownership.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/LockContext.cpp#L12-L25).
    fn is_tracking_context(&self) -> bool;
    /// Release this transaction lock synchronously, returning concrete cleanup failures.
    ///
    /// Registry contexts forward read/add/remove and transient-abort cleanup to
    /// metadata locks. Not the provider's logged-operation Unlock or an async drain.
    /// [Forwarder](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManagerTransactionContext.cpp#L14-L55);
    /// [cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/MetadataManager.cpp#L987-L1100).
    fn unlock(&self) -> Result<()>;
}

/// Required instance surface of native common/TransactionBase.h, projected as
/// a trait. Static ID/recovery/retry helpers and operators are not ported here.
/// Dispose is explicit and synchronous; dropping a Rust handle is not its proof.
/// Shared handles do not authorize concurrent state-changing use.
/// [Native surface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.h#L10-L87);
/// [state gates](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionStateMachine.h#L67-L120).
pub trait TransactionBase: Send + Sync + 'static {
    /// Observe creation-role snapshot, not current primary/write authority.
    /// [Inline getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.h#L45-L83);
    /// [creation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L24-L79).
    fn get_is_primary_transaction(&self) -> bool;
    /// Observe immutable signed identity: positive ordinary, negative atomic, zero invalid.
    ///
    /// Not an LSN or application deduplication token.
    /// [Inline getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.h#L45-L83).
    fn get_transaction_id(&self) -> TransactionId;
    /// Test native identity < 0; a one-write ordinary transaction is not atomic here.
    /// [Inline getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.h#L45-L83).
    fn get_is_atomic_operation(&self) -> bool;
    /// Test identity != 0, not Active state, live manager, role or registration.
    /// [Inline getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.h#L45-L83).
    fn get_is_valid_transaction(&self) -> bool;
    /// Observe state-machine State; not native separate StateValue backing property.
    ///
    /// An observation does not reserve the instance against concurrent state changes.
    /// [Getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L130-L137).
    fn get_state(&self) -> TransactionState;
    /// Observe commit LSN, initially invalid and assigned before committed operation apply.
    ///
    /// Single/atomic uses its operation LSN; multi-write uses end-record LSN.
    /// Positive value alone proves neither quorum, successful Commit nor released locks.
    /// [Inline getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.h#L45-L83);
    /// [initial commit LSN](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L15-L57);
    /// [single/atomic assignment](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L682-L766);
    /// [assignment before apply](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L768-L966).
    fn get_commit_sequence_number(&self) -> SequenceNumber;
    /// One-time positive commit-LSN assignment by apply/replay, not application commit.
    ///
    /// Native asserts previous invalid and new > 0; Result represents the crate's
    /// fallible callback convention, not a new native error-normalization policy.
    /// [Setter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L181-L190).
    fn set_commit_sequence_number(&self, value: SequenceNumber) -> Result<()>;
    /// Retain shared lock capture synchronously; append can fail.
    ///
    /// Does not acquire/unlock or commit. Explicit disposal later releases
    /// nontracking captures independently of logged-operation contexts.
    /// [Append](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L243-L257).
    fn add_lock_context(&self, lock_context: Arc<dyn LockContext>) -> Result<()>;
    /// Explicit idempotent synchronous disposal of a valid transaction.
    ///
    /// Base releases nontracking locks; ordinary transaction first attempts
    /// visibility unregister (native ignores its status) and starts abort if Active.
    /// Must not race Reading/Committing;
    /// return is not completion of owned async abort or a Drop-as-success guarantee.
    /// [Base](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L161-L179);
    /// [ordinary](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L101-L190).
    /// [dispose state gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionStateMachine.h#L90-L120).
    fn dispose(&self) -> Result<()>;
}

/// Native common/ITransaction.h: both CommitAsync overloads and synchronous
/// Abort. Successful Abort schedules owned async abort work; it is not a drain.
/// Timeout/cancellation restores Active only after the source-classified retry
/// path, not by interrupting already accepted work or promising rollback.
///
/// Native state gates distinguish transaction-aborted, transaction-not-active,
/// concurrent-use (multithreaded transactions not allowed), and retryable
/// service-too-busy; Rust results do not add runtime status mappings.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/ITransaction.h#L10-L46);
/// [state machine](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionStateMachine.cpp#L36-L293).
pub trait ITransaction: Send + Sync + 'static {
    /// Commit Active transaction with native infinite timeout/no token defaults.
    ///
    /// Read-only commit needs no log/positive commit LSN; cached single write uses
    /// optimized begin/commit record; multi-write uses end record. Success marks
    /// Committed after logging completion, not just synchronous admission.
    /// [Entry](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L192-L236);
    /// [paths](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L406-L485).
    fn commit(&self) -> ApiFuture<'_, ()>;
    /// Commit with retry-boundary timeout/token observation, not whole-call interruption.
    ///
    /// Active→Committing; after no-write-quorum/reconfiguration-pending/
    /// replication-queue-full/service-too-busy failure, inspect token then elapsed
    /// timeout and restore Active if cancelled/timed out. Other attempted-operation
    /// failures fault; successful work wins without late cancel. Early weak-manager
    /// failure is not universally normalized to Faulted. None timeout is infinite.
    /// Future implementations must retain accepted work despite observer drop.
    /// [Retry/commit](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L406-L485);
    /// [classification](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L192-L209).
    fn commit_with_options(
        &self,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Admit abort synchronously through state gate and start owned async abort work.
    ///
    /// Read-only can abort locally; cached write unlocks original capture without log;
    /// multi-write submits abort end record. Retriable worker failures back off;
    /// nonretriable end-record failures fault asynchronously. An early expired-manager
    /// failure leaves Aborting; native worker does not normalize it to Faulted.
    /// Return does not prove Aborted,
    /// log completion, lock release or drain. No cancellation input.
    /// [Entry](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L131-L190);
    /// [worker](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L310-L385).
    fn abort(&self) -> Result<()>;
}

/// Application/provider surface of native common/Transaction.h. AddOperation
/// is synchronous (including first-operation caching); it is not a new quorum
/// wait or serial admission fence. Core-only abort helpers/stopwatch are excluded.
/// Native captures weak manager ownership; shared Rust handles do not permit
/// overlapping async read/commit/abort state use.
/// [Type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.h#L10-L51);
/// [creation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L24-L79);
/// [weak owner storage](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L35-L57).
pub trait Transaction: TransactionBase + ITransaction {
    /// Synchronously admit write under Active-state lock and creation-primary checks.
    ///
    /// Expired weak manager fails object-closed; nonprimary-origin fails not-active.
    /// First operation validates registration and caches nullable payloads/capture;
    /// next submits cached begin then new record. A later failure does not undo
    /// earlier accepted records. Incremental records may interleave with other
    /// transactions; success is neither Apply nor quorum/commit.
    /// [Admission](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L238-L308);
    /// [expired manager](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L140-L158);
    /// [creation-role rejection](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L249-L257);
    /// [log ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp#L638-L845).
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()>;
    /// Enter Reading, lazily register/cache one snapshot VSN, then restore Active.
    ///
    /// Registration waits dispatching barrier and samples last applied barrier;
    /// not this transaction's commit LSN. Repeated calls reuse registration.
    /// Explicit disposal attempts unregister; dropping this observer does not release it.
    /// State/manager failures remain distinct from an absent/zero version.
    /// [Read](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L387-L404);
    /// [read state gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L192-L206);
    /// [register](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L276-L319);
    /// [version sample](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L577-L588).
    fn get_visibility_sequence_number(&self) -> ApiFuture<'_, SequenceNumber>;
}

/// Trait projection of native common/AtomicOperation.h. One operation per
/// instance; unlike AtomicRedoOperation this signature includes undo data.
/// Does not inherit ITransaction's commit/abort. Native false-progress uses undo;
/// misleading header prose does not remove the actual undo argument.
/// [Type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/AtomicOperation.h#L10-L46);
/// [undo caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TruncateTailManager.cpp#L152-L268).
pub trait AtomicOperation: TransactionBase {
    /// Submit one undo-capable atomic record: creation-primary/live-manager and Active gates.
    ///
    /// Active→Committing; success marks Committed after apply/replication and unlock
    /// invocation. Classified transient calls the retry hook, without an internal
    /// retry loop. At this pin the hook does not assign Active, leaving Committing;
    /// do not promise usable caller retry. Other attempted-operation failure faults. Own nullable
    /// data/capture; no native timeout/token input or observer-drop disposal.
    /// [Atomic add](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/AtomicOperation.cpp#L63-L118);
    /// [retry hook limitation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionStateMachine.cpp#L84-L96);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, ()>;
}

/// Trait projection of native common/AtomicRedoOperation.h; no undo input.
/// False progress with target's recovered redo-only LSN above source start forces full copy.
/// One successful operation per instance, without ITransaction commit/abort.
/// [Type](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/AtomicRedoOperation.h#L10-L48);
/// [copy mode](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/CopyModeResult.cpp#L75-L94).
pub trait AtomicRedoOperation: TransactionBase {
    /// Submit redo-only atomic record with owned nullable metadata/redo/capture.
    ///
    /// Creation-primary/live manager and Active→Committing gates match atomic add.
    /// Successful completion commits; classified transient calls the same nonassigning
    /// retry hook, leaving Committing at this pin, not a usable Active retry state.
    /// Other attempted-operation failure faults. No token/undo input.
    /// Manager receives commit LSN; this public unit result is not an LSN output.
    /// [Redo add](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/AtomicRedoOperation.cpp#L65-L120);
    /// [retry hook limitation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionStateMachine.cpp#L84-L96);
    /// [manager](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L296-L326).
    fn add_operation(
        &self,
        metadata: Option<OperationData>,
        redo: Option<OperationData>,
        state_provider_id: StateProviderId,
        operation_context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, ()>;
}
