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
/// Sequential single-consumer use is required; Send/Sync is not cursor concurrency.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IOperationDataStream.h#L10-L43);
/// [helper](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/OperationDataStream.cpp#L23-L90).
pub trait IOperationDataStream: Send + Sync + 'static {
    /// Await the next sequential payload; successful None is EOS, Some(empty) is data.
    ///
    /// Native named stream forwards token to current child. Failure/cancellation
    /// is not EOS or disposal; no payload acknowledgment exists at this boundary.
    /// [Enumeration](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/NamedOperationDataStream.cpp#L32-L105).
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>>;
    /// Explicit synchronous stream cleanup, separate from EOS or cancelled observation.
    ///
    /// Native helper's default cleanup is empty; named/copy streams dispose retained
    /// children/readers. Future implementations must retain this upper disposal
    /// boundary, not assume Drop or exhausting an inner stream completed it.
    /// [Helper](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/OperationDataStream.cpp#L23-L90);
    /// [upper cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/NamedOperationDataStream.cpp#L107-L140).
    /// [copy-reader cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L113-L150).
    fn dispose(&self) -> Result<()>;
}

/// Native Data::IEnumerator specialized to state-provider name/object pairs.
/// Single-consumer cursor; Current is valid only after successful MoveNext.
/// Potential native exceptions become Result, not an empty enumeration.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/IEnumerator.h#L10-L18);
/// [registry factory](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2872-L2939).
pub trait IStateProviderEnumerator: Send + Sync + 'static {
    /// Obtain owned name/provider pair at current cursor after true move_next.
    ///
    /// Native cursor rejects null/tail; before-first/end access is not successful
    /// absence. Cloned provider lifetime does not guarantee ongoing registration.
    /// [Cursor](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/utilities/ConcurrentSkipListEnumerator.h#L36-L83).
    fn current(&self) -> Result<StateProviderEntry>;
    /// Advance synchronously; false is tail, errors are not an empty enumeration.
    ///
    /// Registry factory materializes filtered entries; no transactional read lock,
    /// snapshot-version registration, cancellation or global ordering is implied.
    /// [Cursor](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/utilities/ConcurrentSkipListEnumerator.h#L36-L83);
    /// [factory](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2872-L2939).
    fn move_next(&self) -> Result<bool>;
}

/// Native common/IStateProvider2.h, distinct from the lower V1 IStateProvider.
/// All callbacks are declared, including future persisted profiles; declaration
/// alone does not implement or qualify recovery, checkpoint, backup or restore.
/// Native throwing void callbacks are represented by fallible Rust results.
/// StateManager/ApiDispatcher owns lifecycle ordering and routes by registered ID,
/// not guessed URI hierarchy. Provider isolation/idempotency/rollback are provider
/// obligations, not algorithms supplied by this trait.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateProvider2.h#L10-L155);
/// [dispatch](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2796-L2860).
pub trait IStateProvider2: Send + Sync + 'static {
    /// Borrow provider's identity name, used as root for recursive child discovery.
    ///
    /// UTF-8 str projects native URI; getter neither changes name nor looks up
    /// writable registration. Provider implementation must retain borrowed name.
    /// [Concrete getter](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L1538-L1541);
    /// [Caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3926-L4030).
    fn get_name(&self) -> &str;
    /// Describe children synchronously under supplied root URI; empty means no children.
    ///
    /// Manager recursively discovers/factory-creates children with assigned IDs
    /// before transactional registration. Description failure propagates, not
    /// partial successful registry creation; callback does not initialize/open them.
    /// [Discovery](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3926-L4030).
    fn get_children(&self, root_name: &str) -> Result<Vec<StateProviderInfo>>;
    /// Initialize weak owner/work-folder/child handles before open.
    ///
    /// Weak storage prevents owner cycle; live owner is a lifecycle precondition,
    /// not permission to succeed with an expired target. None children differs from
    /// supplied empty slice. Manager initializes children first and delays open
    /// until parent's list is ready. Borrowed path/children must be retained as
    /// owned values if needed later; native exceptions become Result.
    /// [Dispatch](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ApiDispatcher.cpp#L71-L104);
    /// [ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4797-L4966).
    fn initialize(
        &self,
        transactional_replicator: Weak<dyn ITransactionalReplicator>,
        work_folder: &Path,
        children: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()>;

    /// Await open after initialize and before recovery/ready metadata publication.
    ///
    /// Token is forwarded even where outer lifecycle supplies native None. Failure
    /// prevents successful initialization and triggers close/abort cleanup for
    /// already opened providers; it is not a recovered checkpoint.
    /// [Ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4797-L4966).
    fn open(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Await ordered role callback before manager publishes transition completion.
    ///
    /// Must not race in-flight apply. Active-secondary initialization passes through
    /// IdleSecondary; None precedes remove_state. Token/failures pass through
    /// dispatcher, not a new role-authority or rollback policy.
    /// [Role ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L103-L179);
    /// [new provider](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4797-L4966).
    fn change_role(&self, new_role: ReplicaRole, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Await terminal close with forwarded token, not Drop or checkpoint installation.
    ///
    /// Dispatcher can abort on close failure and return successful terminal fallback,
    /// or propagate failure in its other mode; not every native close failure
    /// uniformly reaches caller. Explicit abort remains a separate boundary.
    /// [Close modes](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ApiDispatcher.cpp#L162-L206).
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Native noexcept synchronous terminal abort, used for lifecycle failure fallback.
    ///
    /// No result/token/async drain is native; dispatcher asserts on thrown callback.
    /// Future implementations must own any terminal cleanup rather than treating
    /// dropped observation/cancellation as completed disposal.
    /// [Abort](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ApiDispatcher.cpp#L208-L226).
    fn abort(&self);

    /// Synchronously capture prepared state at checkpoint LSN after registry snapshot.
    ///
    /// Not installed checkpoint. Logging gates later perform on stability and
    /// serializes checkpoint APIs; preparation failures fail checkpoint processing.
    /// [Manager](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1389-L1430);
    /// [logging](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L368-L452).
    fn prepare_checkpoint(&self, checkpoint_lsn: SequenceNumber) -> Result<()>;
    /// Perform previously prepared checkpoint asynchronously on captured provider set.
    ///
    /// Completion precedes aggregate copy/checkpoint metadata publication; native
    /// logging serializes phases and faults service on failure. Forwarded token
    /// is not a guarantee to undo file/state work after cancellation.
    /// [Perform](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1432-L1467);
    /// [fault path](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1393-L1552).
    fn perform_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Finish/install prepared checkpoint before aggregate file replacement/cleanup.
    ///
    /// Recovery can call this after open and before recover_checkpoint when checkpoint
    /// end lacks completion. Token/errors follow dispatcher; not new durability proof.
    /// [Completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1469-L1512);
    /// [recovery ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2982-L3102).
    fn complete_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Recover checkpoint contents into memory after initialize/open/completion.
    ///
    /// Runs before log replay and separately after restore file installation.
    /// Forwarded token and errors do not constitute successful empty state.
    /// [Open/recover](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2982-L3102);
    /// [restore/recover](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1558-L1701).
    fn recover_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Back up this provider's current checkpoint to owned provider-specific directory.
    ///
    /// Aggregate backs registry first and invokes active providers under logging
    /// coordination. Token reaches callback; failure fails provider backup, while
    /// success alone is not complete transaction/log backup or external upload.
    /// [Backup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/CheckpointManager.cpp#L849-L921).
    fn backup_checkpoint(
        &self,
        backup_directory: PathBuf,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Install checkpoint files from owned directory, without recovering their contents.
    ///
    /// Aggregate separately opens, restores, then recovers provider. Token/failure
    /// are preserved; this callback does not restore log or become primary.
    /// [Restore ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1558-L1701);
    /// [dispatch](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ApiDispatcher.cpp#L827-L850).
    fn restore_checkpoint(
        &self,
        backup_directory: PathBuf,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Remove provider state after role None, including deferred safe-horizon cleanup.
    ///
    /// Logical registry remove admission/commit does not immediately call this:
    /// delete LSN (or stale create LSN) must be strictly below retained log/copy horizon.
    /// Token/errors affect cleanup, not retroactive transactional rollback.
    /// [Cleanup](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5301-L5425).
    fn remove_state(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    /// Synchronously create optional provider copy stream, with no target replica ID.
    ///
    /// None means no provider copy stream and aggregate skips it; nonnull stream
    /// may immediately return EOS or emit empty payloads. Creating stream is not
    /// enumerating/installing state; explicit upper stream disposal is still needed.
    /// [Capture](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/CheckpointManager.cpp#L537-L648).
    fn get_current_state(&self) -> Result<Option<Arc<dyn IOperationDataStream>>>;
    /// Begin provider installation after initialize/open/recover/IdleSecondary.
    ///
    /// No native token. Manager tracks only providers whose begin succeeds;
    /// failure does not authorize a successful end of partial copy.
    /// [Begin ordering](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L3665-L3910).
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()>;
    /// Install required provider copy payload after successful begin, in stream order.
    ///
    /// Signed record number is global aggregate ordinal, not necessarily contiguous
    /// per provider. Owned data survives accepted work; provider token exists but
    /// aggregate native caller supplies None. Errors/cancel are not EOS or copy completion.
    /// [Routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5657-L5775);
    /// [ordinal/caller](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L974-L1100).
    fn set_current_state(
        &self,
        state_record_number: i64,
        operation_data: OperationData,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// End valid provider state stage only for successfully begun providers.
    ///
    /// Native aggregate calls with None-token after local rebuild; aborted partial
    /// state copy skips End. Completion does not imply overlapping log replay or
    /// copied checkpoint publication, and failures are not successful empty state.
    /// [End](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L5657-L5775);
    /// [abort path](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L974-L1100).
    fn end_setting_current_state(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;

    /// Coordinate removal with owned transaction after registry lock/transient delete.
    ///
    /// Runs before delete-operation admission, not physical state deletion.
    /// Failure/token interruption leaves transaction-owned lock/cleanup obligations;
    /// callback does not commit/abort the transaction on its own.
    /// [Remove sequence](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4255-L4454).
    fn prepare_for_remove(
        &self,
        transaction: Arc<dyn TransactionBase>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Apply operation at its LSN with role/operation discriminator and owned transaction.
    ///
    /// Multi-write committed operations apply in operation order with distinct
    /// end-record commit LSN already assigned; single/atomic LSNs can coincide.
    /// Redo/false-progress undo are different paths. Metadata/data and returned
    /// context may be absent, not empty. None result does not discard an admitted
    /// capture. Apply can precede quorum: provider locks/version contexts govern
    /// visibility, not an invented aggregate root-swap/rollback. No token input.
    /// Failures reach logging service-error paths; unlock is separately required.
    /// [Apply](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L638-L966);
    /// [provider routing](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L2796-L2860);
    /// [false-progress undo](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TruncateTailManager.cpp#L152-L268);
    /// [completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527).
    fn apply(
        &self,
        logical_sequence_number: SequenceNumber,
        transaction: Arc<dyn TransactionBase>,
        apply_context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>>;
    /// Explicit synchronous release of routed provider capture, not Drop/async disposal.
    ///
    /// Primary invokes after apply and replication waits even on failure; secondary
    /// follows apply. Providers may reuse a transaction capture and implement
    /// repeated unlock idempotently; globally unique contexts are not required.
    /// Failure records a service error separately from transaction completion status;
    /// outside recovery it reports a fault, while recovery surfaces the error at open.
    /// Neither proves a successful commit rolled back.
    /// [Unlock](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L366-L616);
    /// [primary completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L480-L527);
    /// [nonprimary completion](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L333-L359);
    /// [service-error gate](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L969-L990);
    /// [concrete repeated release](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L2372-L2393).
    fn unlock(&self, operation_context: &dyn OperationContext) -> Result<()>;
}

/// Native statemanager/IStateProvider2Factory.h synchronous Create callback.
/// Constructs a provider, not initialize/open/role/recovery. Native status/out
/// pointer becomes Result with nonnull provider success.
/// [Interface](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/IStateProvider2Factory.h#L12-L22);
/// [dispatcher](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ApiDispatcher.cpp#L39-L69).
pub trait IStateProvider2Factory: Send + Sync + 'static {
    /// Construct from borrowed assigned identity/type/partition/replica/optional parameters.
    ///
    /// Native dispatcher preserves failure and nulls failed output; successful
    /// call sites require nonnull provider. Copy any arguments needed later;
    /// weak owner, work folder and child handles arrive separately at initialize.
    /// [Create](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/ApiDispatcher.cpp#L39-L69);
    /// [initialization](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L4797-L4966).
    fn create(&self, arguments: &FactoryArguments) -> Result<Arc<dyn IStateProvider2>>;
}
