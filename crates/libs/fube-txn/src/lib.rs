// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Service Fabric V2 contracts and supported ordinary volatile A1 execution.
//! [`VolatileTransactionalReplicator`] constructs the real runtime and primary
//! provider registry over one normally constructed [`fube_data::FabricReplicator`].
//! The service returns the actual common/primary transactional composite.
//! Production and tests use public state/provider/operation/stream contracts
//! plus normal settings/copy capability; no lower lifecycle, rebase, execution,
//! identity, diagnostic or qualification hooks. Public host execution retains
//! owned synchronous admission; exact native invocation dispatch stays internal
//! to the framework, not an upper binder or authority.
//! Existing private A0 algorithms supply incremental records, transactions,
//! map/version bookkeeping, Apply/Unlock, checkpoint and staged Full copy.
//! Test payload providers are not ReliableCollections implementations.
//!
//! Source authority is the native `data/txnreplicator` headers and implementations at
//! [SF commit 51a7703](https://github.com/microsoft/service-fabric/tree/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator).
//! Each trait and method documents its source interface/concrete-class projection
//! and pinned implementation or caller evidence. Contracts describe native
//! semantics; private deterministic tests execute supported source-derived paths.
//! Neither declarations nor those tests establish COM ABI, executed native
//! interoperability, deployment qualification or persisted-state support.
//!
//! Rust follows the existing fube SDK representation: object-safe Send futures,
//! Result for native status/exception results, UTF-8 URI/type names, UUIDs,
//! owned immutable segmented payloads and Arc/Weak references. A timeout of
//! `None` represents native infinite timeout; `Some(Duration::ZERO)` does not.
//! Overloads have explicit Rust names and no default implementations.
//! Async inputs own their transaction handles, paths and names so implementations
//! can retain accepted work independently of a caller's observation future.
//! Synchronous callbacks borrow views where native ownership need not transfer.
//!
//! Cancellation and dropped futures are observations, not proof of rollback or
//! disposal. Implementations must preserve native owned completion,
//! Apply/replication/Unlock ordering and synchronous Abort's owned async work.
//! Native concrete transaction classes remain public trait projections;
//! supported concrete kernels execute behind the public A1 service facade.
//! Runtime control remains in fube-runtime/fube-data, not here.
//!
//! The four aggregate state/log manager interfaces are crate-internal and are not
//! SDK exports. Private-item Rustdoc exposes those contracts to maintainers only.

#![deny(missing_docs)]

mod internal;
mod kernel;
mod provider;
mod replicator;
mod runtime;
mod transaction;
mod types;

pub use fube_data::OperationData;
pub use fube_runtime::{
    ApiFuture, Cancellation, Epoch, Error, IStatefulServicePartition, ReplicaRole, Result,
};
pub use provider::{
    IOperationDataStream, IStateProvider2, IStateProvider2Factory, IStateProviderEnumerator,
};
pub use replicator::{
    EnumerationCompletionResult, IBackupCallbackHandler, IDataLossHandler,
    IStateManagerChangeHandler, IStateProviderMap, ITransactionChangeHandler, ITransactionManager,
    ITransactionVersionManager, ITransactionalReplicator, IVersionManager,
};
pub use runtime::{VolatileLogSettings, VolatileRuntimeSettings, VolatileTransactionalReplicator};
pub use transaction::{
    AtomicOperation, AtomicRedoOperation, ITransaction, LockContext, OperationContext, Transaction,
    TransactionBase,
};
pub use types::{
    ApplyContext, BackupInfo, BackupOption, BackupVersion, FactoryArguments, GetOrAddResult,
    RestorePolicy, SequenceNumber, StateProviderEntry, StateProviderId, StateProviderInfo,
    TransactionId, TransactionState, TryRemoveVersionResult,
};
