// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! API-only Rust declarations for the Service Fabric V2 application/provider
//! boundary. There is no replicator, transaction, log, state manager or provider
//! implementation in this crate.
//!
//! Source authority is the native `data/txnreplicator` headers and implementations at
//! [SF commit 51a7703](https://github.com/microsoft/service-fabric/tree/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator).
//! Each trait and method documents its source interface/concrete-class projection
//! and pinned implementation or caller evidence. Contracts describe native
//! semantics and requirements for future Rust implementations, not executed behavior.
//! These declarations are not COM ABI, native wire-format interoperability,
//! persisted-state support or evidence of implemented V2 semantics.
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
//! disposal. Future implementations must preserve native owned completion,
//! Apply/replication/Unlock ordering and synchronous Abort's owned async work.
//! Native concrete transaction classes are exposed as traits until their
//! kernels exist. Runtime control remains in fube-runtime/fube-data, not here.
//!
//! The four aggregate state/log manager interfaces are crate-internal and are not
//! SDK exports. Private-item Rustdoc exposes those contracts to maintainers only.

#![deny(missing_docs)]

mod internal;
mod provider;
mod replicator;
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
pub use transaction::{
    AtomicOperation, AtomicRedoOperation, ITransaction, LockContext, OperationContext, Transaction,
    TransactionBase,
};
pub use types::{
    ApplyContext, BackupInfo, BackupOption, BackupVersion, FactoryArguments, GetOrAddResult,
    RestorePolicy, SequenceNumber, StateProviderEntry, StateProviderId, StateProviderInfo,
    TransactionId, TransactionState, TryRemoveVersionResult,
};
