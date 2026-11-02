// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Source-guided algorithms: Service Fabric Reliability/Replication at
// 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d. See individual modules.

//! Owned volatile classic V1 replication, not durable storage.
//!
//! The native node remains the membership/epoch authority. Receiving or dropping
//! an operation is not application acknowledgment. Native interoperability is a
//! separate executed qualification gate, not implied by codec unit tests.
//!
//! # Provider integration
//!
//! Construct a [`FabricReplicator`] with explicit runtime-issued native
//! integration for authority and callback/destructor supervision. Implement
//! [`IStateProvider`] to reconcile authoritative history at epoch boundaries and
//! return coherent inclusive-horizon copy state, including application retry identities.
//! The provider is application state, not the quorum engine.
//! Normal construction does not query provider progress. Lower Open marks Opened;
//! a composite may then initialize its upper log. The Opened current-progress
//! getter or first Primary queries the actual provider tail, caches success only,
//! and first Primary creates its initial queue at tail + 1. Opened catch-up
//! capability is zero; initial secondary uses transmitted start, not a tail rebase.
//! [`FabricReplicator::create`] defaults to classic copy;
//! [`FabricReplicator::create_with_capabilities`] explicitly selects latest-assigned
//! copy through [`extensions::StateProviderCapabilities`], never SDK discovery.
//!
//! [`IStateReplicator::replicate`] admits a nonzero sequence; apply that accepted sequence to
//! local primary state **before** awaiting [`ReplicationCompletion::wait`]. Admission and
//! local application must remain owned if the client disconnects. Canceling or
//! dropping the waiter is ambiguous and cannot undo admission. Secondary pumps
//! consume copy/replication streams, apply each item, then explicitly call
//! [`IOperation::acknowledge`]; polling, receiving or dropping never ACKs.
//! Secondary-to-Primary promotion supplies role-generation EOS before ACK drain
//! completes; reacquire streams on a later secondary role on that same owner.
//! Idle-to-Active promotion instead retains live streams/captures and completes
//! lower role immediately under this explicit-service-ACK profile. An upper
//! transactional composite still waits its copy completion, not replication EOS.
//!
//! [`extensions::Progress`] fields are distinct, role-dependent observations. In particular,
//! primary application state may lead its queue's reconciliation `applied`
//! frontier; secondary receipt/application is not a global quorum proof.
//! A Primary role or diagnostic `ready` alone cannot grant reads/writes: the
//! application must also validate the current native partition's access,
//! lifetime, epoch and host authority.
//!
//! [`Settings`] enforces finite payload, queue, peer, copy and callback budgets.
//! Resource errors are explicit, not unbounded waiting or silent eviction.
//! [`fube_runtime::ErrorKind::ReplicationQueueFull`] is temporary admission
//! pressure; [`fube_runtime::ErrorKind::ReplicationOperationTooLarge`] is permanent
//! sizing rejection. They preserve distinct native HRESULTs; generic resource
//! exhaustion and display text are not a replication retry classifier.
//! This Linux volatile profile has no durable restart, transactional stack,
//! TLS/Windows replication or rolling executable-upgrade guarantee. See
//! `docs/runtime/FubeData.md` in the repository for the executed profile,
//! reproducible qualification and the isolated native-only reference peer.
//! Historical deployed records cover their recorded source/profile only, not
//! this breaking migration. `docs/runtime/SfApiAlignment.md` separates signature,
//! values, completion and capability for each affected contract.
//!
//! # Breaking SDK contracts
//! Removed implementation/legacy paths are not compatibility APIs:
//! ```compile_fail
//! use fube_data::StateProvider;
//! ```
//! ```compile_fail
//! use fube_data::api::StateProvider;
//! ```
//! ```compile_fail
//! use fube_data::engine::Engine;
//! ```
//! ```compile_fail
//! use fube_data::{Operation, OperationStream, Replication};
//! ```
//! ```compile_fail
//! use fube_data::queue::OperationQueue;
//! ```
//! Replay and origin are explicitly supplied capabilities, never SDK discovery:
//! ```compile_fail
//! fn replay(provider: &dyn fube_data::IStateProvider) {
//!     let _ = provider.replication_checkpoint(1, fube_data::Cancellation::default());
//! }
//! ```
//! ```compile_fail
//! fn old_submit(replicator: &fube_data::FabricReplicator) {
//!     let _ = replicator.submit_in_epoch(fube_data::Epoch::ZERO,
//!         fube_data::OperationData::new(vec![]).unwrap(),
//!         fube_data::Cancellation::default());
//! }
//! ```
//! ```compile_fail
//! fn getter_is_not_async(replicator: &dyn fube_data::IStateReplicator) {
//!     let _: fube_runtime::ApiFuture<'_, std::sync::Arc<dyn fube_data::IOperationStream>>
//!         = replicator.get_copy_stream();
//! }
//! ```
//! Positive object-safe/synchronous shape witness:
//! ```
//! use fube_data::{IStateProvider, IStateReplicator, IOperationDataStream,
//!     IOperationStream, Result};
//! use std::sync::Arc;
//! fn factories(provider: &dyn IStateProvider, replicator: &dyn IStateReplicator) {
//!     let _: Result<i64> = provider.get_last_committed_sequence_number();
//!     let _: Result<Arc<dyn IOperationDataStream>> = provider.get_copy_context();
//!     let _: Result<Arc<dyn IOperationDataStream>> = provider.get_copy_state(0, None);
//!     let _: Result<Arc<dyn IOperationStream>> = replicator.get_copy_stream();
//!     let _: Result<Arc<dyn IOperationStream>> = replicator.get_replication_stream();
//! }
//! ```

mod api;
mod engine;
pub mod extensions;
mod fabric_replicator;
mod owned_lifecycle;
pub mod provider;
mod queue;
mod quorum;
mod supervised;
/// Low-level bounded peer protocol tooling for source-contract fixtures and
/// qualification. This exposes no engine/control/admission authority.
pub mod transport;
/// Low-level codecs/endpoints, not the SDK data/control contract or native proof.
pub mod wire;

pub(crate) use api::{Acknowledgment, Operation, OperationStream, Progress};
pub use api::{
    IOperation, IOperationDataStream, IOperationStream, IStateProvider, IStateReplicator,
    OperationData, OperationMetadata, OperationType, ReplicationAdmission, ReplicationCompletion,
    Settings,
};
pub use fabric_replicator::FabricReplicator;
pub use fube_runtime::{Cancellation, Epoch, Error, ReplicaRole, Result};
pub use provider::{MemoryProvider, Mutation, ProviderSnapshot};
