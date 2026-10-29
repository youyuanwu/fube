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
//! Bind an [`Engine`] with [`Engine::bind_partition`] for native runtime
//! authority and callback/destructor supervision. [`Engine::bind`] is the
//! standalone test seam and does not grant application access. Implement
//! [`StateProvider`] to reconcile authoritative history at epoch boundaries and
//! return an exact-horizon snapshot, including application retry identities.
//! The provider is application state, not the quorum engine.
//!
//! [`Engine::submit`] admits a nonzero sequence; apply that accepted sequence to
//! local primary state **before** awaiting [`Replication::wait`]. Admission and
//! local application must remain owned if the client disconnects. Canceling or
//! dropping the waiter is ambiguous and cannot undo admission. Secondary pumps
//! consume copy/replication streams, apply each item, then explicitly call
//! [`Operation::acknowledge`]; polling, receiving or dropping never ACKs.
//!
//! [`Progress`] fields are distinct, role-dependent observations. In particular,
//! primary application state may lead its queue's reconciliation `applied`
//! frontier; secondary receipt/application is not a global quorum proof.
//! A Primary role or diagnostic `ready` alone cannot grant reads/writes: the
//! application must also validate the current native partition's access,
//! lifetime, epoch and host authority.
//!
//! [`Settings`] enforces finite payload, queue, peer, copy and callback budgets.
//! Resource errors are explicit, not unbounded waiting or silent eviction.
//! This Linux volatile profile has no durable restart, transactional stack,
//! TLS/Windows replication or rolling executable-upgrade guarantee. See
//! `docs/runtime/FubeData.md` in the repository for the executed profile,
//! reproducible qualification and the isolated native-only reference peer.

pub mod api;
pub mod engine;
pub mod provider;
pub mod queue;
pub mod quorum;
mod supervised;
pub mod transport;
pub mod wire;

pub use api::*;
pub use engine::{Engine, EngineSnapshot, PeerSnapshot, Replication};
pub use mssf_runtime::{Cancellation, Epoch, ReplicaRole, StatefulPartition};
pub use mssf_transport::{Error, Result};
pub use provider::{MemoryProvider, Mutation, ProviderSnapshot};
