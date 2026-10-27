// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Source-guided algorithms: Service Fabric Reliability/Replication at
// 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d. See individual modules.

//! Owned volatile classic V1 replication, not durable storage.
//!
//! The native node remains the membership/epoch authority. Receiving or dropping
//! an operation is not application acknowledgment. Native interoperability is a
//! separate executed qualification gate, not implied by codec unit tests.

pub mod api;
pub mod queue;
pub mod wire;

pub use api::*;
pub use mssf_runtime::{Cancellation, Epoch, ReplicaRole};
pub use mssf_transport::{Error, Result};
