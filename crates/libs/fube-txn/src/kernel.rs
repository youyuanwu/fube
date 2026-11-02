// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Private source-derived A0 kernels, not an A1 runtime construction surface.
//!
//! Native authority: Service Fabric 51a7703a1f83749a9234d3ceb4296d8622e42cca.
//! Controlled lower/callback seams exercise algorithms without claiming native
//! execution, mixed-replica interoperability or persisted recovery.

#![allow(dead_code, reason = "A0 kernels are private, awaiting A1 integration")]

pub(crate) mod checkpoint;
pub(crate) mod composite;
pub(crate) mod copy;
pub(crate) mod copy_codec;
pub(crate) mod copy_sender;
pub(crate) mod identity;
pub(crate) mod limits;
pub(crate) mod maintenance;
pub(crate) mod map;
pub(crate) mod memory_log;
pub(crate) mod named;
pub(crate) mod native_bytes;
pub(crate) mod processor;
pub(crate) mod provider;
pub(crate) mod record;
pub(crate) mod registry;
pub(crate) mod registry_codec;
pub(crate) mod runtime;
pub(crate) mod runtime_lower;
pub(crate) mod runtime_owner;
pub(crate) mod state;
pub(crate) mod status;
pub(crate) mod transaction;
pub(crate) mod version;

#[cfg(test)]
mod callback_tests;
#[cfg(test)]
mod phase1d_tests;
#[cfg(test)]
mod phase2_tests;
#[cfg(test)]
mod phase3_tests;
#[cfg(test)]
mod runtime_bootstrap_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod trace_tests;
