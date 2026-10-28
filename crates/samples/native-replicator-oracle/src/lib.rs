// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Activated test-only host for the actual native classic replicator.
//!
//! Successful null stream results are represented by `Option`, not by invalid
//! null values of the generated non-null Rust interface types. No engine is
//! registered or started by this crate's separate ABI self-test.

#![deny(unsafe_op_in_unsafe_fn)]

mod abi;
mod async_context;
mod com;
mod context;
pub mod data;
pub mod host;
pub mod live;
pub mod provider;
pub mod stream;

#[cfg(test)]
mod tests;

use mssf_com::FabricRuntime::{
    IFabricReplicator, IFabricStateProvider, IFabricStateReplicator,
    IFabricStatefulServicePartition,
};
use mssf_com::FabricTypes::{FABRIC_REPLICATOR_SETTINGS, FABRIC_REPLICATOR_SETTINGS_FLAGS};
use windows_core::{Result, PCWSTR};

/// Both native objects are required: node control and application data access.
pub struct NativeReplicator {
    pub control: IFabricReplicator,
    pub state: IFabricStateReplicator,
}

/// Construct the actual native V1 engine through a node-supplied partition.
///
/// The activated host retains the provider and returns `control` unchanged from
/// its service replica's EndOpen. No partition is synthesized by this fixture.
pub fn create_native_replicator(
    partition: &IFabricStatefulServicePartition,
    provider: &IFabricStateProvider,
    address: &str,
) -> Result<NativeReplicator> {
    if address.is_empty() || address.contains('\0') {
        return Err(com::E_INVALIDARG.into());
    }
    let address = mssf_core::WString::from(address);
    let flags = FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_ADDRESS.0
        | FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_REQUIRE_SERVICE_ACK.0
        | FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_REPLICATION_QUEUE_INITIAL_SIZE.0
        | FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_REPLICATION_QUEUE_MAX_SIZE.0
        | FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_COPY_QUEUE_INITIAL_SIZE.0
        | FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_COPY_QUEUE_MAX_SIZE.0;
    let settings = FABRIC_REPLICATOR_SETTINGS {
        Flags: flags as u32,
        ReplicatorAddress: PCWSTR::from_raw(address.as_ptr()),
        RequireServiceAck: true,
        InitialReplicationQueueSize: 64,
        MaxReplicationQueueSize: 1024,
        InitialCopyQueueSize: 64,
        MaxCopyQueueSize: 1024,
        ..Default::default()
    };
    let mut control = None;
    // The address and settings live through this synchronous native call.
    let state = unsafe { partition.CreateReplicator(provider, &settings, &mut control)? };
    Ok(NativeReplicator {
        control: control.ok_or_else(|| windows_core::Error::from(com::E_POINTER))?,
        state,
    })
}
