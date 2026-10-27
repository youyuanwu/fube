// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Published interface identities and layouts with explicitly nullable outputs.
//!
//! The generated mssf projections require non-null interface returns, even where
//! the native contract permits null. These declarations retain the complete
//! published method order; windows-interface and windows_core::implement generate
//! all vtables, identity, allocation and reference-counting code.

#![allow(non_snake_case, unsafe_op_in_unsafe_fn)]

use mssf_com::{
    FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext},
    FabricRuntime::{IFabricOperationData, IFabricOperationDataStream},
    FabricTypes::{FABRIC_EPOCH, FABRIC_SEQUENCE_NUMBER},
};
use windows_core::{IUnknown, Ref, Result};

#[windows_interface::interface("841720bf-c9e8-4e6f-9c3f-6b7f4ac73bcd")]
pub(crate) unsafe trait NullableContext: IUnknown {
    fn IsCompleted(&self) -> bool;
    fn CompletedSynchronously(&self) -> bool;
    fn get_Callback(&self, callback: *mut Option<IFabricAsyncOperationCallback>) -> Result<()>;
    fn Cancel(&self) -> Result<()>;
}

#[windows_interface::interface("c4e9084c-be92-49c9-8c18-d44d088c2e32")]
pub(crate) unsafe trait NullableDataStream: IUnknown {
    pub(crate) fn BeginGetNext(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        context: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()>;
    pub(crate) fn EndGetNext(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        data: *mut Option<IFabricOperationData>,
    ) -> Result<()>;
}

#[windows_interface::interface("3ebfec79-bd27-43f3-8be8-da38ee723951")]
pub(crate) unsafe trait NullableStateProvider: IUnknown {
    fn BeginUpdateEpoch(
        &self,
        epoch: *const FABRIC_EPOCH,
        previous_boundary: FABRIC_SEQUENCE_NUMBER,
        callback: Ref<IFabricAsyncOperationCallback>,
        context: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()>;
    fn EndUpdateEpoch(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<()>;
    fn GetLastCommittedSequenceNumber(&self, boundary: *mut FABRIC_SEQUENCE_NUMBER) -> Result<()>;
    fn BeginOnDataLoss(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        context: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()>;
    fn EndOnDataLoss(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        changed: *mut bool,
    ) -> Result<()>;
    fn GetCopyContext(&self, stream: *mut Option<IFabricOperationDataStream>) -> Result<()>;
    fn GetCopyState(
        &self,
        boundary: FABRIC_SEQUENCE_NUMBER,
        copy_context: Ref<IFabricOperationDataStream>,
        stream: *mut Option<IFabricOperationDataStream>,
    ) -> Result<()>;
}

#[cfg(test)]
#[windows_interface::interface("a98fb97a-d6b0-408a-a878-a9edb09c2587")]
pub(crate) unsafe trait NullableOperationStream: IUnknown {
    fn BeginGetOperation(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        context: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()>;
    fn EndGetOperation(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        operation: *mut Option<mssf_com::FabricRuntime::IFabricOperation>,
    ) -> Result<()>;
}
