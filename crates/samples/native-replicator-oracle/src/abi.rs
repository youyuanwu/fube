// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Unresolved nullable stream adapters retained pending binding/API permission.
//!
//! The published implementation traits cannot return S_OK/null EOS. These
//! existing declarations are NOT a duplicate-free solution; see the README's
//! concrete blocker. Providers, contexts and operation data use published traits.

#![allow(non_snake_case, unsafe_op_in_unsafe_fn)]

use mssf_com::{
    FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext},
    FabricRuntime::IFabricOperationData,
};
use windows_core::{IUnknown, Ref, Result};

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
