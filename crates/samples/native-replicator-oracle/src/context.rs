// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

use mssf_com::FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext};
use windows_core::{implement, Interface, Ref, Result};

use crate::{
    abi::{NullableContext, NullableContext_Impl},
    com,
};

#[implement(NullableContext)]
struct CompletedContext {
    callback: Option<IFabricAsyncOperationCallback>,
}

impl NullableContext_Impl for CompletedContext_Impl {
    unsafe fn IsCompleted(&self) -> bool {
        true
    }

    unsafe fn CompletedSynchronously(&self) -> bool {
        true
    }

    unsafe fn get_Callback(&self, out: *mut Option<IFabricAsyncOperationCallback>) -> Result<()> {
        unsafe { com::return_interface(out, self.callback.clone()) }
    }

    unsafe fn Cancel(&self) -> Result<()> {
        // Cancellation cannot undo an already completed synchronous fixture result.
        Ok(())
    }
}

pub(crate) fn completed(
    callback: Ref<IFabricAsyncOperationCallback>,
) -> Result<IFabricAsyncOperationContext> {
    let context: NullableContext = CompletedContext {
        callback: callback.as_ref().cloned(),
    }
    .into();
    context.cast()
}

/// Publish the result and output context before calling this (reentrant callback).
pub(crate) fn notify(context: &IFabricAsyncOperationContext) {
    let mut raw = std::ptr::null_mut();
    let callback: Result<Option<IFabricAsyncOperationCallback>> = unsafe {
        let status = (context.vtable().get_Callback)(context.as_raw(), &mut raw);
        com::nullable_result(status, raw)
    };
    if let Ok(Some(callback)) = callback {
        unsafe { callback.Invoke(context) };
    }
}

pub(crate) struct Pending<T> {
    context: IFabricAsyncOperationContext,
    result: T,
}

impl<T> Pending<T> {
    pub(crate) fn new(context: IFabricAsyncOperationContext, result: T) -> Self {
        Self { context, result }
    }

    pub(crate) fn matches(&self, context: &Ref<IFabricAsyncOperationContext>) -> bool {
        context
            .as_ref()
            .is_some_and(|context| self.context.as_raw() == context.as_raw())
    }

    pub(crate) fn result_ref(&self) -> &T {
        &self.result
    }
}

pub(crate) fn finish<T>(
    pending: &mut Option<Pending<T>>,
    context: Ref<IFabricAsyncOperationContext>,
) -> Result<T> {
    if !pending.as_ref().is_some_and(|p| p.matches(&context)) {
        return Err(com::E_INVALIDARG.into());
    }
    Ok(pending.take().ok_or(com::E_INVALIDARG)?.result)
}
