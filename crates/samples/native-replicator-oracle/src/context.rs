// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

use mssf_com::FabricCommon::{
    IFabricAsyncOperationCallback, IFabricAsyncOperationContext, IFabricAsyncOperationContext_Impl,
};
use windows_core::{implement, Interface, Ref, Result};

use crate::com;

#[implement(IFabricAsyncOperationContext)]
struct CompletedContext {
    callback: Option<IFabricAsyncOperationCallback>,
}

impl IFabricAsyncOperationContext_Impl for CompletedContext_Impl {
    fn IsCompleted(&self) -> bool {
        true
    }

    fn CompletedSynchronously(&self) -> bool {
        true
    }

    fn get_Callback(&self) -> Result<IFabricAsyncOperationCallback> {
        self.callback.clone().ok_or_else(|| com::E_POINTER.into())
    }

    fn Cancel(&self) -> Result<()> {
        // Cancellation cannot undo an already completed synchronous fixture result.
        Ok(())
    }
}

pub(crate) fn completed(
    callback: Ref<IFabricAsyncOperationCallback>,
) -> Result<IFabricAsyncOperationContext> {
    Ok(CompletedContext {
        callback: callback.as_ref().cloned(),
    }
    .into())
}

/// Publish the result before calling this; callbacks may reenter End.
pub(crate) fn notify(context: &IFabricAsyncOperationContext) {
    if let Ok(callback) = unsafe { context.get_Callback() } {
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
