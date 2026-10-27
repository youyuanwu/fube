// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Fixed-snapshot provider construction for ABI feasibility tests.
//!
//! This is not the eventual replicated map provider. It cannot apply operations,
//! reconcile divergent tails, or create a snapshot at another boundary.

use std::{ptr, sync::Mutex};

use mssf_com::{
    FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext},
    FabricRuntime::{IFabricOperationData, IFabricOperationDataStream, IFabricStateProvider},
    FabricTypes::{FABRIC_EPOCH, FABRIC_SEQUENCE_NUMBER},
};
use windows_core::{implement, Interface, Ref, Result};

use crate::{
    abi::{NullableStateProvider, NullableStateProvider_Impl},
    com,
    context::{self, Pending},
    stream::{operation_data_stream, StreamItem, MAX_STREAM_ITEMS},
};

enum Completion {
    Epoch,
    DataLoss,
}

#[implement(NullableStateProvider)]
struct Provider {
    boundary: i64,
    snapshot: Vec<IFabricOperationData>,
    pending: Mutex<Option<Pending<Completion>>>,
    epoch: Mutex<Option<(i64, i64)>>,
}

/// A bounded immutable snapshot; only its exact boundary can be copied.
pub fn snapshot_provider(
    boundary: i64,
    snapshot: Vec<IFabricOperationData>,
) -> Result<IFabricStateProvider> {
    if boundary < 0 || snapshot.len() > MAX_STREAM_ITEMS {
        return Err(com::E_INVALIDARG.into());
    }
    let provider: NullableStateProvider = Provider {
        boundary,
        snapshot,
        pending: Mutex::new(None),
        epoch: Mutex::new(None),
    }
    .into();
    provider.cast()
}

impl NullableStateProvider_Impl for Provider_Impl {
    unsafe fn GetLastCommittedSequenceNumber(
        &self,
        out: *mut FABRIC_SEQUENCE_NUMBER,
    ) -> Result<()> {
        if out.is_null() {
            return Err(com::E_POINTER.into());
        }
        unsafe { out.write(FABRIC_SEQUENCE_NUMBER(self.boundary)) };
        Ok(())
    }

    unsafe fn GetCopyContext(&self, out: *mut Option<IFabricOperationDataStream>) -> Result<()> {
        // Volatile classic copy context is null, not an allocated empty stream.
        unsafe { com::clear_out(out) }
    }

    unsafe fn GetCopyState(
        &self,
        boundary: FABRIC_SEQUENCE_NUMBER,
        copy_context: Ref<IFabricOperationDataStream>,
        out: *mut Option<IFabricOperationDataStream>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            if copy_context.as_ref().is_some() {
                return Err(com::E_INVALIDARG.into());
            }
            if boundary.0 != self.boundary {
                return Err(com::E_NOTIMPL.into());
            }
            let stream = operation_data_stream(
                self.snapshot
                    .iter()
                    .cloned()
                    .map(StreamItem::Data)
                    .collect(),
            )?;
            unsafe { com::return_interface(out, Some(stream)) }
        })
    }

    unsafe fn BeginUpdateEpoch(
        &self,
        epoch: *const FABRIC_EPOCH,
        previous_boundary: FABRIC_SEQUENCE_NUMBER,
        callback: Ref<IFabricAsyncOperationCallback>,
        out: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let epoch = unsafe { epoch.as_ref() }.ok_or(com::E_POINTER)?;
            if previous_boundary.0 != self.boundary {
                // This fixed fixture cannot truthfully reconcile another history.
                return Err(com::E_NOTIMPL.into());
            }
            if epoch.DataLossNumber < 0 || epoch.ConfigurationNumber < 0 {
                return Err(com::E_INVALIDARG.into());
            }
            let next = (epoch.DataLossNumber, epoch.ConfigurationNumber);
            let context = context::completed(callback)?;
            {
                let mut pending = self.pending.lock().map_err(|_| com::E_UNEXPECTED)?;
                if pending.is_some() {
                    return Err(com::E_INVALIDARG.into());
                }
                let mut current = self.epoch.lock().map_err(|_| com::E_UNEXPECTED)?;
                if current.is_some_and(|current| next < current) {
                    return Err(com::E_INVALIDARG.into());
                }
                *current = Some(next);
                *pending = Some(Pending::new(context.clone(), Completion::Epoch));
            }
            unsafe { com::return_interface(out, Some(context.clone()))? };
            context::notify(&context);
            Ok(())
        })
    }

    unsafe fn EndUpdateEpoch(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<()> {
        com::guard(|| {
            let mut pending = self.pending.lock().map_err(|_| com::E_UNEXPECTED)?;
            if !pending.as_ref().is_some_and(|pending| {
                pending.matches(&context) && matches!(pending.result_ref(), Completion::Epoch)
            }) {
                return Err(com::E_INVALIDARG.into());
            }
            context::finish(&mut pending, context)?;
            Ok(())
        })
    }

    unsafe fn BeginOnDataLoss(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        out: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let context = context::completed(callback)?;
            {
                let mut pending = self.pending.lock().map_err(|_| com::E_UNEXPECTED)?;
                if pending.is_some() {
                    return Err(com::E_INVALIDARG.into());
                }
                *pending = Some(Pending::new(context.clone(), Completion::DataLoss));
            }
            unsafe { com::return_interface(out, Some(context.clone()))? };
            context::notify(&context);
            Ok(())
        })
    }

    unsafe fn EndOnDataLoss(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        changed: *mut bool,
    ) -> Result<()> {
        com::guard(|| {
            if changed.is_null() {
                return Err(com::E_POINTER.into());
            }
            unsafe { changed.write(false) };
            let mut pending = self.pending.lock().map_err(|_| com::E_UNEXPECTED)?;
            if !pending.as_ref().is_some_and(|pending| {
                pending.matches(&context) && matches!(pending.result_ref(), Completion::DataLoss)
            }) {
                return Err(com::E_INVALIDARG.into());
            }
            context::finish(&mut pending, context)?;
            // The immutable fixture retains its state; no fabricated state loss.
            Ok(())
        })
    }
}

/// Null-aware projection for the volatile provider's GetCopyContext.
pub fn get_copy_context(
    provider: &IFabricStateProvider,
) -> Result<Option<IFabricOperationDataStream>> {
    let mut raw = ptr::null_mut();
    unsafe {
        let status = (provider.vtable().GetCopyContext)(provider.as_raw(), &mut raw);
        com::nullable_result(status, raw)
    }
}
