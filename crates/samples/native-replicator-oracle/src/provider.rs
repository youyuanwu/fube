// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Fixed-snapshot provider construction for ABI feasibility tests.
//!
//! This is not the eventual replicated map provider. It cannot apply operations,
//! reconcile divergent tails, or create a snapshot at another boundary.

use std::{ptr, sync::Mutex};

use mssf_com::{
    FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext},
    FabricRuntime::{
        IFabricOperationData, IFabricOperationDataStream, IFabricStateProvider,
        IFabricStateProvider_Impl,
    },
    FabricTypes::{FABRIC_EPOCH, FABRIC_SEQUENCE_NUMBER},
};
use windows_core::{implement, Interface, Ref, Result};

use crate::{
    com,
    context::{self, Pending},
    stream::{operation_data_stream, StreamItem, MAX_STREAM_ITEMS},
};

enum Completion {
    Epoch,
    DataLoss,
}

#[implement(IFabricStateProvider)]
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
    Ok(Provider {
        boundary,
        snapshot,
        pending: Mutex::new(None),
        epoch: Mutex::new(None),
    }
    .into())
}

impl IFabricStateProvider_Impl for Provider_Impl {
    fn GetLastCommittedSequenceNumber(&self) -> Result<FABRIC_SEQUENCE_NUMBER> {
        Ok(FABRIC_SEQUENCE_NUMBER(self.boundary))
    }

    fn GetCopyContext(&self) -> Result<IFabricOperationDataStream> {
        // Native volatile replication never requests copy context.
        operation_data_stream(Vec::new())
    }

    fn GetCopyState(
        &self,
        boundary: FABRIC_SEQUENCE_NUMBER,
        copy_context: Ref<IFabricOperationDataStream>,
    ) -> Result<IFabricOperationDataStream> {
        com::guard(|| {
            if copy_context.as_ref().is_some() {
                return Err(com::E_INVALIDARG.into());
            }
            if boundary.0 != self.boundary {
                return Err(com::E_NOTIMPL.into());
            }
            operation_data_stream(
                self.snapshot
                    .iter()
                    .cloned()
                    .map(StreamItem::Data)
                    .collect(),
            )
        })
    }

    fn BeginUpdateEpoch(
        &self,
        epoch: *const FABRIC_EPOCH,
        previous_boundary: FABRIC_SEQUENCE_NUMBER,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
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
            context::notify(&context);
            Ok(context)
        })
    }

    fn EndUpdateEpoch(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<()> {
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

    fn BeginOnDataLoss(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
            let context = context::completed(callback)?;
            {
                let mut pending = self.pending.lock().map_err(|_| com::E_UNEXPECTED)?;
                if pending.is_some() {
                    return Err(com::E_INVALIDARG.into());
                }
                *pending = Some(Pending::new(context.clone(), Completion::DataLoss));
            }
            context::notify(&context);
            Ok(context)
        })
    }

    fn EndOnDataLoss(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<bool> {
        com::guard(|| {
            let mut pending = self.pending.lock().map_err(|_| com::E_UNEXPECTED)?;
            if !pending.as_ref().is_some_and(|pending| {
                pending.matches(&context) && matches!(pending.result_ref(), Completion::DataLoss)
            }) {
                return Err(com::E_INVALIDARG.into());
            }
            context::finish(&mut pending, context)?;
            // The immutable fixture retains its state; no fabricated state loss.
            Ok(false)
        })
    }
}

/// Null-aware receive helper; this fixture returns a non-null empty stream.
pub fn get_copy_context(
    provider: &IFabricStateProvider,
) -> Result<Option<IFabricOperationDataStream>> {
    let mut raw = ptr::null_mut();
    unsafe {
        let status = (provider.vtable().GetCopyContext)(provider.as_raw(), &mut raw);
        com::nullable_result(status, raw)
    }
}
