// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Local nullable adapters for legacy native stream EOS (S_OK/null).

use std::{collections::VecDeque, ptr, sync::Mutex};

use mssf_com::{
    FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext},
    FabricRuntime::{
        IFabricOperation, IFabricOperationData, IFabricOperationDataStream, IFabricOperationStream,
    },
};
use windows_core::{implement, Interface, Ref, Result, HRESULT};

use crate::{
    abi::{NullableDataStream, NullableDataStream_Impl},
    com,
    context::{self, Pending},
};

pub const MAX_STREAM_ITEMS: usize = 1024;

/// A deterministic fixture result. EOS is exhaustion, never an empty Data item.
pub enum StreamItem {
    Data(IFabricOperationData),
    Failure(HRESULT),
}

struct Enumeration {
    queue: VecDeque<StreamItem>,
    pending: Option<Pending<Result<Option<IFabricOperationData>>>>,
}

#[implement(NullableDataStream)]
struct DataStream(Mutex<Enumeration>);

/// Synchronous-completion enumeration with one bounded outstanding Begin/End.
pub fn operation_data_stream(items: Vec<StreamItem>) -> Result<IFabricOperationDataStream> {
    if items.len() > MAX_STREAM_ITEMS
        || items
            .iter()
            .any(|item| matches!(item, StreamItem::Failure(status) if status.is_ok()))
    {
        return Err(com::E_INVALIDARG.into());
    }
    let stream: NullableDataStream = DataStream(Mutex::new(Enumeration {
        queue: items.into(),
        pending: None,
    }))
    .into();
    stream.cast()
}

impl NullableDataStream_Impl for DataStream_Impl {
    unsafe fn BeginGetNext(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        out: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let context = context::completed(callback)?;
            {
                let mut enumeration = self.0.lock().map_err(|_| com::E_UNEXPECTED)?;
                if enumeration.pending.is_some() {
                    return Err(com::E_INVALIDARG.into());
                }
                let result = match enumeration.queue.pop_front() {
                    Some(StreamItem::Data(data)) => Ok(Some(data)),
                    Some(StreamItem::Failure(status)) => Err(status.into()),
                    None => Ok(None),
                };
                enumeration.pending = Some(Pending::new(context.clone(), result));
            }
            unsafe { com::return_interface(out, Some(context.clone()))? };
            context::notify(&context);
            Ok(())
        })
    }

    unsafe fn EndGetNext(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        out: *mut Option<IFabricOperationData>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let result = {
                let mut enumeration = self.0.lock().map_err(|_| com::E_UNEXPECTED)?;
                context::finish(&mut enumeration.pending, context)?
            }?;
            unsafe { com::return_interface(out, result) }
        })
    }
}

/// Bypass the generated non-null projection, retaining HRESULT and null EOS.
pub fn end_get_next(
    stream: &IFabricOperationDataStream,
    context: &IFabricAsyncOperationContext,
) -> Result<Option<IFabricOperationData>> {
    let mut raw = ptr::null_mut();
    unsafe {
        let status = (stream.vtable().EndGetNext)(stream.as_raw(), context.as_raw(), &mut raw);
        com::nullable_result(status, raw)
    }
}

/// The same null-aware receive boundary for an actual native operation stream.
///
/// A returned operation has NOT been acknowledged. Its consumer must apply it
/// successfully before explicitly calling IFabricOperation::Acknowledge.
pub fn end_get_operation(
    stream: &IFabricOperationStream,
    context: &IFabricAsyncOperationContext,
) -> Result<Option<IFabricOperation>> {
    let mut raw = ptr::null_mut();
    unsafe {
        let status = (stream.vtable().EndGetOperation)(stream.as_raw(), context.as_raw(), &mut raw);
        com::nullable_result(status, raw)
    }
}
