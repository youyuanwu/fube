// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

use std::{
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use mssf_com::{
    FabricCommon::{
        IFabricAsyncOperationCallback, IFabricAsyncOperationCallback_Impl,
        IFabricAsyncOperationContext,
    },
    FabricRuntime::{
        IFabricOperation, IFabricOperationData, IFabricOperationDataStream,
        IFabricOperationData_Impl, IFabricOperationStream,
    },
    FabricTypes::{FABRIC_EPOCH, FABRIC_OPERATION_DATA_BUFFER, FABRIC_SEQUENCE_NUMBER},
};
use windows_core::{implement, IUnknown, Interface, Ref, Result, GUID, HRESULT};

use crate::{
    abi::{NullableOperationStream, NullableOperationStream_Impl},
    com, context,
    data::{operation_data, read_segments, MAX_OPERATION_BYTES, MAX_SEGMENTS},
    provider::{get_copy_context, snapshot_provider},
    stream::{end_get_next, end_get_operation, operation_data_stream, StreamItem},
};

const E_FAIL: HRESULT = HRESULT(0x8000_4005u32 as i32);

fn next(stream: &IFabricOperationDataStream) -> Result<Option<IFabricOperationData>> {
    let context = unsafe { stream.BeginGetNext(None)? };
    assert!(unsafe { context.IsCompleted() });
    assert!(unsafe { context.CompletedSynchronously() });
    end_get_next(stream, &context)
}

#[test]
fn segmented_nonempty_and_empty_data_are_not_null_eos() {
    let payloads = [
        vec![vec![1, 2, 3], vec![], vec![4, 5]],
        vec![],
        vec![vec![]],
    ];
    let items = payloads
        .iter()
        .cloned()
        .map(|payload| StreamItem::Data(operation_data(payload).unwrap()))
        .collect();
    let stream = operation_data_stream(items).unwrap();
    for expected in payloads {
        let operation = next(&stream).unwrap().expect("non-null data object");
        assert_eq!(unsafe { read_segments(&operation).unwrap() }, expected);
    }

    let context = unsafe { stream.BeginGetNext(None).unwrap() };
    let mut raw = stream.as_raw();
    let status =
        unsafe { (stream.vtable().EndGetNext)(stream.as_raw(), context.as_raw(), &mut raw) };
    assert_eq!(status, com::S_OK);
    assert!(raw.is_null());
    assert!(next(&stream).unwrap().is_none());
}

#[test]
fn errors_preserve_hresult_clear_outputs_and_consume_only_once() {
    let stream = operation_data_stream(vec![StreamItem::Failure(E_FAIL)]).unwrap();
    let context = unsafe { stream.BeginGetNext(None).unwrap() };
    let mut raw = stream.as_raw();
    assert_eq!(
        unsafe { (stream.vtable().EndGetNext)(stream.as_raw(), context.as_raw(), &mut raw) },
        E_FAIL
    );
    assert!(raw.is_null());
    assert_eq!(
        end_get_next(&stream, &context).unwrap_err().code(),
        com::E_INVALIDARG
    );

    let stream = operation_data_stream(vec![StreamItem::Failure(E_FAIL)]).unwrap();
    assert_eq!(next(&stream).unwrap_err().code(), E_FAIL);
    assert!(operation_data_stream(vec![StreamItem::Failure(com::S_OK)]).is_err());
}

#[test]
fn invalid_outputs_foreign_contexts_and_overlapping_begin_preserve_pending_result() {
    let stream = operation_data_stream(vec![StreamItem::Data(
        operation_data(vec![vec![9]]).unwrap(),
    )])
    .unwrap();
    assert_eq!(
        unsafe {
            (stream.vtable().BeginGetNext)(stream.as_raw(), ptr::null_mut(), ptr::null_mut())
        },
        com::E_POINTER
    );
    let context = unsafe { stream.BeginGetNext(None).unwrap() };
    assert_eq!(
        unsafe { stream.BeginGetNext(None) }.unwrap_err().code(),
        com::E_INVALIDARG
    );

    let other = operation_data_stream(Vec::new()).unwrap();
    let foreign = unsafe { other.BeginGetNext(None).unwrap() };
    assert_eq!(
        end_get_next(&stream, &foreign).unwrap_err().code(),
        com::E_INVALIDARG
    );
    assert_eq!(
        unsafe { (stream.vtable().EndGetNext)(stream.as_raw(), context.as_raw(), ptr::null_mut()) },
        com::E_POINTER
    );
    let operation = end_get_next(&stream, &context).unwrap().unwrap();
    assert_eq!(unsafe { read_segments(&operation).unwrap() }, vec![vec![9]]);
}

#[test]
fn buffer_bounds_and_null_outputs_are_checked() {
    assert!(operation_data(vec![vec![]; MAX_SEGMENTS + 1]).is_err());
    assert!(operation_data(vec![vec![0; MAX_OPERATION_BYTES + 1]]).is_err());
    let data = operation_data(vec![vec![3]]).unwrap();
    let mut buffers = ptr::null_mut();
    assert_eq!(
        unsafe { (data.vtable().GetData)(data.as_raw(), ptr::null_mut(), &mut buffers) },
        com::E_POINTER
    );
    assert!(buffers.is_null());
}

#[test]
fn local_nullable_projection_preserves_empty_data_and_null_eos() {
    let published =
        operation_data_stream(vec![StreamItem::Data(operation_data(Vec::new()).unwrap())]).unwrap();
    let nullable = published.cast::<crate::abi::NullableDataStream>().unwrap();
    for has_data in [true, false] {
        let mut context = None;
        unsafe { nullable.BeginGetNext(None, &mut context).unwrap() };
        let mut data = None;
        unsafe { nullable.EndGetNext(context.as_ref(), &mut data).unwrap() };
        assert_eq!(data.is_some(), has_data);
        if let Some(data) = data {
            assert!(unsafe { read_segments(&data).unwrap() }.is_empty());
        }
    }
}

fn check_identity<T: Interface>(value: &T) {
    let unknown = value.cast::<IUnknown>().unwrap();
    let round_trip = unknown.cast::<T>().unwrap();
    assert_eq!(round_trip.as_raw(), value.as_raw());
    assert_eq!(
        round_trip.cast::<IUnknown>().unwrap().as_raw(),
        unknown.as_raw()
    );
    let mut out = value.as_raw();
    assert_eq!(
        unsafe { (unknown.vtable().QueryInterface)(unknown.as_raw(), &GUID::zeroed(), &mut out) },
        com::E_NOINTERFACE
    );
    assert!(out.is_null());
    assert_eq!(
        unsafe { (unknown.vtable().QueryInterface)(unknown.as_raw(), ptr::null(), &mut out) },
        com::E_POINTER
    );
    assert!(out.is_null());
    assert_eq!(
        unsafe { (unknown.vtable().QueryInterface)(unknown.as_raw(), &T::IID, ptr::null_mut()) },
        com::E_POINTER
    );
    drop(round_trip);
    drop(unknown);
}

#[implement(IFabricOperationData)]
struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl IFabricOperationData_Impl for DropProbe_Impl {
    fn GetData(&self, count: *mut u32) -> Result<*mut FABRIC_OPERATION_DATA_BUFFER> {
        if count.is_null() {
            return Err(com::E_POINTER.into());
        }
        unsafe { count.write(0) };
        Ok(ptr::null_mut())
    }
}

#[test]
fn query_interface_addref_release_and_transferred_ownership_are_balanced() {
    let drops = Arc::new(AtomicUsize::new(0));
    let data: IFabricOperationData = DropProbe(drops.clone()).into();
    check_identity(&data);
    let unknown = data.cast::<IUnknown>().unwrap();
    assert_eq!(unsafe { (unknown.vtable().AddRef)(unknown.as_raw()) }, 3);
    assert_eq!(unsafe { (unknown.vtable().Release)(unknown.as_raw()) }, 2);
    drop(unknown);

    let stream = operation_data_stream(vec![StreamItem::Data(data)]).unwrap();
    check_identity(&stream);
    let context = unsafe { stream.BeginGetNext(None).unwrap() };
    check_identity(&context);
    let operation = end_get_next(&stream, &context).unwrap().unwrap();
    drop(stream);
    drop(context);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(operation);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_owner_releases_queued_and_unended_results() {
    for begin in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let stream =
            operation_data_stream(vec![StreamItem::Data(DropProbe(drops.clone()).into())]).unwrap();
        let context = if begin {
            Some(unsafe { stream.BeginGetNext(None).unwrap() })
        } else {
            None
        };
        drop(stream);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(context);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

type CallbackResult = Arc<Mutex<Option<Result<Option<IFabricOperationData>>>>>;

#[implement(IFabricAsyncOperationCallback)]
struct ReentrantCallback {
    stream: IFabricOperationDataStream,
    calls: Arc<AtomicUsize>,
    output: CallbackResult,
}

impl IFabricAsyncOperationCallback_Impl for ReentrantCallback_Impl {
    fn Invoke(&self, context: Ref<IFabricAsyncOperationContext>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.output.lock().unwrap() = Some(end_get_next(&self.stream, context.unwrap()));
    }
}

#[test]
fn synchronous_callback_can_reenter_end_without_lock_or_ownership_hazards() {
    let stream = operation_data_stream(vec![StreamItem::Data(
        operation_data(vec![vec![7]]).unwrap(),
    )])
    .unwrap();
    let output: CallbackResult = Arc::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let callback: IFabricAsyncOperationCallback = ReentrantCallback {
        stream: stream.clone(),
        calls: calls.clone(),
        output: output.clone(),
    }
    .into();
    let context = unsafe { stream.BeginGetNext(&callback).unwrap() };
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let data = output.lock().unwrap().take().unwrap().unwrap().unwrap();
    assert_eq!(unsafe { read_segments(&data).unwrap() }, vec![vec![7]]);
    assert_eq!(
        unsafe { context.get_Callback().unwrap() }.as_raw(),
        callback.as_raw()
    );
    unsafe { context.Cancel().ok().unwrap() };
    assert!(next(&stream).unwrap().is_none());
}

#[test]
fn absent_callback_is_raw_success_null_and_cancel_does_not_undo_completion() {
    let stream = operation_data_stream(Vec::new()).unwrap();
    let context = unsafe { stream.BeginGetNext(None).unwrap() };
    let mut raw = context.as_raw();
    let status = unsafe { (context.vtable().get_Callback)(context.as_raw(), &mut raw) };
    assert_eq!(status, com::S_OK);
    assert!(raw.is_null());
    unsafe { context.Cancel().ok().unwrap() };
    assert!(end_get_next(&stream, &context).unwrap().is_none());
}

#[test]
fn provider_has_nullable_volatile_context_and_exact_snapshot_boundary() {
    let payload = vec![vec![4, 8], vec![]];
    let provider = snapshot_provider(7, vec![operation_data(payload.clone()).unwrap()]).unwrap();
    check_identity(&provider);
    assert_eq!(
        unsafe { provider.GetLastCommittedSequenceNumber().unwrap() }.0,
        7
    );
    assert!(get_copy_context(&provider).unwrap().is_none());
    let stream = unsafe {
        provider
            .GetCopyState(FABRIC_SEQUENCE_NUMBER(7), None)
            .unwrap()
    };
    let operation = next(&stream).unwrap().unwrap();
    assert_eq!(unsafe { read_segments(&operation).unwrap() }, payload);
    assert!(next(&stream).unwrap().is_none());
    assert_eq!(
        unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(6), None) }
            .unwrap_err()
            .code(),
        com::E_NOTIMPL
    );
    assert_eq!(
        unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(7), &stream) }
            .unwrap_err()
            .code(),
        com::E_INVALIDARG
    );
}

#[test]
fn provider_copy_stream_retains_snapshot_after_provider_release() {
    let drops = Arc::new(AtomicUsize::new(0));
    let provider = snapshot_provider(3, vec![DropProbe(drops.clone()).into()]).unwrap();
    let stream = unsafe {
        provider
            .GetCopyState(FABRIC_SEQUENCE_NUMBER(3), None)
            .unwrap()
    };
    drop(provider);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let operation = next(&stream).unwrap().unwrap();
    drop(stream);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(operation);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn provider_rejects_null_epoch_and_foreign_context_without_consuming_result() {
    let provider = snapshot_provider(0, Vec::new()).unwrap();
    let mut raw = provider.as_raw();
    let status = unsafe {
        (provider.vtable().BeginUpdateEpoch)(
            provider.as_raw(),
            ptr::null(),
            FABRIC_SEQUENCE_NUMBER(0),
            ptr::null_mut(),
            &mut raw,
        )
    };
    assert_eq!(status, com::E_POINTER);
    assert!(raw.is_null());

    let other = snapshot_provider(0, Vec::new()).unwrap();
    let foreign = unsafe { other.BeginOnDataLoss(None).unwrap() };
    let context = unsafe { provider.BeginOnDataLoss(None).unwrap() };
    for invalid in [None, Some(&foreign)] {
        assert_eq!(
            unsafe { provider.EndOnDataLoss(invalid) }
                .unwrap_err()
                .code(),
            com::E_INVALIDARG
        );
    }
    assert!(!unsafe { provider.EndOnDataLoss(&context).unwrap() });
    assert!(!unsafe { other.EndOnDataLoss(&foreign).unwrap() });
}

#[test]
fn provider_callbacks_reject_wrong_kind_context_and_unsupported_reconciliation() {
    let provider = snapshot_provider(7, Vec::new()).unwrap();
    let epoch = FABRIC_EPOCH {
        DataLossNumber: 1,
        ConfigurationNumber: 2,
        ..Default::default()
    };
    assert_eq!(
        unsafe { provider.BeginUpdateEpoch(&epoch, FABRIC_SEQUENCE_NUMBER(6), None) }
            .unwrap_err()
            .code(),
        com::E_NOTIMPL
    );
    let context = unsafe {
        provider
            .BeginUpdateEpoch(&epoch, FABRIC_SEQUENCE_NUMBER(7), None)
            .unwrap()
    };
    assert_eq!(
        unsafe { provider.EndOnDataLoss(&context) }
            .unwrap_err()
            .code(),
        com::E_INVALIDARG
    );
    assert_eq!(
        unsafe { provider.BeginOnDataLoss(None) }
            .unwrap_err()
            .code(),
        com::E_INVALIDARG
    );
    unsafe { provider.EndUpdateEpoch(&context).unwrap() };
    assert_eq!(
        unsafe { provider.EndUpdateEpoch(&context) }
            .ok()
            .unwrap_err()
            .code(),
        com::E_INVALIDARG
    );
    let stale = FABRIC_EPOCH {
        ConfigurationNumber: 1,
        ..epoch
    };
    assert_eq!(
        unsafe { provider.BeginUpdateEpoch(&stale, FABRIC_SEQUENCE_NUMBER(7), None) }
            .unwrap_err()
            .code(),
        com::E_INVALIDARG
    );
    let context = unsafe { provider.BeginOnDataLoss(None).unwrap() };
    assert!(!unsafe { provider.EndOnDataLoss(&context).unwrap() });
    assert_eq!(
        unsafe { provider.GetLastCommittedSequenceNumber().unwrap() }.0,
        7
    );
}

#[implement(NullableOperationStream)]
struct OperationStreamFixture(HRESULT);

impl NullableOperationStream_Impl for OperationStreamFixture_Impl {
    unsafe fn BeginGetOperation(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        out: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let context = context::completed(callback)?;
            unsafe { com::return_interface(out, Some(context.clone()))? };
            context::notify(&context);
            Ok(())
        })
    }

    unsafe fn EndGetOperation(
        &self,
        _: Ref<IFabricAsyncOperationContext>,
        out: *mut Option<IFabricOperation>,
    ) -> Result<()> {
        unsafe { com::clear_out(out)? };
        self.0.ok()
    }
}

#[test]
fn native_operation_receive_adapter_preserves_null_eos_and_hresult() {
    let stream: NullableOperationStream = OperationStreamFixture(com::S_OK).into();
    let stream = stream.cast::<IFabricOperationStream>().unwrap();
    let context = unsafe { stream.BeginGetOperation(None).unwrap() };
    assert!(end_get_operation(&stream, &context).unwrap().is_none());
    let stream: NullableOperationStream = OperationStreamFixture(E_FAIL).into();
    let stream = stream.cast::<IFabricOperationStream>().unwrap();
    let context = unsafe { stream.BeginGetOperation(None).unwrap() };
    assert_eq!(
        end_get_operation(&stream, &context).unwrap_err().code(),
        E_FAIL
    );
}
