// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

use super::*;
use mssf_com::{
    FabricCommon::IFabricAsyncOperationCallback_Impl,
    FabricRuntime::{
        IFabricOperationData, IFabricReplicator_Impl, IFabricStateProvider, IFabricStateReplicator,
        IFabricStateReplicator_Impl, IFabricStatefulServicePartition_Impl,
    },
};
use std::sync::atomic::AtomicUsize;
use windows_core::OutRef;

pub(super) struct EngineState {
    pub workers: Arc<Workers>,
    pending: Slot<i64>,
    pub release: Notify,
    pub begins: AtomicUsize,
    pub ends: AtomicUsize,
    pub fail_begin: AtomicBool,
    pub fail_end: AtomicBool,
    assigned: AtomicI64,
    pub creates: AtomicUsize,
    pub control_opens: AtomicUsize,
    pub progress_calls: AtomicUsize,
}

impl EngineState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            workers: Workers::new(tokio::runtime::Handle::current()),
            pending: Slot::default(),
            release: Notify::new(),
            begins: AtomicUsize::new(0),
            ends: AtomicUsize::new(0),
            fail_begin: AtomicBool::new(false),
            fail_end: AtomicBool::new(false),
            assigned: AtomicI64::new(0),
            creates: AtomicUsize::new(0),
            control_opens: AtomicUsize::new(0),
            progress_calls: AtomicUsize::new(0),
        })
    }

    pub fn partition(self: &Arc<Self>) -> (IFabricStatefulServicePartition, IFabricReplicator) {
        let control: IFabricReplicator = EngineDouble(self.clone()).into();
        (
            PartitionDouble {
                state: self.clone(),
                control: control.clone(),
            }
            .into(),
            control,
        )
    }
}

#[implement(IFabricReplicator, IFabricStateReplicator)]
struct EngineDouble(Arc<EngineState>);

impl IFabricStateReplicator_Impl for EngineDouble_Impl {
    fn BeginReplicate(
        &self,
        data: Ref<IFabricOperationData>,
        callback: Ref<IFabricAsyncOperationCallback>,
        sequence: *mut FABRIC_SEQUENCE_NUMBER,
    ) -> Result<IFabricAsyncOperationContext> {
        self.0.begins.fetch_add(1, Ordering::SeqCst);
        let sequence = unsafe { sequence.as_mut() }.ok_or(com::E_POINTER)?;
        // Failed native out parameters must never be treated as acceptance.
        if self.0.fail_begin.load(Ordering::Acquire) {
            sequence.0 = 77;
            return Err(com::E_INVALIDARG.into());
        }
        let data = data.as_ref().ok_or(com::E_POINTER)?;
        unsafe { crate::data::read_segments(data)? };
        sequence.0 = self.0.assigned.fetch_add(1, Ordering::AcqRel) + 1;
        let assigned = sequence.0;
        let state = self.0.clone();
        self.0
            .pending
            .begin(&self.0.workers, callback, move |_| async move {
                state.release.notified().await;
                if state.fail_end.load(Ordering::Acquire) {
                    Err(com::E_UNEXPECTED.into())
                } else {
                    Ok(assigned)
                }
            })
    }

    fn EndReplicate(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
    ) -> Result<FABRIC_SEQUENCE_NUMBER> {
        self.0.ends.fetch_add(1, Ordering::SeqCst);
        self.0.pending.end(context).map(FABRIC_SEQUENCE_NUMBER)
    }
    fn GetReplicationStream(&self) -> Result<IFabricOperationStream> {
        Err(com::E_NOTIMPL.into())
    }
    fn GetCopyStream(&self) -> Result<IFabricOperationStream> {
        Err(com::E_NOTIMPL.into())
    }
    fn UpdateReplicatorSettings(&self, _: *const FABRIC_REPLICATOR_SETTINGS) -> Result<()> {
        Err(com::E_NOTIMPL.into())
    }
}

impl IFabricReplicator_Impl for EngineDouble_Impl {
    fn BeginOpen(
        &self,
        _: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        self.0.control_opens.fetch_add(1, Ordering::SeqCst);
        Err(com::E_NOTIMPL.into())
    }
    fn EndOpen(&self, _: Ref<IFabricAsyncOperationContext>) -> Result<IFabricStringResult> {
        Err(com::E_NOTIMPL.into())
    }
    fn BeginChangeRole(
        &self,
        _: *const FABRIC_EPOCH,
        _: FABRIC_REPLICA_ROLE,
        _: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        Err(com::E_NOTIMPL.into())
    }
    fn EndChangeRole(&self, _: Ref<IFabricAsyncOperationContext>) -> Result<()> {
        Err(com::E_NOTIMPL.into())
    }
    fn BeginUpdateEpoch(
        &self,
        _: *const FABRIC_EPOCH,
        _: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        Err(com::E_NOTIMPL.into())
    }
    fn EndUpdateEpoch(&self, _: Ref<IFabricAsyncOperationContext>) -> Result<()> {
        Err(com::E_NOTIMPL.into())
    }
    fn BeginClose(
        &self,
        _: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        Err(com::E_NOTIMPL.into())
    }
    fn EndClose(&self, _: Ref<IFabricAsyncOperationContext>) -> Result<()> {
        Err(com::E_NOTIMPL.into())
    }
    fn Abort(&self) {}
    fn GetCurrentProgress(&self) -> Result<FABRIC_SEQUENCE_NUMBER> {
        self.0.progress_calls.fetch_add(1, Ordering::AcqRel);
        Ok(FABRIC_SEQUENCE_NUMBER(
            self.0.assigned.load(Ordering::Acquire),
        ))
    }
    fn GetCatchUpCapability(&self) -> Result<FABRIC_SEQUENCE_NUMBER> {
        self.GetCurrentProgress()
    }
}

#[implement(IFabricStatefulServicePartition)]
struct PartitionDouble {
    state: Arc<EngineState>,
    control: IFabricReplicator,
}

impl IFabricStatefulServicePartition_Impl for PartitionDouble_Impl {
    fn GetPartitionInfo(&self) -> Result<*mut FABRIC_SERVICE_PARTITION_INFORMATION> {
        Err(com::E_NOTIMPL.into())
    }
    fn GetReadStatus(&self) -> Result<FABRIC_SERVICE_PARTITION_ACCESS_STATUS> {
        Ok(FABRIC_SERVICE_PARTITION_ACCESS_STATUS::FABRIC_SERVICE_PARTITION_ACCESS_STATUS_GRANTED)
    }
    fn GetWriteStatus(&self) -> Result<FABRIC_SERVICE_PARTITION_ACCESS_STATUS> {
        self.GetReadStatus()
    }
    fn CreateReplicator(
        &self,
        provider: Ref<IFabricStateProvider>,
        settings: *const FABRIC_REPLICATOR_SETTINGS,
        control: OutRef<IFabricReplicator>,
    ) -> Result<IFabricStateReplicator> {
        let settings = unsafe { settings.as_ref() }.ok_or(com::E_POINTER)?;
        if provider.as_ref().is_none()
            || !settings.RequireServiceAck
            || settings.Flags
                & FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_REQUIRE_SERVICE_ACK.0 as u32
                == 0
        {
            return Err(com::E_INVALIDARG.into());
        }
        self.state.creates.fetch_add(1, Ordering::SeqCst);
        control.write(Some(self.control.clone()))?;
        self.control.cast()
    }
    fn ReportLoad(&self, _: u32, _: *const FABRIC_LOAD_METRIC) -> Result<()> {
        Ok(())
    }
    fn ReportFault(&self, _: FABRIC_FAULT_TYPE) -> Result<()> {
        Ok(())
    }
}

type Action = Box<dyn FnOnce(IFabricAsyncOperationContext) + Send>;

#[implement(IFabricAsyncOperationCallback)]
struct Callback {
    action: Mutex<Option<Action>>,
    drops: Arc<AtomicUsize>,
}

impl Drop for Callback {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl IFabricAsyncOperationCallback_Impl for Callback_Impl {
    fn Invoke(&self, context: Ref<IFabricAsyncOperationContext>) {
        if let (Some(action), Some(context)) =
            (self.action.lock().unwrap().take(), context.as_ref())
        {
            action(context.clone());
        }
    }
}

pub(super) fn callback(
    drops: Arc<AtomicUsize>,
    action: impl FnOnce(IFabricAsyncOperationContext) + Send + 'static,
) -> IFabricAsyncOperationCallback {
    Callback {
        action: Mutex::new(Some(Box::new(action))),
        drops,
    }
    .into()
}
