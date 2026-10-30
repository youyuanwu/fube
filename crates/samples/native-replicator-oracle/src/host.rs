// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Activated test host. HTTP is observation/client traffic, never replication.
use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

use fube_data::extensions::EpochDomains;
use fube_data::{Cancellation, Mutation, OperationData};
use mssf_com::{
    FabricCommon::{
        IFabricAsyncOperationCallback, IFabricAsyncOperationContext, IFabricStringResult,
    },
    FabricRuntime::{
        IFabricOperationStream, IFabricReplicator, IFabricStateReplicator2,
        IFabricStatefulServiceFactory, IFabricStatefulServiceFactory_Impl,
        IFabricStatefulServicePartition, IFabricStatefulServiceReplica,
        IFabricStatefulServiceReplica_Impl,
    },
    FabricTypes::*,
};
use mssf_core::{
    runtime::{create_com_runtime, node_context::NodeContext, CodePackageActivationContext},
    strings::StringResult,
    sync::AwaitableCallback,
    WString,
};
use mssf_util::tokio::TokioExecutor;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};
use uuid::Uuid;
use windows_core::{implement, Interface, Ref, Result, GUID, PCWSTR};

#[path = "../../rust-runtime-app/src/copy_ack_hold.rs"]
mod copy_ack_hold;
use copy_ack_hold::CopyAckHold;

use crate::{
    async_context::{guard_future, ReentrantAbort, Slot, Workers, E_ABORT},
    com, create_native_replicator,
    data::{operation_data, read_operation},
    live::{application_error, Application},
    stream::end_get_operation,
    NativeReplicator,
};

const TYPE: &str = "RustStatefulRuntimeType";
const LIMIT: usize = 4096;
const WAIT: Duration = Duration::from_secs(5);

fn role_name(role: FABRIC_REPLICA_ROLE) -> &'static str {
    match role {
        FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_NONE => "None",
        FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY => "Primary",
        FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_IDLE_SECONDARY => "IdleSecondary",
        FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_ACTIVE_SECONDARY => "ActiveSecondary",
        _ => "Unknown",
    }
}
fn access_name(status: FABRIC_SERVICE_PARTITION_ACCESS_STATUS) -> &'static str {
    match status {
        FABRIC_SERVICE_PARTITION_ACCESS_STATUS::FABRIC_SERVICE_PARTITION_ACCESS_STATUS_GRANTED => "Granted",
        FABRIC_SERVICE_PARTITION_ACCESS_STATUS::FABRIC_SERVICE_PARTITION_ACCESS_STATUS_NOT_PRIMARY => "NotPrimary",
        FABRIC_SERVICE_PARTITION_ACCESS_STATUS::FABRIC_SERVICE_PARTITION_ACCESS_STATUS_RECONFIGURATION_PENDING => "ReconfigurationPending",
        FABRIC_SERVICE_PARTITION_ACCESS_STATUS::FABRIC_SERVICE_PARTITION_ACCESS_STATUS_NO_WRITE_QUORUM => "NoWriteQuorum",
        _ => "Invalid",
    }
}

struct Native {
    partition: IFabricStatefulServicePartition,
    replicator: NativeReplicator,
}
struct Pump {
    stop: Cancellation,
    join: JoinHandle<()>,
}
struct Completion {
    sequence: i64,
    result: Mutex<Option<std::result::Result<i64, String>>>,
    changed: Notify,
    returned_committed: AtomicBool,
}
struct Route {
    app: Arc<Application>,
    native: Mutex<Option<Native>>,
    role: Mutex<FABRIC_REPLICA_ROLE>,
    partition: String,
    replica: i64,
    incarnation: Uuid,
    service_address: String,
    node: String,
    host: String,
    bind_address: String,
    workers: Arc<Workers>,
    pump: tokio::sync::Mutex<Option<Pump>>,
    delay: AtomicU64,
    copy_ack_hold: CopyAckHold,
    copy_done: AtomicBool,
    copy_stream_items: AtomicU64,
    copy_stream_eos: AtomicU64,
    replication_stream_items: AtomicU64,
    replication_stream_eos: AtomicU64,
    acknowledged_sequence: [AtomicI64; 2],
    acknowledgment_calls: [AtomicU64; 2],
    received: AtomicI64,
    assigned: AtomicI64,
    completions: Mutex<BTreeMap<String, Arc<Completion>>>,
    admission: Arc<tokio::sync::Semaphore>,
    control: tokio::sync::Mutex<()>,
    active: Mutex<usize>,
    inactive: Notify,
    fault: Mutex<Option<String>>,
    stopped: AtomicBool,
}

impl Route {
    fn native_state(&self) -> Result<mssf_com::FabricRuntime::IFabricStateReplicator> {
        self.native
            .lock()
            .map_err(|_| com::E_UNEXPECTED)?
            .as_ref()
            .map(|n| n.replicator.state.clone())
            .ok_or_else(|| E_ABORT.into())
    }

    fn access(
        &self,
    ) -> Result<(
        FABRIC_SERVICE_PARTITION_ACCESS_STATUS,
        FABRIC_SERVICE_PARTITION_ACCESS_STATUS,
    )> {
        let native = self.native.lock().map_err(|_| com::E_UNEXPECTED)?;
        let native = native.as_ref().ok_or(E_ABORT)?;
        unsafe {
            Ok((
                native.partition.GetReadStatus()?,
                native.partition.GetWriteStatus()?,
            ))
        }
    }

    fn authority(&self, write: bool) -> Result<()> {
        if self.stopped.load(Ordering::Acquire)
            || self.workers.stop.is_cancelled()
            || self.fault.lock().map_err(|_| com::E_UNEXPECTED)?.is_some()
            || *self.role.lock().map_err(|_| com::E_UNEXPECTED)?
                != FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY
            || self.app.provider.snapshot().copy_in_progress
            || !self.copy_done.load(Ordering::Acquire)
        {
            return Err(E_ABORT.into());
        }
        let access = self.access()?;
        if (if write { access.1 } else { access.0 })
            != FABRIC_SERVICE_PARTITION_ACCESS_STATUS::FABRIC_SERVICE_PARTITION_ACCESS_STATUS_GRANTED
        {
            return Err(E_ABORT.into());
        }
        Ok(())
    }

    fn fail(&self, error: impl ToString) {
        if let Ok(mut fault) = self.fault.lock() {
            *fault = Some(error.to_string());
        }
        // A failed state-provider apply cannot masquerade as a successful build.
        if let Ok(native) = self.native.lock() {
            if let Some(native) = native.as_ref() {
                let _ = unsafe {
                    native
                        .partition
                        .ReportFault(FABRIC_FAULT_TYPE::FABRIC_FAULT_TYPE_TRANSIENT)
                };
            }
        }
    }

    async fn stop_pump(&self) -> Result<()> {
        if let Some(pump) = self.pump.lock().await.take() {
            pump.stop.cancel();
            pump.join.await.map_err(application_error)?;
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let _control = self.control.lock().await;
        {
            let _active = self.active.lock().map_err(|_| com::E_UNEXPECTED)?;
            self.stopped.store(true, Ordering::Release);
        }
        self.workers.stop.cancel();
        let pump_result = self.stop_pump().await;
        loop {
            let changed = self.inactive.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if *self.active.lock().map_err(|_| com::E_UNEXPECTED)? == 0 {
                break;
            }
            changed.await;
        }
        let workers_result = self.workers.join().await;
        self.app.clear();
        // Native owns its callbacks; release our captures only after draining.
        self.completions
            .lock()
            .map_err(|_| com::E_UNEXPECTED)?
            .clear();
        self.native.lock().map_err(|_| com::E_UNEXPECTED)?.take();
        pump_result.and(workers_result)
    }

    async fn change_role(
        self: &Arc<Self>,
        role: FABRIC_REPLICA_ROLE,
    ) -> Result<IFabricStringResult> {
        let _control = self.control.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return Err(E_ABORT.into());
        }
        let old = *self.role.lock().map_err(|_| com::E_UNEXPECTED)?;
        {
            let _gate = self.app.gate.lock().await;
            *self.role.lock().map_err(|_| com::E_UNEXPECTED)? =
                FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_NONE;
        }
        if role == FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY {
            self.stop_pump().await?;
            if self.app.provider.snapshot().copy_in_progress
                || (matches!(
                    old,
                    FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_IDLE_SECONDARY
                        | FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_ACTIVE_SECONDARY
                ) && !self.copy_done.load(Ordering::Acquire))
            {
                self.fail("promotion during incomplete native copy");
                return Err(com::E_UNEXPECTED.into());
            }
            self.copy_done.store(true, Ordering::Release);
        } else if role == FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_IDLE_SECONDARY
            || role == FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_ACTIVE_SECONDARY
        {
            let mut pump = self.pump.lock().await;
            if pump.is_none() {
                let native = self.native_state()?;
                let copy = role == FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_IDLE_SECONDARY;
                let copy_stream = if copy {
                    Some(unsafe { native.GetCopyStream()? })
                } else {
                    None
                };
                let replication = unsafe { native.GetReplicationStream()? };
                let route = self.clone();
                let stop = Cancellation::default();
                let token = stop.clone();
                let join = self.workers.rt.spawn(async move {
                    let result = guard_future(async {
                        if let Some(stream) = copy_stream {
                            route.consume(stream, true, token.clone()).await?;
                            if route.app.provider.snapshot().copy_in_progress {
                                return Err(application_error(
                                    "native copy ended before provider CopyEnd",
                                ));
                            }
                            route.copy_done.store(true, Ordering::Release);
                        }
                        route.consume(replication, false, token.clone()).await
                    })
                    .await;
                    if let Err(error) = result {
                        if !token.is_cancelled() && !route.workers.stop.is_cancelled() {
                            route.fail(error);
                        }
                    }
                });
                *pump = Some(Pump { stop, join });
            }
        } else if role == FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_NONE {
            self.stop_pump().await?;
        } else {
            return Err(com::E_INVALIDARG.into());
        }
        *self.role.lock().map_err(|_| com::E_UNEXPECTED)? = role;
        Ok(StringResult::new(WString::from(self.service_address.as_str())).into())
    }

    async fn consume(
        &self,
        stream: IFabricOperationStream,
        copy: bool,
        stop: Cancellation,
    ) -> Result<()> {
        loop {
            let begin = stream.clone();
            let end = stream.clone();
            let native_cancel = Cancellation::default();
            let rx = native_begin_end(
                move |callback| unsafe { begin.BeginGetOperation(callback) },
                move |context| end_get_operation(&end, context.ok_or(com::E_POINTER)?),
            );
            let operation = native_wait(rx, &stop, &self.workers.stop, native_cancel).await?;
            let Some(operation) = operation else {
                self.observe_stream_end(copy);
                return Ok(());
            };
            if stop.is_cancelled() || self.workers.stop.is_cancelled() {
                return Err(E_ABORT.into());
            }
            let metadata = unsafe { operation.get_Metadata().as_ref() }.ok_or(com::E_POINTER)?;
            let sequence = metadata.SequenceNumber.0;
            let segments = unsafe { read_operation(&operation)? };
            if metadata.Type == FABRIC_OPERATION_TYPE::FABRIC_OPERATION_TYPE_END_OF_STREAM {
                if segments.iter().any(|s| !s.is_empty())
                    || self.app.provider.snapshot().copy_in_progress
                {
                    return Err(application_error("invalid native end-of-stream operation"));
                }
                unsafe { operation.Acknowledge().ok()? };
                self.observe_stream_end(copy);
                return Ok(());
            }
            if metadata.Type != FABRIC_OPERATION_TYPE::FABRIC_OPERATION_TYPE_NORMAL {
                return Err(application_error("unsupported native operation type"));
            }
            let data = OperationData::new(segments).map_err(application_error)?;
            (if copy {
                &self.copy_stream_items
            } else {
                &self.replication_stream_items
            })
            .fetch_add(1, Ordering::Relaxed);
            if !copy {
                self.received.fetch_max(sequence, Ordering::AcqRel);
            }
            {
                let _gate = self.app.gate.lock().await;
                if copy {
                    self.app
                        .provider
                        .apply_copy(&data)
                        .map_err(application_error)?;
                    self.received
                        .fetch_max(self.app.provider.snapshot().last, Ordering::AcqRel);
                } else {
                    self.app
                        .provider
                        .apply_replication(sequence, &data)
                        .map_err(application_error)?;
                }
            }
            let held = if copy
                && !self.app.provider.snapshot().copy_in_progress
                && self.app.provider.snapshot().last > 0
            {
                self.copy_ack_hold
                    .wait(
                        sequence,
                        self.app.provider.snapshot().last,
                        &stop,
                        &self.workers.stop,
                    )
                    .await
                    .map_err(|error| {
                        if stop.is_cancelled() || self.workers.stop.is_cancelled() {
                            E_ABORT.into()
                        } else {
                            application_error(error)
                        }
                    })?
            } else {
                false
            };
            // Hold the explicit ACK after application, matching the Rust pump.
            // This exposes applied-but-not-acknowledged progress to qualification.
            let delay = self.delay.load(Ordering::Acquire);
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(E_ABORT.into()),
                _ = self.workers.stop.cancelled() => return Err(E_ABORT.into()),
                _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
            }
            if stop.is_cancelled() || self.workers.stop.is_cancelled() {
                return Err(E_ABORT.into());
            }
            // Receipt, empty data, local progress and EOS are never implicit ACKs.
            unsafe { operation.Acknowledge().ok()? };
            if held {
                self.copy_ack_hold.acknowledged();
            }
            let index = usize::from(!copy);
            self.acknowledged_sequence[index].store(sequence, Ordering::Release);
            self.acknowledgment_calls[index].fetch_add(1, Ordering::AcqRel);
        }
    }

    fn observe_stream_end(&self, copy: bool) {
        (if copy {
            &self.copy_stream_eos
        } else {
            &self.replication_stream_eos
        })
        .fetch_add(1, Ordering::Relaxed);
    }

    async fn write(self: &Arc<Self>, mutation: Mutation) -> (u16, Value) {
        let rejected = |error: String| {
            (
                503,
                json!({
                    "outcome":"rejected","request_id":mutation.id,"duplicate":false,"error":error
                }),
            )
        };
        let completion;
        let duplicate;
        {
            let _gate = self.app.gate.lock().await;
            if let Err(error) = self.authority(true) {
                return rejected(error.to_string());
            }
            // Only the real provider UpdateEpoch callback publishes this value.
            // The application gate also serializes that callback with BeginReplicate.
            let epoch = match self.app.native_epoch.lock() {
                Ok(epoch) => match *epoch {
                    Some(epoch) => epoch.primary(),
                    None => {
                        return rejected("native primary epoch unavailable before admission".into())
                    }
                },
                Err(error) => return rejected(error.to_string()),
            };
            match self.app.provider.admission(&mutation) {
                Err(error) => return rejected(error.to_string()),
                Ok(Some(sequence)) => {
                    duplicate = true;
                    completion = match self
                        .completions
                        .lock()
                        .ok()
                        .and_then(|c| c.get(&mutation.id).cloned())
                    {
                        Some(completion) if completion.sequence == sequence => completion,
                        _ => {
                            return (
                                202,
                                json!({"outcome":"ambiguous","request_id":mutation.id,"sequence":sequence,"duplicate":true}),
                            )
                        }
                    };
                }
                Ok(None) => {
                    duplicate = false;
                    let permit = match self.admission.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(error) => return rejected(error.to_string()),
                    };
                    let state = self.app.provider.snapshot();
                    if let Ok(mut completions) = self.completions.lock() {
                        completions.retain(|id, c| state.results.get(id) == Some(&c.sequence));
                    }
                    let native = match self.native_state() {
                        Ok(v) => v,
                        Err(e) => return rejected(e.to_string()),
                    };
                    let payload = match mutation.operation_for_epoch(epoch) {
                        Ok(v) => v,
                        Err(e) => return rejected(e.to_string()),
                    };
                    let data = match operation_data(payload.to_segments()) {
                        Ok(v) => v,
                        Err(e) => return rejected(e.to_string()),
                    };
                    let mut sequence = FABRIC_SEQUENCE_NUMBER(0);
                    let mut begun = false;
                    let end = native.clone();
                    let cancel = Cancellation::default();
                    let mut rx = native_begin_end(
                        |callback| {
                            let context =
                                unsafe { native.BeginReplicate(&data, callback, &mut sequence) };
                            begun = context.is_ok();
                            context
                        },
                        move |context| unsafe { end.EndReplicate(context) }.map(|n| n.0),
                    );
                    // This is the synchronous assigned-LSN/local-apply seam. Never
                    // await EndReplicate before making this application visible.
                    if !begun || sequence.0 <= 0 {
                        let error = match rx.result().await {
                            Err(error) => error.to_string(),
                            Ok(_) => "native BeginReplicate returned no assigned LSN".to_owned(),
                        };
                        return rejected(error);
                    }
                    self.assigned.fetch_max(sequence.0, Ordering::AcqRel);
                    self.received.fetch_max(sequence.0, Ordering::AcqRel);
                    completion = Arc::new(Completion {
                        sequence: sequence.0,
                        result: Mutex::new(None),
                        changed: Notify::new(),
                        returned_committed: AtomicBool::new(false),
                    });
                    if let Err(error) = self.app.provider.apply_replication(sequence.0, &payload) {
                        self.fail(error);
                        // The native operation was accepted: this is ambiguous,
                        // never a fabricated rejection or a successful commit.
                        cancel.cancel();
                    }
                    match self.completions.lock() {
                        Ok(mut entries) => {
                            entries.insert(mutation.id.clone(), completion.clone());
                        }
                        Err(error) => self.fail(error),
                    }
                    let route = self.clone();
                    let result = completion.clone();
                    if let Err(error) = self.workers.spawn(async move {
                        let _permit = permit;
                        let outcome =
                            native_wait(rx, &route.workers.stop, &route.workers.stop, cancel).await;
                        let outcome = outcome.and_then(|sequence| {
                            if sequence == result.sequence {
                                Ok(sequence)
                            } else {
                                Err(application_error("native EndReplicate LSN mismatch"))
                            }
                        });
                        if let Ok(mut value) = result.result.lock() {
                            *value = Some(outcome.map_err(|e| e.to_string()));
                        }
                        result.changed.notify_waiters();
                    }) {
                        self.fail(error);
                    }
                }
            }
        }
        let _ = tokio::time::timeout(WAIT, async {
            loop {
                let notified = completion.changed.notified();
                if completion.result.lock().map_or(true, |r| r.is_some()) {
                    break;
                }
                notified.await;
            }
        })
        .await;
        let committed = self.authority(true).is_ok()
            && self.fault.lock().is_ok_and(|f| f.is_none())
            && completion
                .result
                .lock()
                .is_ok_and(|r| matches!(*r, Some(Ok(_))))
            && self
                .app
                .provider
                .admission(&mutation)
                .is_ok_and(|r| r == Some(completion.sequence));
        if committed {
            completion.returned_committed.store(true, Ordering::Release);
        }
        let mut body = json!({
            "outcome":if committed {"committed"} else {"ambiguous"},
            "request_id":mutation.id,"sequence":completion.sequence,"duplicate":duplicate
        });
        if let Ok(result) = completion.result.lock() {
            if let Some(Err(error)) = result.as_ref() {
                body["error"] = error.clone().into();
            }
        }
        (if committed { 200 } else { 202 }, body)
    }

    fn diagnostics(&self) -> Result<Value> {
        let (read, write) = self.access()?;
        let state = self.app.provider.snapshot();
        let role = role_name(*self.role.lock().map_err(|_| com::E_UNEXPECTED)?);
        let epoch = self
            .app
            .native_epoch
            .lock()
            .map_err(|_| com::E_UNEXPECTED)?
            .map(|epoch| json!({"data_loss_number":epoch.data_loss_number,"configuration_number":epoch.configuration_number}));
        let ready = self.authority(true).is_ok();
        let native = self.native.lock().map_err(|_| com::E_UNEXPECTED)?;
        let native = native.as_ref().ok_or(E_ABORT)?;
        let effective = (|| -> Result<Value> {
            let state: IFabricStateReplicator2 = native.replicator.state.cast()?;
            let result = unsafe { state.GetReplicatorSettings()? };
            let settings =
                unsafe { result.get_ReplicatorSettings().as_ref() }.ok_or(com::E_POINTER)?;
            Ok(json!({
                "flags": settings.Flags, "require_service_ack": settings.RequireServiceAck,
                "initial_replication_queue_size":settings.InitialReplicationQueueSize,
                "max_replication_queue_size":settings.MaxReplicationQueueSize,
                "initial_copy_queue_size":settings.InitialCopyQueueSize,
                "max_copy_queue_size":settings.MaxCopyQueueSize
            }))
        })();
        // Native SecondaryReplicator::GetCurrentProgress reports a transient
        // replica fault while copy is in progress. Diagnostics must not invoke
        // that state-changing error path merely to observe an InBuild replica.
        let native_progress = if self.copy_done.load(Ordering::Acquire)
            && !state.copy_in_progress
            && matches!(role, "Primary" | "ActiveSecondary")
        {
            unsafe { native.replicator.control.GetCurrentProgress() }
                .ok()
                .map(|n| n.0)
        } else {
            None
        };
        let fault = self.fault.lock().map_err(|_| com::E_UNEXPECTED)?.clone();
        let completions = self.completions.lock().map_err(|_| com::E_UNEXPECTED)?;
        let valid = completions
            .iter()
            .filter(|(id, c)| state.results.get(*id) == Some(&c.sequence));
        let committed = valid
            .clone()
            .filter(|(_, c)| c.result.lock().is_ok_and(|r| matches!(*r, Some(Ok(_)))))
            .map(|(_, c)| c.sequence)
            .max()
            .unwrap_or(0);
        let client_completed = valid
            .filter(|(_, c)| c.returned_committed.load(Ordering::Acquire))
            .map(|(_, c)| c.sequence)
            .max()
            .unwrap_or(0);
        let engine = json!({
            "role":role,"epoch":epoch,
            "progress":{"assigned":self.assigned.load(Ordering::Acquire),"received":self.received.load(Ordering::Acquire),
                "applied":state.last,"committed":committed,"client_completed":client_completed,"completed":committed,
                "first_retained":if state.last > 0 {1} else {0},
                "retained_operations":state.history_count,"retained_bytes":state.retained_bytes},
            "copy_done":self.copy_done.load(Ordering::Acquire),
            "ready":ready,"closed":self.stopped.load(Ordering::Acquire),
            "fault":fault,"provider_callbacks":self.app.callbacks.load(Ordering::Relaxed),
            "peers":null
        });
        Ok(json!({
            "profile":"fube-data-v1","engine_kind":"native-classic-v1",
            "partition":self.partition,"replica":self.replica,"incarnation":self.incarnation.to_string(),
            "engine_incarnation":self.incarnation.to_string(),"node":self.node,"host":self.host,"pid":std::process::id(),
            "service_address":self.service_address,"replication_address":null,
            "replication_address_source":"native control EndOpen is owned by RA; query native replica address",
            "replication_bind_address":self.bind_address,
            "native_epoch":epoch,"read_status":access_name(read),"write_status":access_name(write),
            "state":state,"role":role,"current_role":role,"replicator_role":null,
            "native_settings":effective.as_ref().ok(),
            "native_settings_error":effective.err().map(|e| e.to_string()),
            "requested_native_settings":{
                "require_service_ack":true,
                "require_service_ack_flag":FABRIC_REPLICATOR_SETTINGS_FLAGS::FABRIC_REPLICATOR_REQUIRE_SERVICE_ACK.0,
                "initial_replication_queue_size":64,"max_replication_queue_size":1024,
                "initial_copy_queue_size":64,"max_copy_queue_size":1024
            },
            "native_current_progress":native_progress,
            "native_provider_callback_calls":self.app.callback_calls.load(Ordering::Relaxed),
            "native_copy_horizons":self.app.copy_observations()?,
            "native_streams":{
                "copy_items":self.copy_stream_items.load(Ordering::Acquire),
                "copy_eos":self.copy_stream_eos.load(Ordering::Acquire),
                "replication_items":self.replication_stream_items.load(Ordering::Acquire),
                "replication_eos":self.replication_stream_eos.load(Ordering::Acquire),
                "copy_acknowledged_sequence":self.acknowledged_sequence[0].load(Ordering::Acquire),
                "replication_acknowledged_lsn":self.acknowledged_sequence[1].load(Ordering::Acquire),
                "copy_acknowledgment_calls":self.acknowledgment_calls[0].load(Ordering::Acquire),
                "replication_acknowledgment_calls":self.acknowledgment_calls[1].load(Ordering::Acquire),
                "ack_observation":"last successful application Acknowledge call per stream; not native quorum commitment",
                "replication_original_epochs":null,
                "epoch_observation":"published IFabricOperation metadata does not expose original replication epochs"
            },
            "native_library_boundary":"mssf-core 0.10 dynamic FabricRuntime/Common/Client loader; native engine",
            "commit_observation":"local successful native EndReplicate only; secondary quorum watermark is not publicly observed",
            "ack_delay_ms":self.delay.load(Ordering::Acquire),
            "copy_delay_ms":self.app.copy_delay.load(Ordering::Acquire),"pump_fault":fault,
            "copy_ack_hold":self.copy_ack_hold.snapshot(),
            "engine":engine
        }))
    }
}

struct NativeReceiver<T> {
    receiver: tokio::sync::oneshot::Receiver<Result<T>>,
    context: Option<IFabricAsyncOperationContext>,
}

impl<T> NativeReceiver<T> {
    async fn result(&mut self) -> Result<T> {
        let result = (&mut self.receiver).await.map_err(application_error)?;
        self.context.take();
        result
    }

    fn cancel(&mut self) -> Result<()> {
        if let Some(context) = self.context.take() {
            unsafe { context.Cancel() }.ok()?;
        }
        Ok(())
    }
}

impl<T> Drop for NativeReceiver<T> {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

/// Keep typed native HRESULTs, including Begin failures, without the core
/// proxy's FabricGetLastErrorMessage loader side effect on local COM doubles.
fn native_begin_end<T: Send + 'static>(
    begin: impl FnOnce(Option<&IFabricAsyncOperationCallback>) -> Result<IFabricAsyncOperationContext>,
    end: impl FnOnce(Option<&IFabricAsyncOperationContext>) -> Result<T> + Send + 'static,
) -> NativeReceiver<T> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let callback = AwaitableCallback::new_interface(move |context| {
        let _ = sender.send(end(context.as_ref()));
    });
    match begin(Some(&callback)) {
        Ok(context) => NativeReceiver {
            receiver,
            context: Some(context),
        },
        Err(error) => {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let _ = sender.send(Err(error));
            NativeReceiver {
                receiver,
                context: None,
            }
        }
    }
}

async fn native_wait<T: Send + 'static>(
    mut receiver: NativeReceiver<T>,
    stop: &Cancellation,
    owner_stop: &Cancellation,
    cancel: Cancellation,
) -> Result<T> {
    tokio::select! {
        biased;
        result = receiver.result() => result,
        _ = async { tokio::select! {
            _ = stop.cancelled() => {},
            _ = owner_stop.cancelled() => {},
            _ = cancel.cancelled() => {},
        } } => {
            receiver.cancel()?;
            tokio::time::timeout(WAIT, receiver.result()).await.map_err(application_error)?
        }
    }
}

struct Host {
    rt: tokio::runtime::Handle,
    node: String,
    ip: IpAddr,
    port: u16,
    host: String,
    routes: Mutex<HashMap<String, Weak<Route>>>,
    // Keep even reentrant callback task handles owned until an outside drain.
    worker_owners: Mutex<Vec<Arc<Workers>>>,
}

#[implement(IFabricStatefulServiceFactory)]
struct Factory(Arc<Host>);

impl IFabricStatefulServiceFactory_Impl for Factory_Impl {
    fn CreateReplica(
        &self,
        service_type: &PCWSTR,
        _service_name: &FABRIC_URI,
        length: u32,
        initialization: *const u8,
        partition: &GUID,
        replica: FABRIC_REPLICA_ID,
    ) -> Result<IFabricStatefulServiceReplica> {
        com::guard(|| {
            if service_type.is_null() || WString::from(*service_type).to_string_lossy() != TYPE {
                return Err(com::E_INVALIDARG.into());
            }
            if length != 0 {
                if initialization.is_null() || length > LIMIT as u32 {
                    return Err(com::E_INVALIDARG.into());
                }
                let data = unsafe { std::slice::from_raw_parts(initialization, length as usize) };
                if data != b"fube-data-v1" {
                    return Err(com::E_INVALIDARG.into());
                }
            }
            let incarnation = Uuid::new_v4();
            let partition = Uuid::from_u128(partition.to_u128()).to_string();
            let path = format!("/data/{partition}/{}/{incarnation}", replica.0);
            let workers = Workers::new(self.0.rt.clone());
            let route = Arc::new(Route {
                app: Application::new(workers.clone()),
                native: Mutex::new(None),
                role: Mutex::new(FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_NONE),
                partition,
                replica: replica.0,
                incarnation,
                service_address: format!(
                    "http://{}{path}/service",
                    SocketAddr::new(self.0.ip, self.0.port)
                ),
                node: self.0.node.clone(),
                host: self.0.host.clone(),
                bind_address: SocketAddr::new(self.0.ip, 0).to_string(),
                workers,
                pump: tokio::sync::Mutex::new(None),
                delay: AtomicU64::new(0),
                copy_ack_hold: CopyAckHold::default(),
                copy_done: AtomicBool::new(false),
                copy_stream_items: AtomicU64::new(0),
                copy_stream_eos: AtomicU64::new(0),
                replication_stream_items: AtomicU64::new(0),
                replication_stream_eos: AtomicU64::new(0),
                acknowledged_sequence: [AtomicI64::new(0), AtomicI64::new(0)],
                acknowledgment_calls: [AtomicU64::new(0), AtomicU64::new(0)],
                received: AtomicI64::new(0),
                assigned: AtomicI64::new(0),
                completions: Mutex::new(BTreeMap::new()),
                admission: Arc::new(tokio::sync::Semaphore::new(64)),
                control: tokio::sync::Mutex::new(()),
                active: Mutex::new(0),
                inactive: Notify::new(),
                fault: Mutex::new(None),
                stopped: AtomicBool::new(false),
            });
            let mut routes = self.0.routes.lock().map_err(|_| com::E_UNEXPECTED)?;
            routes.retain(|_, r| r.strong_count() != 0);
            if routes.len() >= 64 {
                return Err(com::E_INVALIDARG.into());
            }
            let lifecycle = Workers::new(self.0.rt.clone());
            let mut owners = self.0.worker_owners.lock().map_err(|_| com::E_UNEXPECTED)?;
            owners.retain(|workers| !workers.can_release());
            if owners.len() >= 128 {
                return Err(com::E_INVALIDARG.into());
            }
            owners.extend([route.workers.clone(), lifecycle.clone()]);
            routes.insert(path, Arc::downgrade(&route));
            Ok(Replica {
                route,
                lifecycle,
                open: Slot::default(),
                role: Slot::default(),
                close: Slot::default(),
            }
            .into())
        })
    }
}

#[implement(IFabricStatefulServiceReplica)]
struct Replica {
    route: Arc<Route>,
    lifecycle: Arc<Workers>,
    open: Slot<IFabricReplicator>,
    role: Slot<IFabricStringResult>,
    close: Slot<()>,
}

impl IFabricStatefulServiceReplica_Impl for Replica_Impl {
    fn BeginOpen(
        &self,
        _mode: FABRIC_REPLICA_OPEN_MODE,
        partition: Ref<IFabricStatefulServicePartition>,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
            let partition = partition.as_ref().ok_or(com::E_POINTER)?.clone();
            let route = self.route.clone();
            self.open
                .begin_cleanup(&self.lifecycle, callback, move |_| async move {
                    let _control = route.control.lock().await;
                    let mut native = route.native.lock().map_err(|_| com::E_UNEXPECTED)?;
                    if native.is_some() || route.stopped.load(Ordering::Acquire) {
                        return Err(com::E_INVALIDARG.into());
                    }
                    let replicator = create_native_replicator(
                        &partition,
                        &route.app.bridge(),
                        &route.bind_address,
                    )?;
                    let actual = replicator.control.clone();
                    *native = Some(Native {
                        partition,
                        replicator,
                    });
                    Ok(actual)
                })
        })
    }
    fn EndOpen(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<IFabricReplicator> {
        // Exactly the IFabricReplicator returned by native CreateReplicator.
        com::guard(|| self.open.end(context))
    }
    fn BeginChangeRole(
        &self,
        role: FABRIC_REPLICA_ROLE,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
            let route = self.route.clone();
            self.role
                .begin_cleanup(&self.lifecycle, callback, move |_| async move {
                    route.change_role(role).await
                })
        })
    }
    fn EndChangeRole(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
    ) -> Result<IFabricStringResult> {
        com::guard(|| self.role.end(context))
    }
    fn BeginClose(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
            let route = self.route.clone();
            self.close
                .begin_cleanup(&self.lifecycle, callback, move |_| async move {
                    route.shutdown().await
                })
        })
    }
    fn EndClose(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<()> {
        com::guard(|| {
            self.close.end_with(context, || {
                self.lifecycle.stop.cancel();
                self.open.clear();
                self.role.clear();
            })
        })
    }
    fn Abort(&self) {
        let _callback = ReentrantAbort::enter();
        let _ = com::guard(|| {
            self.lifecycle.stop.cancel();
            let executor = TokioExecutor::new(self.lifecycle.rt.clone());
            let result = executor.block_on_any(async {
                let route = self.route.shutdown().await;
                let lifecycle = self.lifecycle.join().await;
                route.and(lifecycle)
            });
            self.open.clear();
            self.role.clear();
            self.close.clear();
            result
        });
    }
}

enum Request {
    Read,
    Diagnostics,
    DataLoss(i64),
    Write(Mutation),
    Test { copy: bool, delay: u64 },
    CopyAckHold(u64),
}
fn decode(value: &str) -> std::result::Result<String, &'static str> {
    let mut output = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        output.push(match byte {
            b'+' => b' ',
            b'%' => {
                let digit = |byte: Option<u8>| {
                    byte.and_then(|b| (b as char).to_digit(16))
                        .ok_or("invalid percent encoding")
                };
                (digit(bytes.next())? * 16 + digit(bytes.next())?) as u8
            }
            byte => byte,
        });
    }
    String::from_utf8(output).map_err(|_| "invalid query UTF-8")
}
fn parse(operation: &str, query: &str) -> std::result::Result<Request, &'static str> {
    if query.len() > LIMIT {
        return Err("data query limit");
    }
    let mut parameters = HashMap::new();
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or("invalid query parameter")?;
            if parameters.insert(decode(key)?, decode(value)?).is_some() {
                return Err("duplicate query parameter");
            }
        }
    }
    match operation {
        "service" if parameters.is_empty() => Ok(Request::Read),
        "diagnostics" if parameters.is_empty() => Ok(Request::Diagnostics),
        "write" if parameters.len() == 3 => {
            let mut take = |name| parameters.remove(name).ok_or("missing write parameter");
            let mutation = Mutation {
                id: take("id")?,
                key: take("key")?,
                value: take("value")?,
            };
            mutation.operation().map_err(|_| "invalid mutation")?;
            Ok(Request::Write(mutation))
        }
        "test" if parameters.len() == 1 => {
            if let Some(value) = parameters.get("copy_ack_hold_ms") {
                let milliseconds = value
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n <= 6000)
                    .filter(|_| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                    .ok_or("copy ACK hold must be 0..6000 ms")?;
                return Ok(Request::CopyAckHold(milliseconds));
            }
            if let Some(value) = parameters.get("data_loss_boundary") {
                let boundary = value
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .filter(|_| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                    .ok_or("invalid data loss qualification boundary")?;
                return Ok(Request::DataLoss(boundary));
            }
            let copy = parameters.contains_key("copy_delay_ms");
            let name = if copy {
                "copy_delay_ms"
            } else {
                "ack_delay_ms"
            };
            let delay = parameters
                .get(name)
                .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse().ok())
                .filter(|n: &u64| *n <= if copy { 100 } else { 2000 })
                .ok_or("test requires exactly ack_delay_ms=0..2000 or copy_delay_ms=0..100")?;
            Ok(Request::Test { copy, delay })
        }
        _ => Err("unsupported data operation/parameters"),
    }
}

async fn response(stream: &mut TcpStream, status: u16, body: Value) -> Result<()> {
    let body = body.to_string();
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let head = format!("HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(application_error)?;
    stream
        .write_all(body.as_bytes())
        .await
        .map_err(application_error)?;
    stream.shutdown().await.map_err(application_error)
}

impl Host {
    async fn request(&self, mut stream: TcpStream) -> Result<()> {
        let mut buffer = [0u8; LIMIT + 1];
        let mut count = 0;
        loop {
            let read = stream
                .read(&mut buffer[count..])
                .await
                .map_err(application_error)?;
            count += read;
            if count > LIMIT {
                return response(&mut stream, 400, json!({"error":"request limit"})).await;
            }
            if buffer[..count].windows(4).any(|s| s == b"\r\n\r\n") {
                break;
            }
            if read == 0 {
                return Err(E_ABORT.into());
            }
        }
        let text = std::str::from_utf8(&buffer[..count]).map_err(application_error)?;
        let mut line = text.lines().next().unwrap_or("").split_whitespace();
        let method = line.next();
        let uri = line.next().unwrap_or("");
        if method != Some("GET")
            || !matches!(line.next(), Some("HTTP/1.1" | "HTTP/1.0"))
            || line.next().is_some()
        {
            return response(&mut stream, 400, json!({"error":"invalid GET request"})).await;
        }
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
        let (base, operation) = path.rsplit_once('/').unwrap_or(("", ""));
        let route = {
            let routes = self.routes.lock().map_err(|_| com::E_UNEXPECTED)?;
            routes.get(base).and_then(Weak::upgrade).or_else(|| {
                // Discovery only; writes and hook releases still require the
                // canonical incarnation-bearing route returned by diagnostics.
                (operation == "diagnostics")
                    .then(|| {
                        routes.values().filter_map(Weak::upgrade).find(|route| {
                            base == format!("/data/{}/{}", route.partition, route.replica)
                        })
                    })
                    .flatten()
            })
        };
        let Some(route) = route else {
            return response(&mut stream, 404, json!({})).await;
        };
        let active = {
            let mut active = route.active.lock().map_err(|_| com::E_UNEXPECTED)?;
            if route.stopped.load(Ordering::Acquire) {
                false
            } else {
                *active += 1;
                true
            }
        };
        if !active {
            return response(&mut stream, 503, json!({})).await;
        }
        struct Active(Arc<Route>);
        impl Drop for Active {
            fn drop(&mut self) {
                if let Ok(mut active) = self.0.active.lock() {
                    *active -= 1;
                }
                self.0.inactive.notify_waiters();
            }
        }
        let _active = Active(route.clone());
        let request =
            match parse(operation, query) {
                Ok(request) => request,
                Err(error) => return response(
                    &mut stream,
                    400,
                    json!({"outcome":"rejected","request_id":null,"duplicate":false,"error":error}),
                )
                .await,
            };
        match request {
            Request::CopyAckHold(milliseconds) => {
                route.access()?;
                if route.copy_ack_hold.set(milliseconds).is_err() {
                    return response(&mut stream, 400, json!({})).await;
                }
                response(
                    &mut stream,
                    200,
                    json!({"qualification_only":true,
                    "copy_ack_hold_ms":milliseconds}),
                )
                .await
            }
            Request::DataLoss(boundary) => {
                route.access()?;
                route
                    .app
                    .provider
                    .request_data_loss(boundary)
                    .map_err(application_error)?;
                response(
                    &mut stream,
                    200,
                    json!({"qualification_only":true,
                    "data_loss_boundary":boundary,"armed_only":true,
                    "note":"Only a later native OnDataLoss callback may change provider state."}),
                )
                .await
            }
            Request::Write(mutation) => {
                let (status, body) = route.write(mutation).await;
                response(&mut stream, status, body).await
            }
            Request::Test { copy, delay } => {
                if copy {
                    route.app.copy_delay.store(delay, Ordering::Release);
                } else {
                    route.delay.store(delay, Ordering::Release);
                }
                response(
                    &mut stream,
                    200,
                    json!({"profile":"fube-data-v1","qualification_only":true,
                    "ack_delay_ms":route.delay.load(Ordering::Acquire),
                    "copy_delay_ms":route.app.copy_delay.load(Ordering::Acquire)}),
                )
                .await
            }
            Request::Read | Request::Diagnostics => {
                let read = matches!(request, Request::Read);
                if read && route.authority(false).is_err() {
                    return response(&mut stream, 503, json!({})).await;
                }
                match route.diagnostics() {
                    Ok(body) if !read || route.authority(false).is_ok() => {
                        response(&mut stream, 200, body).await
                    }
                    _ => response(&mut stream, 503, json!({})).await,
                }
            }
        }
    }
}

fn validate_native_node_identity(
    node_name: &str,
    ip: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> std::result::Result<(), String> {
    // SF_NODE_* describes the inherited container, not a node in a five-node
    // onebox. Only native activation's Fabric_* fields bind this process.
    for (name, actual) in [("Fabric_NodeName", node_name), ("Fabric_NodeIPOrFQDN", ip)] {
        if let Some(expected) = lookup(name) {
            if expected != actual {
                return Err(format!(
                    "{name} {expected:?} differs from actual native NodeContext {actual:?}"
                ));
            }
        }
    }
    Ok(())
}

/// Run as an SF activated code package; errors are not replaced with a fake host.
pub async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let node = NodeContext::get(Duration::from_secs(10), None).await?;
    let node_name = node.node_name.to_string_lossy();
    let ip: IpAddr = node.ip_address_or_fqdn.to_string_lossy().parse()?;
    validate_native_node_identity(&node_name, &ip.to_string(), |name| std::env::var(name).ok())?;
    let activation = CodePackageActivationContext::create()?;
    let endpoint = activation.get_endpoint_resource(&WString::from("Rpc"))?;
    if !matches!(endpoint.protocol.to_string_lossy().as_str(), "http" | "tcp")
        || endpoint.port == 0
        || endpoint.port > u16::MAX as u32
    {
        return Err("activation Rpc endpoint must be HTTP/TCP with a nonzero port".into());
    }
    let host_id = std::env::var("Fabric_ApplicationHostId")?;
    Uuid::parse_str(&host_id)?;
    let host = Arc::new(Host {
        rt: tokio::runtime::Handle::current(),
        node: node_name,
        ip,
        port: endpoint.port as u16,
        host: host_id,
        routes: Mutex::new(HashMap::new()),
        worker_owners: Mutex::new(Vec::new()),
    });
    let listener = TcpListener::bind(SocketAddr::new(ip, host.port)).await?;
    let runtime = create_com_runtime()?;
    let factory: IFabricStatefulServiceFactory = Factory(host.clone()).into();
    unsafe {
        runtime
            .RegisterStatefulServiceFactory(WString::from(TYPE).as_pcwstr(), &factory)
            .ok()?
    };
    println!(
        "{}",
        json!({"profile":"fube-data-v1","engine_kind":"native-classic-v1","activated":true,"node":host.node,"pid":std::process::id(),"rpc_port":host.port,"native_interoperability_qualified":false})
    );
    let mut requests = JoinSet::new();
    let listen_result = loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => break result,
            Some(_) = requests.join_next(), if !requests.is_empty() => {},
            accepted = listener.accept(), if requests.len() < 64 => {
                let (stream, _) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(error) };
                let host = host.clone();
                requests.spawn(async move {
                    let _ = tokio::time::timeout(Duration::from_secs(12), host.request(stream)).await;
                });
            },
        }
    };
    drop(listener);
    let routes: Vec<_> = host
        .routes
        .lock()
        .map_err(|_| "route lock poisoned")?
        .values()
        .filter_map(Weak::upgrade)
        .collect();
    let mut cleanup_result = Ok(());
    for route in routes {
        if let Err(error) = route.shutdown().await {
            cleanup_result = Err(error);
        }
    }
    while requests.join_next().await.is_some() {}
    let owners = std::mem::take(
        &mut *host
            .worker_owners
            .lock()
            .map_err(|_| "worker owner lock poisoned")?,
    );
    for workers in owners {
        if let Err(error) = workers.join_fully().await {
            cleanup_result = Err(error);
        }
    }
    drop(factory);
    drop(runtime);
    listen_result?;
    cleanup_result?;
    Ok(())
}

#[cfg(test)]
#[path = "host_test_doubles.rs"]
mod test_doubles;

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
