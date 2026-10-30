// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Application-state bridge only: the native node constructs and owns the engine.
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use fube_data::{Cancellation, Epoch, IOperationDataStream, IStateProvider, MemoryProvider};
use mssf_com::{
    FabricCommon::{IFabricAsyncOperationCallback, IFabricAsyncOperationContext},
    FabricRuntime::{
        IFabricOperationData, IFabricOperationDataStream, IFabricStateProvider,
        IFabricStateProvider_Impl,
    },
    FabricTypes::{FABRIC_EPOCH, FABRIC_SEQUENCE_NUMBER},
};
use serde_json::{json, Value};
use windows_core::{implement, Ref, Result};

use crate::{
    abi::{NullableDataStream, NullableDataStream_Impl},
    async_context::{Slot, Workers},
    com,
    data::operation_data,
};

pub(crate) fn application_error(error: impl std::fmt::Display) -> windows_core::Error {
    windows_core::Error::new(com::E_UNEXPECTED, error.to_string())
}

pub(crate) struct Application {
    pub(crate) provider: Arc<MemoryProvider>,
    pub(crate) gate: tokio::sync::Mutex<()>,
    pub(crate) workers: Arc<Workers>,
    pub(crate) callbacks: AtomicUsize,
    pub(crate) callback_calls: AtomicUsize,
    pub(crate) native_epoch: Mutex<Option<Epoch>>,
    pub(crate) copy_delay: AtomicU64,
    copy_horizons: Mutex<VecDeque<Arc<CopyObservation>>>,
    epoch: Slot<()>,
    loss: Slot<bool>,
}

impl Application {
    pub(crate) fn new(workers: Arc<Workers>) -> Arc<Self> {
        Arc::new(Self {
            provider: MemoryProvider::new(),
            gate: tokio::sync::Mutex::new(()),
            workers,
            callbacks: AtomicUsize::new(0),
            callback_calls: AtomicUsize::new(0),
            native_epoch: Mutex::new(None),
            copy_delay: AtomicU64::new(0),
            copy_horizons: Mutex::new(VecDeque::new()),
            epoch: Slot::default(),
            loss: Slot::default(),
        })
    }

    pub(crate) fn bridge(self: &Arc<Self>) -> IFabricStateProvider {
        Provider(self.clone()).into()
    }

    pub(crate) fn clear(&self) {
        self.epoch.clear();
        self.loss.clear();
    }

    pub(crate) fn copy_observations(&self) -> Result<Value> {
        let horizons = self.copy_horizons.lock().map_err(|_| com::E_UNEXPECTED)?;
        horizons
            .iter()
            .map(|observation| {
                let mut value = observation
                    .value
                    .lock()
                    .map_err(|_| com::E_UNEXPECTED)?
                    .clone();
                value["callback_count"] = observation.callbacks.load(Ordering::Acquire).into();
                value["end_count"] = observation.ends.load(Ordering::Acquire).into();
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::from)
    }

    fn observe_copy(&self, horizon: i64) -> Result<Arc<CopyObservation>> {
        let observation = Arc::new(CopyObservation {
            value: Mutex::new(json!({
                "requested_horizon": horizon,
                "provider_progress_at_request": self.provider.get_last_committed_sequence_number().map_err(application_error)?,
                "build_context": null,
                "provider_progress_at_end": null,
                "enumeration_end": false,
                "items": 0,
                "begin_count": 0,
                "copy_context": "native_volatile_null"
            })),
            callbacks: Arc::default(),
            ends: AtomicUsize::new(0),
        });
        let mut horizons = self.copy_horizons.lock().map_err(|_| com::E_UNEXPECTED)?;
        if horizons.len() == 32 {
            horizons.pop_front();
        }
        horizons.push_back(observation.clone());
        Ok(observation)
    }

    async fn horizon(&self, horizon: i64, cancel: Cancellation) -> Result<()> {
        if horizon < 0 {
            return Err(com::E_INVALIDARG.into());
        }
        // BeginReplicate publishes an LSN before primary local application. Native
        // copy requests at that LSN must wait, not lower the requested horizon.
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if self.provider.get_last_committed_sequence_number().map_err(application_error)? >= horizon {
                    return Ok(());
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Err(crate::async_context::E_ABORT.into()),
                    _ = self.workers.stop.cancelled() => return Err(crate::async_context::E_ABORT.into()),
                    _ = tokio::time::sleep(Duration::from_millis(2)) => {}
                }
            }
        }).await.map_err(application_error)?
    }
}

#[implement(IFabricStateProvider)]
struct Provider(Arc<Application>);

struct Active<'a>(&'a AtomicUsize);
impl<'a> Active<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count)
    }
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl IFabricStateProvider_Impl for Provider_Impl {
    fn GetLastCommittedSequenceNumber(&self) -> Result<FABRIC_SEQUENCE_NUMBER> {
        com::guard(|| {
            self.0
                .provider
                .get_last_committed_sequence_number()
                .map(FABRIC_SEQUENCE_NUMBER)
                .map_err(application_error)
        })
    }

    fn BeginUpdateEpoch(
        &self,
        epoch: *const FABRIC_EPOCH,
        previous: FABRIC_SEQUENCE_NUMBER,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
            let epoch = unsafe { epoch.as_ref() }.ok_or(com::E_POINTER)?;
            if epoch.DataLossNumber < 0 || epoch.ConfigurationNumber < 0 || previous.0 < -1 {
                return Err(com::E_INVALIDARG.into());
            }
            let epoch = Epoch {
                data_loss_number: epoch.DataLossNumber,
                configuration_number: epoch.ConfigurationNumber,
            };
            let app = self.0.clone();
            self.0
                .epoch
                .begin(&self.0.workers, callback, move |cancel| async move {
                    let _active = Active::new(&app.callbacks);
                    app.callback_calls.fetch_add(1, Ordering::Relaxed);
                    if previous.0 >= 0 {
                        app.horizon(previous.0, cancel.clone()).await?;
                    }
                    let _gate = app.gate.lock().await;
                    app.provider
                        .update_epoch(epoch, previous.0, cancel)
                        .await
                        .map_err(application_error)?;
                    *app.native_epoch.lock().map_err(|_| com::E_UNEXPECTED)? = Some(epoch);
                    Ok(())
                })
        })
    }
    fn EndUpdateEpoch(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<()> {
        com::guard(|| self.0.epoch.end(context))
    }
    fn BeginOnDataLoss(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
    ) -> Result<IFabricAsyncOperationContext> {
        com::guard(|| {
            let app = self.0.clone();
            self.0
                .loss
                .begin(&self.0.workers, callback, move |cancel| async move {
                    let _active = Active::new(&app.callbacks);
                    app.callback_calls.fetch_add(1, Ordering::Relaxed);
                    let _gate = app.gate.lock().await;
                    app.provider
                        .on_data_loss(cancel)
                        .await
                        .map_err(application_error)
                })
        })
    }
    fn EndOnDataLoss(&self, context: Ref<IFabricAsyncOperationContext>) -> Result<bool> {
        com::guard(|| self.0.loss.end(context))
    }
    fn GetCopyContext(&self) -> Result<IFabricOperationDataStream> {
        com::guard(|| {
            let empty = self
                .0
                .provider
                .get_copy_context()
                .map_err(application_error)?;
            stream(self.0.clone(), Source::Ready(empty), None)
        })
    }
    fn GetCopyState(
        &self,
        horizon: FABRIC_SEQUENCE_NUMBER,
        context: Ref<IFabricOperationDataStream>,
    ) -> Result<IFabricOperationDataStream> {
        com::guard(|| {
            if context.as_ref().is_some() || horizon.0 < 0 {
                return Err(com::E_INVALIDARG.into());
            }
            self.0.callback_calls.fetch_add(1, Ordering::Relaxed);
            let observation = self.0.observe_copy(horizon.0)?;
            stream(
                self.0.clone(),
                Source::Horizon(horizon.0),
                Some(observation),
            )
        })
    }
}

enum Source {
    Horizon(i64),
    Ready(Arc<dyn IOperationDataStream>),
}

struct CopyObservation {
    value: Mutex<Value>,
    callbacks: Arc<AtomicUsize>,
    ends: AtomicUsize,
}

struct EnumerationAttempt {
    terminal: Arc<Mutex<bool>>,
    finished: bool,
}

impl Drop for EnumerationAttempt {
    fn drop(&mut self) {
        if !self.finished {
            if let Ok(mut terminal) = self.terminal.lock() {
                *terminal = true;
            }
        }
    }
}

impl CopyObservation {
    fn increment(&self, name: &str) -> Result<()> {
        let mut value = self.value.lock().map_err(|_| com::E_UNEXPECTED)?;
        value[name] = value[name].as_u64().unwrap_or(0).saturating_add(1).into();
        Ok(())
    }
}

#[implement(NullableDataStream)]
struct CopyStream {
    app: Arc<Application>,
    source: Arc<tokio::sync::Mutex<Source>>,
    result: Slot<Option<IFabricOperationData>>,
    terminal: Arc<Mutex<bool>>,
    observation: Option<Arc<CopyObservation>>,
}

fn stream(
    app: Arc<Application>,
    source: Source,
    observation: Option<Arc<CopyObservation>>,
) -> Result<IFabricOperationDataStream> {
    use windows_core::Interface;
    let adapter: NullableDataStream = CopyStream {
        app,
        source: Arc::new(tokio::sync::Mutex::new(source)),
        result: Slot::default(),
        terminal: Arc::new(Mutex::new(false)),
        observation,
    }
    .into();
    adapter.cast()
}

impl NullableDataStream_Impl for CopyStream_Impl {
    unsafe fn BeginGetNext(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        out: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let app = self.app.clone();
            let source = self.source.clone();
            let terminal = self.terminal.clone();
            let observation = self.observation.clone();
            let context = self.result.begin_counted(
                &self.app.workers,
                callback,
                observation.as_ref().map(|o| o.callbacks.clone()),
                move |cancel| async move {
                    let mut attempt = EnumerationAttempt {
                        terminal: terminal.clone(),
                        finished: false,
                    };
                    let result = tokio::time::timeout(Duration::from_secs(8), async move {
                        let _active = Active::new(&app.callbacks);
                        if *terminal.lock().map_err(|_| com::E_UNEXPECTED)? {
                            return Err(com::E_UNEXPECTED.into());
                        }
                        let mut source = source.lock().await;
                        if let Source::Horizon(horizon) = *source {
                            app.horizon(horizon, cancel.clone()).await?;
                            let _gate = app.gate.lock().await;
                            *source = Source::Ready(
                                app.provider
                                    .get_copy_state(horizon, None)
                                    .map_err(application_error)?,
                            );
                            if let Some(observation) = &observation {
                                let state = app.provider.snapshot();
                                observation.value.lock().map_err(|_| com::E_UNEXPECTED)?
                                    ["build_context"] = json!({
                                    "requested_horizon": horizon,
                                    "provider_progress": state.last,
                                    "epoch": {"data_loss": state.epoch_data_loss,
                                        "configuration": state.epoch_configuration}
                                });
                            }
                        }
                        let Source::Ready(ref source) = *source else {
                            return Err(com::E_UNEXPECTED.into());
                        };
                        match source.get_next(cancel).await {
                            Ok(Some(data)) => {
                                tokio::time::sleep(Duration::from_millis(
                                    app.copy_delay.load(Ordering::Acquire),
                                ))
                                .await;
                                let data = operation_data(data.to_segments())?;
                                if let Some(observation) = &observation {
                                    observation.increment("items")?;
                                }
                                Ok(Some(data))
                            }
                            Ok(None) => {
                                if let Some(observation) = &observation {
                                    let mut value =
                                        observation.value.lock().map_err(|_| com::E_UNEXPECTED)?;
                                    if value["enumeration_end"] != true {
                                        value["enumeration_end"] = true.into();
                                        value["provider_progress_at_end"] = app
                                            .provider
                                            .get_last_committed_sequence_number()
                                            .map_err(application_error)?
                                            .into();
                                    }
                                }
                                Ok(None)
                            }
                            Err(error) => {
                                *terminal.lock().map_err(|_| com::E_UNEXPECTED)? = true;
                                Err(application_error(error))
                            }
                        }
                    })
                    .await
                    .map_err(application_error)?;
                    attempt.finished = result.is_ok();
                    result
                },
            )?;
            if let Some(observation) = &self.observation {
                observation.increment("begin_count")?;
            }
            self.app.callback_calls.fetch_add(1, Ordering::Relaxed);
            unsafe { com::return_interface(out, Some(context)) }
        })
    }

    unsafe fn EndGetNext(
        &self,
        context: Ref<IFabricAsyncOperationContext>,
        out: *mut Option<IFabricOperationData>,
    ) -> Result<()> {
        com::guard(|| {
            unsafe { com::clear_out(out)? };
            let result = self
                .result
                .end_counted(context, self.observation.as_ref().map(|o| &o.ends))?;
            unsafe { com::return_interface(out, result) }
        })
    }
}

#[cfg(test)]
#[path = "live_tests.rs"]
mod tests;
