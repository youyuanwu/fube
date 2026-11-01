//! Private volatile runtime composition. Native TransactionalReplicator.cpp:
//! 257–459, StateManager.cpp:103–179,4797–4919 and SecondaryDrainManager.cpp:
//! 200–233,323–376,653–799,906–972. Binding/owned futures are FUBE ADAPTATION;
//! no new admission policy or second binder. Native staged volatile copy and
//! memory/checkpoint execution remain distinct from the unfinished public facade.
use super::{
    checkpoint::await_all,
    copy_codec::{remove_stage, CopyContext, CopyMetadata, Stage},
    copy_sender::LazyCopy,
    identity,
    maintenance::{LogSettings, Maintenance},
    processor::KernelManager,
    provider::ProviderDirectory,
    record::{Record, RecordKind},
    registry::{Registry, RegistrySettings},
    runtime_lower::RuntimeLower,
    runtime_owner::ProviderOwner,
    status::Status,
    transaction::{own, RetryRuntime},
    version::Completion,
};
use crate::{
    ApiFuture, Cancellation, Epoch, Error, IDataLossHandler, IStateProvider2, OperationData,
    ReplicaRole, Result,
};
use fube_data::{
    extensions::{
        ReplicationEndpointOptions, ReplicatorDiagnostics, ReplicatorLifecycle,
        StateProviderCapabilities, VolatileLogInitialization,
    },
    FabricReplicator, IOperationStream, IStateProvider, IStateReplicator, Settings,
};
use fube_runtime::{extensions::NativeRuntimeIntegration, IStatefulServicePartition};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex, Weak,
    },
};

pub(crate) struct RuntimeConfig {
    pub(crate) log_settings: LogSettings,
    pub(crate) registry: Option<RegistrySettings>,
    pub(crate) providers: Vec<(i64, Arc<dyn IStateProvider2>)>,
    pub(crate) work_folder: PathBuf,
    pub(crate) memory_bytes: usize,
    pub(crate) data_loss: Arc<dyn IDataLossHandler>,
}
pub(crate) struct Generation {
    pub(crate) copy_done: Completion<()>,
    pub(crate) done: Completion<()>,
}
pub(crate) struct VolatileRuntime {
    #[cfg(test)]
    pub(crate) copy_log_read_error: Mutex<Option<usize>>,
    #[cfg(test)]
    pub(crate) copy_write_failure: Mutex<Option<(i64, Error)>>,
    #[cfg(test)]
    pub(crate) copy_selection_pause: Mutex<Option<(Completion<()>, Completion<()>)>>,
    full_copy_unfinished: AtomicBool,
    pub(crate) maintenance: Mutex<Option<Arc<Maintenance>>>,
    pub(crate) registry: Arc<Registry>,
    #[cfg(test)]
    pub(crate) role_events: Mutex<Vec<String>>,
    weak: Weak<Self>,
    pub(crate) owner: Arc<ProviderOwner>,
    pub(crate) lower: Mutex<Option<Arc<FabricReplicator>>>,
    pub(crate) logging: Mutex<Option<Arc<RuntimeLower>>>,
    native: NativeRuntimeIntegration,
    config: RuntimeConfig,
    life: tokio::sync::Mutex<()>,
    opened: Mutex<Vec<Arc<dyn IStateProvider2>>>,
    pub(crate) generation: Mutex<Option<Arc<Generation>>>,
    terminal_started: AtomicBool,
    pub(crate) terminal: Completion<()>,
    pub(crate) cleanup_done: Completion<()>,
    pub(crate) fault_done: Completion<()>,
    pub(crate) provider_fault_done: Completion<()>,
    pub(crate) provider_fault_reports: AtomicI64,
    pub(crate) fault_reports: AtomicI64,
}
struct Adapter(Weak<VolatileRuntime>);
impl Adapter {
    fn runtime(&self) -> Result<Arc<VolatileRuntime>> {
        self.0.upgrade().ok_or_else(|| Status::ObjectClosed.error())
    }
}
impl VolatileRuntime {
    pub(crate) fn create(
        partition: Arc<dyn IStatefulServicePartition>,
        native: NativeRuntimeIntegration,
        endpoint: ReplicationEndpointOptions,
        settings: Settings,
        mut config: RuntimeConfig,
    ) -> Result<Arc<Self>> {
        let mut names = std::collections::BTreeSet::new();
        if config
            .providers
            .iter()
            .any(|(_, provider)| !names.insert(provider.get_name()))
        {
            return Err(Error::InvalidInput("duplicate supplied provider name"));
        }
        let registry = Registry::new(config.registry.take(), &config.providers)?;
        let directory =
            ProviderDirectory::preinitialized(config.providers.clone(), Some(registry.clone()))?;
        directory.set_local_callbacks(registry.clone());
        let manager = KernelManager::fresh(directory, Arc::new(RetryRuntime::default()));
        manager.fence_application();
        let owner = Arc::new(ProviderOwner {
            manager: manager.clone(),
            registry: registry.clone(),
            partition: partition.clone(),
            opened: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            read_after: AtomicI64::new(i64::MAX),
        });
        let runtime = Arc::new_cyclic(|weak| Self {
            #[cfg(test)]
            copy_log_read_error: Mutex::new(None),
            #[cfg(test)]
            copy_write_failure: Mutex::new(None),
            #[cfg(test)]
            copy_selection_pause: Mutex::new(None),
            full_copy_unfinished: AtomicBool::new(false),
            maintenance: Mutex::new(None),
            registry: registry.clone(),
            #[cfg(test)]
            role_events: Mutex::new(Vec::new()),
            weak: weak.clone(),
            owner: owner.clone(),
            lower: Mutex::new(None),
            logging: Mutex::new(None),
            native: native.clone(),
            config,
            life: tokio::sync::Mutex::new(()),
            opened: Mutex::new(Vec::new()),
            generation: Mutex::new(None),
            terminal_started: AtomicBool::new(false),
            terminal: Completion::new(),
            cleanup_done: Completion::new(),
            fault_done: Completion::new(),
            provider_fault_done: Completion::new(),
            provider_fault_reports: AtomicI64::new(0),
            fault_reports: AtomicI64::new(0),
        });
        let adapter = Arc::new(Adapter(Arc::downgrade(&runtime)));
        let lower = FabricReplicator::create_with_lifecycle(
            partition,
            native,
            adapter.clone(),
            None,
            endpoint,
            settings.clone(),
            StateProviderCapabilities {
                supports_copy_until_latest_lsn: true,
            },
            adapter,
        )?;
        let logging = RuntimeLower::with_operation_limits(lower.clone(), &settings)?;
        manager.bind_runtime(logging.clone());
        *runtime.lower.lock().unwrap() = Some(lower);
        *runtime.logging.lock().unwrap() = Some(logging);
        let weak = Arc::downgrade(&owner);
        manager.set_write_access(Arc::new(move || {
            weak.upgrade()
                .ok_or_else(|| Status::ObjectClosed.error())?
                .writable()
        }));
        registry.bind(Arc::downgrade(&runtime));
        let maintenance = Maintenance::new(
            Arc::downgrade(&runtime),
            runtime.config.log_settings.clone(),
        )?;
        *runtime.maintenance.lock().unwrap() = Some(maintenance.clone());
        let weak = Arc::downgrade(&maintenance);
        let throttle = Arc::downgrade(&maintenance);
        manager.set_maintenance_hooks(
            Arc::new(move |record| {
                weak.upgrade()
                    .ok_or_else(|| Status::ObjectClosed.error())?
                    .after_record(record)
            }),
            Arc::new(move || {
                throttle
                    .upgrade()
                    .ok_or_else(|| Status::ObjectClosed.error())?
                    .throttle()
            }),
        );
        let weak = Arc::downgrade(&maintenance);
        manager.set_checkpoint_hook(Arc::new(move |lsn| {
            weak.upgrade()
                .ok_or_else(|| Status::ObjectClosed.error())?
                .checkpoint_on_barrier(lsn)
        }));
        let weak = Arc::downgrade(&runtime);
        manager.faults.set_hook(Arc::new(move |_| {
            if let Some(runtime) = weak.upgrade() {
                runtime.report_fault();
            }
        }));
        let weak = Arc::downgrade(&runtime);
        manager.faults.set_provider_hook(Arc::new(move |_| {
            if let Some(runtime) = weak.upgrade() {
                runtime.report_provider_fault();
            }
        }));
        Ok(runtime)
    }
    fn shared(&self) -> Arc<Self> {
        self.weak.upgrade().expect("owned volatile runtime")
    }
    pub(crate) fn maintenance(&self) -> Arc<Maintenance> {
        self.maintenance.lock().unwrap().as_ref().unwrap().clone()
    }
    pub(crate) fn request_group_commit(&self) {
        self.logging().request_group(self.owner.manager.clone());
    }
    pub(crate) fn identity(&self) -> (uuid::Uuid, i64) {
        let identity = self.native.identity();
        (identity.partition_id, identity.replica_id)
    }
    pub(crate) fn work_folder(&self) -> PathBuf {
        self.config.work_folder.clone()
    }
    pub(crate) fn copy_limits(&self) -> (usize, usize) {
        self.logging().copy_limits()
    }
    pub(crate) fn adopt_opened(
        &self,
        id: i64,
        provider: Arc<dyn IStateProvider2>,
        mode: super::provider::ProviderMode,
    ) -> Result<()> {
        let mut opened = self.opened.lock().unwrap();
        if opened.len() >= 128 {
            return Err(Status::InsufficientResources.error());
        }
        opened.push(provider.clone());
        self.owner.manager.directory.entries.lock().unwrap().insert(
            id,
            super::provider::ProviderEntry {
                provider,
                mode,
                deleting_by: None,
            },
        );
        Ok(())
    }
    pub(crate) async fn retire_provider(&self, id: i64) -> Result<()> {
        let entry = self
            .owner
            .manager
            .directory
            .entries
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| Status::InvalidParameter.error())?;
        if entry.provider.close(Cancellation::default()).await.is_err() {
            entry.provider.abort();
        }
        self.owner
            .manager
            .directory
            .entries
            .lock()
            .unwrap()
            .remove(&id);
        self.opened
            .lock()
            .unwrap()
            .retain(|p| !Arc::ptr_eq(p, &entry.provider));
        self.owner.manager.directory.retire_checkpoint_metadata(id);
        Ok(())
    }
    pub(crate) fn replicator(&self) -> Arc<FabricReplicator> {
        self.lower.lock().unwrap().as_ref().unwrap().clone()
    }
    fn logging(&self) -> Arc<RuntimeLower> {
        self.logging.lock().unwrap().as_ref().unwrap().clone()
    }
    fn providers(&self) -> Vec<Arc<dyn IStateProvider2>> {
        self.owner
            .manager
            .directory
            .entries
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.mode == super::provider::ProviderMode::Active)
            .map(|entry| entry.provider.clone())
            .collect()
    }
    async fn close_providers(&self, cancellation: Cancellation) -> Result<()> {
        let providers = std::mem::take(&mut *self.opened.lock().unwrap());
        // ApiDispatcher::Close(AbortStateProvider): all peers start, failed Close
        // invokes native synchronous Abort fallback and terminal result is success.
        await_all(
            providers
                .into_iter()
                .map(|provider| {
                    let cancellation = cancellation.clone();
                    Box::pin(async move {
                        if provider.close(cancellation).await.is_err() {
                            provider.abort();
                        }
                        Ok(())
                    }) as ApiFuture<'static, ()>
                })
                .collect(),
        )
        .await
    }
    async fn open(&self, cancellation: Cancellation) -> Result<()> {
        let _life = self.life.lock().await;
        self.owner.manager.open_memory(self.config.memory_bytes)?;
        self.replicator().initialize_volatile_log().await?;
        self.owner.opened.store(true, Ordering::Release); // callback-ready, NOT app-ready
        self.owner.manager.open_cleanup_gate();
        self.owner
            .read_after
            .store(self.owner.manager.tail(), Ordering::Release);
        let result: Result<()> = async {
            for (id, provider) in self.registry.initialize_initial(&self.config.providers)? {
                provider.open(cancellation.clone()).await?;
                let mode = self
                    .registry
                    .rows()
                    .into_iter()
                    .find(|r| r.id == id)
                    .unwrap()
                    .mode;
                self.adopt_opened(id, provider.clone(), mode)?;
                if mode == super::provider::ProviderMode::Active {
                    provider.recover_checkpoint(cancellation.clone()).await?;
                }
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            self.owner.manager.close_cleanup_gate().await;
            self.close_providers(Cancellation::default()).await?;
            self.owner.opened.store(false, Ordering::Release);
        }
        result
    }
    async fn provider_role(&self, role: ReplicaRole, cancellation: Cancellation) -> Result<()> {
        await_all(
            self.providers()
                .into_iter()
                .map(|provider| {
                    let cancellation = cancellation.clone();
                    Box::pin(async move { provider.change_role(role, cancellation).await })
                        as ApiFuture<'static, ()>
                })
                .collect(),
        )
        .await?;
        if role == ReplicaRole::None {
            await_all(
                self.providers()
                    .into_iter()
                    .map(|provider| {
                        let cancellation = cancellation.clone();
                        Box::pin(async move { provider.remove_state(cancellation).await })
                            as ApiFuture<'static, ()>
                    })
                    .collect(),
            )
            .await?;
        }
        Ok(())
    }
    async fn role(&self, role: ReplicaRole, cancellation: Cancellation) -> Result<()> {
        let _life = self.life.lock().await;
        self.owner.ready.store(false, Ordering::Release);
        self.owner.manager.fence_application();
        if self.terminal_started.load(Ordering::Acquire) || self.owner.manager.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        let previous = crate::ITransactionManager::get_role(self.owner.manager.as_ref());
        #[cfg(test)]
        self.role_events
            .lock()
            .unwrap()
            .push(format!("start:{previous:?}->{role:?}"));
        if previous == role {
            if self.full_copy_unfinished.load(Ordering::Acquire) {
                return Ok(());
            }
            self.owner.manager.open_activity_gate()?;
            self.owner.ready.store(true, Ordering::Release);
            return Ok(());
        }
        let generation = self.generation.lock().unwrap().clone();
        if previous == ReplicaRole::IdleSecondary && role == ReplicaRole::ActiveSecondary {
            if let Some(generation) = &generation {
                generation.copy_done.wait().await.as_ref().clone()?;
            }
            let _pause = self.owner.manager.pause_dispatch().await;
            if let Err(error) = self.provider_role(role, cancellation).await {
                self.owner
                    .manager
                    .faults
                    .process_service_error(self.owner.manager.role(), error.clone());
                return Err(error);
            }
            self.owner.manager.set_role(role);
        } else {
            if let Some(generation) = &generation {
                generation.done.wait().await.as_ref().clone()?;
            }
            self.maintenance().stop().await;
            if previous == ReplicaRole::Primary
                || matches!(role, ReplicaRole::None | ReplicaRole::Unknown)
            {
                let event = if role == ReplicaRole::None {
                    7
                } else if role == ReplicaRole::Unknown {
                    4
                } else {
                    5
                };
                self.logging()
                    .finish_generation(&self.owner.manager, event)
                    .await?;
                self.owner.manager.cleanup_pending_operations();
            } else {
                self.logging().stop().await;
            }
            *self.generation.lock().unwrap() = None;
            let _pause = self.owner.manager.pause_dispatch().await;
            self.owner.manager.set_role(role);
            self.maintenance().resume();
            // Native newly initialized ActiveSecondary providers pass Idle first.
            if previous == ReplicaRole::Unknown && role == ReplicaRole::ActiveSecondary {
                self.provider_role(ReplicaRole::IdleSecondary, cancellation.clone())
                    .await?;
            }
            if let Err(error) = self.provider_role(role, cancellation).await {
                if role == ReplicaRole::ActiveSecondary {
                    self.owner
                        .manager
                        .faults
                        .process_service_error(self.owner.manager.role(), error.clone());
                }
                return Err(error);
            }
            if matches!(
                role,
                ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
            ) {
                self.start_generation()?;
            }
            if role == ReplicaRole::Primary {
                self.logging().enable_barriers(&self.owner.manager)?;
            }
        }
        if self.owner.manager.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        if !matches!(role, ReplicaRole::None | ReplicaRole::Unknown)
            && !self.full_copy_unfinished.load(Ordering::Acquire)
        {
            self.owner.manager.open_activity_gate()?;
            self.owner.ready.store(true, Ordering::Release);
        }
        #[cfg(test)]
        self.role_events.lock().unwrap().push(format!(
            "done:{role:?}:generation={}",
            self.generation.lock().unwrap().is_some()
        ));
        Ok(())
    }
    fn start_generation(&self) -> Result<()> {
        let copy = self.replicator().get_copy_stream()?;
        let generation = Arc::new(Generation {
            copy_done: Completion::new(),
            done: Completion::new(),
        });
        *self.generation.lock().unwrap() = Some(generation.clone());
        let runtime = self.shared();
        drop(own(Box::pin(async move {
            let copied = runtime.drain_copy(copy).await;
            generation.copy_done.complete(copied.clone());
            let result = match copied {
                Err(error) => Err(error),
                Ok(()) => match runtime.replicator().get_replication_stream() {
                    Err(error) => Err(error),
                    Ok(stream) => runtime.drain_replication(stream).await,
                },
            };
            if let Err(error) = &result {
                if !runtime.terminal_started.load(Ordering::Acquire)
                    && !runtime.replicator().snapshot().closed
                {
                    runtime
                        .owner
                        .manager
                        .faults
                        .report_checkpoint_fault(error.clone());
                }
            }
            generation.done.complete(result);
            Ok(())
        })));
        Ok(())
    }
    async fn boundary(&self, event: i32) -> Result<()> {
        let boundary = self.owner.manager.local_boundary(12, event)?;
        boundary.flush.wait().await.as_ref().clone()?;
        boundary.apply.wait().await.as_ref().clone()?;
        boundary.processing.wait().await.as_ref().clone()
    }
    async fn drain_copy(&self, stream: Arc<dyn IOperationStream>) -> Result<()> {
        let Some(header) = stream.get_operation(Cancellation::default()).await? else {
            return Ok(());
        };
        let data = header.get_data()?;
        if data.segments().len() != 1 || data.bytes() != 16 {
            return Err(Error::InvalidInput("native copy header length"));
        }
        let bytes = &data.segments()[0];
        if u32::from_le_bytes(bytes[..4].try_into().unwrap()) != 1 {
            return Err(Status::NotImplemented.error());
        }
        let stage = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if stage != 2 && stage != 3 && stage != 7 {
            return Err(Status::NotImplemented.error());
        }
        header.acknowledge()?;
        drop(header); // actual lower capture BEFORE next poll
        if stage == 3 {
            return self.drain_full(stream).await;
        }
        if stage == 2 {
            if stream
                .get_operation(Cancellation::default())
                .await?
                .is_some()
            {
                return Err(Error::InvalidInput("None copy requires EOS"));
            }
            return Ok(());
        }
        while let Some(operation) = stream.get_operation(Cancellation::default()).await? {
            let data = operation.get_data()?;
            let segments = data.segments();
            if segments
                .last()
                .is_none_or(|stage| stage.as_ref() != 7i32.to_le_bytes())
            {
                return Err(Status::NotImplemented.error());
            }
            let batch = OperationData::new(
                segments[..segments.len() - 1]
                    .iter()
                    .map(|s| s.to_vec())
                    .collect(),
            )?;
            let records = Record::decode_batch(&batch)?;
            for record in &records {
                KernelManager::preflight_replay(record)?;
                self.owner.manager.preflight_replay_routing(record)?;
            }
            let mut failure = None;
            for record in records {
                let epoch = matches!(record.kind, RecordKind::UpdateEpoch { .. });
                let activity = if matches!(record.kind, RecordKind::UpdateEpoch { .. }) {
                    self.owner.manager.replay_copied_epoch(record)?
                } else {
                    Some(self.owner.manager.replay_decoded(record)?)
                };
                self.owner.manager.flush(Ok(()));
                if let Some(activity) = activity {
                    if let Err(error) = activity.flush.wait().await.as_ref() {
                        failure.get_or_insert(error.clone());
                    } else if !epoch {
                        let _ = self.maintenance().after_record(activity.node.record())?;
                    }
                }
            }
            // Native copied-log batch ACK follows settlement of ALL flush waiters,
            // including failures (SecondaryDrainManager.cpp:715–799). Not state ACK.
            let ack = operation.acknowledge();
            drop(operation);
            ack?;
            if let Some(error) = failure {
                return Err(error);
            }
        }
        self.boundary(2).await
    }
    async fn drain_replication(&self, stream: Arc<dyn IOperationStream>) -> Result<()> {
        while let Some(operation) = stream.get_operation(Cancellation::default()).await? {
            let mut record = Record::decode(&operation.get_data()?)?;
            record.lsn = operation.get_metadata()?.sequence_number;
            KernelManager::preflight_replay(&record)?;
            self.owner.manager.preflight_replay_routing(&record)?;
            if record.lsn <= self.owner.manager.tail()
                && !matches!(record.kind, RecordKind::UpdateEpoch { .. })
            {
                self.owner.manager.wait_flushed(record.lsn).await?;
                operation.acknowledge()?;
                drop(operation);
                continue;
            }
            let activity = self.owner.manager.replay_decoded(record)?;
            self.owner.manager.flush(Ok(()));
            activity.flush.wait().await.as_ref().clone()?; // failed replication flush has NO ACK
            let _ = self.maintenance().after_record(activity.node.record())?;
            if matches!(activity.node.record().kind, RecordKind::Barrier { .. }) {
                self.maintenance().block_secondary(false).await;
            }
            operation.acknowledge()?;
            drop(operation); // capture retires before polling even if Apply awaits Barrier
        }
        self.boundary(3).await
    }
    async fn next_copy_log(
        &self,
        stream: &Arc<dyn IOperationStream>,
    ) -> Result<Option<Arc<dyn fube_data::IOperation>>> {
        #[cfg(test)]
        {
            let mut fault = self.copy_log_read_error.lock().unwrap();
            if let Some(remaining) = fault.as_mut() {
                if *remaining == 0 {
                    *fault = None;
                    return Err(Error::InvalidInput("controlled copy stream read failure"));
                }
                *remaining -= 1;
            }
        }
        stream.get_operation(Cancellation::default()).await
    }
    async fn drain_full(&self, stream: Arc<dyn IOperationStream>) -> Result<()> {
        use crate::internal::IStateProviderManager;
        self.full_copy_unfinished.store(true, Ordering::Release);
        self.owner.manager.fence_application();
        self.owner.ready.store(false, Ordering::Release);
        self.owner.read_after.store(i64::MAX, Ordering::Release);
        let Some(mut operation) = stream.get_operation(Cancellation::default()).await? else {
            return Ok(());
        };
        let mut began = false;
        let mut number = 0i64;
        let metadata = loop {
            let data = operation.get_data()?;
            let (stage, payload) = remove_stage(&data)?;
            match stage {
                Stage::State => {
                    if !began {
                        self.owner
                            .manager
                            .directory
                            .begin_setting_current_state()
                            .await?;
                        began = true;
                    }
                    self.owner
                        .manager
                        .directory
                        .set_current_state(number, payload)
                        .await?;
                    operation.acknowledge()?;
                    drop(operation);
                    number = number
                        .checked_add(1)
                        .ok_or_else(|| Status::InvalidOperation.error())?;
                    let Some(next) = stream.get_operation(Cancellation::default()).await? else {
                        return Err(Status::Cancelled.error());
                    };
                    operation = next;
                }
                Stage::Progress => {
                    if !began {
                        return Err(Status::InvalidOperation.error());
                    }
                    let metadata = CopyMetadata::decode(&data)?;
                    self.owner
                        .manager
                        .directory
                        .end_setting_current_state()
                        .await?;
                    break metadata;
                }
                _ => return Err(Status::NotImplemented.error()),
            }
        };
        // Retain EXACT transition operation through actual native log setup,
        // first checkpoint initiation and only then ACK.
        let mut checkpoint = false;
        let mut completed = false;
        let result: Result<()> = async {
            self.owner.manager.processing_drained().await;
            self.maintenance().stop().await;
            self.maintenance().resume();
            self.owner
                .manager
                .reset_copy_log(&metadata, self.config.memory_bytes)?;
            self.owner
                .read_after
                .store(metadata.highest_provider, Ordering::Release);
            let mut setup_failure = None;
            if let Some(last) = metadata
                .vector
                .last()
                .filter(|v| v.lsn == metadata.starting)
            {
                let record = Record {
                    lsn: metadata.starting,
                    kind: RecordKind::UpdateEpoch {
                        epoch: last.epoch,
                        primary_replica_id: last.primary,
                        timestamp_ticks: last.ticks,
                    },
                };
                let seeded = self.owner.manager.replay_decoded(record)?;
                self.owner.manager.flush(Ok(()));
                if let Err(error) = seeded.flush.wait().await.as_ref() {
                    setup_failure = Some(error.clone());
                }
            }
            if metadata.starting == metadata.checkpoint && setup_failure.is_none() {
                self.maintenance()
                    .start_checkpoint(true)?
                    .ok_or_else(|| Status::InvalidOperation.error())?;
                checkpoint = true;
                if metadata.starting == metadata.upto {
                    match self.maintenance().finish_copy(metadata.upto).await {
                        Ok(()) => completed = true,
                        Err(error) => setup_failure = Some(error),
                    }
                }
            }
            operation.acknowledge()?;
            drop(operation);
            if let Some(error) = setup_failure {
                return Err(error);
            }
            while let Some(operation) = self.next_copy_log(&stream).await? {
                let (stage, payload) = remove_stage(&operation.get_data()?)?;
                if stage != Stage::Log {
                    return Err(Status::NotImplemented.error());
                }
                let records = Record::decode_batch(&payload)?;
                for record in &records {
                    KernelManager::preflight_replay(record)?;
                    self.owner.manager.preflight_replay_routing(record)?;
                }
                let mut failure = None;
                for record in records {
                    let current = self.owner.manager.tail();
                    if record.lsn < current
                        || (record.lsn == current
                            && !matches!(record.kind, RecordKind::UpdateEpoch { .. }))
                    {
                        continue;
                    }
                    let epoch = matches!(record.kind, RecordKind::UpdateEpoch { .. });
                    #[cfg(test)]
                    {
                        let fault = self.copy_write_failure.lock().unwrap().clone();
                        if let Some((lsn, error)) = fault.filter(|(lsn, _)| *lsn == record.lsn) {
                            self.copy_write_failure.lock().unwrap().take();
                            self.owner
                                .manager
                                .memory()
                                .unwrap()
                                .lock()
                                .unwrap()
                                .write_error = Some(error);
                            let _ = lsn;
                        }
                    }
                    let activity = if epoch {
                        self.owner.manager.replay_copied_epoch(record)?
                    } else {
                        Some(self.owner.manager.replay_decoded(record)?)
                    };
                    self.owner.manager.flush(Ok(()));
                    if let Some(activity) = activity {
                        if let Err(error) = activity.flush.wait().await.as_ref() {
                            failure.get_or_insert(error.clone());
                        } else if !epoch {
                            let tail = self.owner.manager.tail();
                            if tail < metadata.checkpoint {
                                self.owner
                                    .manager
                                    .index_if_needed(self.config.log_settings.index_interval())?;
                            } else if tail > metadata.checkpoint {
                                self.maintenance().after_record(activity.node.record())?;
                            }
                        }
                    }
                    if !checkpoint
                        && failure.is_none()
                        && self.owner.manager.tail() == metadata.checkpoint
                    {
                        self.maintenance()
                            .start_checkpoint(true)?
                            .ok_or_else(|| Status::InvalidOperation.error())?;
                        checkpoint = true;
                    }
                    if checkpoint && !completed && self.owner.manager.tail() == metadata.upto {
                        match self.maintenance().finish_copy(metadata.upto).await {
                            Ok(()) => completed = true,
                            Err(error) => {
                                failure.get_or_insert(error);
                            }
                        }
                    }
                }
                let ack = operation.acknowledge();
                drop(operation);
                ack?;
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            if !completed {
                return Err(Status::Cancelled.error());
            }
            self.boundary(2).await?;
            self.full_copy_unfinished.store(false, Ordering::Release);
            // Only the lifecycle owner may publish readiness after provider roles.
            // copy_done can release a concurrently fenced Idle->Active callback.
            Ok(())
        }
        .await;
        if result.is_err() && checkpoint && !completed {
            self.maintenance().settle_failed_copy(metadata.upto).await;
        }
        result
    }
    fn report_fault(&self) {
        self.owner.ready.store(false, Ordering::Release);
        self.owner.manager.fence_fault();
        let runtime = self.shared();
        drop(own(Box::pin(async move {
            runtime.fault_reports.fetch_add(1, Ordering::AcqRel);
            // RoleContextDrainState.cpp:54–68 closes UPPER admission and reports
            // once; it does NOT issue lower Abort or cancel accepted quorum.
            // Native RA's subsequent Close/Abort/reconfiguration remains authority.
            let result = runtime
                .native
                .report_fault(fube_runtime::FaultType::Transient)
                .await
                .map(|_| ());
            runtime.fault_done.complete(result);
            Ok(())
        })));
    }
    fn report_provider_fault(&self) {
        let runtime = self.shared();
        drop(own(Box::pin(async move {
            runtime
                .provider_fault_reports
                .fetch_add(1, Ordering::AcqRel);
            // Native StateManager's direct notification does not latch processor
            // service error, change role, or abort the lower.
            let result = runtime
                .native
                .report_fault(fube_runtime::FaultType::Transient)
                .await
                .map(|_| ());
            runtime.provider_fault_done.complete(result);
            Ok(())
        })));
    }
    async fn finish_cleanup(&self, cancellation: Cancellation) -> Result<()> {
        self.owner.manager.processing_drained().await;
        self.logging().drained().await;
        self.owner.manager.close_cleanup_gate().await;
        self.owner.manager.cleanup_pending_operations();
        self.close_providers(cancellation).await?;
        self.owner.opened.store(false, Ordering::Release);
        Ok(())
    }
    async fn close_work(&self, cancellation: Cancellation) -> Result<()> {
        let _life = self.life.lock().await;
        self.owner.ready.store(false, Ordering::Release);
        self.owner.manager.fence_application();
        let generation = self.generation.lock().unwrap().clone();
        let drained = if let Some(generation) = generation {
            generation.done.wait().await.as_ref().clone()
        } else {
            Ok(())
        };
        self.maintenance().stop().await;
        let processed = self
            .logging()
            .finish_generation(&self.owner.manager, 4)
            .await;
        if let Err(error) = processed {
            // No physical record was fabricated. Report failed terminal attempt
            // independently of unresolved logical processing, keeping a strong
            // owner until ACTUAL cleanup/fencing. This is not completed drain.
            let runtime = self.shared();
            drop(own(Box::pin(async move {
                let _life = runtime.life.lock().await;
                let cleanup = runtime.finish_cleanup(cancellation).await;
                runtime.cleanup_done.complete(cleanup);
                Ok(())
            })));
            return drained.and(Err(error));
        }
        let cleanup = self.finish_cleanup(cancellation).await;
        self.cleanup_done.complete(cleanup.clone());
        drained.and(cleanup)
    }
    async fn close(&self, cancellation: Cancellation) -> Result<()> {
        self.owner.manager.fence_terminal();
        self.owner.ready.store(false, Ordering::Release);
        if self.terminal_started.swap(true, Ordering::AcqRel) {
            return self.terminal.wait().await.as_ref().clone();
        }
        let result = self.close_work(cancellation).await;
        self.terminal.complete(result.clone());
        result
    }
    fn abort(&self) {
        self.owner.manager.fence_terminal();
        self.owner.ready.store(false, Ordering::Release);
        if self.terminal_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let runtime = self.shared();
        drop(own(Box::pin(async move {
            let result = runtime.close_work(Cancellation::default()).await;
            runtime.terminal.complete(result);
            Ok(())
        })));
    }
}
impl IStateProvider for Adapter {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        Ok(self.runtime()?.owner.manager.tail())
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let runtime = self.runtime()?;
            if runtime.owner.manager.role() == super::processor::ProcessingRole::Primary {
                runtime.logging().wait_publications().await;
                runtime.owner.manager.wait_lsn_ordering(previous).await;
            }
            if let Some(activity) = runtime.owner.manager.update_epoch_provider(
                epoch,
                previous,
                runtime.native.identity().replica_id,
                identity::native_system_time()?,
            )? {
                runtime.owner.manager.flush(Ok(()));
                activity.processing.wait().await.as_ref().clone()?;
                activity.flush.wait().await.as_ref().clone()?;
            }
            Ok(())
        })
    }
    fn on_data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async move {
            self.runtime()?
                .config
                .data_loss
                .data_loss(cancellation)
                .await
        })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        let runtime = self.runtime()?;
        let manager = &runtime.owner.manager;
        let tail = manager.tail();
        let memory = manager.memory().unwrap();
        let memory = memory.lock().unwrap();
        let (_, _, head, head_epoch) = memory.head_identity();
        let data = CopyContext {
            replica: runtime.identity().1,
            vector: memory.vector.clone(),
            head_epoch,
            head,
            tail,
            atomic_redo: 0,
        }
        .encode()?;
        Ok(Arc::new(ContextStream(Mutex::new(Some(data)))))
    }
    fn get_copy_state(
        &self,
        upto: i64,
        context: Option<Arc<dyn fube_data::IOperationDataStream>>,
    ) -> Result<Arc<dyn fube_data::IOperationDataStream>> {
        Ok(LazyCopy::new(
            Arc::downgrade(&self.runtime()?),
            upto,
            context,
        ))
    }
}
struct ContextStream(Mutex<Option<OperationData>>);
impl fube_data::IOperationDataStream for ContextStream {
    fn get_next(&self, _: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move { Ok(self.0.lock().unwrap().take()) })
    }
}
impl ReplicatorLifecycle for Adapter {
    fn opened(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.runtime()?.open(cancellation).await })
    }
    fn role_changed(
        &self,
        _: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.runtime()?.role(role, cancellation).await })
    }
    fn closed(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.runtime()?.close(cancellation).await })
    }
    fn aborted(&self) {
        if let Ok(runtime) = self.runtime() {
            runtime.abort();
        }
    }
}
