//! Runtime-owned adaptation of native invocation records to one lower owner.
//! RAP still calls the actual service-returned object; this is its retained
//! lower, never a replacement-object binder.
use std::sync::Arc;

use fube_data::{
    extensions::{ReplayCheckpointProvider, ReplicationEndpointOptions, StateProviderCapabilities},
    hosting::{
        Configuration, HealthClient, HostServices, HostingEndpoint, MembershipSource,
        MutationCheck, PreWrite, Progress, Removal, Replica, ReplicatorFactory, StableFacts,
    },
    FabricReplicator, IStateProvider, IStateReplicator, Settings,
};

use crate::{
    extensions::{
        CatchUpResult, ConfigurationStage, EpochDomains, NativeInvocation, NativeInvocationKind,
        NativeProgressSource, NativeReplica, NativeReplicaSet, NativeRuntimeIntegration,
        ReplicaDiagnostics, ReplicaLifecycle, ReplicaRemoval, ReplicationProgress,
    },
    ApiFuture, Cancellation, Epoch, Error, FaultType, IPrimaryReplicator, IReplicator,
    IStatefulServicePartition, ReplicaInformation, ReplicaRole, ReplicaSetConfig,
    ReplicaSetQuorumMode, Result, ServicePartitionInformation,
};

struct Validity(NativeRuntimeIntegration);
impl MutationCheck for Validity {
    fn validate(&self) -> Result<()> {
        self.0.validate()
    }
}
pub(crate) struct ControlCheck(pub(crate) NativeInvocation);
impl MutationCheck for ControlCheck {
    fn validate(&self) -> Result<()> {
        self.0.validate()
    }
}
struct Membership(NativeRuntimeIntegration);
impl MembershipSource for Membership {
    fn observe(&self) -> Result<StableFacts> {
        // The previous lower ignored unavailable snapshots, but rejected an
        // ambiguous eligible primary. Preserve that distinction.
        match self.0.status() {
            Ok(status) => stable_facts(&status),
            Err(_) => Ok(StableFacts::default()),
        }
    }
}
struct Health(NativeRuntimeIntegration);
impl HealthClient for Health {
    fn report_fault(&self, fault: FaultType) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.0.report_fault(fault).await.map(|_| ()) })
    }
}
struct LowerProgress(std::sync::Weak<FabricReplicator>);
impl NativeProgressSource for LowerProgress {
    fn sample_progress(&self) -> Result<ReplicationProgress> {
        let progress = self.0.upgrade().ok_or(Error::Closed)?.sample_progress()?;
        Ok(ReplicationProgress {
            first: progress.first,
            last: progress.last,
        })
    }
}

fn project_replica(replica: &NativeReplica) -> Result<Replica> {
    // Keep descriptor validation (including native role decoding) runtime-side.
    let descriptor = replica.descriptor()?;
    Ok(project_descriptor(&descriptor))
}
pub fn project_descriptor(replica: &crate::extensions::NativeReplicaDescriptor) -> Replica {
    Replica {
        replica_id: replica.replica_id,
        incarnation: replica.incarnation,
        up: replica.up,
        ready: replica.ready,
        address: replica.address.clone(),
        progress: Progress {
            first: replica.progress.first,
            last: replica.progress.last,
        },
        must_catch_up: replica.must_catch_up,
    }
}
pub fn project_configuration(config: &NativeReplicaSet) -> Result<Configuration> {
    Ok(Configuration {
        epoch: Epoch {
            data_loss_number: config.epoch.data_loss,
            configuration_number: config.epoch.configuration,
        },
        replicas: config
            .replicas
            .iter()
            .map(project_replica)
            .collect::<Result<_>>()?,
        write_quorum: config.write_quorum,
        local_member: config.local_member,
    })
}
fn prewrite(proof: &NativeInvocation) -> Option<PreWrite> {
    proof.prewrite_epochs().map(|(cc, pc)| PreWrite {
        current: Epoch {
            data_loss_number: cc.data_loss,
            configuration_number: cc.configuration,
        },
        previous: Epoch {
            data_loss_number: pc.data_loss,
            configuration_number: pc.configuration,
        },
    })
}
fn stable_facts(status: &ReplicaDiagnostics) -> Result<StableFacts> {
    let mut facts = StableFacts::default();
    if status.lifecycle != ReplicaLifecycle::Open || !status.replicator_open {
        return Ok(facts);
    }
    let epoch = status.current_epoch.primary();
    if status.current_role == ReplicaRole::Primary
        && status.service_role == ReplicaRole::Primary
        && status.replicator_role == ReplicaRole::Primary
        && status.configuration_stage == ConfigurationStage::Current
        && status.catch_up == CatchUpResult::Completed
        && status.minimum_replica_set_size == 1
        && status.configuration.is_empty()
    {
        facts.singleton = Some(epoch);
    }
    if status.current_role == ReplicaRole::ActiveSecondary
        && status.replicator_role == ReplicaRole::ActiveSecondary
        && status.service_role == ReplicaRole::ActiveSecondary
    {
        let mut primaries = status
            .configuration
            .iter()
            .filter(|peer| peer.role == ReplicaRole::Primary && peer.up && peer.ready);
        if let Some(primary) = primaries.next() {
            if primaries.next().is_some() {
                return Err(Error::Protocol("ambiguous native primary"));
            }
            facts.primary = Some((epoch, project_descriptor(primary)));
        }
    }
    Ok(facts)
}

#[cfg(feature = "test-hooks")]
pub type NativeBuildPause = Arc<
    std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<NativeInvocation>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;
#[cfg(feature = "test-hooks")]
pub type NativeRemovePause = Arc<
    std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<NativeInvocation>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
>;

/// Supported generation-bound lower. Public common/primary and state views
/// retain one owner; native capture, equality, once-entry and exact delayed
/// validation remain in this runtime adapter.
#[derive(Clone)]
pub struct RuntimeReplicator {
    native: NativeRuntimeIntegration,
    lower: Arc<FabricReplicator>,
    hosting: HostingEndpoint,
    _progress: Arc<dyn NativeProgressSource>,
    #[cfg(feature = "test-hooks")]
    pub native_configuration: Arc<std::sync::Mutex<Option<NativeInvocation>>>,
    #[cfg(feature = "test-hooks")]
    pub native_build_pause: NativeBuildPause,
    #[cfg(feature = "test-hooks")]
    pub native_remove_pause: NativeRemovePause,
}

impl NativeRuntimeIntegration {
    /// Construct inside the owning service Open entry. Identity and the single
    /// engine claim are checked before activating provider/listener resources.
    /// No provider tail is queried until an Opened getter or first Primary.
    pub fn create_replicator(
        &self,
        partition: Arc<dyn IStatefulServicePartition>,
        provider: Arc<dyn IStateProvider>,
        replay: Option<Arc<dyn ReplayCheckpointProvider>>,
        endpoint: ReplicationEndpointOptions,
        settings: Settings,
    ) -> Result<Arc<RuntimeReplicator>> {
        self.create_replicator_with_capabilities(
            partition,
            provider,
            replay,
            endpoint,
            settings,
            StateProviderCapabilities::default(),
        )
    }

    /// Explicit copy capability; classic construction keeps its old default.
    pub fn create_replicator_with_capabilities(
        &self,
        partition: Arc<dyn IStatefulServicePartition>,
        provider: Arc<dyn IStateProvider>,
        replay: Option<Arc<dyn ReplayCheckpointProvider>>,
        endpoint: ReplicationEndpointOptions,
        settings: Settings,
        capabilities: StateProviderCapabilities,
    ) -> Result<Arc<RuntimeReplicator>> {
        settings.validate()?;
        let information = partition.get_partition_information()?;
        self.validate()?;
        let partition_id = match information {
            ServicePartitionInformation::Singleton(info) => info.id,
            ServicePartitionInformation::Int64Range(info) => info.id,
            ServicePartitionInformation::Named(info) => info.id,
            ServicePartitionInformation::Invalid => {
                return Err(Error::InvalidInput("invalid partition information"))
            }
        };
        if partition_id != self.identity().partition_id {
            return Err(Error::InvalidInput(
                "replication actor does not match native partition",
            ));
        }
        self.execution().check_constructor()?;
        self.reserve_engine(
            partition_id,
            self.identity().replica_id,
            endpoint.actor_incarnation,
        )?;
        let (lower, hosting) = ReplicatorFactory::create(
            partition_id,
            self.identity().replica_id,
            partition,
            provider,
            replay,
            endpoint,
            settings,
            capabilities,
            HostServices {
                execution: self.execution(),
                generation: self.binding().generation,
                validity: Arc::new(Validity(self.clone())),
                membership: Arc::new(Membership(self.clone())),
                health: Arc::new(Health(self.clone())),
            },
        )?;
        let progress: Arc<dyn NativeProgressSource> =
            Arc::new(LowerProgress(Arc::downgrade(&lower)));
        if let Err(error) = self.install_progress_source(&progress) {
            lower.abort();
            return Err(error);
        }
        Ok(Arc::new(RuntimeReplicator {
            native: self.clone(),
            lower,
            hosting,
            _progress: progress,
            #[cfg(feature = "test-hooks")]
            native_configuration: Arc::default(),
            #[cfg(feature = "test-hooks")]
            native_build_pause: Arc::default(),
            #[cfg(feature = "test-hooks")]
            native_remove_pause: Arc::default(),
        }))
    }
}

impl RuntimeReplicator {
    /// Best-effort lower observation; never a control or access grant.
    pub fn snapshot(&self) -> fube_data::extensions::EngineSnapshot {
        use fube_data::extensions::ReplicatorDiagnostics;
        self.lower.snapshot()
    }
    /// Published lower endpoint observation, not an authority token.
    pub fn endpoint(&self) -> fube_data::wire::Endpoint {
        use fube_data::extensions::ReplicatorDiagnostics;
        self.lower.endpoint()
    }
    pub fn get_state_replicator(&self) -> Arc<dyn IStateReplicator> {
        self.lower.get_state_replicator()
    }
    /// Explicit non-SDK observations used by the data sample. This view does
    /// not capture or issue native controls. Transactional consumers need only
    /// the public primary/state views and separately supplied host execution.
    /// The factory's hosting endpoint is retained privately, not recoverable
    /// from this view, even with caller-authored validation and owned execution.
    ///
    /// ```compile_fail,E0599
    /// use std::sync::Arc;
    /// use fube_data::hosting::{Configuration, MutationCheck};
    /// fn configure(r: &fube_runtime::RuntimeReplicator, c: Configuration, check: Arc<dyn MutationCheck>) {
    ///     r.diagnostics().configure(c, None, None, Default::default(), check);
    /// }
    /// ```
    /// ```compile_fail,E0599
    /// use std::sync::Arc;
    /// use fube_data::hosting::{Replica, MutationCheck};
    /// async fn build(r: &fube_runtime::RuntimeReplicator, target: Replica, check: Arc<dyn MutationCheck>) {
    ///     r.diagnostics().build(target, Default::default(), check).await;
    /// }
    /// ```
    /// ```compile_fail,E0599
    /// use std::sync::Arc;
    /// use fube_data::hosting::{Replica, Removal, MutationCheck};
    /// fn remove(r: &fube_runtime::RuntimeReplicator, target: Replica, check: Arc<dyn MutationCheck>) {
    ///     r.diagnostics().remove(target, Removal::BuildRetryCleanup, Default::default(), check);
    /// }
    /// ```
    /// ```compile_fail,E0616
    /// fn recover(r: &fube_runtime::RuntimeReplicator) {
    ///     let _live_owner_endpoint = r.clone().hosting;
    /// }
    /// ```
    pub fn diagnostics(&self) -> Arc<FabricReplicator> {
        self.lower.clone()
    }

    #[cfg(test)]
    pub(crate) fn hosting_for_test(&self) -> HostingEndpoint {
        self.hosting.clone()
    }

    fn configure(
        &self,
        current: ReplicaSetConfig,
        previous: Option<ReplicaSetConfig>,
    ) -> Result<()> {
        self.native.execution().blocking_deadline()?;
        use fube_data::extensions::ReplicatorDiagnostics;
        crate::extensions::ExecutionRuntime::reject_engine_reentry(
            self.lower.execution_identity(),
        )?;
        let proof = self
            .native
            .capture_configuration(&current, previous.as_ref())?;
        let kind = if previous.is_some() {
            NativeInvocationKind::CatchUpConfiguration
        } else {
            NativeInvocationKind::CurrentConfiguration
        };
        let (cc, pc) = proof
            .configuration()
            .ok_or(Error::InvalidInput("missing frozen native configuration"))?;
        if proof.kind() != kind
            || cc.sdk()? != current
            || pc.map(|pc| pc.sdk()).transpose()? != previous
        {
            return Err(Error::InvalidInput(
                "configuration differs from frozen native projection",
            ));
        }
        let current = project_configuration(cc)?;
        let previous = pc.map(project_configuration).transpose()?;
        let prewrite = prewrite(&proof);
        let cancellation = proof.cancellation();
        proof.enter()?;
        #[cfg(feature = "test-hooks")]
        {
            *self.native_configuration.lock().unwrap() = Some(proof.clone());
        }
        self.hosting.configure(
            current,
            previous,
            prewrite,
            cancellation,
            Arc::new(ControlCheck(proof)),
        )
    }
}
impl IReplicator for RuntimeReplicator {
    fn open(&self, cancel: Cancellation) -> ApiFuture<'_, String> {
        self.lower.open(cancel)
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.lower.close(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.lower.change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.lower.update_epoch(epoch, cancel)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.lower.get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.lower.get_catch_up_capability()
    }
    fn abort(&self) {
        self.lower.abort();
    }
}
impl IPrimaryReplicator for RuntimeReplicator {
    fn on_data_loss(&self, cancel: Cancellation) -> ApiFuture<'_, bool> {
        let valid = self.native.validate();
        Box::pin(async move {
            valid?;
            self.native.validate()?;
            let result = self.lower.on_data_loss(cancel).await?;
            self.native.validate().map_err(Error::uncertain)?;
            Ok(result)
        })
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.configure(current, Some(previous))
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.configure(current, None)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        if mode == ReplicaSetQuorumMode::Invalid {
            return Box::pin(async { Err(Error::InvalidInput("invalid quorum mode")) });
        }
        let valid = self.native.validate();
        Box::pin(async move {
            valid?;
            self.native.validate()?;
            self.lower.wait_for_catch_up_quorum(mode, cancel).await?;
            self.native.validate().map_err(Error::uncertain)
        })
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        // Freeze, compare and enter NOW, even if the returned future is unpolled.
        let captured = (|| {
            let proof = self.native.capture_build(&replica)?;
            if proof.kind() != NativeInvocationKind::Build
                || proof.replica().map(|r| r.sdk()).transpose()?.as_ref() != Some(&replica)
            {
                return Err(Error::InvalidInput(
                    "build differs from frozen native target",
                ));
            }
            let target = project_replica(
                proof
                    .replica()
                    .ok_or(Error::InvalidInput("missing native build target"))?,
            )?;
            proof.enter()?;
            Ok((proof, target))
        })();
        match captured {
            Ok((proof, target)) => {
                #[cfg(feature = "test-hooks")]
                if let Some((captured, release)) = self.native_build_pause.lock().unwrap().take() {
                    let _ = captured.send(proof.clone());
                    let future = self
                        .hosting
                        .build(target, cancel, Arc::new(ControlCheck(proof)));
                    return Box::pin(async move {
                        let _ = release.await;
                        future.await
                    });
                }
                self.hosting
                    .build(target, cancel, Arc::new(ControlCheck(proof)))
            }
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
    fn remove_replica(&self, replica_id: i64) -> Result<()> {
        self.native.execution().blocking_deadline()?;
        use fube_data::extensions::ReplicatorDiagnostics;
        crate::extensions::ExecutionRuntime::reject_engine_reentry(
            self.lower.execution_identity(),
        )?;
        let proof = self.native.capture_removal(replica_id)?;
        let NativeInvocationKind::Remove(reason) = proof.kind() else {
            return Err(Error::InvalidInput("not a native removal invocation"));
        };
        let target = project_replica(
            proof
                .replica()
                .ok_or(Error::InvalidInput("missing native removal target"))?,
        )?;
        if target.replica_id != replica_id {
            return Err(Error::InvalidInput(
                "removal differs from frozen native target",
            ));
        }
        let purpose = match reason {
            ReplicaRemoval::Idle => Removal::Idle,
            ReplicaRemoval::BuildRetryCleanup => Removal::BuildRetryCleanup,
        };
        let cancellation = proof.cancellation();
        proof.enter()?;
        #[cfg(feature = "test-hooks")]
        if let Some((captured, release)) = self.native_remove_pause.lock().unwrap().take() {
            let _ = captured.send(proof.clone());
            release
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("native remove setup release");
        }
        self.hosting
            .remove(target, purpose, cancellation, Arc::new(ControlCheck(proof)))
    }
}

#[cfg(test)]
#[path = "replication_projection_tests.rs"]
mod projection_tests;
