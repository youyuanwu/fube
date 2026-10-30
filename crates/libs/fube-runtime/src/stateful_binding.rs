//! Runtime-owned native authority, separate from SDK traits and diagnostics.
use super::*;
use std::sync::Weak;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaBinding {
    pub identity: ReplicaIdentity,
    pub generation: u64,
}
/// Optional private-integration progress sampling for a built-in engine. It
/// grants no authority and is not an SDK discovery/control member.
pub trait NativeProgressSource: Send + Sync {
    fn sample_progress(&self) -> Result<ReplicationProgress>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeInvocationKind {
    CatchUpConfiguration,
    CurrentConfiguration,
    Build,
    Remove(ReplicaRemoval),
}
/// Read-only native projection facts, not SDK descriptors or authority grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeEpoch {
    pub data_loss: i64,
    pub configuration: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeReplica {
    pub node_id: [u8; 16],
    pub node_instance: u64,
    pub replica_id: i64,
    pub incarnation: i64,
    pub role: i64,
    pub previous_role: i64,
    pub up: bool,
    pub ready: bool,
    pub address: String,
    pub first_lsn: i64,
    pub last_lsn: i64,
    pub must_catch_up: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeReplicaSet {
    pub epoch: NativeEpoch,
    pub replicas: Vec<NativeReplica>,
    pub write_quorum: usize,
    pub local_member: bool,
}
impl NativeReplica {
    pub fn descriptor(&self) -> Result<NativeReplicaDescriptor> {
        Ok(NativeReplicaDescriptor {
            node_id: self.node_id,
            node_instance: self.node_instance,
            replica_id: self.replica_id,
            incarnation: self.incarnation,
            role: role(self.role)?,
            previous_role: role(self.previous_role)?,
            up: self.up,
            ready: self.ready,
            address: self.address.clone(),
            progress: ReplicationProgress {
                first: self.first_lsn,
                last: self.last_lsn,
            },
            must_catch_up: self.must_catch_up,
        })
    }
    pub fn sdk(&self) -> Result<crate::ReplicaInformation> {
        Ok(self.descriptor()?.sdk())
    }
}
impl NativeReplicaSet {
    pub fn descriptor(&self) -> Result<NativeReplicaSetConfiguration> {
        Ok(NativeReplicaSetConfiguration {
            epoch: Epoch {
                data_loss_number: self.epoch.data_loss,
                configuration_number: self.epoch.configuration,
            },
            replicas: self
                .replicas
                .iter()
                .map(NativeReplica::descriptor)
                .collect::<Result<_>>()?,
            write_quorum: self.write_quorum,
            local_member: self.local_member,
        })
    }
    pub fn sdk(&self) -> Result<crate::ReplicaSetConfig> {
        self.descriptor()?.sdk()
    }
}
fn native_role(role: ReplicaRole) -> i64 {
    match role {
        ReplicaRole::Unknown => 0,
        ReplicaRole::None => 1,
        ReplicaRole::IdleSecondary => 2,
        ReplicaRole::ActiveSecondary => 3,
        ReplicaRole::Primary => 4,
    }
}
impl From<&Epoch> for NativeEpoch {
    fn from(epoch: &Epoch) -> Self {
        Self {
            data_loss: epoch.data_loss_number,
            configuration: epoch.configuration_number,
        }
    }
}
impl From<&NativeReplicaDescriptor> for NativeReplica {
    fn from(replica: &NativeReplicaDescriptor) -> Self {
        Self {
            node_id: replica.node_id,
            node_instance: replica.node_instance,
            replica_id: replica.replica_id,
            incarnation: replica.incarnation,
            role: native_role(replica.role),
            previous_role: native_role(replica.previous_role),
            up: replica.up,
            ready: replica.ready,
            address: replica.address.clone(),
            first_lsn: replica.progress.first,
            last_lsn: replica.progress.last,
            must_catch_up: replica.must_catch_up,
        }
    }
}
impl From<&NativeReplicaSetConfiguration> for NativeReplicaSet {
    fn from(config: &NativeReplicaSetConfiguration) -> Self {
        Self {
            epoch: NativeEpoch::from(&config.epoch),
            replicas: config.replicas.iter().map(NativeReplica::from).collect(),
            write_quorum: config.write_quorum,
            local_member: config.local_member,
        }
    }
}
/// Binding only, never an alternative configuration/build/removal interface.
pub trait NativeControlBinding: Send + Sync {
    fn bind_invocation(&self, invocation: NativeInvocation) -> Result<Arc<dyn IPrimaryReplicator>>;
}
struct NativeUnit {
    binding: ReplicaBinding,
    state: Weak<Mutex<State>>,
    valid: Arc<AtomicBool>,
    authority: Arc<Authority>,
    execution: crate::extensions::ExecutionRuntime,
    engine_claimed: AtomicBool,
    actor: Mutex<Option<uuid::Uuid>>,
    binder: Mutex<Option<Weak<dyn NativeControlBinding>>>,
    progress: Mutex<Option<Weak<dyn NativeProgressSource>>>,
    ledger: Mutex<Vec<Arc<Issued>>>,
}
/// Explicit non-SDK construction/integration capability. Only Runtime issues it.
/// Private issued state cannot be constructed from public SDK descriptors or
/// editable diagnostics. Retention does not prevent generation/revision/lease
/// revocation. Trusted clear IPC is not cryptographic host authentication or a
/// claim of global unforgeability.
#[derive(Clone)]
pub struct NativeRuntimeIntegration(Arc<NativeUnit>);
/// Factory-owned explicit integration lookup. This is not a partition/SDK
/// discovery member. Capture it synchronously during canonical creation.
#[derive(Clone)]
pub struct NativeIntegrationProvider(
    Arc<dyn Fn(uuid::Uuid, i64) -> Result<NativeRuntimeIntegration> + Send + Sync>,
);
impl NativeIntegrationProvider {
    pub(crate) fn new(
        lookup: impl Fn(uuid::Uuid, i64) -> Result<NativeRuntimeIntegration> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(lookup))
    }
    pub fn capture(
        &self,
        partition: uuid::Uuid,
        replica_id: i64,
    ) -> Result<NativeRuntimeIntegration> {
        (self.0)(partition, replica_id)
    }
}
struct Issued {
    kind: NativeInvocationKind,
    request: w::ProxyRequest,
    current: Option<NativeReplicaSet>,
    previous: Option<NativeReplicaSet>,
    replica: Option<NativeReplica>,
    revision: Arc<AtomicU64>,
    expected: u64,
    remote: Option<([u8; 16], u64)>,
    lifetime: Cancellation,
    cancellation: Cancellation,
    prewrite: Option<(NativeEpoch, NativeEpoch)>,
    actor: Option<uuid::Uuid>,
}
/// Opaque immutable issued proof. SDK values or diagnostic snapshots cannot mint it.
/// Every use revalidates native lifetime, generation, revision and actor identity;
/// a retained proof can be revoked. No issuer-less primary-control support.
#[derive(Clone)]
pub struct NativeInvocation {
    unit: Arc<NativeUnit>,
    issued: Arc<Issued>,
    entered: Arc<AtomicBool>,
}
impl NativeInvocation {
    pub fn cancellation(&self) -> Cancellation {
        self.issued.cancellation.clone()
    }
    pub fn kind(&self) -> NativeInvocationKind {
        self.issued.kind
    }
    pub fn binding(&self) -> &ReplicaBinding {
        &self.unit.binding
    }
    pub fn configuration(&self) -> Option<(&NativeReplicaSet, Option<&NativeReplicaSet>)> {
        self.issued
            .current
            .as_ref()
            .map(|cc| (cc, self.issued.previous.as_ref()))
    }
    pub fn replica(&self) -> Option<&NativeReplica> {
        self.issued.replica.as_ref()
    }
    pub fn prewrite_epochs(&self) -> Option<(NativeEpoch, NativeEpoch)> {
        self.issued.prewrite.clone()
    }
    pub fn actor(&self) -> Option<uuid::Uuid> {
        self.issued.actor
    }
    pub fn full_epochs(&self) -> (NativeEpoch, NativeEpoch) {
        (
            NativeEpoch {
                data_loss: self.issued.request.unit.current.data_loss,
                configuration: self.issued.request.unit.current.configuration,
            },
            NativeEpoch {
                data_loss: self.issued.request.unit.previous.data_loss,
                configuration: self.issued.request.unit.previous.configuration,
            },
        )
    }
    pub fn validate(&self) -> Result<()> {
        self.unit.authority.check()?;
        if !self.unit.valid.load(Ordering::Acquire)
            || self.issued.lifetime.is_cancelled()
            || self.issued.cancellation.is_cancelled()
            || self.issued.revision.load(Ordering::Acquire) != self.issued.expected
        {
            return Err(Error::Closed);
        }
        let state = self.unit.state.upgrade().ok_or(Error::Closed)?;
        let state = state.lock().unwrap();
        if state.generation != self.binding().generation {
            return Err(Error::Closed);
        }
        if *self.unit.actor.lock().unwrap() != self.issued.actor {
            return Err(Error::Closed);
        }
        if let Some((node, sequence)) = self.issued.remote {
            if !state
                .remotes
                .get(&node)
                .is_some_and(|remote| remote.sequence == sequence)
            {
                return Err(Error::Closed);
            }
        }
        Ok(())
    }
    pub fn enter(&self) -> Result<()> {
        self.validate()?;
        self.entered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::InvalidInput("native invocation already consumed"))?;
        Ok(())
    }
}
impl NativeRuntimeIntegration {
    /// One generation-bound report record, associated before emission. This is
    /// a fube observation extension, not SDK admission or remote durability.
    pub fn observe_report(
        &self,
    ) -> Result<(
        Arc<dyn IStatefulServicePartition>,
        crate::extensions::ReportingObservation,
    )> {
        self.validate()?;
        let state = self.0.state.upgrade().ok_or(Error::Closed)?;
        let state = state.lock().unwrap();
        let kind = match state.request.unit.consistency.kind {
            1 => PartitionKind::Singleton,
            2 => PartitionKind::Int64Range {
                low: state.request.unit.consistency.low,
                high: state.request.unit.consistency.high,
            },
            3 => PartitionKind::Named(state.request.unit.consistency.name.clone()),
            _ => return Err(Error::Protocol("invalid native partition kind")),
        };
        Ok(crate::instance_integration::observe_stateful(
            StatefulPartition {
                identity: self.identity().clone(),
                kind,
                valid: self.0.valid.clone(),
                authority: self.0.authority.clone(),
                status: state.status.clone(),
                reporting: state.reporting.clone(),
            },
        ))
    }
    pub fn install_progress_source(&self, source: &Arc<dyn NativeProgressSource>) -> Result<()> {
        self.execution().check_constructor()?;
        self.validate()?;
        let mut current = self.0.progress.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|source| source.strong_count() != 0)
        {
            return Err(Error::new(
                crate::ErrorKind::InvalidState,
                "native progress source already installed",
            ));
        }
        *current = Some(Arc::downgrade(source));
        Ok(())
    }
    pub(crate) fn progress_source(&self) -> Result<Option<Arc<dyn NativeProgressSource>>> {
        self.validate()?;
        let source = self
            .0
            .progress
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade);
        self.validate()?;
        Ok(source)
    }
    #[cfg(test)]
    pub(crate) fn reporting_for_test(&self) -> Arc<crate::reporting::Reporting> {
        self.0
            .state
            .upgrade()
            .unwrap()
            .lock()
            .unwrap()
            .reporting
            .clone()
            .unwrap()
    }
    pub fn identity(&self) -> &ReplicaIdentity {
        &self.0.binding.identity
    }
    pub(super) fn retire_ledger(&self) {
        self.0.ledger.lock().unwrap().clear();
        self.0.binder.lock().unwrap().take();
        self.0.progress.lock().unwrap().take();
    }
    pub fn binding(&self) -> &ReplicaBinding {
        &self.0.binding
    }
    pub fn status(&self) -> Result<ReplicaDiagnostics> {
        self.validate()?;
        let state = self.0.state.upgrade().ok_or(Error::Closed)?;
        let status = state.lock().unwrap().status.lock().unwrap().clone();
        self.validate()?;
        Ok(status)
    }
    pub fn access_status(
        &self,
    ) -> Result<(ServicePartitionAccessStatus, ServicePartitionAccessStatus)> {
        Ok(self.status()?.access())
    }
    pub fn authority_remaining(&self) -> Result<Duration> {
        self.validate()?;
        self.0.authority.remaining()
    }
    pub fn supervise_owned_callback(&self, timeout: Duration) -> Result<StatefulCallbackGuard> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(Error::InvalidInput(
                "owned callback deadline must be >0 and <=30 seconds",
            ));
        }
        Ok(StatefulCallbackGuard {
            _deadline: self.0.authority.callback_deadline(timeout, true)?,
        })
    }
    pub fn fault_delivery(&self) -> Option<crate::FaultDelivery> {
        self.0
            .state
            .upgrade()?
            .lock()
            .unwrap()
            .reporting
            .as_ref()?
            .fault_status()
    }
    pub fn report_fault(
        &self,
        fault: crate::FaultType,
    ) -> crate::ApiFuture<'_, crate::FaultDelivery> {
        Box::pin(async move {
            self.validate()?;
            let reporting = self
                .0
                .state
                .upgrade()
                .ok_or(Error::Closed)?
                .lock()
                .unwrap()
                .reporting
                .clone()
                .ok_or(Error::Closed)?;
            reporting.fault(fault).await
        })
    }
    pub fn capture_configuration(
        &self,
        current: &crate::ReplicaSetConfig,
        previous: Option<&crate::ReplicaSetConfig>,
    ) -> Result<NativeInvocation> {
        let kind = if previous.is_some() {
            NativeInvocationKind::CatchUpConfiguration
        } else {
            NativeInvocationKind::CurrentConfiguration
        };
        let invocation = self.lookup_record(kind, None, None)?;
        let (cc, pc) = invocation
            .configuration()
            .ok_or(Error::InvalidInput("missing native configuration"))?;
        if cc.sdk()? != *current || pc.map(NativeReplicaSet::sdk).transpose()?.as_ref() != previous
        {
            return Err(Error::InvalidInput(
                "configuration differs from native projection",
            ));
        }
        Ok(invocation)
    }
    /// Captures the exact native target before an async Build future is returned.
    pub fn capture_build(&self, replica: &crate::ReplicaInformation) -> Result<NativeInvocation> {
        let invocation = self.lookup_record(NativeInvocationKind::Build, Some(replica.id), None)?;
        if invocation
            .replica()
            .map(NativeReplica::sdk)
            .transpose()?
            .as_ref()
            != Some(replica)
        {
            return Err(Error::InvalidInput("build differs from native projection"));
        }
        Ok(invocation)
    }
    pub fn capture_removal(&self, replica_id: i64) -> Result<NativeInvocation> {
        self.lookup_record(
            NativeInvocationKind::Remove(ReplicaRemoval::Idle),
            Some(replica_id),
            None,
        )
    }
    /// Native-aware custom controls may observe the currently issued cleanup
    /// purpose. This does not select a new target or authorize a later retry.
    pub fn capture_pending_removal(&self, replica_id: i64) -> Result<NativeInvocation> {
        let ledger = self.0.ledger.lock().unwrap().clone();
        let mut valid = Vec::new();
        for issued in ledger.into_iter().rev() {
            if matches!(issued.kind, NativeInvocationKind::Remove(_))
                && issued
                    .replica
                    .as_ref()
                    .is_some_and(|r| r.replica_id == replica_id)
            {
                let invocation = NativeInvocation {
                    unit: self.0.clone(),
                    issued,
                    entered: Arc::new(AtomicBool::new(false)),
                };
                if invocation.validate().is_ok() {
                    valid.push(invocation);
                }
            }
        }
        let selected = valid
            .first()
            .ok_or(Error::InvalidInput("native removal is not authorized"))?
            .clone();
        if valid
            .iter()
            .any(|other| other.replica() != selected.replica())
        {
            return Err(Error::InvalidInput("ambiguous native removal"));
        }
        Ok(selected)
    }
    pub fn execution(&self) -> crate::extensions::ExecutionRuntime {
        self.0.execution.clone()
    }
    pub fn validate(&self) -> Result<()> {
        self.0.authority.check()?;
        if !self.0.valid.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let state = self.0.state.upgrade().ok_or(Error::Closed)?;
        if state.lock().unwrap().generation != self.0.binding.generation {
            return Err(Error::Closed);
        }
        Ok(())
    }
    pub fn reserve_engine(
        &self,
        partition: uuid::Uuid,
        replica: i64,
        actor: uuid::Uuid,
    ) -> Result<()> {
        self.validate()?;
        if partition != self.binding().identity.partition_id
            || replica != self.binding().identity.replica_id
        {
            return Err(Error::InvalidInput("engine does not match native binding"));
        }
        self.0
            .engine_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::InvalidInput("native unit already owns an engine"))?;
        *self.0.actor.lock().unwrap() = Some(actor);
        self.validate()
    }
    pub fn install_binding(
        &self,
        actor: uuid::Uuid,
        binder: &Arc<dyn NativeControlBinding>,
    ) -> Result<()> {
        self.validate()?;
        if *self.0.actor.lock().unwrap() != Some(actor) {
            return Err(Error::InvalidInput("native control actor mismatch"));
        }
        let mut current = self.0.binder.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|binding| binding.strong_count() != 0)
        {
            return Err(Error::InvalidInput("native binding already installed"));
        }
        *current = Some(Arc::downgrade(binder));
        Ok(())
    }
    /// Private immutable ledger lookup for coordinated canonical root migration.
    /// It does not acquire a native logical gate or constitute a public control.
    pub(crate) fn lookup_record(
        &self,
        kind: NativeInvocationKind,
        replica_id: Option<i64>,
        node: Option<[u8; 16]>,
    ) -> Result<NativeInvocation> {
        self.validate()?;
        // Never hold the ledger while validating against State: issue uses
        // State -> ledger order. Revoked records cannot shadow a replacement.
        let ledger = self.0.ledger.lock().unwrap().clone();
        let mut candidates = ledger
            .iter()
            .filter(|issued| {
                if issued.kind != kind {
                    return false;
                }
                if node.is_some_and(|node| {
                    !issued
                        .replica
                        .as_ref()
                        .is_some_and(|replica| replica.node_id == node)
                }) {
                    return false;
                }
                match kind {
                    NativeInvocationKind::CurrentConfiguration
                    | NativeInvocationKind::CatchUpConfiguration => issued.kind == kind,
                    NativeInvocationKind::Build => {
                        issued.kind == kind
                            && issued
                                .replica
                                .as_ref()
                                .is_some_and(|replica| Some(replica.replica_id) == replica_id)
                    }
                    NativeInvocationKind::Remove(_) => issued
                        .replica
                        .as_ref()
                        .is_some_and(|replica| Some(replica.replica_id) == replica_id),
                }
            })
            .filter_map(|issued| {
                let invocation = NativeInvocation {
                    unit: self.0.clone(),
                    issued: issued.clone(),
                    entered: Arc::new(AtomicBool::new(false)),
                };
                invocation.validate().ok().map(|()| invocation)
            });
        let selected = candidates
            .next()
            .ok_or(Error::InvalidInput("native operation is not authorized"))?;
        if candidates.any(|other| other.replica() != selected.replica()) {
            return Err(Error::InvalidInput("ambiguous native replica authority"));
        }
        selected.validate()?;
        Ok(selected)
    }
    pub(super) fn new(
        unit: &Unit,
        identity: ReplicaIdentity,
        generation: u64,
        valid: Arc<AtomicBool>,
        authority: Arc<Authority>,
        execution: crate::extensions::ExecutionRuntime,
    ) -> Self {
        Self(Arc::new(NativeUnit {
            binding: ReplicaBinding {
                identity,
                generation,
            },
            state: Arc::downgrade(&unit.state),
            valid,
            authority,
            execution,
            engine_claimed: AtomicBool::new(false),
            actor: Mutex::new(None),
            binder: Mutex::new(None),
            progress: Mutex::new(None),
            ledger: Mutex::new(Vec::new()),
        }))
    }
}
pub(super) struct IssueInput {
    pub kind: NativeInvocationKind,
    pub current: Option<NativeReplicaSetConfiguration>,
    pub previous: Option<NativeReplicaSetConfiguration>,
    pub replica: Option<NativeReplicaDescriptor>,
    pub revision: (Arc<AtomicU64>, u64),
    pub remote: Option<([u8; 16], u64)>,
    pub cancellation: Cancellation,
}
pub(super) fn issue(
    unit: &Unit,
    request: &w::ProxyRequest,
    input: IssueInput,
) -> Result<NativeInvocation> {
    let IssueInput {
        kind,
        current,
        previous,
        replica,
        revision,
        remote,
        cancellation,
    } = input;
    let state = unit.state.lock().unwrap();
    let native = state.native.as_ref().ok_or(Error::Closed)?;
    let status = state.status.lock().unwrap();
    let prewrite = (status.lifecycle == ReplicaLifecycle::Open
        && status.replicator_open
        && status.current_role == ReplicaRole::ActiveSecondary
        && status.service_role == ReplicaRole::Primary
        && status.replicator_role == ReplicaRole::Primary
        && status.configuration_stage == ConfigurationStage::PreWriteRevoke)
        .then_some((
            NativeEpoch::from(&status.current_epoch.primary()),
            NativeEpoch::from(&status.previous_epoch.primary()),
        ));
    let issued = Arc::new(Issued {
        kind,
        request: request.clone(),
        current: current.as_ref().map(NativeReplicaSet::from),
        previous: previous.as_ref().map(NativeReplicaSet::from),
        replica: replica.as_ref().map(NativeReplica::from),
        revision: revision.0,
        expected: revision.1,
        remote,
        lifetime: state.cancel.clone(),
        cancellation,
        prewrite,
        actor: *native.0.actor.lock().unwrap(),
    });
    // At most the two configuration forms plus the bounded native remote ledger.
    let mut ledger = native.0.ledger.lock().unwrap();
    ledger.retain(|old| {
        !(old.kind == kind
            && old.replica.as_ref().map(|r| (r.node_id, r.replica_id))
                == issued.replica.as_ref().map(|r| (r.node_id, r.replica_id)))
    });
    if ledger.len() >= 2 + 3 * 256 {
        return Err(Error::ResourceLimit("native invocation ledger"));
    }
    ledger.push(issued.clone());
    Ok(NativeInvocation {
        unit: native.0.clone(),
        issued,
        entered: Arc::new(AtomicBool::new(false)),
    })
}
pub(super) fn bind(
    invocation: NativeInvocation,
    fallback: Arc<dyn IPrimaryReplicator>,
) -> Result<Arc<dyn IPrimaryReplicator>> {
    invocation.validate()?;
    let integration = NativeRuntimeIntegration(invocation.unit.clone());
    let selected = integration.lookup_record(
        invocation.kind(),
        invocation.replica().map(|replica| replica.replica_id),
        invocation.replica().map(|replica| replica.node_id),
    )?;
    if !Arc::ptr_eq(&selected.issued, &invocation.issued) {
        return Err(Error::Closed);
    }
    let binder = invocation
        .unit
        .binder
        .lock()
        .unwrap()
        .as_ref()
        .and_then(Weak::upgrade);
    match binder {
        Some(binder) => binder.bind_invocation(invocation),
        None => Ok(fallback),
    }
}
