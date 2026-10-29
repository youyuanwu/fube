//! Runtime-owned native authority, separate from SDK traits and diagnostics.
use super::*;
use std::sync::Weak;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaBinding {
    pub identity: ReplicaIdentity,
    pub generation: u64,
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
fn native_role(role: ReplicaRole) -> i64 {
    match role {
        ReplicaRole::Unknown => 0,
        ReplicaRole::None => 1,
        ReplicaRole::Idle => 2,
        ReplicaRole::Secondary => 3,
        ReplicaRole::Primary => 4,
    }
}
impl From<&Epoch> for NativeEpoch {
    fn from(epoch: &Epoch) -> Self {
        Self {
            data_loss: epoch.data_loss,
            configuration: epoch.configuration,
        }
    }
}
impl From<&ReplicaInformation> for NativeReplica {
    fn from(replica: &ReplicaInformation) -> Self {
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
impl From<&ReplicaSetConfiguration> for NativeReplicaSet {
    fn from(config: &ReplicaSetConfiguration) -> Self {
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
    fn bind_invocation(&self, invocation: NativeInvocation) -> Result<Arc<dyn Replicator>>;
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
    ledger: Mutex<Vec<Arc<Issued>>>,
}
/// Explicit non-SDK construction/integration capability. Only Runtime issues it.
#[derive(Clone)]
pub struct NativeRuntimeIntegration(Arc<NativeUnit>);
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
#[derive(Clone)]
pub struct NativeInvocation {
    unit: Arc<NativeUnit>,
    issued: Arc<Issued>,
    entered: Arc<AtomicBool>,
}
impl NativeInvocation {
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
            NativeEpoch::from(&api_epoch(&self.issued.request.unit.current)),
            NativeEpoch::from(&api_epoch(&self.issued.request.unit.previous)),
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
    pub(super) fn retire_ledger(&self) {
        self.0.ledger.lock().unwrap().clear();
        self.0.binder.lock().unwrap().take();
    }
    pub fn from_partition(partition: &StatefulPartition) -> Result<Self> {
        partition.ensure_valid()?;
        partition
            .native
            .clone()
            .ok_or(Error::InvalidInput("partition has no native issuer"))
    }
    pub fn binding(&self) -> &ReplicaBinding {
        &self.0.binding
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
        if current.as_ref().and_then(Weak::upgrade).is_some() {
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
        let ledger = self.0.ledger.lock().unwrap();
        let mut candidates = ledger.iter().filter(|issued| {
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
        });
        let selected = candidates
            .next()
            .ok_or(Error::InvalidInput("native operation is not authorized"))?
            .clone();
        if candidates.any(|other| other.replica != selected.replica) {
            return Err(Error::InvalidInput("ambiguous native replica authority"));
        }
        drop(ledger);
        let invocation = NativeInvocation {
            unit: self.0.clone(),
            issued: selected,
            entered: Arc::new(AtomicBool::new(false)),
        };
        invocation.validate()?;
        Ok(invocation)
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
            ledger: Mutex::new(Vec::new()),
        }))
    }
}
pub(super) fn issue(
    unit: &Unit,
    request: &w::ProxyRequest,
    kind: NativeInvocationKind,
    current: Option<ReplicaSetConfiguration>,
    previous: Option<ReplicaSetConfiguration>,
    replica: Option<ReplicaInformation>,
    revision: (Arc<AtomicU64>, u64),
    remote: Option<([u8; 16], u64)>,
    cancellation: Cancellation,
) -> Result<NativeInvocation> {
    let state = unit.state.lock().unwrap();
    let native = state.native.as_ref().ok_or(Error::Closed)?;
    let status = state.status.lock().unwrap();
    let prewrite = (status.lifecycle == ReplicaLifecycle::Open
        && status.replicator_open
        && status.current_role == ReplicaRole::Secondary
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
    fallback: Arc<dyn Replicator>,
) -> Result<Arc<dyn Replicator>> {
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
