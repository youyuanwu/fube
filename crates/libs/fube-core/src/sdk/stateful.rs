//! Owned volatile stateful/custom-replicator contracts. The native node, not
//! these objects, chooses membership. Descriptors do not mint host authority.
use crate::{ApiFuture, Cancellation, Error, Result, ServicePartitionInformation};
use std::sync::Arc;

pub const MAX_REPLICA_SET_SIZE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
/// Public SF role values; RAP's private Idle=2/Secondary=3/Primary=4 differ.
/// Auxiliary roles in newer mssf are not supported by this native profile.
pub enum ReplicaRole {
    Unknown = 0,
    None = 1,
    Primary = 2,
    IdleSecondary = 3,
    ActiveSecondary = 4,
}

/// SDK epoch values, not RAP absence: INVALID is (-1,-1), ZERO is valid (0,0).
/// Fube's primary-domain masking is an explicit hosting extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Epoch {
    pub data_loss_number: i64,
    pub configuration_number: i64,
}
impl Epoch {
    pub const INVALID: Self = Self {
        data_loss_number: -1,
        configuration_number: -1,
    };
    pub const ZERO: Self = Self {
        data_loss_number: 0,
        configuration_number: 0,
    };
}

pub trait IStatefulServiceFactory: Send + Sync + 'static {
    /// Synchronous fallible construction. UTF-8 names/UUIDs replace mssf's
    /// WString/URI/GUID representations; native incarnation is not a factory
    /// argument. Factories own their activation-context provider and snapshot.
    fn create_replica(
        &self,
        service_type: String,
        service_name: String,
        initialization_data: &[u8],
        partition_id: uuid::Uuid,
        replica_id: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>>;
}

/// Callbacks run outside protocol locks/readers. Cancellation is advisory;
/// their futures must complete only after owned work has quiesced. An ignored
/// cancellation is not a completed operation. Callback deadlines/panics are
/// terminal host faults. Abort disposes owned resources synchronously.
pub trait IStatefulServiceReplica: Send + Sync + 'static {
    /// Runtime dispatches New for this volatile profile. Existing/Invalid are
    /// rejected before activation, not advertised as recovery/persistence.
    /// Return owned primary control; application-spawned descendants remain
    /// service-owned and must be stopped/joined in Close/Abort.
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>>;
    /// Service endpoint and replicator address are independent. An empty service
    /// endpoint is permitted outside Primary; publication is not an access grant.
    fn change_role(&self, role: ReplicaRole, cancellation: Cancellation) -> ApiFuture<'_, String>;
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn abort(&self);
}

/// Custom-replicator control seam. There is deliberately no default all-success
/// implementation and no application write, storage or built-in-replicator API.
pub trait IReplicator: Send + Sync + 'static {
    /// Async listener-ready control address, independent of the service endpoint.
    fn open(&self, cancellation: Cancellation) -> ApiFuture<'_, String>;
    /// Async native-selected epoch/role barrier, not a peer election.
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Preserve full public epoch bits; masking is explicit integration conversion.
    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// A primary reports quorum progress, a secondary received progress, not
    /// universal durability.
    fn get_current_progress(&self) -> Result<i64>;
    /// Local eligible retained-history capability, not a normalization of
    /// unknown remote LSN (-1). Native built-in publication samples both values
    /// coherently through private integration, not two public getter calls.
    fn get_catch_up_capability(&self) -> Result<i64>;
    /// Async terminal cleanup of owned control resources.
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    /// Synchronously dispose owned control resources before returning.
    fn abort(&self);
}

/// Primary-only canonical controls. The built-in implementation requires real
/// runtime/partition-issued authority: SDK descriptors and diagnostics cannot
/// construct it. Root and invocation-bound controls are functional views over
/// the same owner, not a separate non-SDK success path.
pub trait IPrimaryReplicator: IReplicator {
    /// Return whether application state changed, NOT whether the native node
    /// advanced its data-loss epoch.
    fn on_data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool>;
    /// Synchronous completed owner installation and required physical disposal,
    /// never enqueue success. Built-in native-bound root and bound views use
    /// immutable runtime-issued membership/epoch/identity records outside SDK
    /// descriptors. Invoke on a reserved blocking entry; no executor block_on.
    /// Post-admission timeout/revocation is uncertain, not rollback.
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()>;
    /// Same synchronous applied/disposed boundary as catch-up configuration,
    /// with no previous configuration. Public arrays do not carry local votes,
    /// node/incarnation or epoch proofs; these remain privately native-issued.
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()>;
    /// Write and All/required-target obligations differ; a diagnostic progress
    /// observation cannot acknowledge new writes.
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// Freeze native identity/revision at this call, before returning the future.
    /// Async Build completion includes required copy/application acknowledgment;
    /// cancellation of an invocation does not dispose an established peer.
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()>;
    /// ID-only synchronous completion, including required physical disposal.
    /// Native incarnation and retry-versus-final purpose are captured privately,
    /// never resolved to a mutable latest ID after waiting. Ordinary root
    /// removal is final; native retry cleanup uses the same canonical method
    /// with its separately issued invocation-bound purpose.
    fn remove_replica(&self, replica_id: i64) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSetQuorumMode {
    Invalid,
    Write,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Explicit open intent. New is supported; Existing is Unsupported and Invalid
/// is InvalidArgument before activation. Corrects reference mssf `Invald`
/// spelling without an alias; this does not add persistence/recovery.
pub enum OpenMode {
    Invalid,
    New,
    Existing,
}
impl OpenMode {
    pub fn validate(self) -> Result<()> {
        match self {
            Self::New => Ok(()),
            Self::Existing => Err(Error::new(
                crate::ErrorKind::Unsupported,
                "Existing open is unavailable in the volatile profile",
            )),
            Self::Invalid => Err(Error::InvalidInput("invalid open mode")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// SDK descriptor liveness, distinct from host diagnostics.
pub enum ReplicaStatus {
    Invalid,
    Down,
    Up,
}
/// SDK observations only. Native identity/incarnation and authority live outside
/// this descriptor. Remote unknown progress (-1) is not initialized local zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaInformation {
    pub id: i64,
    pub role: ReplicaRole,
    pub status: ReplicaStatus,
    pub replicator_address: String,
    pub current_progress: i64,
    pub catch_up_capability: i64,
    pub must_catch_up: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
/// SDK remote descriptors/write quorum, not an authority envelope. Missing or
/// down peers never shrink native current/previous quorum obligations.
pub struct ReplicaSetConfig {
    pub replicas: Vec<ReplicaInformation>,
    pub write_quorum: u32,
}

/// Object-safe information/access and synchronous reports. Reports require a
/// supported reserved execution entry; do not await their Result or block an
/// async worker. Information/access getters are immediate fallible observations.
/// Native CreateReplicator and mssf try_get_com have no member here: neither COM
/// proxy nor runtime-owned native replication is supported by this SDK trait.
pub trait IStatefulServicePartition: Send + Sync + 'static {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation>;
    /// Check current read access per request; cached Granted is not authority.
    fn get_read_status(&self) -> Result<ServicePartitionAccessStatus>;
    /// Independent write status; may change before a subsequent replicate call.
    fn get_write_status(&self) -> Result<ServicePartitionAccessStatus>;
    /// Local frame-write completion, not RA/FM acknowledgment. Native-bound
    /// current configuration role selects primary or secondary/node arrays.
    /// This does not grant application read/write access. Unknown/None are
    /// stricter profile errors, not native FUP admission-result parity.
    fn report_load(&self, metrics: &[crate::LoadMetric]) -> Result<()>;
    /// Initial immutable local-write/coalescing result, not native recovery ACK.
    /// Later bounded retries and observations remain explicit runtime behavior.
    fn report_fault(&self, fault: crate::FaultType) -> Result<()>;
    /// Load-route local write with the same private configuration-role snapshot.
    /// Supports Zero/Low/Medium/High, not version-skewed VeryHigh.
    fn report_move_cost(&self, cost: crate::MoveCost) -> Result<()>;
    /// Partition-only native entity (kind/tag 3); one exact HM result, not fresh
    /// durability or SDK queue/batching admission. No hidden health retry.
    fn report_partition_health(&self, health: &crate::HealthInformation) -> Result<()>;
    /// Native kind/tag 1, public replica ID, invalid replica-instance -1.
    /// Same exact-result completion and uncertain timeout as partition health.
    fn report_replica_health(&self, health: &crate::HealthInformation) -> Result<()>;
    /// Both Immediate values directly send; true adds no reporting-interval
    /// delay but does not mean HM success or remote durability.
    fn report_partition_health2(
        &self,
        health: &crate::HealthInformation,
        options: Option<&crate::HealthReportSendOption>,
    ) -> Result<()>;
    /// Replica-health variant; optional borrowed send options follow health.
    /// Immediate false and true both direct-send, with one matched HM result.
    fn report_replica_health2(
        &self,
        health: &crate::HealthInformation,
        options: Option<&crate::HealthReportSendOption>,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ServicePartitionAccessStatus {
    Invalid = 0,
    Granted = 1,
    ReconfigurationPending = 2,
    NotPrimary = 3,
    NoWriteQuorum = 4,
}
impl ServicePartitionAccessStatus {
    /// Public FABRIC_SERVICE_PARTITION_ACCESS_STATUS, not RAP's internal enum.
    pub fn native_value(self) -> i64 {
        match self {
            Self::Invalid => 0,
            Self::Granted => 1,
            Self::ReconfigurationPending => 2,
            Self::NotPrimary => 3,
            Self::NoWriteQuorum => 4,
        }
    }
}
