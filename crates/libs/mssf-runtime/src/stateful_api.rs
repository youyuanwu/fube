//! Owned volatile stateful/custom-replicator contract.
//!
//! These interfaces describe lifecycle and control operations, not a storage or
//! application-write API. A custom replicator must justify its build/catch-up
//! results; returning success merely because the application is empty is not a
//! replication policy. The native node, not these objects, chooses membership.
use crate::{ActivationContext, Cancellation, PartitionKind, ServiceFuture};
use mssf_transport::{Error, Result};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

/// Bound on native-selected configuration descriptors admitted by this profile.
pub const MAX_REPLICA_SET_SIZE: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIdentity {
    pub service_name: String,
    pub service_type: String,
    pub partition_id: uuid::Uuid,
    pub replica_id: i64,
    pub incarnation: i64,
    pub node_id: [u8; 16],
    pub node_instance: u64,
}

#[derive(Debug, Clone)]
pub struct StatefulServiceContext {
    pub identity: ReplicaIdentity,
    pub initialization_data: Vec<u8>,
    pub activation: ActivationContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    Unknown,
    None,
    Idle,
    Secondary,
    Primary,
}

/// Full native epoch. Replicator operations use [`Epoch::primary`]; membership
/// changes in the low configuration bits are not primary changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Epoch {
    pub data_loss: i64,
    pub configuration: i64,
}
impl Epoch {
    pub const INVALID: Self = Self {
        data_loss: 0,
        configuration: 0,
    };
    pub fn primary(self) -> Self {
        Self {
            data_loss: self.data_loss,
            configuration: self.configuration & !0xffff_ffff,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaInformation {
    pub node_id: [u8; 16],
    pub node_instance: u64,
    pub replica_id: i64,
    pub incarnation: i64,
    pub role: ReplicaRole,
    pub previous_role: ReplicaRole,
    pub up: bool,
    pub ready: bool,
    /// Opaque custom-replicator/control address, NEVER the client service address.
    pub address: String,
    pub progress: ReplicationProgress,
    /// Native selected swap target, not a peer's self-election.
    pub must_catch_up: bool,
}

/// Initialized native control progress, independent of epoch/callback counts.
/// `first` is the catch-up/replay capability; zero can mean no eligible retained
/// completed history even when `last` is nonzero. `last` is the replicator's
/// role-specific current progress. Permanently empty state reports (0,0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationProgress {
    pub first: i64,
    pub last: i64,
}
impl ReplicationProgress {
    pub const EMPTY: Self = Self { first: 0, last: 0 };
    pub fn ensure_initialized(self) -> Result<()> {
        if self.first < 0 || self.last < self.first {
            Err(Error::Protocol(
                "invalid/uninitialized replication progress",
            ))
        } else {
            Ok(())
        }
    }
}

/// Remote up/Ready members projected by RAP. Local is excluded from the array;
/// count its vote only when `local_member` is true. A quorum is not clamped to
/// the available remote array length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSetConfiguration {
    pub epoch: Epoch,
    pub replicas: Vec<ReplicaInformation>,
    pub write_quorum: usize,
    pub local_member: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpMode {
    WriteQuorum,
    All,
}

pub trait StatefulFactory: Send + Sync + 'static {
    fn create(
        &self,
        context: StatefulServiceContext,
    ) -> ServiceFuture<'_, Arc<dyn StatefulService>>;
}

/// Callbacks run outside protocol locks/readers. Cancellation is advisory;
/// their futures must complete only after owned work has quiesced. An ignored
/// cancellation is not a completed operation. Callback deadlines/panics are
/// terminal host faults. Abort disposes owned resources synchronously.
pub trait StatefulService: Send + Sync + 'static {
    /// New volatile lifetime only. Return an owned, non-null custom replicator.
    fn open(
        &self,
        partition: StatefulPartition,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Arc<dyn Replicator>>;
    /// Service endpoint and replicator address are independent. An empty service
    /// endpoint is permitted outside Primary; publication is not an access grant.
    fn change_role(
        &self,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, String>;
    fn close(&self, cancellation: Cancellation) -> ServiceFuture<'_, ()>;
    fn abort(&self);
}

/// Owned removal purpose selected by the RAP adapter, never by peer traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRemoval {
    /// Native idle-owner removal; fence later builds of this same incarnation.
    Idle,
    /// Dispose partial/previous build resources before a node-authorized retry.
    /// Successful cleanup is not a successful Build or a final native tombstone.
    BuildRetryCleanup,
}

/// Custom-replicator control seam. There is deliberately no default all-success
/// implementation and no application write, storage or built-in-replicator API.
pub trait Replicator: Send + Sync + 'static {
    fn open(&self, cancellation: Cancellation) -> ServiceFuture<'_, String>;
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ServiceFuture<'_, ()>;
    fn progress(&self) -> Result<ReplicationProgress>;
    /// Return whether application state changed, NOT whether the native node
    /// advanced its data-loss epoch.
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool>;
    fn update_catch_up_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn update_current_configuration(
        &self,
        current: ReplicaSetConfiguration,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn wait_for_catch_up(
        &self,
        mode: CatchUpMode,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn remove_replica(
        &self,
        replica: ReplicaInformation,
        reason: ReplicaRemoval,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()>;
    fn close(&self, cancellation: Cancellation) -> ServiceFuture<'_, ()>;
    fn abort(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessStatus {
    ReconfigurationPending,
    NotPrimary,
    NoWriteQuorum,
    Granted,
}
impl AccessStatus {
    /// Public FABRIC_SERVICE_PARTITION_ACCESS_STATUS, not RAP's internal enum.
    pub fn native_value(self) -> i64 {
        match self {
            Self::Granted => 1,
            Self::ReconfigurationPending => 2,
            Self::NotPrimary => 3,
            Self::NoWriteQuorum => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaLifecycle {
    Opening,
    Open,
    Closing,
    Closed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationStage {
    Current,
    CatchUp,
    PreWriteRevoke,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpResult {
    NotStarted,
    DataLossReported,
    Completed,
}

/// Observational owned snapshot. Only the runtime can publish the snapshot used
/// by a partition handle; editing a returned copy cannot grant authority.
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub lifecycle: ReplicaLifecycle,
    pub service_role: ReplicaRole,
    pub replicator_role: ReplicaRole,
    pub replicator_open: bool,
    pub current_role: ReplicaRole,
    pub current_epoch: Epoch,
    pub previous_epoch: Epoch,
    pub configuration_stage: ConfigurationStage,
    pub catch_up: CatchUpResult,
    pub minimum_replica_set_size: usize,
    /// Native remote PC/CC descriptors, including down/non-Ready members.
    pub configuration: Vec<ReplicaInformation>,
    pub service_address: String,
    pub replication_address: String,
    pub progress: ReplicationProgress,
}
impl ReplicaStatus {
    /// Source-corresponding native RAP access calculation. The partition handle
    /// independently overlays its lifetime and process authority; these values
    /// are diagnostic and never enable application writes.
    pub fn access(&self) -> (AccessStatus, AccessStatus) {
        use AccessStatus::{Granted as G, NotPrimary as N, ReconfigurationPending as T};
        use ReplicaRole::{Idle, Primary, Secondary};
        let current =
            self.service_role == self.current_role && self.replicator_role == self.current_role;
        match self.lifecycle {
            ReplicaLifecycle::Closing | ReplicaLifecycle::Closed => return (N, N),
            ReplicaLifecycle::Opening => {
                return if self.current_role != Primary {
                    (N, N)
                } else if !current {
                    (T, T)
                } else {
                    (G, self.quorum_status(false))
                }
            }
            ReplicaLifecycle::Open => {}
        }
        if self.current_role == Idle {
            return (N, N);
        }
        if self.previous_epoch == Epoch::INVALID {
            return if self.service_role == Primary {
                (G, self.quorum_status(false))
            } else {
                (N, N)
            };
        }
        if self.configuration.is_empty() && self.current_role == Secondary {
            return (N, N);
        }
        let primary_change = self.previous_epoch.primary() != self.current_epoch.primary();
        let not_started = self.catch_up == CatchUpResult::NotStarted;
        if primary_change && self.current_role == Secondary {
            return if !not_started {
                (N, N)
            } else if self.configuration_stage == ConfigurationStage::PreWriteRevoke {
                (G, self.quorum_status(true))
            } else {
                (G, T)
            };
        }
        if primary_change && self.current_role == Primary {
            if !current || not_started {
                return (T, T);
            }
            return if self.configuration_stage == ConfigurationStage::CatchUp {
                (G, T)
            } else {
                (G, self.quorum_status(false))
            };
        }
        if !primary_change {
            return (
                G,
                self.quorum_status(
                    not_started || self.configuration_stage == ConfigurationStage::CatchUp,
                ),
            );
        }
        (N, N)
    }

    fn quorum_status(&self, include_previous: bool) -> AccessStatus {
        let (mut cc, mut cc_ready, mut pc, mut pc_ready) = (1, 1, 1, 1);
        for replica in &self.configuration {
            let ready = usize::from(replica.up && replica.ready);
            if matches!(replica.role, ReplicaRole::Primary | ReplicaRole::Secondary) {
                cc += 1;
                cc_ready += ready;
            }
            if replica.previous_role == ReplicaRole::Secondary {
                pc += 1;
                pc_ready += ready;
            }
        }
        if self.replicator_open
            && cc >= self.minimum_replica_set_size
            && cc_ready > cc / 2
            && (!include_previous || pc_ready > pc / 2)
        {
            AccessStatus::Granted
        } else {
            AccessStatus::NoWriteQuorum
        }
    }
}

/// Owned accounting for a stateful participant's callback or cleanup.
///
/// Keep this guard until the callback future AND its captures have actually
/// been disposed, including on cancellation and unwind. Its finite deadline
/// participates in the runtime's existing independent process fence and cannot
/// be renewed or disarmed by the holder.
///
/// This is NOT authorization: it grants no role, lifetime, or native access.
/// A guard may account for already-owned cleanup after partition invalidation;
/// application operations still require the normal partition/access checks.
#[must_use = "retain until the owned callback and its captures are actually disposed"]
pub struct StatefulCallbackGuard {
    _deadline: crate::lease::CallbackDeadline,
}

/// Generation-bound stateful partition. Fault reporting uses the bound native
/// replica identity; it never infers readiness from the returned client endpoint.
#[derive(Clone)]
pub struct StatefulPartition {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) kind: PartitionKind,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) authority: Arc<crate::lease::Authority>,
    pub(crate) status: Arc<Mutex<ReplicaStatus>>,
    pub(crate) reporting: Option<Arc<crate::reporting::Reporting>>,
}
impl StatefulPartition {
    pub fn identity(&self) -> &ReplicaIdentity {
        &self.identity
    }
    pub fn kind(&self) -> &PartitionKind {
        &self.kind
    }
    pub fn ensure_valid(&self) -> Result<()> {
        self.authority.check()?;
        if self.valid.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }
    /// Await the initial local fault-frame write, not native acknowledgment or
    /// replacement. Retries retain this generation's native identity until unit
    /// disposal, even if the caller drops its future or executor.
    /// Initial errors leave a bounded retry stage; permanent supersedes transient.
    pub async fn report_fault(&self, fault: crate::FaultType) -> Result<crate::FaultDelivery> {
        self.ensure_valid()?;
        self.reporting
            .as_ref()
            .ok_or(Error::Closed)?
            .fault(fault)
            .await
    }
    /// Latest bounded fault-stage status, including retry error/cancellation.
    pub fn fault_delivery(&self) -> Option<crate::FaultDelivery> {
        self.reporting
            .as_ref()
            .and_then(|reporting| reporting.fault_status())
    }
    /// Account for an owned callback/capture lifetime, including its destruction.
    ///
    /// This does not authorize invoking new application work. Callers must
    /// independently enforce partition validity and native access. Cleanup of
    /// already-owned work may enroll after unit validity is revoked, without
    /// reviving terminal process authority or extending any existing deadline.
    /// Enrollment is finite (at most 30 seconds), shares the runtime's 64
    /// callback slots, and is rejected once runtime disarming begins.
    pub fn supervise_owned_callback(&self, timeout: Duration) -> Result<StatefulCallbackGuard> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(Error::InvalidInput(
                "owned callback deadline must be >0 and <=30 seconds",
            ));
        }
        Ok(StatefulCallbackGuard {
            _deadline: self.authority.callback_deadline(timeout, true)?,
        })
    }
    pub fn authority_remaining(&self) -> Result<std::time::Duration> {
        self.ensure_valid()?;
        self.authority.remaining()
    }
    pub fn status(&self) -> Result<ReplicaStatus> {
        self.ensure_valid()?;
        let status = self.status.lock().unwrap().clone();
        self.ensure_valid()?;
        Ok(status)
    }
    pub fn access_status(&self) -> Result<(AccessStatus, AccessStatus)> {
        Ok(self.status()?.access())
    }
}

#[cfg(test)]
#[path = "stateful_api_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "stateful_callback_tests.rs"]
mod callback_tests;
