//! Owned volatile stateful/custom-replicator contract.
//!
//! These interfaces describe lifecycle and control operations, not a storage or
//! application-write API. A custom replicator must justify its build/catch-up
//! results; returning success merely because the application is empty is not a
//! replication policy. The native node, not these objects, chooses membership.
use crate::extensions::EpochDomains;
use crate::{Error, PartitionKind, Result, ServicePartitionInformation};
pub use fube_core::sdk::{
    Epoch, IPrimaryReplicator, IReplicator, IStatefulServiceFactory, IStatefulServicePartition,
    IStatefulServiceReplica, OpenMode, ReplicaInformation, ReplicaRole, ReplicaSetConfig,
    ReplicaSetQuorumMode, ReplicaStatus, ServicePartitionAccessStatus, MAX_REPLICA_SET_SIZE,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
#[cfg(test)]
use std::time::Duration;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeReplicaDescriptor {
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
pub struct NativeReplicaSetConfiguration {
    pub epoch: Epoch,
    pub replicas: Vec<NativeReplicaDescriptor>,
    pub write_quorum: usize,
    pub local_member: bool,
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

impl NativeReplicaDescriptor {
    pub fn sdk(&self) -> ReplicaInformation {
        ReplicaInformation {
            id: self.replica_id,
            role: self.role,
            status: if self.up {
                ReplicaStatus::Up
            } else {
                ReplicaStatus::Down
            },
            replicator_address: self.address.clone(),
            current_progress: self.progress.last,
            catch_up_capability: self.progress.first,
            must_catch_up: self.must_catch_up,
        }
    }
}
impl NativeReplicaSetConfiguration {
    pub fn sdk(&self) -> Result<ReplicaSetConfig> {
        Ok(ReplicaSetConfig {
            replicas: self
                .replicas
                .iter()
                .map(NativeReplicaDescriptor::sdk)
                .collect(),
            write_quorum: self
                .write_quorum
                .try_into()
                .map_err(|_| Error::InvalidInput("quorum exceeds u32"))?,
        })
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
pub struct ReplicaDiagnostics {
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
    pub configuration: Vec<NativeReplicaDescriptor>,
    pub service_address: String,
    pub replication_address: String,
    pub progress: ReplicationProgress,
}
impl ReplicaDiagnostics {
    /// Source-corresponding native RAP access calculation. The partition handle
    /// independently overlays its lifetime and process authority; these values
    /// are diagnostic and never enable application writes.
    pub fn access(&self) -> (ServicePartitionAccessStatus, ServicePartitionAccessStatus) {
        use ReplicaRole::{ActiveSecondary as Secondary, IdleSecondary as Idle, Primary};
        use ServicePartitionAccessStatus::{
            Granted as G, NotPrimary as N, ReconfigurationPending as T,
        };
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

    fn quorum_status(&self, include_previous: bool) -> ServicePartitionAccessStatus {
        let (mut cc, mut cc_ready, mut pc, mut pc_ready) = (1, 1, 1, 1);
        for replica in &self.configuration {
            let ready = usize::from(replica.up && replica.ready);
            if matches!(
                replica.role,
                ReplicaRole::Primary | ReplicaRole::ActiveSecondary
            ) {
                cc += 1;
                cc_ready += ready;
            }
            if replica.previous_role == ReplicaRole::ActiveSecondary {
                pc += 1;
                pc_ready += ready;
            }
        }
        if self.replicator_open
            && cc >= self.minimum_replica_set_size
            && cc_ready > cc / 2
            && (!include_previous || pc_ready > pc / 2)
        {
            ServicePartitionAccessStatus::Granted
        } else {
            ServicePartitionAccessStatus::NoWriteQuorum
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
    pub(crate) _deadline: crate::lease::CallbackDeadline,
}

/// Generation-bound stateful partition. Fault reporting uses the bound native
/// replica identity; it never infers readiness from the returned client endpoint.
#[derive(Clone)]
pub(crate) struct StatefulPartition {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) kind: PartitionKind,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) authority: Arc<crate::lease::Authority>,
    pub(crate) status: Arc<Mutex<ReplicaDiagnostics>>,
    pub(crate) reporting: Option<Arc<crate::reporting::Reporting>>,
}
impl StatefulPartition {
    #[cfg(test)]
    pub fn identity(&self) -> &ReplicaIdentity {
        &self.identity
    }
    pub fn ensure_valid(&self) -> Result<()> {
        self.authority.check()?;
        if self.valid.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }
    /// Account for an owned callback/capture lifetime, including its destruction.
    ///
    /// This does not authorize invoking new application work. Callers must
    /// independently enforce partition validity and native access. Cleanup of
    /// already-owned work may enroll after unit validity is revoked, without
    /// reviving terminal process authority or extending any existing deadline.
    /// Enrollment is finite (at most 30 seconds), shares the runtime's 64
    /// callback slots, and is rejected once runtime disarming begins.
    #[cfg(test)]
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
    #[cfg(test)]
    pub fn authority_remaining(&self) -> Result<std::time::Duration> {
        self.ensure_valid()?;
        self.authority.remaining()
    }
}
impl IStatefulServicePartition for StatefulPartition {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation> {
        self.ensure_valid()?;
        Ok(self.kind.information(self.identity.partition_id))
    }
    fn get_read_status(&self) -> Result<ServicePartitionAccessStatus> {
        Ok(self.access_status()?.0)
    }
    fn get_write_status(&self) -> Result<ServicePartitionAccessStatus> {
        Ok(self.access_status()?.1)
    }
    fn report_fault(&self, fault: crate::FaultType) -> Result<()> {
        self.ensure_valid()?;
        let reporting = self.reporting.as_ref().ok_or(Error::Closed)?.clone();
        let worker = reporting.clone();
        reporting.complete(async move { worker.fault(fault).await.map(|_| ()) })
    }
    fn report_load(&self, metrics: &[crate::LoadMetric]) -> Result<()> {
        self.ensure_valid()?;
        let reporting = self.reporting.as_ref().ok_or(Error::Closed)?.clone();
        let worker = reporting.clone();
        let metrics = metrics.to_vec();
        reporting.complete(async move { worker.load(&metrics).await })
    }
    fn report_move_cost(&self, cost: crate::MoveCost) -> Result<()> {
        self.ensure_valid()?;
        let reporting = self.reporting.as_ref().ok_or(Error::Closed)?.clone();
        let worker = reporting.clone();
        reporting.complete(async move { worker.move_cost(cost).await })
    }
    fn report_partition_health(&self, health: &crate::HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Partition)
            .map(|_| ())
    }
    fn report_replica_health(&self, health: &crate::HealthInformation) -> Result<()> {
        self.health(health, crate::reporting::HealthTarget::Replica)
            .map(|_| ())
    }
    fn report_partition_health2(
        &self,
        health: &crate::HealthInformation,
        _: Option<&crate::HealthReportSendOption>,
    ) -> Result<()> {
        IStatefulServicePartition::report_partition_health(self, health)
    }
    fn report_replica_health2(
        &self,
        health: &crate::HealthInformation,
        _: Option<&crate::HealthReportSendOption>,
    ) -> Result<()> {
        IStatefulServicePartition::report_replica_health(self, health)
    }
}
impl StatefulPartition {
    pub(crate) fn health(
        &self,
        health: &crate::HealthInformation,
        target: crate::reporting::HealthTarget,
    ) -> Result<crate::reporting::HealthReceipt> {
        self.ensure_valid()?;
        let health = crate::HealthReport::try_from(health)?;
        let reporting = self.reporting.as_ref().ok_or(Error::Closed)?.clone();
        let worker = reporting.clone();
        reporting.complete(async move { worker.health(&health, target).await })
    }
    pub fn status(&self) -> Result<ReplicaDiagnostics> {
        self.ensure_valid()?;
        let status = self.status.lock().unwrap().clone();
        self.ensure_valid()?;
        Ok(status)
    }
    pub fn access_status(
        &self,
    ) -> Result<(ServicePartitionAccessStatus, ServicePartitionAccessStatus)> {
        Ok(self.status()?.access())
    }
}

#[cfg(test)]
#[path = "stateful_api_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "stateful_callback_tests.rs"]
mod callback_tests;
