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

/// Bound on native-selected configuration descriptors admitted by this profile.
pub const MAX_REPLICA_SET_SIZE: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIdentity {
    pub service_name: String,
    pub service_type: String,
    pub partition_id: uuid::Uuid,
    pub replica_id: i64,
    pub incarnation: i64,
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
        data_loss: -1,
        configuration: -1,
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

/// Initialized application progress, independent of epochs and callback counts.
/// A permanently empty implementation may report (0,0), never an invented log.
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

/// Remote up/Ready members projected by RAP. The local replica is implicit and
/// counts towards write_quorum; a quorum is not clamped to available remotes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSetConfiguration {
    pub epoch: Epoch,
    pub replicas: Vec<ReplicaInformation>,
    pub write_quorum: usize,
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
/// Abort must dispose owned resources synchronously before returning.
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

/// Generation-bound stateful partition. It does not expose stateless-shaped
/// reporting methods or infer readiness from the returned client endpoint.
#[derive(Clone)]
pub struct StatefulPartition {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) kind: PartitionKind,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) authority: Arc<crate::lease::Authority>,
    pub(crate) status: Arc<Mutex<ReplicaStatus>>,
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
