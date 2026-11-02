//! Replication-defined factory and host adapters, not SDK authority interfaces.
//! The host interprets its own control records. Only lower algorithm values and
//! an operation-specific validation callback cross this boundary.
use std::sync::Arc;

use fube_core::{
    hosting::ExecutionRuntime, ApiFuture, Cancellation, Epoch, FaultType,
    IStatefulServicePartition, Result,
};

use crate::{
    extensions::{ReplayCheckpointProvider, ReplicationEndpointOptions, StateProviderCapabilities},
    FabricReplicator, IStateProvider, Settings,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub first: i64,
    pub last: i64,
}
impl Progress {
    pub const EMPTY: Self = Self {
        first: -1,
        last: -1,
    };
}

/// A selected lower peer. Host node identity, request records and revisions are
/// deliberately absent; the adapter validates those before projecting this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replica {
    pub replica_id: i64,
    pub incarnation: i64,
    pub up: bool,
    pub ready: bool,
    pub address: String,
    pub progress: Progress,
    pub must_catch_up: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    pub epoch: Epoch,
    pub replicas: Vec<Replica>,
    pub write_quorum: usize,
    pub local_member: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Removal {
    Idle,
    BuildRetryCleanup,
}

#[derive(Clone, Copy, Debug)]
pub struct PreWrite {
    pub current: Epoch,
    pub previous: Epoch,
}

/// One operation only: revalidate at actual queued mutation and completion.
/// This cannot capture, enter, inspect or replace host authority.
pub trait MutationCheck: Send + Sync {
    fn validate(&self) -> Result<()>;
}

/// Eligible read-only lower facts, computed by the host at observation time.
#[derive(Clone, Debug, Default)]
pub struct StableFacts {
    pub singleton: Option<Epoch>,
    pub primary: Option<(Epoch, Replica)>,
}
pub trait MembershipSource: Send + Sync {
    fn observe(&self) -> Result<StableFacts>;
}
pub trait HealthClient: Send + Sync {
    fn report_fault(&self, fault: FaultType) -> ApiFuture<'_, ()>;
}

/// Neutral construction inputs. Execution carries opaque host cleanup ancestry;
/// neither this factory nor the engine interprets that ancestry.
#[derive(Clone)]
pub struct HostServices {
    pub execution: ExecutionRuntime,
    pub generation: u64,
    pub validity: Arc<dyn MutationCheck>,
    pub membership: Arc<dyn MembershipSource>,
    pub health: Arc<dyn HealthClient>,
}

pub struct ReplicatorFactory;
impl ReplicatorFactory {
    /// Create exactly one lower owner. The hosting caller performs its own
    /// generation-bound constructor/identity/one-engine claim before calling.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        partition_id: uuid::Uuid,
        replica_id: i64,
        partition: Arc<dyn IStatefulServicePartition>,
        provider: Arc<dyn IStateProvider>,
        replay: Option<Arc<dyn ReplayCheckpointProvider>>,
        endpoint: ReplicationEndpointOptions,
        settings: Settings,
        capabilities: StateProviderCapabilities,
        host: HostServices,
    ) -> Result<(Arc<FabricReplicator>, HostingEndpoint)> {
        let engine = crate::engine::Engine::bind_hosted(
            partition,
            crate::wire::Actor {
                partition: partition_id,
                replica: replica_id,
                incarnation: endpoint.actor_incarnation,
            },
            &endpoint.listen_address,
            provider,
            replay,
            settings,
            capabilities,
            host,
        )?;
        Ok((
            Arc::new(FabricReplicator {
                engine: engine.clone(),
            }),
            HostingEndpoint { engine },
        ))
    }
}

/// Factory-retained ingress for exactly one lower owner. It cannot be obtained
/// from any public replicator/state/diagnostic view, or retargeted to another.
///
/// ```compile_fail,E0616
/// fn mint(r: &fube_data::FabricReplicator) {
///     let _endpoint = fube_data::hosting::HostingEndpoint { engine: r.clone().engine };
/// }
/// ```
/// ```compile_fail,E0308
/// fn recover(r: fube_data::FabricReplicator) -> fube_data::hosting::HostingEndpoint {
///     r.clone()
/// }
/// ```
#[derive(Clone)]
pub struct HostingEndpoint {
    engine: Arc<crate::engine::Engine>,
}

impl FabricReplicator {
    /// Internal adapter regression support; never part of normal construction.
    #[cfg(feature = "test-hooks")]
    pub fn engine_for_test(&self) -> Arc<testing::Engine> {
        self.engine.clone()
    }
    pub fn sample_progress(&self) -> Result<Progress> {
        self.engine.progress()
    }
}

impl HostingEndpoint {
    pub fn configure(
        &self,
        current: Configuration,
        previous: Option<Configuration>,
        prewrite: Option<PreWrite>,
        cancellation: Cancellation,
        check: Arc<dyn MutationCheck>,
    ) -> Result<()> {
        self.engine
            .configure_hosted(current, previous, prewrite, cancellation, check)
    }

    /// Projection and once-entry must already have happened at method entry.
    pub fn build(
        &self,
        replica: Replica,
        cancellation: Cancellation,
        check: Arc<dyn MutationCheck>,
    ) -> ApiFuture<'_, ()> {
        self.engine
            .build_authorized(replica, cancellation, Some(check))
    }
    pub fn remove(
        &self,
        replica: Replica,
        purpose: Removal,
        cancellation: Cancellation,
        check: Arc<dyn MutationCheck>,
    ) -> Result<()> {
        self.engine
            .remove_hosted(replica, purpose, cancellation, check)
    }
}

/// Implementation regression hooks used only by the runtime adapter's tests.
/// No runtime proofs or test-support harness APIs are owned here.
#[cfg(feature = "test-hooks")]
pub mod testing {
    pub use crate::engine::Engine;
    pub use crate::transport::PeerTransport;
}
