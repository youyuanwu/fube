//! One native-bound owner shared by canonical common, primary and state access.
use crate::{
    engine::{Engine, Replication},
    extensions::{
        EpochCheckedReplication, OperationOriginStream, ReplayCheckpointProvider,
        ReplicationEndpointOptions,
    },
    wire::Actor,
    Cancellation, Epoch, IOperationStream, IStateProvider, IStateReplicator, OperationData,
    ReplicationAdmission, ReplicationCompletion, Result, Settings,
};
use fube_runtime::{
    extensions::NativeRuntimeIntegration, ApiFuture, IPrimaryReplicator, IReplicator,
    IStatefulServicePartition, ReplicaInformation, ReplicaRole, ReplicaSetConfig,
    ReplicaSetQuorumMode,
};
use std::sync::Arc;

/// Managed FabricReplicator responsibility, with Rust object-safe control/data
/// views over ONE private engine, listener and owner. Clones share that owner.
/// Not a native COM proxy/CreateReplicator or mutable native settings facade.
/// Canonical root and private invocation-bound primary controls both work with
/// valid native-issued records; neither public descriptors nor diagnostic
/// snapshots create those records. No standalone out-of-host primary authority.
#[derive(Clone)]
pub struct FabricReplicator {
    pub(crate) engine: Arc<Engine>,
}

impl FabricReplicator {
    /// Data-layer construction integration, NOT an extra SDK partition method.
    /// Call in the runtime-owned service Open entry. Missing/stale/mismatched
    /// runtime-issued authority fails before listener/provider activation.
    /// Provider and optional replay share one supervised ownership group; no
    /// replay supertrait, implicit discovery or standalone authority is required.
    /// This helper's location/signature is fube-specific, not native partition
    /// CreateReplicator source/ABI compatibility. See the production sample
    /// `crates/samples/rust-runtime-app/src/data.rs` for construction and pumps.
    pub fn create(
        partition: Arc<dyn IStatefulServicePartition>,
        native: NativeRuntimeIntegration,
        provider: Arc<dyn IStateProvider>,
        replay: Option<Arc<dyn ReplayCheckpointProvider>>,
        endpoint: ReplicationEndpointOptions,
        settings: Settings,
    ) -> Result<Arc<Self>> {
        let identity = native.identity();
        let actor = Actor {
            partition: identity.partition_id,
            replica: identity.replica_id,
            incarnation: endpoint.actor_incarnation,
        };
        Ok(Arc::new(Self {
            engine: Engine::bind_partition(
                partition,
                native,
                actor,
                &endpoint.listen_address,
                provider,
                replay,
                settings,
            )?,
        }))
    }

    /// Retained state access shares the control object's owner. No engine escape
    /// hatch and no relay/task/queue is allocated by this view.
    pub fn get_state_replicator(&self) -> Arc<dyn IStateReplicator> {
        Arc::new(self.clone())
    }

    /// Subscribe to best-effort fube diagnostics, not canonical SF callbacks.
    /// Subscribe before triggering work: no event history is replayed. Slow receivers get
    /// `RecvError::Lagged`; publishers never wait for subscriber progress. Events
    /// neither pause the owner nor grant membership, epoch or write authority.
    ///
    /// ```
    /// # async fn read_one(replicator: &fube_data::FabricReplicator)
    /// # -> Result<fube_data::extensions::ReplicationEvent, tokio::sync::broadcast::error::RecvError> {
    /// let mut events = replicator.subscribe_events();
    /// events.recv().await // Propagates lag and closure to the caller.
    /// # }
    /// ```
    pub fn subscribe_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::extensions::ReplicationEvent> {
        self.engine.subscribe_events()
    }

    pub(crate) fn snapshot(&self) -> crate::extensions::EngineSnapshot {
        self.engine.snapshot()
    }
    pub(crate) fn endpoint(&self) -> crate::wire::Endpoint {
        self.engine.endpoint()
    }
    pub(crate) fn execution(&self) -> fube_runtime::extensions::ExecutionRuntime {
        self.engine.execution_runtime()
    }
    pub(crate) fn copy_with_origin(&self) -> Result<Arc<dyn OperationOriginStream>> {
        self.engine.ensure_open()?;
        Ok(self.engine.copy_stream())
    }
    pub(crate) fn replication_with_origin(&self) -> Result<Arc<dyn OperationOriginStream>> {
        self.engine.ensure_open()?;
        Ok(self.engine.replication_stream())
    }
    #[cfg(feature = "qualification")]
    pub(crate) fn transport_snapshot(&self) -> serde_json::Value {
        self.engine.qualification_transport_snapshot()
    }
    #[cfg(feature = "qualification")]
    pub(crate) fn probe(
        &self,
        kind: &str,
        target: Option<i64>,
    ) -> ApiFuture<'_, serde_json::Value> {
        self.engine.qualification_probe(kind, target)
    }
}

fn admission(replication: Replication) -> ReplicationAdmission {
    ReplicationAdmission {
        sequence_number: replication.sequence,
        completion: ReplicationCompletion {
            receiver: replication.completion,
        },
    }
}

impl IStateReplicator for FabricReplicator {
    fn replicate(
        &self,
        data: OperationData,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ReplicationAdmission> {
        Box::pin(async move { self.engine.submit(data, cancel).await.map(admission) })
    }
    fn get_copy_stream(&self) -> Result<Arc<dyn IOperationStream>> {
        self.engine.ensure_open()?;
        Ok(self.engine.copy_stream())
    }
    fn get_replication_stream(&self) -> Result<Arc<dyn IOperationStream>> {
        self.engine.ensure_open()?;
        Ok(self.engine.replication_stream())
    }
}
impl EpochCheckedReplication for FabricReplicator {
    fn replicate_in_epoch(
        &self,
        epoch: Epoch,
        data: OperationData,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ReplicationAdmission> {
        Box::pin(async move {
            self.engine
                .submit_in_epoch(epoch, data, cancel)
                .await
                .map(admission)
        })
    }
}
impl IReplicator for FabricReplicator {
    fn open(&self, cancel: Cancellation) -> ApiFuture<'_, String> {
        IReplicator::open(&*self.engine, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        IReplicator::close(&*self.engine, cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        IReplicator::change_role(&*self.engine, epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ApiFuture<'_, ()> {
        IReplicator::update_epoch(&*self.engine, epoch, cancel)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.engine.get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.engine.get_catch_up_capability()
    }
    fn abort(&self) {
        IReplicator::abort(&*self.engine);
    }
}
impl IPrimaryReplicator for FabricReplicator {
    fn on_data_loss(&self, cancel: Cancellation) -> ApiFuture<'_, bool> {
        IPrimaryReplicator::on_data_loss(&*self.engine, cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.engine
            .update_catch_up_replica_set_configuration(current, previous)
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.engine
            .update_current_replica_set_configuration(current)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.engine.wait_for_catch_up_quorum(mode, cancel)
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.engine.build_replica(replica, cancel)
    }
    fn remove_replica(&self, replica: i64) -> Result<()> {
        self.engine.remove_replica(replica)
    }
}
