//! Ordinary public control delegation used to witness canonical composition.
//! No binding installation, invocation capture, engine access or setters.
use fube_runtime::{
    ApiFuture, Cancellation, Epoch, IPrimaryReplicator, IReplicator, ReplicaInformation,
    ReplicaRole, ReplicaSetConfig, ReplicaSetQuorumMode, Result,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct DelegatingPrimary(pub Arc<dyn IPrimaryReplicator>);
impl IReplicator for DelegatingPrimary {
    fn open(&self, cancellation: Cancellation) -> ApiFuture<'_, String> {
        self.0.open(cancellation)
    }
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        self.0.close(cancellation)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0.change_role(epoch, role, cancellation)
    }
    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        self.0.update_epoch(epoch, cancellation)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.0.get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.0.get_catch_up_capability()
    }
    fn abort(&self) {
        self.0.abort();
    }
}
impl IPrimaryReplicator for DelegatingPrimary {
    fn on_data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool> {
        self.0.on_data_loss(cancellation)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.0
            .update_catch_up_replica_set_configuration(current, previous)
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.0.update_current_replica_set_configuration(current)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0.wait_for_catch_up_quorum(mode, cancellation)
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0.build_replica(replica, cancellation)
    }
    fn remove_replica(&self, replica: i64) -> Result<()> {
        self.0.remove_replica(replica)
    }
}
