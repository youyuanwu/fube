//! Private physical-lifetime proxies over the canonical SDK contracts.
use crate::{execution::*, *};
use std::sync::Arc;

pub(crate) fn stateless_factory(
    execution: &ExecutionRuntime,
    factory: Arc<dyn IStatelessServiceFactory>,
) -> Result<Arc<dyn IStatelessServiceFactory>> {
    let ticket =
        execution.reserve_object(&execution.factory_quota(), &DisposalTracker::default())?;
    Ok(Arc::new(StatelessFactoryOwner {
        inner: ticket.publish(factory),
        execution: execution.clone(),
    }))
}
pub(crate) fn stateful_factory(
    execution: &ExecutionRuntime,
    factory: Arc<dyn IStatefulServiceFactory>,
) -> Result<Arc<dyn IStatefulServiceFactory>> {
    let ticket =
        execution.reserve_object(&execution.factory_quota(), &DisposalTracker::default())?;
    Ok(Arc::new(StatefulFactoryOwner {
        inner: ticket.publish(factory),
        execution: execution.clone(),
    }))
}
struct StatelessFactoryOwner {
    inner: ManagedObject<Arc<dyn IStatelessServiceFactory>>,
    execution: ExecutionRuntime,
}
impl IStatelessServiceFactory for StatelessFactoryOwner {
    fn create_instance(
        &self,
        service_type: String,
        service_name: String,
        initialization: &[u8],
        partition: uuid::Uuid,
        instance: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>> {
        let ticket = self
            .execution
            .reserve_construction(&self.execution.service_quota(), &DisposalTracker::default())?;
        let service = self.inner.get().create_instance(
            service_type,
            service_name,
            initialization,
            partition,
            instance,
        )?;
        Ok(Box::new(StatelessOwner(ticket.construct(service))))
    }
}
struct StatelessOwner(ManagedObject<Box<dyn IStatelessServiceInstance>>);
impl IStatelessServiceInstance for StatelessOwner {
    fn open(
        &self,
        partition: Arc<dyn IStatelessServicePartition>,
        cancel: Cancellation,
    ) -> ApiFuture<'_, String> {
        self.0.get().open(partition, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.0.get().close(cancel)
    }
    fn abort(&self) {
        self.0.get().abort();
    }
}
struct StatefulFactoryOwner {
    inner: ManagedObject<Arc<dyn IStatefulServiceFactory>>,
    execution: ExecutionRuntime,
}
impl IStatefulServiceFactory for StatefulFactoryOwner {
    fn create_replica(
        &self,
        service_type: String,
        service_name: String,
        initialization: &[u8],
        partition: uuid::Uuid,
        replica: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        let ticket = self
            .execution
            .reserve_construction(&self.execution.service_quota(), &DisposalTracker::default())?;
        let service = self.inner.get().create_replica(
            service_type,
            service_name,
            initialization,
            partition,
            replica,
        )?;
        Ok(Box::new(StatefulOwner {
            inner: ticket.construct(service),
            execution: self.execution.clone(),
        }))
    }
}
struct StatefulOwner {
    inner: ManagedObject<Box<dyn IStatefulServiceReplica>>,
    execution: ExecutionRuntime,
}
impl IStatefulServiceReplica for StatefulOwner {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        cancel: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
            let ticket = self.execution.reserve_construction(
                &self.execution.control_quota(),
                &DisposalTracker::default(),
            )?;
            let control = self.inner.get().open(mode, partition, cancel).await?;
            Ok(Box::new(ControlOwner(ticket.construct(control))) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, role: ReplicaRole, cancel: Cancellation) -> ApiFuture<'_, String> {
        self.inner.get().change_role(role, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.inner.get().close(cancel)
    }
    fn abort(&self) {
        self.inner.get().abort();
    }
}
struct ControlOwner(ManagedObject<Box<dyn IPrimaryReplicator>>);
impl IReplicator for ControlOwner {
    fn open(&self, cancel: Cancellation) -> ApiFuture<'_, String> {
        self.0.get().open(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0.get().change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.0.get().update_epoch(epoch, cancel)
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.0.get().get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.0.get().get_catch_up_capability()
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        self.0.get().close(cancel)
    }
    fn abort(&self) {
        self.0.get().abort();
    }
}
impl IPrimaryReplicator for ControlOwner {
    fn on_data_loss(&self, cancel: Cancellation) -> ApiFuture<'_, bool> {
        self.0.get().on_data_loss(cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.0
            .get()
            .update_catch_up_replica_set_configuration(current, previous)
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.0
            .get()
            .update_current_replica_set_configuration(current)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0.get().wait_for_catch_up_quorum(mode, cancel)
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancel: Cancellation,
    ) -> ApiFuture<'_, ()> {
        self.0.get().build_replica(replica, cancel)
    }
    fn remove_replica(&self, replica_id: i64) -> Result<()> {
        self.0.get().remove_replica(replica_id)
    }
}
