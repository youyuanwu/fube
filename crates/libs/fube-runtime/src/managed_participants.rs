//! Private lifetime proxies; baseline public contracts are unchanged in Phase 1.
use crate::{execution::*, *};
use std::sync::Arc;

pub(crate) fn stateless_factory(
    execution: &ExecutionRuntime,
    factory: Arc<dyn StatelessFactory>,
) -> RuntimeResult<Arc<dyn StatelessFactory>> {
    let ticket =
        execution.reserve_object(&execution.factory_quota(), &DisposalTracker::default())?;
    Ok(Arc::new(StatelessFactoryOwner {
        inner: ticket.publish(factory),
        execution: execution.clone(),
    }))
}
pub(crate) fn stateful_factory(
    execution: &ExecutionRuntime,
    factory: Arc<dyn StatefulFactory>,
) -> RuntimeResult<Arc<dyn StatefulFactory>> {
    let ticket =
        execution.reserve_object(&execution.factory_quota(), &DisposalTracker::default())?;
    Ok(Arc::new(StatefulFactoryOwner {
        inner: ticket.publish(factory),
        execution: execution.clone(),
    }))
}
struct StatelessFactoryOwner {
    inner: ManagedObject<Arc<dyn StatelessFactory>>,
    execution: ExecutionRuntime,
}
impl StatelessFactory for StatelessFactoryOwner {
    fn create(&self, context: ServiceContext) -> ServiceFuture<'_, Arc<dyn StatelessService>> {
        Box::pin(async move {
            let ticket = self.execution.reserve_construction(
                &self.execution.service_quota(),
                &DisposalTracker::default(),
            )?;
            let service = self.inner.get().create(context).await?;
            Ok(Arc::new(StatelessOwner(ticket.construct(service))) as Arc<dyn StatelessService>)
        })
    }
}
struct StatelessOwner(ManagedObject<Arc<dyn StatelessService>>);
impl StatelessService for StatelessOwner {
    fn open(&self, partition: Partition, cancel: Cancellation) -> ServiceFuture<'_, String> {
        self.0.get().open(partition, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.get().close(cancel)
    }
    fn abort(&self) {
        self.0.get().abort();
    }
}
struct StatefulFactoryOwner {
    inner: ManagedObject<Arc<dyn StatefulFactory>>,
    execution: ExecutionRuntime,
}
impl StatefulFactory for StatefulFactoryOwner {
    fn create(
        &self,
        context: StatefulServiceContext,
    ) -> ServiceFuture<'_, Arc<dyn StatefulService>> {
        Box::pin(async move {
            let ticket = self.execution.reserve_construction(
                &self.execution.service_quota(),
                &DisposalTracker::default(),
            )?;
            let service = self.inner.get().create(context).await?;
            Ok(Arc::new(StatefulOwner {
                inner: ticket.construct(service),
                execution: self.execution.clone(),
            }) as Arc<dyn StatefulService>)
        })
    }
}
struct StatefulOwner {
    inner: ManagedObject<Arc<dyn StatefulService>>,
    execution: ExecutionRuntime,
}
impl StatefulService for StatefulOwner {
    fn open(
        &self,
        partition: StatefulPartition,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, Arc<dyn Replicator>> {
        Box::pin(async move {
            let ticket = self.execution.reserve_construction(
                &self.execution.control_quota(),
                &DisposalTracker::default(),
            )?;
            let control = self.inner.get().open(partition, cancel).await?;
            Ok(Arc::new(ControlOwner(ticket.construct(control))) as Arc<dyn Replicator>)
        })
    }
    fn change_role(&self, role: ReplicaRole, cancel: Cancellation) -> ServiceFuture<'_, String> {
        self.inner.get().change_role(role, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.inner.get().close(cancel)
    }
    fn abort(&self) {
        self.inner.get().abort();
    }
}
struct ControlOwner(ManagedObject<Arc<dyn Replicator>>);
impl Replicator for ControlOwner {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        self.0.get().open(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.get().change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.get().update_epoch(epoch, cancel)
    }
    fn progress(&self) -> RuntimeResult<ReplicationProgress> {
        self.0.get().progress()
    }
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.0.get().on_data_loss(cancel)
    }
    fn update_catch_up_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0
            .get()
            .update_catch_up_configuration(current, previous, cancel)
    }
    fn update_current_configuration(
        &self,
        current: ReplicaSetConfiguration,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.get().update_current_configuration(current, cancel)
    }
    fn wait_for_catch_up(&self, mode: CatchUpMode, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.get().wait_for_catch_up(mode, cancel)
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.get().build_replica(replica, cancel)
    }
    fn remove_replica(
        &self,
        replica: ReplicaInformation,
        reason: ReplicaRemoval,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.get().remove_replica(replica, reason, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.get().close(cancel)
    }
    fn abort(&self) {
        self.0.get().abort();
    }
}
