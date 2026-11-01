//! Native ComTransactionalReplicator.cpp:482–600,654–886: the returned
//! composite delegates to ONE lower public control and owns upper lifecycle.
//! Retained callback scope/owned continuation is a Rust hosting adapter.
use super::{runtime::VolatileRuntime, transaction::own};
use crate::{ApiFuture, Cancellation, Epoch, ReplicaRole, Result};
use fube_runtime::{
    extensions::{CallLane, CallOrigin, CommandCleanup},
    IPrimaryReplicator, IReplicator, ReplicaInformation, ReplicaSetConfig, ReplicaSetQuorumMode,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
    time::Duration,
};

#[derive(Clone)]
pub(crate) struct Composite(pub(crate) Arc<VolatileRuntime>);
struct Scoped<T> {
    work: ApiFuture<'static, T>,
    cleanup: Option<CommandCleanup>,
}
impl<T> Future for Scoped<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _scope = self.cleanup.as_ref().map(CommandCleanup::enter);
        self.work.as_mut().poll(cx)
    }
}
impl Composite {
    fn owned<T: Clone + Send + Sync + 'static>(
        &self,
        work: ApiFuture<'static, T>,
    ) -> ApiFuture<'static, T> {
        own(Box::pin(Scoped {
            work,
            cleanup: self.0.execution().command_cleanup(),
        }))
    }
    pub(crate) fn owned_control<T: Clone + Send + Sync + 'static>(
        execution: &fube_runtime::extensions::ExecutionRuntime,
        work: ApiFuture<'static, T>,
    ) -> ApiFuture<'static, T> {
        own(Box::pin(Scoped {
            work,
            cleanup: execution.command_cleanup(),
        }))
    }
}
impl IReplicator for Composite {
    fn open(&self, cancellation: Cancellation) -> ApiFuture<'_, String> {
        let runtime = self.0.clone();
        self.owned(Box::pin(async move {
            let address = runtime.lower_control().open(cancellation.clone()).await?;
            #[cfg(test)]
            runtime
                .control_events
                .lock()
                .unwrap()
                .push(super::runtime::ControlEvent::Opened(address.clone()));
            if let Err(error) = runtime.open(cancellation).await {
                let lower = runtime.lower_control().clone();
                // Abort is synchronous owned disposal, not cleanup-entry
                // authority. Reserve the existing distinct host provider lane.
                let aborted = runtime
                    .execution()
                    .reserve(
                        CallLane::Provider,
                        CallOrigin::default(),
                        Duration::from_secs(30),
                        true,
                    )
                    .map(|permit| {
                        permit.run_owned(move || {
                            lower.abort();
                            Ok(())
                        })
                    });
                match aborted {
                    Ok(call) => {
                        if call.await.is_err() {
                            runtime.execution().retire();
                        }
                    }
                    Err(_) => runtime.execution().retire(),
                }
                return Err(error);
            }
            Ok(address)
        }))
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        let runtime = self.0.clone();
        self.owned(Box::pin(async move {
            runtime.owner.ready.store(false, Ordering::Release);
            runtime.owner.manager.fence_application();
            #[cfg(test)]
            runtime
                .control_events
                .lock()
                .unwrap()
                .push(super::runtime::ControlEvent::RoleEntry(role));
            let lower_result = runtime
                .lower_control()
                .change_role(epoch, role, cancellation.clone())
                .await;
            #[cfg(test)]
            runtime
                .control_events
                .lock()
                .unwrap()
                .push(super::runtime::ControlEvent::RoleResult(
                    role,
                    lower_result.is_ok(),
                ));
            lower_result?;
            #[cfg(test)]
            runtime
                .control_events
                .lock()
                .unwrap()
                .push(super::runtime::ControlEvent::Role(role));
            if let Err(error) = runtime.role(role, cancellation).await {
                runtime.report_fault();
                return Err(error);
            }
            Ok(())
        }))
    }
    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        let lower = self.0.lower_control().clone();
        self.owned(Box::pin(async move {
            lower.update_epoch(epoch, cancellation).await
        }))
    }
    fn get_current_progress(&self) -> Result<i64> {
        self.0.lower_control().get_current_progress()
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.0.lower_control().get_catch_up_capability()
    }
    fn close(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        let runtime = self.0.clone();
        self.owned(Box::pin(async move {
            runtime.closing_lower.store(true, Ordering::Release);
            runtime.owner.ready.store(false, Ordering::Release);
            runtime.owner.manager.fence_application();
            runtime.lower_control().close(cancellation.clone()).await?;
            #[cfg(test)]
            runtime
                .control_events
                .lock()
                .unwrap()
                .push(super::runtime::ControlEvent::Closed);
            runtime.close(cancellation).await
        }))
    }
    fn abort(&self) {
        self.0.closing_lower.store(true, Ordering::Release);
        self.0.lower_control().abort();
        #[cfg(test)]
        self.0
            .control_events
            .lock()
            .unwrap()
            .push(super::runtime::ControlEvent::Aborted);
        self.0.abort();
    }
}
impl IPrimaryReplicator for Composite {
    fn on_data_loss(&self, cancellation: Cancellation) -> ApiFuture<'_, bool> {
        let lower = self.0.lower_control().clone();
        self.owned(Box::pin(
            async move { lower.on_data_loss(cancellation).await },
        ))
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.0
            .lower_control()
            .update_catch_up_replica_set_configuration(current, previous)
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.0
            .lower_control()
            .update_current_replica_set_configuration(current)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        let lower = self.0.lower_control().clone();
        self.owned(Box::pin(async move {
            lower.wait_for_catch_up_quorum(mode, cancellation).await
        }))
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        // Eager delegation preserves the actual invocation before future return.
        self.0.lower_control().build_replica(replica, cancellation)
    }
    fn remove_replica(&self, replica: i64) -> Result<()> {
        self.0.lower_control().remove_replica(replica)
    }
}
