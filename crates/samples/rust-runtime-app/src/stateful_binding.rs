//! Native proof binding for the empty-state sample, not an SDK discovery hook.
use super::*;
use fube_runtime::{ErrorKind, ReplicaInformation as SdkReplica, ReplicaSetConfig};

struct Binder(Weak<EmptyReplicator>);
struct Bound {
    root: Arc<EmptyReplicator>,
    proof: NativeInvocation,
}
pub(super) struct EmptyControl(pub(super) Arc<EmptyReplicator>);
impl NativeControlBinding for Binder {
    fn bind_invocation(
        &self,
        proof: NativeInvocation,
    ) -> RuntimeResult<Arc<dyn IPrimaryReplicator>> {
        let root = self.0.upgrade().ok_or(RuntimeError::Closed)?;
        proof.validate()?;
        if root.native()?.binding() != proof.binding()
            || *root.native_actor.lock().unwrap() != proof.actor()
        {
            return Err(RuntimeError::InvalidInput(
                "empty control native binding mismatch",
            ));
        }
        Ok(Arc::new(Bound { root, proof }))
    }
}
impl EmptyReplicator {
    pub(super) fn attach_native(
        self: &Arc<Self>,
        native: NativeRuntimeIntegration,
    ) -> RuntimeResult<()> {
        let actor = uuid::Uuid::new_v4();
        native.execution().check_constructor()?;
        native.reserve_engine(self.identity.partition_id, self.identity.replica_id, actor)?;
        let binder: Arc<dyn NativeControlBinding> = Arc::new(Binder(Arc::downgrade(self)));
        native.install_binding(actor, &binder)?;
        *self.native_actor.lock().unwrap() = Some(actor);
        *self.native.lock().unwrap() = Some(native);
        *self.binder.lock().unwrap() = Some(binder);
        Ok(())
    }
    fn native(&self) -> RuntimeResult<NativeRuntimeIntegration> {
        self.native.lock().unwrap().clone().ok_or_else(|| {
            RuntimeError::new(
                ErrorKind::Unsupported,
                "standalone empty fixture has no native issuer",
            )
        })
    }
    fn configure_sdk(
        self: &Arc<Self>,
        cc: ReplicaSetConfig,
        pc: Option<ReplicaSetConfig>,
        bound: Option<NativeInvocation>,
    ) -> RuntimeResult<()> {
        let native = self.native()?;
        let execution = native.execution();
        let deadline = execution.blocking_deadline()?;
        let proof = match bound {
            Some(proof) => proof,
            None => native.capture_configuration(&cc, pc.as_ref())?,
        };
        let kind = if pc.is_some() {
            NativeInvocationKind::CatchUpConfiguration
        } else {
            NativeInvocationKind::CurrentConfiguration
        };
        let (current, previous) = proof
            .configuration()
            .ok_or(RuntimeError::InvalidInput("missing native configuration"))?;
        if proof.kind() != kind
            || current.sdk()? != cc
            || previous.map(|p| p.sdk()).transpose()? != pc
        {
            return Err(RuntimeError::InvalidInput(
                "empty configuration differs from frozen native inputs",
            ));
        }
        let current = current.descriptor()?;
        let previous = previous.map(|p| p.descriptor()).transpose()?;
        let cancellation = proof.cancellation();
        let permit = execution.reserve(
            CallLane::Provider,
            CallOrigin::default(),
            deadline.saturating_duration_since(Instant::now()),
            false,
        )?;
        proof.enter()?;
        let root = self.clone();
        permit
            .run_future(async move {
                proof.validate()?;
                match previous {
                    Some(previous) => {
                        root.update_catch_up_configuration(current, previous, cancellation)
                            .await?
                    }
                    None => {
                        root.update_current_configuration(current, cancellation)
                            .await?
                    }
                }
                proof.validate()
            })
            .wait()
    }
    fn build_sdk(
        self: &Arc<Self>,
        replica: SdkReplica,
        cancellation: Cancellation,
        bound: Option<NativeInvocation>,
    ) -> ServiceFuture<'_, ()> {
        let captured = (|| {
            let proof = match bound {
                Some(proof) => proof,
                None => self.native()?.capture_build(&replica)?,
            };
            if proof.kind() != NativeInvocationKind::Build
                || proof.replica().map(|r| r.sdk()).transpose()?.as_ref() != Some(&replica)
            {
                return Err(RuntimeError::InvalidInput(
                    "empty build differs from frozen native target",
                ));
            }
            let native = proof
                .replica()
                .ok_or(RuntimeError::InvalidInput("missing native replica"))?
                .descriptor()?;
            proof.enter()?;
            Ok((proof, native))
        })();
        match captured {
            Ok((proof, native)) => self.build_frozen(native, cancellation, Some(proof)),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
    fn remove_sdk(&self, id: i64, bound: Option<NativeInvocation>) -> RuntimeResult<()> {
        self.native()?.execution().blocking_deadline()?;
        let proof = match bound {
            Some(proof) => proof,
            None => self.native()?.capture_removal(id)?,
        };
        let NativeInvocationKind::Remove(reason) = proof.kind() else {
            return Err(RuntimeError::InvalidInput("not a native removal"));
        };
        let replica = proof
            .replica()
            .ok_or(RuntimeError::InvalidInput("missing native removal target"))?
            .descriptor()?;
        if replica.replica_id != id {
            return Err(RuntimeError::InvalidInput("native removal ID mismatch"));
        }
        proof.enter()?;
        self.remove_frozen(replica, reason, proof.cancellation())?;
        proof.validate()
    }
}
impl IReplicator for EmptyControl {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        self.0.open(cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.close(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.0.change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.update_epoch(epoch, cancel)
    }
    fn get_current_progress(&self) -> RuntimeResult<i64> {
        Ok(self.0.progress()?.last)
    }
    fn get_catch_up_capability(&self) -> RuntimeResult<i64> {
        Ok(self.0.progress()?.first)
    }
    fn abort(&self) {
        self.0.abort();
    }
}
impl IPrimaryReplicator for EmptyControl {
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.0.on_data_loss(cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        cc: ReplicaSetConfig,
        pc: ReplicaSetConfig,
    ) -> RuntimeResult<()> {
        self.0.configure_sdk(cc, Some(pc), None)
    }
    fn update_current_replica_set_configuration(&self, cc: ReplicaSetConfig) -> RuntimeResult<()> {
        self.0.configure_sdk(cc, None, None)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        if mode == CatchUpMode::Invalid {
            return Box::pin(async { Err(RuntimeError::InvalidInput("invalid quorum mode")) });
        }
        self.0.wait_for_catch_up(mode, cancel)
    }
    fn build_replica(&self, replica: SdkReplica, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.build_sdk(replica, cancel, None)
    }
    fn remove_replica(&self, id: i64) -> RuntimeResult<()> {
        self.0.remove_sdk(id, None)
    }
}
impl IReplicator for Bound {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        self.root.open(cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.root.close(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.root.change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.root.update_epoch(epoch, cancel)
    }
    fn get_current_progress(&self) -> RuntimeResult<i64> {
        Ok(self.root.progress()?.last)
    }
    fn get_catch_up_capability(&self) -> RuntimeResult<i64> {
        Ok(self.root.progress()?.first)
    }
    fn abort(&self) {
        self.root.abort();
    }
}
impl IPrimaryReplicator for Bound {
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.root.on_data_loss(cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        cc: ReplicaSetConfig,
        pc: ReplicaSetConfig,
    ) -> RuntimeResult<()> {
        self.root
            .configure_sdk(cc, Some(pc), Some(self.proof.clone()))
    }
    fn update_current_replica_set_configuration(&self, cc: ReplicaSetConfig) -> RuntimeResult<()> {
        self.root.configure_sdk(cc, None, Some(self.proof.clone()))
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        if mode == CatchUpMode::Invalid {
            return Box::pin(async { Err(RuntimeError::InvalidInput("invalid quorum mode")) });
        }
        self.root.wait_for_catch_up(mode, cancel)
    }
    fn build_replica(&self, replica: SdkReplica, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.root
            .build_sdk(replica, cancel, Some(self.proof.clone()))
    }
    fn remove_replica(&self, id: i64) -> RuntimeResult<()> {
        self.root.remove_sdk(id, Some(self.proof.clone()))
    }
}
