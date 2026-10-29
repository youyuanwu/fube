//! Invocation-bound control views over the same owner; no control-operation
//! extension trait and no SDK discovery members.
use super::*;
use fube_runtime::extensions::{NativeReplica, NativeReplicaSet};
fn native_role(role: ReplicaRole) -> i64 {
    match role {
        ReplicaRole::Unknown => 0,
        ReplicaRole::None => 1,
        ReplicaRole::Idle => 2,
        ReplicaRole::Secondary => 3,
        ReplicaRole::Primary => 4,
    }
}
fn replica_matches(native: &NativeReplica, replica: &ReplicaInformation) -> bool {
    native.node_id == replica.node_id
        && native.node_instance == replica.node_instance
        && native.replica_id == replica.replica_id
        && native.incarnation == replica.incarnation
        && native.role == native_role(replica.role)
        && native.previous_role == native_role(replica.previous_role)
        && native.up == replica.up
        && native.ready == replica.ready
        && native.address == replica.address
        && native.first_lsn == replica.progress.first
        && native.last_lsn == replica.progress.last
        && native.must_catch_up == replica.must_catch_up
}
fn configuration_matches(native: &NativeReplicaSet, config: &ReplicaSetConfiguration) -> bool {
    native.epoch.data_loss == config.epoch.data_loss
        && native.epoch.configuration == config.epoch.configuration
        && native.write_quorum == config.write_quorum
        && native.local_member == config.local_member
        && native.replicas.len() == config.replicas.len()
        && native
            .replicas
            .iter()
            .zip(&config.replicas)
            .all(|(native, replica)| replica_matches(native, replica))
}
pub(super) struct NativeBinder(pub Weak<Engine>);
impl NativeControlBinding for NativeBinder {
    fn bind_invocation(&self, proof: NativeInvocation) -> Result<Arc<dyn Replicator>> {
        proof.validate()?;
        let engine = self.0.upgrade().ok_or(Error::Closed)?;
        let binding = engine
            .native_runtime
            .as_ref()
            .ok_or(Error::Closed)?
            .binding();
        if binding != proof.binding() || proof.actor() != Some(engine.endpoint.actor.incarnation) {
            return Err(Error::InvalidInput(
                "native invocation does not bind this engine",
            ));
        }
        Ok(Arc::new(BoundControl { engine, proof }))
    }
}
struct BoundControl {
    engine: Arc<Engine>,
    proof: NativeInvocation,
}
impl Engine {
    pub(super) fn configure_authorized(
        &self,
        current: ReplicaSetConfiguration,
        previous: Option<ReplicaSetConfiguration>,
        cancellation: Cancellation,
        authority: Result<Option<NativeInvocation>>,
    ) -> ServiceFuture<'_, ()> {
        let authority = authority.and_then(|proof| {
            if let Some(proof) = &proof {
                let expected_kind = if previous.is_some() {
                    NativeInvocationKind::CatchUpConfiguration
                } else {
                    NativeInvocationKind::CurrentConfiguration
                };
                let matches = proof.configuration().is_some_and(|(cc, pc)| {
                    configuration_matches(cc, &current)
                        && match (pc, previous.as_ref()) {
                            (None, None) => true,
                            (Some(pc), Some(previous)) => configuration_matches(pc, previous),
                            _ => false,
                        }
                });
                if proof.kind() != expected_kind || !matches {
                    return Err(Error::InvalidInput(
                        "configuration differs from frozen native projection",
                    ));
                }
                proof.enter()?;
            }
            Ok(proof)
        });
        let prewrite = authority
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|proof| match proof {
                Some(proof) => {
                    Ok(proof
                        .prewrite_epochs()
                        .map(|(current, previous)| NativePreWrite {
                            current: Epoch {
                                data_loss: current.data_loss,
                                configuration: current.configuration,
                            },
                            previous: Epoch {
                                data_loss: previous.data_loss,
                                configuration: previous.configuration,
                            },
                        }))
                }
                None => self.native_prewrite(),
            });
        Box::pin(async move {
            let proof = authority?;
            let native_prewrite = prewrite?;
            self.request_authorized(proof, |reply| Command::Configure {
                current,
                previous,
                native_prewrite,
                cancellation,
                reply,
                deadline: Instant::now() + self.settings.callback_timeout,
            })
            .await
        })
    }
    pub(super) fn build_authorized(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
        authority: Result<Option<NativeInvocation>>,
    ) -> ServiceFuture<'_, ()> {
        let authority = authority.and_then(|proof| {
            if let Some(proof) = &proof {
                if proof.kind() != NativeInvocationKind::Build
                    || !proof
                        .replica()
                        .is_some_and(|native| replica_matches(native, &replica))
                {
                    return Err(Error::InvalidInput(
                        "build differs from frozen native target",
                    ));
                }
                proof.enter()?;
            }
            Ok(proof)
        });
        Box::pin(async move {
            self.request_authorized(authority?, |reply| {
                Command::Build(replica, cancellation, reply)
            })
            .await
        })
    }
    pub(super) fn remove_authorized(
        &self,
        replica: ReplicaInformation,
        reason: ReplicaRemoval,
        cancellation: Cancellation,
        authority: Result<Option<NativeInvocation>>,
    ) -> ServiceFuture<'_, ()> {
        let authority = authority.and_then(|proof| {
            if let Some(proof) = &proof {
                if proof.kind() != NativeInvocationKind::Remove(reason)
                    || !proof
                        .replica()
                        .is_some_and(|native| replica_matches(native, &replica))
                {
                    return Err(Error::InvalidInput(
                        "removal differs from frozen native target/purpose",
                    ));
                }
                proof.enter()?;
            }
            Ok(proof)
        });
        Box::pin(async move {
            self.request_authorized(authority?, |reply| {
                Command::Remove(
                    replica,
                    reason,
                    cancellation,
                    reply,
                    Instant::now() + self.settings.callback_timeout,
                )
            })
            .await
        })
    }
}
impl Replicator for BoundControl {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        self.engine.open(cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.engine.change_role(epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.engine.update_epoch(epoch, cancel)
    }
    fn progress(&self) -> Result<ReplicationProgress> {
        self.engine.progress()
    }
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.engine.on_data_loss(cancel)
    }
    fn update_catch_up_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.engine.configure_authorized(
            current,
            Some(previous),
            cancel,
            Ok(Some(self.proof.clone())),
        )
    }
    fn update_current_configuration(
        &self,
        current: ReplicaSetConfiguration,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.engine
            .configure_authorized(current, None, cancel, Ok(Some(self.proof.clone())))
    }
    fn wait_for_catch_up(&self, mode: CatchUpMode, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.engine.wait_for_catch_up(mode, cancel)
    }
    fn build_replica(
        &self,
        replica: ReplicaInformation,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.engine
            .build_authorized(replica, cancel, Ok(Some(self.proof.clone())))
    }
    fn remove_replica(
        &self,
        replica: ReplicaInformation,
        reason: ReplicaRemoval,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.engine
            .remove_authorized(replica, reason, cancel, Ok(Some(self.proof.clone())))
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        self.engine.close(cancel)
    }
    fn abort(&self) {
        self.engine.abort();
    }
}
