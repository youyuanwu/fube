//! Immutable native proof projection onto the same canonical control methods.
use super::*;
use fube_runtime::{ErrorKind, ReplicaSetQuorumMode};

#[cfg(test)]
pub(super) struct NativeBinder(pub Weak<Engine>);
pub(super) struct NativeProgress(pub Weak<Engine>);
impl fube_runtime::extensions::NativeProgressSource for NativeProgress {
    fn sample_progress(&self) -> Result<ReplicationProgress> {
        self.0.upgrade().ok_or(Error::Closed)?.progress()
    }
}
#[cfg(test)]
impl NativeControlBinding for NativeBinder {
    fn bind_invocation(&self, proof: NativeInvocation) -> Result<Arc<dyn IPrimaryReplicator>> {
        proof.validate()?;
        let engine = self.0.upgrade().ok_or(Error::Closed)?;
        let native = engine.native_runtime.as_ref().ok_or(Error::Closed)?;
        if native.binding() != proof.binding()
            || proof.actor() != Some(engine.endpoint.actor.incarnation)
        {
            return Err(Error::InvalidInput(
                "native invocation does not bind this engine",
            ));
        }
        if engine.native_root_dispatch.load(Ordering::Acquire)
            && proof.kind() != NativeInvocationKind::Remove(ReplicaRemoval::BuildRetryCleanup)
        {
            return Ok(Arc::new(crate::FabricReplicator { engine }));
        }
        Ok(Arc::new(BoundControl { engine, proof }))
    }
}
#[cfg(test)]
struct BoundControl {
    engine: Arc<Engine>,
    proof: NativeInvocation,
}

impl Engine {
    pub(super) fn native_required(&self) -> Result<&NativeRuntimeIntegration> {
        self.native_runtime.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "standalone engine has no native invocation issuer",
            )
        })
    }
    fn proof_prewrite(proof: &NativeInvocation) -> Option<NativePreWrite> {
        proof.prewrite_epochs().map(|(cc, pc)| NativePreWrite {
            current: Epoch {
                data_loss_number: cc.data_loss,
                configuration_number: cc.configuration,
            },
            previous: Epoch {
                data_loss_number: pc.data_loss,
                configuration_number: pc.configuration,
            },
        })
    }
    #[cfg(test)]
    pub(super) fn configure_authorized(
        &self,
        current: ReplicaSetConfiguration,
        previous: Option<ReplicaSetConfiguration>,
        cancellation: Cancellation,
        authority: Result<Option<NativeInvocation>>,
    ) -> ServiceFuture<'_, ()> {
        let authority = authority.and_then(|proof| {
            if let Some(proof) = &proof {
                let kind = if previous.is_some() {
                    NativeInvocationKind::CatchUpConfiguration
                } else {
                    NativeInvocationKind::CurrentConfiguration
                };
                let (cc, pc) = proof
                    .configuration()
                    .ok_or(Error::InvalidInput("missing native configuration"))?;
                if proof.kind() != kind
                    || cc.descriptor()? != current
                    || pc.map(|pc| pc.descriptor()).transpose()? != previous
                {
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
                Some(proof) => Ok(Self::proof_prewrite(proof)),
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
        // All authority resolution is eager, including for an unpolled future.
        let authority = authority.and_then(|proof| {
            if let Some(proof) = &proof {
                if proof.kind() != NativeInvocationKind::Build
                    || proof
                        .replica()
                        .map(|r| r.descriptor())
                        .transpose()?
                        .as_ref()
                        != Some(&replica)
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
    #[cfg(test)]
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
                    || proof
                        .replica()
                        .map(|r| r.descriptor())
                        .transpose()?
                        .as_ref()
                        != Some(&replica)
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
    fn configure_sync(
        &self,
        current: ReplicaSetConfig,
        previous: Option<ReplicaSetConfig>,
        bound: Option<NativeInvocation>,
    ) -> Result<()> {
        let deadline = self.shared.execution.blocking_deadline()?;
        ExecutionRuntime::reject_engine_reentry(self.shared.engine_id)?;
        let proof = match bound {
            Some(proof) => proof,
            None => self
                .native_required()?
                .capture_configuration(&current, previous.as_ref())?,
        };
        let kind = if previous.is_some() {
            NativeInvocationKind::CatchUpConfiguration
        } else {
            NativeInvocationKind::CurrentConfiguration
        };
        let (cc, pc) = proof
            .configuration()
            .ok_or(Error::InvalidInput("missing frozen native configuration"))?;
        if proof.kind() != kind
            || cc.sdk()? != current
            || pc.map(|pc| pc.sdk()).transpose()? != previous
        {
            return Err(Error::InvalidInput(
                "configuration differs from frozen native projection",
            ));
        }
        let current = cc.descriptor()?;
        let previous = pc.map(|pc| pc.descriptor()).transpose()?;
        let native_prewrite = Self::proof_prewrite(&proof);
        let cancellation = proof.cancellation();
        proof.enter()?;
        #[cfg(test)]
        {
            *self.native_configuration.lock().unwrap() = Some(proof.clone());
        }
        self.request_blocking(Some(proof), |reply| Command::Configure {
            current,
            previous,
            native_prewrite,
            cancellation,
            reply,
            deadline: Instant::from_std(deadline)
                .min(Instant::now() + self.settings.callback_timeout),
        })
    }
    fn build_sdk(
        &self,
        replica: SdkReplicaInformation,
        cancellation: Cancellation,
        bound: Option<NativeInvocation>,
    ) -> ServiceFuture<'_, ()> {
        let captured = (|| {
            let proof = match bound {
                Some(proof) => proof,
                None => self.native_required()?.capture_build(&replica)?,
            };
            if proof.kind() != NativeInvocationKind::Build
                || proof.replica().map(|r| r.sdk()).transpose()?.as_ref() != Some(&replica)
            {
                return Err(Error::InvalidInput(
                    "build differs from frozen native target",
                ));
            }
            let descriptor = proof
                .replica()
                .ok_or(Error::InvalidInput("missing native build target"))?
                .descriptor()?;
            Ok((proof, descriptor))
        })();
        match captured {
            Ok((proof, descriptor)) => {
                #[cfg(test)]
                if let Some((captured, release)) = self.native_build_pause.lock().unwrap().take() {
                    let _ = captured.send(proof.clone());
                    let future = self.build_authorized(descriptor, cancellation, Ok(Some(proof)));
                    return Box::pin(async move {
                        let _ = release.await;
                        future.await
                    });
                }
                self.build_authorized(descriptor, cancellation, Ok(Some(proof)))
            }
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
    fn remove_sync(&self, replica_id: i64, bound: Option<NativeInvocation>) -> Result<()> {
        let deadline = self.shared.execution.blocking_deadline()?;
        ExecutionRuntime::reject_engine_reentry(self.shared.engine_id)?;
        let proof = match bound {
            Some(proof) => proof,
            None => self.native_required()?.capture_removal(replica_id)?,
        };
        let NativeInvocationKind::Remove(reason) = proof.kind() else {
            return Err(Error::InvalidInput("not a native removal invocation"));
        };
        let descriptor = proof
            .replica()
            .ok_or(Error::InvalidInput("missing native removal target"))?
            .descriptor()?;
        if descriptor.replica_id != replica_id {
            return Err(Error::InvalidInput(
                "removal differs from frozen native target",
            ));
        }
        let cancellation = proof.cancellation();
        proof.enter()?;
        #[cfg(test)]
        if let Some((captured, release)) = self.native_remove_pause.lock().unwrap().take() {
            let _ = captured.send(proof.clone());
            release
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("native remove setup release");
        }
        self.request_blocking(Some(proof), |reply| {
            Command::Remove(
                descriptor,
                reason,
                cancellation,
                reply,
                Instant::from_std(deadline).min(Instant::now() + self.settings.callback_timeout),
            )
        })
    }
}
impl IReplicator for Engine {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        Engine::open(self, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        Engine::close(self, cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Engine::change_role(self, epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        Engine::update_epoch(self, epoch, cancel)
    }
    fn get_current_progress(&self) -> Result<i64> {
        #[cfg(test)]
        self.scalar_progress_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.progress()?.last)
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        #[cfg(test)]
        self.scalar_progress_reads.fetch_add(1, Ordering::SeqCst);
        if self.native_runtime.is_some() && self.snapshot().role == ReplicaRole::Unknown {
            self.check()?;
            if !self.shared.opened.load(Ordering::Acquire) {
                return Err(Error::InvalidInput("replicator is not Opened"));
            }
            self.native_required()?.validate()?;
            return Ok(0);
        }
        Ok(self.progress()?.first)
    }
    fn abort(&self) {
        Engine::abort(self);
    }
}
impl IPrimaryReplicator for Engine {
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        let native = self.native_required().and_then(|native| {
            native.validate()?;
            Ok(native.clone())
        });
        Box::pin(async move {
            let native = native?;
            native.validate()?;
            let result = Engine::on_data_loss(self, cancel).await?;
            native.validate().map_err(Error::uncertain)?;
            Ok(result)
        })
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.configure_sync(current, Some(previous), None)
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.configure_sync(current, None, None)
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        if mode == ReplicaSetQuorumMode::Invalid {
            return Box::pin(async { Err(Error::InvalidInput("invalid quorum mode")) });
        }
        let native = self.native_required().and_then(|native| {
            native.validate()?;
            Ok(native.clone())
        });
        Box::pin(async move {
            let native = native?;
            native.validate()?;
            self.wait_for_catch_up(mode, cancel).await?;
            native.validate().map_err(Error::uncertain)
        })
    }
    fn build_replica(
        &self,
        replica: SdkReplicaInformation,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.build_sdk(replica, cancel, None)
    }
    fn remove_replica(&self, replica_id: i64) -> Result<()> {
        self.remove_sync(replica_id, None)
    }
}
#[cfg(test)]
impl IReplicator for BoundControl {
    fn open(&self, cancel: Cancellation) -> ServiceFuture<'_, String> {
        IReplicator::open(&*self.engine, cancel)
    }
    fn close(&self, cancel: Cancellation) -> ServiceFuture<'_, ()> {
        IReplicator::close(&*self.engine, cancel)
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        IReplicator::change_role(&*self.engine, epoch, role, cancel)
    }
    fn update_epoch(&self, epoch: Epoch, cancel: Cancellation) -> ServiceFuture<'_, ()> {
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
#[cfg(test)]
impl IPrimaryReplicator for BoundControl {
    fn on_data_loss(&self, cancel: Cancellation) -> ServiceFuture<'_, bool> {
        self.engine.on_data_loss(cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        self.engine
            .configure_sync(current, Some(previous), Some(self.proof.clone()))
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        self.engine
            .configure_sync(current, None, Some(self.proof.clone()))
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        IPrimaryReplicator::wait_for_catch_up_quorum(&*self.engine, mode, cancel)
    }
    fn build_replica(
        &self,
        replica: SdkReplicaInformation,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.engine
            .build_sdk(replica, cancel, Some(self.proof.clone()))
    }
    fn remove_replica(&self, replica_id: i64) -> Result<()> {
        self.engine
            .remove_sync(replica_id, Some(self.proof.clone()))
    }
}
