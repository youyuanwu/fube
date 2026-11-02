//! Lower command ingress. Native capture/entry belongs to the runtime adapter.
use super::*;
use fube_core::{ErrorKind, ReplicaSetQuorumMode};

impl Engine {
    pub(super) fn validate_host(&self) -> Result<()> {
        self.host
            .as_ref()
            .map_or(Ok(()), |host| host.validity.validate())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn configure_authorized(
        &self,
        current: ReplicaSetConfiguration,
        previous: Option<ReplicaSetConfiguration>,
        cancellation: Cancellation,
        check: Result<Option<Arc<dyn MutationCheck>>>,
    ) -> ServiceFuture<'_, ()> {
        #[cfg(test)]
        let prewrite = self.native_prewrite();
        #[cfg(not(test))]
        let prewrite: Result<Option<NativePreWrite>> = Ok(None);
        Box::pin(async move {
            let prewrite = prewrite?;
            self.request_authorized(check?, |reply| Command::Configure {
                current,
                previous,
                native_prewrite: prewrite,
                cancellation,
                reply,
                deadline: Instant::now() + self.settings.callback_timeout,
            })
            .await
        })
    }

    pub(crate) fn build_authorized(
        &self,
        replica: ReplicaInformation,
        cancellation: Cancellation,
        check: Option<Arc<dyn MutationCheck>>,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request_authorized(check, |reply| Command::Build(replica, cancellation, reply))
                .await
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn remove_authorized(
        &self,
        replica: ReplicaInformation,
        reason: ReplicaRemoval,
        cancellation: Cancellation,
        check: Result<Option<Arc<dyn MutationCheck>>>,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.request_authorized(check?, |reply| {
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

    pub(crate) fn configure_hosted(
        &self,
        current: ReplicaSetConfiguration,
        previous: Option<ReplicaSetConfiguration>,
        native_prewrite: Option<NativePreWrite>,
        cancellation: Cancellation,
        check: Arc<dyn MutationCheck>,
    ) -> Result<()> {
        let deadline = self.shared.execution.blocking_deadline()?;
        self.request_blocking(Some(check), |reply| Command::Configure {
            current,
            previous,
            native_prewrite,
            cancellation,
            reply,
            deadline: Instant::from_std(deadline)
                .min(Instant::now() + self.settings.callback_timeout),
        })
    }

    pub(crate) fn remove_hosted(
        &self,
        replica: ReplicaInformation,
        purpose: ReplicaRemoval,
        cancellation: Cancellation,
        check: Arc<dyn MutationCheck>,
    ) -> Result<()> {
        let deadline = self.shared.execution.blocking_deadline()?;
        self.request_blocking(Some(check), |reply| {
            Command::Remove(
                replica,
                purpose,
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
        #[cfg(any(test, feature = "test-hooks"))]
        self.scalar_progress_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.progress()?.last)
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        #[cfg(any(test, feature = "test-hooks"))]
        self.scalar_progress_reads.fetch_add(1, Ordering::SeqCst);
        if self.host.is_some() && self.snapshot().role == ReplicaRole::Unknown {
            self.check()?;
            if !self.shared.opened.load(Ordering::Acquire) {
                return Err(Error::InvalidInput("replicator is not Opened"));
            }
            self.validate_host()?;
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
        Engine::on_data_loss(self, cancel)
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        _: ReplicaSetConfig,
        _: ReplicaSetConfig,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "host control adapter required",
        ))
    }
    fn update_current_replica_set_configuration(&self, _: ReplicaSetConfig) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "host control adapter required",
        ))
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancel: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        if mode == ReplicaSetQuorumMode::Invalid {
            return Box::pin(async { Err(Error::InvalidInput("invalid quorum mode")) });
        }
        self.wait_for_catch_up(mode, cancel)
    }
    fn build_replica(&self, _: SdkReplicaInformation, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async {
            Err(Error::new(
                ErrorKind::Unsupported,
                "host control adapter required",
            ))
        })
    }
    fn remove_replica(&self, _: i64) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "host control adapter required",
        ))
    }
}
