//! Callback-ready weak provider owner. This private adapter forwards supported
//! A0 transaction/version kernels; it does not claim registry/collection products.
use super::{processor::KernelManager, provider::ProviderMode, status::Status};
use crate::*;
use fube_runtime::ServicePartitionAccessStatus as Access;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

pub(crate) struct ProviderOwner {
    pub(crate) manager: Arc<KernelManager>,
    pub(crate) partition: Arc<dyn IStatefulServicePartition>,
    pub(crate) opened: AtomicBool,
    pub(crate) ready: AtomicBool,
    pub(crate) read_after: AtomicI64,
}
impl ProviderOwner {
    pub(crate) fn writable(&self) -> Result<()> {
        self.manager.gate()?;
        if self.manager.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        // StateManager.cpp:4711–4754 permits transient access revocations;
        // only NotPrimary is an immediate write rejection. Lower remains authority.
        match self.partition.get_write_status()? {
            Access::Invalid => Err(Error::InvalidInput("invalid native write status")),
            Access::NotPrimary => Err(Status::NotPrimary.error()),
            _ => Ok(()),
        }
    }
    fn readable(&self) -> Result<()> {
        self.manager.gate()?;
        if self.manager.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        let status = self.partition.get_read_status()?;
        if status == Access::Granted
            || (self.manager.get_role() == ReplicaRole::ActiveSecondary && self.get_is_readable())
        {
            Ok(())
        } else {
            Err(Status::NotReadable.error())
        }
    }
    fn api(&self) -> Result<()> {
        if self.opened.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::ObjectClosed.error())
        }
    }
}
impl ITransactionVersionManager for ProviderOwner {
    fn register(&self) -> ApiFuture<'_, i64> {
        self.manager.register()
    }
    fn unregister(&self, version: i64) -> Result<()> {
        self.manager.unregister(version)
    }
}
impl IVersionManager for ProviderOwner {
    fn try_remove_checkpoint(&self, old: i64, next: i64) -> ApiFuture<'_, ()> {
        if let Err(error) = self.api() {
            return Box::pin(async move { Err(error) });
        }
        self.manager.try_remove_checkpoint(old, next)
    }
    fn try_remove_version(
        &self,
        id: i64,
        commit: i64,
        next: i64,
    ) -> Result<TryRemoveVersionResult> {
        self.api()?;
        self.manager.try_remove_version(id, commit, next)
    }
}
macro_rules! forward {
    ($name:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty) => {
        fn $name(&self, $($arg: $ty),*) -> $ret { self.manager.$name($($arg),*) }
    };
}
impl ITransactionManager for ProviderOwner {
    forward!(get_role() -> ReplicaRole);
    fn create_transaction(&self) -> Result<Arc<dyn Transaction>> {
        if self.manager.faults.is_closing() {
            return Err(Status::ObjectClosed.error());
        }
        self.manager.create_transaction()
    }
    forward!(create_atomic_operation() -> Result<Arc<dyn AtomicOperation>>);
    forward!(create_atomic_redo_operation() -> Result<Arc<dyn AtomicRedoOperation>>);
    forward!(
        single_operation_transaction_abort_unlock(
            id: i64,
            context: &dyn OperationContext,
        ) -> Result<()>
    );
    forward!(error_if_state_provider_is_not_registered(id: i64, transaction: i64) -> Result<()>);
    forward!(
        begin_transaction(
            transaction: &dyn Transaction,
            metadata: Option<OperationData>,
            undo: Option<OperationData>,
            redo: Option<OperationData>,
            id: i64,
            context: Option<Arc<dyn OperationContext>>,
        ) -> Result<()>
    );
    forward!(
        begin_transaction_async(
            transaction: Arc<dyn Transaction>,
            metadata: Option<OperationData>,
            undo: Option<OperationData>,
            redo: Option<OperationData>,
            id: i64,
            context: Option<Arc<dyn OperationContext>>,
        ) -> ApiFuture<'_, i64>
    );
    forward!(
        add_operation(
            transaction: &dyn Transaction,
            metadata: Option<OperationData>,
            undo: Option<OperationData>,
            redo: Option<OperationData>,
            id: i64,
            context: Option<Arc<dyn OperationContext>>,
        ) -> Result<()>
    );
    forward!(
        add_atomic_operation(
            transaction: Arc<dyn AtomicOperation>,
            metadata: Option<OperationData>,
            undo: Option<OperationData>,
            redo: Option<OperationData>,
            id: i64,
            context: Option<Arc<dyn OperationContext>>,
        ) -> ApiFuture<'_, i64>
    );
    forward!(
        add_atomic_redo_operation(
            transaction: Arc<dyn AtomicRedoOperation>,
            metadata: Option<OperationData>,
            redo: Option<OperationData>,
            id: i64,
            context: Option<Arc<dyn OperationContext>>,
        ) -> ApiFuture<'_, i64>
    );
    forward!(commit_transaction(transaction: Arc<dyn Transaction>) -> ApiFuture<'_, i64>);
    forward!(abort_transaction(transaction: Arc<dyn Transaction>) -> ApiFuture<'_, i64>);
}
impl IStateProviderMap for ProviderOwner {
    fn get(&self, name: &str) -> Result<Arc<dyn IStateProvider2>> {
        self.readable()?;
        self.manager
            .directory
            .entries
            .lock()
            .unwrap()
            .values()
            .find(|entry| entry.mode == ProviderMode::Active && entry.provider.get_name() == name)
            .map(|entry| entry.provider.clone())
            .ok_or_else(|| Status::NameDoesNotExist.error())
    }
    fn add(
        &self,
        _: Arc<dyn Transaction>,
        _: String,
        _: String,
        _: Option<OperationData>,
        _: Option<Duration>,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn remove(
        &self,
        _: Arc<dyn Transaction>,
        _: String,
        _: Option<Duration>,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn create_enumerator(&self, _: bool) -> Result<Arc<dyn IStateProviderEnumerator>> {
        Err(Status::NotImplemented.error())
    }
    fn get_or_add(
        &self,
        _: Arc<dyn Transaction>,
        _: String,
        _: String,
        _: Option<OperationData>,
        _: Option<Duration>,
        _: Cancellation,
    ) -> ApiFuture<'_, GetOrAddResult> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}
impl ITransactionalReplicator for ProviderOwner {
    fn get_is_readable(&self) -> bool {
        if !self.ready.load(Ordering::Acquire)
            || self.manager.gate().is_err()
            || self.manager.faults.is_closing()
        {
            return false;
        }
        // LoggingReplicatorImpl.cpp:422–432: primary native access; otherwise
        // last applied boundary versus recovery/copied consistency frontier.
        if self.manager.get_role() == ReplicaRole::Primary {
            self.partition
                .get_read_status()
                .is_ok_and(|status| status == Access::Granted)
        } else {
            self.manager.version() >= self.read_after.load(Ordering::Acquire)
        }
    }
    fn get_stateful_partition(&self) -> Arc<dyn IStatefulServicePartition> {
        self.partition.clone()
    }
    fn get_has_persisted_state(&self) -> bool {
        false
    }
    fn get_last_stable_sequence_number(&self) -> Result<i64> {
        self.api()?;
        Ok(self.manager.stable())
    }
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.api()?;
        Ok(self.manager.tail())
    }
    fn get_current_epoch(&self) -> Result<Epoch> {
        self.api()?;
        Ok(self.manager.epoch())
    }
    fn register_transaction_change_handler(
        &self,
        _: Arc<dyn ITransactionChangeHandler>,
    ) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn unregister_transaction_change_handler(&self) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn register_state_manager_change_handler(
        &self,
        _: Arc<dyn IStateManagerChangeHandler>,
    ) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn unregister_state_manager_change_handler(&self) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn backup(&self, _: Arc<dyn IBackupCallbackHandler>) -> ApiFuture<'_, BackupInfo> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn backup_with_options(
        &self,
        _: Arc<dyn IBackupCallbackHandler>,
        _: BackupOption,
        _: Option<Duration>,
        _: Cancellation,
    ) -> ApiFuture<'_, BackupInfo> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn restore(&self, _: PathBuf) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn restore_with_options(
        &self,
        _: PathBuf,
        _: RestorePolicy,
        _: Option<Duration>,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}
