// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Compile-only consumers of internal signatures; no mock engines or providers.

use super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "compile-only witness exercises distinct internal signature inputs"
)]
fn state_provider_manager(
    manager: &dyn IStateProviderManager,
    transaction: Arc<dyn TransactionBase>,
    context: &dyn OperationContext,
    data: OperationData,
    path: PathBuf,
    token: Cancellation,
    apply: ApplyContext,
    role: ReplicaRole,
) {
    let _: ApiFuture<'_, Option<Arc<dyn OperationContext>>> =
        manager.apply(1, transaction, apply, None, Some(data.clone()));
    let _: Result<()> = manager.unlock(context);
    let _: Result<()> = manager.prepare_checkpoint(0);
    let _: ApiFuture<'_, ()> = manager.perform_checkpoint(token.clone());
    let _: ApiFuture<'_, ()> = manager.complete_checkpoint(token.clone());
    let _: ApiFuture<'_, ()> = manager.backup_checkpoint(path.clone(), token.clone());
    let _: ApiFuture<'_, ()> = manager.restore_checkpoint(path, token.clone());
    let _: Result<Arc<dyn IOperationDataStream>> = manager.get_current_state(42);
    let _: ApiFuture<'_, ()> = manager.begin_setting_current_state();
    let _: ApiFuture<'_, ()> = manager.set_current_state(1, data);
    let _: ApiFuture<'_, ()> = manager.end_setting_current_state();
    let _: ApiFuture<'_, ()> = manager.change_role(role, token);
}

fn transaction_state_manager(
    manager: &dyn ITransactionStateManager,
    transaction: Arc<dyn Transaction>,
    atomic: Arc<dyn AtomicOperation>,
    atomic_redo: Arc<dyn AtomicRedoOperation>,
    context: Arc<dyn OperationContext>,
    data: OperationData,
) {
    let _: Result<()> = manager.single_operation_transaction_abort_unlock(7, context.as_ref());
    let _: Result<()> = manager.error_if_state_provider_is_not_registered(7, 9);
    let _: ApiFuture<'_, SequenceNumber> = manager.begin_transaction_async(
        transaction.clone(),
        7,
        None,
        Some(data.clone()),
        None,
        Some(context.clone()),
    );
    let _: Result<()> = manager.begin_transaction(
        transaction.as_ref(),
        7,
        Some(data.clone()),
        None,
        None,
        Some(context.clone()),
    );
    let _: Result<()> = manager.add_operation(
        transaction.as_ref(),
        7,
        None,
        None,
        Some(data.clone()),
        Some(context.clone()),
    );
    let _: ApiFuture<'_, SequenceNumber> = manager.add_atomic_operation(
        atomic,
        7,
        None,
        Some(data.clone()),
        None,
        Some(context.clone()),
    );
    let _: ApiFuture<'_, SequenceNumber> =
        manager.add_atomic_redo_operation(atomic_redo, 7, None, Some(data), Some(context));
}

fn state_manager(
    manager: &dyn IStateManager,
    owner: Weak<dyn ITransactionalReplicator>,
    logging: Weak<dyn ILoggingReplicator>,
    handler: Arc<dyn IStateManagerChangeHandler>,
    token: Cancellation,
    role: ReplicaRole,
) {
    let _: &dyn IStateProviderMap = manager;
    let _: &dyn ITransactionStateManager = manager;
    let _: &dyn IStateProviderManager = manager;
    let _: Result<()> = manager.initialize(owner, logging);
    let _: ApiFuture<'_, ()> = manager.open(true, false, token.clone());
    let _: ApiFuture<'_, ()> = manager.change_role(role, token.clone());
    let _: ApiFuture<'_, ()> = manager.close(token);
    let _: Result<()> = manager.register_state_manager_change_handler(handler);
    let _: Result<()> = manager.unregister_state_manager_change_handler();
}

#[allow(
    clippy::too_many_arguments,
    reason = "compile-only witness exercises distinct internal signature inputs"
)]
fn logging_replicator(
    logging: &dyn ILoggingReplicator,
    information: RecoveryInformation,
    delegate: StateManagerBecomeSecondaryDelegate,
    backup: Arc<dyn IBackupCallbackHandler>,
    handler: Arc<dyn ITransactionChangeHandler>,
    path: PathBuf,
    token: Cancellation,
    role: ReplicaRole,
) {
    let _: &dyn ITransactionManager = logging;
    let _: &dyn IVersionManager = logging;
    let _: &dyn crate::ITransactionVersionManager = logging;
    let _: bool = information.log_complete_checkpoint_after_recovery;
    let _: bool = information.should_remove_local_state_due_to_incomplete_restore;
    let _: bool = information.should_skip_recovery_due_to_incomplete_change_role_none;
    let _: ApiFuture<'static, ()> = delegate(role, token.clone());
    let _: bool = logging.get_has_persisted_state();
    let _: ApiFuture<'_, RecoveryInformation> = logging.open();
    let _: ApiFuture<'_, ()> = logging.perform_recovery(information);
    let _: ApiFuture<'_, ()> = logging.close();
    let _: ApiFuture<'_, ()> = logging.become_active_secondary(delegate);
    let _: ApiFuture<'_, ()> = logging.become_idle_secondary();
    let _: ApiFuture<'_, ()> = logging.become_none();
    let _: ApiFuture<'_, ()> = logging.become_primary(false);
    let _: ApiFuture<'_, ()> = logging.delete_log();
    let _: Result<SequenceNumber> = logging.get_safe_lsn_to_remove_state_provider();
    let _: bool = logging.is_readable();
    let _: Result<SequenceNumber> = logging.get_last_stable_sequence_number();
    let _: Result<SequenceNumber> = logging.get_last_committed_sequence_number();
    let _: Result<Epoch> = logging.get_current_epoch();
    let _: ApiFuture<'_, BackupInfo> = logging.backup(backup.clone());
    let _: ApiFuture<'_, BackupInfo> =
        logging.backup_with_options(backup, BackupOption::Full, None, token.clone());
    let _: ApiFuture<'_, ()> = logging.restore(path.clone());
    let _: ApiFuture<'_, ()> = logging.restore_with_options(path, RestorePolicy::Safe, None, token);
    let _: Result<()> = logging.register_transaction_change_handler(handler);
    let _: Result<()> = logging.unregister_transaction_change_handler();
    let _: Result<()> = logging.dispose();
}
