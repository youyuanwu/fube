//! Compile-only public consumer witnesses. No engines, providers, transactions
//! or default implementations are supplied, and these functions are not run.
#![allow(dead_code)]

use fube_txn::{
    ApiFuture, ApplyContext, AtomicOperation, AtomicRedoOperation, BackupInfo, BackupOption,
    Cancellation, EnumerationCompletionResult, Epoch, FactoryArguments, GetOrAddResult,
    IBackupCallbackHandler, IDataLossHandler, IOperationDataStream, IStateManagerChangeHandler,
    IStateProvider2, IStateProvider2Factory, IStateProviderEnumerator, IStateProviderMap,
    IStatefulServicePartition, ITransaction, ITransactionChangeHandler, ITransactionManager,
    ITransactionVersionManager, ITransactionalReplicator, IVersionManager, LockContext,
    OperationContext, OperationData, ReplicaRole, RestorePolicy, Result, SequenceNumber,
    StateProviderEntry, StateProviderInfo, Transaction, TransactionBase, TransactionState,
    TryRemoveVersionResult,
};
use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::Duration,
};

fn inheritance(root: Arc<dyn ITransactionalReplicator>, transaction: Arc<dyn Transaction>) {
    let _: &dyn IStateProviderMap = root.as_ref();
    let _: &dyn ITransactionManager = root.as_ref();
    let _: &dyn ITransactionVersionManager = root.as_ref();
    let _: &dyn IVersionManager = root.as_ref();
    let _: Arc<dyn IStateProviderMap> = root.clone();
    let _: Arc<dyn ITransactionManager> = root.clone();
    let _: Arc<dyn IVersionManager> = root;
    let _: &dyn ITransaction = transaction.as_ref();
    let _: &dyn TransactionBase = transaction.as_ref();
    let _: Arc<dyn TransactionBase> = transaction.clone();
    let _: Arc<dyn ITransaction> = transaction;
}

fn transactions(
    transaction: Arc<dyn Transaction>,
    atomic: Arc<dyn AtomicOperation>,
    atomic_redo: Arc<dyn AtomicRedoOperation>,
    context: Arc<dyn OperationContext>,
    lock: Arc<dyn LockContext>,
    cancellation: Cancellation,
) {
    let _: &dyn Any = context.as_ref();
    let _: bool = transaction.get_is_primary_transaction();
    let _: i64 = transaction.get_transaction_id();
    let _: bool = transaction.get_is_atomic_operation();
    let _: bool = transaction.get_is_valid_transaction();
    let _: TransactionState = transaction.get_state();
    let _: SequenceNumber = transaction.get_commit_sequence_number();
    let _: Result<()> = transaction.set_commit_sequence_number(10);
    let _: bool = lock.is_tracking_context();
    let _: Result<()> = lock.unlock();
    let _: Result<()> = transaction.add_lock_context(lock);
    let _: Result<()> = transaction.add_operation(None, None, None, 1, Some(context.clone()));
    let _: ApiFuture<'_, SequenceNumber> = transaction.get_visibility_sequence_number();
    let _: ApiFuture<'_, ()> = transaction.commit();
    let _: ApiFuture<'_, ()> = transaction.commit_with_options(None, cancellation.clone());
    let _: ApiFuture<'_, ()> = transaction.commit_with_options(Some(Duration::ZERO), cancellation);
    let _: Result<()> = transaction.abort();
    let _: Result<()> = transaction.dispose();
    let _: &dyn TransactionBase = atomic.as_ref();
    let _: ApiFuture<'_, ()> = atomic.add_operation(None, None, None, 1, Some(context.clone()));
    let _: &dyn TransactionBase = atomic_redo.as_ref();
    let _: ApiFuture<'_, ()> = atomic_redo.add_operation(None, None, 1, Some(context));
}

fn manager(
    manager: &dyn ITransactionManager,
    transaction: Arc<dyn Transaction>,
    atomic: Arc<dyn AtomicOperation>,
    atomic_redo: Arc<dyn AtomicRedoOperation>,
    context: Arc<dyn OperationContext>,
    payload: OperationData,
) {
    let _: ReplicaRole = manager.get_role();
    let _: Result<Arc<dyn Transaction>> = manager.create_transaction();
    let _: Result<Arc<dyn AtomicOperation>> = manager.create_atomic_operation();
    let _: Result<Arc<dyn AtomicRedoOperation>> = manager.create_atomic_redo_operation();
    let _: Result<()> = manager.single_operation_transaction_abort_unlock(1, context.as_ref());
    let _: Result<()> = manager.error_if_state_provider_is_not_registered(1, 2);
    let _: Result<()> = manager.begin_transaction(
        transaction.as_ref(),
        Some(payload.clone()),
        None,
        None,
        1,
        Some(context.clone()),
    );
    let _: ApiFuture<'_, SequenceNumber> = manager.begin_transaction_async(
        transaction.clone(),
        None,
        None,
        None,
        1,
        Some(context.clone()),
    );
    let _: Result<()> = manager.add_operation(
        transaction.as_ref(),
        None,
        None,
        None,
        1,
        Some(context.clone()),
    );
    let _: ApiFuture<'_, SequenceNumber> =
        manager.add_atomic_operation(atomic, None, None, None, 1, Some(context.clone()));
    let _: ApiFuture<'_, SequenceNumber> =
        manager.add_atomic_redo_operation(atomic_redo, None, Some(payload), 1, Some(context));
    let _: ApiFuture<'_, SequenceNumber> = manager.commit_transaction(transaction.clone());
    let _: ApiFuture<'_, SequenceNumber> = manager.abort_transaction(transaction);
    let _: ApiFuture<'_, SequenceNumber> = manager.register();
    let _: Result<()> = manager.unregister(10);
}

fn registry(
    map: &dyn IStateProviderMap,
    transaction: Arc<dyn Transaction>,
    cancellation: Cancellation,
) {
    let _: Result<Arc<dyn IStateProvider2>> = map.get("fabric:/provider");
    let _: Result<Arc<dyn IStateProviderEnumerator>> = map.create_enumerator(true);
    let _: ApiFuture<'_, ()> = map.add(
        transaction.clone(),
        "fabric:/provider".into(),
        "TestProvider".into(),
        None,
        None,
        cancellation.clone(),
    );
    let _: ApiFuture<'_, GetOrAddResult> = map.get_or_add(
        transaction.clone(),
        "fabric:/provider".into(),
        "TestProvider".into(),
        None,
        None,
        cancellation.clone(),
    );
    let _: ApiFuture<'_, ()> =
        map.remove(transaction, "fabric:/provider".into(), None, cancellation);
}

fn provider(
    provider: &dyn IStateProvider2,
    replicator: Weak<dyn ITransactionalReplicator>,
    transaction: Arc<dyn TransactionBase>,
    context: Arc<dyn OperationContext>,
    payload: OperationData,
    cancellation: Cancellation,
) {
    let _: &str = provider.get_name();
    let _: Result<Vec<StateProviderInfo>> = provider.get_children("fabric:/parent");
    let _: Result<()> = provider.initialize(replicator.clone(), Path::new("work"), None);
    let _: Result<()> = provider.initialize(replicator, Path::new("work"), Some(&[]));
    let _: ApiFuture<'_, ()> = provider.open(cancellation.clone());
    let _: ApiFuture<'_, ()> =
        provider.change_role(ReplicaRole::ActiveSecondary, cancellation.clone());
    let _: ApiFuture<'_, ()> = provider.close(cancellation.clone());
    let _: () = provider.abort();
    let _: Result<()> = provider.prepare_checkpoint(10);
    let _: ApiFuture<'_, ()> = provider.perform_checkpoint(cancellation.clone());
    let _: ApiFuture<'_, ()> = provider.complete_checkpoint(cancellation.clone());
    let _: ApiFuture<'_, ()> = provider.recover_checkpoint(cancellation.clone());
    let _: ApiFuture<'_, ()> =
        provider.backup_checkpoint(PathBuf::from("backup"), cancellation.clone());
    let _: ApiFuture<'_, ()> =
        provider.restore_checkpoint(PathBuf::from("backup"), cancellation.clone());
    let _: ApiFuture<'_, ()> = provider.remove_state(cancellation.clone());
    let _: Result<Option<Arc<dyn IOperationDataStream>>> = provider.get_current_state();
    let _: ApiFuture<'_, ()> = provider.begin_setting_current_state();
    let _: ApiFuture<'_, ()> = provider.set_current_state(0, payload.clone(), cancellation.clone());
    let _: ApiFuture<'_, ()> = provider.end_setting_current_state(cancellation.clone());
    let _: ApiFuture<'_, ()> = provider.prepare_for_remove(transaction.clone(), cancellation);
    let _: ApiFuture<'_, Option<Arc<dyn OperationContext>>> = provider.apply(
        11,
        transaction.clone(),
        ApplyContext::PrimaryRedo,
        None,
        Some(payload),
    );
    let _: ApiFuture<'_, Option<Arc<dyn OperationContext>>> = provider.apply(
        11,
        transaction,
        ApplyContext::SecondaryFalseProgress,
        None,
        None,
    );
    let _: Result<()> = provider.unlock(context.as_ref());
}

fn enumerations_and_factory(
    stream: &dyn IOperationDataStream,
    enumerator: &dyn IStateProviderEnumerator,
    factory: &dyn IStateProvider2Factory,
    arguments: &FactoryArguments,
    cancellation: Cancellation,
) {
    let _: ApiFuture<'_, Option<OperationData>> = stream.get_next(cancellation);
    let _: Result<()> = stream.dispose();
    let _: Result<bool> = enumerator.move_next();
    let _: Result<StateProviderEntry> = enumerator.current();
    let _: Result<Arc<dyn IStateProvider2>> = factory.create(arguments);
}

fn versions(
    manager: &dyn IVersionManager,
    notification: &dyn EnumerationCompletionResult,
    result: TryRemoveVersionResult,
) {
    let _: ApiFuture<'_, ()> = manager.try_remove_checkpoint(10, 20);
    let _: Result<TryRemoveVersionResult> = manager.try_remove_version(1, 10, 20);
    let _: SequenceNumber = notification.get_visibility_sequence_number();
    let _: ApiFuture<'_, SequenceNumber> = notification.get_notification();
    let _: bool = notification.get_is_notification_completed();
    let _: bool = result.can_be_removed;
    let _: Option<std::collections::BTreeSet<SequenceNumber>> = result.enumeration_set;
    let _: Option<Vec<Arc<dyn EnumerationCompletionResult>>> =
        result.enumeration_completion_notifications;
}

fn replicator(
    root: &dyn ITransactionalReplicator,
    transaction_handler: Arc<dyn ITransactionChangeHandler>,
    state_manager_handler: Arc<dyn IStateManagerChangeHandler>,
    backup_handler: Arc<dyn IBackupCallbackHandler>,
    cancellation: Cancellation,
) {
    let _: bool = root.get_is_readable();
    let _: Arc<dyn IStatefulServicePartition> = root.get_stateful_partition();
    let _: bool = root.get_has_persisted_state();
    let _: Result<SequenceNumber> = root.get_last_stable_sequence_number();
    let _: Result<SequenceNumber> = root.get_last_committed_sequence_number();
    let _: Result<Epoch> = root.get_current_epoch();
    let _: Result<()> = root.register_transaction_change_handler(transaction_handler);
    let _: Result<()> = root.unregister_transaction_change_handler();
    let _: Result<()> = root.register_state_manager_change_handler(state_manager_handler);
    let _: Result<()> = root.unregister_state_manager_change_handler();
    let _: ApiFuture<'_, BackupInfo> = root.backup(backup_handler.clone());
    let _: ApiFuture<'_, BackupInfo> = root.backup_with_options(
        backup_handler,
        BackupOption::Full,
        None,
        cancellation.clone(),
    );
    let _: ApiFuture<'_, ()> = root.restore(PathBuf::from("backup"));
    let _: ApiFuture<'_, ()> = root.restore_with_options(
        PathBuf::from("backup"),
        RestorePolicy::Safe,
        None,
        cancellation,
    );
}

fn callbacks(
    root: &dyn ITransactionalReplicator,
    transaction: &dyn ITransaction,
    provider: &dyn IStateProvider2,
    transaction_handler: &dyn ITransactionChangeHandler,
    state_manager_handler: &dyn IStateManagerChangeHandler,
    enumerator: &dyn IStateProviderEnumerator,
) {
    let _: Result<()> = transaction_handler.on_transaction_committed(root, transaction);
    let _: Result<()> = state_manager_handler.on_rebuilt(root, enumerator);
    let _: Result<()> =
        state_manager_handler.on_added(root, transaction, "fabric:/provider", provider);
    let _: Result<()> =
        state_manager_handler.on_removed(root, transaction, "fabric:/provider", provider);
}

fn backup_and_loss(
    backup_handler: &dyn IBackupCallbackHandler,
    loss_handler: &dyn IDataLossHandler,
    backup: BackupInfo,
    cancellation: Cancellation,
) {
    let _: ApiFuture<'_, bool> = backup_handler.upload_backup(backup, cancellation.clone());
    let _: ApiFuture<'_, bool> = loss_handler.data_loss(cancellation);
}

fn retained_async_inputs(
    root: &dyn ITransactionalReplicator,
    transaction: Arc<dyn Transaction>,
) -> ApiFuture<'_, SequenceNumber> {
    root.commit_transaction(transaction)
}
