// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Bounded preinitialized directory: SF 51a7703 StateManager.cpp:970–1155,
//! 1290–1387,2796–2874,4032–4072,4566–4620. The point-map fixture follows
//! Store.h:510–551,851–932,3428–3483,4453–4479 and StoreTransaction.h:310–358
//! only for point locks, post-admission own writes, versioned Apply and explicit
//! idempotent Unlock. Its payload,
//! fault-read policy and finite initialization seam are not product TStore.

use super::{
    named,
    processor::{FaultState, KernelManager, ProcessingRole},
    status::Status,
    transaction::own,
    version::Completion,
};
use crate::{
    internal::{IStateProviderManager, ITransactionStateManager},
    ApiFuture, ApplyContext, AtomicOperation, AtomicRedoOperation, Cancellation, Error,
    IOperationDataStream, IStateProvider2, ITransactionalReplicator, LockContext, OperationContext,
    OperationData, ReplicaRole, Result, SequenceNumber, StateProviderId, StateProviderInfo,
    Transaction, TransactionBase, TransactionId,
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

pub(crate) const REGISTRY_ID: StateProviderId = i64::MIN;

pub(crate) struct NamedContext {
    pub(crate) provider: StateProviderId,
    pub(crate) inner: Arc<dyn OperationContext>,
}
impl OperationContext for NamedContext {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderMode {
    Active,
    DelayDelete,
    Stale,
}

#[derive(Clone)]
pub(crate) struct ProviderEntry {
    pub(crate) provider: Arc<dyn IStateProvider2>,
    pub(crate) mode: ProviderMode,
    pub(crate) deleting_by: Option<TransactionId>,
}

/// Opaque native registry callback seam. No substitute registry metadata codec.
pub(crate) trait RegistryCallbacks: Send + Sync {
    fn apply(
        &self,
        lsn: SequenceNumber,
        transaction: Arc<dyn TransactionBase>,
        context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>>;
    fn unlock(&self, context: &dyn OperationContext) -> Result<()>;
}

pub(crate) struct ProviderDirectory {
    pub(crate) entries: Mutex<BTreeMap<StateProviderId, ProviderEntry>>,
    pub(crate) faults: Arc<FaultState>,
    manager: Mutex<Option<Weak<KernelManager>>>,
    role: Mutex<ProcessingRole>,
    registry: Option<Arc<dyn RegistryCallbacks>>,
}

impl ProviderDirectory {
    pub(crate) fn preinitialized(
        entries: impl IntoIterator<Item = (StateProviderId, Arc<dyn IStateProvider2>)>,
        registry: Option<Arc<dyn RegistryCallbacks>>,
    ) -> Result<Arc<Self>> {
        let mut installed = BTreeMap::new();
        for (id, provider) in entries {
            if id == 0
                || id == REGISTRY_ID
                || installed.len() >= 64
                || installed
                    .insert(
                        id,
                        ProviderEntry {
                            provider,
                            mode: ProviderMode::Active,
                            deleting_by: None,
                        },
                    )
                    .is_some()
            {
                return Err(Error::InvalidInput("invalid bounded initialized directory"));
            }
        }
        Ok(Arc::new(Self {
            entries: Mutex::new(installed),
            faults: Arc::new(FaultState::default()),
            manager: Mutex::new(None),
            role: Mutex::new(ProcessingRole::Primary),
            registry,
        }))
    }
    pub(crate) fn bind(&self, manager: Weak<KernelManager>, role: ProcessingRole) {
        *self.manager.lock().expect("directory binding poisoned") = Some(manager);
        *self.role.lock().expect("directory role poisoned") = role;
    }
    fn manager(&self) -> Result<Arc<KernelManager>> {
        self.manager
            .lock()
            .expect("directory binding poisoned")
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Status::ObjectClosed.error())
    }
    pub(crate) fn set_mode(
        &self,
        id: StateProviderId,
        mode: ProviderMode,
        deleting_by: Option<TransactionId>,
    ) {
        let mut entries = self.entries.lock().expect("directory poisoned");
        let entry = entries
            .get_mut(&id)
            .expect("preinitialized provider exists");
        entry.mode = mode;
        entry.deleting_by = deleting_by;
    }
    pub(crate) fn entry(&self, id: StateProviderId) -> Result<ProviderEntry> {
        self.entries
            .lock()
            .expect("directory poisoned")
            .get(&id)
            .cloned()
            .ok_or_else(|| Status::InvalidParameter.error())
    }
    fn registered(
        &self,
        id: StateProviderId,
        transaction: TransactionId,
        deleting: bool,
    ) -> Result<()> {
        if id == REGISTRY_ID {
            return Ok(());
        }
        let entry = self.entry(id)?;
        if deleting && entry.deleting_by == Some(transaction) {
            return Err(Status::ObjectClosed.error());
        }
        Ok(())
    }
    fn wrap(
        &self,
        provider: StateProviderId,
        id: TransactionId,
        metadata: Option<OperationData>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<(OperationData, Option<Arc<dyn OperationContext>>)> {
        self.registered(provider, id, false)?;
        if provider == REGISTRY_ID && self.registry.is_none() {
            return Err(Status::NotImplemented.error());
        }
        let metadata = named::encode(provider, metadata.as_ref())?;
        let context = context
            .map(|inner| Arc::new(NamedContext { provider, inner }) as Arc<dyn OperationContext>);
        Ok((metadata, context))
    }
    fn direct_unlock(
        &self,
        provider: StateProviderId,
        context: &dyn OperationContext,
    ) -> Result<()> {
        if provider == REGISTRY_ID {
            self.registry
                .as_ref()
                .ok_or_else(|| Status::NotImplemented.error())?
                .unlock(context)
        } else {
            self.entry(provider)?.provider.unlock(context)
        }
    }
}

impl ITransactionStateManager for ProviderDirectory {
    fn single_operation_transaction_abort_unlock(
        &self,
        provider: StateProviderId,
        context: &dyn OperationContext,
    ) -> Result<()> {
        let result = self.direct_unlock(provider, context);
        if let Err(error) = &result {
            // This StateManager path reports partition fault; it does not call
            // OperationProcessor::ProcessServiceError or latch its service slot.
            self.faults.report_provider_fault(error.clone());
        }
        result
    }
    fn error_if_state_provider_is_not_registered(
        &self,
        provider: StateProviderId,
        id: TransactionId,
    ) -> Result<()> {
        self.registered(provider, id, true)
    }
    fn begin_transaction_async(
        &self,
        transaction: Arc<dyn Transaction>,
        provider: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber> {
        let admitted = (|| {
            let (metadata, context) = self.wrap(
                provider,
                transaction.get_transaction_id(),
                metadata,
                context,
            )?;
            self.manager()?
                .admit_operation(transaction, metadata, undo, redo, context, Some(true))
        })();
        KernelManager::observe_admission(admitted)
    }
    fn begin_transaction(
        &self,
        transaction: &dyn Transaction,
        provider: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()> {
        let (metadata, context) = self.wrap(
            provider,
            transaction.get_transaction_id(),
            metadata,
            context,
        )?;
        self.manager()?
            .admit_borrowed(transaction, metadata, undo, redo, context, true)
    }
    fn add_operation(
        &self,
        transaction: &dyn Transaction,
        provider: StateProviderId,
        metadata: Option<OperationData>,
        undo: Option<OperationData>,
        redo: Option<OperationData>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<()> {
        let (metadata, context) = self.wrap(
            provider,
            transaction.get_transaction_id(),
            metadata,
            context,
        )?;
        self.manager()?
            .admit_borrowed(transaction, metadata, undo, redo, context, false)
    }
    fn add_atomic_operation(
        &self,
        _: Arc<dyn AtomicOperation>,
        _: StateProviderId,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn add_atomic_redo_operation(
        &self,
        _: Arc<dyn AtomicRedoOperation>,
        _: StateProviderId,
        _: Option<OperationData>,
        _: Option<OperationData>,
        _: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'_, SequenceNumber> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}

impl IStateProviderManager for ProviderDirectory {
    fn apply(
        &self,
        lsn: SequenceNumber,
        transaction: Arc<dyn TransactionBase>,
        context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>> {
        if !matches!(
            context,
            ApplyContext::PrimaryRedo | ApplyContext::SecondaryRedo | ApplyContext::RecoveryRedo
        ) {
            return Box::pin(async { Err(Status::NotImplemented.error()) });
        }
        // Native primed metadata has the same fields as this checked envelope;
        // provider contents never enter the record/processor interpretation.
        let routed = (|| {
            let metadata = metadata.ok_or(Error::InvalidInput("missing named Apply metadata"))?;
            let (provider, metadata) = named::decode(&metadata)?;
            if provider == 0 {
                return Err(Error::InvalidInput("empty Apply provider identity"));
            }
            if provider == REGISTRY_ID {
                return Ok((provider, metadata, None));
            }
            Ok((provider, metadata, Some(self.entry(provider)?)))
        })();
        Box::pin(async move {
            let (provider, metadata, entry) = routed?;
            let returned = if let Some(entry) = entry {
                match entry.mode {
                    ProviderMode::DelayDelete => return Ok(None),
                    ProviderMode::Stale => return Err(Status::NotImplemented.error()),
                    ProviderMode::Active => {}
                }
                entry
                    .provider
                    .apply(lsn, transaction, context, metadata, data)
                    .await?
            } else {
                self.registry
                    .as_ref()
                    .ok_or_else(|| Status::NotImplemented.error())?
                    .apply(lsn, transaction, context, metadata, data)
                    .await?
            };
            Ok(returned.map(|inner| {
                Arc::new(NamedContext { provider, inner }) as Arc<dyn OperationContext>
            }))
        })
    }
    fn unlock(&self, context: &dyn OperationContext) -> Result<()> {
        let context = (context as &dyn Any)
            .downcast_ref::<NamedContext>()
            .ok_or(Error::InvalidInput("expected NamedOperationContext"))?;
        self.direct_unlock(context.provider, context.inner.as_ref())
    }
    fn prepare_checkpoint(&self, _: SequenceNumber) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn perform_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn complete_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn backup_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn restore_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn get_current_state(&self, _: i64) -> Result<Arc<dyn IOperationDataStream>> {
        Err(Status::NotImplemented.error())
    }
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn set_current_state(&self, _: i64, _: OperationData) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn end_setting_current_state(&self) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PointEvent {
    Apply {
        operation: SequenceNumber,
        commit: SequenceNumber,
        transaction: TransactionId,
        role: ApplyContext,
        metadata: Option<OperationData>,
        key: i64,
        value: i64,
    },
    Unlock {
        capture: u64,
        transaction: TransactionId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PointVersion {
    pub(crate) commit: SequenceNumber,
    pub(crate) operation: SequenceNumber,
    pub(crate) value: i64,
}

#[derive(Clone, Copy)]
pub(crate) enum CaptureResult {
    Replace,
    RetainOriginal,
}

#[derive(Clone)]
pub(crate) struct ApplyPlan {
    pub(crate) gate: Option<Completion<()>>,
    pub(crate) result: Result<()>,
    pub(crate) capture: CaptureResult,
    pub(crate) on_enter: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl Default for ApplyPlan {
    fn default() -> Self {
        Self {
            gate: None,
            result: Ok(()),
            capture: CaptureResult::Replace,
            on_enter: None,
        }
    }
}

#[derive(Default)]
struct KeyLock {
    writer: Option<TransactionId>,
    readers: BTreeSet<TransactionId>,
}
#[derive(Default)]
struct PointWrites {
    keys: BTreeSet<i64>,
    writes: BTreeMap<i64, i64>,
    completed: bool,
}
struct PointTransaction {
    id: TransactionId,
    writes: Mutex<PointWrites>,
}

pub(crate) struct PointContext {
    provider: Weak<PointProvider>,
    transaction: Arc<PointTransaction>,
    pub(crate) capture: u64,
}
impl OperationContext for PointContext {}
impl LockContext for PointContext {
    fn is_tracking_context(&self) -> bool {
        false
    }
    fn unlock(&self) -> Result<()> {
        self.provider
            .upgrade()
            .ok_or_else(|| Status::ObjectClosed.error())?
            .release(&self.transaction)
    }
}

#[derive(Default)]
struct PointState {
    locks: BTreeMap<i64, KeyLock>,
    transactions: BTreeMap<TransactionId, Arc<PointTransaction>>,
    versions: BTreeMap<i64, Vec<PointVersion>>,
    events: Vec<PointEvent>,
    apply: VecDeque<ApplyPlan>,
    unlock: VecDeque<Result<()>>,
    capture: u64,
    /// Explicit local fixture policy: failed Apply rejects subsequent reads.
    read_faulted: bool,
    closed: bool,
}

pub(crate) struct PointProvider {
    self_weak: Weak<Self>,
    name: String,
    state: Mutex<PointState>,
    released: Notify,
}

impl PointProvider {
    /// Finite preinitialized fixture, not native Initialize/Open qualification.
    pub(crate) fn preinitialized(name: &str) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            name: name.to_owned(),
            state: Mutex::new(PointState::default()),
            released: Notify::new(),
        })
    }
    pub(crate) fn events(&self) -> Vec<PointEvent> {
        self.state
            .lock()
            .expect("point state poisoned")
            .events
            .clone()
    }
    pub(crate) fn versions(&self, key: i64) -> Vec<PointVersion> {
        self.state
            .lock()
            .expect("point state poisoned")
            .versions
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }
    pub(crate) fn is_locked(&self, key: i64) -> bool {
        self.state
            .lock()
            .expect("point state poisoned")
            .locks
            .get(&key)
            .is_some_and(|lock| lock.writer.is_some() || !lock.readers.is_empty())
    }
    pub(crate) fn inject_apply(&self, plan: ApplyPlan) {
        self.state
            .lock()
            .expect("point state poisoned")
            .apply
            .push_back(plan);
    }
    pub(crate) fn inject_unlock(&self, result: Result<()>) {
        self.state
            .lock()
            .expect("point state poisoned")
            .unlock
            .push_back(result);
    }
    pub(crate) fn read_faulted(&self) -> bool {
        self.state
            .lock()
            .expect("point state poisoned")
            .read_faulted
    }

    fn transaction(
        &self,
        transaction: &dyn TransactionBase,
        state: &mut PointState,
    ) -> Result<Arc<PointTransaction>> {
        let id = transaction.get_transaction_id();
        if let Some(context) = state.transactions.get(&id) {
            return Ok(context.clone());
        }
        let context = Arc::new(PointTransaction {
            id,
            writes: Mutex::new(PointWrites::default()),
        });
        if transaction.get_is_primary_transaction() {
            transaction.add_lock_context(Arc::new(PointContext {
                provider: self.self_weak.clone(),
                transaction: context.clone(),
                capture: 0,
            }))?;
        }
        state.transactions.insert(id, context.clone());
        Ok(context)
    }
    fn context(
        &self,
        transaction: Arc<PointTransaction>,
        state: &mut PointState,
    ) -> Arc<PointContext> {
        state.capture += 1;
        Arc::new(PointContext {
            provider: self.self_weak.clone(),
            transaction,
            capture: state.capture,
        })
    }

    /// Local nonproduct payload: one segment [key:i64 LE,value:i64 LE].
    /// It is provider data, not an additional log envelope.
    pub(crate) fn payload(key: i64, value: i64) -> OperationData {
        let mut bytes = key.to_le_bytes().to_vec();
        bytes.extend_from_slice(&value.to_le_bytes());
        OperationData::new(vec![bytes]).expect("point payload fits")
    }
    fn decode(data: Option<OperationData>) -> Result<(i64, i64)> {
        let data = data.ok_or(Error::InvalidInput("point fixture requires redo data"))?;
        if data.segments().len() != 1 || data.segments()[0].len() != 16 {
            return Err(Error::InvalidInput("invalid local point-fixture payload"));
        }
        let bytes = &data.segments()[0];
        Ok((
            i64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")),
            i64::from_le_bytes(bytes[8..].try_into().expect("8 bytes")),
        ))
    }

    pub(crate) fn stage(
        &self,
        transaction: &dyn Transaction,
        key: i64,
        value: i64,
    ) -> Result<Arc<PointContext>> {
        self.stage_write(transaction, key, Some(value))
    }

    fn stage_write(
        &self,
        transaction: &dyn Transaction,
        key: i64,
        value: Option<i64>,
    ) -> Result<Arc<PointContext>> {
        let mut state = self.state.lock().expect("point state poisoned");
        if state.read_faulted || state.closed {
            return Err(Status::ObjectClosed.error());
        }
        let id = transaction.get_transaction_id();
        let lock = state.locks.entry(key).or_default();
        if lock.writer.is_some_and(|writer| writer != id)
            || lock.readers.iter().any(|reader| *reader != id)
        {
            return Err(Status::ServiceTooBusy.error());
        }
        let context = self.transaction(transaction, &mut state)?;
        let mut writes = context.writes.lock().expect("point transaction poisoned");
        if writes.completed {
            return Err(Status::TransactionNotActive.error());
        }
        state.locks.get_mut(&key).expect("key lock exists").writer = Some(id);
        writes.keys.insert(key);
        if let Some(value) = value {
            writes.writes.insert(key, value);
        }
        drop(writes);
        Ok(self.context(context, &mut state))
    }

    pub(crate) fn write(
        &self,
        transaction: &dyn Transaction,
        provider: StateProviderId,
        key: i64,
        value: i64,
    ) -> Result<Arc<PointContext>> {
        let context = self.stage_write(transaction, key, None)?;
        transaction.add_operation(
            None,
            None,
            Some(Self::payload(key, value)),
            provider,
            Some(context.clone()),
        )?;
        context
            .transaction
            .writes
            .lock()
            .expect("point transaction poisoned")
            .writes
            .insert(key, value);
        Ok(context)
    }

    pub(crate) fn snapshot(
        &self,
        transaction: &dyn TransactionBase,
        key: i64,
        version: SequenceNumber,
    ) -> Result<Option<i64>> {
        let state = self.state.lock().expect("point state poisoned");
        if state.read_faulted || state.closed {
            return Err(Status::ObjectClosed.error());
        }
        if let Some(context) = state.transactions.get(&transaction.get_transaction_id()) {
            if let Some(value) = context
                .writes
                .lock()
                .expect("point transaction poisoned")
                .writes
                .get(&key)
            {
                return Ok(Some(*value));
            }
        }
        Ok(state
            .versions
            .get(&key)
            .and_then(|versions| versions.iter().rev().find(|entry| entry.commit <= version))
            .map(|entry| entry.value))
    }

    pub(crate) fn try_repeatable(
        &self,
        transaction: &dyn TransactionBase,
        key: i64,
    ) -> Result<Option<i64>> {
        let mut state = self.state.lock().expect("point state poisoned");
        if state.read_faulted || state.closed {
            return Err(Status::ObjectClosed.error());
        }
        let id = transaction.get_transaction_id();
        let lock = state.locks.entry(key).or_default();
        if lock.writer.is_some_and(|writer| writer != id) {
            return Err(Status::ServiceTooBusy.error());
        }
        let context = self.transaction(transaction, &mut state)?;
        let mut writes = context.writes.lock().expect("point transaction poisoned");
        if writes.completed {
            return Err(Status::TransactionNotActive.error());
        }
        state
            .locks
            .get_mut(&key)
            .expect("key lock exists")
            .readers
            .insert(id);
        writes.keys.insert(key);
        if let Some(value) = writes.writes.get(&key) {
            return Ok(Some(*value));
        }
        Ok(state
            .versions
            .get(&key)
            .and_then(|versions| versions.last())
            .map(|entry| entry.value))
    }

    pub(crate) fn repeatable(
        &self,
        transaction: Arc<dyn TransactionBase>,
        key: i64,
    ) -> ApiFuture<'static, Option<i64>> {
        let provider = self
            .self_weak
            .upgrade()
            .expect("point provider shared owner");
        own(Box::pin(async move {
            loop {
                let wait = provider.released.notified();
                tokio::pin!(wait);
                wait.as_mut().enable();
                match provider.try_repeatable(transaction.as_ref(), key) {
                    Err(error) if error.native_code() == Some(Status::ServiceTooBusy.code()) => {
                        wait.await
                    }
                    result => return result,
                }
            }
        }))
    }

    fn release(&self, transaction: &Arc<PointTransaction>) -> Result<()> {
        let mut state = self.state.lock().expect("point state poisoned");
        let mut writes = transaction
            .writes
            .lock()
            .expect("point transaction poisoned");
        if writes.completed {
            return Ok(());
        }
        for key in &writes.keys {
            if let Some(lock) = state.locks.get_mut(key) {
                if lock.writer == Some(transaction.id) {
                    lock.writer = None;
                }
                lock.readers.remove(&transaction.id);
                if lock.writer.is_none() && lock.readers.is_empty() {
                    state.locks.remove(key);
                }
            }
        }
        writes.completed = true;
        writes.writes.clear();
        state.transactions.remove(&transaction.id);
        drop(writes);
        drop(state);
        self.released.notify_waiters();
        Ok(())
    }
}

impl IStateProvider2 for PointProvider {
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_children(&self, _: &str) -> Result<Vec<StateProviderInfo>> {
        Ok(Vec::new())
    }
    fn initialize(
        &self,
        _: Weak<dyn ITransactionalReplicator>,
        _: &Path,
        _: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn open(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn abort(&self) {
        let mut state = self.state.lock().expect("point state poisoned");
        state.closed = true;
        state.locks.clear();
        drop(state);
        self.released.notify_waiters();
    }
    fn prepare_checkpoint(&self, _: SequenceNumber) -> Result<()> {
        Err(Status::NotImplemented.error())
    }
    fn perform_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn complete_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn recover_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn backup_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn restore_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn remove_state(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn get_current_state(&self) -> Result<Option<Arc<dyn IOperationDataStream>>> {
        Err(Status::NotImplemented.error())
    }
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn set_current_state(&self, _: i64, _: OperationData, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn end_setting_current_state(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn prepare_for_remove(
        &self,
        _: Arc<dyn TransactionBase>,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn apply(
        &self,
        operation: SequenceNumber,
        transaction: Arc<dyn TransactionBase>,
        role: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>> {
        let plan = self
            .state
            .lock()
            .expect("point state poisoned")
            .apply
            .pop_front()
            .unwrap_or_default();
        Box::pin(async move {
            if !matches!(
                role,
                ApplyContext::PrimaryRedo
                    | ApplyContext::SecondaryRedo
                    | ApplyContext::RecoveryRedo
            ) {
                return Err(Status::NotImplemented.error());
            }
            let (key, value) = Self::decode(data)?;
            {
                let mut state = self.state.lock().expect("point state poisoned");
                state.events.push(PointEvent::Apply {
                    operation,
                    commit: transaction.get_commit_sequence_number(),
                    transaction: transaction.get_transaction_id(),
                    role,
                    metadata,
                    key,
                    value,
                });
            }
            if let Some(on_enter) = plan.on_enter {
                on_enter();
            }
            if let Some(gate) = plan.gate {
                gate.wait().await.as_ref().clone()?;
            }
            if let Err(error) = plan.result {
                self.state
                    .lock()
                    .expect("point state poisoned")
                    .read_faulted = true;
                return Err(error);
            }
            let commit = transaction.get_commit_sequence_number();
            if commit <= 0 {
                return Err(Error::InvalidInput(
                    "point Apply requires assigned commit LSN",
                ));
            }
            let mut state = self.state.lock().expect("point state poisoned");
            if state.closed {
                return Err(Status::ObjectClosed.error());
            }
            let versions = state.versions.entry(key).or_default();
            let position = versions
                .partition_point(|entry| (entry.commit, entry.operation) <= (commit, operation));
            versions.insert(
                position,
                PointVersion {
                    commit,
                    operation,
                    value,
                },
            );
            if matches!(plan.capture, CaptureResult::RetainOriginal) {
                return Ok(None);
            }
            let context = self.transaction(transaction.as_ref(), &mut state)?;
            Ok(Some(
                self.context(context, &mut state) as Arc<dyn OperationContext>
            ))
        })
    }
    fn unlock(&self, context: &dyn OperationContext) -> Result<()> {
        let context = (context as &dyn Any)
            .downcast_ref::<PointContext>()
            .ok_or(Error::InvalidInput("expected point fixture context"))?;
        {
            let mut state = self.state.lock().expect("point state poisoned");
            state.events.push(PointEvent::Unlock {
                capture: context.capture,
                transaction: context.transaction.id,
            });
            state.unlock.pop_front().unwrap_or(Ok(()))?;
        }
        self.release(&context.transaction)
    }
}
