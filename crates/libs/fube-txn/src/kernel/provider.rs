// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Bounded preinitialized directory: SF 51a7703 StateManager.cpp:970–1155,
//! 1290–1387,2796–2874,4032–4072,4566–4620. The point-map fixture follows
//! Store.h:510–551,851–932,3428–3483,4453–4479 and StoreTransaction.h:310–358
//! only for point locks, post-admission own writes, versioned Apply and explicit
//! idempotent Unlock. Its payload,
//! fault-read policy and finite initialization seam are not product TStore.
//! Checkpoint/copy callbacks follow StateManager.cpp:1389–1514,3592–3924,
//! 5634–5776. Registry bytes and local effects stay opaque; only bounded
//! initialized/resurrected entries and volatile snapshots are supported.

use super::{
    checkpoint::await_all,
    copy::NamedStateStream,
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

#[derive(Clone)]
pub(crate) struct RegistryMetadata {
    pub(crate) id: StateProviderId,
    pub(crate) name: String,
    pub(crate) mode: ProviderMode,
    pub(crate) provider: Arc<dyn IStateProvider2>,
}

#[derive(Clone, Copy)]
pub(crate) struct CopiedMetadata {
    pub(crate) id: StateProviderId,
    pub(crate) mode: ProviderMode,
}

/// Opaque source-format registry codec and volatile local side effects. The seam
/// supplies decoded metadata only for initialized/resurrected directory entries;
/// it does not implement factory/open/recover/hierarchy or persisted files.
pub(crate) trait LocalStateCallbacks: Send + Sync + 'static {
    fn prepare(&self, lsn: SequenceNumber, metadata: &[RegistryMetadata]) -> Result<()>;
    fn perform(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn complete(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn cleanup(&self, cancellation: Cancellation) -> ApiFuture<'_, ()>;
    fn get_current_state(
        &self,
        target_replica_id: i64,
        metadata: &[RegistryMetadata],
    ) -> Result<Arc<dyn IOperationDataStream>>;
    fn begin(&self) -> Result<()>;
    fn set(&self, number: i64, data: OperationData) -> ApiFuture<'_, Vec<CopiedMetadata>>;
    fn end(&self) -> ApiFuture<'_, ()>;
}

#[derive(Default)]
struct AggregateCheckpoint {
    prepared: Vec<RegistryMetadata>,
    published: Vec<RegistryMetadata>,
    ready: bool,
    performed: bool,
}

pub(crate) struct ProviderDirectory {
    self_weak: Weak<Self>,
    pub(crate) entries: Mutex<BTreeMap<StateProviderId, ProviderEntry>>,
    pub(crate) faults: Arc<FaultState>,
    manager: Mutex<Option<Weak<KernelManager>>>,
    role: Mutex<ProcessingRole>,
    registry: Option<Arc<dyn RegistryCallbacks>>,
    local: Mutex<Option<Arc<dyn LocalStateCallbacks>>>,
    checkpoint: Mutex<AggregateCheckpoint>,
    copy_progress: Mutex<Option<Vec<StateProviderId>>>,
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
        Ok(Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            entries: Mutex::new(installed),
            faults: Arc::new(FaultState::default()),
            manager: Mutex::new(None),
            role: Mutex::new(ProcessingRole::Primary),
            registry,
            local: Mutex::new(None),
            checkpoint: Mutex::new(AggregateCheckpoint::default()),
            copy_progress: Mutex::new(None),
        }))
    }
    pub(crate) fn set_local_callbacks(&self, local: Arc<dyn LocalStateCallbacks>) {
        *self.local.lock().expect("local callbacks poisoned") = Some(local);
    }
    fn local(&self) -> Result<Arc<dyn LocalStateCallbacks>> {
        self.local
            .lock()
            .expect("local callbacks poisoned")
            .clone()
            .ok_or_else(|| Status::NotImplemented.error())
    }
    fn shared(&self) -> Arc<Self> {
        self.self_weak.upgrade().expect("shared directory owner")
    }
    pub(crate) fn copy_progress(&self) -> Option<Vec<StateProviderId>> {
        self.copy_progress
            .lock()
            .expect("copy progress poisoned")
            .clone()
    }
    pub(crate) fn checkpoint_metadata(&self) -> Vec<RegistryMetadata> {
        self.checkpoint
            .lock()
            .expect("checkpoint metadata poisoned")
            .published
            .clone()
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
        // Native IsRegistered uses the active ID map, not retained deleted
        // provider inventory (MetadataManager::SoftDelete removes the active ID).
        match entry.mode {
            ProviderMode::Active => {}
            ProviderMode::DelayDelete => return Err(Status::InvalidParameter.error()),
            ProviderMode::Stale => return Err(Status::NotImplemented.error()),
        }
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
            Ok(self.manager()?.observe_async(
                super::record::Record {
                    lsn: -1,
                    kind: super::record::RecordKind::Begin {
                        transaction_id: transaction.get_transaction_id(),
                        single: true,
                        metadata: Some(metadata),
                        undo,
                        redo,
                    },
                },
                Some(transaction),
                context,
            ))
        })();
        match admitted {
            Ok(future) => future,
            Err(error) => Box::pin(async move { Err(error) }),
        }
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
    fn prepare_checkpoint(&self, lsn: SequenceNumber) -> Result<()> {
        if lsn < 0 {
            return Err(Status::InvalidParameter.error());
        }
        let local = self.local()?;
        let metadata: Vec<_> = self
            .entries
            .lock()
            .expect("directory poisoned")
            .iter()
            .filter(|(_, entry)| entry.mode != ProviderMode::Stale)
            .map(|(id, entry)| RegistryMetadata {
                id: *id,
                name: entry.provider.get_name().to_owned(),
                mode: entry.mode,
                provider: entry.provider.clone(),
            })
            .collect();
        {
            let mut checkpoint = self
                .checkpoint
                .lock()
                .expect("checkpoint metadata poisoned");
            checkpoint.ready = false;
            checkpoint.performed = false;
            checkpoint.prepared = metadata.clone();
        }
        local.prepare(lsn, &metadata)?;
        for entry in &metadata {
            if entry.mode == ProviderMode::Active {
                entry.provider.prepare_checkpoint(lsn)?;
            }
        }
        self.checkpoint
            .lock()
            .expect("checkpoint metadata poisoned")
            .ready = true;
        Ok(())
    }
    fn perform_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        let directory = self.shared();
        own(Box::pin(async move {
            let local = directory.local()?;
            let metadata = {
                let checkpoint = directory
                    .checkpoint
                    .lock()
                    .expect("checkpoint metadata poisoned");
                if !checkpoint.ready {
                    return Err(Status::InvalidOperation.error());
                }
                checkpoint.prepared.clone()
            };
            let work = metadata
                .iter()
                .filter(|entry| entry.mode == ProviderMode::Active)
                .map(|entry| {
                    let provider = entry.provider.clone();
                    let cancellation = cancellation.clone();
                    Box::pin(async move { provider.perform_checkpoint(cancellation).await })
                        as ApiFuture<'static, ()>
                })
                .collect();
            await_all(work).await?;
            local.perform(cancellation).await?;
            let mut checkpoint = directory
                .checkpoint
                .lock()
                .expect("checkpoint metadata poisoned");
            checkpoint.published = metadata;
            checkpoint.performed = true;
            Ok(())
        }))
    }
    fn complete_checkpoint(&self, cancellation: Cancellation) -> ApiFuture<'_, ()> {
        let directory = self.shared();
        own(Box::pin(async move {
            let local = directory.local()?;
            let metadata = {
                let checkpoint = directory
                    .checkpoint
                    .lock()
                    .expect("checkpoint metadata poisoned");
                if !checkpoint.performed {
                    return Err(Status::InvalidOperation.error());
                }
                checkpoint.published.clone()
            };
            let work = metadata
                .into_iter()
                .filter(|entry| entry.mode == ProviderMode::Active)
                .map(|entry| {
                    let cancellation = cancellation.clone();
                    Box::pin(async move { entry.provider.complete_checkpoint(cancellation).await })
                        as ApiFuture<'static, ()>
                })
                .collect();
            await_all(work).await?;
            local.complete(cancellation.clone()).await?;
            local.cleanup(cancellation).await
        }))
    }
    fn backup_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn restore_checkpoint(&self, _: PathBuf, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
    fn get_current_state(&self, target: i64) -> Result<Arc<dyn IOperationDataStream>> {
        let local = self.local()?;
        let mut metadata = self.checkpoint_metadata();
        metadata.sort_by(|left, right| right.name.cmp(&left.name));
        let registry = local.get_current_state(target, &metadata)?;
        let mut streams = Vec::new();
        for entry in metadata {
            if entry.mode == ProviderMode::Active {
                match entry.provider.get_current_state() {
                    Ok(Some(stream)) => streams.push((entry.id, stream)),
                    Ok(None) => {}
                    Err(error) => {
                        // Native destruction disposes partially constructed named
                        // streams. Preserve that cleanup explicitly, not via Drop.
                        let _ = NamedStateStream::new(registry, streams).dispose();
                        return Err(error);
                    }
                }
            }
        }
        Ok(NamedStateStream::new(registry, streams))
    }
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()> {
        let directory = self.shared();
        own(Box::pin(async move {
            let local = directory.local()?;
            if directory.copy_progress().is_some() {
                return Err(Status::InvalidOperation.error());
            }
            for entry in directory
                .entries
                .lock()
                .expect("directory poisoned")
                .values_mut()
            {
                entry.mode = ProviderMode::Stale;
            }
            *directory
                .copy_progress
                .lock()
                .expect("copy progress poisoned") = Some(Vec::new());
            local.begin()
        }))
    }
    fn set_current_state(&self, number: i64, data: OperationData) -> ApiFuture<'_, ()> {
        let directory = self.shared();
        own(Box::pin(async move {
            let (id, payload) = named::decode_copy(&data)?;
            if id == 0 {
                return Err(Status::InvalidParameter.error());
            }
            if directory.copy_progress().is_none() {
                return Err(Status::InvalidOperation.error());
            }
            if id == REGISTRY_ID {
                for metadata in directory.local()?.set(number, payload).await? {
                    if metadata.mode == ProviderMode::Stale {
                        return Err(Status::NotImplemented.error());
                    }
                    if metadata.id == 0 || metadata.id == REGISTRY_ID {
                        return Err(Status::InvalidParameter.error());
                    }
                    // Uninitialized entries require the excluded factory/open/
                    // recover/hierarchy path, not an invented successful Begin.
                    let entry = directory
                        .entry(metadata.id)
                        .map_err(|_| Status::NotImplemented.error())?;
                    if metadata.mode == ProviderMode::Active {
                        if directory
                            .copy_progress()
                            .is_some_and(|progress| progress.contains(&metadata.id))
                        {
                            return Err(Status::InvalidOperation.error());
                        }
                        directory.set_mode(metadata.id, ProviderMode::Active, None);
                        entry.provider.begin_setting_current_state().await?;
                        directory
                            .copy_progress
                            .lock()
                            .expect("copy progress poisoned")
                            .as_mut()
                            .expect("begun aggregate")
                            .push(metadata.id);
                    } else {
                        directory.set_mode(metadata.id, ProviderMode::DelayDelete, None);
                    }
                }
                return Ok(());
            }
            let entry = directory.entry(id)?;
            match entry.mode {
                ProviderMode::DelayDelete => return Err(Status::ObjectClosed.error()),
                ProviderMode::Stale => return Err(Status::NotImplemented.error()),
                ProviderMode::Active => {}
            }
            if !directory
                .copy_progress()
                .is_some_and(|progress| progress.contains(&id))
            {
                return Err(Status::InvalidOperation.error());
            }
            entry
                .provider
                .set_current_state(number, payload, Cancellation::default())
                .await
        }))
    }
    fn end_setting_current_state(&self) -> ApiFuture<'_, ()> {
        let directory = self.shared();
        own(Box::pin(async move {
            let progress = directory
                .copy_progress()
                .ok_or_else(|| Status::InvalidOperation.error())?;
            directory.local()?.end().await?;
            let mut work = Vec::new();
            for id in progress {
                let provider = directory.entry(id)?.provider;
                work.push(Box::pin(async move {
                    provider
                        .end_setting_current_state(Cancellation::default())
                        .await
                }) as ApiFuture<'static, ()>);
            }
            await_all(work).await?;
            *directory
                .copy_progress
                .lock()
                .expect("copy progress poisoned") = None;
            Ok(())
        }))
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Status::NotImplemented.error()) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PointEvent {
    Callback {
        kind: CallbackKind,
        completed: bool,
    },
    Prepare(SequenceNumber),
    CopySet {
        number: i64,
        data: OperationData,
        cancelled: bool,
    },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CallbackKind {
    Prepare,
    Perform,
    Complete,
    BeginCopy,
    SetCopy,
    EndCopy,
    GetCopy,
}

#[derive(Clone)]
pub(crate) struct CallbackPlan {
    pub(crate) gate: Option<Completion<()>>,
    pub(crate) result: Result<()>,
    pub(crate) on_enter: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Default for CallbackPlan {
    fn default() -> Self {
        Self {
            gate: None,
            result: Ok(()),
            on_enter: None,
        }
    }
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
    callbacks: BTreeMap<CallbackKind, VecDeque<CallbackPlan>>,
    checkpoint_prepared: Option<BTreeMap<i64, i64>>,
    checkpoint_performed: Option<BTreeMap<i64, i64>>,
    checkpoint_completed: Option<BTreeMap<i64, i64>>,
    copying: Option<BTreeMap<i64, i64>>,
    copied: BTreeMap<i64, i64>,
    stream_override: Option<Option<Vec<OperationData>>>,
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
    pub(crate) fn inject_callback(&self, kind: CallbackKind, plan: CallbackPlan) {
        self.state
            .lock()
            .expect("point state poisoned")
            .callbacks
            .entry(kind)
            .or_default()
            .push_back(plan);
    }
    pub(crate) fn set_copy_stream(&self, stream: Option<Vec<OperationData>>) {
        self.state
            .lock()
            .expect("point state poisoned")
            .stream_override = Some(stream);
    }
    pub(crate) fn copied(&self) -> BTreeMap<i64, i64> {
        self.state
            .lock()
            .expect("point state poisoned")
            .copied
            .clone()
    }
    pub(crate) fn completed_checkpoint(&self) -> Option<BTreeMap<i64, i64>> {
        self.state
            .lock()
            .expect("point state poisoned")
            .checkpoint_completed
            .clone()
    }
    fn callback_plan(&self, kind: CallbackKind) -> CallbackPlan {
        let mut state = self.state.lock().expect("point state poisoned");
        state.events.push(PointEvent::Callback {
            kind,
            completed: false,
        });
        state
            .callbacks
            .get_mut(&kind)
            .and_then(VecDeque::pop_front)
            .unwrap_or_default()
    }
    fn callback_completed(&self, kind: CallbackKind) {
        self.state
            .lock()
            .expect("point state poisoned")
            .events
            .push(PointEvent::Callback {
                kind,
                completed: true,
            });
    }
    async fn callback_wait(&self, plan: CallbackPlan) -> Result<()> {
        if let Some(on_enter) = plan.on_enter {
            on_enter();
        }
        if let Some(gate) = plan.gate {
            gate.wait().await.as_ref().clone()?;
        }
        plan.result?;
        Ok(())
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

struct PointStream {
    data: Mutex<VecDeque<OperationData>>,
    disposed: Mutex<bool>,
}

impl IOperationDataStream for PointStream {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            if *self.disposed.lock().expect("point stream poisoned") {
                return Err(Status::ObjectClosed.error());
            }
            if cancellation.is_cancelled() {
                return Err(Status::Cancelled.error());
            }
            Ok(self.data.lock().expect("point stream poisoned").pop_front())
        })
    }
    fn dispose(&self) -> Result<()> {
        *self.disposed.lock().expect("point stream poisoned") = true;
        self.data.lock().expect("point stream poisoned").clear();
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
    fn prepare_checkpoint(&self, lsn: SequenceNumber) -> Result<()> {
        if lsn < 0 {
            return Err(Status::InvalidParameter.error());
        }
        let plan = self.callback_plan(CallbackKind::Prepare);
        if plan.gate.is_some() {
            return Err(Error::InvalidInput(
                "synchronous Prepare cannot await a gate",
            ));
        }
        if let Some(on_enter) = plan.on_enter {
            on_enter();
        }
        plan.result?;
        let mut state = self.state.lock().expect("point state poisoned");
        state.events.push(PointEvent::Prepare(lsn));
        state.checkpoint_prepared = Some(
            state
                .versions
                .iter()
                .filter_map(|(key, versions)| versions.last().map(|version| (*key, version.value)))
                .collect(),
        );
        drop(state);
        self.callback_completed(CallbackKind::Prepare);
        Ok(())
    }
    fn perform_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        let provider = self.self_weak.upgrade().expect("shared point owner");
        own(Box::pin(async move {
            let plan = provider.callback_plan(CallbackKind::Perform);
            provider.callback_wait(plan).await?;
            let mut state = provider.state.lock().expect("point state poisoned");
            state.checkpoint_performed = Some(
                state
                    .checkpoint_prepared
                    .clone()
                    .ok_or_else(|| Status::InvalidOperation.error())?,
            );
            drop(state);
            provider.callback_completed(CallbackKind::Perform);
            Ok(())
        }))
    }
    fn complete_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        let provider = self.self_weak.upgrade().expect("shared point owner");
        own(Box::pin(async move {
            let plan = provider.callback_plan(CallbackKind::Complete);
            provider.callback_wait(plan).await?;
            let mut state = provider.state.lock().expect("point state poisoned");
            state.checkpoint_completed = Some(
                state
                    .checkpoint_performed
                    .clone()
                    .ok_or_else(|| Status::InvalidOperation.error())?,
            );
            drop(state);
            provider.callback_completed(CallbackKind::Complete);
            Ok(())
        }))
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
        let plan = self.callback_plan(CallbackKind::GetCopy);
        if plan.gate.is_some() {
            return Err(Error::InvalidInput(
                "synchronous GetCurrentState cannot await a gate",
            ));
        }
        if let Some(on_enter) = plan.on_enter {
            on_enter();
        }
        plan.result?;
        let state = self.state.lock().expect("point state poisoned");
        let data = match &state.stream_override {
            Some(stream) => stream.clone(),
            None => Some(
                state
                    .checkpoint_performed
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(key, value)| Self::payload(key, value))
                    .collect(),
            ),
        };
        let stream = data.map(|data| {
            Arc::new(PointStream {
                data: Mutex::new(data.into()),
                disposed: Mutex::new(false),
            }) as Arc<dyn IOperationDataStream>
        });
        drop(state);
        self.callback_completed(CallbackKind::GetCopy);
        Ok(stream)
    }
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()> {
        let provider = self.self_weak.upgrade().expect("shared point owner");
        own(Box::pin(async move {
            let plan = provider.callback_plan(CallbackKind::BeginCopy);
            provider.callback_wait(plan).await?;
            provider.state.lock().expect("point state poisoned").copying = Some(BTreeMap::new());
            provider.callback_completed(CallbackKind::BeginCopy);
            Ok(())
        }))
    }
    fn set_current_state(
        &self,
        number: i64,
        data: OperationData,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        let provider = self.self_weak.upgrade().expect("shared point owner");
        own(Box::pin(async move {
            let plan = provider.callback_plan(CallbackKind::SetCopy);
            provider
                .state
                .lock()
                .expect("point state poisoned")
                .events
                .push(PointEvent::CopySet {
                    number,
                    data: data.clone(),
                    cancelled: cancellation.is_cancelled(),
                });
            provider.callback_wait(plan).await?;
            // Header-only native data is a valid empty provider copy record.
            let value = if data.segments().is_empty() {
                None
            } else {
                Some(Self::decode(Some(data))?)
            };
            let mut state = provider.state.lock().expect("point state poisoned");
            let copying = state
                .copying
                .as_mut()
                .ok_or_else(|| Status::InvalidOperation.error())?;
            if let Some((key, value)) = value {
                copying.insert(key, value);
            }
            drop(state);
            provider.callback_completed(CallbackKind::SetCopy);
            Ok(())
        }))
    }
    fn end_setting_current_state(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        let provider = self.self_weak.upgrade().expect("shared point owner");
        own(Box::pin(async move {
            let plan = provider.callback_plan(CallbackKind::EndCopy);
            provider.callback_wait(plan).await?;
            let mut state = provider.state.lock().expect("point state poisoned");
            state.copied = state
                .copying
                .take()
                .ok_or_else(|| Status::InvalidOperation.error())?;
            drop(state);
            provider.callback_completed(CallbackKind::EndCopy);
            Ok(())
        }))
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
