// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native primary map and volatile registry checkpoint/copy/factory lifecycle.
//! StateManager.cpp:217–960,4074–4530,4797–5034,5264–5425 and
//! MetadataManager.cpp:547–819,987–1100; no custom registry protocol.
use super::{
    provider::{
        CopiedMetadata, LocalStateCallbacks, ProviderMode, RegistryCallbacks, RegistryMetadata,
    },
    registry_codec::{self, ApplyType, InsertRedo, Metadata, RegistryOperation, RegistryPacket},
    runtime::VolatileRuntime,
    status::Status,
    transaction::own,
};
use crate::*;
use fube_data::extensions::ReplicatorDiagnostics;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

pub(crate) struct RegistrySettings {
    pub(crate) factory: Arc<dyn IStateProvider2Factory>,
    pub(crate) metadata: Vec<Metadata>,
}
struct State {
    rows: BTreeMap<i64, Metadata>,
    prepared: Vec<Metadata>,
    published: Vec<Metadata>,
    version: bool,
    seen: BTreeSet<i64>,
    staged: BTreeMap<i64, (Metadata, Arc<dyn IStateProvider2>)>,
    events: Vec<&'static str>,
    pending: BTreeMap<i64, Pending>,
    locks: BTreeMap<String, Arc<NameLock>>,
    inflight: BTreeMap<i64, BTreeSet<String>>,
    high_water: i64,
}
struct Pending {
    row: Metadata,
    provider: Arc<dyn IStateProvider2>,
    transaction: i64,
}
#[derive(Default)]
struct Grants {
    writer: Option<(i64, usize)>,
    readers: BTreeMap<i64, usize>,
    disposed: bool,
    writers: std::collections::VecDeque<u64>,
    waiting_readers: BTreeMap<u64, i64>,
    ready_readers: BTreeSet<u64>,
    next_waiter: u64,
}
struct NameLock {
    id: i64,
    grants: Mutex<Grants>,
    changed: Notify,
}
#[derive(Clone, Copy)]
enum LockKind {
    Read,
    Add,
    Remove,
}
struct RegistryLock {
    registry: Arc<Registry>,
    name: String,
    lock: Arc<NameLock>,
    transaction: i64,
    kind: LockKind,
    released: std::sync::atomic::AtomicBool,
}
impl OperationContext for RegistryLock {}
impl LockContext for RegistryLock {
    fn is_tracking_context(&self) -> bool {
        false
    }
    fn unlock(&self) -> Result<()> {
        self.registry.release(self, true)
    }
}
struct Enumerator {
    entries: Vec<StateProviderEntry>,
    cursor: Mutex<Option<usize>>,
}
impl IStateProviderEnumerator for Enumerator {
    fn current(&self) -> Result<StateProviderEntry> {
        self.cursor
            .lock()
            .unwrap()
            .and_then(|i| self.entries.get(i).cloned())
            .ok_or_else(|| Status::InvalidOperation.error())
    }
    fn move_next(&self) -> Result<bool> {
        let mut cursor = self.cursor.lock().unwrap();
        let next = cursor.map_or(0, |i| i.saturating_add(1));
        *cursor = Some(next);
        Ok(next < self.entries.len())
    }
}
pub(crate) struct Registry {
    weak: Weak<Self>,
    runtime: Mutex<Weak<VolatileRuntime>>,
    factory: Option<Arc<dyn IStateProvider2Factory>>,
    state: Mutex<State>,
    #[cfg(test)]
    remove_discovery_pause: Mutex<
        Option<(
            super::version::Completion<()>,
            super::version::Completion<()>,
        )>,
    >,
    #[cfg(test)]
    disposed_lock_pause: Mutex<
        Option<(
            super::version::Completion<()>,
            super::version::Completion<()>,
        )>,
    >,
}
struct Frames {
    packets: Vec<OperationData>,
    index: Mutex<usize>,
}
impl IOperationDataStream for Frames {
    fn get_next(&self, _: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            let mut i = self.index.lock().unwrap();
            let result = self.packets.get(*i).cloned();
            *i += 1;
            Ok(result)
        })
    }
    fn dispose(&self) -> Result<()> {
        *self.index.lock().unwrap() = self.packets.len();
        Ok(())
    }
}
impl Registry {
    pub(crate) fn initialize_initial(
        &self,
        providers: &[(i64, Arc<dyn IStateProvider2>)],
    ) -> Result<Vec<(i64, Arc<dyn IStateProvider2>)>> {
        let runtime = self.runtime()?;
        let owner: Arc<dyn ITransactionalReplicator> = runtime.owner.clone();
        let mut objects = providers.iter().cloned().collect::<BTreeMap<_, _>>();
        let rows = self.rows();
        for row in &rows {
            if let std::collections::btree_map::Entry::Vacant(entry) = objects.entry(row.id) {
                let factory = self
                    .factory
                    .as_ref()
                    .ok_or_else(|| Status::NotImplemented.error())?;
                let identity = runtime.identity();
                entry.insert(factory.create(&FactoryArguments {
                    name: row.name.clone(),
                    state_provider_id: row.id,
                    type_name: row.type_name.clone(),
                    partition_id: identity.0,
                    replica_id: identity.1,
                    initialization_parameters: row.initialization.clone(),
                })?);
            }
        }
        fn depth(row: &Metadata, rows: &[Metadata]) -> Result<usize> {
            let mut parent = row.parent;
            let mut seen = vec![row.id];
            while parent != 0 {
                if seen.contains(&parent) {
                    return Err(Status::InvalidParameter.error());
                }
                seen.push(parent);
                parent = rows
                    .iter()
                    .find(|r| r.id == parent)
                    .ok_or_else(|| Status::InvalidParameter.error())?
                    .parent;
            }
            Ok(seen.len())
        }
        let mut ordered = rows
            .iter()
            .map(|r| Ok((depth(r, &rows)?, r)))
            .collect::<Result<Vec<_>>>()?;
        ordered.sort_by_key(|(d, _)| std::cmp::Reverse(*d));
        for (_, row) in &ordered {
            let children = rows
                .iter()
                .filter(|r| r.parent == row.id)
                .map(|r| objects[&r.id].clone())
                .collect::<Vec<_>>();
            let declared = objects[&row.id].get_children(&row.name)?;
            if declared.iter().any(|d| {
                !rows
                    .iter()
                    .any(|r| r.parent == row.id && r.name == d.name && r.type_name == d.type_name)
            }) {
                return Err(Status::NotImplemented.error());
            }
            objects[&row.id].initialize(
                Arc::downgrade(&owner),
                &runtime.work_folder().join(row.id.to_string()),
                if children.is_empty() {
                    None
                } else {
                    Some(&children)
                },
            )?;
        }
        ordered.sort_by_key(|(d, _)| *d);
        Ok(ordered
            .into_iter()
            .map(|(_, r)| (r.id, objects[&r.id].clone()))
            .collect())
    }
    pub(crate) fn new(
        settings: Option<RegistrySettings>,
        providers: &[(i64, Arc<dyn IStateProvider2>)],
    ) -> Result<Arc<Self>> {
        let (factory, metadata) = if let Some(settings) = settings {
            (Some(settings.factory), settings.metadata)
        } else {
            (
                None,
                providers
                    .iter()
                    .map(|(id, p)| Metadata {
                        id: *id,
                        parent: 0,
                        name: p.get_name().into(),
                        type_name: "fube.supplied".into(),
                        create_lsn: 0,
                        delete_lsn: -1,
                        initialization: None,
                        mode: ProviderMode::Active,
                    })
                    .collect(),
            )
        };
        let mut rows = BTreeMap::new();
        for row in metadata {
            row.validate()?;
            if rows.len() >= 64 || rows.insert(row.id, row).is_some() {
                return Err(Status::InvalidParameter.error());
            }
        }
        for (id, provider) in providers {
            if rows
                .get(id)
                .is_none_or(|row| row.name != provider.get_name())
            {
                return Err(Status::InvalidParameter.error());
            }
        }
        let mut names = BTreeSet::new();
        for row in rows.values() {
            if !names.insert(&row.name) {
                return Err(Status::InvalidParameter.error());
            }
            let mut id = row.parent;
            let mut ancestors = BTreeSet::from([row.id]);
            while id != 0 {
                if !ancestors.insert(id) {
                    return Err(Status::InvalidParameter.error());
                }
                id = rows
                    .get(&id)
                    .ok_or_else(|| Status::InvalidParameter.error())?
                    .parent;
            }
        }
        Ok(Arc::new_cyclic(|weak| Self {
            weak: weak.clone(),
            runtime: Mutex::new(Weak::new()),
            factory,
            #[cfg(test)]
            remove_discovery_pause: Mutex::new(None),
            #[cfg(test)]
            disposed_lock_pause: Mutex::new(None),
            state: Mutex::new(State {
                high_water: rows.keys().copied().max().unwrap_or(0).max(0),
                rows,
                prepared: Vec::new(),
                published: Vec::new(),
                version: false,
                seen: BTreeSet::new(),
                staged: BTreeMap::new(),
                events: Vec::new(),
                pending: BTreeMap::new(),
                locks: BTreeMap::new(),
                inflight: BTreeMap::new(),
            }),
        }))
    }
    pub(crate) fn bind(&self, runtime: Weak<VolatileRuntime>) {
        *self.runtime.lock().unwrap() = runtime;
    }
    fn runtime(&self) -> Result<Arc<VolatileRuntime>> {
        self.runtime
            .lock()
            .unwrap()
            .upgrade()
            .ok_or_else(|| Status::ObjectClosed.error())
    }
    fn shared(&self) -> Arc<Self> {
        self.weak.upgrade().unwrap()
    }
    pub(crate) fn rows(&self) -> Vec<Metadata> {
        self.state.lock().unwrap().rows.values().cloned().collect()
    }
    fn next_id(&self) -> Result<i64> {
        let now = super::identity::native_system_time()?;
        let mut state = self.state.lock().unwrap();
        state.high_water = if now > state.high_water {
            now
        } else {
            state
                .high_water
                .checked_add(1)
                .ok_or_else(|| Status::InsufficientResources.error())?
        };
        Ok(state.high_water)
    }
    fn validate_name(name: &str) -> Result<()> {
        let Some((scheme, rest)) = name.split_once(':') else {
            return Err(Self::invalid_name());
        };
        if scheme.is_empty()
            || !scheme.as_bytes()[0].is_ascii_alphabetic()
            || !scheme
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"+-.".contains(&c))
            || rest.is_empty()
            || name.chars().any(|c| c.is_control() || c.is_whitespace())
            || name == "fabric:/StateManager"
        {
            return Err(Self::invalid_name());
        }
        Ok(())
    }
    fn invalid_name() -> Error {
        Error::Remote {
            code: 0x80071bbeu32 as i32 as i64,
            message: "InvalidNameUri".into(),
        }
    }
    fn duplicate() -> Error {
        Error::Remote {
            code: 0x80071bc0u32 as i32 as i64,
            message: "NameAlreadyExists".into(),
        }
    }
    fn write_entry(
        &self,
        transaction: &dyn Transaction,
        name: &str,
    ) -> Result<Arc<VolatileRuntime>> {
        let runtime = self.runtime()?;
        runtime.owner.api()?;
        Self::validate_name(name)?;
        runtime.owner.writable()?;
        if transaction.get_is_atomic_operation() || transaction.get_transaction_id() <= 0 {
            return Err(Status::NotImplemented.error());
        }
        Ok(runtime)
    }
    pub(crate) fn get(&self, name: &str) -> Result<Arc<dyn IStateProvider2>> {
        let runtime = self.runtime()?;
        let _activity = runtime.owner.manager.cleanup_entry()?;
        runtime.owner.api()?;
        Self::validate_name(name)?;
        runtime.owner.readable()?;
        self.lookup(name)
    }
    fn lookup(&self, name: &str) -> Result<Arc<dyn IStateProvider2>> {
        self.lookup_metadata(name).map(|(_, provider)| provider)
    }
    fn lookup_metadata(&self, name: &str) -> Result<(Metadata, Arc<dyn IStateProvider2>)> {
        let runtime = self.runtime()?;
        // Capture native metadata and its provider together. Delete Apply uses
        // this same registry→directory lock order; never look up the name twice.
        let state = self.state.lock().unwrap();
        let row = state
            .rows
            .values()
            .find(|row| row.mode == ProviderMode::Active && row.name == name)
            .cloned()
            .ok_or_else(|| Status::NameDoesNotExist.error())?;
        let provider = runtime.owner.manager.directory.entry(row.id)?.provider;
        Ok((row, provider))
    }
    pub(crate) fn enumerator(
        &self,
        parents_only: bool,
    ) -> Result<Arc<dyn IStateProviderEnumerator>> {
        let runtime = self.runtime()?;
        let _activity = runtime.owner.manager.cleanup_entry()?;
        runtime.owner.api()?;
        runtime.owner.readable()?;
        let rows = self.rows();
        let mut entries = rows
            .into_iter()
            .filter(|row| row.mode == ProviderMode::Active && (!parents_only || row.parent == 0))
            .map(|row| {
                Ok((
                    row.name,
                    runtime.owner.manager.directory.entry(row.id)?.provider,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
        Ok(Arc::new(Enumerator {
            entries,
            cursor: Mutex::new(None),
        }))
    }
    async fn acquire(
        &self,
        name: &str,
        id: i64,
        transaction: i64,
        kind: LockKind,
        timeout: Option<Duration>,
    ) -> Result<Arc<RegistryLock>> {
        loop {
            let lock = {
                let mut state = self.state.lock().unwrap();
                if !state.locks.contains_key(name) && state.locks.len() >= 128 {
                    return Err(Status::InsufficientResources.error());
                }
                let active_id = state
                    .rows
                    .values()
                    .find(|row| row.name == name && row.mode == ProviderMode::Active)
                    .map_or(id, |row| row.id);
                state
                    .locks
                    .entry(name.to_owned())
                    .or_insert_with(|| {
                        Arc::new(NameLock {
                            id: active_id,
                            grants: Mutex::new(Grants::default()),
                            changed: Notify::new(),
                        })
                    })
                    .clone()
            };
            let start = Instant::now();
            let mut upgrade = 0;
            let mut queued = None;
            loop {
                let wake = lock.changed.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                let granted = {
                    let mut state = self.state.lock().unwrap();
                    let inflight = state
                        .inflight
                        .get(&transaction)
                        .is_some_and(|keys| keys.contains(name));
                    let mut grants = lock.grants.lock().unwrap();
                    if grants.disposed {
                        break;
                    }
                    let reentrant =
                        grants.writer.is_some_and(|(owner, _)| owner == transaction) && inflight;
                    let read = matches!(kind, LockKind::Read);
                    if !read && upgrade == 0 && inflight {
                        if let Some(count) = grants.readers.get_mut(&transaction) {
                            *count -= 1;
                            if *count == 0 {
                                grants.readers.remove(&transaction);
                            }
                            upgrade = 1;
                            state.inflight.remove(&transaction);
                            lock.changed.notify_waiters();
                        }
                    }
                    let granted = if reentrant {
                        grants.writer.as_mut().unwrap().1 += 1;
                        true
                    } else if read
                        && queued.is_some_and(|ticket| grants.ready_readers.remove(&ticket))
                    {
                        true
                    } else if read && grants.writer.is_none() && grants.writers.is_empty() {
                        *grants.readers.entry(transaction).or_default() += 1;
                        true
                    } else if !read
                        && grants.writer.is_none()
                        && grants.readers.is_empty()
                        && (grants.writers.is_empty() || queued == grants.writers.front().copied())
                    {
                        if queued.is_some() {
                            grants.writers.pop_front();
                        }
                        grants.writer = Some((transaction, 1 + upgrade));
                        true
                    } else {
                        if queued.is_none() {
                            let ticket = grants.next_waiter;
                            grants.next_waiter = ticket
                                .checked_add(1)
                                .ok_or_else(|| Status::InsufficientResources.error())?;
                            if read {
                                grants.waiting_readers.insert(ticket, transaction);
                            } else {
                                grants.writers.push_back(ticket);
                            }
                            queued = Some(ticket);
                        }
                        false
                    };
                    if granted {
                        state
                            .inflight
                            .entry(transaction)
                            .or_default()
                            .insert(name.to_owned());
                    }
                    granted
                };
                if !granted {
                    // Native StateManagerLockContext ignores the cancellation token
                    // while awaiting the key lock; each disposed-lock retry resets timeout.
                    if let Some(timeout) = timeout {
                        if tokio::time::timeout(timeout.saturating_sub(start.elapsed()), wake)
                            .await
                            .is_err()
                        {
                            if let Some(ticket) = queued {
                                let mut grants = lock.grants.lock().unwrap();
                                if grants.ready_readers.contains(&ticket) {
                                    continue;
                                }
                                grants.waiting_readers.remove(&ticket);
                                grants.writers.retain(|id| *id != ticket);
                                Self::grant_waiting_readers(&mut grants);
                                lock.changed.notify_waiters();
                            }
                            return Err(Status::Timeout.error());
                        }
                    } else {
                        wake.await;
                    }
                    continue;
                }
                return Ok(Arc::new(RegistryLock {
                    registry: self.shared(),
                    name: name.to_owned(),
                    lock: lock.clone(),
                    transaction,
                    kind,
                    released: std::sync::atomic::AtomicBool::new(false),
                }));
            }
            #[cfg(test)]
            {
                let pause = self.disposed_lock_pause.lock().unwrap().take();
                if let Some((entered, release)) = pause {
                    entered.complete(Ok(()));
                    release.wait().await.as_ref().clone()?;
                }
            }
        }
    }
    fn retain_lock(transaction: &dyn Transaction, lock: &Arc<RegistryLock>) -> Result<()> {
        if let Err(error) = transaction.add_lock_context(lock.clone()) {
            lock.registry.release(lock, false)?;
            return Err(error);
        }
        Ok(())
    }
    fn release(&self, context: &RegistryLock, cleanup: bool) -> Result<()> {
        use std::sync::atomic::Ordering;
        if context.released.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        let mut dispose = false;
        if cleanup {
            match context.kind {
                LockKind::Add => {
                    if state
                        .pending
                        .get(&context.lock.id)
                        .is_some_and(|p| p.transaction == context.transaction)
                    {
                        state.pending.remove(&context.lock.id);
                        state.staged.remove(&context.lock.id);
                        dispose = true;
                    } else if !state
                        .rows
                        .values()
                        .any(|r| r.name == context.name && r.mode == ProviderMode::Active)
                        && !state.pending.values().any(|p| p.row.name == context.name)
                    {
                        dispose = true;
                    }
                }
                LockKind::Remove => {
                    let runtime = self.runtime()?;
                    let mut entries = runtime.owner.manager.directory.entries.lock().unwrap();
                    if let Some(entry) = entries.get_mut(&context.lock.id) {
                        if entry.deleting_by == Some(context.transaction) {
                            entry.deleting_by = None;
                        }
                        dispose = entry.mode == ProviderMode::DelayDelete;
                    } else {
                        dispose = true;
                    }
                }
                LockKind::Read => {
                    dispose =
                        !state.rows.values().any(|row| {
                            row.name == context.name && row.mode == ProviderMode::Active
                        }) && !state
                            .pending
                            .values()
                            .any(|pending| pending.row.name == context.name);
                }
            }
        }
        let mut grants = context.lock.grants.lock().unwrap();
        if grants.disposed {
            context.released.store(true, Ordering::Release);
            return Ok(());
        }
        if let Some((owner, count)) = grants.writer.as_mut() {
            if *owner == context.transaction {
                *count -= 1;
                if *count == 0 {
                    grants.writer = None;
                    state.inflight.remove(&context.transaction);
                }
            }
        } else if let Some(count) = grants.readers.get_mut(&context.transaction) {
            *count -= 1;
            if *count == 0 {
                grants.readers.remove(&context.transaction);
            }
            state.inflight.remove(&context.transaction);
        }
        if dispose {
            grants.disposed = true;
            if state
                .locks
                .get(&context.name)
                .is_some_and(|l| Arc::ptr_eq(l, &context.lock))
            {
                state.locks.remove(&context.name);
            }
        } else {
            Self::grant_waiting_readers(&mut grants);
        }
        drop(grants);
        drop(state);
        context.lock.changed.notify_waiters();
        context.released.store(true, Ordering::Release);
        Ok(())
    }
    fn grant_waiting_readers(grants: &mut Grants) {
        if grants.writer.is_none() && grants.writers.is_empty() {
            for (ticket, transaction) in std::mem::take(&mut grants.waiting_readers) {
                *grants.readers.entry(transaction).or_default() += 1;
                grants.ready_readers.insert(ticket);
            }
        }
    }
    fn create_provider(&self, row: &Metadata) -> Result<Arc<dyn IStateProvider2>> {
        let runtime = self.runtime()?;
        let identity = runtime.identity();
        self.factory
            .as_ref()
            .ok_or_else(|| Status::NotImplemented.error())?
            .create(&FactoryArguments {
                name: row.name.clone(),
                type_name: row.type_name.clone(),
                state_provider_id: row.id,
                partition_id: identity.0,
                replica_id: identity.1,
                initialization_parameters: row.initialization.clone(),
            })
    }
    fn new_row(
        id: i64,
        name: String,
        type_name: String,
        initialization: Option<OperationData>,
        parent: i64,
    ) -> Metadata {
        Metadata {
            id,
            name,
            type_name,
            initialization,
            parent,
            create_lsn: -1,
            delete_lsn: -1,
            mode: ProviderMode::Active,
        }
    }
    fn children(
        &self,
        root: Arc<dyn IStateProvider2>,
        parent: i64,
    ) -> Result<Vec<(Metadata, Arc<dyn IStateProvider2>)>> {
        let mut queue = std::collections::VecDeque::from([root]);
        let mut children = BTreeMap::new();
        let mut visited = BTreeSet::new();
        let mut captured = 1usize; // retained root counts toward discovery budget
        while let Some(provider) = queue.pop_front() {
            if !visited.insert(provider.get_name().to_owned()) || visited.len() > 64 {
                return Err(Status::InvalidParameter.error());
            }
            for info in provider.get_children(provider.get_name())? {
                // FUBE ADAPTATION: bound live discovery/factory captures BEFORE
                // another object, not after a broad native GetChildren list.
                if captured >= 64 {
                    return Err(Status::InsufficientResources.error());
                }
                Self::validate_name(&info.name)?;
                let row = Self::new_row(
                    self.next_id()?,
                    info.name,
                    info.type_name,
                    info.initialization_parameters,
                    parent,
                );
                row.validate()?;
                let child = self.create_provider(&row)?;
                captured += 1;
                queue.push_back(child.clone());
                // Native GetChildren constructs with initialization, but AddSingle
                // replicates every descendant with null initialization and root parent.
                let mut row = row;
                row.initialization = None;
                children.entry(row.name.clone()).or_insert((row, child));
            }
        }
        Ok(children.into_values().collect())
    }
    async fn replicate(
        &self,
        transaction: Arc<dyn Transaction>,
        metadata: OperationData,
        redo: Option<OperationData>,
        timeout: Option<Duration>,
        cancellation: &Cancellation,
    ) -> Result<()> {
        let start = Instant::now();
        let mut backoff = 8u64;
        while !cancellation.is_cancelled() {
            let result = if fube_runtime::extensions::ExecutionRuntime::check_entry(None).is_ok() {
                transaction.add_operation(
                    Some(metadata.clone()),
                    None,
                    redo.clone(),
                    super::provider::REGISTRY_ID,
                    None,
                )
            } else {
                // An owned async continuation has no caller thread-local entry.
                // Use the existing ordinary native execution lane for the SAME
                // synchronous AddOperation; never substitute async single/End.
                let owner = transaction.clone();
                let metadata = metadata.clone();
                let redo = redo.clone();
                self.runtime()?
                    .replicator()
                    .execution_runtime()
                    .try_reserve()?
                    .run_owned(move || {
                        owner.add_operation(
                            Some(metadata),
                            None,
                            redo,
                            super::provider::REGISTRY_ID,
                            None,
                        )
                    })
                    .await
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) if super::status::retryable(&error) => {
                    if timeout.is_some_and(|timeout| start.elapsed() > timeout) {
                        return Err(Status::Timeout.error());
                    }
                    backoff = (backoff * 2).min(4096);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(Status::Cancelled.error())
    }
    async fn add_single(
        &self,
        transaction: &Arc<dyn Transaction>,
        row: Metadata,
        provider: Arc<dyn IStateProvider2>,
        take_lock: bool,
        timeout: Option<Duration>,
        cancellation: &Cancellation,
    ) -> Result<()> {
        if take_lock {
            let lock = self
                .acquire(
                    &row.name,
                    row.id,
                    transaction.get_transaction_id(),
                    LockKind::Add,
                    timeout,
                )
                .await?;
            Self::retain_lock(transaction.as_ref(), &lock)?;
        }
        let metadata = RegistryOperation {
            id: row.id,
            name: row.name.clone(),
            kind: ApplyType::Insert,
        }
        .encode()?;
        let redo = InsertRedo {
            parent: row.parent,
            type_name: row.type_name.clone(),
            initialization: row.initialization.clone(),
        }
        .encode()?;
        // Decode/preflight sizing before any transient mutation, not after caching.
        InsertRedo::decode(&redo)?;
        {
            let mut state = self.state.lock().unwrap();
            if state
                .rows
                .values()
                .any(|m| m.mode == ProviderMode::Active && m.name == row.name)
                || state.pending.values().any(|p| p.row.name == row.name)
            {
                return Err(Self::duplicate());
            }
            if state.rows.len() + state.pending.len() >= 64 {
                return Err(Status::InsufficientResources.error());
            }
            state.pending.insert(
                row.id,
                Pending {
                    row,
                    provider,
                    transaction: transaction.get_transaction_id(),
                },
            );
        }
        self.replicate(
            transaction.clone(),
            metadata,
            Some(redo),
            timeout,
            cancellation,
        )
        .await
    }
    async fn add_tree(
        &self,
        transaction: &Arc<dyn Transaction>,
        row: Metadata,
        provider: Arc<dyn IStateProvider2>,
        children: Vec<(Metadata, Arc<dyn IStateProvider2>)>,
        timeout: Option<Duration>,
        cancellation: &Cancellation,
    ) -> Result<()> {
        for (child, provider) in children {
            self.add_single(transaction, child, provider, true, timeout, cancellation)
                .await?;
        }
        self.add_single(transaction, row, provider, false, timeout, cancellation)
            .await
    }
    pub(crate) fn add(
        &self,
        transaction: Arc<dyn Transaction>,
        name: String,
        type_name: String,
        initialization: Option<OperationData>,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'static, ()> {
        let registry = self.shared();
        own(Box::pin(async move {
            let runtime = registry.write_entry(transaction.as_ref(), &name)?;
            let _activity = runtime.owner.manager.cleanup_entry()?;
            if registry.factory.is_none() {
                return Err(Status::NotImplemented.error());
            }
            let mut row = Self::new_row(registry.next_id()?, name, type_name, initialization, 0);
            row.validate()?;
            let provider = registry.create_provider(&row)?;
            let mut children = registry.children(provider.clone(), row.id)?;
            let lock = registry
                .acquire(
                    &row.name,
                    row.id,
                    transaction.get_transaction_id(),
                    LockKind::Add,
                    timeout,
                )
                .await?;
            Self::retain_lock(transaction.as_ref(), &lock)?;
            // Native AddAsync807–858 preserves the original factory objects but
            // adopts the acquired parent name-lock identity for metadata/children.
            row.id = lock.lock.id;
            for (child, _) in &mut children {
                child.parent = row.id;
            }
            registry
                .add_tree(
                    &transaction,
                    row,
                    provider,
                    children,
                    timeout,
                    &cancellation,
                )
                .await
        }))
    }
    pub(crate) fn get_or_add(
        &self,
        transaction: Arc<dyn Transaction>,
        name: String,
        type_name: String,
        initialization: Option<OperationData>,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'static, GetOrAddResult> {
        let registry = self.shared();
        own(Box::pin(async move {
            let runtime = registry.write_entry(transaction.as_ref(), &name)?;
            let _activity = runtime.owner.manager.cleanup_entry()?;
            let id = registry.next_id()?;
            let lock = registry
                .acquire(
                    &name,
                    id,
                    transaction.get_transaction_id(),
                    LockKind::Read,
                    timeout,
                )
                .await?;
            match registry.get(&name) {
                Ok(provider) => {
                    Self::retain_lock(transaction.as_ref(), &lock)?;
                    return Ok(GetOrAddResult {
                        state_provider: provider,
                        already_exists: true,
                    });
                }
                Err(error) if Status::NameDoesNotExist.matches(&error) => {}
                Err(error) => {
                    registry.release(&lock, false)?;
                    return Err(error);
                }
            }
            registry.release(&lock, false)?;
            let lock = registry
                .acquire(
                    &name,
                    id,
                    transaction.get_transaction_id(),
                    LockKind::Add,
                    timeout,
                )
                .await?;
            Self::retain_lock(transaction.as_ref(), &lock)?;
            match registry.get(&name) {
                Ok(provider) => {
                    return Ok(GetOrAddResult {
                        state_provider: provider,
                        already_exists: true,
                    })
                }
                Err(error) if Status::NameDoesNotExist.matches(&error) => {}
                Err(error) => return Err(error),
            }
            let row = Self::new_row(lock.lock.id, name, type_name, initialization, 0);
            row.validate()?;
            let provider = registry.create_provider(&row)?;
            let children = registry.children(provider.clone(), row.id)?;
            registry
                .add_tree(
                    &transaction,
                    row,
                    provider.clone(),
                    children,
                    timeout,
                    &cancellation,
                )
                .await?;
            Ok(GetOrAddResult {
                state_provider: provider,
                already_exists: false,
            })
        }))
    }
    pub(crate) fn remove(
        &self,
        transaction: Arc<dyn Transaction>,
        name: String,
        timeout: Option<Duration>,
        cancellation: Cancellation,
    ) -> ApiFuture<'static, ()> {
        let registry = self.shared();
        own(Box::pin(async move {
            let runtime = registry.write_entry(transaction.as_ref(), &name)?;
            let _activity = runtime.owner.manager.cleanup_entry()?;
            let (root, provider) = registry.lookup_metadata(&name)?;
            #[cfg(test)]
            {
                let pause = registry.remove_discovery_pause.lock().unwrap().take();
                if let Some((entered, release)) = pause {
                    entered.complete(Ok(()));
                    release.wait().await.as_ref().clone()?;
                }
            }
            let children = registry.children(provider, root.id)?;
            // StateManager RemoveAsync946–960: parent first, THEN children.
            for child_name in std::iter::once(name).chain(children.into_iter().map(|(r, _)| r.name))
            {
                let row = registry
                    .rows()
                    .into_iter()
                    .find(|r| r.mode == ProviderMode::Active && r.name == child_name)
                    .ok_or_else(|| Status::NameDoesNotExist.error())?;
                let lock = registry
                    .acquire(
                        &child_name,
                        row.id,
                        transaction.get_transaction_id(),
                        LockKind::Remove,
                        timeout,
                    )
                    .await?;
                Self::retain_lock(transaction.as_ref(), &lock)?;
                // Native RemoveSingle4365–4429 refreshes the WHOLE metadata
                // object under the acquired write lock, including its new ID.
                let (row, current) = registry.lookup_metadata(&child_name)?;
                {
                    let mut entries = runtime.owner.manager.directory.entries.lock().unwrap();
                    let entry = entries
                        .get_mut(&row.id)
                        .ok_or_else(|| Status::NameDoesNotExist.error())?;
                    if entry.deleting_by.is_some() {
                        return Err(Status::NameDoesNotExist.error());
                    }
                    entry.deleting_by = Some(transaction.get_transaction_id());
                }
                current
                    .prepare_for_remove(transaction.clone(), cancellation.clone())
                    .await?;
                let metadata = RegistryOperation {
                    id: row.id,
                    name: child_name,
                    kind: ApplyType::Delete,
                }
                .encode()?;
                registry
                    .replicate(transaction.clone(), metadata, None, timeout, &cancellation)
                    .await?;
            }
            Ok(())
        }))
    }
    fn event(&self, event: &'static str) {
        let mut s = self.state.lock().unwrap();
        if s.events.len() == 1024 {
            s.events.remove(0);
        }
        s.events.push(event);
    }
    pub(crate) fn events(&self) -> Vec<&'static str> {
        self.state.lock().unwrap().events.clone()
    }
    fn tree(&self, root: i64) -> Result<Vec<(Metadata, Arc<dyn IStateProvider2>)>> {
        fn visit(
            id: i64,
            staged: &BTreeMap<i64, (Metadata, Arc<dyn IStateProvider2>)>,
            path: &mut Vec<i64>,
            out: &mut Vec<(Metadata, Arc<dyn IStateProvider2>)>,
        ) -> Result<()> {
            if path.contains(&id) || path.len() >= 64 {
                return Err(Status::InvalidParameter.error());
            }
            let item = staged
                .get(&id)
                .ok_or_else(|| Status::InvalidParameter.error())?
                .clone();
            path.push(id);
            out.push(item);
            for (child, (metadata, _)) in staged {
                if metadata.parent == id {
                    visit(*child, staged, path, out)?;
                }
            }
            path.pop();
            Ok(())
        }
        let state = self.state.lock().unwrap();
        let mut result = Vec::new();
        visit(root, &state.staged, &mut Vec::new(), &mut result)?;
        Ok(result)
    }
    async fn copy_row(&self, row: Metadata, copy: bool) -> Result<Vec<CopiedMetadata>> {
        {
            let mut state = self.state.lock().unwrap();
            state.high_water = state.high_water.max(row.id);
        }
        let runtime = self.runtime()?;
        let existing = runtime
            .owner
            .manager
            .directory
            .entries
            .lock()
            .unwrap()
            .get(&row.id)
            .cloned();
        if let Some(entry) = existing {
            // Native full-copy resurrection reuses the retained object and begins
            // it without another Open/Recover/role; deleted objects remain retained.
            runtime
                .owner
                .manager
                .directory
                .set_mode(row.id, row.mode, None);
            {
                let mut state = self.state.lock().unwrap();
                let retained = state
                    .rows
                    .get_mut(&row.id)
                    .ok_or_else(|| Status::InvalidParameter.error())?;
                retained.mode = row.mode;
                if row.mode == ProviderMode::DelayDelete {
                    retained.delete_lsn = row.delete_lsn;
                }
            }
            if copy && row.mode == ProviderMode::Active {
                entry.provider.begin_setting_current_state().await?;
            }
            return Ok(vec![CopiedMetadata {
                id: row.id,
                mode: row.mode,
            }]);
        }
        let factory = self
            .factory
            .as_ref()
            .ok_or_else(|| Status::NotImplemented.error())?;
        let identity = runtime.identity();
        let provider = factory.create(&FactoryArguments {
            name: row.name.clone(),
            state_provider_id: row.id,
            type_name: row.type_name.clone(),
            partition_id: identity.0,
            replica_id: identity.1,
            initialization_parameters: row.initialization.clone(),
        })?;
        let children: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .staged
            .values()
            .filter(|(child, _)| child.parent == row.id)
            .map(|(_, p)| p.clone())
            .collect();
        let owner: Arc<dyn ITransactionalReplicator> = runtime.owner.clone();
        provider.initialize(
            Arc::downgrade(&owner),
            &runtime.work_folder().join(row.id.to_string()),
            if children.is_empty() {
                None
            } else {
                Some(&children)
            },
        )?;
        self.state
            .lock()
            .unwrap()
            .staged
            .insert(row.id, (row.clone(), provider));
        if row.parent != 0 {
            return Ok(Vec::new());
        }
        let tree = self.tree(row.id)?;
        let mut installed = Vec::new();
        for (metadata, provider) in tree {
            provider.open(Cancellation::default()).await?;
            // Native cleanup metadata/strong ownership immediately after Open,
            // before any fallible Recover/role/Begin callback.
            let registered = runtime.adopt_opened(
                metadata.id,
                provider.clone(),
                if copy {
                    metadata.mode
                } else {
                    ProviderMode::Stale
                },
            );
            if let Err(error) = registered {
                if provider.close(Cancellation::default()).await.is_err() {
                    provider.abort();
                }
                return Err(error);
            }
            self.state
                .lock()
                .unwrap()
                .rows
                .insert(metadata.id, metadata.clone());
            if metadata.mode == ProviderMode::Active {
                provider.recover_checkpoint(Cancellation::default()).await?;
                let role = crate::ITransactionManager::get_role(runtime.owner.manager.as_ref());
                provider
                    .change_role(
                        if copy || role == ReplicaRole::ActiveSecondary {
                            ReplicaRole::IdleSecondary
                        } else {
                            role
                        },
                        Cancellation::default(),
                    )
                    .await?;
                if !copy && role == ReplicaRole::ActiveSecondary {
                    provider.change_role(role, Cancellation::default()).await?;
                }
                if copy {
                    provider.begin_setting_current_state().await?;
                }
            }
            if !copy {
                runtime
                    .owner
                    .manager
                    .directory
                    .set_mode(metadata.id, metadata.mode, None);
            }
            installed.push(CopiedMetadata {
                id: metadata.id,
                mode: metadata.mode,
            });
            self.state.lock().unwrap().staged.remove(&metadata.id);
        }
        Ok(installed)
    }
    pub(crate) async fn cleanup_before(&self, safe: i64) -> Result<()> {
        let runtime = self.runtime()?;
        let doomed = self
            .rows()
            .into_iter()
            .filter(|row| match row.mode {
                ProviderMode::DelayDelete => row.delete_lsn < safe,
                ProviderMode::Stale => {
                    (if row.delete_lsn == -1 {
                        row.create_lsn
                    } else {
                        row.delete_lsn
                    }) < safe
                }
                ProviderMode::Active => false,
            })
            .collect::<Vec<_>>();
        // StateManager5301–5425 is sequential, retiring each successful object
        // immediately. A later failure must not leave earlier closed objects in
        // the deleted list, nor remove cleanup ownership before actual Close.
        for row in doomed {
            let provider = runtime.owner.manager.directory.entry(row.id)?.provider;
            provider
                .change_role(ReplicaRole::None, Cancellation::default())
                .await?;
            provider.remove_state(Cancellation::default()).await?;
            runtime.retire_provider(row.id).await?;
            let mut state = self.state.lock().unwrap();
            state.rows.remove(&row.id);
            state.prepared.retain(|m| m.id != row.id);
            state.published.retain(|m| m.id != row.id);
        }
        Ok(())
    }
}
impl LocalStateCallbacks for Registry {
    fn prepare(&self, _: i64, metadata: &[RegistryMetadata]) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        s.prepared = metadata
            .iter()
            .map(|m| {
                s.rows
                    .get(&m.id)
                    .cloned()
                    .ok_or_else(|| Status::InvalidParameter.error())
            })
            .collect::<Result<_>>()?;
        s.events.push("registry-prepare");
        Ok(())
    }
    fn perform(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("registry-perform-ready");
            Ok(())
        })
    }
    fn publish(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.published = state.prepared.clone();
        state.events.push("registry-perform");
        Ok(())
    }
    fn complete(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("registry-complete");
            Ok(())
        })
    }
    fn cleanup(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.cleanup_before(self.runtime()?.maintenance().safe_lsn())
                .await
        })
    }
    fn get_current_state(
        &self,
        _: i64,
        _: &[RegistryMetadata],
    ) -> Result<Arc<dyn IOperationDataStream>> {
        let rows = self.state.lock().unwrap().published.clone();
        let (operation_segments, operation_bytes) = self.runtime()?.copy_limits();
        Ok(Arc::new(Frames {
            packets: registry_codec::encode_copy_with_limits(
                rows,
                &fube_data::Settings {
                    operation_segments,
                    operation_bytes,
                    ..Default::default()
                },
            )?,
            index: Mutex::new(0),
        }))
    }
    fn begin(&self) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        s.version = false;
        s.seen.clear();
        for row in s.rows.values_mut() {
            row.mode = ProviderMode::Stale;
        }
        s.events.push("registry-begin");
        Ok(())
    }
    fn begins_in_set(&self) -> bool {
        true
    }
    fn set(&self, _: i64, data: OperationData) -> ApiFuture<'_, Vec<CopiedMetadata>> {
        let registry = self.shared();
        own(Box::pin(async move {
            match registry_codec::decode_copy(&data)? {
                RegistryPacket::Version => {
                    let mut s = registry.state.lock().unwrap();
                    if s.version {
                        return Err(Status::InvalidOperation.error());
                    }
                    s.version = true;
                    Ok(Vec::new())
                }
                RegistryPacket::Metadata(rows) => {
                    if !registry.state.lock().unwrap().version {
                        return Err(Status::InvalidOperation.error());
                    }
                    {
                        let mut s = registry.state.lock().unwrap();
                        let mut seen = s.seen.clone();
                        for row in &rows {
                            row.validate()?;
                            if !seen.insert(row.id) {
                                return Err(Status::InvalidOperation.error());
                            }
                            if let Some(existing) = s.rows.get(&row.id) {
                                if existing.name != row.name
                                    || existing.type_name != row.type_name
                                    || existing.parent != row.parent
                                {
                                    return Err(Status::InvalidParameter.error());
                                }
                            }
                        }
                        if seen.len() > 64 {
                            return Err(Status::InsufficientResources.error());
                        }
                        s.seen = seen;
                    }
                    let mut result = Vec::new();
                    for row in rows {
                        result.extend(registry.copy_row(row, true).await?);
                    }
                    Ok(result)
                }
            }
        }))
    }
    fn end(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if !self.state.lock().unwrap().staged.is_empty() {
                return Err(Status::InvalidOperation.error());
            }
            self.event("registry-end");
            Ok(())
        })
    }
}
impl RegistryCallbacks for Registry {
    fn preflight(
        &self,
        metadata: Option<&OperationData>,
        data: Option<&OperationData>,
    ) -> Result<()> {
        let op =
            RegistryOperation::decode(metadata.ok_or_else(|| Status::InvalidParameter.error())?)?;
        if op.kind == ApplyType::Insert {
            let redo = InsertRedo::decode(data.ok_or_else(|| Status::InvalidParameter.error())?)?;
            Self::new_row(
                op.id,
                op.name.clone(),
                redo.type_name,
                redo.initialization,
                redo.parent,
            )
            .validate()?;
            if self.runtime()?.owner.manager.role() == super::processor::ProcessingRole::Primary {
                let state = self.state.lock().unwrap();
                if !state
                    .rows
                    .get(&op.id)
                    .is_some_and(|row| row.name == op.name)
                    && !state
                        .pending
                        .get(&op.id)
                        .is_some_and(|p| p.row.name == op.name)
                {
                    return Err(Status::InvalidParameter.error());
                }
            }
        } else if data.is_some() {
            return Err(Status::InvalidParameter.error());
        }
        Ok(())
    }
    fn apply(
        &self,
        lsn: i64,
        transaction: Arc<dyn TransactionBase>,
        context: ApplyContext,
        metadata: Option<OperationData>,
        data: Option<OperationData>,
    ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>> {
        Box::pin(async move {
            if !matches!(
                context,
                ApplyContext::PrimaryRedo
                    | ApplyContext::SecondaryRedo
                    | ApplyContext::RecoveryRedo
            ) || transaction.get_is_atomic_operation()
            {
                return Err(Status::NotImplemented.error());
            }
            let op = RegistryOperation::decode(
                metadata
                    .as_ref()
                    .ok_or_else(|| Status::InvalidParameter.error())?,
            )?;
            match op.kind {
                ApplyType::Delete => {
                    let runtime = self.runtime()?;
                    let mut s = self.state.lock().unwrap();
                    let row = s
                        .rows
                        .get_mut(&op.id)
                        .ok_or_else(|| Status::InvalidParameter.error())?;
                    if row.name != op.name || data.is_some() {
                        return Err(Status::InvalidParameter.error());
                    }
                    row.mode = ProviderMode::DelayDelete;
                    row.delete_lsn = lsn;
                    runtime.owner.manager.directory.set_mode(
                        op.id,
                        ProviderMode::DelayDelete,
                        None,
                    );
                }
                ApplyType::Insert => {
                    let redo = InsertRedo::decode(
                        data.as_ref()
                            .ok_or_else(|| Status::InvalidParameter.error())?,
                    )?;
                    let row = Metadata {
                        id: op.id,
                        parent: redo.parent,
                        name: op.name,
                        type_name: redo.type_name,
                        create_lsn: lsn,
                        delete_lsn: -1,
                        initialization: redo.initialization,
                        mode: ProviderMode::Active,
                    };
                    row.validate()?;
                    if context == ApplyContext::PrimaryRedo {
                        self.primary_insert(row, transaction.get_transaction_id())
                            .await?;
                    } else {
                        self.secondary_insert(row, transaction.get_commit_sequence_number())
                            .await?;
                    }
                }
            }
            Ok(None)
        })
    }
    fn unlock(&self, _: &dyn OperationContext) -> Result<()> {
        Ok(())
    }
}

impl Registry {
    async fn primary_insert(&self, row: Metadata, transaction: i64) -> Result<()> {
        let runtime = self.runtime()?;
        let provider = {
            let mut state = self.state.lock().unwrap();
            if let Some(pending) = state.pending.get_mut(&row.id) {
                if pending.transaction != transaction
                    || pending.row.name != row.name
                    || pending.row.type_name != row.type_name
                    || pending.row.parent != row.parent
                {
                    return Err(Status::InvalidParameter.error());
                }
                pending.row.create_lsn = row.create_lsn;
                Some(pending.provider.clone())
            } else if state.rows.contains_key(&row.id) {
                None
            } else {
                return Err(Status::InvalidParameter.error());
            }
        };
        let Some(provider) = provider else {
            // Preinitialized source fixtures may replay an existing insertion.
            let _ = self.copy_row(row, false).await?;
            return Ok(());
        };
        let children = self
            .state
            .lock()
            .unwrap()
            .staged
            .values()
            .filter(|(child, _)| child.parent == row.id)
            .map(|(_, provider)| provider.clone())
            .collect::<Vec<_>>();
        let owner: Arc<dyn ITransactionalReplicator> = runtime.owner.clone();
        provider.initialize(
            Arc::downgrade(&owner),
            &runtime.work_folder().join(row.id.to_string()),
            if children.is_empty() {
                None
            } else {
                Some(&children)
            },
        )?;
        self.state
            .lock()
            .unwrap()
            .staged
            .insert(row.id, (row.clone(), provider));
        if row.parent != 0 {
            return Ok(());
        }
        for (metadata, provider) in self.tree(row.id)? {
            provider.open(Cancellation::default()).await?;
            let ready = async {
                provider.recover_checkpoint(Cancellation::default()).await?;
                let role = ITransactionManager::get_role(runtime.owner.manager.as_ref());
                match role {
                    ReplicaRole::Primary | ReplicaRole::IdleSecondary => {
                        provider.change_role(role, Cancellation::default()).await?;
                    }
                    ReplicaRole::ActiveSecondary => {
                        provider
                            .change_role(ReplicaRole::IdleSecondary, Cancellation::default())
                            .await?;
                        provider.change_role(role, Cancellation::default()).await?;
                    }
                    _ => return Err(Status::InvalidOperation.error()),
                }
                runtime.adopt_opened(metadata.id, provider.clone(), ProviderMode::Active)
            }
            .await;
            if let Err(error) = ready {
                if provider.close(Cancellation::default()).await.is_err() {
                    provider.abort();
                }
                return Err(error);
            }
            // Only successful post-Open callbacks reset native TransientCreate.
            let mut state = self.state.lock().unwrap();
            state.rows.insert(metadata.id, metadata.clone());
            state.pending.remove(&metadata.id);
            state.staged.remove(&metadata.id);
        }
        Ok(())
    }
    async fn secondary_insert(&self, row: Metadata, commit: i64) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            state.high_water = state.high_water.max(row.id);
        }
        let runtime = self.runtime()?;
        let existing = self.state.lock().unwrap().rows.get(&row.id).cloned();
        if existing
            .as_ref()
            .is_some_and(|m| m.mode == ProviderMode::DelayDelete)
        {
            return Ok(());
        }
        let active = self
            .rows()
            .into_iter()
            .find(|m| m.mode == ProviderMode::Active && m.name == row.name);
        if let Some(active) = active {
            if active.id != row.id
                || (active.create_lsn != row.create_lsn && active.create_lsn != commit)
            {
                return Err(Status::InvalidParameter.error());
            }
            return Ok(());
        }
        if let Some(retained) = existing.filter(|m| m.mode == ProviderMode::Stale) {
            if retained.name != row.name {
                return Err(Status::InvalidParameter.error());
            }
            let provider = runtime.owner.manager.directory.entry(row.id)?.provider;
            self.state
                .lock()
                .unwrap()
                .rows
                .get_mut(&row.id)
                .unwrap()
                .mode = ProviderMode::Active;
            runtime
                .owner
                .manager
                .directory
                .set_mode(row.id, ProviderMode::Active, None);
            provider.begin_setting_current_state().await?;
            provider
                .end_setting_current_state(Cancellation::default())
                .await?;
            if crate::ITransactionManager::get_role(runtime.owner.manager.as_ref())
                == ReplicaRole::ActiveSecondary
            {
                provider
                    .change_role(ReplicaRole::ActiveSecondary, Cancellation::default())
                    .await?;
            }
            return Ok(());
        }
        let _ = self.copy_row(row, false).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "phase4_registry_tests.rs"]
mod tests;
