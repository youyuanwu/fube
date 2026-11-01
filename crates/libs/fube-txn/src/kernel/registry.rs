// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native volatile registry checkpoint/copy/factory lifecycle. Source StateManager
//! 3592–3924,4926–5034,5301–5425; CheckpointFile154–389; no snapshot envelope.
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
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};

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
}
pub(crate) struct Registry {
    weak: Weak<Self>,
    runtime: Mutex<Weak<VolatileRuntime>>,
    factory: Option<Arc<dyn IStateProvider2Factory>>,
    state: Mutex<State>,
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
            state: Mutex::new(State {
                rows,
                prepared: Vec::new(),
                published: Vec::new(),
                version: false,
                seen: BTreeSet::new(),
                staged: BTreeMap::new(),
                events: Vec::new(),
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
            InsertRedo::decode(data.ok_or_else(|| Status::InvalidParameter.error())?)?;
            if self.runtime()?.owner.manager.role() == super::processor::ProcessingRole::Primary
                && !self.state.lock().unwrap().rows.contains_key(&op.id)
            {
                return Err(Status::NotImplemented.error());
            }
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
                    if context == ApplyContext::PrimaryRedo
                        && !self.state.lock().unwrap().rows.contains_key(&op.id)
                    {
                        return Err(Status::NotImplemented.error());
                    }
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
                    if context == ApplyContext::PrimaryRedo {
                        let _ = self.copy_row(row, false).await?;
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
    async fn secondary_insert(&self, row: Metadata, commit: i64) -> Result<()> {
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
