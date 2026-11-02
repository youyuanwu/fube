// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native CopyStream.cpp:167–351,381–510 and CheckpointManager544–710.
use super::{
    copy_codec::{
        add_stage, find_copy_mode, CopyContext, CopyHeader, CopyMetadata, CopyMode, Stage,
    },
    memory_log::{Checkpoint, MemoryLog},
    runtime::VolatileRuntime,
    status::Status,
    transaction::own,
};
use crate::{
    internal::IStateProviderManager, ApiFuture, Cancellation, IOperationDataStream, OperationData,
    Result,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
struct Cursor {
    header: bool,
    stage: Stage,
    position: u64,
    upto: i64,
    ended: bool,
}
pub(crate) struct CopySender {
    target_replica: i64,
    weak: Weak<Self>,
    runtime: Weak<VolatileRuntime>,
    memory: Arc<Mutex<MemoryLog>>,
    reader: Mutex<Option<u64>>,
    state: Mutex<Option<Arc<dyn IOperationDataStream>>>,
    checkpoint: Option<Checkpoint>,
    starting: i64,
    mode: CopyMode,
    cursor: tokio::sync::Mutex<Cursor>,
    disposed: AtomicBool,
}
impl CopySender {
    pub(crate) async fn create(
        runtime: &Arc<VolatileRuntime>,
        upto: i64,
        target: Option<CopyContext>,
    ) -> Result<Arc<Self>> {
        let maintenance = runtime.maintenance();
        #[cfg(test)]
        maintenance.copy_selection_entered.complete(Ok(()));
        let consistency = maintenance.consistency.clone();
        let held = consistency.lock().await;
        let selected = Self::select(runtime, upto, target);
        drop(held); // native releases descriptor/reader lock BEFORE state capture
        let selected = selected?;
        if matches!(selected.mode, CopyMode::Full(_)) {
            match runtime
                .owner
                .manager
                .directory
                .get_current_state(selected.target_replica)
            {
                Ok(state) => *selected.state.lock().unwrap() = Some(state),
                Err(error) => {
                    selected.dispose()?;
                    return Err(error);
                }
            }
        }
        Ok(selected)
    }
    fn select(
        runtime: &Arc<VolatileRuntime>,
        upto: i64,
        target: Option<CopyContext>,
    ) -> Result<Arc<Self>> {
        let manager = &runtime.owner.manager;
        let tail = manager.tail();
        let memory = manager.memory().unwrap();
        let mut memory_state = memory.lock().unwrap();
        let (head_position, _, head, head_epoch) = memory_state.head_identity();
        let source = CopyContext {
            replica: runtime.identity().1,
            vector: memory_state.vector.clone(),
            head_epoch,
            head,
            tail,
            atomic_redo: 0,
        };
        let target = match target {
            Some(target) => target,
            None => CopyContext::volatile_null(super::identity::native_system_time()?),
        };
        let mode = find_copy_mode(&source, &target)?;
        if matches!(mode, CopyMode::FalseProgress { .. }) {
            return Err(Status::NotImplemented.error());
        }
        let checkpoint = memory_state.completed.clone();
        let (starting, position, stage) = match mode {
            CopyMode::None => (source.tail, head_position, Stage::None),
            CopyMode::Full(_) => {
                let checkpoint = checkpoint
                    .as_ref()
                    .ok_or_else(|| Status::InvalidOperation.error())?;
                (
                    checkpoint.starting_lsn,
                    checkpoint.reader_position,
                    Stage::State,
                )
            }
            CopyMode::Partial { source, target } => {
                let starting = source.min(target);
                let position = memory_state
                    .entries
                    .iter()
                    .rfind(|e| e.logical.is_none() && e.lsn < starting)
                    .map_or(head_position, |e| e.position);
                (starting, position, Stage::Log)
            }
            _ => unreachable!(),
        };
        let reader = if stage == Stage::None {
            None
        } else {
            Some(memory_state.pin_copy(position, stage == Stage::State)?)
        };
        drop(memory_state);
        Ok(Arc::new_cyclic(|weak| Self {
            target_replica: target.replica,
            weak: weak.clone(),
            runtime: Arc::downgrade(runtime),
            memory,
            reader: Mutex::new(reader),
            state: Mutex::new(None),
            checkpoint,
            starting,
            mode,
            cursor: tokio::sync::Mutex::new(Cursor {
                header: false,
                stage,
                position,
                upto,
                ended: false,
            }),
            disposed: AtomicBool::new(false),
        }))
    }
    pub(crate) fn mode(&self) -> CopyMode {
        self.mode
    }
    pub(crate) fn dispose(&self) -> Result<()> {
        if !self.disposed.swap(true, Ordering::AcqRel) {
            if let Some(reader) = self.reader.lock().unwrap().take() {
                self.memory.lock().unwrap().unpin(reader);
            }
            let state = self.state.lock().unwrap().take();
            if let Some(state) = state {
                state.dispose()?;
            }
        }
        Ok(())
    }
    async fn next(&self, cancellation: Cancellation) -> Result<Option<OperationData>> {
        let mut cursor = self.cursor.lock().await;
        if self.disposed.load(Ordering::Acquire) {
            return Err(Status::ObjectClosed.error());
        }
        if cursor.ended {
            return Ok(None);
        }
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| Status::ObjectClosed.error())?;
        if !cursor.header {
            runtime.owner.manager.wait_flushed(cursor.upto).await?;
            if self.disposed.load(Ordering::Acquire) {
                return Err(Status::ObjectClosed.error());
            }
            cursor.header = true;
            return CopyHeader {
                stage: cursor.stage,
                primary: runtime.identity().1,
            }
            .encode()
            .map(Some);
        }
        if cursor.stage == Stage::None {
            cursor.ended = true;
            return Ok(None);
        }
        if cursor.stage == Stage::State {
            let state = self.state.lock().unwrap().as_ref().unwrap().clone();
            let next = state.get_next(cancellation).await?;
            if self.disposed.load(Ordering::Acquire) {
                return Err(Status::ObjectClosed.error());
            }
            if let Some(data) = next {
                return add_stage(data, Stage::State).map(Some);
            }
            let state = self.state.lock().unwrap().take();
            if let Some(state) = state {
                state.dispose()?;
            }
            cursor.stage = Stage::Log;
            let checkpoint = self.checkpoint.as_ref().unwrap();
            cursor.upto = cursor.upto.max(checkpoint.stable);
            let metadata = CopyMetadata {
                vector: checkpoint.vector.clone(),
                starting_epoch: checkpoint.starting_epoch,
                starting: self.starting,
                checkpoint: checkpoint.lsn,
                upto: cursor.upto,
                highest_provider: runtime.owner.manager.tail(),
            };
            return metadata.encode().map(Some);
        }
        runtime.owner.manager.wait_flushed(cursor.upto).await?;
        if self.disposed.load(Ordering::Acquire) {
            return Err(Status::ObjectClosed.error());
        }
        let memory = self.memory.lock().unwrap();
        let mut segments = Vec::new();
        let mut bytes = 0;
        let (max_segments, max_bytes) = runtime.copy_limits();
        for entry in &memory.entries {
            if entry.position < cursor.position {
                continue;
            }
            let Some(node) = &entry.logical else {
                cursor.position = entry.position + 1;
                continue;
            };
            let record = node.record();
            if record.lsn < self.starting
                || (record.lsn == self.starting
                    && !matches!(record.kind, super::record::RecordKind::UpdateEpoch { .. }))
            {
                cursor.position = entry.position + 1;
                continue;
            }
            if record.lsn > cursor.upto {
                break;
            }
            let data = record.encode()?;
            if data.segments().len() + 1 > max_segments || data.bytes() + 4 > max_bytes {
                return Err(crate::Error::new(
                    fube_runtime::ErrorKind::ReplicationOperationTooLarge,
                    "finite native copy framing including stage",
                ));
            }
            if !segments.is_empty()
                && (segments.len() + data.segments().len() + 1 > max_segments
                    || bytes + data.bytes() + 4 > max_bytes)
            {
                break;
            }
            cursor.position = entry.position + 1;
            bytes += data.bytes();
            segments.extend(data.to_segments());
            if bytes > 64 * 1024 {
                break;
            }
        }
        drop(memory);
        if segments.is_empty() {
            cursor.ended = true;
            if let Some(reader) = self.reader.lock().unwrap().take() {
                self.memory.lock().unwrap().unpin(reader);
            }
            return Ok(None);
        }
        segments.push((Stage::Log as i32).to_le_bytes().to_vec());
        OperationData::new(segments).map(Some)
    }
}
impl fube_data::IOperationDataStream for CopySender {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        let sender = self.weak.upgrade().unwrap();
        own(Box::pin(async move { sender.next(cancellation).await }))
    }
}
impl Drop for CopySender {
    fn drop(&mut self) {
        let _ = self.dispose();
    }
}

pub(crate) struct LazyCopy {
    weak: Weak<Self>,
    runtime: Weak<VolatileRuntime>,
    upto: i64,
    context: Mutex<Option<Arc<dyn fube_data::IOperationDataStream>>>,
    sender: tokio::sync::Mutex<Option<Arc<CopySender>>>,
}
impl LazyCopy {
    pub(crate) fn new(
        runtime: Weak<VolatileRuntime>,
        upto: i64,
        context: Option<Arc<dyn fube_data::IOperationDataStream>>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            weak: weak.clone(),
            runtime,
            upto,
            context: Mutex::new(context),
            sender: tokio::sync::Mutex::new(None),
        })
    }
}
impl fube_data::IOperationDataStream for LazyCopy {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        let owner = self.weak.upgrade().unwrap();
        own(Box::pin(async move {
            let mut sender = owner.sender.lock().await;
            if sender.is_none() {
                let runtime = owner
                    .runtime
                    .upgrade()
                    .ok_or_else(|| Status::ObjectClosed.error())?;
                #[cfg(test)]
                {
                    let pause = runtime.copy_selection_pause.lock().unwrap().take();
                    if let Some((entered, release)) = pause {
                        entered.complete(Ok(()));
                        release.wait().await.as_ref().clone()?;
                    }
                }
                runtime.owner.manager.wait_flushed(owner.upto).await?;
                let context = owner.context.lock().unwrap().take();
                let target = if let Some(context) = context {
                    context
                        .get_next(cancellation.clone())
                        .await?
                        .map(|data| CopyContext::decode(&data))
                        .transpose()?
                } else {
                    None
                };
                *sender = Some(CopySender::create(&runtime, owner.upto, target).await?);
            }
            sender.as_ref().unwrap().next(cancellation).await
        }))
    }
}
impl Drop for LazyCopy {
    fn drop(&mut self) {
        if let Ok(sender) = self.sender.try_lock() {
            if let Some(sender) = sender.as_ref() {
                let _ = sender.dispose();
            }
        }
    }
}
