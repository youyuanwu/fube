// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! SF 51a7703 NamedOperationDataStream.cpp:47–142 and
//! SecondaryDrainManager.cpp:974–1100. Native header-first routing and trailing
//! state/progress stages are reused; negotiation and full log/ACK pumps are not.

use super::{named, status::Status, transaction::own};
use crate::{
    internal::IStateProviderManager, ApiFuture, Cancellation, Error, IOperationDataStream,
    OperationData, Result, StateProviderId,
};
use std::sync::{Arc, Mutex, Weak};

struct StreamCursor {
    index: usize,
    disposed: bool,
    streams: Vec<(StateProviderId, Arc<dyn IOperationDataStream>)>,
}

pub(crate) struct NamedStateStream {
    self_weak: Weak<Self>,
    cursor: Mutex<StreamCursor>,
}

impl NamedStateStream {
    pub(crate) fn new(
        registry: Arc<dyn IOperationDataStream>,
        providers: Vec<(StateProviderId, Arc<dyn IOperationDataStream>)>,
    ) -> Arc<Self> {
        let mut streams = vec![(super::provider::REGISTRY_ID, registry)];
        streams.extend(providers);
        Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            cursor: Mutex::new(StreamCursor {
                index: 0,
                disposed: false,
                streams,
            }),
        })
    }
}

impl IOperationDataStream for NamedStateStream {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        let named_stream = self.self_weak.upgrade().expect("shared named stream");
        own(Box::pin(async move {
            loop {
                let (id, stream) = {
                    let cursor = named_stream.cursor.lock().expect("named stream poisoned");
                    if cursor.disposed {
                        return Err(Status::ObjectClosed.error());
                    }
                    let Some((id, stream)) = cursor.streams.get(cursor.index) else {
                        // Source initializes STATUS_UNSUCCESSFUL if no child is polled.
                        return Err(Error::Remote {
                            code: 0xc0000001u32 as i32 as i64,
                            message: "native named stream has no remaining child".into(),
                        });
                    };
                    (*id, stream.clone())
                };
                match stream.get_next(cancellation.clone()).await? {
                    Some(data) => return named::encode_copy(id, &data).map(Some),
                    None => {
                        let mut cursor = named_stream.cursor.lock().expect("named stream poisoned");
                        cursor.index += 1;
                        if cursor.index == cursor.streams.len() {
                            return Ok(None);
                        }
                    }
                }
            }
        }))
    }
    fn dispose(&self) -> Result<()> {
        let mut cursor = self.cursor.lock().expect("named stream poisoned");
        if cursor.disposed {
            return Ok(());
        }
        let mut first_error = None;
        for (_, stream) in &cursor.streams {
            if let Err(error) = stream.dispose() {
                first_error.get_or_insert(error);
            }
        }
        cursor.streams.clear();
        cursor.disposed = true;
        first_error.map_or(Ok(()), Err)
    }
}

/// A received native operation owns its data and ACK identity. Returning its Arc
/// preserves the exact progress transition for the subsequent log-copy path.
pub(crate) trait CopyOperation: Send + Sync + 'static {
    fn data(&self) -> &OperationData;
    fn acknowledge(&self);
}

pub(crate) trait CopyStageStream: Send + Sync {
    fn get_operation(&self) -> ApiFuture<'_, Option<Arc<dyn CopyOperation>>>;
}

/// Retain the entire finite state-stage activity, including post-Set ACK and
/// subsequent polling/End, independently of its completion observer.
pub(crate) fn drain_state(
    manager: Arc<dyn IStateProviderManager>,
    stream: Arc<dyn CopyStageStream + 'static>,
) -> ApiFuture<'static, Option<Arc<dyn CopyOperation>>> {
    own(Box::pin(async move {
        let Some(mut operation) = stream.get_operation().await? else {
            return Ok(None);
        };
        manager.begin_setting_current_state().await?;
        let mut number = 0i64;
        loop {
            let data = operation.data();
            let stage = data
                .segments()
                .last()
                .ok_or(Error::InvalidInput("missing native copy stage"))?;
            if stage.len() < 4 {
                return Err(Error::InvalidInput("truncated native copy stage"));
            }
            match u32::from_le_bytes(stage[..4].try_into().expect("4 bytes")) {
                3 => {
                    let payload = OperationData::new(
                        data.segments()[..data.segments().len() - 1]
                            .iter()
                            .map(|segment| segment.to_vec())
                            .collect(),
                    )?;
                    manager.set_current_state(number, payload).await?;
                    operation.acknowledge();
                    number = number
                        .checked_add(1)
                        .ok_or(Error::InvalidInput("copy ordinal exhausted"))?;
                    operation = stream
                        .get_operation()
                        .await?
                        .ok_or_else(|| Status::Cancelled.error())?;
                }
                4 => {
                    manager.end_setting_current_state().await?;
                    return Ok(Some(operation));
                }
                _ => return Err(Status::NotImplemented.error()),
            }
        }
    }))
}
