use crate::{api::record_bytes, *};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};

enum Command {
    Execute(
        Request,
        Arc<supervisor::Shared>,
        oneshot::Sender<Result<Output>>,
    ),
    Close(oneshot::Sender<()>),
}

/// One fixed memory execution worker, so copying/scanning never starves supervision.
pub(crate) struct Worker {
    sender: mpsc::Sender<Command>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn new(settings: Settings) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel(1);
        let join = std::thread::Builder::new()
            .name("ra-memory-store".into())
            .spawn(move || {
                let mut memory = Memory::new(settings);
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        Command::Execute(request, shared, reply) => {
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    memory.execute(request, &shared)
                                }))
                                .unwrap_or_else(|_| {
                                    Err(Error::new(ErrorKind::Unknown, "memory execution panic"))
                                });
                            let _ = reply.send(result);
                        }
                        Command::Close(reply) => {
                            drop(memory);
                            let _ = reply.send(());
                            return;
                        }
                    }
                }
            })
            .map_err(|e| Error::new(ErrorKind::Io, e.to_string()))?;
        Ok(Self {
            sender,
            join: Some(join),
        })
    }

    pub(crate) async fn execute(
        &mut self,
        request: Request,
        shared: &Arc<supervisor::Shared>,
    ) -> Result<Output> {
        if matches!(request, Request::Maintain) {
            return Ok(Output::Written);
        }
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::Execute(request, shared.clone(), reply))
            .await
            .map_err(|_| Error::new(ErrorKind::Unknown, "memory worker lost"))?;
        result
            .await
            .map_err(|_| Error::new(ErrorKind::Unknown, "memory worker outcome lost"))?
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::Close(reply))
            .await
            .map_err(|_| Error::new(ErrorKind::Unknown, "memory worker lost before close"))?;
        result
            .await
            .map_err(|_| Error::new(ErrorKind::Unknown, "memory worker close lost"))?;
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| Error::new(ErrorKind::Unknown, "memory worker join failed"))?;
        }
        Ok(())
    }
}

pub(crate) struct Memory {
    rows: BTreeMap<RecordId, Vec<u8>>,
    bytes: usize,
    settings: Settings,
}

impl Memory {
    pub(crate) fn new(settings: Settings) -> Self {
        Self {
            rows: BTreeMap::new(),
            bytes: 0,
            settings,
        }
    }

    pub(crate) fn execute(
        &mut self,
        request: Request,
        shared: &Arc<supervisor::Shared>,
    ) -> Result<Output> {
        if let Request::Enumerate(t) = request {
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            for (key, value) in &self.rows {
                if key.record_type != t {
                    continue;
                }
                let record = Record {
                    key: key.clone(),
                    value: value.clone(),
                };
                bytes += record_bytes(&record);
                if bytes > self.settings.max_snapshot_bytes {
                    return Err(Error::new(ErrorKind::Capacity, "snapshot byte budget"));
                }
                rows.push(record);
            }
            return Ok(Output::Snapshot(rows));
        }
        let key = request.key().expect("mutation has key");
        let old = self.rows.get(key);
        match (&request, old) {
            (Request::Insert(_), Some(_)) => {
                return Err(Error::new(ErrorKind::Conflict, "record already exists"))
            }
            (Request::Update(_) | Request::Delete(_), None) => {
                return Err(Error::new(ErrorKind::NotFound, "record not found"))
            }
            _ => {}
        }
        let old_bytes = old.map_or(0, |v| {
            api::RECORD_OVERHEAD + key.record_type.len() + key.id.len() + v.len()
        });
        let new_bytes = match &request {
            Request::Insert(r) | Request::Update(r) => record_bytes(r),
            _ => 0,
        };
        let bytes = self.bytes - old_bytes + new_bytes;
        if bytes > self.settings.max_stored_bytes
            || (matches!(request, Request::Insert(_))
                && self.rows.len() >= self.settings.max_records)
        {
            return Err(Error::new(ErrorKind::Capacity, "stored record budget"));
        }
        // All fallible logical checks and the staged commit gate precede publication.
        #[cfg(test)]
        shared.hooks.before_blocking()?;
        #[cfg(not(test))]
        let _ = shared;
        match request {
            Request::Insert(r) | Request::Update(r) => {
                // Copy tightly: do not retain arbitrarily overallocated caller buffers.
                self.rows.insert(
                    RecordId::new(r.key.record_type.as_str(), r.key.id.as_str()),
                    r.value.to_vec(),
                );
            }
            Request::Delete(key) => {
                self.rows.remove(&key);
            }
            Request::Enumerate(_) | Request::Maintain => unreachable!(),
        }
        self.bytes = bytes;
        Ok(Output::Written)
    }
}
