//! Source-shaped volatile log, not persisted recovery/native disk qualification.
//! MemoryLog.cpp:13–399; LogManager.cpp:246–341; physical record writers at
//! Service Fabric 51a7703. Logical LSN, every-record PSN and byte positions differ.
use super::{
    identity,
    map::RecordNode,
    record::{Record, RecordKind},
    status::Status,
};
use crate::{Epoch, Error, Result};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const CHUNK: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgressEntry {
    pub(crate) epoch: Epoch,
    pub(crate) lsn: i64,
    pub(crate) primary: i64,
    pub(crate) ticks: i64,
}
pub(crate) fn zero_vector() -> Vec<ProgressEntry> {
    vec![ProgressEntry {
        epoch: Epoch {
            data_loss_number: 0,
            configuration_number: 0,
        },
        lsn: 0,
        primary: -1,
        ticks: 0,
    }]
}
pub(crate) fn vector_bytes(vector: &[ProgressEntry]) -> Vec<u8> {
    let mut bytes = (vector.len() as u32).to_le_bytes().to_vec();
    for entry in vector {
        for value in [
            entry.epoch.data_loss_number,
            entry.epoch.configuration_number,
            entry.lsn,
            entry.primary,
            entry.ticks,
        ] {
            bytes.extend(value.to_le_bytes());
        }
    }
    bytes
}
#[derive(Clone)]
pub(crate) struct Checkpoint {
    pub(crate) lsn: i64,
    pub(crate) starting_lsn: i64,
    pub(crate) starting_epoch: Epoch,
    pub(crate) vector: Vec<ProgressEntry>,
    pub(crate) begin_position: u64,
    pub(crate) reader_position: u64,
    pub(crate) stable: i64,
}
pub(crate) struct Retention {
    _bytes: OwnedSemaphorePermit,
}
pub(crate) struct Entry {
    pub(crate) position: u64,
    pub(crate) psn: i64,
    pub(crate) lsn: i64,
    pub(crate) native_type: i32,
    pub(crate) logical: Option<Arc<RecordNode>>,
    _storage: Option<Retention>,
}
pub(crate) struct MemoryLog {
    chunks: BTreeMap<u64, Box<[u8; CHUNK]>>,
    pub(crate) head: u64,
    pub(crate) tail_position: u64,
    pub(crate) tail_lsn: i64,
    pub(crate) flushed_lsn: i64,
    psn: i64,
    physical: Option<u64>,
    linked: Option<u64>,
    head_record: (u64, i64, i64, Epoch),
    indices: BTreeMap<u64, (i64, i64, Epoch)>,
    pub(crate) entries: VecDeque<Entry>,
    pub(crate) completed: Option<Checkpoint>,
    pub(crate) pending: Option<Checkpoint>,
    pending_ended: bool,
    pub(crate) vector: Vec<ProgressEntry>,
    allocation: Arc<Semaphore>,
    logging_error: Option<Error>,
    #[cfg(test)]
    pub(crate) write_error: Option<Error>,
    capacity: usize,
    readers: BTreeMap<u64, u64>,
    next_reader: u64,
    pub(crate) events: VecDeque<(i32, i64, u64)>,
}
impl MemoryLog {
    pub(crate) fn new(capacity: usize) -> Result<Self> {
        if !(8 * CHUNK..=16 * 1024 * 1024).contains(&capacity) {
            return Err(Error::InvalidInput(
                "volatile log capacity must be 512KiB..=16MiB",
            ));
        }
        let zero = Epoch {
            data_loss_number: 0,
            configuration_number: 0,
        };
        Ok(Self {
            chunks: BTreeMap::from([(0, Box::new([0; CHUNK]))]),
            head: 0,
            tail_position: 0,
            tail_lsn: 0,
            flushed_lsn: 0,
            psn: 0,
            physical: None,
            linked: None,
            head_record: (0, 0, 0, zero),
            indices: BTreeMap::new(),
            entries: VecDeque::new(),
            completed: None,
            pending: None,
            pending_ended: false,
            vector: zero_vector(),
            // Explicit uniform Rust allocation profile, not reserved native
            // admission or future End/Barrier/checkpoint progress capacity.
            allocation: Arc::new(Semaphore::new(capacity)),
            logging_error: None,
            #[cfg(test)]
            write_error: None,
            capacity,
            readers: BTreeMap::new(),
            next_reader: 1,
            events: VecDeque::new(),
        })
    }
    fn allocation_size(&self, record: &Record) -> Result<usize> {
        let wire = record.encode()?;
        let transaction_physical = if record.transaction_id().is_some() {
            12
        } else {
            0
        };
        Ok(wire.bytes() + wire.retained_bytes() + 28 + transaction_physical + 256)
    }
    fn allocation_permit(&self, record: &Record) -> Result<OwnedSemaphorePermit> {
        let bytes = self.allocation_size(record)?;
        if bytes > self.capacity {
            return Err(Status::OperationTooLarge.error());
        }
        self.allocation
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| Status::InsufficientResources.error())
    }
    pub(crate) fn reserve(&self, record: &Record) -> Result<Arc<Retention>> {
        Ok(Arc::new(Retention {
            _bytes: self.allocation_permit(record)?,
        }))
    }
    #[cfg(test)]
    pub(crate) fn allocated_bytes(&self) -> usize {
        self.capacity - self.allocation.available_permits()
    }
    #[cfg(test)]
    pub(crate) fn occupy_remaining_allocation(&self) -> OwnedSemaphorePermit {
        self.allocation
            .clone()
            .try_acquire_many_owned(self.allocation.available_permits() as u32)
            .unwrap()
    }
    pub(crate) fn fail_writer(&mut self, error: Error) {
        // PhysicalLogWriter.cpp:686–755 retains the FIRST loggingError_.
        // Only existing-owned write/flush failure enters this path, not
        // pre-record allocation rejection.
        self.logging_error.get_or_insert(error);
    }
    fn logical_sections(&self, node: &Arc<RecordNode>) -> Result<Vec<Vec<u8>>> {
        let record = node.record();
        let wire = record.encode()?;
        let mut sections = wire.to_segments();
        sections.insert(1, self.common_physical()?);
        if record.transaction_id().is_some() {
            let parent = node
                .parent()
                .map(|parent| {
                    self.entries
                        .iter()
                        .find(|entry| {
                            entry
                                .logical
                                .as_ref()
                                .is_some_and(|old| Arc::ptr_eq(old, &parent))
                        })
                        .map(|entry| entry.position)
                        .ok_or(Error::InvalidInput("physical parent is not retained"))
                })
                .transpose()?;
            sections.insert(
                4,
                section(
                    &parent
                        .map_or(0, |position| self.tail_position - position)
                        .to_le_bytes(),
                ),
            );
        }
        Ok(sections)
    }
    pub(crate) fn preflight_logical(&self, node: &Arc<RecordNode>) -> Result<()> {
        if let Some(error) = &self.logging_error {
            return Err(error.clone());
        }
        let size: usize = self.logical_sections(node)?.iter().map(Vec::len).sum();
        self.check_frame(size + 8)
    }
    fn check_frame(&self, size: usize) -> Result<()> {
        self.psn
            .checked_add(1)
            .ok_or(Error::ResourceLimit("physical sequence exhausted"))?;
        self.tail_position
            .checked_add(size as u64)
            .ok_or(Error::ResourceLimit("physical position exhausted"))?;
        if self.tail_position - self.head + size as u64 > self.capacity as u64 {
            return Err(Status::InsufficientResources.error());
        }
        Ok(())
    }
    pub(crate) fn append_logical(&mut self, node: Arc<RecordNode>) -> Result<()> {
        let record = node.record();
        let wire = record.encode()?;
        let native_type = i32::from_le_bytes(wire.segments()[0][4..8].try_into().unwrap());
        let sections = self.logical_sections(&node)?;
        let position = self.write(native_type, record.lsn, sections, false)?;
        self.tail_lsn = self.tail_lsn.max(record.lsn);
        self.entries.back_mut().unwrap().logical = Some(node);
        debug_assert_eq!(self.entries.back().unwrap().position, position);
        Ok(())
    }
    fn common_physical(&self) -> Result<Vec<u8>> {
        let next_psn = self
            .psn
            .checked_add(1)
            .ok_or(Error::ResourceLimit("physical sequence exhausted"))?;
        Ok(section(
            &[
                next_psn.to_le_bytes().to_vec(),
                (self.physical.map_or(0, |p| self.tail_position - p))
                    .to_le_bytes()
                    .to_vec(),
            ]
            .concat(),
        ))
    }
    fn physical_record(&mut self, native_type: i32, lsn: i64, fields: Vec<Vec<u8>>) -> Result<u64> {
        let mut sections = vec![
            section(
                &[
                    native_type.to_le_bytes().to_vec(),
                    lsn.to_le_bytes().to_vec(),
                ]
                .concat(),
            ),
            self.common_physical()?,
            section(
                &self
                    .linked
                    .map_or(0, |p| self.tail_position - p)
                    .to_le_bytes(),
            ),
        ];
        sections.extend(fields);
        self.write(native_type, lsn, sections, true)
    }
    fn write(
        &mut self,
        native_type: i32,
        lsn: i64,
        sections: Vec<Vec<u8>>,
        physical: bool,
    ) -> Result<u64> {
        if let Some(error) = &self.logging_error {
            return Err(error.clone());
        }
        #[cfg(test)]
        if let Some(error) = self.write_error.take() {
            return Err(error);
        }
        let body: Vec<_> = sections.into_iter().flatten().collect();
        let size = u32::try_from(body.len())
            .map_err(|_| Error::ResourceLimit("physical frame too large"))?;
        let frame = [
            size.to_le_bytes().as_slice(),
            body.as_slice(),
            size.to_le_bytes().as_slice(),
        ]
        .concat();
        self.check_frame(frame.len())?;
        let storage = if physical {
            Some(Retention {
                _bytes: self
                    .allocation
                    .clone()
                    .try_acquire_many_owned((frame.len() + 256) as u32)
                    .map_err(|_| Status::InsufficientResources.error())?,
            })
        } else {
            None
        };
        let position = self.tail_position;
        let mut remaining = frame.as_slice();
        while !remaining.is_empty() {
            let chunk = self.tail_position / CHUNK as u64;
            let offset = self.tail_position as usize % CHUNK;
            let count = remaining.len().min(CHUNK - offset);
            self.chunks
                .entry(chunk)
                .or_insert_with(|| Box::new([0; CHUNK]))[offset..offset + count]
                .copy_from_slice(&remaining[..count]);
            self.tail_position += count as u64;
            remaining = &remaining[count..];
        }
        self.psn += 1;
        if physical {
            self.physical = Some(position);
        }
        self.entries.push_back(Entry {
            position,
            psn: self.psn,
            lsn,
            native_type,
            logical: None,
            _storage: storage,
        });
        if self.events.len() == 1024 {
            self.events.pop_front();
        }
        self.events.push_back((native_type, lsn, position));
        Ok(position)
    }
    pub(crate) fn indexing(&mut self, lsn: i64, epoch: Epoch) -> Result<u64> {
        let position = self.physical_record(
            9,
            lsn,
            vec![section(
                &[
                    epoch.data_loss_number.to_le_bytes(),
                    epoch.configuration_number.to_le_bytes(),
                ]
                .concat(),
            )],
        )?;
        self.indices.insert(position, (self.psn, lsn, epoch));
        if position == 0 {
            self.head_record = (position, self.psn, lsn, epoch);
        }
        Ok(position)
    }
    pub(crate) fn begin_checkpoint(
        &mut self,
        lsn: i64,
        earliest: Option<&Arc<RecordNode>>,
    ) -> Result<Checkpoint> {
        let position = self.tail_position;
        if self.pending.is_some()
            || self.vector.is_empty()
            || self.vector.len() > 128
            || lsn != self.tail_lsn
        {
            return Err(Error::InvalidInput("checkpoint/vector capacity"));
        }
        // Native receives a typed earliest-pending Begin selected within the
        // checkpoint frontier (TransactionMap.cpp:392–415). Invalid Some is not
        // the absent-record sentinel, and must not manufacture a reclaim anchor.
        let reader_position = if let Some(node) = earliest {
            if !matches!(node.record().kind, RecordKind::Begin { single: false, .. })
                || node.lsn() <= 0
                || node.lsn() > lsn
            {
                return Err(Error::InvalidInput("invalid earliest checkpoint Begin"));
            }
            self.entries
                .iter()
                .find(|entry| {
                    entry.lsn == node.lsn()
                        && entry.position < position
                        && entry
                            .logical
                            .as_ref()
                            .is_some_and(|retained| Arc::ptr_eq(retained, node))
                })
                .map(|entry| entry.position)
                .ok_or(Error::InvalidInput(
                    "earliest checkpoint Begin is not retained",
                ))?
        } else {
            position
        };
        let epoch = earliest.map_or_else(|| self.vector.last().unwrap().epoch, |n| n.epoch());
        let mut fields = vector_bytes(&self.vector);
        fields.extend((position - reader_position).to_le_bytes());
        fields.extend(epoch.data_loss_number.to_le_bytes());
        fields.extend(epoch.configuration_number.to_le_bytes());
        fields.extend([0; 16]); // required no-backup GUID scalar, not backup support
        fields.extend([0; 24]); // zero backup epoch and LSN
        fields.extend([0; 8]); // u32 count and u32 size
        fields.extend(identity::native_system_time()?.to_le_bytes());
        fields.extend(identity::native_system_time()?.to_le_bytes());
        self.physical_record(7, lsn, vec![section(&fields)])?;
        let cp = Checkpoint {
            lsn,
            starting_lsn: earliest.map_or(lsn, |n| n.lsn() - 1),
            starting_epoch: epoch,
            vector: self.vector.clone(),
            begin_position: position,
            reader_position,
            stable: lsn,
        };
        self.pending = Some(cp.clone());
        self.pending_ended = false;
        Ok(cp)
    }
    fn head_fields(&self) -> Vec<u8> {
        let (position, psn, lsn, epoch) = self.head_record;
        section(
            &[
                epoch.data_loss_number.to_le_bytes().to_vec(),
                epoch.configuration_number.to_le_bytes().to_vec(),
                lsn.to_le_bytes().to_vec(),
                psn.to_le_bytes().to_vec(),
                (self.tail_position - position).to_le_bytes().to_vec(),
            ]
            .concat(),
        )
    }
    pub(crate) fn end_checkpoint(&mut self) -> Result<()> {
        if self.pending_ended {
            return Err(Error::InvalidInput("checkpoint already ended"));
        }
        let cp = self
            .pending
            .clone()
            .ok_or(Error::InvalidInput("end lacks begin checkpoint"))?;
        let fields = section(
            &[
                cp.stable.to_le_bytes().to_vec(),
                (self.tail_position - cp.begin_position)
                    .to_le_bytes()
                    .to_vec(),
            ]
            .concat(),
        );
        let position = self.physical_record(8, self.tail_lsn, vec![self.head_fields(), fields])?;
        self.linked = Some(position);
        self.pending_ended = true;
        Ok(())
    }
    pub(crate) fn complete_checkpoint(&mut self) -> Result<()> {
        if self.pending.is_none() || !self.pending_ended {
            return Err(Error::InvalidInput("complete lacks ended checkpoint"));
        }
        let position =
            self.physical_record(13, self.tail_lsn, vec![self.head_fields(), section(&[])])?;
        // ReplicatedLogManager.cpp:263–275 advances the linked physical identity
        // to CompleteCheckpoint; following Information/Indexing must use it.
        self.linked = Some(position);
        self.completed = self.pending.take();
        self.pending_ended = false;
        Ok(())
    }
    pub(crate) fn information(&mut self, event: i32) -> Result<()> {
        self.physical_record(12, self.tail_lsn, vec![section(&event.to_le_bytes())])?;
        Ok(())
    }
    pub(crate) fn flush(&mut self) {
        if self.logging_error.is_none() {
            self.flushed_lsn = self.tail_lsn;
        }
    }
    pub(crate) fn read(&self, position: u64, count: usize) -> Vec<u8> {
        if position < self.head || position >= self.tail_position {
            return Vec::new();
        }
        (position
            ..position
                .saturating_add(count as u64)
                .min(self.tail_position))
            .map(|p| self.chunks[&(p / CHUNK as u64)][p as usize % CHUNK])
            .collect()
    }
    pub(crate) fn pin(&mut self, position: u64) -> Result<u64> {
        if position < self.head || position > self.tail_position {
            return Err(Error::InvalidInput("copy reader outside retained log"));
        }
        let id = self.next_reader;
        self.next_reader = id
            .checked_add(1)
            .ok_or(Error::ResourceLimit("reader IDs"))?;
        if self.readers.len() >= 16 {
            return Err(Status::InsufficientResources.error());
        }
        self.readers.insert(id, position);
        Ok(id)
    }
    pub(crate) fn unpin(&mut self, id: u64) {
        self.readers.remove(&id);
    }
    pub(crate) fn reclaim(&mut self, position: u64) -> Result<bool> {
        let &(psn, lsn, epoch) = self.indices.get(&position).ok_or(Error::InvalidInput(
            "head must be a retained Indexing record",
        ))?;
        let pinned = self.readers.values().min().copied().unwrap_or(position);
        let anchor = self
            .completed
            .as_ref()
            .map_or(self.head, |cp| cp.reader_position);
        if position > pinned || position >= anchor || position <= self.head {
            return Ok(false);
        }
        let fields = section(
            &[
                vec![1],
                identity::native_system_time()?.to_le_bytes().to_vec(),
            ]
            .concat(),
        );
        let old_head = self.head_record;
        self.head_record = (position, psn, lsn, epoch);
        let linked = match self.physical_record(10, self.tail_lsn, vec![self.head_fields(), fields])
        {
            Ok(linked) => linked,
            Err(error) => {
                self.head_record = old_head;
                return Err(error);
            }
        };
        self.linked = Some(linked);
        self.flush();
        self.head = position;
        self.chunks
            .retain(|chunk, _| (chunk + 1) * CHUNK as u64 > position);
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.position < position)
        {
            self.entries.pop_front();
        }
        self.indices.retain(|index, _| *index >= position);
        Ok(true)
    }
    pub(crate) fn bootstrap(&mut self) -> Result<Vec<Record>> {
        if self.tail_position != 0 {
            return Err(Error::InvalidInput("bootstrap requires empty memory log"));
        }
        let zero = Epoch {
            data_loss_number: 0,
            configuration_number: 0,
        };
        self.indexing(0, zero)?;
        let epoch = Record {
            lsn: 0,
            kind: RecordKind::UpdateEpoch {
                epoch: zero,
                primary_replica_id: 0,
                timestamp_ticks: 0,
            },
        };
        self.append_logical(RecordNode::new(epoch.clone(), None, None)?)?;
        self.begin_checkpoint(0, None)?;
        let barrier = Record {
            lsn: 1,
            kind: RecordKind::Barrier { last_stable_lsn: 0 },
        };
        self.append_logical(RecordNode::new(barrier.clone(), None, None)?)?;
        self.end_checkpoint()?;
        self.complete_checkpoint()?;
        self.flush();
        self.information(1)?; // native fresh-image consumption: Recovered
        self.flush();
        Ok(vec![epoch, barrier])
    }
}
fn section(fields: &[u8]) -> Vec<u8> {
    [(fields.len() as u32 + 4).to_le_bytes().as_slice(), fields].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_bootstrap_physical_bytes_psn_offsets_and_frontiers() {
        let mut log = MemoryLog::new(1024 * 1024).unwrap();
        log.bootstrap().unwrap();
        assert_eq!(
            log.events.iter().map(|e| (e.0, e.1)).collect::<Vec<_>>(),
            vec![(9, 0), (5, 0), (7, 0), (4, 1), (8, 1), (13, 1), (12, 1)]
        );
        assert_eq!(
            (log.tail_lsn, log.flushed_lsn, log.psn, log.tail_position),
            (1, 1, 7, 700)
        );
        for entry in &log.entries {
            let bytes = log.read(entry.position, 4);
            let size = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
            let suffix = log.read(entry.position + 4 + size as u64, 4);
            assert_eq!(
                u32::from_le_bytes(suffix.try_into().unwrap()) as usize,
                size
            );
        }
        assert_eq!(log.completed.as_ref().unwrap().stable, 0);
        assert_eq!(log.vector, zero_vector());
    }
}
