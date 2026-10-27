// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Source-guided: Reliability/Replication/OperationQueue.{h,cpp} and
// ReplicationQueueManager.cpp at 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.

//! Bounded ordered ownership with separate receive, commit, completion and
//! retention frontiers. Native role-specific policy belongs above this queue.

use crate::{Epoch, Error, OperationData, Progress, Result, Settings};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Entry {
    pub sequence: i64,
    pub epoch: Epoch,
    pub data: OperationData,
    pub acknowledged: bool,
}

pub struct OperationQueue {
    entries: BTreeMap<i64, Entry>,
    settings: Settings,
    bytes: usize,
    assigned: i64,
    received: i64,
    applied: i64,
    committed: i64,
    completed: i64,
    client_completed: i64,
    released: i64,
}
impl OperationQueue {
    pub fn new(settings: Settings, initial: i64) -> Result<Self> {
        settings.validate()?;
        if initial < 0 || initial == i64::MAX {
            return Err(Error::InvalidInput("invalid initial LSN"));
        }
        Ok(Self {
            settings,
            entries: BTreeMap::new(),
            bytes: 0,
            assigned: initial,
            received: initial,
            applied: initial,
            committed: initial,
            completed: initial,
            client_completed: initial,
            released: initial,
        })
    }
    pub fn progress(&self) -> Progress {
        Progress {
            assigned: self.assigned,
            received: self.received,
            applied: self.applied,
            committed: self.committed,
            client_completed: self.client_completed,
            completed: self.completed,
            first_retained: self
                .entries
                .first_key_value()
                .map_or(self.released.saturating_add(1), |(&sequence, _)| sequence),
            retained_operations: self.entries.len(),
            retained_bytes: self.bytes,
        }
    }
    pub fn get(&self, sequence: i64) -> Option<&Entry> {
        self.entries.get(&sequence)
    }
    pub fn range(&self, from: i64) -> impl Iterator<Item = &Entry> {
        self.entries.range(from..).map(|(_, entry)| entry)
    }
    fn admit(&self, data: &OperationData) -> Result<()> {
        if data.bytes() > self.settings.operation_bytes
            || data.segments().len() > self.settings.operation_segments
            || self.entries.len() >= self.settings.queue_operations
            || data.retained_bytes() > self.settings.queue_bytes.saturating_sub(self.bytes)
        {
            return Err(Error::ResourceLimit("replication queue"));
        }
        Ok(())
    }
    /// Sequence assignment occurs only AFTER resource admission.
    pub fn enqueue(&mut self, epoch: Epoch, data: OperationData) -> Result<i64> {
        self.admit(&data)?;
        let sequence = self
            .assigned
            .checked_add(1)
            .filter(|s| *s > 0 && *s < i64::MAX)
            .ok_or(Error::InvalidInput("LSN exhausted"))?;
        self.insert(Entry {
            sequence,
            epoch,
            data,
            acknowledged: false,
        });
        self.assigned = sequence;
        self.received = sequence;
        Ok(sequence)
    }
    /// A duplicate must have exactly the original epoch/data. The receive
    /// frontier advances only through a contiguous prefix.
    pub fn receive(&mut self, sequence: i64, epoch: Epoch, data: OperationData) -> Result<bool> {
        if sequence <= 0 || sequence == i64::MAX {
            return Err(Error::Protocol("invalid operation LSN"));
        }
        if sequence <= self.released {
            return Ok(false);
        }
        if let Some(old) = self.entries.get(&sequence) {
            if old.epoch != epoch || old.data != data {
                return Err(Error::Protocol("conflicting duplicate operation"));
            }
            return Ok(false);
        }
        if sequence - self.received > self.settings.queue_operations as i64 {
            return Err(Error::ResourceLimit("receive gap"));
        }
        self.admit(&data)?;
        self.insert(Entry {
            sequence,
            epoch,
            data,
            acknowledged: false,
        });
        self.assigned = self.assigned.max(sequence);
        while self.entries.contains_key(&(self.received + 1)) {
            self.received += 1;
        }
        Ok(true)
    }
    fn insert(&mut self, entry: Entry) {
        self.bytes += entry.data.retained_bytes();
        self.entries.insert(entry.sequence, entry);
    }
    /// Out-of-order explicit ACKs cannot bridge an unacknowledged gap.
    pub fn acknowledge(&mut self, sequence: i64) -> Result<i64> {
        if sequence > self.received {
            return Err(Error::Protocol("ACK beyond received prefix"));
        }
        let entry = self.entries.get_mut(&sequence).ok_or(Error::Closed)?;
        if entry.acknowledged {
            return Err(Error::InvalidInput("duplicate queue ACK"));
        }
        entry.acknowledged = true;
        while self
            .entries
            .get(&(self.applied + 1))
            .is_some_and(|entry| entry.acknowledged)
        {
            self.applied += 1;
        }
        Ok(self.applied)
    }
    /// Configuration changes may retreat the commit head, but cannot undo
    /// completed/released history or repeat successful callbacks.
    pub fn commit(&mut self, sequence: i64) -> Result<std::ops::RangeInclusive<i64>> {
        if sequence < self.completed || sequence > self.received {
            return Err(Error::Protocol("commit outside queue frontiers"));
        }
        self.committed = sequence;
        let first = self.client_completed + 1;
        self.client_completed = self.client_completed.max(sequence);
        Ok(first..=sequence)
    }
    /// Primary release policy uses commitment, relevant receive ACKs and idle
    /// build fences; it is not every replica's application progress.
    pub fn complete(&mut self, sequence: i64) -> Result<()> {
        if sequence < self.completed || sequence > self.committed {
            return Err(Error::Protocol("completion outside commit boundary"));
        }
        self.completed = sequence;
        Ok(())
    }
    /// Keep a suffix even after completion. Retransmission references elsewhere
    /// are independent owners and must remain budgeted by their session.
    pub fn release_through(&mut self, sequence: i64) -> Result<()> {
        if sequence < self.released || sequence > self.completed {
            return Err(Error::Protocol("release outside completion boundary"));
        }
        let tail = self.entries.split_off(&(sequence + 1));
        self.entries = tail;
        self.bytes = self.entries.values().map(|e| e.data.retained_bytes()).sum();
        self.released = sequence;
        Ok(())
    }
    /// Remove a non-authoritative tail after the provider has reconciled.
    /// This does not itself mutate provider state.
    pub fn truncate_after(&mut self, sequence: i64) -> Result<()> {
        if sequence < self.released || sequence > self.received {
            return Err(Error::Protocol("truncate outside retained prefix"));
        }
        self.entries.split_off(&(sequence + 1));
        self.bytes = self.entries.values().map(|e| e.data.retained_bytes()).sum();
        self.assigned = sequence;
        self.received = sequence;
        self.applied = self.applied.min(sequence);
        self.committed = self.committed.min(sequence);
        self.completed = self.completed.min(sequence);
        // Successful callbacks are lifetime notifications, not a rewound queue head.
        Ok(())
    }
    pub fn reset(&mut self, authoritative_progress: i64) -> Result<()> {
        let replacement = Self::new(self.settings.clone(), authoritative_progress)?;
        *self = replacement;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const E: Epoch = Epoch {
        data_loss: 1,
        configuration: 0x100000000,
    };
    fn data(n: u8) -> OperationData {
        OperationData::new(vec![vec![], vec![n]]).unwrap()
    }
    #[test]
    fn gaps_and_out_of_order_acknowledgment_have_distinct_frontiers() {
        let mut q = OperationQueue::new(Settings::default(), 0).unwrap();
        q.receive(2, E, data(2)).unwrap();
        assert_eq!(q.progress().received, 0);
        assert!(q.acknowledge(2).is_err());
        q.receive(1, E, data(1)).unwrap();
        assert_eq!(q.progress().received, 2);
        q.acknowledge(2).unwrap();
        assert_eq!(q.progress().applied, 0);
        q.acknowledge(1).unwrap();
        assert_eq!(q.progress().applied, 2);
        assert_eq!(q.progress().committed, 0);
    }
    #[test]
    fn full_rejection_does_not_consume_sequence() {
        let s = Settings {
            queue_operations: 1,
            ..Settings::default()
        };
        let mut q = OperationQueue::new(s, 0).unwrap();
        assert_eq!(q.enqueue(E, data(1)).unwrap(), 1);
        assert!(q.enqueue(E, data(2)).is_err());
        q.commit(1).unwrap();
        q.complete(1).unwrap();
        q.release_through(1).unwrap();
        assert_eq!(q.enqueue(E, data(2)).unwrap(), 2);
    }
    #[test]
    fn rollback_does_not_repeat_callbacks_or_cross_completed() {
        let mut q = OperationQueue::new(Settings::default(), 0).unwrap();
        for n in 1..=4 {
            q.enqueue(E, data(n)).unwrap();
        }
        assert_eq!(q.commit(4).unwrap().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        q.complete(2).unwrap();
        q.release_through(1).unwrap();
        assert!(q.commit(1).is_err());
        assert!(q.commit(2).unwrap().is_empty());
        assert!(q.commit(4).unwrap().is_empty());
        assert_eq!(q.progress().retained_operations, 3);
        assert_eq!(q.progress().applied, 0);
    }
    #[test]
    fn duplicate_conflict_bounds_reset_and_tail_reconciliation() {
        let mut q = OperationQueue::new(Settings::default(), 0).unwrap();
        assert!(q.receive(1, E, data(1)).unwrap());
        assert!(!q.receive(1, E, data(1)).unwrap());
        assert!(q.receive(1, E, data(2)).is_err());
        assert!(q.receive(1026, E, data(1)).is_err());
        q.receive(2, E, data(2)).unwrap();
        q.truncate_after(1).unwrap();
        assert_eq!(q.enqueue(E, data(3)).unwrap(), 2);
        q.reset(11).unwrap();
        assert_eq!(q.enqueue(E, data(4)).unwrap(), 12);
        assert!(q.receive(0, E, data(0)).is_err());
        assert!(q.reset(i64::MAX).is_err());
    }
}
