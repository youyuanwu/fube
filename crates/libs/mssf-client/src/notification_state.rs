use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::{
    notification_protocol::{Interval, Page},
    resolution::Generation,
    Error, NotificationError, NotificationResult, ServiceNotification,
};

pub(crate) const MAX_EVENTS: usize = 256;
pub(crate) const MAX_MEMORY: usize = 8 * 1024 * 1024;
pub(crate) const MAX_IDENTITIES: usize = 4096;
pub(crate) const MAX_INTERVALS: usize = 4096;
pub(crate) const MAX_PAGES: usize = 32;
pub(crate) const MAX_FILTERS: usize = 32;
pub(crate) const MAX_CLEANUPS: usize = 32;

#[cfg(test)]
#[path = "notification_state_tests.rs"]
mod tests;

#[derive(Default)]
pub(crate) struct Budget(Mutex<usize>);
pub(crate) struct Memory {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Budget {
    fn charge(&self, bytes: usize) -> NotificationResult<()> {
        let mut used = self.0.lock().unwrap();
        if bytes > MAX_MEMORY.saturating_sub(*used) {
            return Err(NotificationError::Lagged);
        }
        *used += bytes;
        Ok(())
    }
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> NotificationResult<Memory> {
        self.charge(bytes)?;
        Ok(Memory {
            budget: self.clone(),
            bytes,
        })
    }
}
impl Memory {
    pub fn grow(&mut self, bytes: usize) -> NotificationResult<()> {
        self.budget.charge(bytes)?;
        self.bytes += bytes;
        Ok(())
    }
    fn split(&mut self, bytes: usize) -> NotificationResult<Self> {
        if bytes > self.bytes {
            return Err(NotificationError::ResourceLimit(
                "decoded notification accounting",
            ));
        }
        self.bytes -= bytes;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
        })
    }
}
impl Drop for Memory {
    fn drop(&mut self) {
        *self.budget.0.lock().unwrap() -= self.bytes;
    }
}

#[derive(Default)]
struct Delivery {
    queue: VecDeque<(ServiceNotification, Memory)>,
    terminal: Option<NotificationError>,
    emitted: bool,
    closed: bool,
}
pub(crate) struct Receiver {
    inner: Mutex<Delivery>,
    changed: Notify,
    pub budget: Arc<Budget>,
    _queue_memory: Memory,
}
impl Default for Receiver {
    fn default() -> Self {
        let budget = Arc::new(Budget::default());
        let queue = VecDeque::with_capacity(MAX_EVENTS);
        let storage = queue.capacity() * size_of::<(ServiceNotification, Memory)>();
        let memory = budget
            .reserve(storage)
            .expect("fixed queue fits memory profile");
        Self {
            inner: Mutex::new(Delivery {
                queue,
                ..Default::default()
            }),
            changed: Notify::new(),
            budget,
            _queue_memory: memory,
        }
    }
}
impl Receiver {
    pub fn fail(&self, error: NotificationError) {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return;
        }
        inner.terminal = Some(error);
        inner.closed = true;
        inner.queue.clear();
        drop(inner);
        self.changed.notify_one();
    }
    pub fn finish(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        inner.queue.clear();
        drop(inner);
        self.changed.notify_one();
    }
    pub fn error(&self) -> Option<NotificationError> {
        self.inner.lock().unwrap().terminal.clone()
    }
    pub fn common_error(&self, error: Error) -> NotificationError {
        self.error().unwrap_or(NotificationError::Common(error))
    }
    pub async fn recv(&self) -> Option<NotificationResult<ServiceNotification>> {
        loop {
            let changed = self.changed.notified();
            {
                let mut inner = self.inner.lock().unwrap();
                if inner.terminal.is_some() && !inner.emitted {
                    inner.emitted = true;
                    return Some(Err(inner.terminal.clone().unwrap()));
                }
                if inner.closed {
                    return None;
                }
                if let Some((event, _memory)) = inner.queue.pop_front() {
                    return Some(Ok(event));
                }
            }
            changed.await;
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Progress {
    generation: Generation,
    ranges: Vec<Interval>,
    partitions: HashMap<Uuid, i64>,
}
impl Progress {
    pub fn new(generation: Generation) -> Self {
        Self {
            generation,
            ..Default::default()
        }
    }
    /// No delivery or progress becomes visible until the entire page fits.
    pub fn commit(
        &mut self,
        page: Page,
        mut memory: Memory,
        receiver: &Receiver,
    ) -> NotificationResult<()> {
        if page.generation < self.generation {
            return Ok(());
        }
        // Cloning is not the only peak: a one-entry insertion can rehash the
        // whole prospective map, and range merge/reallocation overlaps buffers.
        // Retained protocol state is separately bounded; all transient copies
        // must fit delivery accounting before constructing prospective state.
        let copy_bytes = if page.generation == self.generation {
            4 * (self.partitions.capacity() * (size_of::<(Uuid, i64)>() + 16)
                + self.ranges.capacity() * size_of::<Interval>())
        } else {
            0
        };
        let _scratch = receiver.budget.reserve(copy_bytes)?;
        let mut next = if page.generation > self.generation {
            Self {
                generation: page.generation,
                ..Default::default()
            }
        } else {
            self.clone()
        };
        let mut updates = Vec::new();
        for slot in page.entries.0 {
            let entry = slot.0.ok_or(Error::Protocol("null notification entry"))?;
            let table = entry
                .entry
                .0
                .ok_or(Error::Protocol("null notification partition"))?;
            // Decoding is already charged. Reserve the additional DTO/name/
            // endpoint copies and growing update/admission containers before
            // constructing them; no wire schema is walked a second time.
            let endpoints =
                table.replicas.replicas.0.len() + usize::from(table.replicas.primary_valid);
            let conversion_bytes = 8
                * (size_of::<ServiceNotification>() + size_of::<(ServiceNotification, Memory)>())
                + table.name.len() * 4
                + endpoints * 4 * size_of::<crate::ServiceEndpoint>()
                + table.replicas.primary.len()
                + table
                    .replicas
                    .replicas
                    .0
                    .iter()
                    .map(String::len)
                    .sum::<usize>();
            memory.grow(conversion_bytes)?;
            let version = table.identity(page.generation)?;
            let lookup = version.lookup;
            if next
                .ranges
                .iter()
                .any(|r| r.start <= lookup && lookup < r.end)
                || next
                    .partitions
                    .get(&version.partition)
                    .is_some_and(|old| *old >= lookup)
            {
                continue;
            }
            if !next.partitions.contains_key(&version.partition)
                && next.partitions.len() == MAX_IDENTITIES
            {
                return Err(NotificationError::ResourceLimit(
                    "notification partition identities",
                ));
            }
            next.partitions.insert(version.partition, lookup);
            let (kind, endpoints) = table.endpoints();
            updates.push(ServiceNotification {
                service_name: version.name.clone(),
                partition_id: version.partition,
                kind,
                endpoints,
                version,
            });
        }
        if let Some(versions) = page.versions.0 {
            next.ranges.extend(versions.ranges.0);
        }
        if next.ranges.iter().any(|r| r.start < 0 || r.start >= r.end) {
            return Err(Error::Protocol("invalid notification version range").into());
        }
        next.ranges.sort_unstable_by_key(|r| r.start);
        let mut merged: Vec<Interval> = Vec::new();
        for range in next.ranges {
            if let Some(last) = merged.last_mut().filter(|last| range.start <= last.end) {
                last.end = last.end.max(range.end);
            } else {
                merged.push(range);
            }
        }
        if merged.len() > MAX_INTERVALS {
            return Err(NotificationError::ResourceLimit(
                "notification version intervals",
            ));
        }
        next.ranges = merged;
        let mut inner = receiver.inner.lock().unwrap();
        if inner.closed {
            return Err(inner
                .terminal
                .clone()
                .unwrap_or(NotificationError::Common(Error::Closed)));
        }
        if updates.len() > MAX_EVENTS.saturating_sub(inner.queue.len()) {
            return Err(NotificationError::Lagged);
        }
        let mut admitted = Vec::with_capacity(updates.len());
        for event in updates {
            // Endpoint vectors and both owned service identities survive page disposal.
            let bytes = size_of::<ServiceNotification>()
                + 128
                + event.service_name.as_str().len() * 4
                + event.endpoints.capacity() * size_of::<crate::ServiceEndpoint>()
                + event
                    .endpoints
                    .iter()
                    .map(|e| e.address.capacity())
                    .sum::<usize>();
            admitted.push((event, memory.split(bytes)?));
        }
        // The only publication point: queue entries and progress advance together.
        inner.queue.extend(admitted);
        *self = next;
        drop(inner);
        receiver.changed.notify_one();
        Ok(())
    }
}
