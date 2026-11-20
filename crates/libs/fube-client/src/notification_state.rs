use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::{
    notification_protocol::{Interval, Page},
    resolution::Generation,
    FurpcClientError, NotificationError, NotificationResult, ServiceNotification,
};

pub(crate) const MAX_EVENTS: usize = 256;
pub(crate) const MAX_IDENTITIES: usize = 4096;
pub(crate) const MAX_INTERVALS: usize = 4096;
pub(crate) const MAX_FILTERS: usize = 32;
pub(crate) const MAX_CLEANUPS: usize = 32;

#[cfg(test)]
#[path = "notification_state_tests.rs"]
mod tests;

#[derive(Default)]
struct Delivery {
    queue: VecDeque<ServiceNotification>,
    terminal: Option<NotificationError>,
    emitted: bool,
    closed: bool,
}
pub(crate) struct Receiver {
    inner: Mutex<Delivery>,
    changed: Notify,
}
impl Default for Receiver {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Delivery {
                queue: VecDeque::with_capacity(MAX_EVENTS),
                ..Default::default()
            }),
            changed: Notify::new(),
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
    pub fn common_error(&self, error: impl Into<FurpcClientError>) -> NotificationError {
        self.error()
            .unwrap_or_else(|| NotificationError::Common(error.into()))
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
                if let Some(event) = inner.queue.pop_front() {
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
    pub fn commit(&mut self, page: Page, receiver: &Receiver) -> NotificationResult<()> {
        if page.generation < self.generation {
            return Ok(());
        }
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
            let entry = slot
                .0
                .ok_or(FurpcClientError::Protocol("null notification entry"))?;
            let table = entry
                .entry
                .0
                .ok_or(FurpcClientError::Protocol("null notification partition"))?;
            let version = table
                .identity(page.generation)
                .map_err(FurpcClientError::Protocol)?;
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
            return Err(FurpcClientError::Protocol("invalid notification version range").into());
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
                .unwrap_or_else(|| furpc::Error::Closed.into()));
        }
        if updates.len() > MAX_EVENTS.saturating_sub(inner.queue.len()) {
            return Err(NotificationError::Lagged);
        }
        // The only publication point: queue entries and progress advance together.
        inner.queue.extend(updates);
        *self = next;
        drop(inner);
        receiver.changed.notify_one();
        Ok(())
    }
}
