//! Actual serialized local Hosting consumers. Required lifecycle/FM completion
//! is explicitly unavailable; a local decision is not deployment or an ACK.
use super::*;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostingReadiness {
    pub fm_ready: bool,
    pub node_active: bool,
    pub upgrading: bool,
    pub ra_open: bool,
    pub ra_closing: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostingRegistered {
    pub unit: PartitionId,
    pub readiness: HostingReadiness,
    pub service_type: ServiceType,
    pub version: VersionInstance,
    pub activation: PartitionId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostingRuntimeClosed {
    pub unit: PartitionId,
    pub readiness: HostingReadiness,
    pub host: String,
    pub runtime: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostingHostClosed {
    pub unit: PartitionId,
    pub readiness: HostingReadiness,
    pub host: String,
    pub activity: String,
}

pub(super) fn consume(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
) -> Result<Transition> {
    let invalid = |_| Failure::Admission("invalid Hosting event input");
    let (ready, disposition) = match work.kind {
        Kind::HostingRegistered => {
            let event: HostingRegistered = from_slice(&work.bytes).map_err(invalid)?;
            let ready = event.readiness;
            let disposition = if !ready.node_active
                || ready.upgrading
                || !admission.node_open
                || !admission.node_activated
                || admission.fabric_upgrading
            {
                Disposition::IgnoredNotReady
            } else if let Some(s) = current.filter(|s| s.phase != Phase::Closed) {
                let exclusive = event.activation.guid.0 != [0; 16];
                if !compare_service_types(&s.body.service.service_type, &event.service_type).is_eq()
                    || s.body.replica.version != event.version
                    || (s.body.service.activation_mode == 1) != exclusive
                    || (exclusive && event.activation.guid != event.unit.guid)
                {
                    Disposition::IgnoredStale
                } else if !s.body.replica.up {
                    // Native down-reopen requires persisted state, which this
                    // stateless consumer profile does not admit.
                    Disposition::HostingMatchedNoWork
                } else {
                    match s.phase {
                        Phase::Opening => Disposition::HostingOpenRetryRequired,
                        Phase::Closing => Disposition::HostingCloseRetryRequired,
                        Phase::Ready => Disposition::HostingMatchedNoWork,
                        Phase::Closed => unreachable!(),
                    }
                }
            } else {
                Disposition::IgnoredStale
            };
            (ready, disposition)
        }
        Kind::HostingRuntimeClosed => {
            let event: HostingRuntimeClosed = from_slice(&work.bytes).map_err(invalid)?;
            let disposition = if current.is_some_and(|s| {
                !matches!(s.phase, Phase::Closing | Phase::Closed)
                    && s.body.replica.up
                    && s.runtime
                        .as_ref()
                        .is_some_and(|r| r.host == event.host && r.runtime == event.runtime)
            }) {
                Disposition::HostingRuntimeCloseRequired
            } else {
                Disposition::IgnoredStale
            };
            (event.readiness, disposition)
        }
        Kind::HostingHostClosed => {
            let event: HostingHostClosed = from_slice(&work.bytes).map_err(invalid)?;
            let disposition = if current.is_some_and(|s| {
                s.phase != Phase::Closed && s.runtime.as_ref().is_some_and(|r| r.host == event.host)
            }) {
                Disposition::HostingHostCrashRequired
            } else {
                Disposition::IgnoredStale
            };
            (event.readiness, disposition)
        }
        _ => unreachable!(),
    };
    let can_run = if matches!(work.kind, Kind::HostingRegistered) {
        ready.ra_open
    } else {
        ready.ra_open || ready.ra_closing
    };
    if !admission.owner_ready || !ready.fm_ready || !can_run {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
    // No acquisition/lookup is fabricated from an event. A matching required
    // action is explicitly gated, before any mutation/store/effect publication.
    Ok(Transition::ignored(disposition))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingStatus {
    pub service_type: ServiceType,
    pub enabled: bool,
}
#[derive(Debug, Clone)]
struct History {
    id: ServiceType,
    sequence: u64,
    updated: i64,
}
#[derive(Debug, Clone)]
pub struct LocalStatusConsumer {
    history: Vec<History>,
    pending: Vec<PendingStatus>,
    message_sequence: u64,
    now: i64,
    next_cleanup: i64,
    max_types: usize,
    max_bytes: usize,
}
impl LocalStatusConsumer {
    pub fn new(max_types: usize, max_bytes: usize) -> Result<Self> {
        if max_types == 0 || max_types > 65536 || max_bytes == 0 {
            return Err(Failure::Admission("invalid status bounds"));
        }
        Ok(Self {
            history: Vec::new(),
            pending: Vec::new(),
            message_sequence: 0,
            now: 0,
            next_cleanup: 36_000_000_000,
            max_types,
            max_bytes,
        })
    }
    /// Native cleanup timer is hourly; sequence entries retain ten minutes.
    /// Cleanup removes history only, never pending unacknowledged status.
    pub fn tick(&mut self, now: i64) -> Result<()> {
        if now < self.now {
            return Err(Failure::Admission("status clock regressed"));
        }
        let next_cleanup = if now >= self.next_cleanup {
            now.checked_add(36_000_000_000).ok_or(Failure::Capacity)?
        } else {
            self.next_cleanup
        };
        if now >= self.next_cleanup {
            self.history
                .retain(|h| now.saturating_sub(h.updated) < 6_000_000_000);
            self.next_cleanup = next_cleanup;
        }
        self.now = now;
        Ok(())
    }
    pub fn consume(
        &mut self,
        id: ServiceType,
        sequence: u64,
        enabled: bool,
        fm_ready: bool,
        now: i64,
    ) -> Result<Disposition> {
        if !fm_ready {
            return Ok(Disposition::IgnoredNotReady);
        }
        let mut next = self.clone();
        next.tick(now)?;
        if let Some(h) = next
            .history
            .iter_mut()
            .find(|h| compare_service_types(&h.id, &id).is_eq())
        {
            if sequence <= h.sequence {
                return Ok(Disposition::IgnoredStale);
            }
            h.sequence = sequence;
            h.updated = now;
        } else {
            next.history.push(History {
                id: id.clone(),
                sequence,
                updated: now,
            });
        }
        let opposite = next.pending.iter().position(|p| {
            compare_service_types(&p.service_type, &id).is_eq() && p.enabled != enabled
        });
        if let Some(i) = opposite {
            next.pending.remove(i);
        }
        let should_increment =
            opposite.is_some() || !enabled || !next.pending.iter().any(|p| !p.enabled);
        if !next
            .pending
            .iter()
            .any(|p| compare_service_types(&p.service_type, &id).is_eq())
        {
            next.pending.push(PendingStatus {
                service_type: id,
                enabled,
            });
            if should_increment {
                next.message_sequence = next
                    .message_sequence
                    .checked_add(1)
                    .ok_or(Failure::Capacity)?;
            }
        }
        let bytes = next
            .history
            .iter()
            .map(|h| type_bytes(&h.id) + 64)
            .sum::<usize>()
            + next
                .pending
                .iter()
                .map(|p| type_bytes(&p.service_type) + 64)
                .sum::<usize>();
        if next.history.len() > next.max_types
            || next.pending.len() > next.max_types
            || bytes > next.max_bytes
        {
            return Err(Failure::Capacity);
        }
        *self = next;
        Ok(Disposition::HostingStatusProcessed)
    }
    pub fn pending(&self) -> &[PendingStatus] {
        &self.pending
    }
    pub fn message_sequence(&self) -> u64 {
        self.message_sequence
    }
    pub fn next_enabled(&self) -> Option<bool> {
        if self.pending.iter().any(|p| !p.enabled) {
            Some(false)
        } else if self.pending.is_empty() {
            None
        } else {
            Some(true)
        }
    }
    // No capture/send/ACK API exists while the FM consumer is capability-gated.
}
fn type_bytes(id: &ServiceType) -> usize {
    128 + id.package.application.type_name.len() + id.package.name.len() + id.name.len()
}
