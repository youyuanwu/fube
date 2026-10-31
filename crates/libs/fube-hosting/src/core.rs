use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Operation {
    Start {
        id: u64,
        record: u64,
        timer: Option<u64>,
    },
    Finish {
        id: u64,
        record: u64,
    },
    Timeout {
        id: u64,
        record: u64,
        marked: bool,
    },
}
impl Operation {
    pub(crate) fn id(&self) -> u64 {
        match self {
            Self::Start { id, .. } | Self::Finish { id, .. } | Self::Timeout { id, .. } => *id,
        }
    }
    pub(crate) fn record(&self) -> u64 {
        match self {
            Self::Start { record, .. }
            | Self::Finish { record, .. }
            | Self::Timeout { record, .. } => *record,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct State {
    pub next_id: u64,
    pub hosts: Vec<HostRecord>,
    pub hosts_closed: bool,
    pub runtimes: Vec<RuntimeRecord>,
    pub runtimes_closed: bool,
    pub timers: Vec<Timer>,
    pub operations: Vec<Operation>,
    pub type_sequence: u64,
    pub types: Vec<TypeEntry>,
    pub types_closed: bool,
    pub failures: Vec<String>,
    pub closed_registered: Vec<ClosedRegisteredEvent>,
    pub enabled_disabled: Vec<EnabledDisabledEvent>,
    pub obligations: Vec<Obligation>,
    pub public_activations: Vec<PublicActivationEntry>,
    pub pending_activations: Vec<PendingActivation>,
}
impl State {
    pub(crate) fn id(&mut self) -> Result<u64> {
        self.next_id = self.next_id.checked_add(1).ok_or(Error::Capacity)?;
        Ok(self.next_id)
    }
    pub(crate) fn cancel_timer(&mut self, id: u64) {
        self.timers.retain(|t| t.id != id);
    }
    pub(crate) fn collect_hosts(&mut self) {
        let removed: Vec<_> = self
            .hosts
            .iter()
            .filter(|h| !h.member && !self.operations.iter().any(|o| o.record() == h.record))
            .map(|h| h.record)
            .collect();
        let timers: Vec<_> = self
            .hosts
            .iter()
            .filter(|h| removed.contains(&h.record))
            .filter_map(|h| h.timer)
            .collect();
        self.hosts.retain(|h| !removed.contains(&h.record));
        self.timers.retain(|t| !timers.contains(&t.id));
    }
    fn check(&self, limits: &Limits) -> Result<()> {
        let bytes = self.hosts.iter().map(HostRecord::bytes).sum::<usize>()
            + self
                .runtimes
                .iter()
                .map(RuntimeRecord::bytes)
                .sum::<usize>()
            + self.timers.iter().map(Timer::bytes).sum::<usize>()
            + self.operations.len() * 64
            + self.types.iter().map(TypeEntry::bytes).sum::<usize>()
            + self.failures.iter().map(|f| 32 + f.len()).sum::<usize>()
            + self
                .public_activations
                .iter()
                .map(|p| {
                    64 + p.key.len()
                        + p.public_activation.len()
                        + p.service_name.as_ref().map_or(0, String::len)
                        + p.application_name.as_ref().map_or(0, String::len)
                })
                .sum::<usize>()
            + self
                .pending_activations
                .iter()
                .map(|p| 64 + p.request.bytes())
                .sum::<usize>();
        if self.hosts.len() > limits.hosts
            || self.runtimes.len() > limits.runtimes
            || self.timers.len() > limits.timers
            || self.operations.len() + self.pending_activations.len() > limits.operations
            || self.public_activations.len() > limits.packages
            || self.types.len() > limits.types
            || self.failures.len() > limits.failures
            || self.types.iter().map(|e| e.sources.len()).sum::<usize>() > limits.sources
            || bytes > limits.state_bytes
            || self.closed_registered.len() > limits.events_per_lane
            || self.enabled_disabled.len() > limits.events_per_lane
            || self
                .closed_registered
                .iter()
                .map(ClosedRegisteredEvent::bytes)
                .sum::<usize>()
                > limits.event_bytes_per_lane
            || self
                .enabled_disabled
                .iter()
                .map(EnabledDisabledEvent::bytes)
                .sum::<usize>()
                > limits.event_bytes_per_lane
            || self.obligations.len() > limits.obligations
            || self
                .obligations
                .iter()
                .map(Obligation::bytes)
                .sum::<usize>()
                > limits.obligation_bytes
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}

/// Single-owner serialized core. Each change reserves a bounded projected state
/// before publication. Split methods retain native interleaving boundaries.
#[derive(Debug)]
pub struct Hosting {
    pub(crate) state: State,
    pub(crate) limits: Limits,
}
impl Hosting {
    pub fn new(limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            state: State::default(),
            limits,
        })
    }

    pub(crate) fn change<T>(&mut self, f: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        let mut next = self.state.clone();
        let result = f(&mut next)?;
        next.collect_hosts();
        next.check(&self.limits)?;
        self.state = next;
        Ok(result)
    }
    pub fn timers(&self) -> &[Timer] {
        &self.state.timers
    }
    pub fn outstanding_operations(&self) -> usize {
        self.state.operations.len() + self.state.pending_activations.len()
    }
    pub fn retained_host_records(&self) -> usize {
        self.state.hosts.len()
    }
    /// No live RA event consumer/IPC/lease/process capability is supplied.
    /// H2 accounting alone does not satisfy node composition admission.
    pub fn require_live_composition(&self) -> Result<()> {
        Err(Error::UnsupportedCapability)
    }
    /// Explicit shutdown of registration tables. This is not a host/process
    /// closure event, and does not assert external resources were cleaned.
    pub fn close_tables(&mut self) -> Result<()> {
        self.change(|s| {
            s.hosts_closed = true;
            s.runtimes_closed = true;
            for h in &mut s.hosts {
                h.member = false;
            }
            s.runtimes.clear();
            s.timers
                .retain(|t| matches!(t.kind, TimerKind::DisableType { .. }));
            Ok(())
        })
    }
    /// Close all registration state, retaining already accepted owned outputs and
    /// external pending activation observations. This receipt is not successful
    /// physical shutdown; its remaining work must be driven/drained by the owner.
    pub fn close_core(&mut self) -> Result<CoreClosure> {
        self.change(|s| {
            s.hosts_closed = true;
            s.runtimes_closed = true;
            s.types_closed = true;
            for h in &mut s.hosts {
                h.member = false;
            }
            s.runtimes.clear();
            s.types.clear();
            s.failures.clear();
            s.public_activations.clear();
            s.timers.clear();
            Ok(CoreClosure {
                held_host_operations: s.operations.len(),
                pending_activations: s.pending_activations.len(),
                queued_events: s.closed_registered.len() + s.enabled_disabled.len(),
                queued_obligations: s.obligations.len(),
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreClosure {
    pub held_host_operations: usize,
    pub pending_activations: usize,
    pub queued_events: usize,
    pub queued_obligations: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operation_and_type_sequence_overflow_veto_without_publication() {
        let mut h = Hosting::new(Limits::default()).unwrap();
        h.state.next_id = u64::MAX;
        let before = h.state.clone();
        assert_eq!(h.add_host("H".into()), Err(Error::Capacity));
        assert_eq!(h.state, before);
        h.state.type_sequence = u64::MAX;
        let before = h.state.clone();
        assert_eq!(
            h.get_type_state(&TypeInstance::default()),
            Err(Error::Capacity)
        );
        assert_eq!(h.state, before);
    }
}
