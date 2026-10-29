use fube_transport::{Error, Result};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
const CALLBACK_DEADLINE_SLOTS: usize = 64;
#[cfg(test)]
#[path = "lease_callback_tests.rs"]
mod callback_tests;

/// A callback deadline never renews the node lease. Completion includes dropping
/// the callback's future/captures, not merely signaling its cancellation token.
pub(crate) struct CallbackDeadline {
    authority: Arc<Authority>,
    slot: usize,
    deadline: u64,
}
impl Drop for CallbackDeadline {
    fn drop(&mut self) {
        if self.authority.elapsed(Instant::now()) >= self.deadline {
            self.authority
                .retired
                .fetch_min(self.deadline, Ordering::SeqCst);
        }
        let _ = self.authority.callbacks[self.slot].compare_exchange(
            self.deadline,
            u64::MAX,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
}

/// Atomic terminal authority. The hard fence never waits for a user lock or I/O.
#[derive(Debug)]
pub(crate) struct Authority {
    origin: Instant,
    expires: AtomicU64,
    retired: AtomicU64,
    disarmed: AtomicBool,
    disarming: AtomicBool,
    callbacks: [AtomicU64; CALLBACK_DEADLINE_SLOTS],
}
impl Authority {
    pub(crate) fn new(sent: Instant, ticks: i64) -> Result<Arc<Self>> {
        let expires = ttl_nanos(ticks)?;
        Ok(Arc::new(Self {
            origin: sent,
            expires: AtomicU64::new(expires),
            retired: AtomicU64::new(u64::MAX),
            disarmed: AtomicBool::new(false),
            disarming: AtomicBool::new(false),
            callbacks: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
        }))
    }
    fn elapsed(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.origin)
            .as_nanos()
            .min(u64::MAX as u128 - 1) as u64
    }
    fn candidate(&self, sent: Instant, ticks: i64) -> Result<u64> {
        self.elapsed(sent)
            .checked_add(ttl_nanos(ticks)?)
            .filter(|v| *v < u64::MAX)
            .ok_or(Error::Protocol("lease clock overflow"))
    }
    fn effective_deadline(&self) -> u64 {
        self.callbacks
            .iter()
            .map(|deadline| deadline.load(Ordering::SeqCst))
            .fold(self.expires.load(Ordering::SeqCst), u64::min)
    }
    pub(crate) fn callback_deadline(
        self: &Arc<Self>,
        timeout: Duration,
        cleanup: bool,
    ) -> Result<CallbackDeadline> {
        if !cleanup {
            self.check()?;
        }
        if self.disarming.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        let budget = u64::try_from(timeout.as_nanos())
            .map_err(|_| Error::Protocol("callback deadline overflow"))?;
        if budget == 0 {
            return Err(Error::InvalidInput("zero callback deadline"));
        }
        let deadline = self
            .elapsed(Instant::now())
            .checked_add(budget)
            .filter(|value| *value < u64::MAX)
            .ok_or(Error::Protocol("callback deadline overflow"))?;
        for (slot, value) in self.callbacks.iter().enumerate() {
            if value
                .compare_exchange(u64::MAX, deadline, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let guard = CallbackDeadline {
                    authority: self.clone(),
                    slot,
                    deadline,
                };
                if self.disarming.load(Ordering::SeqCst) {
                    return Err(Error::Closed);
                }
                return Ok(guard);
            }
        }
        Err(Error::ResourceLimit("callback deadline slots"))
    }
    /// Replace provisional startup deadline only after a fresh correlated poll.
    pub(crate) fn establish(&self, sent: Instant, ticks: i64) -> Result<()> {
        self.check()?;
        self.expires
            .store(self.candidate(sent, ticks)?, Ordering::SeqCst);
        self.check()
    }
    pub(crate) fn renew(&self, sent: Instant, ticks: i64, now: Instant) -> Result<()> {
        let expires = self.candidate(sent, ticks)?;
        let current = self.elapsed(now);
        if self.retired.load(Ordering::SeqCst) != u64::MAX
            || current >= self.effective_deadline()
            || self.disarmed.load(Ordering::SeqCst)
        {
            self.retired.fetch_min(current, Ordering::SeqCst);
            return Err(Error::Closed);
        }
        if expires <= current {
            return Err(Error::Timeout);
        }
        self.expires.fetch_max(expires, Ordering::SeqCst);
        self.check()
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self.retired.load(Ordering::SeqCst) != u64::MAX
            || self.disarmed.load(Ordering::SeqCst)
            || self.elapsed(Instant::now()) >= self.effective_deadline()
        {
            self.retire();
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn retire(&self) {
        self.retired
            .fetch_min(self.elapsed(Instant::now()), Ordering::SeqCst);
    }
    pub(crate) fn remaining(&self) -> Result<Duration> {
        self.check()?;
        Ok(Duration::from_nanos(
            self.effective_deadline()
                .saturating_sub(self.elapsed(Instant::now())),
        ))
    }
    pub(crate) fn disarm(&self) -> Result<()> {
        self.disarming.store(true, Ordering::SeqCst);
        if self
            .callbacks
            .iter()
            .any(|deadline| deadline.load(Ordering::SeqCst) != u64::MAX)
        {
            self.retire();
            return Err(Error::Protocol("callback ownership remains at shutdown"));
        }
        self.disarmed.store(true, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) fn watchdog(self: &Arc<Self>) -> Result<()> {
        // Strong ownership deliberately survives Runtime drop, task cancellation
        // and failed shutdown. Only proven cleanup may disarm it.
        let authority = self.clone();
        std::thread::Builder::new()
            .name("sf-authority".into())
            .spawn(move || loop {
                if authority.disarmed.load(Ordering::SeqCst) {
                    return;
                }
                let deadline = authority
                    .effective_deadline()
                    .min(authority.retired.load(Ordering::SeqCst));
                if authority.elapsed(Instant::now()).saturating_sub(deadline) >= 500_000_000 {
                    // _exit does not acquire stdio locks, flush streams or invoke
                    // application-controlled atexit handlers. OS process boundary only.
                    unsafe { libc::_exit(70) }
                }
                std::thread::sleep(Duration::from_millis(10));
            })?;
        Ok(())
    }
}
fn ttl_nanos(ticks: i64) -> Result<u64> {
    if ticks <= 0 {
        return Err(Error::Closed);
    }
    (ticks as u64)
        .checked_mul(100)
        .filter(|v| *v < u64::MAX)
        .ok_or(Error::Protocol("lease TTL overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delayed_reply_is_send_based_and_never_revives() {
        let now = Instant::now();
        let a = Authority::new(now, 100_000_000).unwrap();
        assert!(a
            .renew(now, 20_000_000, now + Duration::from_secs(3))
            .is_err());
        assert_eq!(a.expires.load(Ordering::SeqCst), 10_000_000_000);
        assert!(a
            .renew(
                now + Duration::from_secs(9),
                100_000_000,
                now + Duration::from_secs(11)
            )
            .is_err());
        assert!(a
            .renew(
                now + Duration::from_secs(11),
                100_000_000,
                now + Duration::from_secs(11)
            )
            .is_err());
    }
    #[test]
    fn terminal_invalidation_and_invalid_ttls() {
        let now = Instant::now();
        for ttl in [0, -1, i64::MAX] {
            assert!(Authority::new(now, ttl).is_err());
        }
        let a = Authority::new(now, 100_000_000).unwrap();
        a.retire();
        assert!(a.check().is_err());
        assert!(a.renew(now, 200_000_000, now).is_err());
        assert!(a.establish(now, 200_000_000).is_err());
    }
}
