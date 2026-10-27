use mssf_transport::{Error, Result};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// Atomic terminal authority. The hard fence never waits for a user lock or I/O.
#[derive(Debug)]
pub(crate) struct Authority {
    origin: Instant,
    expires: AtomicU64,
    retired: AtomicU64,
    disarmed: AtomicBool,
}
impl Authority {
    pub(crate) fn new(sent: Instant, ticks: i64) -> Result<Arc<Self>> {
        let expires = ttl_nanos(ticks)?;
        Ok(Arc::new(Self {
            origin: sent,
            expires: AtomicU64::new(expires),
            retired: AtomicU64::new(u64::MAX),
            disarmed: AtomicBool::new(false),
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
            || current >= self.expires.load(Ordering::SeqCst)
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
            || self.elapsed(Instant::now()) >= self.expires.load(Ordering::SeqCst)
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
            self.expires
                .load(Ordering::SeqCst)
                .saturating_sub(self.elapsed(Instant::now())),
        ))
    }
    pub(crate) fn disarm(&self) {
        self.disarmed.store(true, Ordering::SeqCst);
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
                    .expires
                    .load(Ordering::SeqCst)
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
