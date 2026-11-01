use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// Node-local renewal permission, not a Federation/distributed lease agent.
pub(crate) struct Authority {
    origin: Instant,
    duration: u64,
    state: AtomicU64,
    pub(crate) cancel: Notify,
}
const TERMINAL: u64 = 1 << 63;
const STOPPED: u64 = 1 << 62;
const EXPIRY: u64 = STOPPED - 1;

impl Authority {
    pub(crate) fn new(duration: Duration) -> Arc<Self> {
        let nanos = u64::try_from(duration.as_nanos()).expect("validated finite lease");
        Arc::new(Self {
            origin: Instant::now(),
            duration: nanos,
            state: AtomicU64::new(nanos),
            cancel: Notify::new(),
        })
    }
    fn now(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
    pub(crate) fn live(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        if state & TERMINAL != 0 || self.now() >= state & EXPIRY {
            self.revoke();
            return false;
        }
        true
    }
    pub(crate) fn poll(&self) -> Option<i64> {
        loop {
            let state = self.state.load(Ordering::SeqCst);
            let now = self.now();
            if state & TERMINAL != 0 || now >= state & EXPIRY {
                self.revoke();
                return None;
            }
            let next = if state & STOPPED != 0 {
                state
            } else {
                now.checked_add(self.duration)
                    .filter(|value| *value <= EXPIRY)?
            };
            // Stop/retire and renewal share one linearization point.
            if self
                .state
                .compare_exchange(state, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                if !self.live() {
                    return None;
                }
                return i64::try_from((next & EXPIRY).saturating_sub(self.now()) / 100)
                    .ok()
                    .filter(|ticks| *ticks > 0);
            }
        }
    }
    pub(crate) fn stop_renewal(&self) {
        self.state.fetch_or(STOPPED, Ordering::SeqCst);
    }
    pub(crate) fn retire(&self) {
        self.state.fetch_or(TERMINAL, Ordering::SeqCst);
    }
    pub(crate) fn revoke(&self) {
        self.retire();
        self.cancel.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finite_expiry_is_terminal() {
        let a = Authority::new(Duration::from_millis(10));
        assert!(a.poll().unwrap() <= 100_000);
        a.stop_renewal();
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(a.poll(), None);
        assert_eq!(a.poll(), None);
    }
    #[test]
    fn revocation_cannot_renew() {
        let a = Authority::new(Duration::from_secs(30));
        assert!(a.poll().unwrap() <= 300_000_000);
        a.revoke();
        assert_eq!(a.poll(), None);
    }
}
