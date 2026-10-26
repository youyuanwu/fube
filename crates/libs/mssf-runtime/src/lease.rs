use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use mssf_transport::{Error, Result};

#[derive(Debug)]
struct State {
    expires: Instant,
    retired: Option<Instant>,
    disarmed: bool,
}

/// Terminal authority. Native TTL is relative to request send, never receipt.
#[derive(Debug)]
pub(crate) struct Authority(Mutex<State>);
impl Authority {
    pub(crate) fn new(sent: Instant, ticks: i64) -> Result<Arc<Self>> {
        Ok(Arc::new(Self(Mutex::new(State {
            expires: expiration(sent, ticks)?,
            retired: None,
            disarmed: false,
        }))))
    }
    pub(crate) fn renew(&self, sent: Instant, ticks: i64, now: Instant) -> Result<()> {
        let expires = expiration(sent, ticks)?;
        let mut state = self.0.lock().unwrap();
        if state.retired.is_some() || now >= state.expires || state.disarmed {
            state.retired.get_or_insert(now);
            return Err(Error::Closed);
        }
        if expires <= now {
            return Err(Error::Timeout);
        }
        state.expires = state.expires.max(expires);
        Ok(())
    }
    pub(crate) fn check(&self) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        if state.retired.is_some() || state.disarmed || Instant::now() >= state.expires {
            state.retired.get_or_insert_with(Instant::now);
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn retire(&self) {
        self.0
            .lock()
            .unwrap()
            .retired
            .get_or_insert_with(Instant::now);
    }
    pub(crate) fn disarm(&self) {
        self.0.lock().unwrap().disarmed = true;
    }
    pub(crate) fn watchdog(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("sf-authority".into())
            .spawn(move || {
                loop {
                    {
                        let Some(authority) = weak.upgrade() else {
                            return;
                        };
                        let state = authority.0.lock().unwrap();
                        if state.disarmed {
                            return;
                        }
                        let deadline = state
                            .retired
                            .map_or(state.expires, |retired| retired.min(state.expires));
                        if Instant::now() >= deadline + Duration::from_millis(500) {
                            // This is deliberately independent of the async executor,
                            // callback cooperation, locks and user abort handlers.
                            eprintln!("mssf-runtime: authority lost; terminating application");
                            std::process::exit(70);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
            .expect("authority watchdog thread");
    }
}
fn expiration(sent: Instant, ticks: i64) -> Result<Instant> {
    if ticks <= 0 {
        return Err(Error::Closed);
    }
    sent.checked_add(Duration::from_nanos(
        (ticks as u64)
            .checked_mul(100)
            .ok_or(Error::Protocol("lease TTL overflow"))?,
    ))
    .ok_or(Error::Protocol("lease clock overflow"))
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
        assert_eq!(a.0.lock().unwrap().expires, now + Duration::from_secs(10));
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
    }
}
