//! Shared test-fixture hook, not a replication or native-control contract.
use std::{sync::Mutex, time::Duration};

use fube_data::{Cancellation, Error, Result};
use serde_json::{json, Value};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct CopyAckHold {
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Default)]
struct State {
    milliseconds: u64,
    held: bool,
    sequence: Option<i64>,
    boundary: Option<i64>,
    released: bool,
    acknowledged: bool,
    expired: bool,
    cancelled: bool,
}

impl CopyAckHold {
    pub(crate) fn set(&self, milliseconds: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if milliseconds > 6000 || (milliseconds != 0 && state.held) {
            return Err(Error::InvalidInput(
                "copy ACK hold must be 0..6000 ms and not rearmed while held",
            ));
        }
        if milliseconds == 0 {
            state.milliseconds = 0;
            state.released |= state.held;
        } else {
            *state = State {
                milliseconds,
                ..State::default()
            };
        }
        drop(state);
        self.changed.notify_one();
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Value {
        let state = self.state.lock().unwrap();
        json!({
            "armed_ms":state.milliseconds,"held":state.held,
            "sequence":state.sequence,"boundary":state.boundary,
            "copy_end_received":state.sequence.is_some(),"released":state.released,
            "acknowledged":state.acknowledged,"expired":state.expired,"cancelled":state.cancelled
        })
    }

    pub(crate) async fn wait(
        &self,
        sequence: i64,
        boundary: i64,
        stop: &Cancellation,
        owner_stop: &Cancellation,
    ) -> Result<bool> {
        let milliseconds = {
            let mut state = self.state.lock().unwrap();
            if state.milliseconds == 0 {
                return Ok(false);
            }
            state.held = true;
            state.sequence = Some(sequence);
            state.boundary = Some(boundary);
            state.milliseconds
        };
        let expires = tokio::time::Instant::now() + Duration::from_millis(milliseconds);
        let deadline = tokio::time::sleep_until(expires);
        tokio::pin!(deadline);
        loop {
            // Register before checking release, including a release before wait.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                if stop.is_cancelled() || owner_stop.is_cancelled() {
                    state.held = false;
                    state.cancelled = true;
                    return Err(Error::Closed);
                }
                if tokio::time::Instant::now() >= expires {
                    state.held = false;
                    state.expired = true;
                    return Err(Error::Timeout);
                }
                if state.released {
                    return Ok(true);
                }
            }
            tokio::select! {
                biased;
                _ = stop.cancelled() => {},
                _ = owner_stop.cancelled() => {},
                _ = &mut changed => {},
                _ = &mut deadline => {
                    let mut state = self.state.lock().unwrap();
                    state.held = false;
                    state.expired = true;
                    return Err(Error::Timeout);
                }
            }
        }
    }

    pub(crate) fn acknowledged(&self) {
        let mut state = self.state.lock().unwrap();
        state.held = false;
        state.acknowledged = true;
    }
}
