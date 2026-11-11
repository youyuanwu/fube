//! Source-pinned endpoint publication helper shared with the bounded planned swap.
use super::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum State {
    #[default]
    None,
    Pending,
    Completed,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EndpointPublishState {
    state: State,
}
impl EndpointPublishState {
    pub fn start_reconfiguration(&mut self) {
        self.state = State::None;
    }
    pub fn endpoint_updated(
        &mut self,
        elapsed_ticks: i64,
        timeout_ticks: i64,
        endpoint_available: &mut bool,
    ) -> bool {
        if self.state == State::Completed {
            return false;
        }
        if self.state == State::None {
            self.state = State::Pending;
        }
        Self::mark_available(endpoint_available, elapsed_ticks >= timeout_ticks)
    }
    pub fn timer(
        &mut self,
        elapsed_ticks: i64,
        timeout_ticks: i64,
        endpoint_available: &mut bool,
    ) -> (bool, bool) {
        if self.state != State::Pending {
            return (false, false);
        }
        if elapsed_ticks < timeout_ticks {
            return (false, true);
        }
        (Self::mark_available(endpoint_available, true), false)
    }
    fn mark_available(endpoint_available: &mut bool, expired: bool) -> bool {
        if expired && !*endpoint_available {
            *endpoint_available = true;
            true
        } else {
            false
        }
    }
    pub fn acknowledge(&mut self, endpoint_available: &mut bool) {
        if self.state == State::Pending {
            self.state = State::Completed;
            *endpoint_available = false;
        }
    }
    pub fn clear(&mut self, endpoint_available: &mut bool) {
        if self.state == State::Pending {
            self.state = State::None;
            *endpoint_available = false;
        }
    }
    pub fn validate(
        &self,
        reconfiguring: bool,
        closing: bool,
        endpoint_available: bool,
    ) -> Result<()> {
        if (self.state == State::Pending && (!reconfiguring || closing))
            || (endpoint_available && self.state != State::Pending)
        {
            return Err(Failure::Admission(
                "endpoint publication requires active non-closing reconfiguration",
            ));
        }
        Ok(())
    }
}
