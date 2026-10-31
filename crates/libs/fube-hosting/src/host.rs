//! ApplicationHostRegistration.cpp:38-145; ApplicationHostManager.cpp:
//! 1292-1299,1715-1864,1966-2053,2266-2299.
use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostStatus {
    NotRegistered,
    InProgress,
    Completed,
    Timedout,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRecord {
    pub(crate) record: u64,
    pub host_id: String,
    pub status: HostStatus,
    pub(crate) timer: Option<u64>,
    pub(crate) member: bool,
}
impl HostRecord {
    pub(crate) fn bytes(&self) -> usize {
        64 + self.host_id.len()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishHandle(pub(crate) u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartHandle(pub(crate) u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutHandle(pub(crate) u64);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishResult {
    pub reply: Result<()>,
    pub state_method: Option<Result<()>>,
    /// Registration callback obligation only; not a lease/host-closed event.
    pub proxy_callback: Option<String>,
}

impl State {
    pub(crate) fn host_index(&self, host: &str) -> Result<usize> {
        if self.hosts_closed {
            return Err(Error::ObjectClosed);
        }
        self.hosts
            .iter()
            .position(|h| h.member && compare_names(&h.host_id, host).is_eq())
            .ok_or(Error::HostNotRegistered)
    }
    pub(crate) fn host_valid(&self, host: &str) -> Result<bool> {
        Ok(self.hosts[self.host_index(host)?].status == HostStatus::Completed)
    }
    pub(crate) fn remove_host(&mut self, host: &str) -> Result<HostRecord> {
        let index = self.host_index(host)?;
        self.hosts[index].member = false;
        Ok(self.hosts[index].clone())
    }
}

impl Hosting {
    /// Table insertion only, intentionally separate from timer/state Start.
    pub fn add_host(&mut self, host_id: String) -> Result<()> {
        self.change(|s| {
            match s.host_index(&host_id) {
                Ok(_) => return Err(Error::HostAlreadyRegistered),
                Err(Error::HostNotRegistered) => {}
                Err(e) => return Err(e),
            }
            let record = s.id()?;
            s.hosts.push(HostRecord {
                record,
                host_id,
                status: HostStatus::NotRegistered,
                timer: None,
                member: true,
            });
            Ok(())
        })
    }
    /// Hold a record before timer arm/OnStart, like native Add's returned record.
    /// Table insertion remains separate. This is not an IPC registration reply.
    pub fn begin_start_host(&mut self, host: &str) -> Result<StartHandle> {
        self.change(|s| {
            let record = s.hosts[s.host_index(host)?].record;
            let id = s.id()?;
            s.operations.push(Operation::Start {
                id,
                record,
                timer: None,
            });
            Ok(StartHandle(id))
        })
    }
    pub fn arm_host_start_timer(
        &mut self,
        handle: StartHandle,
        clock: &dyn Clock,
        timeout_ticks: i64,
    ) -> Result<Timer> {
        let now = clock.now_ticks();
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| o.id() == handle.0)
                .ok_or(Error::InvalidState)?;
            let Operation::Start {
                record,
                timer: None,
                ..
            } = s.operations[index]
            else {
                return Err(Error::InvalidState);
            };
            let host = s
                .hosts
                .iter()
                .find(|h| h.record == record)
                .expect("held")
                .host_id
                .clone();
            let timer = Timer {
                id: s.id()?,
                due_ticks: now.saturating_add(timeout_ticks.max(1)),
                kind: TimerKind::HostFinish(host),
            };
            if let Operation::Start { timer: t, .. } = &mut s.operations[index] {
                *t = Some(timer.id);
            }
            s.timers.push(timer.clone());
            Ok(timer)
        })
    }
    /// State method uses held record even when removed. A canceled timer can
    /// still be stored, matching a callback winning before OnStart.
    pub fn complete_start_host(&mut self, handle: StartHandle) -> Result<()> {
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| o.id() == handle.0)
                .ok_or(Error::InvalidState)?;
            let Operation::Start {
                record,
                timer: Some(timer),
                ..
            } = s.operations[index]
            else {
                return Err(Error::InvalidState);
            };
            s.operations.remove(index);
            let host = s
                .hosts
                .iter_mut()
                .find(|h| h.record == record)
                .expect("held");
            if host.status != HostStatus::NotRegistered {
                return Ok(Err(Error::InvalidState));
            }
            host.status = HostStatus::InProgress;
            host.timer = Some(timer);
            Ok(Ok(()))
        })?
    }
    /// Convenience composition of independently drivable timer and state steps.
    /// This does not implement the native wire Start reply's lease/bootstrap.
    pub fn start_host(
        &mut self,
        host: &str,
        clock: &dyn Clock,
        timeout_ticks: i64,
    ) -> Result<Timer> {
        let handle = self.begin_start_host(host)?;
        let timer = match self.arm_host_start_timer(handle, clock, timeout_ticks) {
            Ok(timer) => timer,
            Err(e) => {
                self.cancel_start_operation(handle)?;
                return Err(e);
            }
        };
        self.complete_start_host(handle)?;
        Ok(timer)
    }
    pub fn find_host(&self, host: &str) -> Result<HostRecord> {
        Ok(self.state.hosts[self.state.host_index(host)?].clone())
    }
    pub fn is_host_registered(&self, host: &str) -> Result<bool> {
        self.state.host_valid(host)
    }
    /// Low-level table removal, NOT ordinary unregister/HostClosed processing.
    pub fn remove_host_entry(&mut self, host: &str) -> Result<HostRecord> {
        self.change(|s| s.remove_host(host))
    }
    /// Hold the actual table record across removal, like native shared ownership.
    pub fn begin_finish_host(&mut self, host: &str) -> Result<FinishHandle> {
        self.change(|s| {
            let record = s.hosts[s.host_index(host)?].record;
            let id = s.id()?;
            s.operations.push(Operation::Finish { id, record });
            Ok(FinishHandle(id))
        })
    }
    pub fn complete_finish_host(
        &mut self,
        handle: FinishHandle,
        proxy_present: bool,
    ) -> Result<FinishResult> {
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| matches!(o, Operation::Finish { id, .. } if *id == handle.0))
                .ok_or(Error::InvalidState)?;
            let record = s.operations.remove(index).record();
            let h = s
                .hosts
                .iter_mut()
                .find(|h| h.record == record)
                .expect("held record");
            let state_method = if h.status == HostStatus::InProgress {
                h.status = HostStatus::Completed;
                Ok(())
            } else {
                Err(Error::InvalidState)
            };
            let timer = if state_method.is_ok() {
                h.timer.take()
            } else {
                None
            };
            let host = h.host_id.clone();
            if let Some(timer) = timer {
                s.cancel_timer(timer);
            }
            Ok(FinishResult {
                reply: Ok(()), // native processor intentionally ignores state-method error
                state_method: Some(state_method),
                proxy_callback: proxy_present.then_some(host),
            })
        })
    }
    pub fn finish_host(&mut self, host: &str, proxy_present: bool) -> Result<FinishResult> {
        match self.begin_finish_host(host) {
            Ok(handle) => self.complete_finish_host(handle, proxy_present),
            Err(e @ (Error::HostNotRegistered | Error::ObjectClosed)) => Ok(FinishResult {
                reply: Err(e),
                state_method: None,
                proxy_callback: None,
            }),
            Err(e) => Err(e),
        }
    }
    /// Timer callback cancellation precedes table Find. A callback already owned
    /// by the driver may run after cancellation: native host timeout has no
    /// invented global generation guard.
    pub fn begin_host_timeout(&mut self, timer: &Timer) -> Result<TimeoutHandle> {
        let TimerKind::HostFinish(host) = &timer.kind;
        self.change(|s| {
            s.cancel_timer(timer.id);
            Ok(())
        })?;
        self.change(|s| {
            let record = s.hosts[s.host_index(host)?].record;
            let id = s.id()?;
            s.operations.push(Operation::Timeout {
                id,
                record,
                marked: false,
            });
            Ok(TimeoutHandle(id))
        })
    }
    pub fn mark_host_timeout(&mut self, handle: TimeoutHandle) -> Result<()> {
        // A state-method failure stops the processor and releases held ownership.
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| o.id() == handle.0)
                .ok_or(Error::InvalidState)?;
            let Operation::Timeout { record, marked, .. } = s.operations[index].clone() else {
                return Err(Error::InvalidState);
            };
            if marked {
                return Err(Error::InvalidState);
            }
            let host = s
                .hosts
                .iter_mut()
                .find(|h| h.record == record)
                .expect("held");
            if host.status != HostStatus::InProgress {
                s.operations.remove(index);
                return Ok(Err(Error::InvalidState));
            }
            host.status = HostStatus::Timedout;
            if let Operation::Timeout { marked, .. } = &mut s.operations[index] {
                *marked = true;
            }
            Ok(Ok(()))
        })?
    }
    pub fn remove_timedout_host(&mut self, handle: TimeoutHandle) -> Result<()> {
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| o.id() == handle.0)
                .ok_or(Error::InvalidState)?;
            let Operation::Timeout {
                record,
                marked: true,
                ..
            } = s.operations[index]
            else {
                return Err(Error::InvalidState);
            };
            let host = s
                .hosts
                .iter()
                .find(|h| h.record == record)
                .expect("held")
                .host_id
                .clone();
            s.operations.remove(index);
            // Native Remove uses host ID, not record identity.
            Ok(s.remove_host(&host).map(|_| ()))
        })?
    }
    pub fn host_timeout(&mut self, timer: &Timer) -> Result<()> {
        let handle = self.begin_host_timeout(timer)?;
        self.mark_host_timeout(handle)?;
        self.remove_timedout_host(handle)
    }
    /// Abandoning an observer is not implicit cancellation; explicitly release a
    /// held test/driver operation to avoid retaining removed host records forever.
    pub fn cancel_host_operation(&mut self, id: FinishHandle) -> Result<()> {
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| matches!(o, Operation::Finish { id: handle, .. } if *handle == id.0))
                .ok_or(Error::InvalidState)?;
            s.operations.remove(index);
            Ok(())
        })
    }
    pub fn cancel_timeout_operation(&mut self, handle: TimeoutHandle) -> Result<()> {
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| matches!(o, Operation::Timeout { id, .. } if *id == handle.0))
                .ok_or(Error::InvalidState)?;
            s.operations.remove(index);
            Ok(())
        })
    }
    /// Local explicit cancellation of held Start work, not a native successful
    /// unregister or successful registration receipt.
    pub fn cancel_start_operation(&mut self, handle: StartHandle) -> Result<()> {
        self.change(|s| {
            let index = s
                .operations
                .iter()
                .position(|o| o.id() == handle.0)
                .ok_or(Error::InvalidState)?;
            let Operation::Start { timer, .. } = s.operations[index] else {
                return Err(Error::InvalidState);
            };
            s.operations.remove(index);
            if let Some(timer) = timer {
                s.cancel_timer(timer);
            }
            Ok(())
        })
    }
}
