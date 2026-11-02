//! Non-native fixture backend. It observes finite deadlines but has no process
//! watchdog: the application must provide any blocking-code terminal fence.
use super::*;
use crate::Error;
use std::{
    sync::{Mutex, Weak},
    time::Duration,
};

#[derive(Default)]
pub(super) struct StandaloneAuthority {
    retired: Mutex<Option<Instant>>,
    deadlines: Mutex<Vec<Weak<StandaloneDeadline>>>,
    threads: Mutex<Vec<Arc<StandaloneThread>>>,
}
impl StandaloneAuthority {
    #[cfg(test)]
    pub(super) fn retired_at_for_test(&self) -> Option<Instant> {
        *self.retired.lock().unwrap()
    }
}
struct StandaloneDeadline {
    authority: Arc<StandaloneAuthority>,
    times: Mutex<Option<(Instant, Instant)>>,
}
impl ExecutionDeadline for StandaloneDeadline {
    fn arm_until(&self, until: Instant) {
        let mut times = self.times.lock().unwrap();
        if times.is_none() {
            *times = Some((until, until));
        }
    }
    fn set_effective_until(&self, until: Instant) {
        let mut times = self.times.lock().unwrap();
        if let Some((original, effective)) = times.as_mut() {
            *effective = until.min(*original);
        }
    }
}
impl Drop for StandaloneDeadline {
    fn drop(&mut self) {
        if let Some((_, until)) = *self.times.lock().unwrap() {
            if Instant::now() >= until {
                self.authority.retire_at(until);
            }
        }
    }
}
struct DeadlineHandle(Arc<StandaloneDeadline>);
impl ExecutionDeadline for DeadlineHandle {
    fn arm_until(&self, until: Instant) {
        self.0.arm_until(until);
    }
    fn set_effective_until(&self, until: Instant) {
        self.0.set_effective_until(until);
    }
}
struct StandaloneThread(Mutex<Option<std::thread::JoinHandle<()>>>);
impl ExecutionThread for StandaloneThread {
    fn join_finished(&self) -> ApiFuture<'_, ()> {
        Box::pin(async {
            loop {
                let finished = {
                    let mut thread = self.0.lock().unwrap();
                    match thread.as_ref() {
                        None => return Ok(()),
                        Some(handle) if handle.is_finished() => thread.take(),
                        Some(_) => None,
                    }
                };
                if let Some(thread) = finished {
                    return thread
                        .join()
                        .map_err(|_| Error::Protocol("owned thread panicked"));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    }
}
impl ExecutionAuthority for StandaloneAuthority {
    fn check(&self) -> Result<()> {
        if self.retired.lock().unwrap().is_some() {
            return Err(Error::Closed);
        }
        let expired = self
            .deadlines
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|deadline| {
                deadline
                    .times
                    .lock()
                    .unwrap()
                    .map(|(_, effective)| effective)
            })
            .filter(|until| *until <= Instant::now())
            .min();
        if let Some(until) = expired {
            self.retire_at(until);
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn retire(&self) {
        self.retire_at(Instant::now());
    }
    fn retire_at(&self, until: Instant) {
        let mut retired = self.retired.lock().unwrap();
        *retired = Some(retired.map_or(until, |previous| previous.min(until)));
    }
    fn reserve_callback_deadline(
        self: Arc<Self>,
        cleanup: bool,
    ) -> Result<Box<dyn ExecutionDeadline>> {
        if !cleanup {
            self.check()?;
        }
        let mut deadlines = self.deadlines.lock().unwrap();
        deadlines.retain(|deadline| deadline.strong_count() != 0);
        if deadlines.len() >= 64 {
            return Err(Error::ResourceLimit("callback deadline slots"));
        }
        let deadline = Arc::new(StandaloneDeadline {
            authority: self.clone(),
            times: Mutex::new(None),
        });
        deadlines.push(Arc::downgrade(&deadline));
        Ok(Box::new(DeadlineHandle(deadline)))
    }
    fn own_thread(&self, thread: std::thread::JoinHandle<()>) -> Arc<dyn ExecutionThread> {
        let thread = Arc::new(StandaloneThread(Mutex::new(Some(thread))));
        self.threads.lock().unwrap().push(thread.clone());
        thread
    }
}
