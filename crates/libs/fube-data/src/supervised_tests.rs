// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Public provider/stream contracts, with a private accounting probe instead of
//! manufacturing a native partition. The real public guard/process fence is
//! tested by fube-runtime; neither set of tests claims native Node execution.

use super::*;
use crate::Error;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Mutex,
    },
    time::Instant,
};

struct BlockedEvent {
    name: &'static str,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

#[derive(Default)]
struct Probe {
    active: AtomicUsize,
    limit: AtomicUsize,
    refused: AtomicUsize,
    reject: AtomicUsize,
    closed: AtomicBool,
    mode: AtomicUsize,
    events: Mutex<Vec<(&'static str, usize)>>,
    blocked: Mutex<Option<BlockedEvent>>,
}
impl Probe {
    fn event(&self, name: &'static str) {
        let active = self.active.load(Ordering::SeqCst);
        assert!(active > 0, "unaccounted application code: {name}");
        self.events.lock().unwrap().push((name, active));
        if let Some(blocked) = self.blocked.lock().unwrap().as_ref() {
            if blocked.name == name {
                blocked.entered.send(()).unwrap();
                blocked
                    .release
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }
    }
    fn count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}
#[derive(Clone)]
struct Accounting(Arc<Probe>);
struct Guard(Arc<Probe>);
impl Supervision for Accounting {
    type Guard = Guard;
    fn arm(&self) -> Result<Guard> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if self
            .0
            .reject
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Error::ResourceLimit("callback deadline slots"));
        }
        let limit = self.0.limit.load(Ordering::SeqCst);
        if self
            .0
            .active
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (limit == 0 || n < limit).then_some(n + 1)
            })
            .is_err()
        {
            self.0.refused.fetch_add(1, Ordering::SeqCst);
            return Err(Error::ResourceLimit("callback deadline slots"));
        }
        Ok(Guard(self.0.clone()))
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        assert!(self.0.active.fetch_sub(1, Ordering::SeqCst) > 0);
    }
}

// Logical milliseconds avoid sleeps across many callback budgets. Runtime
// separately tests the independent OS watchdog and terminal authority.
const CALLBACK_TIMEOUT_MS: usize = 8_000;
#[derive(Clone)]
struct DeadlineAccounting {
    accounting: Accounting,
    now: Arc<AtomicUsize>,
    retired: Arc<AtomicBool>,
}
struct DeadlineGuard {
    _guard: Guard,
    now: Arc<AtomicUsize>,
    retired: Arc<AtomicBool>,
    deadline: usize,
}
impl Supervision for DeadlineAccounting {
    type Guard = DeadlineGuard;
    fn arm(&self) -> Result<Self::Guard> {
        Ok(DeadlineGuard {
            _guard: self.accounting.arm()?,
            now: self.now.clone(),
            retired: self.retired.clone(),
            deadline: self.now.load(Ordering::SeqCst) + CALLBACK_TIMEOUT_MS,
        })
    }
}
impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        if self.now.load(Ordering::SeqCst) >= self.deadline {
            self.retired.store(true, Ordering::SeqCst);
        }
    }
}

struct Provider(Arc<Probe>);
impl Drop for Provider {
    fn drop(&mut self) {
        self.0.event("provider-drop");
        if self.0.mode.load(Ordering::SeqCst) == 5 {
            panic!("controlled provider destructor panic");
        }
    }
}
struct Stream(Arc<Probe>);
impl Drop for Stream {
    fn drop(&mut self) {
        self.0.event("stream-drop");
        if self.0.mode.load(Ordering::SeqCst) == 4 {
            panic!("controlled stream destructor panic");
        }
    }
}

struct Callback<T> {
    probe: Arc<Probe>,
    output: Option<T>,
}
impl<T: Unpin> Future for Callback<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.probe.event("poll");
        match self.probe.mode.load(Ordering::SeqCst) {
            1 => Poll::Pending,
            2 => panic!("controlled poll panic"),
            _ => Poll::Ready(Ok(self.output.take().unwrap())),
        }
    }
}
impl<T> Drop for Callback<T> {
    fn drop(&mut self) {
        self.probe.event("future-drop");
        if self.probe.mode.load(Ordering::SeqCst) == 6 {
            panic!("controlled future destructor panic");
        }
    }
}
impl ReplayCheckpointProvider for Provider {
    fn replication_checkpoint(
        &self,
        _: i64,
        _: Cancellation,
    ) -> ServiceFuture<'_, Option<ReplayOperation>> {
        self.0.event("checkpoint-constructor");
        if self.0.mode.load(Ordering::SeqCst) == 3 {
            panic!("controlled checkpoint constructor panic");
        }
        Box::pin(Callback {
            probe: self.0.clone(),
            output: Some(None),
        })
    }
}
impl IStateProvider for Provider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.0.event("last");
        Ok(0)
    }
    fn update_epoch(&self, _: Epoch, _: i64, _: Cancellation) -> ServiceFuture<'_, ()> {
        self.0.event("update-constructor");
        if self.0.mode.load(Ordering::SeqCst) == 3 {
            panic!("controlled constructor panic");
        }
        Box::pin(Callback {
            probe: self.0.clone(),
            output: Some(()),
        })
    }
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        self.0.event("loss-constructor");
        Box::pin(Callback {
            probe: self.0.clone(),
            output: Some(false),
        })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        self.0.event("context");
        let stream: Arc<dyn IOperationDataStream> = Arc::new(Stream(self.0.clone()));
        match self.0.mode.load(Ordering::SeqCst) {
            7 => panic!("controlled stream creation panic"),
            8 => Err(Error::Closed),
            _ => Ok(stream),
        }
    }
    fn get_copy_state(
        &self,
        _: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        self.0.event("state");
        let stream: Arc<dyn IOperationDataStream> = Arc::new(Stream(self.0.clone()));
        let result = match self.0.mode.load(Ordering::SeqCst) {
            7 => panic!("controlled stream creation panic"),
            8 => Err(Error::Closed),
            _ => Ok(stream),
        };
        drop(context);
        result
    }
}
impl IOperationDataStream for Stream {
    fn get_next(&self, _: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        self.0.event("next-constructor");
        if self.0.mode.load(Ordering::SeqCst) == 3 {
            panic!("controlled next constructor panic");
        }
        Box::pin(Callback {
            probe: self.0.clone(),
            output: Some(if self.0.mode.load(Ordering::SeqCst) == 9 {
                Some(OperationData::new(vec![vec![42]]).unwrap())
            } else {
                None
            }),
        })
    }
}

fn provider() -> (SupervisedProvider<Accounting>, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let provider = Arc::new(Provider(probe.clone()));
    (
        SupervisedProvider {
            inner: Some(provider.clone()),
            replay: Some(provider),
            supervision: Accounting(probe.clone()),
        },
        probe,
    )
}

fn poll<T>(future: &mut ServiceFuture<'_, T>) -> Poll<Result<T>> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn all_callbacks_enroll_before_construction_and_hold_through_actual_future_disposal() {
    let (provider, probe) = provider();
    assert_eq!(provider.get_last_committed_sequence_number().unwrap(), 0);
    assert_eq!(probe.count(), 0);
    let mut epoch = provider.update_epoch(Epoch::INVALID, 0, Cancellation::default());
    assert_eq!(probe.count(), 1);
    assert!(matches!(poll(&mut epoch), Poll::Ready(Ok(()))));
    // Completion itself does not release accounting for retained captures.
    assert_eq!(probe.count(), 1);
    drop(epoch);
    assert_eq!(probe.count(), 0);
    let unpolled = provider.replication_checkpoint(1, Cancellation::default());
    assert_eq!(probe.count(), 1);
    drop(unpolled);
    assert_eq!(probe.count(), 0);
    let mut checkpoint = provider.replication_checkpoint(1, Cancellation::default());
    assert!(matches!(poll(&mut checkpoint), Poll::Ready(Ok(None))));
    assert_eq!(probe.count(), 1);
    drop(checkpoint);
    assert_eq!(probe.count(), 0);
    let unpolled = provider.on_data_loss(Cancellation::default());
    drop(unpolled);
    assert_eq!(probe.count(), 0);
    let mut loss = provider.on_data_loss(Cancellation::default());
    assert!(matches!(poll(&mut loss), Poll::Ready(Ok(false))));
    drop(loss);
    drop(provider);
    assert_eq!(probe.count(), 0);
    assert!(probe.events.lock().unwrap().contains(&("provider-drop", 1)));
}

#[test]
fn returned_enumerators_are_idle_until_next_and_guard_final_arc_disposal() {
    let (provider, probe) = provider();
    for stream in [
        provider.get_copy_context().unwrap(),
        provider.get_copy_state(0, None).unwrap(),
    ] {
        let clone = stream.clone();
        assert_eq!(probe.count(), 0);
        let mut next = stream.get_next(Cancellation::default());
        assert_eq!(probe.count(), 1);
        assert!(matches!(poll(&mut next), Poll::Ready(Ok(None))));
        assert_eq!(probe.count(), 1);
        drop(next);
        assert_eq!(probe.count(), 0);
        drop(stream);
        assert_eq!(probe.count(), 0);
        drop(clone);
        assert_eq!(probe.count(), 0);
    }
    assert_eq!(probe.count(), 0);
    let stream = provider
        .get_copy_state(0, Some(Arc::new(Stream(probe.clone()))))
        .unwrap();
    drop(stream);
    drop(provider);
    assert_eq!(probe.count(), 0);
    let events = probe.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|(event, _)| *event == "stream-drop")
            .count(),
        4
    );
    assert!(events
        .iter()
        .filter(|(event, _)| *event == "next-constructor")
        .all(|(_, n)| *n == 1));
    assert!(events
        .iter()
        .filter(|(event, _)| *event == "stream-drop")
        .all(|(_, n)| *n == 1));
}

#[test]
fn progressing_nonempty_enumerators_can_outlive_many_callback_budgets_and_idle_intervals() {
    let probe = Arc::new(Probe::default());
    let supervision = DeadlineAccounting {
        accounting: Accounting(probe.clone()),
        now: Arc::default(),
        retired: Arc::default(),
    };
    let provider = SupervisedProvider {
        replay: None,
        inner: Some(Arc::new(Provider(probe.clone()))),
        supervision: supervision.clone(),
    };
    probe.mode.store(9, Ordering::SeqCst);
    for stream in [
        provider.get_copy_context().unwrap(),
        provider.get_copy_state(0, None).unwrap(),
    ] {
        assert_eq!(probe.count(), 0);
        for record in 0..1_000 {
            let mut next = stream.get_next(Cancellation::default());
            assert_eq!(probe.count(), 1);
            supervision.now.fetch_add(20, Ordering::SeqCst);
            assert!(matches!(poll(&mut next), Poll::Ready(Ok(Some(_)))));
            assert_eq!(probe.count(), 1);
            drop(next);
            assert_eq!(probe.count(), 0);
            if record % 100 == 0 {
                supervision
                    .now
                    .fetch_add(CALLBACK_TIMEOUT_MS + 1, Ordering::SeqCst);
            }
            assert!(!supervision.retired.load(Ordering::SeqCst));
        }
        drop(stream);
        assert_eq!(probe.count(), 0);
        assert!(!supervision.retired.load(Ordering::SeqCst));
    }
    drop(provider);
    assert_eq!(probe.count(), 0);
    assert!(!supervision.retired.load(Ordering::SeqCst));
}

#[test]
fn blocked_next_construction_poll_and_actual_destructors_each_hold_one_guard() {
    for name in ["next-constructor", "poll", "future-drop", "stream-drop"] {
        let (provider, probe) = provider();
        let stream = provider.get_copy_state(0, None).unwrap();
        assert_eq!(probe.count(), 0);
        let (entered, waiting) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        *probe.blocked.lock().unwrap() = Some(BlockedEvent {
            name,
            entered,
            release: blocked,
        });
        let worker = std::thread::spawn(move || {
            let mut next = stream.get_next(Cancellation::default());
            assert!(matches!(poll(&mut next), Poll::Ready(Ok(None))));
            drop(next);
            drop(stream);
        });
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(probe.count(), 1, "{name}");
        assert_eq!(probe.events.lock().unwrap().last(), Some(&(name, 1)));
        release.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(probe.count(), 0, "{name}");
        drop(provider);
        assert_eq!(probe.count(), 0);
    }
}

#[test]
fn cancellation_and_abandoned_unpolled_stream_future_do_not_disarm_early() {
    let (provider, probe) = provider();
    let cancellation = Cancellation::default();
    probe.mode.store(1, Ordering::SeqCst);
    let mut callback = provider.update_epoch(Epoch::INVALID, 0, cancellation.clone());
    assert!(poll(&mut callback).is_pending());
    cancellation.cancel();
    assert!(poll(&mut callback).is_pending());
    assert_eq!(probe.count(), 1);
    drop(callback);
    assert_eq!(probe.count(), 0);
    let stream = provider.get_copy_context().unwrap();
    assert_eq!(probe.count(), 0);
    let cancellation = Cancellation::default();
    let mut next = stream.get_next(cancellation.clone());
    assert!(poll(&mut next).is_pending());
    cancellation.cancel();
    assert!(poll(&mut next).is_pending());
    assert_eq!(probe.count(), 1);
    assert!(!probe
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|(event, _)| *event == "stream-drop"));
    assert_eq!(
        probe
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, _)| *event == "future-drop")
            .count(),
        1
    );
    drop(next);
    assert_eq!(probe.count(), 0);
    let unpolled = stream.get_next(cancellation);
    assert_eq!(probe.count(), 1);
    drop(unpolled);
    assert_eq!(probe.count(), 0);
    drop(stream);
    drop(provider);
    assert_eq!(probe.count(), 0);
}

#[test]
fn constructor_poll_and_stream_destructor_unwind_keep_capture_destruction_accounted() {
    let (provider, probe) = provider();
    probe.mode.store(3, Ordering::SeqCst);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(provider.update_epoch(Epoch::INVALID, 0, Cancellation::default()));
    }))
    .is_err());
    assert_eq!(probe.count(), 0);
    probe.mode.store(2, Ordering::SeqCst);
    let mut callback = provider.on_data_loss(Cancellation::default());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll(&mut callback))).is_err()
    );
    assert_eq!(probe.count(), 1);
    drop(callback);
    assert_eq!(probe.count(), 0);
    probe.mode.store(3, Ordering::SeqCst);
    let stream = provider.get_copy_context().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(stream.get_next(Cancellation::default()));
    }))
    .is_err());
    assert_eq!(probe.count(), 0);
    probe.mode.store(2, Ordering::SeqCst);
    let mut next = stream.get_next(Cancellation::default());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll(&mut next))).is_err());
    assert_eq!(probe.count(), 1);
    drop(next);
    assert_eq!(probe.count(), 0);
    probe.mode.store(6, Ordering::SeqCst);
    let callback = stream.get_next(Cancellation::default());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(callback))).is_err());
    assert_eq!(probe.count(), 0);
    drop(stream);
    assert_eq!(probe.count(), 0);
    probe.mode.store(4, Ordering::SeqCst);
    let stream = provider.get_copy_context().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(stream))).is_err());
    assert_eq!(probe.count(), 0);
    probe.mode.store(5, Ordering::SeqCst);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(provider))).is_err());
    assert_eq!(probe.count(), 0);
}

#[test]
fn stream_creation_panic_and_error_dispose_results_and_context_under_creation_guard() {
    for with_context in [false, true] {
        for mode in [7, 8] {
            let (provider, probe) = provider();
            probe.mode.store(mode, Ordering::SeqCst);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if with_context {
                    provider.get_copy_state(0, Some(Arc::new(Stream(probe.clone()))))
                } else {
                    provider.get_copy_context()
                }
            }));
            if mode == 7 {
                assert!(result.is_err());
            } else {
                assert!(matches!(result, Ok(Err(Error::Closed))));
            }
            assert_eq!(probe.count(), 0);
            drop(provider);
            assert_eq!(probe.count(), 0);
            let events = probe.events.lock().unwrap();
            assert!(events.iter().all(|(_, n)| *n == 1));
            assert_eq!(
                events
                    .iter()
                    .filter(|(event, _)| *event == "stream-drop")
                    .count(),
                if with_context { 2 } else { 1 }
            );
        }
    }
}

#[test]
fn disarming_rejection_uses_existing_outer_runtime_cleanup_ownership() {
    let (provider, probe) = provider();
    let stream = provider.get_copy_context().unwrap();
    assert_eq!(probe.count(), 0);
    // Native Runtime joins Engine shutdown/final participant destruction under
    // its own guard before disarming. A rejected nested guard must not pretend
    // to grant fresh ownership or access.
    let outer = provider.supervision.arm().unwrap();
    probe.closed.store(true, Ordering::SeqCst);
    assert!(matches!(
        provider.get_last_committed_sequence_number(),
        Err(Error::Closed)
    ));
    assert!(provider
        .get_copy_state(0, Some(Arc::new(Stream(probe.clone()))))
        .is_err());
    let mut next = stream.get_next(Cancellation::default());
    assert!(matches!(poll(&mut next), Poll::Ready(Err(Error::Closed))));
    drop(next);
    drop(stream);
    drop(provider);
    assert_eq!(probe.count(), 1);
    assert_eq!(
        *probe.events.lock().unwrap(),
        vec![
            ("context", 1),
            ("stream-drop", 1),
            ("stream-drop", 1),
            ("provider-drop", 1)
        ]
    );
    drop(outer);
    assert_eq!(probe.count(), 0);
}

#[test]
fn admission_failure_never_invokes_user_code_and_rejected_capture_cleanup_waits_for_a_slot() {
    let (provider, probe) = provider();
    probe.reject.store(1, Ordering::SeqCst);
    assert!(matches!(
        provider.get_last_committed_sequence_number(),
        Err(Error::ResourceLimit(_))
    ));
    probe.reject.store(1, Ordering::SeqCst);
    let mut callback = provider.update_epoch(Epoch::INVALID, 0, Cancellation::default());
    assert!(matches!(
        poll(&mut callback),
        Poll::Ready(Err(Error::ResourceLimit(_)))
    ));
    drop(callback);
    probe.reject.store(1, Ordering::SeqCst);
    let mut checkpoint = provider.replication_checkpoint(1, Cancellation::default());
    assert!(matches!(
        poll(&mut checkpoint),
        Poll::Ready(Err(Error::ResourceLimit(_)))
    ));
    drop(checkpoint);
    probe.reject.store(1, Ordering::SeqCst);
    assert!(provider.get_copy_context().is_err());
    assert!(probe.events.lock().unwrap().is_empty());
    probe.reject.store(2, Ordering::SeqCst);
    assert!(provider
        .get_copy_state(0, Some(Arc::new(Stream(probe.clone()))))
        .is_err());
    assert_eq!(*probe.events.lock().unwrap(), vec![("stream-drop", 1)]);
    probe.reject.store(1, Ordering::SeqCst);
    drop(provider);
    assert_eq!(probe.count(), 0);
    assert_eq!(
        *probe.events.lock().unwrap(),
        vec![("stream-drop", 1), ("provider-drop", 1)]
    );
}

#[test]
fn exhausted_slots_reject_next_without_user_code_and_final_stream_cleanup_waits_for_an_owner() {
    let (provider, probe) = provider();
    let stream = provider.get_copy_context().unwrap();
    probe.limit.store(1, Ordering::SeqCst);
    let owner = provider.supervision.arm().unwrap();
    let mut next = stream.get_next(Cancellation::default());
    assert!(matches!(
        poll(&mut next),
        Poll::Ready(Err(Error::ResourceLimit(_)))
    ));
    drop(next);
    assert_eq!(probe.count(), 1);
    assert_eq!(*probe.events.lock().unwrap(), vec![("context", 1)]);
    let refusals = probe.refused.load(Ordering::SeqCst);
    let worker = std::thread::spawn(move || drop(stream));
    let started = Instant::now();
    while probe.refused.load(Ordering::SeqCst) == refusals {
        assert!(started.elapsed() < Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(probe.count(), 1);
    assert_eq!(*probe.events.lock().unwrap(), vec![("context", 1)]);
    drop(owner);
    worker.join().unwrap();
    assert_eq!(probe.count(), 0);
    drop(provider);
    assert_eq!(probe.count(), 0);
    assert_eq!(
        *probe.events.lock().unwrap(),
        vec![("context", 1), ("stream-drop", 1), ("provider-drop", 1)]
    );
}
