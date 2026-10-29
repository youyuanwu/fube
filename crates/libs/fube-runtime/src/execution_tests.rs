#[tokio::test]
async fn every_sibling_and_nested_release_acknowledges_its_own_tracker() {
    let runtime = runtime();
    for shared_tracker in [false, true] {
        let first = DisposalTracker::default();
        let second = if shared_tracker {
            first.clone()
        } else {
            DisposalTracker::default()
        };
        let quota = ObjectQuota::new(2).unwrap();
        let a = runtime.reserve_object(&quota, &first).unwrap();
        let b = runtime.reserve_object(&quota, &second).unwrap();
        let (entered, mut entered_rx) = tokio::sync::mpsc::channel(2);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        struct Value(tokio::sync::mpsc::Sender<()>, Arc<(Mutex<bool>, Condvar)>);
        impl Drop for Value {
            fn drop(&mut self) {
                self.0.try_send(()).unwrap();
                let ready = self.1 .0.lock().unwrap();
                let (ready, _) = self
                    .1
                     .1
                    .wait_timeout_while(ready, Duration::from_secs(2), |v| !*v)
                    .unwrap();
                assert!(*ready);
            }
        }
        let released = release.clone();
        let call = runtime.try_reserve().unwrap().run_owned(move || {
            let a = a.publish(Value(entered.clone(), released.clone()));
            let b = b.publish(Value(entered, released));
            drop((a, b));
            Ok(())
        });
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        call.await.unwrap();
        assert!(first.is_drained());
        assert!(second.is_drained());
    }
    let outer_tracker = DisposalTracker::default();
    let inner_tracker = DisposalTracker::default();
    let quota = ObjectQuota::new(2).unwrap();
    let outer = runtime.reserve_object(&quota, &outer_tracker).unwrap();
    let inner = runtime.reserve_object(&quota, &inner_tracker).unwrap();
    let call = runtime.try_reserve().unwrap().run_owned(move || {
        drop(outer.publish(inner.publish(5usize)));
        Ok(())
    });
    call.await.unwrap();
    assert!(outer_tracker.is_drained() && inner_tracker.is_drained());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_between_result_check_and_commit_disposes_result_before_finished() {
    let runtime = runtime();
    let (release, resume) = mpsc::sync_channel(1);
    let pause = Arc::new(CleanupPause {
        entered: Notify::new(),
        resume: Mutex::new(resume),
    });
    *runtime.inner.cleanup_pause.lock().unwrap() = Some(pause.clone());
    let (dropped, drop_rx) = tokio::sync::oneshot::channel();
    struct Value(Option<tokio::sync::oneshot::Sender<String>>);
    impl Drop for Value {
        fn drop(&mut self) {
            let _ = self
                .0
                .take()
                .unwrap()
                .send(std::thread::current().name().unwrap().to_owned());
        }
    }
    let call = runtime
        .try_reserve()
        .unwrap()
        .run_owned(move || Ok(Value(Some(dropped))));
    tokio::time::timeout(Duration::from_secs(1), pause.entered.notified())
        .await
        .unwrap();
    let task = call.task.clone();
    task.schedule(true);
    assert!(task.state.lock().unwrap().canceled);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), call.cancel_and_wait())
        .await
        .unwrap();
    assert_eq!(drop_rx.await.unwrap(), "fube-owned-cleanup");
    let state = task.state.lock().unwrap();
    assert!(state.finished && state.result.is_none());
    drop(state);
    drop(task);
    runtime.shutdown().await.unwrap();
}

use super::*;
use std::sync::{atomic::AtomicUsize, mpsc};

fn runtime() -> ExecutionRuntime {
    ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap()
}

#[tokio::test]
async fn capture_free_reservation_shares_64_slots_and_unused_permit_has_no_user_value() {
    let runtime = runtime();
    let mut permits = Vec::new();
    for lane in [CallLane::Native, CallLane::Provider] {
        for _ in 0..32 {
            permits.push(
                runtime
                    .reserve(lane, CallOrigin::default(), Duration::from_secs(30), false)
                    .unwrap(),
            );
        }
    }
    let caller_value = Box::new(73);
    assert!(matches!(
        runtime.try_reserve(),
        Err(Error::ResourceLimit(_))
    ));
    assert!(matches!(
        runtime.reserve(
            CallLane::Provider,
            CallOrigin::default(),
            Duration::from_secs(30),
            false
        ),
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(*caller_value, 73);
    assert_eq!(runtime.inner.outstanding.available_permits(), 0);
    drop(permits);
    assert_eq!(runtime.inner.outstanding.available_permits(), 64);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn accepted_root_construction_poll_and_capture_drop_leave_observer_executor() {
    let runtime = runtime();
    let (dropped, drop_rx) = mpsc::sync_channel(1);
    struct Capture(mpsc::SyncSender<String>);
    impl Drop for Capture {
        fn drop(&mut self) {
            self.0
                .send(std::thread::current().name().unwrap().to_owned())
                .unwrap();
        }
    }
    let capture = Capture(dropped);
    let call = runtime.try_reserve().unwrap().run_future(async move {
        assert_eq!(std::thread::current().name(), Some("fube-native-call"));
        let _capture = capture;
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(std::thread::current().name(), Some("fube-native-call"));
        Ok(17)
    });
    assert_eq!(call.await.unwrap(), 17);
    // The Rust async future may destroy a completed local on its final poll;
    // this is still the accepted blocking lane under the active root guard.
    let thread = drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(thread == "fube-native-call" || thread == "fube-owned-cleanup");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn eager_unobserved_result_is_disposed_on_reserved_cleanup() {
    let runtime = runtime();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (dropped, mut dropped_rx) = tokio::sync::oneshot::channel();
    struct Value(Option<tokio::sync::oneshot::Sender<String>>);
    impl Drop for Value {
        fn drop(&mut self) {
            let _ = self
                .0
                .take()
                .unwrap()
                .send(std::thread::current().name().unwrap().to_owned());
        }
    }
    let call = runtime.try_reserve().unwrap().run_owned(move || {
        entered.send(()).unwrap();
        Ok(Value(Some(dropped)))
    });
    entered_rx.await.unwrap(); // Submission happened without polling `call`.
    drop(call);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), &mut dropped_rx)
            .await
            .unwrap()
            .unwrap(),
        "fube-owned-cleanup"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let notified = runtime.inner.changed.notified();
            if runtime.inner.outstanding.available_permits() == 64 {
                break;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn coalesced_wakes_never_poll_the_pinned_root_concurrently() {
    let runtime = runtime();
    struct Repeated {
        polls: usize,
        active: Arc<AtomicUsize>,
    }
    impl Future for Repeated {
        type Output = Result<usize>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
            self.polls += 1;
            for _ in 0..100 {
                cx.waker().wake_by_ref();
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            if self.polls == 100 {
                Poll::Ready(Ok(self.polls))
            } else {
                Poll::Pending
            }
        }
    }
    let call = runtime.try_reserve().unwrap().run_future(Repeated {
        polls: 0,
        active: Arc::new(AtomicUsize::new(0)),
    });
    assert_eq!(call.await.unwrap(), 100);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implicit_application_spawn_is_not_a_lane_or_internal_core_task() {
    let runtime = runtime();
    let value = runtime
        .try_reserve()
        .unwrap()
        .run_future(async {
            let thread = tokio::spawn(async {
                std::thread::current()
                    .name()
                    .unwrap_or("application")
                    .to_owned()
            })
            .await
            .unwrap();
            assert_ne!(thread, "fube-native-call");
            assert_ne!(thread, "fube-host-core");
            assert_ne!(thread, "fube-engine");
            assert_ne!(thread, "fube-peer");
            Ok(())
        })
        .await;
    value.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn published_object_is_dormant_and_final_disposal_has_a_separate_budget() {
    let runtime = runtime();
    let tracker = DisposalTracker::default();
    let quota = ObjectQuota::new(1).unwrap();
    let ticket = runtime.reserve_object(&quota, &tracker).unwrap();
    let object = runtime
        .reserve(
            CallLane::Native,
            CallOrigin::default(),
            Duration::from_millis(50),
            false,
        )
        .unwrap()
        .run_owned(move || Ok(ticket.publish(41usize)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    runtime.inner.authority.check().unwrap();
    assert_eq!(runtime.inner.outstanding.available_permits(), 64);
    assert_eq!(*object.get(), 41);
    assert_eq!(
        runtime.inner.dormant.available_permits(),
        DORMANT_TICKETS - 1
    );
    drop(object);
    tracker.drained().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn root_result_waits_for_its_managed_child_actual_destructor() {
    let runtime = runtime();
    let tracker = DisposalTracker::default();
    let quota = ObjectQuota::new(1).unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = mpsc::sync_channel(1);
    struct Value(Option<tokio::sync::oneshot::Sender<()>>, mpsc::Receiver<()>);
    impl Drop for Value {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
            self.0.take().unwrap().send(()).unwrap();
            self.1.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    }
    let ticket = runtime.reserve_object(&quota, &tracker).unwrap();
    let mut call = runtime.try_reserve().unwrap().run_owned(move || {
        let object = ticket.publish(Value(Some(entered), release_rx));
        drop(object);
        Ok(())
    });
    entered_rx.await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut call)
        .await
        .is_err());
    release.send(()).unwrap();
    call.await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_native_workers_do_not_occupy_provider_or_reserved_cleanup_workers() {
    let runtime = runtime();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered, mut entered_rx) = tokio::sync::mpsc::channel(32);
    let mut native = Vec::new();
    for _ in 0..32 {
        let release = release.clone();
        let entered = entered.clone();
        native.push(runtime.try_reserve().unwrap().run_owned(move || {
            entered.try_send(()).unwrap();
            let (lock, changed) = &*release;
            let ready = lock.lock().unwrap();
            let (ready, timeout) = changed
                .wait_timeout_while(ready, Duration::from_secs(3), |ready| !*ready)
                .unwrap();
            assert!(*ready && !timeout.timed_out());
            Ok(())
        }));
    }
    for _ in 0..32 {
        tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let provider = runtime
        .reserve(
            CallLane::Provider,
            CallOrigin::default(),
            Duration::from_secs(30),
            false,
        )
        .unwrap()
        .run_owned(|| {
            assert_eq!(std::thread::current().name(), Some("fube-provider-call"));
            Ok(())
        });
    tokio::time::timeout(Duration::from_secs(1), provider)
        .await
        .unwrap()
        .unwrap();
    let tracker = DisposalTracker::default();
    let quota = ObjectQuota::new(1).unwrap();
    let ticket = runtime.reserve_object(&quota, &tracker).unwrap();
    let (dropped, drop_rx) = tokio::sync::oneshot::channel();
    struct Value(Option<tokio::sync::oneshot::Sender<String>>);
    impl Drop for Value {
        fn drop(&mut self) {
            let _ = self
                .0
                .take()
                .unwrap()
                .send(std::thread::current().name().unwrap().to_owned());
        }
    }
    drop(ticket.publish(Value(Some(dropped))));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), drop_rx)
            .await
            .unwrap()
            .unwrap(),
        "fube-owned-cleanup"
    );
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    for call in native {
        call.await.unwrap();
    }
    runtime.shutdown().await.unwrap();
}

#[test]
fn parentless_final_release_at_full64_is_fenced_without_inline_drop() {
    const CHILD: &str = "FUBE_EXECUTION_FULL64_RELEASE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let app = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime = ExecutionRuntime::standalone(app.handle().clone()).unwrap();
        runtime.inner.authority.watchdog().unwrap();
        let quota = ObjectQuota::new(1).unwrap();
        let tracker = DisposalTracker::default();
        struct MustRemainOwned;
        impl Drop for MustRemainOwned {
            fn drop(&mut self) {
                // This entire adversarial witness is confined to the child.
                unsafe { libc::_exit(84) }
            }
        }
        let object = runtime
            .reserve_object(&quota, &tracker)
            .unwrap()
            .publish(MustRemainOwned);
        let mut permits = Vec::new();
        for lane in [CallLane::Native, CallLane::Provider] {
            for _ in 0..32 {
                permits.push(
                    runtime
                        .reserve(lane, CallOrigin::default(), Duration::from_secs(30), false)
                        .unwrap(),
                );
            }
        }
        drop(object);
        assert_eq!(runtime.inner.outstanding.available_permits(), 0);
        assert_eq!(tracker.pending.load(Ordering::Acquire), 1);
        std::hint::black_box(&permits);
        loop {
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "execution::tests::parentless_final_release_at_full64_is_fenced_without_inline_drop",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(4);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(70));
            break;
        }
        if Instant::now() >= until {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("independent terminal fence failed within bounded child lifetime");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
