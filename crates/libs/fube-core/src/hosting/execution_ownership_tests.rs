use super::*;
use std::sync::mpsc as std_mpsc;

fn runtime() -> ExecutionRuntime {
    ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap()
}

#[tokio::test]
async fn finite_provider_dormant_object_terminal_and_cleanup_reservations_cannot_grow_backlogs() {
    let runtime = runtime();
    let mut held = Vec::new();
    for _ in 0..64 {
        held.push(
            runtime
                .reserve(
                    CallLane::Provider,
                    CallOrigin::default(),
                    Duration::from_millis(10),
                    false,
                )
                .unwrap(),
        );
    }
    assert!(runtime.try_reserve().is_err());
    assert!(runtime
        .reserve(
            CallLane::Provider,
            CallOrigin::default(),
            Duration::from_secs(30),
            false
        )
        .is_err());
    tokio::time::sleep(Duration::from_millis(25)).await;
    runtime.inner.authority.check().unwrap(); // unused reservation is unarmed
    drop(held);
    for quota in [
        runtime.factory_quota(),
        runtime.service_quota(),
        runtime.control_quota(),
    ] {
        let held: Vec<_> = (0..32)
            .map(|_| {
                runtime
                    .reserve_object::<usize>(&quota, &DisposalTracker::default())
                    .unwrap()
            })
            .collect();
        assert!(runtime
            .reserve_object::<usize>(&quota, &DisposalTracker::default())
            .is_err());
        drop(held);
    }
    let mut objects = Vec::new();
    let mut quota = ObjectQuota::new(64).unwrap();
    for n in 0..DORMANT_TICKETS {
        if n % 64 == 0 {
            quota = ObjectQuota::new(64).unwrap();
        }
        objects.push(
            runtime
                .reserve_object::<usize>(&quota, &DisposalTracker::default())
                .unwrap(),
        );
    }
    assert!(runtime
        .reserve_object::<usize>(&ObjectQuota::new(1).unwrap(), &DisposalTracker::default())
        .is_err());
    assert_eq!(runtime.inner.outstanding.available_permits(), 64);
    drop(objects);
    let terminal: Vec<_> = (0..32)
        .map(|_| runtime.reserve_terminal(CallOrigin::default()).unwrap())
        .collect();
    assert!(runtime.reserve_terminal(CallOrigin::default()).is_err());
    drop(terminal);
    let owner = runtime.clone();
    runtime
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let cleanup = owner.reserve_cleanup(Duration::from_millis(250))?;
            assert!(owner.reserve_cleanup(Duration::from_millis(250)).is_err());
            drop(cleanup);
            let mut terminal = owner.reserve_terminal(CallOrigin::default())?;
            let record = terminal.begin()?;
            let cleanup = record.cleanup_after_callbacks(Duration::from_millis(250))?;
            assert!(record
                .cleanup_after_callbacks(Duration::from_millis(250))
                .is_err());
            drop((cleanup, record));
            Ok(())
        })
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    assert!(runtime.try_reserve().is_err());
    assert!(runtime
        .reserve(
            CallLane::Native,
            CallOrigin::default(),
            Duration::from_secs(30),
            true
        )
        .is_err());
    assert!(runtime.reserve_cleanup(Duration::from_millis(250)).is_err());
    assert!(runtime.reserve_terminal(CallOrigin::default()).is_err());
    assert!(runtime
        .reserve_object::<usize>(&ObjectQuota::new(1).unwrap(), &DisposalTracker::default())
        .is_err());
}

#[tokio::test]
async fn late_unaccepted_constructor_result_cannot_publish_after_original_deadline() {
    let runtime = runtime();
    let owner = runtime.clone();
    let tracker = DisposalTracker::default();
    let tracked = tracker.clone();
    let result = runtime
        .reserve(
            CallLane::Provider,
            CallOrigin::default(),
            Duration::from_millis(50),
            false,
        )
        .unwrap()
        .run_owned(move || {
            Ok(owner
                .reserve_construction(&ObjectQuota::new(1)?, &tracked)?
                .construct(42usize))
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert!(result.publication().accept().is_err());
    drop(result);
    tracker.drained().await;
    assert!(runtime.inner.authority.check().is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn constructor_result_keeps_original_record_until_owner_accepts_then_is_dormant() {
    let runtime = runtime();
    let owner = runtime.clone();
    let quota = ObjectQuota::new(1).unwrap();
    let tracker = DisposalTracker::default();
    let tracker_in = tracker.clone();
    let call = runtime
        .reserve(
            CallLane::Provider,
            CallOrigin {
                engine: 7,
                session: 11,
                generation: 13,
            },
            Duration::from_millis(150),
            false,
        )
        .unwrap()
        .run_owned(move || {
            let ticket = owner.reserve_construction(&quota, &tracker_in)?;
            Ok(ticket.construct(42usize))
        });
    let disposition = call.disposition();
    let object = call.await.unwrap();
    assert!(!disposition.is_finished());
    assert_eq!(runtime.inner.outstanding.available_permits(), 63);
    object.publication().accept().unwrap();
    disposition.wait().await;
    tokio::time::sleep(Duration::from_millis(175)).await;
    runtime.inner.authority.check().unwrap();
    assert_eq!(*object.get(), 42);
    assert_eq!(runtime.inner.outstanding.available_permits(), 64);
    drop(object);
    tracker.drained().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_result_disposal_retains_constructor_record_and_origin_through_actual_drop() {
    let runtime = runtime();
    let owner = runtime.clone();
    let quota = ObjectQuota::new(1).unwrap();
    let tracker = DisposalTracker::default();
    let tracker_in = tracker.clone();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std_mpsc::sync_channel(1);
    struct Value(
        Option<tokio::sync::oneshot::Sender<CallOrigin>>,
        std_mpsc::Receiver<()>,
    );
    impl Drop for Value {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
            let origin = ExecutionRuntime::current_origin().unwrap();
            assert!(ExecutionRuntime::check_entry(Some(origin.engine)).is_err());
            self.0.take().unwrap().send(origin).unwrap();
            self.1.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    }
    let origin = CallOrigin {
        engine: 8,
        session: 12,
        generation: 14,
    };
    let call = runtime
        .reserve(CallLane::Provider, origin, Duration::from_secs(2), false)
        .unwrap()
        .run_owned(move || {
            let ticket = owner.reserve_construction(&quota, &tracker_in)?;
            Ok(ticket.construct(Value(Some(entered), release_rx)))
        });
    let disposition = call.disposition();
    let object = call.await.unwrap();
    drop(object);
    assert_eq!(entered_rx.await.unwrap(), origin);
    assert!(!disposition.is_finished());
    assert_eq!(runtime.inner.outstanding.available_permits(), 63);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), disposition.wait())
        .await
        .unwrap();
    tracker.drained().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn existing_parent_cleanup_uses_reserved_route_at_full_shared64() {
    let runtime = runtime();
    let quota = ObjectQuota::new(1).unwrap();
    let tracker = DisposalTracker::default();
    let ticket = runtime.reserve_object(&quota, &tracker).unwrap();
    let (dropped, drop_rx) = tokio::sync::oneshot::channel();
    struct Value(Option<tokio::sync::oneshot::Sender<String>>);
    impl Drop for Value {
        fn drop(&mut self) {
            let _ = self
                .0
                .take()
                .unwrap()
                .send(std::thread::current().name().unwrap().into());
        }
    }
    let object = ticket.publish(Value(Some(dropped)));
    let mut held = Vec::new();
    for _ in 0..31 {
        held.push(runtime.try_reserve().unwrap());
    }
    for _ in 0..32 {
        held.push(
            runtime
                .reserve(
                    CallLane::Provider,
                    CallOrigin::default(),
                    Duration::from_secs(30),
                    false,
                )
                .unwrap(),
        );
    }
    let owner = runtime.clone();
    runtime
        .try_reserve()
        .unwrap()
        .run_future(async move {
            assert_eq!(owner.inner.outstanding.available_permits(), 0);
            let receipt = object.receipt();
            drop(object);
            receipt.wait().await
        })
        .await
        .unwrap();
    assert_eq!(drop_rx.await.unwrap(), "fube-owned-cleanup");
    drop(held);
    tracker.drained().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_retains_terminal_join_and_cleanup_leaves_without_cleanup_worker_join_wait() {
    let runtime = runtime();
    let terminal = runtime.reserve_terminal(CallOrigin::default()).unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std_mpsc::sync_channel(1);
    let (leaf_tx, leaf_rx) = tokio::sync::oneshot::channel::<DisposalReceipt>();
    let (ready, ready_rx) = std_mpsc::sync_channel(1);
    let observer = tokio::spawn(async move {
        leaf_rx.await.unwrap().wait().await.unwrap();
        ready.send(()).unwrap();
    });
    struct Value(
        Option<tokio::sync::oneshot::Sender<()>>,
        std_mpsc::Receiver<()>,
    );
    impl Drop for Value {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
            self.0.take().unwrap().send(()).unwrap();
            self.1.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    }
    let call = runtime.try_reserve().unwrap().run_future(async move {
        let mut terminal = terminal;
        let record = terminal.begin()?;
        let cleanup = record.cleanup_after_callbacks(Duration::from_millis(250))?;
        let leaf = cleanup.dispose(Value(Some(entered), release_rx));
        leaf_tx.send(leaf).ok().unwrap();
        let joined = record.run(move || {
            assert_eq!(std::thread::current().name(), Some("fube-terminal-join"));
            ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            Ok(())
        });
        joined.wait().await
    });
    let disposition = call.disposition();
    entered_rx.await.unwrap();
    drop(call);
    assert!(!disposition.is_finished());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), disposition.wait())
        .await
        .unwrap();
    observer.await.unwrap();
    assert_eq!(runtime.inner.terminal_tickets.available_permits(), 32);
    runtime.shutdown().await.unwrap();
}
