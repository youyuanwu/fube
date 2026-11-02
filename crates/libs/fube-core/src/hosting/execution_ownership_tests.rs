use super::*;
use std::sync::mpsc as std_mpsc;

fn runtime() -> ExecutionRuntime {
    ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap()
}

#[tokio::test]
async fn queued_second_callback_retains_absolute_deadline_and_cannot_refreeze_it() {
    let runtime = runtime();
    let deadline = Instant::now() + Duration::from_secs(1);
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let permit = runtime
            .reserve(
                CallLane::Provider,
                CallOrigin::default(),
                Duration::from_secs(2),
                false,
            )
            .unwrap()
            .with_deadline(deadline)
            .unwrap();
        let owner = runtime.clone();
        assert_eq!(
            permit
                .run_owned(move || owner.current_deadline())
                .await
                .unwrap(),
            deadline
        );
    }
    let permit = runtime
        .try_reserve()
        .unwrap()
        .with_deadline(deadline)
        .unwrap();
    assert!(permit
        .with_deadline(deadline + Duration::from_secs(1))
        .is_err());
    assert!(runtime
        .try_reserve()
        .unwrap()
        .with_deadline(Instant::now())
        .is_err());
    runtime.finish_standalone().await.unwrap();
}

#[tokio::test]
async fn distinct_engine_callback_uses_existing_provider_budget_owner_deadline_and_reentry_guard() {
    let runtime = runtime();
    let root = CallOrigin {
        engine: 11,
        session: 0,
        generation: 1,
    };
    let child = CallOrigin { engine: 22, ..root };
    let owner = runtime.clone();
    runtime
        .reserve(CallLane::Provider, root, Duration::from_secs(1), false)
        .unwrap()
        .run_future(async move {
            let deadline = owner.current_deadline()?;
            assert!(owner
                .reserve(CallLane::Provider, child, Duration::from_secs(1), false)
                .is_err());
            assert!(owner.reserve_engine_callback(root, deadline).is_err());
            let callback = owner.reserve_engine_callback(child, deadline)?;
            let callback_owner = owner.clone();
            callback
                .run_owned(move || {
                    assert_eq!(ExecutionRuntime::current_origin(), Some(child));
                    assert_eq!(callback_owner.current_deadline()?, deadline);
                    assert!(ExecutionRuntime::reject_engine_reentry(child.engine).is_err());
                    assert!(ExecutionRuntime::check_entry(Some(root.engine)).is_err());
                    assert!(ExecutionRuntime::reject_engine_reentry(root.engine).is_err());
                    assert!(callback_owner
                        .reserve_engine_callback(root, deadline)
                        .is_err());
                    assert!(callback_owner
                        .reserve_engine_callback(child, deadline)
                        .is_err());
                    Ok(())
                })
                .await
        })
        .await
        .unwrap();
    runtime.finish_standalone().await.unwrap();
}

#[tokio::test]
async fn callback_ancestry_survives_yields_publication_scopes_and_dropped_observer_cleanup() {
    struct Probe {
        owner: ExecutionRuntime,
        engines: Vec<u64>,
        dropped: Option<oneshot::Sender<()>>,
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(ExecutionRuntime::is_cleanup_entry());
            for engine in &self.engines {
                assert!(ExecutionRuntime::check_entry(Some(*engine)).is_err());
                assert!(ExecutionRuntime::reject_engine_reentry(*engine).is_err());
                assert!(self
                    .owner
                    .reserve_engine_callback(
                        CallOrigin {
                            engine: *engine,
                            generation: 1,
                            session: 0
                        },
                        self.owner.current_deadline().unwrap(),
                    )
                    .is_err());
            }
            self.dropped.take().unwrap().send(()).unwrap();
        }
    }
    let runtime = runtime();
    let foreign = self::runtime();
    let a = CallOrigin {
        engine: 11,
        generation: 1,
        session: 0,
    };
    let b = CallOrigin { engine: 22, ..a };
    let c = CallOrigin { engine: 33, ..a };
    let owner = runtime.clone();
    let foreign_owner = foreign.clone();
    let root_permit = runtime
        .reserve(CallLane::Provider, a, Duration::from_secs(2), false)
        .unwrap();
    let child_permit = runtime
        .reserve_engine_callback(b, Instant::now() + Duration::from_secs(2))
        .unwrap();
    let root = ExecutionRuntime::engine_publication(7, || {
        Ok(root_permit.run_future(async move {
            let deadline = owner.current_deadline()?;
            assert!(foreign_owner.reserve_engine_callback(b, deadline).is_err());
            let child_owner = owner.clone();
            child_permit
                .run_future(async move {
                    tokio::task::yield_now().await;
                    for engine in [7, a.engine, b.engine] {
                        assert!(ExecutionRuntime::check_entry(Some(engine)).is_err());
                        assert!(ExecutionRuntime::reject_engine_reentry(engine).is_err());
                        assert!(child_owner
                            .reserve_engine_callback(CallOrigin { engine, ..a }, deadline)
                            .is_err());
                    }
                    let grandchild_owner = child_owner.clone();
                    let tracker = DisposalTracker::default();
                    let (dropped, received) = oneshot::channel();
                    let (ready, completed) = oneshot::channel();
                    let observer = child_owner
                        .reserve_engine_callback(c, deadline)?
                        .run_future(async move {
                            tokio::task::yield_now().await;
                            assert_eq!(ExecutionRuntime::current_origin(), Some(c));
                            for engine in [7, a.engine, b.engine, c.engine] {
                                assert!(ExecutionRuntime::check_entry(Some(engine)).is_err());
                                assert!(ExecutionRuntime::reject_engine_reentry(engine).is_err());
                                assert!(grandchild_owner
                                    .reserve_engine_callback(CallOrigin { engine, ..a }, deadline)
                                    .is_err());
                            }
                            let object = grandchild_owner
                                .reserve_construction(&ObjectQuota::new(1)?, &tracker)?
                                .construct(Probe {
                                    owner: grandchild_owner.clone(),
                                    engines: vec![7, a.engine, b.engine, c.engine],
                                    dropped: Some(dropped),
                                });
                            ready.send(()).unwrap();
                            Ok(object)
                        });
                    completed.await.unwrap();
                    tokio::task::yield_now().await;
                    drop(observer);
                    received.await.unwrap();
                    Ok(())
                })
                .await
        }))
    })
    .unwrap();
    root.await.unwrap();
    runtime.finish_standalone().await.unwrap();
    foreign.finish_standalone().await.unwrap();
}

#[tokio::test]
async fn stale_permit_pending_cleanup_unions_construction_and_release_callback_ancestry() {
    struct Probe {
        owner: ExecutionRuntime,
        engines: Vec<u64>,
        deadline: Instant,
        dropped: Option<oneshot::Sender<()>>,
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(ExecutionRuntime::is_cleanup_entry());
            assert_eq!(self.owner.current_deadline().unwrap(), self.deadline);
            for engine in &self.engines {
                assert!(ExecutionRuntime::check_entry(Some(*engine)).is_err());
                assert!(ExecutionRuntime::reject_engine_reentry(*engine).is_err());
                assert!(self
                    .owner
                    .reserve_engine_callback(
                        CallOrigin {
                            engine: *engine,
                            generation: 1,
                            session: 0,
                        },
                        self.deadline,
                    )
                    .is_err());
            }
            self.dropped.take().unwrap().send(()).unwrap();
        }
    }
    struct Construction {
        owner: ExecutionRuntime,
        spare: Arc<OwnedCallPermit>,
        children: Vec<CallOrigin>,
        reservation: Option<ConstructionReservation<Probe>>,
        probe: Option<Probe>,
        object: Option<ManagedObject<Probe>>,
        release_outside: bool,
    }
    fn callbacks(mut construction: Construction) -> OwnedFuture<Option<ManagedObject<Probe>>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            if !construction.children.is_empty() {
                let child = construction.children.remove(0);
                return construction
                    .owner
                    .reserve_engine_callback(child, construction.owner.current_deadline()?)?
                    .run_future(callbacks(construction))
                    .await;
            }
            let object = match construction.object.take() {
                Some(object) => object,
                None => {
                    let reservation = match construction.reservation.take() {
                        Some(reservation) => reservation,
                        None => construction.spare.object_reservation(
                            &ObjectQuota::new(1)?,
                            &DisposalTracker::default(),
                        )?,
                    };
                    reservation.construct(construction.probe.take().unwrap())
                }
            };
            if construction.release_outside {
                return Ok(Some(object));
            }
            let receipt = object.receipt();
            drop(object);
            receipt.wait().await?;
            Ok(None)
        })
    }
    for depth in [2, 3] {
        for pattern in ["inside", "constructed-outside", "reserved-outside"] {
            let runtime = runtime();
            let deadline = Instant::now() + Duration::from_secs(2);
            let origin = |engine| CallOrigin {
                engine,
                generation: 1,
                session: 0,
            };
            let spare = Arc::new(
                runtime
                    .reserve_engine_callback(origin(99), deadline)
                    .unwrap(),
            );
            let root = runtime
                .reserve(
                    CallLane::Provider,
                    origin(11),
                    Duration::from_secs(3),
                    false,
                )
                .unwrap();
            let (dropped, received) = oneshot::channel();
            let probe = Probe {
                owner: runtime.clone(),
                engines: [
                    vec![7, 99, 11, 22],
                    if depth == 3 { vec![33] } else { vec![] },
                ]
                .concat(),
                deadline,
                dropped: Some(dropped),
            };
            let mut construction = Construction {
                owner: runtime.clone(),
                spare: spare.clone(),
                children: if depth == 3 {
                    vec![origin(22), origin(33)]
                } else {
                    vec![origin(22)]
                },
                reservation: None,
                probe: Some(probe),
                object: None,
                release_outside: pattern == "reserved-outside",
            };
            if pattern != "inside" {
                let reservation = spare
                    .object_reservation(&ObjectQuota::new(1).unwrap(), &DisposalTracker::default())
                    .unwrap();
                if pattern == "constructed-outside" {
                    construction.object =
                        Some(reservation.construct(construction.probe.take().unwrap()));
                } else {
                    construction.reservation = Some(reservation);
                }
            }
            // The publication tag exists only at scheduling, not on workers.
            let object = ExecutionRuntime::engine_publication(7, || {
                Ok(root.run_future(callbacks(construction)))
            })
            .unwrap()
            .await
            .unwrap();
            if let Some(object) = object {
                let receipt = object.receipt();
                drop(object);
                receipt.wait().await.unwrap();
            }
            received.await.unwrap();
            drop(spare);
            runtime.finish_standalone().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cleanup_ancestry_overflow_does_not_leak_owner_count_or_reservations() {
    fn publications<T>(engines: &[u64], body: &mut dyn FnMut() -> Result<T>) -> Result<T> {
        match engines.split_first() {
            Some((engine, rest)) => {
                ExecutionRuntime::engine_publication(*engine, || publications(rest, body))
            }
            None => body(),
        }
    }
    let runtime = runtime();
    let foreign = self::runtime();
    let mut permit = Some(
        publications(&(1..=32).collect::<Vec<_>>(), &mut || {
            runtime.reserve(
                CallLane::Provider,
                CallOrigin {
                    engine: 1,
                    generation: 1,
                    session: 0,
                },
                Duration::from_secs(2),
                false,
            )
        })
        .unwrap(),
    );
    let other = foreign.clone();
    let call = publications(&(33..=64).collect::<Vec<_>>(), &mut || {
        let other = other.clone();
        Ok(permit.take().unwrap().run_owned(move || {
            ExecutionRuntime::engine_publication(65, || {
                for _ in 0..2 {
                    assert!(matches!(
                        other.reserve_cleanup(Duration::from_millis(250)),
                        Err(Error::ResourceLimit("engine callback ancestry"))
                    ));
                    assert!(other.cleanup_quiescent());
                    assert_eq!(other.inner.cleanup_owners.load(Ordering::Acquire), 0);
                    assert_eq!(other.inner.outstanding.available_permits(), RECORDS);
                    other.inner.authority.check()?;
                }
                Ok(())
            })
        }))
    })
    .unwrap();
    call.await.unwrap();
    let cleanup = foreign.reserve_cleanup(Duration::from_millis(250)).unwrap();
    assert!(!foreign.cleanup_quiescent());
    cleanup.run(|| Ok(())).wait().await.unwrap();
    runtime.finish_standalone().await.unwrap();
    foreign.finish_standalone().await.unwrap();
    assert!(foreign.cleanup_quiescent());
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
