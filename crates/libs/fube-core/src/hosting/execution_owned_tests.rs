use super::*;

struct Fixture {
    runtime: ExecutionRuntime,
    authority: Arc<StandaloneAuthority>,
}
impl std::ops::Deref for Fixture {
    type Target = ExecutionRuntime;
    fn deref(&self) -> &ExecutionRuntime {
        &self.runtime
    }
}
fn runtime() -> Fixture {
    let authority = Arc::new(StandaloneAuthority::default());
    Fixture {
        runtime: ExecutionRuntime::with_mode(
            authority.clone(),
            tokio::runtime::Handle::current(),
            false,
        ),
        authority,
    }
}

#[tokio::test]
async fn retained_deadline_observation_does_not_authorize_cleanup_waiting() {
    let runtime = runtime();
    let owned = runtime.clone();
    runtime
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let deadline = owned.current_deadline()?;
            let cleanup = owned.command_cleanup().unwrap();
            let _entry = cleanup.enter();
            assert_eq!(owned.current_deadline()?, deadline);
            assert!(owned.blocking_deadline().is_err());
            Ok(())
        })
        .await
        .unwrap();
    runtime.finish_standalone().await.unwrap();
}

#[tokio::test]
async fn expired_scope_finish_retains_exact_original_fence_under_live_root() {
    let runtime = runtime();
    let root = runtime.try_reserve().unwrap();
    let active = root.active.clone();
    let until = Instant::now() - Duration::from_millis(200);
    let scope = Scope::new(active.clone(), None, CallOrigin::default(), until, None);
    scope.finish();
    assert_eq!(runtime.authority.retired_at_for_test(), Some(until));
    assert!(active.until() > Instant::now() + Duration::from_secs(20));
    assert_eq!(active.children.load(Ordering::Acquire), 0);
    drop((scope, active, root));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_pending_result_release_retains_exact_fence_and_earlier_retirement_minimum() {
    for already_retired in [false, true] {
        let runtime = runtime();
        let root = runtime.try_reserve().unwrap();
        let active = root.active.clone();
        let earlier = Instant::now() - Duration::from_secs(1);
        if already_retired {
            runtime.authority.retire_at(earlier);
        }
        let until = Instant::now() + Duration::from_millis(50);
        let scope = Scope::new(active.clone(), None, CallOrigin::default(), until, None);
        scope.pending_results.store(1, Ordering::Release);
        scope.finish();
        assert_eq!(
            runtime.authority.retired_at_for_test(),
            already_retired.then_some(earlier)
        );
        tokio::time::sleep_until((until + Duration::from_millis(10)).into()).await;
        scope.release_result();
        assert_eq!(
            runtime.authority.retired_at_for_test(),
            Some(if already_retired { earlier } else { until })
        );
        assert!(scope.active.lock().unwrap().is_none());
        drop((scope, active, root));
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn nested_and_sibling_expired_scopes_cannot_replace_earlier_fence() {
    let runtime = runtime();
    let root = runtime.try_reserve().unwrap();
    let active = root.active.clone();
    let parent_until = Instant::now() - Duration::from_millis(100);
    let first_until = parent_until - Duration::from_millis(100);
    let parent = Scope::new(
        active.clone(),
        None,
        CallOrigin::default(),
        parent_until,
        None,
    );
    let first = Scope::new(
        active.clone(),
        Some(parent.clone()),
        CallOrigin::default(),
        first_until,
        None,
    );
    let sibling = Scope::new(
        active.clone(),
        Some(parent.clone()),
        CallOrigin::default(),
        parent_until,
        None,
    );
    parent.finish();
    first.finish();
    sibling.finish();
    assert_eq!(runtime.authority.retired_at_for_test(), Some(first_until));
    assert_eq!(active.children.load(Ordering::Acquire), 0);
    drop((parent, first, sibling, active, root));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cleanup_error_capacity_and_shutdown_rejections_drop_on_existing_worker() {
    #[derive(Debug)]
    struct Cause(Arc<Mutex<Option<(String, bool)>>>, Error);
    impl std::fmt::Display for Cause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
            f.write_str("custom cleanup I/O cause")
        }
    }
    impl std::error::Error for Cause {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.1)
        }
    }
    impl Drop for Cause {
        fn drop(&mut self) {
            *self.0.lock().unwrap() = Some((
                std::thread::current().name().unwrap().to_owned(),
                ExecutionRuntime::is_cleanup_entry(),
            ));
        }
    }
    for terminal in [false, true] {
        for shutdown in [false, true] {
            let runtime = runtime();
            let exhausted = (!shutdown).then(|| {
                runtime
                    .inner
                    .dormant
                    .clone()
                    .try_acquire_many_owned(DORMANT_TICKETS as u32)
                    .unwrap()
            });
            let placement = Arc::new(Mutex::new(None));
            let capture = placement.clone();
            let (entered, entered_rx) = oneshot::channel();
            let (release, release_rx) = std::sync::mpsc::sync_channel(1);
            let body = move || {
                entered.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                Err(Error::from(std::io::Error::other(Cause(
                    capture,
                    Error::Remote {
                        code: -703,
                        message: "nested native".into(),
                    },
                ))))
            };
            let receipt = if terminal {
                runtime
                    .reserve_terminal(CallOrigin::default())
                    .unwrap()
                    .run(body)
                    .unwrap()
            } else {
                runtime
                    .reserve_cleanup(Duration::from_millis(250))
                    .unwrap()
                    .run(body)
            };
            entered_rx.await.unwrap();
            let mut finish = Box::pin(runtime.shutdown());
            if shutdown {
                assert!(tokio::time::timeout(Duration::from_millis(5), &mut finish)
                    .await
                    .is_err());
                assert!(runtime.inner.stopping.load(Ordering::Acquire));
            }
            release.send(()).unwrap();
            let error = receipt.wait().await.unwrap_err();
            assert_eq!(
                error.kind(),
                if shutdown {
                    crate::ErrorKind::ObjectClosed
                } else {
                    crate::ErrorKind::ResourceExhausted
                }
            );
            assert_eq!(
                placement.lock().unwrap().as_ref(),
                Some(&("fube-owned-cleanup".into(), true))
            );
            drop((error, receipt, exhausted));
            finish.await.unwrap();
        }
    }
}
