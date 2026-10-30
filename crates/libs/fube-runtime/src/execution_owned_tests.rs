use super::*;

fn runtime() -> ExecutionRuntime {
    ExecutionRuntime::with_mode(
        Authority::new(Instant::now() - Duration::from_secs(10), i64::MAX / 100).unwrap(),
        tokio::runtime::Handle::current(),
        false,
    )
}

#[tokio::test]
async fn expired_scope_finish_retains_exact_original_fence_under_live_root() {
    let runtime = runtime();
    let root = runtime.try_reserve().unwrap();
    let active = root.active.clone();
    let until = Instant::now() - Duration::from_millis(200);
    let scope = Scope::new(active.clone(), None, CallOrigin::default(), until);
    scope.finish();
    assert_eq!(runtime.inner.authority.retired_at_for_test(), Some(until));
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
            runtime.inner.authority.retire_at(earlier);
        }
        let until = Instant::now() + Duration::from_millis(50);
        let scope = Scope::new(active.clone(), None, CallOrigin::default(), until);
        scope.pending_results.store(1, Ordering::Release);
        scope.finish();
        assert_eq!(
            runtime.inner.authority.retired_at_for_test(),
            already_retired.then_some(earlier)
        );
        tokio::time::sleep_until((until + Duration::from_millis(10)).into()).await;
        scope.release_result();
        assert_eq!(
            runtime.inner.authority.retired_at_for_test(),
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
    let parent = Scope::new(active.clone(), None, CallOrigin::default(), parent_until);
    let first = Scope::new(
        active.clone(),
        Some(parent.clone()),
        CallOrigin::default(),
        first_until,
    );
    let sibling = Scope::new(
        active.clone(),
        Some(parent.clone()),
        CallOrigin::default(),
        parent_until,
    );
    parent.finish();
    first.finish();
    sibling.finish();
    assert_eq!(
        runtime.inner.authority.retired_at_for_test(),
        Some(first_until)
    );
    assert_eq!(active.children.load(Ordering::Acquire), 0);
    drop((parent, first, sibling, active, root));
    runtime.shutdown().await.unwrap();
}

#[test]
fn late_cleanup_watchdog_keeps_original_fence_with_idle_dispatcher() {
    const CHILD: &str = "FUBE_LATE_CLEANUP_FENCE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let application = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let authority = Authority::new(Instant::now(), 100_000_000).unwrap();
        authority.watchdog().unwrap();
        let runtime = ExecutionRuntime::new(authority, application.handle().clone());
        application.block_on(async {
            let owner = runtime.clone();
            let call = runtime.try_reserve().unwrap().run_future(async move {
                let cleanup = owner.reserve_cleanup(Duration::from_millis(250))?;
                let receipt = cleanup.run(|| {
                    std::thread::sleep(Duration::from_millis(450));
                    Ok(())
                });
                receipt.wait().await
            });
            let _ = call.await;
        });
        loop {
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }
    let started = Instant::now();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "execution::owned::tests::late_cleanup_watchdog_keeps_original_fence_with_idle_dispatcher",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(70));
            // Timestamp tests above carry the exact assertion. This subprocess
            // is only a generous bounded integration check, not a tight timer.
            assert!(started.elapsed() < Duration::from_secs(2));
            break;
        }
        if started.elapsed() >= Duration::from_secs(3) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("original cleanup fence did not terminate the child");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
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

#[test]
fn cleanup_error_owned_cycles_keep_original_terminal_fence() {
    const CHILD: &str = "FUBE_CYCLIC_CLEANUP_ERROR_CHILD";
    struct Cause {
        _child: Mutex<Option<Box<dyn Send>>>,
        native: Error,
    }
    impl std::fmt::Debug for Cause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Cause with cleanup obligation")
        }
    }
    impl std::fmt::Display for Cause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("error owns terminal cleanup obligation")
        }
    }
    impl std::error::Error for Cause {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.native)
        }
    }
    impl Drop for Cause {
        fn drop(&mut self) {
            if std::thread::current().name() != Some("fube-owned-cleanup")
                || !ExecutionRuntime::is_cleanup_entry()
            {
                unsafe { libc::_exit(84) }
            }
        }
    }
    fn error(child: impl Send + 'static) -> Error {
        Error::with_cause(
            crate::ErrorKind::Provider,
            "cause owns a scope dependency",
            Cause {
                _child: Mutex::new(Some(Box::new(child))),
                native: Error::Remote {
                    code: -704,
                    message: "native cycle metadata".into(),
                },
            },
        )
    }
    if let Some(scenario) = std::env::var_os(CHILD) {
        let application = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let authority = Authority::new(Instant::now(), 100_000_000).unwrap();
        authority.watchdog().unwrap();
        let runtime = ExecutionRuntime::new(authority, application.handle().clone());
        application.block_on(async {
            let receipt = if scenario == "child" {
                let record = runtime
                    .reserve_terminal(CallOrigin::default())
                    .unwrap()
                    .begin()
                    .unwrap();
                let child = record
                    .cleanup_after_callbacks(Duration::from_millis(250))
                    .unwrap();
                record.run(move || Err(error(child)))
            } else if scenario == "ancestor" {
                let owner = runtime.clone();
                runtime
                    .reserve(
                        CallLane::Native,
                        CallOrigin::default(),
                        Duration::from_secs(2),
                        true,
                    )
                    .unwrap()
                    .run_owned(move || {
                        let command = owner.command_cleanup().unwrap();
                        let entry = command.enter();
                        let receipt = owner
                            .reserve_cleanup(Duration::from_millis(250))?
                            .run(move || Err(error(command)));
                        drop(entry);
                        Ok(receipt)
                    })
                    .await
                    .unwrap()
            } else {
                assert_eq!(scenario, "publication");
                let owner = runtime.clone();
                runtime
                    .reserve_cleanup(Duration::from_millis(250))
                    .unwrap()
                    .run(move || {
                        let object = owner
                            .reserve_construction(
                                &ObjectQuota::new(1)?,
                                &DisposalTracker::default(),
                            )?
                            .construct(42usize);
                        Err(error(object))
                    })
            };
            // Some cycles keep the receipt pending; a pending publication may
            // instead complete the receipt while retaining the actual scope.
            // Neither case gets a fabricated ResourceExhausted/source-loss policy.
            let error = receipt.wait().await.unwrap_err();
            assert_eq!(error.kind(), crate::ErrorKind::Provider);
            assert_eq!(error.native_code(), Some(-704));
            assert!(std::error::Error::source(&error).unwrap().is::<Cause>());
            drop((receipt, error));
            runtime.shutdown().await.unwrap();
            panic!("cyclic returned ownership unexpectedly drained");
        });
        unreachable!();
    }
    for scenario in ["child", "ancestor", "publication"] {
        let started = Instant::now();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "execution::owned::tests::cleanup_error_owned_cycles_keep_original_terminal_fence",
                "--nocapture",
            ])
            .env(CHILD, scenario)
            .spawn()
            .unwrap();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(70), "{scenario}");
                assert!(started.elapsed() < Duration::from_secs(2), "{scenario}");
                break;
            }
            if started.elapsed() >= Duration::from_secs(3) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("{scenario}: original terminal fence did not terminate bounded child");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
