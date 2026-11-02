use super::*;
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

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
        let runtime = new(authority.clone(), application.handle().clone());
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
            "execution::owned_tests::late_cleanup_watchdog_keeps_original_fence_with_idle_dispatcher",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(70));
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
        let runtime = new(authority.clone(), application.handle().clone());
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
            // Cyclic captures must retain the real scope and original deadline,
            // not a fabricated capacity error or an unsupervised destructor.
            let error = receipt.wait().await.unwrap_err();
            assert_eq!(error.kind(), crate::ErrorKind::Provider);
            assert_eq!(error.native_code(), Some(-704));
            assert!(std::error::Error::source(&error).unwrap().is::<Cause>());
            drop((receipt, error));
            shutdown(&runtime, &authority).await.unwrap();
            panic!("cyclic returned ownership unexpectedly drained");
        });
        unreachable!();
    }
    for scenario in ["child", "ancestor", "publication"] {
        let started = Instant::now();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "execution::owned_tests::cleanup_error_owned_cycles_keep_original_terminal_fence",
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
