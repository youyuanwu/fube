use super::*;
use std::time::{Duration, Instant};

#[tokio::test]
async fn neutral_authority_backend_keeps_runtime_slots_deadlines_and_thread_accounting() {
    let authority = Authority::new(Instant::now() - Duration::from_secs(1), 1_000_000_000).unwrap();
    let backend: Arc<dyn fube_core::hosting::ExecutionAuthority> = authority.clone();
    let mut held = Vec::new();
    for _ in 0..64 {
        held.push(backend.clone().reserve_callback_deadline(false).unwrap());
    }
    assert!(matches!(
        authority.reserve_callback_deadline(false),
        Err(Error::ResourceLimit("callback deadline slots"))
    ));
    drop(held);
    let deadline = backend.clone().reserve_callback_deadline(false).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    deadline.arm_until(until);
    assert!(authority.remaining().unwrap() <= Duration::from_secs(2));
    deadline.set_effective_until(until + Duration::from_secs(10));
    assert!(authority.remaining().unwrap() <= Duration::from_secs(2));
    let expired = Instant::now() - Duration::from_millis(10);
    deadline.set_effective_until(expired);
    drop(deadline);
    assert_eq!(authority.retired_at_for_test(), Some(expired));
    assert!(authority
        .renew(Instant::now(), 1_000_000_000, Instant::now())
        .is_err());
    let earlier = expired - Duration::from_millis(10);
    backend.retire_at(earlier);
    backend.retire_at(until);
    assert_eq!(authority.retired_at_for_test(), Some(earlier));

    let thread = backend.own_thread(std::thread::spawn(|| {}));
    thread.join_finished().await.unwrap();
    thread.join_finished().await.unwrap();
    authority.disarm().unwrap();
    assert!(backend.clone().reserve_callback_deadline(true).is_err());
}

#[tokio::test]
async fn borrowed_execution_shutdown_requires_the_exact_host_backend() {
    let authority = Authority::new(Instant::now(), 1_000_000_000).unwrap();
    let execution = new(authority.clone(), tokio::runtime::Handle::current());
    assert!(execution.finish_standalone().await.is_err());
    let foreign = Authority::new(Instant::now(), 1_000_000_000).unwrap();
    assert!(shutdown(&execution, &foreign).await.is_err());
    execution
        .try_reserve()
        .unwrap()
        .run_owned(|| Ok(()))
        .await
        .unwrap();
    shutdown(&execution, &authority).await.unwrap();
    assert!(execution.try_reserve().is_err());
    authority.disarm().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum InvocationHelper {
    Invoke,
    Creation,
    Cleanup,
}

async fn invoke_helper(
    helper: InvocationHelper,
    execution: &ExecutionRuntime,
    operation: impl std::future::Future<Output = Result<()>> + Send + 'static,
) -> Result<()> {
    let timeout = Duration::from_secs(1);
    match helper {
        InvocationHelper::Invoke => execution.invoke(timeout, operation).await,
        InvocationHelper::Creation => {
            let ((), publication) = execution.invoke_creation(timeout, operation).await?;
            publication.accept()
        }
        InvocationHelper::Cleanup => execution.invoke_cleanup(timeout, operation).await,
    }
}

async fn invocation_helpers_keep_exact_owner(foreign_native: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};

    let authority = Authority::new(Instant::now(), 1_000_000_000).unwrap();
    let execution = new(authority.clone(), tokio::runtime::Handle::current());
    let foreign_authority =
        foreign_native.then(|| Authority::new(Instant::now(), 1_000_000_000).unwrap());
    let foreign = match &foreign_authority {
        Some(authority) => new(authority.clone(), tokio::runtime::Handle::current()),
        None => ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap(),
    };
    for helper in [
        InvocationHelper::Invoke,
        InvocationHelper::Creation,
        InvocationHelper::Cleanup,
    ] {
        let owner = execution.clone();
        let other = foreign.clone();
        let polled = Arc::new(AtomicBool::new(false));
        let observed = polled.clone();
        foreign
            .try_reserve()
            .unwrap()
            .run_future(async move {
                assert!(other.is_current_entry());
                assert!(!owner.is_current_entry());
                let deadline = other.current_deadline()?;
                let origin = ExecutionRuntime::current_origin();
                let cleanup = ExecutionRuntime::is_cleanup_entry();
                let error = invoke_helper(helper, &owner, async move {
                    observed.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap_err();
                assert_eq!(error.kind(), crate::ErrorKind::InvalidExecutionContext);
                assert!(other.is_current_entry());
                assert!(!owner.is_current_entry());
                assert_eq!(other.current_deadline()?, deadline);
                assert!(owner.current_deadline().is_err());
                assert_eq!(ExecutionRuntime::current_origin(), origin);
                assert_eq!(ExecutionRuntime::is_cleanup_entry(), cleanup);
                Ok(())
            })
            .await
            .unwrap();
        assert!(!polled.load(Ordering::SeqCst), "{helper:?}");

        let owner = execution.clone();
        let other = foreign.clone();
        execution
            .try_reserve()
            .unwrap()
            .run_future(async move {
                let deadline = owner.current_deadline()?;
                let origin = ExecutionRuntime::current_origin();
                let current = owner.clone();
                invoke_helper(helper, &owner, async move {
                    assert!(current.is_current_entry());
                    assert!(!other.is_current_entry());
                    assert!(current.current_deadline()? <= deadline);
                    assert!(other.current_deadline().is_err());
                    assert_eq!(ExecutionRuntime::current_origin(), origin);
                    tokio::task::yield_now().await;
                    assert!(current.is_current_entry());
                    assert!(!other.is_current_entry());
                    Ok(())
                })
                .await
            })
            .await
            .unwrap();
    }
    shutdown(&execution, &authority).await.unwrap();
    authority.disarm().unwrap();
    if let Some(authority) = foreign_authority {
        shutdown(&foreign, &authority).await.unwrap();
        authority.disarm().unwrap();
    } else {
        foreign.finish_standalone().await.unwrap();
    }
}

#[tokio::test]
async fn invocation_helpers_reject_foreign_standalone_and_keep_exact_native_owner() {
    invocation_helpers_keep_exact_owner(false).await;
}

#[tokio::test]
async fn invocation_helpers_reject_foreign_native_and_keep_exact_native_owner() {
    invocation_helpers_keep_exact_owner(true).await;
}

#[test]
fn parentless_final_release_at_full64_is_fenced_without_inline_drop() {
    const CHILD: &str = "FUBE_EXECUTION_FULL64_RELEASE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let app = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let authority = Authority::new(Instant::now(), 100_000_000).unwrap();
        authority.watchdog().unwrap();
        let runtime = new(authority, app.handle().clone());
        let quota = ObjectQuota::new(1).unwrap();
        let tracker = DisposalTracker::default();
        struct MustRemainOwned;
        impl Drop for MustRemainOwned {
            fn drop(&mut self) {
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
        assert!(matches!(
            runtime.try_reserve(),
            Err(Error::ResourceLimit(_))
        ));
        assert!(!tracker.is_drained());
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
