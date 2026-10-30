use super::*;
use crate::extensions::ExecutionRuntime;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Condvar,
};

#[derive(Debug)]
struct Cause {
    native: Error,
    started: Arc<AtomicBool>,
    placement: Arc<Mutex<Option<String>>>,
    release: Arc<(Mutex<bool>, Condvar)>,
}
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("closed canceled not-primary")
    }
}
impl std::error::Error for Cause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.native)
    }
}
impl Drop for Cause {
    fn drop(&mut self) {
        let name = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_owned();
        *self.placement.lock().unwrap() = Some(name.clone());
        self.started.store(true, Ordering::Release);
        // A wrong-placement regression must fail, not hang the observer thread.
        if name == "fube-owned-cleanup" {
            let (mutex, changed) = &*self.release;
            let released = mutex.lock().unwrap();
            let (released, timeout) = changed
                .wait_timeout_while(released, std::time::Duration::from_secs(2), |released| {
                    !*released
                })
                .unwrap();
            assert!(
                *released && !timeout.timed_out(),
                "test cause destructor release missing"
            );
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_typed_cause_and_native_code_survive_observation_then_supervised_physical_drop() {
    let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let placement = Arc::new(Mutex::new(None));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let cause = Cause {
        native: Error::Remote {
            code: -701,
            message: "native domain cause".into(),
        },
        started: started.clone(),
        placement: placement.clone(),
        release: release.clone(),
    };
    let error = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            Err::<(), _>(Error::with_cause(
                ErrorKind::Provider,
                "callback failed",
                cause,
            ))
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Provider);
    assert_eq!(error.native_code(), Some(-701));
    assert!(std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<Cause>()
        .is_some());
    let copy = error.clone();
    drop(error);
    assert!(!started.load(Ordering::Acquire));
    drop(copy);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        placement.lock().unwrap().as_deref(),
        Some("fube-owned-cleanup")
    );
    let mut shutdown = Box::pin(execution.finish_standalone());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err(),
        "completion/drop signaling is not physical disposal"
    );
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    shutdown.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_rejected_input_retires_authority_and_drops_factory_on_cleanup() {
    struct Factory(Arc<AtomicBool>);
    impl crate::IStatelessServiceFactory for Factory {
        fn create_instance(
            &self,
            _: String,
            _: String,
            _: &[u8],
            _: uuid::Uuid,
            _: i64,
        ) -> Result<Box<dyn crate::IStatelessServiceInstance>> {
            unreachable!("unaccepted input must not be invoked")
        }
    }
    impl Drop for Factory {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
            self.0.store(true, Ordering::Release);
        }
    }
    let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let input = dropped.clone();
    let error = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            Err::<(), _>(Error::rejected(RejectedRegistrationInput::Stateless(
                Box::new(Factory(input)),
            )))
        })
        .await
        .unwrap_err()
        .uncertain();
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(error.native_result_code(), 0x80071bfdu32 as i32 as i64);
    assert_eq!(
        execution.try_reserve().err().unwrap().kind(),
        ErrorKind::ObjectClosed
    );
    assert!(!dropped.load(Ordering::Acquire));
    drop(error);
    execution.finish_standalone().await.unwrap();
    assert!(dropped.load(Ordering::Acquire));
}
