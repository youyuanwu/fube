use super::*;
use crate::hosting::ExecutionRuntime;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Condvar,
};

#[test]
fn furpc_classification_retains_original_errors_and_codec_resource_causes() {
    for (native, kind) in [
        (furpc::Error::Cancelled, ErrorKind::Canceled),
        (furpc::Error::Timeout("write"), ErrorKind::Timeout),
        (furpc::Error::Closed, ErrorKind::ObjectClosed),
        (furpc::Error::Unsupported("TLS"), ErrorKind::Unsupported),
        (
            furpc::Error::NativeHeader {
                id: 0x8001,
                source: fubst::Error::ResourceLimit("header budget"),
            },
            ErrorKind::ResourceExhausted,
        ),
        (
            furpc::Error::Codec {
                phase: "decode",
                source: furpc::CodecError::resource_limit(fubst::Error::ResourceLimit(
                    "body budget",
                )),
            },
            ErrorKind::ResourceExhausted,
        ),
    ] {
        let error = Error::from(native.clone());
        assert_eq!(error.kind(), kind);
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<furpc::Error>(),
            Some(&native)
        );
        assert_eq!(error.clone().uncertain().kind(), kind);
    }
}

#[tokio::test]
async fn furpc_remote_fault_preserves_native_code_and_original_body_through_wrapping() {
    use furpc::{
        header, Body, IngressLimits, IngressOptions, IngressResponse, IngressService, Message,
        Request, RequestPolicy, ServiceBinding,
    };
    use std::{
        future::{ready, Ready},
        task::{Context, Poll},
        time::Duration,
    };
    use tower_service::Service;

    #[derive(Clone)]
    struct Fault;
    impl Service<Request<Body>> for Fault {
        type Response = IngressResponse;
        type Error = furpc::Error;
        type Future = Ready<furpc::Result<IngressResponse>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: Request<Body>) -> Self::Future {
            ready(Ok(IngressResponse::Reply(
                Message::builder()
                    .append_header(
                        furpc::HeaderRecord::native(
                            header::FAULT,
                            &furpc::Fault {
                                code: 0x80071bcbu32 as i32 as i64,
                                has_body: false,
                            },
                        )
                        .unwrap(),
                    )
                    .body(Body::from_bytes(vec![9, 8, 7]).unwrap())
                    .build()
                    .unwrap(),
            )))
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let allow = |_: &furpc::Headers, _: &furpc::Source| Ok(());
    let service = IngressService::new(
        Fault,
        vec![ServiceBinding::raw(IngressOptions::requests(
            RequestPolicy::new(allow, allow),
            IngressLimits::default(),
        ))
        .unwrap()],
    )
    .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = furpc::Server::builder()
        .service(service)
        .unwrap()
        .build()
        .unwrap();
    let owner = tokio::spawn(server.serve_with_shutdown(
        furpc::TcpAcceptor::from_listener(listener),
        furpc::ConnectionOptions::managed(),
        async {
            let _ = stopped.await;
        },
    ));
    let channel = furpc::Endpoint::try_from(address)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let native = channel
        .peer(deadline)
        .await
        .unwrap()
        .request(Request::new(Body::empty()), deadline)
        .await
        .unwrap_err();
    let error = Error::with_cause(ErrorKind::Provider, "callback", Error::from(native)).uncertain();
    assert_eq!(error.native_code(), Some(0x80071bcbu32 as i32 as i64));
    assert_eq!(error.native_result_code(), 0x80071bcbu32 as i32 as i64);
    let Error::Uncertain(wrapped) = &error else {
        unreachable!()
    };
    let Error::Classified {
        cause: Some(converted),
        ..
    } = &**wrapped
    else {
        unreachable!()
    };
    let converted = converted.downcast_ref::<Error>().unwrap();
    assert_eq!(converted.kind(), ErrorKind::ReplicationQueueFull);
    let native = std::error::Error::source(converted)
        .unwrap()
        .downcast_ref::<furpc::Error>()
        .unwrap();
    let furpc::Error::RemoteFault(fault) = native else {
        unreachable!()
    };
    assert_eq!(
        fault
            .body()
            .segments()
            .flatten()
            .copied()
            .collect::<Vec<_>>(),
        [9, 8, 7]
    );
    drop((error, channel));
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), owner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn replication_pressure_and_sizing_keep_typed_native_codes_and_causes() {
    for (kind, code) in [
        (ErrorKind::ReplicationQueueFull, 0x80071bcbu32 as i32 as i64),
        (
            ErrorKind::ReplicationOperationTooLarge,
            0x80071c2fu32 as i32 as i64,
        ),
    ] {
        let error = Error::with_cause(kind, "not parsed", Error::ResourceLimit("retained budget"));
        assert_eq!(error.kind(), kind);
        assert_eq!(error.native_code(), Some(code));
        assert_eq!(error.native_result_code(), code);
        assert!(std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<Error>()
            .is_some());
        let uncertain = error.clone().uncertain();
        assert_eq!(uncertain.kind(), kind);
        assert_eq!(uncertain.native_result_code(), code);
        let provider = Error::with_cause(ErrorKind::Provider, "callback", error.clone());
        assert_eq!(provider.native_result_code(), code);
    }
    assert_eq!(
        Error::ResourceLimit("other exhaustion").kind(),
        ErrorKind::ResourceExhausted
    );
}

#[derive(Debug)]
struct Cause {
    native: Error,
    started: Arc<AtomicBool>,
    placement: Arc<Mutex<Option<String>>>,
    release: Arc<(Mutex<bool>, Condvar)>,
}
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        assert!(ExecutionRuntime::in_owned_entry());
        f.write_str("closed canceled not-primary")
    }
}
#[tokio::test]
async fn cleanup_and_terminal_errors_preserve_causes_until_final_owned_drop() {
    for route in 0..3 {
        let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
        let started = Arc::new(AtomicBool::new(false));
        let placement = Arc::new(Mutex::new(None));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let cause = Cause {
            native: Error::Remote {
                code: -702,
                message: "cleanup native source".into(),
            },
            started: started.clone(),
            placement: placement.clone(),
            release: release.clone(),
        };
        let body = move || {
            Err(Error::with_cause(
                ErrorKind::Provider,
                "accepted disposal error",
                cause,
            ))
        };
        let receipt = match route {
            0 => execution
                .reserve_cleanup(std::time::Duration::from_millis(250))
                .unwrap()
                .run(body),
            1 => execution
                .reserve_terminal(Default::default())
                .unwrap()
                .run(body)
                .unwrap(),
            _ => execution
                .reserve_terminal(Default::default())
                .unwrap()
                .begin()
                .unwrap()
                .run(body),
        };
        let copy = receipt.clone();
        let error = receipt.wait().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Provider);
        assert_eq!(error.native_code(), Some(-702));
        assert!(std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<Cause>()
            .is_some());
        let clone = error.clone();
        drop((receipt, error));
        assert!(!started.load(Ordering::Acquire));
        assert_eq!(copy.wait().await.unwrap_err().native_code(), Some(-702));
        drop(copy);
        assert!(!started.load(Ordering::Acquire));
        drop(clone);
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
            "final Arc destruction must remain accounted"
        );
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        shutdown.await.unwrap();
    }
}
#[tokio::test]
async fn cleanup_error_preserves_cause_with_independent_ancestor_sibling_and_publication() {
    use crate::hosting::{CallLane, CallOrigin, DisposalTracker, ObjectQuota};
    use std::time::Duration;

    for scenario in ["ancestor", "sibling", "publication"] {
        let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
        let started = Arc::new(AtomicBool::new(false));
        let placement = Arc::new(Mutex::new(None));
        let cause = Cause {
            native: Error::with_cause(
                ErrorKind::Native,
                "nested native metadata",
                Error::Remote {
                    code: -902,
                    message: "ordinary cause, no ownership token".into(),
                },
            ),
            started: started.clone(),
            placement: placement.clone(),
            release: Arc::new((Mutex::new(true), Condvar::new())),
        };
        let owner = execution.clone();
        let (observed, observation) = tokio::sync::oneshot::channel();
        let (release_owner, released) = tokio::sync::oneshot::channel();
        let call = execution
            .reserve(
                CallLane::Native,
                CallOrigin::default(),
                Duration::from_secs(2),
                true,
            )
            .unwrap()
            .run_future(async move {
                let mut token = None;
                let mut publication = None;
                let body = move || {
                    Err(Error::with_cause(
                        ErrorKind::Provider,
                        "ordinary error",
                        cause,
                    ))
                };
                let receipt = if scenario == "ancestor" {
                    let command = owner.command_cleanup().unwrap();
                    let entry = command.enter();
                    let receipt = owner.reserve_cleanup(Duration::from_millis(250))?.run(body);
                    drop(entry);
                    token = Some(command); // Independent; never moved into the cause.
                    receipt
                } else {
                    let command = owner.command_cleanup().unwrap();
                    if scenario == "publication" {
                        let entry = command.enter();
                        publication = Some(
                            owner
                                .reserve_construction(
                                    &ObjectQuota::new(1)?,
                                    &DisposalTracker::default(),
                                )?
                                .construct(123usize),
                        );
                        drop(entry);
                        drop(command);
                    } else {
                        token = Some(command); // Sibling of the cleanup scope, not its parent.
                    }
                    owner.reserve_cleanup(Duration::from_millis(250))?.run(body)
                };
                let error = receipt.wait().await.unwrap_err();
                drop(receipt);
                observed.send(error).unwrap();
                released.await.unwrap();
                drop(token);
                if let Some(object) = publication {
                    object.publication().accept()?;
                    drop(object);
                }
                Ok(())
            });
        let error = tokio::time::timeout(Duration::from_secs(1), observation)
            .await
            .unwrap()
            .unwrap();
        let actual = (
            error.kind(),
            error.native_code(),
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<Cause>())
                .is_some_and(|source| source.native.native_code() == Some(-902)),
        );
        let copy = error.clone();
        drop(error);
        let retained_before_owner_release = !started.load(Ordering::Acquire);
        release_owner.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .unwrap()
            .unwrap();
        let retained_after_owner_release = !started.load(Ordering::Acquire);
        // This is the current-thread application observer, not an owned entry.
        assert!(!ExecutionRuntime::in_owned_entry());
        drop(copy);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        execution.finish_standalone().await.unwrap();
        assert_eq!(
            actual,
            (ErrorKind::Provider, Some(-902), true),
            "{scenario}"
        );
        assert!(
            retained_before_owner_release && retained_after_owner_release,
            "{scenario}"
        );
        assert_eq!(
            placement.lock().unwrap().as_deref(),
            Some("fube-owned-cleanup")
        );
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

#[tokio::test]
async fn furpc_codec_user_cause_remains_supervised_until_final_clone_is_released() {
    let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let placement = Arc::new(Mutex::new(None));
    let cause = Cause {
        native: Error::Remote {
            code: -903,
            message: "codec source".into(),
        },
        started: started.clone(),
        placement: placement.clone(),
        release: Arc::new((Mutex::new(true), Condvar::new())),
    };
    let error = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            Err::<(), _>(Error::from(furpc::Error::Codec {
                phase: "decode",
                source: furpc::CodecError::invalid_payload(cause),
            }))
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(error.native_code(), Some(-903));
    let native = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<furpc::Error>()
        .unwrap();
    let codec = std::error::Error::source(native).unwrap();
    assert!(std::error::Error::source(codec)
        .unwrap()
        .downcast_ref::<Cause>()
        .is_some());
    let copy = error.clone();
    drop(error);
    assert!(!started.load(Ordering::Acquire));
    drop(copy);
    execution.finish_standalone().await.unwrap();
    assert!(started.load(Ordering::Acquire));
    assert_eq!(
        placement.lock().unwrap().as_deref(),
        Some("fube-owned-cleanup")
    );
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
