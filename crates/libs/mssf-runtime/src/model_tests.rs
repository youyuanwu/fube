//! Isolated loopback MODEL peers only; never native interoperability evidence.
use super::*;
use mssf_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId},
    serialization::serde::{ObjectArray, Pointer},
};
use std::{io::Write, process::Stdio};
use tokio::{net::TcpListener, process::Command};

#[test]
fn model_child() {
    let Ok(mode) = std::env::var("MSSF_MODEL_CHILD") else {
        return;
    };
    let mut builder = if matches!(
        mode.as_str(),
        "drop-capture" | "stateful-blocked-create-method" | "stateful-blocked-future-drop"
    ) || mode.starts_with("stateful-blocked-final-")
    {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2);
        builder
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().build().unwrap().block_on(async {
        if mode == "attach-cancel" {
            assert!(
                tokio::time::timeout(Duration::from_millis(50), Runtime::attach())
                    .await
                    .is_err()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
        let attached = Runtime::attach().await;
        if mode.starts_with("startup-")
            || mode == "registration-failure"
            || mode == "overflow"
            || mode == "attach-cancel"
            || mode == "fifo-startup"
        {
            assert!(attached.is_err());
            return;
        }
        let mut runtime = attached.unwrap();
        match mode.as_str() {
            mode if mode.starts_with("stateful-blocked-final-") => {
                runtime
                    .lifecycle
                    .set_stateful_callback_timeout(Duration::from_millis(100));
                runtime
                    .register_stateful(
                        "BlockedStateful",
                        crate::lifecycle::tests::stateful_tests::destructor_model_factory(),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(10)).await;
                panic!("final participant destruction escaped independent cleanup fence");
            }
            "stateful-blocked-factory"
            | "stateful-blocked-create-method"
            | "stateful-blocked-future-drop" => {
                struct Blocked(u8);
                struct BlockingFutureDrop;
                impl std::future::Future for BlockingFutureDrop {
                    type Output = Result<Arc<dyn StatefulService>>;
                    fn poll(
                        self: std::pin::Pin<&mut Self>,
                        _: &mut std::task::Context<'_>,
                    ) -> std::task::Poll<Self::Output> {
                        std::task::Poll::Ready(Err(Error::Protocol("controlled factory error")))
                    }
                }
                impl Drop for BlockingFutureDrop {
                    fn drop(&mut self) {
                        println!("STATEFUL_FUTURE_DROP_ENTERED");
                        std::io::stdout().flush().unwrap();
                        std::thread::sleep(Duration::from_secs(60));
                    }
                }
                impl StatefulFactory for Blocked {
                    fn create(
                        &self,
                        _: StatefulServiceContext,
                    ) -> ServiceFuture<'_, Arc<dyn StatefulService>> {
                        if self.0 == 1 {
                            println!("STATEFUL_FACTORY_ENTERED");
                            std::io::stdout().flush().unwrap();
                            std::thread::sleep(Duration::from_secs(60));
                        }
                        if self.0 == 2 {
                            return Box::pin(BlockingFutureDrop);
                        }
                        Box::pin(async {
                            // No executor, cancellation or stderr progress is
                            // available; only the independent OS fence can win.
                            println!("STATEFUL_FACTORY_ENTERED");
                            std::io::stdout().flush().unwrap();
                            std::thread::sleep(Duration::from_secs(60));
                            Err(Error::Protocol("stateful factory escaped hard fence"))
                        })
                    }
                }
                let kind = match mode.as_str() {
                    "stateful-blocked-create-method" => 1,
                    "stateful-blocked-future-drop" => 2,
                    _ => 0,
                };
                if kind != 0 {
                    runtime
                        .lifecycle
                        .set_stateful_callback_timeout(Duration::from_millis(50));
                }
                runtime
                    .register_stateful("BlockedStateful", Arc::new(Blocked(kind)))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                panic!("stateful callback defeated process fencing");
            }
            "live-update" => {
                struct Unplaced;
                impl StatelessFactory for Unplaced {
                    fn create(
                        &self,
                        _: ServiceContext,
                    ) -> ServiceFuture<'_, Arc<dyn StatelessService>> {
                        Box::pin(async { panic!("model type is not placed") })
                    }
                }
                let old = runtime.activation_context();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while runtime.activation_context().generation() == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(
                    old.packages()
                        .unwrap()
                        .configuration_package("Config")
                        .unwrap()
                        .description()
                        .version(),
                    "1"
                );
                assert_eq!(
                    runtime
                        .activation_context()
                        .packages()
                        .unwrap()
                        .configuration_package("Config")
                        .unwrap()
                        .description()
                        .version(),
                    "2"
                );
                runtime
                    .register_stateless("LaterType", Arc::new(Unplaced))
                    .await
                    .unwrap();
                runtime.shutdown().await.unwrap();
            }
            "update-block" | "update-panic" => {
                let should_panic = mode == "update-panic";
                let _registration = runtime
                    .context_handle()
                    .subscribe_configuration(move |_| {
                        assert!(!should_panic, "injected package callback panic");
                        std::thread::sleep(Duration::from_secs(60));
                    })
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                panic!("blocked callback defeated update deadline");
            }
            "shutdown-ok" => runtime.shutdown().await.unwrap(),
            "shutdown-fail" => assert!(runtime.shutdown().await.is_err()),
            "shutdown-cancel" => assert!(tokio::time::timeout(
                Duration::from_millis(100),
                runtime.shutdown()
            )
            .await
            .is_err()),
            "drop" => drop(runtime),
            "drop-capture" => {
                struct BlockingDrop;
                impl Drop for BlockingDrop {
                    fn drop(&mut self) {
                        std::thread::sleep(Duration::from_secs(60));
                    }
                }
                let capture = BlockingDrop;
                let _registration = runtime
                    .context_handle()
                    .subscribe_configuration(move |_| {
                        let _ = &capture;
                    })
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(600)).await;
                drop(runtime);
                panic!("blocking capture escaped owner-drop hard fence");
            }
            "executor-stall" => {
                // The only async executor thread is blocked, no timer or
                // transport callback can help. The OS watchdog must expire.
                std::thread::sleep(Duration::from_secs(2));
                drop(runtime);
                panic!("executor stall defeated authority expiry");
            }
            "blocked-stderr" => {
                std::thread::spawn(|| {
                    let mut stderr = std::io::stderr().lock();
                    loop {
                        let _ = stderr.write_all(&[b'x'; 8192]);
                    }
                });
                drop(runtime);
            }
            "live-invalidate" | "live-context" | "lease-zero" | "eof" => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                // An armed fence must terminate first.
                drop(runtime);
                panic!("terminal supervisor failed to terminate");
            }
            _ => panic!("unknown model scenario"),
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(
            matches!(mode.as_str(), "shutdown-ok" | "live-update"),
            "armed fence failed"
        );
    });
}

fn start_reply() -> StartReply {
    StartReply {
        node_id: "abcd".into(),
        node_pid: 100,
        lease_handle: 200,
        lease_duration: 300_000_000,
        error: 0,
        node_instance: 3,
        node_name: "MODEL".into(),
        node_type: "model".into(),
        client_address: "127.0.0.1:19000".into(),
        deployment: "/model".into(),
        ip: "127.0.0.1".into(),
        work: "/model".into(),
        initial_ttl: 0,
        application_directories: ObjectArray(vec![]),
        node_directories: ObjectArray(vec![]),
        application_log: Pointer(None),
        system_log: Pointer(None),
    }
}
fn control(action: &str, index: u32, actor: i64) -> Message {
    Message {
        headers: vec![
            Header::actor(actor).unwrap(),
            Header::action(action).unwrap(),
            Header::message_id(MessageId {
                guid: [7; 16],
                index,
            })
            .unwrap(),
            Header::boolean(mssf_transport::message::EXPECTS_REPLY, true).unwrap(),
        ],
        body: vec![],
    }
}

async fn scenario(mode: &str, expected: i32) {
    let fixture = crate::packages::tests::Fixture::new();
    #[cfg(unix)]
    if mode == "fifo-startup" {
        use std::os::unix::ffi::OsStrExt;
        let path = fixture.base.join("RustService.Package.Current.xml");
        std::fs::remove_file(&path).unwrap();
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "model_tests::model_child", "--nocapture"])
        .env("MSSF_MODEL_CHILD", mode)
        .env(
            "Fabric_RuntimeConnectionAddress",
            listener.local_addr().unwrap().to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if mode.starts_with("stateful-blocked-") {
        command.stdout(Stdio::piped());
    }
    for (k, v) in [
        (
            "Fabric_ApplicationHostId",
            "443de6bf-b823-40d2-915d-0826843aeddb",
        ),
        ("Fabric_ApplicationHostType", "Activated_SingleCodePackage"),
        ("Fabric_ApplicationId", "RustApp_App0"),
        ("Fabric_ServicePackageName", "RustService"),
        ("Fabric_CodePackageName", "Code"),
        ("Fabric_CodePackageInstanceSeqNum", "1"),
        ("Fabric_ServicePackageInstanceSeqNum", "2"),
        ("Fabric_ServicePackageVersionInstance", "1.0:2.0:3"),
        ("Fabric_ApplicationName", "fabric:/RustApp"),
        ("Fabric_Folder_App_Work", "/model/work"),
        ("Fabric_Folder_App_Log", "/model/log"),
        ("Fabric_Folder_App_Temp", "/model/temp"),
        ("Fabric_NodeIPOrFQDN", "127.0.0.1"),
        ("Fabric_RuntimeConnectionUseSsl", "false"),
        ("Fabric_IsContainerHost", "false"),
        ("Fabric_IsCodePackageActivatorHost", "false"),
    ] {
        command.env(k, v);
    }
    command
        .env_remove("Fabric_ServicePackageActivationGuid")
        .env_remove("Fabric_ServicePackageActivationId");
    let mut child = command.spawn().unwrap();
    let child_pid = child.id().unwrap();
    let mode_owned = mode.to_owned();
    let deployment = fixture.root.to_str().unwrap().to_owned();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let hello = read_message(&mut stream).await.unwrap();
        assert_eq!(hello.properties().unwrap().actor, 1);
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: [1; 16],
                    index: 1,
                },
                &ListenInstance::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let mut step = 0;
        let mut runtime_context = None::<RuntimeContext>;
        let mut stateful_request = None::<crate::wire::ProxyRequest>;
        let mut close_retry = None::<crate::wire::ProxyRequest>;
        let mut close_index = 102;
        let mut close_tick = tokio::time::interval(Duration::from_millis(25));
        let (mut reader, mut stream) = stream.into_split();
        let mut update_sent = false;
        loop {
            let request = {
                // Keep a partially read frame alive across retry ticks.
                let next = read_message(&mut reader);
                tokio::pin!(next);
                loop {
                    tokio::select! {
                        result=&mut next=>break match result {Ok(r)=>r,Err(_)=>return},
                        _=close_tick.tick(),if close_retry.is_some()=>{
                            close_index+=1;
                            if write_message(&mut stream,&Message {
                                headers:vec![Header::actor(12).unwrap(),Header::action("ReplicaClose").unwrap(),
                                    Header::message_id(MessageId {guid:[9;16],index:close_index}).unwrap()],
                                body:to_vec(close_retry.as_ref().unwrap()).unwrap(),
                            }).await.is_err() {return;}
                        }
                    }
                }
            };
            let p = request.properties().unwrap();
            if p.relates_to != MessageId::default() {
                if p.relates_to.index == 99
                    && matches!(
                        mode_owned.as_str(),
                        "live-update" | "update-block" | "update-panic"
                    )
                {
                    assert_eq!(
                        mode_owned, "live-update",
                        "failed update was falsely acknowledged"
                    );
                    assert_eq!(from_slice::<ErrorReply>(&request.body).unwrap().error, 0);
                }
                continue;
            }
            let body = match p.action.as_str() {
                "StartRegisterApplicationHostRequest" => {
                    assert_eq!(step, 0);
                    step = 1;
                    let start: StartRequest = from_slice(&request.body).unwrap();
                    assert_eq!(start.pid, child_pid);
                    if mode_owned == "attach-cancel" {
                        continue;
                    }
                    let mut reply = start_reply();
                    reply.deployment = deployment.clone();
                    if mode_owned == "registration-failure" {
                        reply.error = 0x80004005u32 as i32 as i64;
                    }
                    to_vec(&reply).unwrap()
                }
                "FinishRegisterApplicationHostRequest" => {
                    assert_eq!(step, 1);
                    step = 2;
                    to_vec(&ErrorReply { error: 0 }).unwrap()
                }
                "RegisterFabricRuntimeRequest" => {
                    assert_eq!(step, 2);
                    step = 3;
                    let runtime: RuntimeRequest = from_slice(&request.body).unwrap();
                    assert_eq!(runtime.context.host.pid, child_pid);
                    runtime_context = Some(runtime.context);
                    to_vec(&ErrorReply { error: 0 }).unwrap()
                }
                "" if p.actor == 15 => {
                    assert!(step >= 3);
                    step = 4;
                    to_vec(&LeaseReply {
                        ttl: if matches!(
                            mode_owned.as_str(),
                            "executor-stall" | "stateful-blocked-factory"
                        ) {
                            5_000_000
                        } else {
                            300_000_000
                        },
                    })
                    .unwrap()
                }
                "UnregisterFabricRuntimeRequest" | "UnregisterApplicationHostRequest" => {
                    if !matches!(mode_owned.as_str(), "shutdown-ok" | "live-update") {
                        continue;
                    }
                    to_vec(&ErrorReply { error: 0 }).unwrap()
                }
                "ReplicaOpenReply" if mode_owned.starts_with("stateful-blocked-final-") => {
                    assert!(
                        !mode_owned.ends_with("failed-open"),
                        "Open acknowledged before final destructor completed"
                    );
                    let mut close = stateful_request.clone().unwrap();
                    close.flags = if mode_owned.contains("abort") { 4 } else { 0 };
                    // Semantic Open reply can precede release of its dispatch
                    // gate. Native retries incompatible Close rather than
                    // assuming this first one-way attempt was admitted.
                    close_retry = Some(close.clone());
                    write_message(
                        &mut stream,
                        &Message {
                            headers: vec![
                                Header::actor(12).unwrap(),
                                Header::action("ReplicaClose").unwrap(),
                                Header::message_id(MessageId {
                                    guid: [9; 16],
                                    index: 102,
                                })
                                .unwrap(),
                            ],
                            body: to_vec(&close).unwrap(),
                        },
                    )
                    .await
                    .unwrap();
                    continue;
                }
                "ReplicaCloseReply" if mode_owned.starts_with("stateful-blocked-final-") => {
                    panic!("Close acknowledged before final destructor completed");
                }
                "RegisterServiceTypeRequest" => {
                    let registration: TypeRequest = from_slice(&request.body).unwrap();
                    if mode_owned == "live-update" {
                        assert_eq!(registration.context.code.version.instance, 4);
                    } else {
                        assert!(mode_owned.starts_with("stateful-blocked-"));
                        assert_eq!(registration.id.name, "BlockedStateful");
                    }
                    assert_eq!(
                        registration.context.id,
                        runtime_context.as_ref().unwrap().id
                    );
                    to_vec(&ErrorReply { error: 0 }).unwrap()
                }
                _ => panic!("unexpected MODEL action {}", p.action),
            };
            if mode_owned == format!("startup-{step}") {
                write_message(&mut stream, &control("InvalidateLeaseRequest", 99, 8))
                    .await
                    .unwrap();
            }
            if mode_owned == "overflow" && step == 1 {
                for n in 1..=33 {
                    if write_message(&mut stream, &control("RAPQuery", n, 12))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if write_message(
                &mut stream,
                &Message {
                    headers: vec![Header::relates_to(p.id).unwrap()],
                    body,
                },
            )
            .await
            .is_err()
            {
                return;
            }
            if p.action == "RegisterServiceTypeRequest"
                && mode_owned.starts_with("stateful-blocked-")
            {
                let context = runtime_context.as_ref().unwrap();
                let mut open =
                    crate::lifecycle::tests::Harness::request_with_context(context, 9, 1, 0);
                open.service.application = context.code.application_name.clone();
                open.service.name = format!("{}/Blocked", context.code.application_name);
                open.service.stateful = true;
                open.service.service_type.name = "BlockedStateful".into();
                open.unit.current.configuration = (1 << 32) + 1;
                if mode_owned.starts_with("stateful-blocked-final-") {
                    open.local.id = 100;
                    open.service.initialization =
                        mssf_transport::serialization::serde::ByteArray(vec![match mode_owned
                            .as_str()
                        {
                            "stateful-blocked-final-service-close" => 232,
                            "stateful-blocked-final-service-failed-open" => 233,
                            "stateful-blocked-final-replicator-close" => 234,
                            "stateful-blocked-final-replicator-abort" => 235,
                            "stateful-blocked-final-service-abort" => 236,
                            _ => panic!("unknown final destructor model"),
                        }]);
                    stateful_request = Some(open.clone());
                }
                let message = Message {
                    headers: vec![
                        Header::actor(12).unwrap(),
                        Header::action("ReplicaOpen").unwrap(),
                        Header::message_id(MessageId {
                            guid: [9; 16],
                            index: 101,
                        })
                        .unwrap(),
                    ],
                    body: to_vec(&open).unwrap(),
                };
                write_message(&mut stream, &message).await.unwrap();
            }
            if step == 4 && !update_sent {
                update_sent = true;
                tokio::time::sleep(Duration::from_millis(100)).await;
                match mode_owned.as_str() {
                    "live-update" | "update-block" | "update-panic" => {
                        fixture.write(2, "new");
                        let mut code = runtime_context.as_ref().unwrap().code.clone();
                        code.version.instance += 1;
                        let mut message = control("UpdateCodePackageContextRequest", 99, 8);
                        message.body = to_vec(&UpdateContext {
                            code,
                            activation: CodeActivation {
                                host: runtime_context.as_ref().unwrap().host.id.clone(),
                                instance: 1234,
                            },
                            timeout: if mode_owned == "update-block" {
                                500_000
                            } else {
                                30_000_000
                            },
                        })
                        .unwrap();
                        write_message(&mut stream, &message).await.unwrap();
                    }
                    "live-invalidate" => {
                        write_message(&mut stream, &control("InvalidateLeaseRequest", 99, 8))
                            .await
                            .unwrap();
                    }
                    "live-context" => {
                        let mut message = control("UpdateCodePackageContextRequest", 99, 8);
                        // A malformed required update is terminal too, not ACKed.
                        message.body = vec![0];
                        write_message(&mut stream, &message).await.unwrap();
                    }
                    "eof" => return,
                    "lease-zero" => {
                        let next = read_message(&mut reader).await.unwrap();
                        write_message(
                            &mut stream,
                            &Message {
                                headers: vec![
                                    Header::relates_to(next.properties().unwrap().id).unwrap()
                                ],
                                body: to_vec(&LeaseReply { ttl: 0 }).unwrap(),
                            },
                        )
                        .await
                        .unwrap();
                    }
                    _ => {}
                }
            }
        }
    });
    let started = Monotonic::now();
    if mode.starts_with("stateful-blocked-") {
        use tokio::io::AsyncBufReadExt;
        let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
        tokio::time::timeout(Duration::from_secs(3), async {
            for _ in 0..16 {
                let line = lines.next_line().await.unwrap().unwrap_or_else(|| {
                    panic!("child {mode} exited without callback/destructor entry")
                });
                assert!(line.len() < 1024);
                let marker = if mode.starts_with("stateful-blocked-final-") {
                    "STATEFUL_PARTICIPANT_DROP_ENTERED"
                } else if mode == "stateful-blocked-future-drop" {
                    "STATEFUL_FUTURE_DROP_ENTERED"
                } else {
                    "STATEFUL_FACTORY_ENTERED"
                };
                if line.contains(marker) {
                    return;
                }
            }
            panic!("stateful factory entry evidence missing");
        })
        .await
        .unwrap_or_else(|_| panic!("model {mode}: callback/destructor entry never reached"));
    }
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap_or_else(|_| panic!("model {mode} exceeded process bound"))
        .unwrap();
    assert_eq!(status.code(), Some(expected), "scenario {mode}");
    if expected == 70 && mode != "lease-zero" {
        assert!(started.elapsed() < Duration::from_millis(1500), "{mode}");
    }
    if !peer.is_finished() {
        peer.abort();
    }
    if let Err(error) = peer.await {
        assert!(error.is_cancelled(), "{error}");
    }
}

#[tokio::test]
async fn successful_native_shaped_update_refreshes_later_registration() {
    scenario("live-update", 0).await;
}

#[tokio::test]
async fn blocking_subscription_capture_destruction_cannot_delay_owner_drop_fence() {
    scenario("drop-capture", 70).await;
}

#[tokio::test]
async fn blocked_stateful_factory_cannot_delay_independent_process_fence() {
    scenario("stateful-blocked-factory", 70).await;
}

#[tokio::test]
async fn stateful_callback_invocation_and_future_drop_are_supervised_with_unexpired_lease() {
    scenario("stateful-blocked-create-method", 70).await;
    scenario("stateful-blocked-future-drop", 70).await;
}

#[tokio::test]
async fn stateful_final_participant_destruction_is_fenced_before_ack_with_healthy_lease() {
    for mode in [
        "stateful-blocked-final-service-failed-open",
        "stateful-blocked-final-service-close",
        "stateful-blocked-final-service-abort",
        "stateful-blocked-final-replicator-close",
        "stateful-blocked-final-replicator-abort",
    ] {
        scenario(mode, 70).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn nonregular_artifact_open_cannot_block_startup_worker() {
    scenario("fifo-startup", 0).await;
}

#[tokio::test]
async fn uncooperative_package_callback_is_deadline_fenced_with_healthy_lease() {
    scenario("update-block", 70).await;
}

#[tokio::test]
async fn package_callback_panic_never_acknowledges_applied_success() {
    scenario("update-panic", 70).await;
}

#[tokio::test]
async fn startup_controls_cannot_grant_authority() {
    for step in 1..=4 {
        scenario(&format!("startup-{step}"), 0).await;
    }
    scenario("registration-failure", 0).await;
    scenario("overflow", 0).await;
    scenario("attach-cancel", 0).await;
}
#[tokio::test]
async fn supervisor_survives_drop_failure_cancellation_and_blocked_diagnostics() {
    for mode in [
        "drop",
        "shutdown-fail",
        "shutdown-cancel",
        "blocked-stderr",
        "executor-stall",
    ] {
        scenario(mode, 70).await;
    }
    scenario("shutdown-ok", 0).await;
}
#[tokio::test]
async fn live_invalidation_context_eof_and_zero_lease_are_terminal() {
    for mode in ["live-invalidate", "live-context", "eof", "lease-zero"] {
        scenario(mode, 70).await;
    }
}

#[test]
fn registration_records_preserve_owned_identity_and_error() {
    assert_eq!(REGISTRATION_TIMEOUT, Duration::from_secs(30));
    assert!(success(ErrorReply {
        error: 0x80004005u32 as i32 as i64
    })
    .is_err());
}
