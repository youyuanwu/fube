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
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
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
            {
                assert!(attached.is_err());
                return;
            }
            let runtime = attached.unwrap();
            match mode.as_str() {
                "shutdown-ok" => runtime.shutdown().await.unwrap(),
                "shutdown-fail" => assert!(runtime.shutdown().await.is_err()),
                "shutdown-cancel" => assert!(tokio::time::timeout(
                    Duration::from_millis(100),
                    runtime.shutdown()
                )
                .await
                .is_err()),
                "drop" => drop(runtime),
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
            assert_eq!(mode, "shutdown-ok", "armed fence failed");
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
    for (k, v) in [
        (
            "Fabric_ApplicationHostId",
            "443de6bf-b823-40d2-915d-0826843aeddb",
        ),
        ("Fabric_ApplicationHostType", "Activated_SingleCodePackage"),
        ("Fabric_ApplicationId", "Model_App0"),
        ("Fabric_ServicePackageName", "ModelService"),
        ("Fabric_CodePackageName", "Code"),
        ("Fabric_CodePackageInstanceSeqNum", "1"),
        ("Fabric_ServicePackageInstanceSeqNum", "2"),
        ("Fabric_ServicePackageVersionInstance", "1.0:1.0:3"),
        ("Fabric_ApplicationName", "fabric:/Model"),
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
        loop {
            let request = match read_message(&mut stream).await {
                Ok(r) => r,
                Err(_) => return,
            };
            let p = request.properties().unwrap();
            if p.relates_to != MessageId::default() {
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
                    to_vec(&ErrorReply { error: 0 }).unwrap()
                }
                "" if p.actor == 15 => {
                    assert_eq!(step, 3);
                    step = 4;
                    to_vec(&LeaseReply { ttl: 300_000_000 }).unwrap()
                }
                "UnregisterFabricRuntimeRequest" | "UnregisterApplicationHostRequest" => {
                    if mode_owned != "shutdown-ok" {
                        continue;
                    }
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
            if step == 4 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                match mode_owned.as_str() {
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
                        let next = read_message(&mut stream).await.unwrap();
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
    for mode in ["drop", "shutdown-fail", "shutdown-cancel", "blocked-stderr"] {
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
