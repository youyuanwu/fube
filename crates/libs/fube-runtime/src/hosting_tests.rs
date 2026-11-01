//! Actual runtime child against production Hosting, independently observed TCP
//! frames. This is not native-library or original native-node qualification.
use super::*;
use fube_hosting::{ipc, wire as hosting_wire};
use fube_transport::{
    connection::{read_message, write_message},
    message::{Message, MessageId},
    serialization::serde::{from_slice, to_vec, ObjectArray, Pointer},
};
use std::{collections::HashMap, io::Write, process::Stdio, time::Instant};
use tokio::{
    io::AsyncBufReadExt,
    net::{TcpListener, TcpStream},
    process::Command,
    sync::mpsc,
    task::JoinSet,
};

#[test]
fn hosting_child() {
    let Ok(mode) = std::env::var("FUBE_HOSTING_CHILD") else {
        return;
    };
    let mut builder = if mode == "destructor" {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2);
        builder
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().build().unwrap().block_on(async {
        let runtime = Runtime::attach().await.unwrap();
        println!("hosting-attached");
        std::io::stdout().flush().unwrap();
        match mode.as_str() {
            "bootstrap" => {
                tokio::time::sleep(Duration::from_millis(2200)).await;
                runtime.shutdown().await.unwrap();
            }
            "expiry" => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                panic!("finite Hosting authority did not fence");
            }
            "executor" => {
                println!("hosting-executor-entered");
                std::io::stdout().flush().unwrap();
                std::thread::sleep(Duration::from_secs(2));
                panic!("blocked caller defeated Hosting authority fence");
            }
            "destructor" => {
                struct Capture;
                impl Drop for Capture {
                    fn drop(&mut self) {
                        println!("hosting-destructor-entered");
                        std::io::stdout().flush().unwrap();
                        std::thread::sleep(Duration::from_secs(60));
                    }
                }
                let capture = Capture;
                let _registration = runtime
                    .context_handle()
                    .subscribe_configuration(move |_| {
                        let _ = &capture;
                    })
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(600)).await;
                drop(runtime);
                tokio::time::sleep(Duration::from_secs(2)).await;
                panic!("blocked capture destruction defeated owned fatal fence");
            }
            _ => panic!("unrecognized Hosting child mode"),
        }
    });
}

enum Observation {
    Request(Message),
    Reply(Message),
}

fn node(root: &std::path::Path) -> hosting_wire::StartReply {
    hosting_wire::StartReply {
        node_id: "1".into(),
        node_pid: std::process::id(),
        lease_handle: 0,
        lease_duration: 0,
        error: 0,
        node_instance: 1,
        node_name: "hosting-local".into(),
        node_type: "local".into(),
        client_address: "127.0.0.1:19000".into(),
        deployment: root.to_str().unwrap().into(),
        ip: "127.0.0.1".into(),
        work: root.to_str().unwrap().into(),
        initial_ttl: 0,
        application_directories: ObjectArray(vec![]),
        node_directories: ObjectArray(vec![]),
        application_log: Pointer(None),
        system_log: Pointer(None),
    }
}

async fn marker(reader: &mut tokio::io::BufReader<tokio::process::ChildStdout>, expected: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..16 {
            let mut line = String::new();
            assert!(
                reader.read_line(&mut line).await.unwrap() > 0,
                "child ended before {expected}"
            );
            assert!(line.len() <= 4096);
            if line.trim() == expected {
                return;
            }
        }
        panic!("missing child marker {expected}");
    })
    .await
    .unwrap();
}

async fn scenario(mode: &str) {
    let started = Instant::now();
    let fixture = crate::packages::tests::Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut server = ipc::Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        ipc::Config {
            node: node(&fixture.root),
            core_limits: Default::default(),
            max_admissions: 1,
            scratch_bytes: 8 * 1024 * 1024,
            decode_bytes: 256 * 1024,
            projection_bytes: 256 * 1024,
            reply_bytes: 64 * 1024,
            lease_duration: if mode == "expiry" {
                Duration::from_millis(500)
            } else {
                ipc::LOCAL_LEASE_DURATION
            },
        },
    )
    .unwrap();
    let mut environment = crate::context::tests::env();
    environment.insert(
        "Fabric_RuntimeConnectionAddress".into(),
        listener.local_addr().unwrap().to_string(),
    );
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "hosting_tests::hosting_child", "--nocapture"])
        .env("FUBE_HOSTING_CHILD", mode)
        .envs(&environment)
        .env_remove("Fabric_ServicePackageActivationGuid")
        .env_remove("Fabric_ServicePackageActivationId")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let pid = child.id().unwrap();
    let mut expected: hosting_wire::RuntimeContext =
        from_slice(&to_vec(&fixture.activation.runtime).unwrap()).unwrap();
    expected.host.pid = pid;
    let lease = server
        .admit(ipc::Admission {
            host: expected.host,
            code: expected.code,
            deadlines: ipc::RequestDeadlines::Propagated,
        })
        .unwrap();
    let address = server.address();
    let (observations, mut received) = mpsc::channel(64);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let hosting = TcpStream::connect(address).await.unwrap();
        let (mut cr, mut cw) = client.into_split();
        let (mut hr, mut hw) = hosting.into_split();
        let requests = observations.clone();
        let upstream = async move {
            while let Ok(message) = read_message(&mut cr).await {
                requests
                    .try_send(Observation::Request(message.clone()))
                    .unwrap();
                if write_message(&mut hw, &message).await.is_err() {
                    break;
                }
            }
        };
        let downstream = async move {
            while let Ok(message) = read_message(&mut hr).await {
                observations
                    .try_send(Observation::Reply(message.clone()))
                    .unwrap();
                if write_message(&mut cw, &message).await.is_err() {
                    break;
                }
            }
        };
        // One owned task, two directly owned futures: cancelling either path
        // discards both partially read/written streams, with no nested tasks.
        tokio::select! { _ = upstream => {}, _ = downstream => {} }
    });
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().unwrap());
    let mut correlations: HashMap<MessageId, (i64, String)> = HashMap::new();
    let mut actions = Vec::new();
    let mut leases = Vec::new();
    // Observe first actual correlated finite reply, not NodeContext cached flags.
    tokio::time::timeout(Duration::from_secs(3), async {
        while leases.is_empty() {
            let observation = received.recv().await.expect("protocol observation closed");
            observe(observation, &mut correlations, &mut actions, &mut leases);
        }
    })
    .await
    .unwrap();
    marker(&mut stdout, "hosting-attached").await;
    match mode {
        "expiry" => lease.stop_renewal(),
        "executor" => {
            marker(&mut stdout, "hosting-executor-entered").await;
            lease.revoke();
        }
        "destructor" => {
            marker(&mut stdout, "hosting-destructor-entered").await;
            lease.revoke();
        }
        "bootstrap" => {}
        _ => unreachable!(),
    }
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    let expected_exit = if mode == "bootstrap" { 0 } else { 70 };
    assert_eq!(status.code(), Some(expected_exit));
    if expected_exit == 70 {
        assert!(started.elapsed() < Duration::from_millis(1500));
    }
    tasks.join_next().await.unwrap().unwrap();
    while let Ok(observation) = received.try_recv() {
        observe(observation, &mut correlations, &mut actions, &mut leases);
    }
    assert!(leases.iter().all(|ttl| *ttl > 0 && *ttl <= 300_000_000));
    assert_eq!(
        &actions[..3],
        &[
            (7, "StartRegisterApplicationHostRequest".to_owned()),
            (7, "FinishRegisterApplicationHostRequest".to_owned()),
            (9, "RegisterFabricRuntimeRequest".to_owned()),
        ]
    );
    if mode == "bootstrap" {
        assert!(leases.len() >= 2);
        assert!(actions.contains(&(9, "UnregisterFabricRuntimeRequest".into())));
        assert!(actions.contains(&(7, "UnregisterApplicationHostRequest".into())));
    }
    server.stop().unwrap();
    let output = server.take_outputs().unwrap();
    assert!(!output.events.is_empty());
    assert!(output.operations.iter().all(|value| value.primary.is_ok()));
    println!(
        "production Hosting mode={mode}: observed finite polls={}, child exit={expected_exit}",
        leases.len()
    );
}

fn observe(
    observation: Observation,
    correlations: &mut HashMap<MessageId, (i64, String)>,
    actions: &mut Vec<(i64, String)>,
    leases: &mut Vec<i64>,
) {
    match observation {
        Observation::Request(message) => {
            let p = message.properties().unwrap();
            if p.actor == 1 {
                return;
            }
            assert!(correlations
                .insert(p.id, (p.actor, p.action.clone()))
                .is_none());
            if p.actor == 15 {
                assert!(p.action.is_empty() && message.body.is_empty());
            } else {
                actions.push((p.actor, p.action));
            }
        }
        Observation::Reply(message) => {
            let p = message.properties().unwrap();
            if p.actor == 1 {
                return;
            }
            let (actor, _) = correlations
                .remove(&p.relates_to)
                .expect("uncorrelated actual reply");
            assert!(p.action.is_empty());
            if actor == 15 {
                leases.push(from_slice::<wire::LeaseReply>(&message.body).unwrap().ttl);
            }
        }
    }
}

#[tokio::test]
async fn hosting_bootstrap_shutdown() {
    scenario("bootstrap").await;
}
#[tokio::test]
async fn hosting_stop_renewal() {
    scenario("expiry").await;
}
#[tokio::test]
async fn hosting_revoke_blocked_executor() {
    scenario("executor").await;
}
#[tokio::test]
async fn hosting_revoke_blocked_destructor() {
    scenario("destructor").await;
}
