// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use super::*;
use crate::extensions::StatefulCallbackGuard;
use crate::lease::Authority;
use std::{
    io::{Read, Write},
    process::{Command, Stdio},
    time::Instant,
};

fn partition() -> StatefulPartition {
    StatefulPartition {
        identity: ReplicaIdentity {
            service_name: "fabric:/app/service".into(),
            service_type: "StatefulType".into(),
            partition_id: uuid::Uuid::new_v4(),
            replica_id: 43,
            incarnation: 7,
            node_id: [1; 16],
            node_instance: 2,
        },
        kind: PartitionKind::Singleton,
        authority: Authority::new(Instant::now(), 1_000_000_000).unwrap(),
        valid: Arc::new(AtomicBool::new(true)),
        status: Arc::new(Mutex::new(super::tests::status())),
        reporting: None,
    }
}

#[test]
fn owned_guard_is_not_validity_or_access_and_cannot_revive_terminal_authority() {
    let partition = partition();
    partition.valid.store(false, Ordering::Release);
    let cleanup = partition
        .supervise_owned_callback(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(partition.ensure_valid(), Err(Error::Closed)));
    assert!(matches!(partition.access_status(), Err(Error::Closed)));
    drop(cleanup);
    partition.authority.retire();
    let terminal_cleanup = partition
        .supervise_owned_callback(Duration::from_secs(1))
        .unwrap();
    assert!(partition.authority.remaining().is_err());
    drop(terminal_cleanup);
    let now = Instant::now();
    assert!(partition.authority.renew(now, 1_000_000_000, now).is_err());
}

#[test]
fn public_owned_guard_deadlines_and_slots_are_bounded_and_prevent_disarm() {
    let partition = partition();
    for timeout in [
        Duration::ZERO,
        Duration::from_secs(30) + Duration::from_nanos(1),
    ] {
        assert!(matches!(
            partition.supervise_owned_callback(timeout),
            Err(Error::InvalidInput(_))
        ));
    }
    let mut guards: Vec<_> = (0..64)
        .map(|_| {
            partition
                .supervise_owned_callback(Duration::from_secs(30))
                .unwrap()
        })
        .collect();
    assert!(matches!(
        partition.supervise_owned_callback(Duration::from_secs(1)),
        Err(Error::ResourceLimit("callback deadline slots"))
    ));
    drop(guards.pop());
    guards.push(
        partition
            .supervise_owned_callback(Duration::from_secs(1))
            .unwrap(),
    );
    assert!(partition.authority.disarm().is_err());
    assert!(matches!(
        partition.supervise_owned_callback(Duration::from_secs(1)),
        Err(Error::Closed)
    ));
    drop(guards);
    assert!(partition.authority.check().is_err());
    assert!(partition
        .supervise_owned_callback(Duration::from_secs(1))
        .is_err());
}

#[test]
fn late_owned_completion_and_later_cleanup_cannot_extend_expired_deadline() {
    let partition = partition();
    let guard = partition
        .supervise_owned_callback(Duration::from_millis(5))
        .unwrap();
    std::thread::sleep(Duration::from_millis(15));
    drop(guard);
    let cleanup = partition
        .supervise_owned_callback(Duration::from_secs(30))
        .unwrap();
    assert!(partition.authority.check().is_err());
    let now = Instant::now();
    assert!(partition.authority.renew(now, 1_000_000_000, now).is_err());
    drop(cleanup);
    assert!(partition.authority.check().is_err());
}

#[test]
fn owned_callback_child() {
    let Ok(mode) = std::env::var("MSSF_OWNED_CALLBACK_CHILD") else {
        return;
    };
    let partition = partition();
    partition.authority.watchdog().unwrap();
    let authority = partition.authority.clone();
    let (ready, readiness) = std::sync::mpsc::sync_channel(0);
    std::thread::spawn(move || {
        let started = Instant::now();
        let now = Instant::now();
        authority.renew(now, 1_000_000_000, now).unwrap();
        println!("HEALTHY_LEASE_RENEWED");
        std::io::stdout().flush().unwrap();
        ready.send(()).unwrap();
        loop {
            let now = Instant::now();
            let renewed = authority.renew(now, 1_000_000_000, now).is_ok();
            if started.elapsed() >= Duration::from_millis(250) {
                assert!(
                    !renewed,
                    "callback expiry must reject otherwise healthy lease polls"
                );
                println!("HEALTHY_LEASE_POLL_AFTER_CALLBACK_EXPIRY");
                std::io::stdout().flush().unwrap();
                return;
            }
            if renewed {
                println!("HEALTHY_LEASE_RENEWED");
                std::io::stdout().flush().unwrap();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    readiness.recv().unwrap();
    let guard: StatefulCallbackGuard = partition
        .supervise_owned_callback(Duration::from_millis(80))
        .unwrap();
    if mode == "callback" {
        let _guard = guard;
        println!("OWNED_CALLBACK_ENTERED");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(Duration::from_secs(60));
    } else {
        struct BlockingFuture {
            block_poll: bool,
        }
        impl std::future::Future for BlockingFuture {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                if self.block_poll {
                    println!("OWNED_FUTURE_POLL_ENTERED");
                    std::io::stdout().flush().unwrap();
                    std::thread::sleep(Duration::from_secs(60));
                }
                std::task::Poll::Ready(())
            }
        }
        impl Drop for BlockingFuture {
            fn drop(&mut self) {
                println!("OWNED_FUTURE_DROP_ENTERED");
                std::io::stdout().flush().unwrap();
                std::thread::sleep(Duration::from_secs(60));
            }
        }
        // The explicit field order models ownership through actual disposal,
        // not an async timeout/cancellation signal.
        struct OwnedFuture {
            future: BlockingFuture,
            _guard: StatefulCallbackGuard,
        }
        impl std::future::Future for OwnedFuture {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                std::pin::Pin::new(&mut self.get_mut().future).poll(cx)
            }
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(OwnedFuture {
                future: BlockingFuture {
                    block_poll: mode == "future-poll",
                },
                _guard: guard,
            });
    }
    panic!("owned callback escaped the independent process fence");
}

#[test]
fn public_owned_callbacks_and_future_destructors_are_process_fenced_with_healthy_polls() {
    for mode in ["callback", "future-poll", "future-drop"] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stateful_api::callback_tests::owned_callback_child",
                "--nocapture",
            ])
            .env("MSSF_OWNED_CALLBACK_CHILD", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if started.elapsed() > Duration::from_secs(4) {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("{mode} escaped the bounded independent process fence");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        assert_eq!(status.code(), Some(70), "{mode}: {stdout}");
        assert!(stdout.contains("HEALTHY_LEASE_RENEWED"), "{stdout}");
        assert!(
            stdout.contains("HEALTHY_LEASE_POLL_AFTER_CALLBACK_EXPIRY"),
            "{stdout}"
        );
        assert!(
            stdout.contains(match mode {
                "callback" => "OWNED_CALLBACK_ENTERED",
                "future-poll" => "OWNED_FUTURE_POLL_ENTERED",
                _ => "OWNED_FUTURE_DROP_ENTERED",
            }),
            "{stdout}"
        );
    }
}
