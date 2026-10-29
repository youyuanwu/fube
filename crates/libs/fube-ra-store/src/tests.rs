use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use tokio::sync::oneshot;

#[derive(Default)]
pub(crate) struct Hooks {
    gate: Mutex<Option<Gate>>,
    cleanup: Mutex<Option<Gate>>,
    fail: AtomicBool,
}
struct Gate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}
impl Hooks {
    pub(crate) fn before_blocking(&self) -> Result<()> {
        let gate = supervisor::lock(&self.gate).take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.release.blocking_recv();
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Io, "injected logical commit veto"));
        }
        Ok(())
    }
    pub(crate) fn cleanup_blocking(&self) {
        if let Some(gate) = supervisor::lock(&self.cleanup).take() {
            let _ = gate.entered.send(());
            let _ = gate.release.blocking_recv();
        }
    }
    fn pause(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, reached) = oneshot::channel();
        let (release, released) = oneshot::channel();
        *supervisor::lock(&self.gate) = Some(Gate {
            entered,
            release: released,
        });
        (reached, release)
    }
}
fn key(t: &str, id: &str) -> RecordId {
    RecordId::new(t, id)
}
fn insert(t: &str, id: &str, bytes: &[u8]) -> Request {
    Request::Insert(Record {
        key: key(t, id),
        value: bytes.to_vec(),
    })
}
async fn execute(store: &dyn KeyValueStore, request: Request) -> Result<std::sync::Arc<Output>> {
    store.submit(request)?.wait().await
}
async fn snapshot(store: &dyn KeyValueStore, t: &str) -> Vec<Record> {
    match execute(store, Request::Enumerate(t.into()))
        .await
        .unwrap()
        .as_ref()
    {
        Output::Snapshot(rows) => rows.clone(),
        _ => panic!("not snapshot"),
    }
}
async fn contract(store: &Store) {
    assert!(snapshot(store, "a").await.is_empty());
    execute(store, insert("a", "1", b"first")).await.unwrap();
    assert_eq!(
        execute(store, insert("a", "1", b"duplicate"))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    assert_eq!(snapshot(store, "a").await[0].value, b"first");
    execute(store, insert("b", "1", b"other type"))
        .await
        .unwrap();
    execute(store, insert("a", "2", b"other id")).await.unwrap();
    assert_eq!(snapshot(store, "a").await.len(), 2);
    assert_eq!(snapshot(store, "b").await[0].value, b"other type");
    execute(
        store,
        Request::Update(Record {
            key: key("a", "1"),
            value: b"new".to_vec(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(snapshot(store, "a").await[0].value, b"new");
    for request in [
        Request::Update(Record {
            key: key("a", "missing"),
            value: vec![],
        }),
        Request::Delete(key("a", "missing")),
    ] {
        assert_eq!(
            execute(store, request).await.unwrap_err().kind,
            ErrorKind::NotFound
        );
    }
    execute(store, Request::Delete(key("a", "1")))
        .await
        .unwrap();
    assert_eq!(snapshot(store, "a").await.len(), 1);
    let first = store.submit(insert("race", "same", b"x")).unwrap();
    let second = store.submit(insert("race", "same", b"y")).unwrap();
    let (a, b) = tokio::join!(first.wait(), second.wait());
    assert!(a.is_ok());
    assert_eq!(b.unwrap_err().kind, ErrorKind::Conflict);
    assert_eq!(snapshot(store, "race").await[0].value, b"x");
}

#[tokio::test]
async fn memory_contract() {
    let store = Store::memory(Settings::default()).await.unwrap();
    contract(&store).await;
    let report = store.close().await.unwrap();
    assert_eq!(
        report.accepted,
        report.succeeded + report.failed + report.not_executed
    );
    assert_eq!(report.first_failure.unwrap().kind, ErrorKind::Conflict);
    assert_eq!(
        store.submit(insert("a", "3", b"")).err().unwrap().kind,
        ErrorKind::Closed
    );
}

#[tokio::test]
async fn memory_staged_failure() {
    let store = Store::memory(Settings::default()).await.unwrap();
    execute(&store, insert("a", "1", b"old")).await.unwrap();
    store.shared().hooks.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        execute(
            &store,
            Request::Update(Record {
                key: key("a", "1"),
                value: b"new".to_vec()
            })
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::Io
    );
    assert_eq!(snapshot(&store, "a").await[0].value, b"old");
    store.close().await.unwrap();
}

#[tokio::test]
async fn memory_cancellation_retains_count_and_bytes_and_close_drains() {
    let settings = Settings {
        max_outstanding: 1,
        ..Settings::default()
    };
    let store = Store::memory(settings).await.unwrap();
    let (entered, release) = store.shared().hooks.pause();
    let operation = store.submit(insert("a", "1", b"committed")).unwrap();
    entered.await.unwrap();
    let charged = store.stats().queued_bytes;
    assert!(charged >= api::REQUEST_OVERHEAD + 9);
    drop(operation);
    assert_eq!(store.stats().outstanding, 1);
    assert_eq!(
        store.submit(insert("a", "2", b"")).err().unwrap().kind,
        ErrorKind::Capacity
    );
    let closing = store.close();
    tokio::pin!(closing);
    tokio::select! { _ = &mut closing => panic!("close must wait"), _ = tokio::task::yield_now() => {} }
    assert_eq!(
        store.submit(insert("a", "2", b"")).err().unwrap().kind,
        ErrorKind::Closed
    );
    release.send(()).unwrap();
    let report = closing.await.unwrap();
    assert!(report.is_clean());
    assert_eq!(report.succeeded, 1);
    assert_eq!(store.stats().queued_bytes, 0);
}

#[tokio::test]
async fn memory_timeout_fences_and_late_success_resolves() {
    let settings = Settings {
        operation_timeout: std::time::Duration::from_millis(50),
        close_timeout: std::time::Duration::from_millis(30),
        ..Settings::default()
    };
    let store = Store::memory(settings).await.unwrap();
    let (entered, release) = store.shared().hooks.pause();
    let first = store.submit(insert("a", "1", b"late")).unwrap();
    entered.await.unwrap();
    let queued = store.submit(insert("a", "2", b"never")).unwrap();
    assert_eq!(first.wait().await.unwrap_err().kind, ErrorKind::Unknown);
    assert_eq!(
        queued.wait().await.unwrap_err().kind,
        ErrorKind::NotExecuted
    );
    assert_eq!(store.stats().outstanding, 1);
    assert_eq!(
        store.submit(insert("a", "3", b"")).err().unwrap().kind,
        ErrorKind::Fenced
    );
    assert_eq!(store.close().await.unwrap_err().kind, ErrorKind::Unknown);
    release.send(()).unwrap();
    first
        .resolve(std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        queued
            .resolve(std::time::Duration::from_secs(2))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotExecuted
    );
    assert!(!store.close().await.unwrap().is_clean());
}

#[tokio::test]
async fn memory_bounds() {
    let settings = Settings {
        max_type_bytes: 2,
        max_id_bytes: 2,
        max_value_bytes: 4,
        max_records: 1,
        max_snapshot_bytes: 130,
        max_stored_bytes: 134,
        max_queued_bytes: 600,
        ..Settings::default()
    };
    let store = Store::memory(settings).await.unwrap();
    for request in [
        insert("abc", "1", b""),
        insert("a", "123", b""),
        insert("a", "1", b"12345"),
        insert("", "1", b""),
        insert("a", "\0", b""),
    ] {
        assert_eq!(
            store.submit(request).err().unwrap().kind,
            ErrorKind::InvalidInput
        );
    }
    let mut huge = Vec::with_capacity(1024);
    huge.push(1);
    assert_eq!(
        store
            .submit(Request::Insert(Record {
                key: key("a", "1"),
                value: huge
            }))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Capacity
    );
    execute(&store, insert("a", "1", b"1234")).await.unwrap();
    assert_eq!(
        execute(&store, insert("a", "2", b""))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    assert_eq!(
        execute(&store, Request::Enumerate("a".into()))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    store.close().await.unwrap();
}

#[test]
fn caller_runtime_loss_does_not_cancel_accepted_memory_commit() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (store, ticket, release) = rt.block_on(async {
        let store = Store::memory(Settings::default()).await.unwrap();
        let (entered, release) = store.shared().hooks.pause();
        let ticket = store
            .submit(insert("a", "1", b"durable only in memory"))
            .unwrap();
        entered.await.unwrap();
        (store, ticket, release)
    });
    drop(rt);
    release.send(()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        ticket.wait().await.unwrap();
        assert_eq!(snapshot(&store, "a").await.len(), 1);
        assert!(store.close().await.unwrap().is_clean());
    });
}

#[tokio::test]
async fn memory_snapshot_is_committed_complete_and_owned() {
    let store = Store::memory(Settings::default()).await.unwrap();
    execute(&store, insert("a", "1", b"before")).await.unwrap();
    let prior = snapshot(&store, "a").await;
    let (entered, release) = store.shared().hooks.pause();
    let update = store
        .submit(Request::Update(Record {
            key: key("a", "1"),
            value: b"after".to_vec(),
        }))
        .unwrap();
    entered.await.unwrap();
    let read = store.submit(Request::Enumerate("a".into())).unwrap();
    assert!(matches!(read.status(), Status::Pending));
    release.send(()).unwrap();
    update.wait().await.unwrap();
    let output = read.wait().await.unwrap();
    assert_eq!(prior[0].value, b"before");
    assert_eq!(
        output.as_ref(),
        &Output::Snapshot(vec![Record {
            key: key("a", "1"),
            value: b"after".to_vec()
        }])
    );
    store.close().await.unwrap();
}

#[tokio::test]
async fn memory_byte_charge_retained_when_wait_is_cancelled() {
    let request = insert("a", "1", b"1234");
    let bytes = request.charge(&Settings::default()).unwrap();
    let settings = Settings {
        max_outstanding: 10,
        max_queued_bytes: bytes,
        ..Settings::default()
    };
    let store = Store::memory(settings).await.unwrap();
    let (entered, release) = store.shared().hooks.pause();
    let operation = store.submit(request).unwrap();
    entered.await.unwrap();
    let mut waiter = Box::pin(operation.wait());
    tokio::select! { _ = &mut waiter => panic!("blocked"), _ = tokio::task::yield_now() => {} }
    drop(waiter);
    assert_eq!(store.stats().queued_bytes, bytes);
    assert_eq!(
        store.submit(insert("a", "2", b"")).err().unwrap().kind,
        ErrorKind::Capacity
    );
    release.send(()).unwrap();
    operation.wait().await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn memory_all_mutation_failures_preserve_committed_state() {
    let store = Store::memory(Settings::default()).await.unwrap();
    execute(&store, insert("a", "1", b"old")).await.unwrap();
    for request in [
        insert("a", "2", b"new"),
        Request::Update(Record {
            key: key("a", "1"),
            value: b"new".to_vec(),
        }),
        Request::Delete(key("a", "1")),
    ] {
        store.shared().hooks.fail.store(true, Ordering::SeqCst);
        assert_eq!(
            execute(&store, request).await.unwrap_err().kind,
            ErrorKind::Io
        );
        assert_eq!(
            snapshot(&store, "a").await,
            vec![Record {
                key: key("a", "1"),
                value: b"old".to_vec()
            }]
        );
    }
    store.close().await.unwrap();
}

#[tokio::test]
async fn invalid_settings_rejected() {
    for settings in [
        Settings {
            max_outstanding: 0,
            ..Settings::default()
        },
        Settings {
            max_queued_bytes: usize::MAX,
            ..Settings::default()
        },
        Settings {
            operation_timeout: std::time::Duration::ZERO,
            ..Settings::default()
        },
        Settings {
            busy_timeout: std::time::Duration::MAX,
            ..Settings::default()
        },
    ] {
        assert_eq!(
            Store::memory(settings).await.err().unwrap().kind,
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn memory_independent_record_and_stored_byte_bounds() {
    let count = Store::memory(Settings {
        max_records: 1,
        ..Settings::default()
    })
    .await
    .unwrap();
    execute(&count, insert("a", "1", b"x")).await.unwrap();
    assert_eq!(
        execute(&count, insert("a", "2", b""))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    execute(&count, Request::Delete(key("a", "1")))
        .await
        .unwrap();
    execute(&count, insert("a", "2", b"")).await.unwrap();
    count.close().await.unwrap();

    let bytes = Store::memory(Settings {
        max_stored_bytes: 134,
        ..Settings::default()
    })
    .await
    .unwrap();
    execute(&bytes, insert("a", "1", b"x")).await.unwrap();
    assert_eq!(
        execute(
            &bytes,
            Request::Update(Record {
                key: key("a", "1"),
                value: b"12345".to_vec()
            })
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::Capacity
    );
    assert_eq!(snapshot(&bytes, "a").await[0].value, b"x");
    assert_eq!(
        execute(&bytes, insert("a", "2", b""))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    execute(&bytes, Request::Delete(key("a", "1")))
        .await
        .unwrap();
    execute(&bytes, insert("a", "2", b"1234")).await.unwrap();
    bytes.close().await.unwrap();
}

#[tokio::test]
async fn memory_cleanup_timeout_abandoned_and_concurrent_closers() {
    let store = Store::memory(Settings {
        close_timeout: std::time::Duration::from_millis(20),
        ..Settings::default()
    })
    .await
    .unwrap();
    let (entered, reached) = oneshot::channel();
    let (release, released) = oneshot::channel();
    *supervisor::lock(&store.shared().hooks.cleanup) = Some(Gate {
        entered,
        release: released,
    });
    let mut abandoned = Box::pin(store.close());
    tokio::select! { _ = &mut abandoned => panic!("cleanup blocked"), _ = reached => {} }
    drop(abandoned);
    let (a, b) = tokio::join!(store.close(), store.close());
    assert_eq!(a.unwrap_err().kind, ErrorKind::Unknown);
    assert_eq!(b.unwrap_err().kind, ErrorKind::Unknown);
    release.send(()).unwrap();
    let (a, b) = tokio::join!(store.close(), store.close());
    assert!(a.unwrap().is_clean());
    assert!(b.unwrap().is_clean());
}
