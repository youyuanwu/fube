use super::{tests::*, *};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_rusqlite::rusqlite::Connection as Raw;

static NEXT: AtomicU64 = AtomicU64::new(0);
pub(crate) struct Directory(pub(crate) PathBuf);
impl Directory {
    pub(crate) fn new() -> Self {
        // Cargo/nextest supplies the runtime (workspace-remapped) package path.
        // A compile-time env! would write into the archive producer's checkout.
        let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
            .expect("run fixtures through Cargo/nextest with a runtime manifest directory");
        let root = PathBuf::from(manifest_dir).join("../../../build/ra-store-tests");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
    pub(crate) fn db(&self) -> PathBuf {
        self.0.join("ra.sqlite")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn identity() -> Identity {
    Identity {
        deployment: "deployment-1".into(),
        node: "node-7".into(),
    }
}
async fn create(dir: &Directory, settings: Settings) -> Store {
    Store::new(SqliteBackend::create_new(dir.db(), identity()), settings)
        .await
        .unwrap()
}
async fn open(dir: &Directory, settings: Settings) -> Store {
    Store::new(SqliteBackend::existing(dir.db(), identity()), settings)
        .await
        .unwrap()
}

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    Sqlite,
}
impl Backend {
    async fn store(self, settings: Settings) -> (Store, Option<Directory>) {
        match self {
            Self::Memory => (
                Store::new(MemoryBackend::new(), settings).await.unwrap(),
                None,
            ),
            Self::Sqlite => {
                let dir = Directory::new();
                let store = create(&dir, settings).await;
                (store, Some(dir))
            }
        }
    }
}
const BOTH: [Backend; 2] = [Backend::Memory, Backend::Sqlite];

#[tokio::test]
async fn shared_crud_isolation_race_and_owned_snapshots() {
    for backend in BOTH {
        let (store, _dir) = backend.store(Settings::default()).await;
        contract(&store).await;
        let prior = snapshot(&store, "race").await;
        let (entered, release) = store.shared().hooks.pause();
        let update = store
            .submit(Request::Update(Record {
                key: key("race", "same"),
                value: b"after".to_vec(),
            }))
            .unwrap();
        entered.await.unwrap();
        let read = store.submit(Request::Enumerate("race".into())).unwrap();
        assert!(matches!(read.status(), Status::Pending));
        release.send(()).unwrap();
        update.wait().await.unwrap();
        assert_eq!(prior[0].value, b"x");
        assert_eq!(
            read.wait().await.unwrap().as_ref(),
            &Output::Snapshot(vec![Record {
                key: key("race", "same"),
                value: b"after".to_vec()
            }])
        );
        execute(&store, Request::Maintain).await.unwrap();
        let report = store.close().await.unwrap();
        assert_eq!(
            report.accepted,
            report.succeeded + report.failed + report.not_executed
        );
    }
}

#[tokio::test]
async fn shared_staged_failure_all_mutations_preserve_content() {
    for backend in BOTH {
        let (store, _dir) = backend.store(Settings::default()).await;
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
}

#[tokio::test]
async fn shared_independent_limits_and_no_partial_snapshot() {
    for backend in BOTH {
        let (store, _dir) = backend
            .store(Settings {
                max_records: 1,
                ..Settings::default()
            })
            .await;
        execute(&store, insert("a", "1", b"")).await.unwrap();
        assert_eq!(
            execute(&store, insert("a", "2", b""))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Capacity
        );
        store.close().await.unwrap();
        let (store, _dir) = backend
            .store(Settings {
                max_stored_bytes: 134,
                max_snapshot_bytes: 130,
                ..Settings::default()
            })
            .await;
        execute(&store, insert("a", "1", b"x")).await.unwrap();
        assert_eq!(
            execute(&store, insert("a", "2", b""))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Capacity
        );
        assert_eq!(
            execute(
                &store,
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
        assert_eq!(
            execute(&store, Request::Enumerate("a".into()))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Capacity
        );
        execute(&store, Request::Delete(key("a", "1")))
            .await
            .unwrap();
        execute(&store, insert("a", "2", b"")).await.unwrap();
        assert_eq!(snapshot(&store, "a").await[0].value, b"");
        store.close().await.unwrap();
        let (store, _dir) = backend
            .store(Settings {
                max_type_bytes: 2,
                max_id_bytes: 2,
                max_value_bytes: 4,
                max_queued_bytes: 600,
                ..Settings::default()
            })
            .await;
        for r in [
            insert("abc", "1", b""),
            insert("a", "123", b""),
            insert("a", "1", b"12345"),
            insert("", "1", b""),
            insert("a", "\0", b""),
        ] {
            assert_eq!(store.submit(r).err().unwrap().kind, ErrorKind::InvalidInput);
        }
        let value = Vec::with_capacity(1024);
        assert_eq!(
            store
                .submit(Request::Insert(Record {
                    key: key("a", "1"),
                    value
                }))
                .err()
                .unwrap()
                .kind,
            ErrorKind::Capacity
        );
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn shared_cancelled_queued_and_inflight_charges_close_admission() {
    for backend in BOTH {
        let (store, _dir) = backend
            .store(Settings {
                max_outstanding: 2,
                ..Settings::default()
            })
            .await;
        let (entered, release) = store.shared().hooks.pause();
        let running = store.submit(insert("a", "1", b"first")).unwrap();
        entered.await.unwrap();
        let queued = store.submit(insert("a", "2", b"second")).unwrap();
        let bytes = store.stats().queued_bytes;
        drop(queued);
        let mut wait = Box::pin(running.wait());
        tokio::select! { _ = &mut wait => panic!("blocked"), _ = tokio::task::yield_now()=>{} }
        drop(wait);
        assert_eq!(store.stats().queued_bytes, bytes);
        assert_eq!(
            store.submit(insert("a", "3", b"")).err().unwrap().kind,
            ErrorKind::Capacity
        );
        let mut close = Box::pin(store.close());
        tokio::select! { _ = &mut close => panic!("blocked close"), _=tokio::task::yield_now()=>{} }
        assert_eq!(
            store.submit(insert("a", "3", b"")).err().unwrap().kind,
            ErrorKind::Closed
        );
        drop(close);
        release.send(()).unwrap();
        let report = store.close().await.unwrap();
        assert_eq!(report.succeeded, 2);
        assert_eq!(store.stats().queued_bytes, 0);
    }
}

#[tokio::test]
async fn shared_byte_budget_cannot_be_released_by_abandonment() {
    for backend in BOTH {
        let bytes = insert("a", "1", b"data")
            .charge(&Settings::default())
            .unwrap();
        let (store, _dir) = backend
            .store(Settings {
                max_queued_bytes: bytes,
                max_outstanding: 10,
                ..Settings::default()
            })
            .await;
        let (entered, release) = store.shared().hooks.pause();
        drop(store.submit(insert("a", "1", b"data")).unwrap());
        entered.await.unwrap();
        assert_eq!(store.stats().queued_bytes, bytes);
        assert_eq!(
            store.submit(insert("a", "2", b"")).err().unwrap().kind,
            ErrorKind::Capacity
        );
        release.send(()).unwrap();
        assert_eq!(store.close().await.unwrap().succeeded, 1);
    }
}

#[tokio::test]
async fn shared_deadline_includes_queue_and_late_result_is_resolvable() {
    for backend in BOTH {
        let (store, _dir) = backend
            .store(Settings {
                operation_timeout: Duration::from_millis(100),
                close_timeout: Duration::from_millis(20),
                ..Settings::default()
            })
            .await;
        let (entered, release) = store.shared().hooks.pause();
        let current = store.submit(insert("a", "1", b"late")).unwrap();
        entered.await.unwrap();
        let queued = store.submit(insert("a", "2", b"never")).unwrap();
        assert_eq!(current.wait().await.unwrap_err().kind, ErrorKind::Unknown);
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
        current.resolve(Duration::from_secs(2)).await.unwrap();
        let report = store.close().await.unwrap();
        assert_eq!((report.succeeded, report.not_executed), (1, 1));
        assert!(!report.is_clean());
    }
}

#[tokio::test]
async fn sqlite_reopen_identity_schema_profile_and_committed_bytes() {
    let dir = Directory::new();
    let store = create(&dir, Settings::default()).await;
    execute(&store, insert("a", "1", b"committed"))
        .await
        .unwrap();
    assert!(store.close().await.unwrap().is_clean());
    let path = dir.db();
    tokio::task::spawn_blocking(move || {
        let conn = Raw::open(path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert_eq!(
            conn.query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    })
    .await
    .unwrap();
    let store = open(&dir, Settings::default()).await;
    assert_eq!(snapshot(&store, "a").await[0].value, b"committed");
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Busy
    );
    store.close().await.unwrap();
    assert_eq!(
        Store::create_new(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Conflict
    );
    let wrong = Identity {
        node: "other".into(),
        ..identity()
    };
    assert_eq!(
        Store::open_existing(dir.db(), wrong, Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    let wrong = Identity {
        deployment: "other".into(),
        ..identity()
    };
    assert_eq!(
        Store::open_existing(dir.db(), wrong, Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    let store = open(&dir, Settings::default()).await;
    assert_eq!(snapshot(&store, "a").await.len(), 1);
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_open_missing_empty_foreign_corrupt_incompatible_and_reduced_budgets() {
    let dir = Directory::new();
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::NotFound
    );
    assert!(!dir.db().exists());
    fs::write(dir.db(), []).unwrap();
    fs::set_permissions(dir.db(), fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    fs::write(dir.db(), b"this is not SQLite").unwrap();
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Corrupt
    );
    fs::remove_file(dir.db()).unwrap();
    let path = dir.db();
    tokio::task::spawn_blocking(move || {
        Raw::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE foreign_table(x)")
            .unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    fs::remove_file(dir.db()).unwrap();
    let store = create(&dir, Settings::default()).await;
    execute(&store, insert("a", "1", b"12345")).await.unwrap();
    execute(&store, insert("a", "2", b"")).await.unwrap();
    store.close().await.unwrap();
    for settings in [
        Settings {
            max_records: 1,
            ..Settings::default()
        },
        Settings {
            max_stored_bytes: 134,
            ..Settings::default()
        },
        Settings {
            max_value_bytes: 4,
            ..Settings::default()
        },
    ] {
        assert_eq!(
            Store::open_existing(dir.db(), identity(), settings)
                .await
                .err()
                .unwrap()
                .kind,
            ErrorKind::Capacity
        );
    }
    let path = dir.db();
    tokio::task::spawn_blocking(move || {
        Raw::open(path)
            .unwrap()
            .execute_batch("PRAGMA user_version=99")
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
}

#[tokio::test]
async fn sqlite_real_commit_veto_is_not_conflict_and_reopen_keeps_old_content() {
    let dir = Directory::new();
    let store = create(&dir, Settings::default()).await;
    execute(&store, insert("a", "1", b"old")).await.unwrap();
    store
        .shared()
        .hooks
        .commit_veto
        .store(true, Ordering::SeqCst);
    let result = execute(
        &store,
        Request::Update(Record {
            key: key("a", "1"),
            value: b"new".to_vec(),
        }),
    )
    .await;
    assert_eq!(result.unwrap_err().kind, ErrorKind::Sqlite);
    assert_eq!(snapshot(&store, "a").await[0].value, b"old");
    assert_eq!(
        store.close().await.unwrap().first_failure.unwrap().kind,
        ErrorKind::Sqlite
    );
    let store = open(&dir, Settings::default()).await;
    assert_eq!(snapshot(&store, "a").await[0].value, b"old");
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_busy_transaction_is_finite_and_nonmutating() {
    let dir = Directory::new();
    let store = create(
        &dir,
        Settings {
            busy_timeout: Duration::from_millis(20),
            ..Settings::default()
        },
    )
    .await;
    let path = dir.db();
    let (release, released) = std::sync::mpsc::channel();
    let (entered, reached) = tokio::sync::oneshot::channel();
    let lock = tokio::task::spawn_blocking(move || {
        let conn = Raw::open(path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        entered.send(()).unwrap();
        released.recv().unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
    });
    reached.await.unwrap();
    assert_eq!(
        execute(&store, insert("a", "1", b"blocked"))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Busy
    );
    release.send(()).unwrap();
    lock.await.unwrap();
    assert!(snapshot(&store, "a").await.is_empty());
    execute(&store, insert("a", "1", b"ok")).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_post_commit_panic_is_unknown_not_rollback() {
    let dir = Directory::new();
    let store = create(&dir, Settings::default()).await;
    store
        .shared()
        .hooks
        .panic_after_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        execute(&store, insert("a", "1", b"really committed"))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unknown
    );
    assert!(matches!(
        store.submit(insert("a", "2", b"")).err().unwrap().kind,
        ErrorKind::Fenced | ErrorKind::Closed
    ));
    assert!(!store.close().await.unwrap().is_clean());
    let store = open(&dir, Settings::default()).await;
    assert_eq!(snapshot(&store, "a").await[0].value, b"really committed");
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_wal_pressure_and_pinned_reader_refuse_then_maintenance_recovers() {
    let dir = Directory::new();
    let store = create(
        &dir,
        Settings {
            max_wal_bytes: 16384,
            busy_timeout: Duration::from_millis(20),
            ..Settings::default()
        },
    )
    .await;
    execute(&store, Request::Maintain).await.unwrap();
    execute(&store, insert("a", "seed", b"seed")).await.unwrap();
    execute(&store, Request::Maintain).await.unwrap();
    let path = dir.db();
    let (release, released) = std::sync::mpsc::channel();
    let (entered, reached) = tokio::sync::oneshot::channel();
    let reader = tokio::task::spawn_blocking(move || {
        let conn = Raw::open(path).unwrap();
        conn.execute_batch("BEGIN; SELECT * FROM records").unwrap();
        entered.send(()).unwrap();
        released.recv().unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
    });
    reached.await.unwrap();
    execute(&store, insert("a", "large", &vec![7; 32768]))
        .await
        .unwrap();
    assert_eq!(
        store.submit(insert("a", "no", b"")).err().unwrap().kind,
        ErrorKind::Capacity
    );
    assert_eq!(
        execute(&store, Request::Maintain).await.unwrap_err().kind,
        ErrorKind::Busy
    );
    assert_eq!(snapshot(&store, "a").await.len(), 2);
    release.send(()).unwrap();
    reader.await.unwrap();
    execute(&store, Request::Maintain).await.unwrap();
    execute(&store, insert("a", "ok", b"ok")).await.unwrap();
    store.close().await.unwrap();
    let store = open(&dir, Settings::default()).await;
    assert_eq!(snapshot(&store, "a").await.len(), 3);
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_database_page_budget_refuses_growth_without_losing_committed_bytes() {
    let dir = Directory::new();
    let store = create(
        &dir,
        Settings {
            max_database_bytes: 32768,
            ..Settings::default()
        },
    )
    .await;
    execute(&store, insert("a", "seed", b"old")).await.unwrap();
    assert_eq!(
        execute(&store, insert("a", "large", &vec![0; 65536]))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    assert_eq!(snapshot(&store, "a").await[0].value, b"old");
    store.close().await.unwrap();
    let store = open(&dir, Settings::default()).await;
    assert_eq!(snapshot(&store, "a").await.len(), 1);
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_cancelled_bootstrap_retains_lease_and_cleans_up() {
    let dir = Directory::new();
    let mut config = supervisor::Config::new(SqliteBackend::create_new(dir.db(), identity()));
    let hooks = Arc::get_mut(&mut config.hooks).unwrap();
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *hooks.bootstrap.get_mut().unwrap() = Some(Gate {
        entered,
        release: released,
    });
    let mut opening = Box::pin(Store::start(Settings::default(), config));
    tokio::select! { _=&mut opening => panic!("blocked bootstrap"), _=reached=>{} }
    drop(opening);
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Busy
    );
    release.send(()).unwrap();
    // Completion is independently owned. Retry only Busy, within a finite test budget.
    let store = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match Store::open_existing(dir.db(), identity(), Settings::default()).await {
                Ok(store) => break store,
                Err(e) if e.kind == ErrorKind::Busy => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(snapshot(&store, "a").await.is_empty());
    store.close().await.unwrap();
}

#[test]
fn sqlite_failure_quarantine_child() {
    let Ok(mode) = std::env::var("FUBE_RA_QUARANTINE_CHILD") else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = Directory::new();
        let store = create(&dir, Settings::default()).await;
        match mode.as_str() {
            "worker" => {
                store
                    .shared()
                    .hooks
                    .lose_worker
                    .store(true, Ordering::SeqCst);
                assert_eq!(
                    execute(&store, insert("a", "1", b""))
                        .await
                        .unwrap_err()
                        .kind,
                    ErrorKind::Unknown
                );
            }
            "supervisor" => {
                store
                    .shared()
                    .hooks
                    .supervisor_panic
                    .store(true, Ordering::SeqCst);
                assert_eq!(
                    execute(&store, insert("a", "1", b""))
                        .await
                        .unwrap_err()
                        .kind,
                    ErrorKind::Unknown
                );
            }
            "close" => {
                store
                    .shared()
                    .hooks
                    .fail_close
                    .store(true, Ordering::SeqCst);
            }
            _ => panic!("invalid child mode"),
        }
        assert_eq!(store.close().await.unwrap_err().kind, ErrorKind::Unknown);
        if mode == "worker" {
            assert_eq!(store.stats().outstanding, 1);
        }
        assert_eq!(
            Store::open_existing(dir.db(), identity(), Settings::default())
                .await
                .err()
                .unwrap()
                .kind,
            ErrorKind::Busy
        );
        // Do not unlink a live quarantined lease inode; process exit ends quarantine.
        std::mem::forget(dir);
    });
}

#[test]
fn sqlite_unknown_worker_supervisor_and_negative_close_witness_keep_exclusion() {
    for mode in ["worker", "supervisor", "close"] {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sqlite_tests::sqlite_failure_quarantine_child",
                "--nocapture",
            ])
            .env("FUBE_RA_QUARANTINE_CHILD", mode)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "mode {mode}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

async fn eventually_open(dir: &Directory) -> Store {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match Store::open_existing(dir.db(), identity(), Settings::default()).await {
                Ok(store) => break store,
                Err(e) if e.kind == ErrorKind::Busy => tokio::task::yield_now().await,
                Err(e) => panic!("cleanup/reopen: {e}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn sqlite_all_observers_abandoned_still_commit_and_close() {
    let dir = Directory::new();
    let store = create(&dir, Settings::default()).await;
    let (entered, release) = store.shared().hooks.pause();
    let operation = store.submit(insert("a", "1", b"no observers")).unwrap();
    entered.await.unwrap();
    drop(operation);
    drop(store);
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Busy
    );
    release.send(()).unwrap();
    let store = eventually_open(&dir).await;
    assert_eq!(snapshot(&store, "a").await[0].value, b"no observers");
    store.close().await.unwrap();
}

#[test]
fn sqlite_accepted_work_survives_caller_runtime_destruction() {
    let dir = Directory::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (store, ticket, release) = rt.block_on(async {
        let store = create(&dir, Settings::default()).await;
        let (entered, release) = store.shared().hooks.pause();
        let ticket = store
            .submit(insert("a", "1", b"runtime independent"))
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
        store.close().await.unwrap();
        let reopened = open(&dir, Settings::default()).await;
        assert_eq!(
            snapshot(&reopened, "a").await[0].value,
            b"runtime independent"
        );
        reopened.close().await.unwrap();
    });
}

#[tokio::test]
async fn sqlite_bootstrap_timeout_reports_unknown_without_releasing_owner() {
    let dir = Directory::new();
    let config = supervisor::Config::new(SqliteBackend::create_new(dir.db(), identity()));
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *supervisor::lock(&config.hooks.bootstrap) = Some(Gate {
        entered,
        release: released,
    });
    let mut opening = Box::pin(Store::start(
        Settings {
            bootstrap_timeout: Duration::from_millis(100),
            ..Settings::default()
        },
        config,
    ));
    tokio::select! { _ = &mut opening => panic!("blocked"), _ = reached => {} }
    assert_eq!(opening.await.err().unwrap().kind, ErrorKind::Unknown);
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Busy
    );
    release.send(()).unwrap();
    let store = eventually_open(&dir).await;
    store.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_internal_page_corruption_and_metadata_tampering_fail_open() {
    let dir = Directory::new();
    let store = create(&dir, Settings::default()).await;
    execute(&store, insert("a", "1", b"committed"))
        .await
        .unwrap();
    store.close().await.unwrap();
    let path = dir.db();
    tokio::task::spawn_blocking(move || {
        use std::io::{Seek, SeekFrom, Write};
        let conn = Raw::open(&path).unwrap();
        let root: i64 = conn
            .query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name='records'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let page: i64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        conn.close().unwrap();
        let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(((root - 1) * page) as u64))
            .unwrap();
        file.write_all(&[0; 32]).unwrap();
        file.sync_all().unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        Store::open_existing(dir.db(), identity(), Settings::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::Corrupt
    );

    for edit in [
        "UPDATE metadata SET version=99",
        "UPDATE metadata SET format='foreign'",
        "DELETE FROM metadata",
        "CREATE TABLE unexpected(x)",
        "CREATE TABLE sqliteXforeign(x)",
        "CREATE TRIGGER sqliteXalter AFTER INSERT ON records BEGIN UPDATE records SET value=x'626164'; END",
    ] {
        let dir = Directory::new();
        create(&dir, Settings::default())
            .await
            .close()
            .await
            .unwrap();
        let path = dir.db();
        tokio::task::spawn_blocking(move || Raw::open(path).unwrap().execute_batch(edit))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            Store::open_existing(dir.db(), identity(), Settings::default())
                .await
                .err()
                .unwrap()
                .kind,
            ErrorKind::Incompatible
        );
    }
}

#[test]
fn sqlite_only_record_primary_key_conflict_is_conflict() {
    use tokio_rusqlite::rusqlite::{ffi, Error as SqlError};
    for (code, expected) in [
        (ffi::SQLITE_CONSTRAINT_PRIMARYKEY, ErrorKind::Conflict),
        (ffi::SQLITE_CONSTRAINT_UNIQUE, ErrorKind::Sqlite),
        (ffi::SQLITE_CONSTRAINT_CHECK, ErrorKind::Sqlite),
        (ffi::SQLITE_CONSTRAINT_COMMITHOOK, ErrorKind::Sqlite),
        (ffi::SQLITE_IOERR_WRITE, ErrorKind::Io),
        (ffi::SQLITE_BUSY, ErrorKind::Busy),
        (ffi::SQLITE_CORRUPT, ErrorKind::Corrupt),
        (ffi::SQLITE_FULL, ErrorKind::Capacity),
    ] {
        let error = SqlError::SqliteFailure(ffi::Error::new(code), None);
        assert_eq!(sqlite::sql(error, true).kind, expected);
    }
    let error = SqlError::SqliteFailure(ffi::Error::new(ffi::SQLITE_CONSTRAINT_PRIMARYKEY), None);
    assert_eq!(sqlite::sql(error, false).kind, ErrorKind::Sqlite);
}
