use fube_ra_store::{
    Backend, BackendContext, Error, ErrorKind, KeyValueStore, MemoryBackend, Output, Record,
    RecordId, Request, Settings, SqliteBackend, Store, StoreFuture,
};
use std::{cell::Cell, sync::Arc, time::Duration};
use tokio::sync::oneshot;

struct Resource(Option<oneshot::Sender<()>>);
impl Drop for Resource {
    fn drop(&mut self) {
        if let Some(released) = self.0.take() {
            let _ = released.send(());
        }
    }
}

// Cell makes this external implementation Send but not Sync.
struct CallerBackend {
    inner: MemoryBackend,
    context: Option<BackendContext>,
    context_observer: Option<oneshot::Sender<BackendContext>>,
    calls: Cell<usize>,
    initialized: Option<oneshot::Sender<String>>,
    initialize_gate: Option<oneshot::Receiver<()>>,
    executing: Option<oneshot::Sender<()>>,
    execute_gate: Option<oneshot::Receiver<()>>,
    resource: Option<oneshot::Sender<()>>,
    fail_initialize: bool,
    fail_close: bool,
    release_ownership: bool,
    lose_worker: bool,
}

impl CallerBackend {
    fn new() -> (Self, oneshot::Receiver<String>, oneshot::Receiver<()>) {
        let (initialized, opened) = oneshot::channel();
        let (resource, released) = oneshot::channel();
        (
            Self {
                inner: MemoryBackend::new(),
                context: None,
                context_observer: None,
                calls: Cell::new(0),
                initialized: Some(initialized),
                initialize_gate: None,
                executing: None,
                execute_gate: None,
                resource: Some(resource),
                fail_initialize: false,
                fail_close: false,
                release_ownership: true,
                lose_worker: false,
            },
            opened,
            released,
        )
    }
}

impl Backend for CallerBackend {
    fn initialize(&mut self, settings: Settings, context: BackendContext) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            context.retain_resource(Resource(self.resource.take()))?;
            self.context = Some(context.clone());
            if let Some(observer) = self.context_observer.take() {
                assert!(observer.send(context.clone()).is_ok());
            }
            self.inner.initialize(settings, context).await?;
            self.initialized
                .take()
                .unwrap()
                .send(std::thread::current().name().unwrap().to_owned())
                .unwrap();
            if let Some(gate) = self.initialize_gate.take() {
                gate.await.unwrap();
            }
            if self.fail_initialize {
                return Err(Error::new(ErrorKind::Io, "caller initialization failed"));
            }
            Ok(())
        })
    }

    fn execute(&mut self, request: Request) -> StoreFuture<'_, Output> {
        Box::pin(async move {
            self.calls.set(self.calls.get() + 1);
            if let Some(executing) = self.executing.take() {
                executing.send(()).unwrap();
            }
            if let Some(gate) = self.execute_gate.take() {
                gate.await.unwrap();
            }
            if self.lose_worker {
                self.context.as_ref().unwrap().mark_worker_lost();
                return Err(Error::new(ErrorKind::Unknown, "caller worker lost"));
            }
            self.inner.execute(request).await
        })
    }

    fn close(&mut self) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.inner.close().await?;
            if self.release_ownership {
                if let Some(context) = &self.context {
                    context.release_resource();
                }
            }
            if self.fail_close {
                return Err(Error::new(ErrorKind::Io, "caller cleanup failed"));
            }
            Ok(())
        })
    }
}

fn insert() -> Request {
    Request::Insert(Record {
        key: RecordId::new("caller", "record"),
        value: b"value".to_vec(),
    })
}

async fn released(resource: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(3), resource)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn boxed_send_only_backend_initializes_on_owned_supervisor() {
    let (backend, initialized, resource) = CallerBackend::new();
    let backend: Box<dyn Backend> = Box::new(backend);
    let store: Arc<dyn KeyValueStore> =
        Arc::new(Store::new(backend, Settings::default()).await.unwrap());
    assert_eq!(initialized.await.unwrap(), "ra-store-supervisor");
    store.submit(insert()).unwrap().wait().await.unwrap();
    assert_eq!(
        store
            .submit(Request::Enumerate("caller".into()))
            .unwrap()
            .wait()
            .await
            .unwrap()
            .as_ref(),
        &Output::Snapshot(vec![Record {
            key: RecordId::new("caller", "record"),
            value: b"value".to_vec(),
        }])
    );
    assert!(store.close().await.unwrap().is_clean());
    released(resource).await;
}

#[tokio::test]
async fn custom_backend_cancellation_retains_admission_and_close_ownership() {
    let (mut backend, _initialized, resource) = CallerBackend::new();
    let (executing, entered) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    backend.executing = Some(executing);
    backend.execute_gate = Some(gate);
    let store = Store::new(
        backend,
        Settings {
            max_outstanding: 1,
            ..Settings::default()
        },
    )
    .await
    .unwrap();
    let operation = store.submit(insert()).unwrap();
    entered.await.unwrap();
    drop(operation);
    assert_eq!(store.stats().outstanding, 1);
    assert!(store.stats().queued_bytes > 0);
    assert_eq!(
        store.submit(insert()).err().unwrap().kind,
        ErrorKind::Capacity
    );
    let closing = store.close();
    tokio::pin!(closing);
    tokio::select! {
        _ = &mut closing => panic!("cleanup must drain accepted work"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(
        store.submit(insert()).err().unwrap().kind,
        ErrorKind::Closed
    );
    release.send(()).unwrap();
    let report = closing.await.unwrap();
    assert!(report.is_clean());
    assert_eq!(report.succeeded, 1);
    assert_eq!(store.stats().outstanding, 0);
    released(resource).await;
}

#[tokio::test]
async fn cancelled_custom_initialization_keeps_supervised_cleanup() {
    let (mut backend, initialized, resource) = CallerBackend::new();
    let (release, gate) = oneshot::channel();
    backend.initialize_gate = Some(gate);
    let mut opening = Box::pin(Store::new(backend, Settings::default()));
    tokio::select! {
        _ = &mut opening => panic!("initialization must wait for the barrier"),
        result = initialized => { result.unwrap(); }
    }
    drop(opening);
    release.send(()).unwrap();
    released(resource).await;
}

#[tokio::test]
async fn failed_initialization_closes_partial_backend_and_reports_cleanup_error() {
    for fail_close in [false, true] {
        let (mut backend, _initialized, resource) = CallerBackend::new();
        backend.fail_initialize = true;
        backend.fail_close = fail_close;
        let error = Store::new(backend, Settings::default())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Io);
        assert!(error.message.contains("caller initialization failed"));
        assert_eq!(error.message.contains("caller cleanup failed"), fail_close);
        released(resource).await;
    }
}

#[tokio::test]
async fn custom_backend_close_failure_is_not_success() {
    let (mut backend, _initialized, resource) = CallerBackend::new();
    backend.fail_close = true;
    let store = Store::new(backend, Settings::default()).await.unwrap();
    let error = store.close().await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.message.contains("caller cleanup failed"));
    released(resource).await;
}

struct SqliteWrapper {
    inner: SqliteBackend,
    context: Option<BackendContext>,
    resource: Option<oneshot::Sender<()>>,
    closing: Option<oneshot::Sender<()>>,
    cleanup_gate: Option<oneshot::Receiver<()>>,
}

impl Backend for SqliteWrapper {
    fn initialize(&mut self, settings: Settings, context: BackendContext) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            context.retain_resource(Resource(self.resource.take()))?;
            self.context = Some(context.clone());
            self.inner.initialize(settings, context).await
        })
    }

    fn execute(&mut self, request: Request) -> StoreFuture<'_, Output> {
        self.inner.execute(request)
    }

    fn close(&mut self) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.inner.close().await?;
            self.closing.take().unwrap().send(()).unwrap();
            self.cleanup_gate.take().unwrap().await.unwrap();
            self.context.as_ref().unwrap().release_resource();
            Ok(())
        })
    }
}

#[tokio::test]
async fn delegated_sqlite_rejects_duplicate_owner_without_releasing_original_guard() {
    use std::{fs, os::unix::fs::DirBuilderExt, path::PathBuf};
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../../build/ra-store-backend-tests");
    fs::create_dir_all(&root).unwrap();
    let directory = root.join(format!("duplicate-owner-{}", std::process::id()));
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let (resource, mut dropped) = oneshot::channel();
    let (closing, entered_cleanup) = oneshot::channel();
    let (release, cleanup_gate) = oneshot::channel();
    let backend = SqliteWrapper {
        inner: SqliteBackend::create_new(
            directory.join("ra.sqlite"),
            fube_ra_store::Identity {
                deployment: "delegated".into(),
                node: "node".into(),
            },
        ),
        context: None,
        resource: Some(resource),
        closing: Some(closing),
        cleanup_gate: Some(cleanup_gate),
    };
    let mut opening = Box::pin(Store::new(backend, Settings::default()));
    tokio::select! {
        _ = &mut opening => panic!("initialization must wait for witnessed wrapper cleanup"),
        result = entered_cleanup => { result.unwrap(); }
    }
    // The failed inner registration and its no-engine close cannot clear the
    // outer guard. Only the wrapper's positive cleanup witness may release it.
    assert_eq!(dropped.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    release.send(()).unwrap();
    let error = opening.await.err().unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    assert!(error.message.contains("resource already retained"));
    released(dropped).await;
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn custom_backend_quarantine_child() {
    let Ok(scenario) = std::env::var("FUBE_RA_STORE_BACKEND_QUARANTINE") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            let (mut backend, _initialized, mut resource) = CallerBackend::new();
            let (observer, context) = oneshot::channel();
            backend.context_observer = Some(observer);
            backend.release_ownership = false;
            backend.fail_initialize = scenario == "initialize";
            backend.lose_worker = scenario == "worker";
            match Store::new(backend, Settings::default()).await {
                Err(error) => {
                    assert_eq!(scenario, "initialize");
                    assert_eq!(error.kind, ErrorKind::Unknown);
                    assert!(error.message.contains("caller initialization failed"));
                }
                Ok(store) => {
                    assert_ne!(scenario, "initialize");
                    if scenario == "idle" {
                        context.await.unwrap().mark_worker_lost();
                        assert_eq!(
                            store.submit(insert()).err().unwrap().kind,
                            ErrorKind::Fenced
                        );
                    }
                    if scenario == "worker" {
                        assert_eq!(
                            store
                                .submit(insert())
                                .unwrap()
                                .wait()
                                .await
                                .unwrap_err()
                                .kind,
                            ErrorKind::Unknown
                        );
                        assert_eq!(store.stats().outstanding, 1);
                    }
                    assert_eq!(store.close().await.unwrap_err().kind, ErrorKind::Unknown);
                    drop(store);
                }
            }
            assert_eq!(
                resource.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            );
        });
}

#[test]
fn unwitnessed_custom_cleanup_and_worker_loss_retain_exclusion() {
    // Unknown ownership intentionally lasts until process exit, not the test scope.
    for scenario in ["initialize", "close", "worker", "idle"] {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "custom_backend_quarantine_child", "--nocapture"])
            .env("FUBE_RA_STORE_BACKEND_QUARANTINE", scenario)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{scenario}: {}{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
    }
}
