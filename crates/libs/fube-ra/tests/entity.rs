use fube_ra::{entity::*, protocol::*};
use fube_ra_store::{
    Backend, BackendContext, Error, ErrorKind, Identity, MemoryBackend, Output, Request,
    Settings as StoreSettings, Store, StoreFuture,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(3);
struct Gate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    fail: bool,
}
#[derive(Default)]
struct Hooks {
    gate: Mutex<Option<Gate>>,
    calls: AtomicUsize,
}
impl Hooks {
    fn pause(&self, fail: bool) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, reached) = oneshot::channel();
        let (release, released) = oneshot::channel();
        *self.gate.lock().unwrap() = Some(Gate {
            entered,
            release: released,
            fail,
        });
        (reached, release)
    }
}
struct Controlled {
    inner: MemoryBackend,
    hooks: Arc<Hooks>,
}
impl Backend for Controlled {
    fn initialize(
        &mut self,
        settings: StoreSettings,
        context: BackendContext,
    ) -> StoreFuture<'_, ()> {
        self.inner.initialize(settings, context)
    }
    fn execute(&mut self, request: Request) -> StoreFuture<'_, Output> {
        Box::pin(async move {
            self.hooks.calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.hooks.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                let _ = gate.entered.send(());
                gate.release
                    .await
                    .expect("test must resolve actual backend work");
                if gate.fail {
                    return Err(Error::new(ErrorKind::Io, "definite precommit veto"));
                }
            }
            self.inner.execute(request).await
        })
    }
    fn close(&mut self) -> StoreFuture<'_, ()> {
        self.inner.close()
    }
}
fn admission() -> Admission {
    Admission {
        local_node: NodeInstance {
            id: [4; 16],
            instance: 20,
        },
        fm_source: NodeInstance {
            id: [8; 16],
            instance: 30,
        },
        generation: GenerationNumber {
            generation: 7,
            owner: [8; 16],
        },
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}
fn body(id: u8) -> AddInstance {
    let mut value = AddInstance::default();
    value.unit.id.guid.0 = [id; 16];
    value.unit.consistency.id = value.unit.id.clone();
    value.unit.current = Epoch {
        configuration: 5,
        data_loss: 1,
    };
    value.replica.node = admission().local_node;
    value.replica.id = 40;
    value.replica.instance = 50;
    value.replica.state = 1;
    value.replica.version = VersionInstance::default();
    value.service.name = "fabric:/app/stateless".into();
    value.service.service_type.name = "Type".into();
    value
}
fn add(value: AddInstance) -> Command {
    Command::Add {
        body: Box::new(value),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}
fn complete(value: &AddInstance, token: u64) -> OpenCompletion {
    let mut replica = value.replica.clone();
    replica.state = 2;
    replica.up = true;
    replica.service_endpoint = "tcp://owned-open-result".into();
    OpenCompletion {
        token,
        unit: value.unit.clone(),
        replica,
        error: 0,
    }
}
async fn setup(timeout: Duration) -> (Store, Executor, Arc<Hooks>) {
    let hooks = Arc::new(Hooks::default());
    let store = Store::new(
        Controlled {
            inner: MemoryBackend::new(),
            hooks: hooks.clone(),
        },
        StoreSettings {
            operation_timeout: timeout,
            ..StoreSettings::default()
        },
    )
    .await
    .unwrap();
    let executor =
        Executor::new(Arc::new(store.clone()), admission(), Settings::default()).unwrap();
    (store, executor, hooks)
}
async fn finish(store: &Store, executor: &Executor) {
    let stats = executor.close(WAIT).await.unwrap();
    assert_eq!(stats.outstanding, 0);
    assert_eq!(stats.accepted, stats.succeeded + stats.failed);
    store.close().await.unwrap();
}

#[tokio::test]
async fn initial_durable_commit_is_private_and_same_entity_work_serializes() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    let (entered, release) = hooks.pause(false);
    let first = executor.submit(add(request.clone())).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    let duplicate = executor.submit(add(request)).unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    assert_eq!(first.status(), Status::Pending);
    assert_eq!(duplicate.status(), Status::Pending);
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
    release.send(()).unwrap();
    assert_eq!(first.wait(WAIT).await, Ok(Disposition::Applied));
    assert_eq!(duplicate.wait(WAIT).await, Ok(Disposition::IgnoredPending));
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().phase,
        Phase::Opening
    );
    let effects = executor.take_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].operation, first.id);
    assert!(matches!(effects[0].effect, Effect::Open { .. }));
    // Repeated observers do not redispatch an accepted operation.
    first.wait(WAIT).await.unwrap();
    first.resolve(WAIT).await.unwrap();
    assert!(executor.take_effects().is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn ready_state_and_semantic_reply_wait_for_actual_update_commit() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    executor.take_effects();
    let (entered, release) = hooks.pause(false);
    let opening = executor.snapshot([1; 16]).committed.unwrap();
    let done = executor
        .submit(Command::CompleteOpen(Box::new(complete(
            &request, first.id,
        ))))
        .unwrap();
    entered.await.unwrap();
    let snapshot = executor.snapshot([1; 16]).committed.unwrap();
    assert!(Arc::ptr_eq(&opening, &snapshot));
    assert_eq!(snapshot.phase, Phase::Opening);
    assert!(snapshot.body.replica.service_endpoint.is_empty());
    assert!(executor.take_effects().is_empty());
    assert_eq!(done.status(), Status::Pending);
    release.send(()).unwrap();
    done.wait(WAIT).await.unwrap();
    assert_eq!(opening.phase, Phase::Opening); // retained readers keep old immutable snapshot
    let ready = executor.snapshot([1; 16]).committed.unwrap();
    assert_eq!(ready.phase, Phase::Ready);
    let effects = executor.take_effects();
    assert_eq!(effects.len(), 1);
    match &effects[0].effect {
        Effect::Reply(reply) => {
            assert_eq!(reply.error, 0);
            assert_eq!(reply.replica, ready.body.replica);
            assert_eq!(reply.replica.service_endpoint, "tcp://owned-open-result");
        }
        _ => panic!("semantic reply required"),
    }
    let writes = hooks.calls.load(Ordering::SeqCst);
    executor
        .submit(add(request))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert_eq!(hooks.calls.load(Ordering::SeqCst), writes); // identical Ready reply has no new write
    assert!(matches!(
        executor.take_effects()[0].effect,
        Effect::Reply(_)
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn definite_failure_discards_state_effects_and_reply_then_permits_retry() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    let (entered, release) = hooks.pause(true);
    let first = executor.submit(add(request.clone())).unwrap();
    entered.await.unwrap();
    release.send(()).unwrap();
    assert!(matches!(first.wait(WAIT).await, Err(Failure::Store(e)) if e.kind == ErrorKind::Io));
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(!executor.snapshot([1; 16]).fenced);
    assert!(executor.take_effects().is_empty());
    let retried = executor.submit(add(request)).unwrap();
    retried.wait(WAIT).await.unwrap();
    assert_eq!(executor.take_effects()[0].operation, retried.id);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn unknown_completion_fences_same_entity_but_allows_other_memory_work_and_late_progress() {
    let (store, executor, hooks) = setup(Duration::from_millis(50)).await;
    let other = body(2);
    executor
        .submit(add(other.clone()))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    executor.take_effects();
    let (entered, release) = hooks.pause(false);
    let first = executor.submit(add(body(1))).unwrap();
    entered.await.unwrap();
    assert_eq!(first.wait(WAIT).await, Err(Failure::Unknown));
    let queued = executor
        .submit(Command::RetryOpen(body(1).unit.id))
        .unwrap();
    let independent = executor.submit(Command::RetryOpen(other.unit.id)).unwrap();
    independent.wait(WAIT).await.unwrap();
    assert_eq!(queued.status(), Status::Pending);
    let snapshot = executor.snapshot([1; 16]);
    assert!(snapshot.fenced && snapshot.committed.is_none());
    assert_eq!(executor.statistics().outstanding, 2);
    let effects = executor.take_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].entity, [2; 16]);
    assert_eq!(
        executor.close(Duration::from_millis(10)).await,
        Err(Failure::Unknown)
    );
    release.send(()).unwrap();
    first.resolve(WAIT).await.unwrap();
    queued.resolve(WAIT).await.unwrap();
    assert!(!executor.snapshot([1; 16]).fenced);
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().retry_attempts,
        1
    );
    let effects = executor.take_effects();
    assert_eq!(
        effects.iter().map(|e| e.operation).collect::<Vec<_>>(),
        vec![first.id, queued.id]
    );
    // Store remains terminally fenced after its deadline; executor never reopens it.
    assert!(store.submit(Request::Maintain).is_err());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn unknown_late_failure_discards_and_releases_only_after_actual_resolution() {
    let (store, executor, hooks) = setup(Duration::from_millis(50)).await;
    let (entered, release) = hooks.pause(true);
    let first = executor.submit(add(body(1))).unwrap();
    entered.await.unwrap();
    assert_eq!(first.wait(WAIT).await, Err(Failure::Unknown));
    let queued = executor
        .submit(Command::RetryOpen(body(1).unit.id))
        .unwrap();
    assert_eq!(queued.status(), Status::Pending);
    assert!(executor.snapshot([1; 16]).fenced);
    release.send(()).unwrap();
    assert!(matches!(first.resolve(WAIT).await, Err(Failure::Store(e)) if e.kind == ErrorKind::Io));
    assert!(matches!(
        queued.wait(WAIT).await,
        Err(Failure::Admission(_))
    ));
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(!executor.snapshot([1; 16]).fenced);
    assert!(executor.take_effects().is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn unknown_update_cannot_publish_ready_or_reply_until_its_late_commit() {
    let (store, executor, hooks) = setup(Duration::from_millis(50)).await;
    let request = body(1);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    executor.take_effects();
    let old = executor.snapshot([1; 16]).committed.unwrap();
    let (entered, release) = hooks.pause(false);
    let done = executor
        .submit(Command::CompleteOpen(Box::new(complete(
            &request, first.id,
        ))))
        .unwrap();
    entered.await.unwrap();
    assert_eq!(done.wait(WAIT).await, Err(Failure::Unknown));
    let duplicate = executor.submit(add(request)).unwrap();
    assert_eq!(duplicate.status(), Status::Pending);
    let snapshot = executor.snapshot([1; 16]);
    assert!(snapshot.fenced);
    assert!(Arc::ptr_eq(&old, &snapshot.committed.unwrap()));
    assert!(executor.take_effects().is_empty());
    assert!(executor.statistics().queued_bytes > 0);
    release.send(()).unwrap();
    done.resolve(WAIT).await.unwrap();
    duplicate.resolve(WAIT).await.unwrap();
    assert_eq!(old.phase, Phase::Opening);
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().phase,
        Phase::Ready
    );
    let effects = executor.take_effects();
    assert_eq!(effects.len(), 2);
    assert_eq!(effects[0].operation, done.id);
    assert_eq!(effects[1].operation, duplicate.id);
    assert!(effects
        .iter()
        .all(|effect| matches!(effect.effect, Effect::Reply(ref r) if r.error==0)));
    assert_eq!(executor.statistics().queued_bytes, 0);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn dropped_last_executor_handle_still_drains_accepted_store_work() {
    let (store, executor, hooks) = setup(WAIT).await;
    let (entered, release) = hooks.pause(false);
    let ticket = executor.submit(add(body(1))).unwrap();
    entered.await.unwrap();
    drop(executor);
    release.send(()).unwrap();
    assert_eq!(ticket.resolve(WAIT).await, Ok(Disposition::Applied));
    let rows = store
        .submit(Request::Enumerate("ra/entity/v1".into()))
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(&*rows, Output::Snapshot(records) if records.len()==1));
    store.close().await.unwrap();
}

#[tokio::test]
async fn update_failure_retains_the_exact_previous_snapshot_and_cancels_success_reply() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    executor.take_effects();
    let old = executor.snapshot([1; 16]).committed.unwrap();
    let (entered, release) = hooks.pause(true);
    let done = executor
        .submit(Command::CompleteOpen(Box::new(complete(
            &request, first.id,
        ))))
        .unwrap();
    entered.await.unwrap();
    release.send(()).unwrap();
    assert!(matches!(done.wait(WAIT).await, Err(Failure::Store(_))));
    assert!(Arc::ptr_eq(
        &old,
        &executor.snapshot([1; 16]).committed.unwrap()
    ));
    assert!(executor.take_effects().is_empty());
    finish(&store, &executor).await;
}

#[test]
fn cancelling_caller_runtime_and_dropping_all_tickets_does_not_retract_owned_work() {
    let caller = tokio::runtime::Runtime::new().unwrap();
    let (store, executor, hooks) = caller.block_on(setup(WAIT));
    let (entered, release) = hooks.pause(false);
    let ticket = executor.submit(add(body(1))).unwrap();
    caller.block_on(entered).unwrap();
    let task = caller.spawn(async move { ticket.wait(WAIT).await });
    task.abort();
    drop(caller);
    release.send(()).unwrap();
    let observer = tokio::runtime::Runtime::new().unwrap();
    observer.block_on(async {
        let stats = executor.close(WAIT).await.unwrap();
        assert_eq!(stats.succeeded, 1);
        assert!(executor.snapshot([1; 16]).committed.is_some());
        assert_eq!(executor.take_effects().len(), 1);
        store.close().await.unwrap();
    });
}

#[tokio::test]
async fn memory_only_retry_never_inserts_or_writes_and_retryable_error_is_not_success() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    assert!(matches!(
        executor
            .submit(Command::RetryOpen(request.unit.id.clone()))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Admission(_))
    ));
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 0);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    executor.take_effects();
    let mut not_registered = complete(&request, first.id);
    not_registered.error = SERVICE_TYPE_NOT_REGISTERED;
    executor
        .submit(Command::CompleteOpen(Box::new(not_registered)))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(executor.take_effects().is_empty());
    let state = executor.snapshot([1; 16]).committed.unwrap();
    assert_eq!(state.phase, Phase::Opening);
    assert_eq!(state.last_retry_error, SERVICE_TYPE_NOT_REGISTERED);
    executor
        .submit(Command::RetryOpen(request.unit.id))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
    let effect = executor.take_effects().pop().unwrap();
    assert!(matches!(effect.effect, Effect::Open { token, .. } if token == first.id));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn stale_identity_epoch_source_generation_and_unsupported_replacement_never_open_or_succeed()
{
    let (store, executor, _) = setup(WAIT).await;
    let request = body(1);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    executor.take_effects();
    let mut stale = request.clone();
    stale.unit.current.configuration -= 1;
    assert_eq!(
        executor.submit(add(stale)).unwrap().wait(WAIT).await,
        Ok(Disposition::IgnoredStale)
    );
    let mut stale_instance = request.clone();
    stale_instance.replica.instance -= 1;
    assert_eq!(
        executor
            .submit(add(stale_instance))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    let mut wrong_node = request.clone();
    wrong_node.replica.node.instance += 1;
    assert!(matches!(
        executor.submit(add(wrong_node)).unwrap().wait(WAIT).await,
        Err(Failure::Admission(_))
    ));
    let mut down = request.clone();
    down.replica.up = false;
    assert!(matches!(
        executor.submit(add(down)).unwrap().wait(WAIT).await,
        Err(Failure::Unsupported(_))
    ));
    let mut replacement = request.clone();
    replacement.replica.id += 1;
    assert!(matches!(
        executor.submit(add(replacement)).unwrap().wait(WAIT).await,
        Err(Failure::Unsupported(_))
    ));
    let mut unsupported = request.clone();
    unsupported.service.stateful = true;
    assert!(matches!(
        executor.submit(add(unsupported)).unwrap().wait(WAIT).await,
        Err(Failure::Unsupported(_))
    ));
    let mut wrong = complete(&request, first.id);
    wrong.token += 1;
    assert_eq!(
        executor
            .submit(Command::CompleteOpen(Box::new(wrong)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    let mut error = complete(&request, first.id);
    error.error = -123;
    assert!(matches!(
        executor
            .submit(Command::CompleteOpen(Box::new(error)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    for (source, generation) in [
        (
            NodeInstance::default(),
            GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        ),
        (
            admission().fm_source,
            GenerationHeader {
                number: GenerationNumber {
                    generation: 0,
                    owner: [9; 16],
                },
                for_fm_replica: false,
            },
        ),
        (
            admission().fm_source,
            GenerationHeader {
                number: admission().generation,
                for_fm_replica: true,
            },
        ),
    ] {
        assert!(executor
            .submit(Command::Add {
                body: Box::new(request.clone()),
                source,
                generation
            })
            .unwrap()
            .wait(WAIT)
            .await
            .is_err());
    }
    assert!(executor.take_effects().is_empty());
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().phase,
        Phase::Opening
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_readiness_upgrade_and_deactivation_have_distinct_reply_semantics() {
    for mode in 0..4 {
        let store = Store::memory(StoreSettings::default()).await.unwrap();
        let mut context = admission();
        match mode {
            0 => context.owner_ready = false,
            1 => context.node_open = false,
            2 => context.fabric_upgrading = true,
            _ => context.node_activated = false,
        }
        let executor =
            Executor::new(Arc::new(store.clone()), context, Settings::default()).unwrap();
        let ticket = executor.submit(add(body(1))).unwrap();
        let disposition = ticket.wait(WAIT).await.unwrap();
        let effects = executor.take_effects();
        if mode == 3 {
            assert_eq!(disposition, Disposition::Applied);
            assert!(matches!(effects[0].effect, Effect::Reply(ref r) if r.error == RA_NOT_READY));
        } else {
            assert_eq!(disposition, Disposition::IgnoredNotReady);
            assert!(effects.is_empty());
        }
        assert!(executor.snapshot([1; 16]).committed.is_none());
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn bounded_effect_reservation_and_close_do_not_duplicate_or_admit_more_work() {
    let store = Store::memory(StoreSettings::default()).await.unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_effects: 1,
            max_entities: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    executor
        .submit(add(body(1)))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(matches!(
        executor.submit(Command::RetryOpen(body(1).unit.id)),
        Err(Failure::Capacity)
    ));
    executor.take_effects();
    assert!(matches!(
        executor.submit(add(body(2))),
        Err(Failure::Capacity)
    ));
    executor
        .submit(Command::RetryOpen(body(1).unit.id))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    executor.close(WAIT).await.unwrap();
    assert!(matches!(
        executor.submit(add(body(1))),
        Err(Failure::Closed)
    ));
    assert_eq!(executor.take_effects().len(), 1);
    assert!(executor.take_effects().is_empty());
    store.close().await.unwrap();
}

#[tokio::test]
async fn real_sqlite_store_commits_one_versioned_aggregate_without_restart_claim() {
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::fs::PermissionsExt;
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    let path = std::env::temp_dir().join(format!(
        "fube-ra-entity-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .unwrap();
    let dir = Directory(path);
    assert_eq!(
        std::fs::metadata(&dir.0).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let store = Store::create_new(
        dir.0.join("ra.sqlite"),
        Identity {
            deployment: "test".into(),
            node: "node".into(),
        },
        StoreSettings::default(),
    )
    .await
    .unwrap();
    let executor =
        Executor::new(Arc::new(store.clone()), admission(), Settings::default()).unwrap();
    let request = body(1);
    let ticket = executor.submit(add(request.clone())).unwrap();
    ticket.wait(WAIT).await.unwrap();
    executor
        .submit(Command::CompleteOpen(Box::new(complete(
            &request, ticket.id,
        ))))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    let rows = store
        .submit(Request::Enumerate("ra/entity/v1".into()))
        .unwrap()
        .wait()
        .await
        .unwrap();
    match &*rows {
        Output::Snapshot(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key.id, "01".repeat(16));
            assert!(!records[0].value.is_empty());
        }
        _ => panic!("expected committed entity aggregate"),
    }
    finish(&store, &executor).await;
}
