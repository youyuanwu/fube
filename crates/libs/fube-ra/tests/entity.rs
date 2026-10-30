use fube_ra::{entity::*, protocol::*};
use fube_ra_store::{
    Backend, BackendContext, Error, ErrorKind, Identity, MemoryBackend, Output, Request,
    Settings as StoreSettings, Store, StoreFuture,
};
use std::{
    sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering},
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
    requests: Mutex<Vec<Request>>,
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
            self.hooks.requests.lock().unwrap().push(match &request {
                Request::Insert(record) => Request::Insert(record.clone()),
                Request::Update(record) => Request::Update(record.clone()),
                Request::Enumerate(prefix) => Request::Enumerate(prefix.clone()),
                _ => panic!("unexpected RA storage operation"),
            });
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
fn complete(value: &AddInstance, _token: u64) -> OpenCompletion {
    let mut replica = value.replica.clone();
    replica.up = true;
    replica.service_endpoint = "tcp://owned-open-result".into();
    OpenCompletion {
        unit: value.unit.clone(),
        replica,
        error: 0,
        flags: 0,
        detail: ProxyErrorDetail::default(),
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
fn registration() -> RuntimeRegistration {
    RuntimeRegistration {
        service_type: body(1).service.service_type,
        host: "trusted-host".into(),
        runtime: "trusted-runtime".into(),
        acquisition: 1,
        code_package: "Code".into(),
    }
}
async fn registered(executor: &Executor, request: &AddInstance, token: u64) {
    let actions = registration_actions(executor, request, token).await;
    assert!(matches!(
        actions.as_slice(),
        [
            Effect::RequestServiceTypeWork { .. },
            Effect::RuntimeOpen { .. }
        ] | [Effect::RuntimeOpen { .. }]
    ));
}
async fn registration_actions(
    executor: &Executor,
    request: &AddInstance,
    token: u64,
) -> Vec<Effect> {
    let lookup = *executor
        .snapshot(request.unit.id.guid.0)
        .committed
        .unwrap()
        .registration_lookups
        .keys()
        .next_back()
        .unwrap();
    let mut runtime = registration();
    runtime.acquisition = lookup;
    runtime.service_type = request.service.service_type.clone();
    executor
        .submit(Command::Registered(Box::new(RegistrationCompletion {
            unit: request.unit.id.clone(),
            token,
            lookup,
            registration: runtime,
            error: 0,
            retryable: false,
            usage_acquired: executor
                .snapshot(request.unit.id.guid.0)
                .committed
                .unwrap()
                .usage
                .is_none(),
            version: request.service.version.clone(),
            ..RegistrationCompletion::default()
        })))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    effects(executor)
}

const CLOCK_ORIGIN: i64 = 1_000_000_000;
struct TestClock(AtomicI64);
impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(CLOCK_ORIGIN)))
    }
    fn set(&self, ticks: i64) {
        self.0.store(ticks, Ordering::SeqCst);
    }
}
impl NotificationClock for TestClock {
    fn now_ticks(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
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
    registered(&executor, &request, first.id).await;
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
    registered(&executor, &request, first.id).await;
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
    registered(&executor, &request, first.id).await;
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
    assert_eq!(state.last_retry_error, 0); // absent registration: raw reply ignored
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
    wrong.replica.instance -= 1;
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
        Ok(Disposition::IgnoredStale)
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
        let stale = generation.number.older_than(&admission().generation);
        let result = executor
            .submit(Command::Add {
                body: Box::new(request.clone()),
                source,
                generation,
            })
            .unwrap()
            .wait(WAIT)
            .await;
        if stale {
            assert_eq!(result, Ok(Disposition::IgnoredStale));
        } else {
            assert!(result.is_err());
        }
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
            max_effects: 2, // Add reserves report + stale reply for a Closed FT.
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
    executor
        .submit(Command::RetryOpen(body(1).unit.id))
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
    executor.take_effects();
    registered(&executor, &request, ticket.id).await;
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

// Translated native scenarios, not executions of C++ handlers. All native paths
// below are Reliability/Failover/ra at pinned 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.
fn remove(value: AddInstance, force: bool) -> Command {
    Command::Remove {
        body: Box::new(RemoveInstance::new(value, force)),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}
async fn run(executor: &Executor, command: Command) -> Disposition {
    executor.submit(command).unwrap().wait(WAIT).await.unwrap()
}
fn state(executor: &Executor) -> Arc<EntityState> {
    executor.snapshot([1; 16]).committed.unwrap()
}
fn effects(executor: &Executor) -> Vec<Effect> {
    executor
        .take_effects()
        .into_iter()
        .map(|d| d.effect)
        .collect()
}
fn operations(hooks: &Hooks) -> Vec<&'static str> {
    hooks
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|r| match r {
            Request::Insert(_) => "Insert",
            Request::Update(_) => "Update",
            _ => "other",
        })
        .collect()
}
async fn make_ready(executor: &Executor, request: &AddInstance) -> u64 {
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    assert!(matches!(
        effects(executor).as_slice(),
        [Effect::Open { .. }]
    ));
    registered(executor, request, first.id).await;
    run(
        executor,
        Command::CompleteOpen(Box::new(complete(request, first.id))),
    )
    .await;
    assert!(matches!(effects(executor).as_slice(), [Effect::Reply(r)] if r.error==0));
    first.id
}
fn close_completion(executor: &Executor, error: i64) -> CloseCompletion {
    let current = state(executor);
    CloseCompletion {
        unit: current.body.unit.clone(),
        replica: current.body.replica.clone(),
        error,
    }
}
async fn close_done(executor: &Executor, error: i64) -> Disposition {
    run(
        executor,
        Command::CompleteClose(Box::new(close_completion(executor, error))),
    )
    .await
}
fn assert_close(effects: &[Effect], flags: i64, current: &EntityState) {
    let [Effect::Close {
        token,
        body,
        registration: runtime,
        flags: actual,
    }] = effects
    else {
        panic!("one native-shaped RAP close required: {effects:?}");
    };
    assert_eq!(*actual, flags);
    assert_eq!(*token, current.close_token);
    assert_eq!(Some(runtime), current.runtime.as_ref());
    assert_eq!(body.unit, current.body.unit);
    assert_eq!(body.service, current.body.service);
    assert_eq!(
        body.replica,
        Replica {
            node: admission().local_node,
            id: current.body.replica.id,
            instance: current.body.replica.instance,
            ..Replica::default()
        }
    );
}
fn ack(mut report: ReplicaUp, in_dropped: bool) -> Command {
    if !in_dropped {
        report.replicas = report.dropped;
        report.dropped = Default::default();
    }
    Command::ReplicaUpReply {
        body: Box::new(report),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn native_ready_delete_duplicate_retry_and_close_completion_order() {
    // Test.StateMachine.Close.cpp:367–384; ReplicaLifecycle.cpp:1747–1766,1920–1942.
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    make_ready(&executor, &request).await;
    let old = state(&executor);
    run(&executor, remove(request.clone(), false)).await;
    let closing = state(&executor);
    assert_eq!(closing.phase, Phase::Closing);
    assert_eq!(closing.close_mode, Some(CloseMode::Delete));
    assert!(closing.local_deleted);
    assert!(!closing.cleanup_pending);
    assert_close(&effects(&executor), 8, &closing);
    assert_eq!(
        run(&executor, remove(request.clone(), false)).await,
        Disposition::IgnoredPending
    );
    assert!(effects(&executor).is_empty());
    run(&executor, Command::RetryClose(request.unit.id.clone())).await;
    assert_close(&effects(&executor), 8, &closing);
    close_done(&executor, 0).await;
    assert!(matches!(effects(&executor).as_slice(), [
                Effect::ReleaseRegistration(_), Effect::ReleaseUsage { .. }, Effect::Health(HealthEvent::Close), Effect::RemoveReply(r)
            ] if r.error==0 && r.replica.state==3 && !r.replica.up && r.replica.service_endpoint.is_empty()
                && r.replica.version==VersionInstance::invalid()));
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(state(&executor).cleanup_pending);
    assert_eq!(old.phase, Phase::Ready);
    assert_eq!(operations(&hooks), ["Insert", "Update", "Update", "Update"]);
    run(&executor, remove(request, false)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RemoveReply(_)]
    ));
    assert_eq!(operations(&hooks).len(), 4);
    assert_eq!(close_done(&executor, 0).await, Disposition::IgnoredStale);
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_missing_closed_tombstone_identity_epoch_ordering() {
    // Test.StateMachine.cpp:7210–7218; DeleteReplica.cpp:81–94,214–275.
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    run(&executor, remove(request.clone(), true)).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::RemoveReply(r)] if r.replica.instance==50)
    );
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(state(&executor).local_deleted && state(&executor).cleanup_pending);
    assert_eq!(operations(&hooks), ["Insert"]); // no physical Delete
    let mut lower = request.clone();
    lower.replica.instance -= 1;
    run(&executor, remove(lower, false)).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::RemoveReply(r)] if r.replica.instance==50)
    );
    assert_eq!(operations(&hooks).len(), 1);
    let mut higher = request.clone();
    higher.replica.id += 1;
    higher.replica.instance += 1;
    higher.unit.current.configuration += 1;
    run(&executor, remove(higher.clone(), false)).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::RemoveReply(r)] if r.replica.id==41 && r.replica.instance==51 && r.unit.current.configuration==6)
    );
    let mut stale_id = higher.clone();
    stale_id.replica.id -= 1;
    stale_id.unit.current.configuration += 1;
    run(&executor, remove(stale_id, false)).await;
    assert!(effects(&executor).is_empty());
    assert_eq!(state(&executor).body.unit.current.configuration, 7); // update precedes ID guard
    assert_eq!(state(&executor).body.replica.id, 41);
    run(&executor, remove(higher.clone(), false)).await;
    assert!(effects(&executor).is_empty()); // non-force lower epoch
    run(&executor, remove(higher, true)).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::RemoveReply(r)] if r.unit.current.configuration==7)
    );
    assert_eq!(operations(&hooks), ["Insert", "Update", "Update"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_open_remove_guards_accept_lower_instance_but_reject_impossible_newer() {
    // DeleteReplica.cpp:347–386; infrastructure.FailoverUnitEntityTraits.cpp:204–219,271–275.
    for (force, lower_epoch) in [(false, false), (false, true), (true, true)] {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        let mut incoming = request.clone();
        incoming.replica.id += 1;
        assert_eq!(
            run(&executor, remove(incoming, force)).await,
            Disposition::IgnoredStale
        );
        let mut incoming = request.clone();
        incoming.replica.instance += 1;
        assert!(matches!(
            executor
                .submit(remove(incoming, force))
                .unwrap()
                .wait(WAIT)
                .await,
            Err(Failure::Admission(_))
        ));
        let mut incoming = request.clone();
        incoming.replica.instance -= 1;
        if lower_epoch {
            incoming.unit.current.data_loss -= 1;
        }
        run(&executor, remove(incoming, force)).await;
        if lower_epoch && !force {
            assert_eq!(state(&executor).phase, Phase::Ready);
            assert!(effects(&executor).is_empty());
            assert_eq!(operations(&hooks).len(), 2);
        } else {
            assert_eq!(state(&executor).phase, Phase::Closing);
            if force {
                assert!(matches!(
                    effects(&executor).as_slice(),
                    [Effect::TerminateHost { .. }]
                ));
            } else {
                assert_close(&effects(&executor), 8, &state(&executor));
            }
            assert_eq!(state(&executor).body.unit, request.unit); // open epoch not assigned
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_obliterate_requires_host_down_reports_replica_up_and_ack_list_selects_reply() {
    // ReplicaLifecycle.cpp:1715–1731,1769–1857,1976–1987;
    // ReplicaUpReply.cpp:233–249,336–352; ReconfigurationAgent.cpp:3330–3408.
    for dropped_list in [false, true] {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        run(&executor, remove(request.clone(), true)).await;
        assert!(
            matches!(effects(&executor).as_slice(), [Effect::TerminateHost { registration: r }] if r==&registration())
        );
        assert!(!state(&executor).local_deleted);
        run(&executor, Command::RetryClose(request.unit.id.clone())).await;
        assert!(effects(&executor).is_empty()); // no repeated terminate and no RAP close
        assert_eq!(close_done(&executor, 0).await, Disposition::IgnoredStale);
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: request.unit.id.clone(),
                host: "other".into(),
            })),
        )
        .await;
        assert_eq!(state(&executor).phase, Phase::Closing);
        assert!(effects(&executor).is_empty());
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: request.unit.id.clone(),
                host: registration().host,
            })),
        )
        .await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [
                Effect::ReleaseRegistration(_),
                Effect::ReleaseUsage { .. },
                Effect::RequestReportRetry,
                Effect::Health(HealthEvent::Close)
            ]
        ));
        assert_eq!(state(&executor).phase, Phase::Closed);
        assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
        assert!(!state(&executor).local_deleted);
        run(&executor, Command::RetryReport(request.unit.id.clone())).await;
        let [Effect::ReplicaUp(report)]: [Effect; 1] = effects(&executor).try_into().unwrap()
        else {
            panic!("ReplicaUp required")
        };
        assert!(report.replicas.0.is_empty());
        assert_eq!(report.dropped.0.len(), 1);
        let info = &report.dropped.0[0];
        assert_eq!(info.service, request.service);
        assert_eq!(info.unit, request.unit);
        assert_eq!(info.intermediate, Epoch::default());
        assert!(
            !info.local_deleted && !info.report_from_primary && !report.last && !report.from_fmm
        );
        assert_eq!(info.replicas.0[0].intermediate_role, 1);
        assert_eq!(info.replicas.0[0].description.state, 3);
        run(&executor, Command::RetryReport(request.unit.id.clone())).await;
        assert_eq!(effects(&executor), vec![Effect::ReplicaUp(report.clone())]);
        assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped); // send is not ack
        let mut wrong = (*report).clone();
        wrong.dropped.0[0].replicas.0[0].description.id += 1;
        assert_eq!(
            run(&executor, ack(wrong, dropped_list)).await,
            Disposition::IgnoredStale
        );
        assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
        // Native outer ID + dropped-stage guard does NOT require matching instance/epoch.
        let mut acknowledged = (*report).clone();
        acknowledged.dropped.0[0].replicas.0[0].description.instance -= 1;
        acknowledged.dropped.0[0].unit.current.data_loss -= 1;
        run(&executor, ack(acknowledged, dropped_list)).await;
        if dropped_list {
            assert!(effects(&executor).is_empty());
        } else {
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::RemoveReply(_)]
            ));
        }
        assert!(state(&executor).local_deleted && state(&executor).cleanup_pending);
        assert_eq!(state(&executor).fm_stage, FmStage::None);
        assert_eq!(
            operations(&hooks),
            ["Insert", "Update", "Update", "Update", "Update"]
        );
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_delete_open_races_before_and_after_registration_are_explicit() {
    // DeleteReplica.cpp:388–400 are shared-helper inputs (misnamed force test is nonforce);
    // FailoverUnit.cpp:4985–5068 and ReconfigurationAgent.cpp:3782–3828 are authority.
    for registered_first in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        let first = executor.submit(add(request.clone())).unwrap();
        first.wait(WAIT).await.unwrap();
        effects(&executor);
        if registered_first {
            registered(&executor, &request, first.id).await;
        }
        run(&executor, remove(request.clone(), false)).await;
        if registered_first {
            assert_eq!(state(&executor).phase, Phase::Closing);
            assert_close(&effects(&executor), 8, &state(&executor));
            assert_eq!(
                run(
                    &executor,
                    Command::CompleteOpen(Box::new(complete(&request, first.id)))
                )
                .await,
                Disposition::IgnoredStale
            );
            assert!(effects(&executor).is_empty());
            close_done(&executor, -5).await; // failure is not proof of draining Open
            assert_eq!(state(&executor).phase, Phase::Closing);
            assert!(effects(&executor).is_empty());
            run(&executor, Command::RetryClose(request.unit.id.clone())).await;
            assert_close(&effects(&executor), 8, &state(&executor));
            close_done(&executor, 0).await; // trusted cancellation/drain/cleanup witness
            assert!(matches!(
                effects(&executor).as_slice(),
                [
                    Effect::ReleaseRegistration(_),
                    Effect::ReleaseUsage { .. },
                    Effect::Health(HealthEvent::Close),
                    Effect::RemoveReply(_)
                ]
            ));
        } else {
            assert_eq!(state(&executor).phase, Phase::Closed);
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::Health(HealthEvent::Close), Effect::RemoveReply(_)]
            ));
            run(
                &executor,
                Command::Registered(Box::new(RegistrationCompletion {
                    unit: request.unit.id.clone(),
                    token: first.id,
                    lookup: first.id,
                    registration: registration(),
                    error: 0,
                    retryable: false,
                    usage_acquired: true,
                    ..RegistrationCompletion::default()
                })),
            )
            .await;
            assert_eq!(
                effects(&executor),
                [
                    Effect::ReleaseRegistration(registration()),
                    Effect::ReleaseUsage {
                        acquisition: first.id
                    }
                ]
            ); // no RuntimeOpen, unused separately acquired usage is also disposed
        }
        for error in [0, -5, SERVICE_TYPE_NOT_REGISTERED] {
            let mut completion = complete(&request, first.id);
            completion.error = error;
            assert_eq!(
                run(&executor, Command::CompleteOpen(Box::new(completion))).await,
                Disposition::IgnoredStale
            );
            assert!(effects(&executor).is_empty());
            assert_eq!(state(&executor).phase, Phase::Closed);
        }
        let mut recreate = request.clone();
        recreate.replica.instance += 1;
        run(&executor, add(recreate.clone())).await;
        assert!(!state(&executor).local_deleted && !state(&executor).cleanup_pending);
        assert_eq!(state(&executor).fm_stage, FmStage::None);
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::Open { .. }]
        ));
        assert_eq!(
            run(
                &executor,
                Command::CompleteOpen(Box::new(complete(&request, first.id)))
            )
            .await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_close_failures_warning_forceabort_and_captured_reply_mode() {
    // FailoverUnit.cpp:5372–5418; ReplicaLifecycle.cpp:1747–1766,1920–1942;
    // RetryableErrorState.cpp:21–51 (warning wins equality tie).
    let store = Store::memory(StoreSettings::default()).await.unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            close_warning: 1,
            delete_drop: 2,
            ..Settings::default()
        },
    )
    .unwrap();
    let request = body(1);
    make_ready(&executor, &request).await;
    run(&executor, remove(request.clone(), false)).await;
    effects(&executor);
    close_done(&executor, -5).await;
    assert_eq!(
        effects(&executor),
        [Effect::Health(HealthEvent::CloseWarning)]
    );
    assert_eq!(state(&executor).close_mode, Some(CloseMode::Delete));
    close_done(&executor, -5).await;
    assert_eq!(state(&executor).close_mode, Some(CloseMode::ForceAbort));
    assert!(state(&executor).local_deleted);
    assert!(effects(&executor).is_empty()); // StartClose internal; next timer sends Abort
    run(&executor, Command::RetryClose(request.unit.id.clone())).await;
    assert_close(&effects(&executor), 4, &state(&executor));
    close_done(&executor, 0).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [
            Effect::ReleaseRegistration(_),
            Effect::ReleaseUsage { .. },
            Effect::Health(HealthEvent::Close)
        ]
    ));
    run(&executor, remove(request, false)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RemoveReply(_)]
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_lookup_failure_and_apphostdown_are_not_generic_close_success() {
    // ReplicaLifecycle.cpp:1769–1797; Close.cpp:413–464; RA.cpp:1138–1181,970–978.
    for fatal_lookup in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        run(&executor, remove(request.clone(), false)).await;
        effects(&executor);
        if fatal_lookup {
            close_done(&executor, APPLICATION_HOST_CRASH).await;
            assert_eq!(
                effects(&executor),
                [Effect::ReleaseRegistration(registration())]
            );
            assert_eq!(state(&executor).phase, Phase::Closing);
            assert!(state(&executor).runtime.is_none());
            run(&executor, Command::RetryClose(request.unit.id.clone())).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::FindRegistration { .. }]
            ));
            let token = state(&executor).close_token;
            let lookup = *state(&executor)
                .registration_lookups
                .keys()
                .next_back()
                .unwrap();
            run(
                &executor,
                Command::Registered(Box::new(RegistrationCompletion {
                    unit: request.unit.id.clone(),
                    token,
                    lookup,
                    registration: Default::default(),
                    error: -99,
                    retryable: true,
                    ..RegistrationCompletion::default()
                })),
            )
            .await;
            assert_eq!(state(&executor).phase, Phase::Closing);
            assert!(effects(&executor).is_empty());
            run(&executor, Command::RetryClose(request.unit.id.clone())).await;
            effects(&executor);
            let lookup = *state(&executor)
                .registration_lookups
                .keys()
                .next_back()
                .unwrap();
            run(
                &executor,
                Command::Registered(Box::new(RegistrationCompletion {
                    unit: request.unit.id.clone(),
                    token,
                    lookup,
                    registration: Default::default(),
                    error: APPLICATION_INSTANCE_DELETED,
                    retryable: false,
                    // Existing-replica native policy: unknown errors retry;
                    // ApplicationInstanceDeleted is the fatal exception.
                    ..RegistrationCompletion::default()
                })),
            )
            .await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [
                    Effect::ReleaseUsage { .. },
                    Effect::Health(HealthEvent::Close),
                    Effect::RemoveReply(_)
                ]
            ));
        } else {
            run(
                &executor,
                Command::HostDown(Box::new(HostDown {
                    unit: request.unit.id.clone(),
                    host: registration().host,
                })),
            )
            .await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [
                    Effect::ReleaseRegistration(_),
                    Effect::ReleaseUsage { .. },
                    Effect::Health(HealthEvent::Close)
                ]
            ));
            assert_eq!(state(&executor).fm_stage, FmStage::None); // deleted: no Down report
            run(&executor, remove(request.clone(), false)).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::RemoveReply(_)]
            ));
        }
        assert_eq!(state(&executor).phase, Phase::Closed);
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_closed_add_reports_then_stale_reply_and_tombstone_equal_suppresses_report() {
    // Test.StateMachine.cpp:3567–3589; RA.cpp:1687–1721; FT.cpp:4720–4740.
    let (store, executor, _) = setup(WAIT).await;
    let request = body(1);
    make_ready(&executor, &request).await;
    run(&executor, remove(request.clone(), true)).await;
    effects(&executor);
    run(
        &executor,
        Command::HostDown(Box::new(HostDown {
            unit: request.unit.id.clone(),
            host: registration().host,
        })),
    )
    .await;
    effects(&executor);
    run(&executor, add(request.clone())).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ReplicaUp(_)]
    ));
    let mut older = request.clone();
    older.replica.instance -= 1;
    run(&executor, add(older.clone())).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::ReplicaUp(_), Effect::Reply(r)] if r.error==STALE_REQUEST && r.replica.instance==49)
    );
    run(&executor, remove(request.clone(), false)).await;
    effects(&executor);
    run(&executor, add(request)).await;
    assert!(effects(&executor).is_empty()); // native deleted config is excluded, not a fake report
    run(&executor, add(older)).await;
    assert!(matches!(effects(&executor).as_slice(), [Effect::Reply(r)] if r.error==STALE_REQUEST));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn removal_and_finish_close_keep_private_proposals_and_same_unknown_ticket() {
    // Rust-specific real P1 Store invariants, not native test translations.
    for finish_stage in [false, true] {
        for (unknown, fail) in [(false, true), (true, false), (true, true)] {
            let (store, executor, hooks) = setup(if unknown {
                Duration::from_millis(50)
            } else {
                WAIT
            })
            .await;
            let request = body(1);
            make_ready(&executor, &request).await;
            if finish_stage {
                run(&executor, remove(request.clone(), false)).await;
                effects(&executor);
            }
            let old = state(&executor);
            let writes = operations(&hooks).len();
            let command = if finish_stage {
                Command::CompleteClose(Box::new(close_completion(&executor, 0)))
            } else {
                remove(request.clone(), false)
            };
            let (entered, release) = hooks.pause(fail);
            let ticket = executor.submit(command).unwrap();
            entered.await.unwrap();
            let queued = executor.submit(remove(request.clone(), false)).unwrap();
            assert!(Arc::ptr_eq(&old, &state(&executor)));
            assert!(effects(&executor).is_empty());
            if unknown {
                assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
                assert!(executor.snapshot([1; 16]).fenced);
                assert_eq!(queued.status(), Status::Pending);
                assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
                // Operation.wait's observation deadline may precede the P1
                // supervisor's fence. Observe the actual public fence before
                // releasing the backend; do not assume those clocks coincide.
                tokio::time::timeout(WAIT, async {
                    while store.stats().lifecycle != fube_ra_store::Lifecycle::Fenced {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            assert_eq!(operations(&hooks).len(), writes + 1); // no replacement/resubmit
            release.send(()).unwrap();
            let result = ticket.resolve(WAIT).await;
            if fail {
                assert!(matches!(result, Err(Failure::Store(_))));
            } else {
                assert_eq!(result, Ok(Disposition::Applied));
            }
            let queued_result = queued.resolve(WAIT).await;
            if unknown && fail && !finish_stage {
                // P1 can finish draining/closing the fenced store between
                // publishing the failed write and this queued submission.
                // Both admission states reject it without another backend call.
                assert!(
                    matches!(queued_result, Err(Failure::Store(ref e)) if matches!(e.kind, ErrorKind::Fenced | ErrorKind::Closed)),
                    "queued removal after witnessed store fence: {queued_result:?}"
                );
                assert!(Arc::ptr_eq(&old, &state(&executor)));
                assert_eq!(operations(&hooks).len(), writes + 1);
            } else {
                queued_result.unwrap();
            }
            let actions = executor.take_effects();
            if fail {
                assert!(actions.iter().all(|d| d.operation == queued.id));
                if unknown && !finish_stage {
                    assert!(actions.is_empty());
                }
                // Finish failed: queued duplicate cannot complete pending close.
                if finish_stage {
                    assert!(Arc::ptr_eq(&old, &state(&executor)));
                    assert!(actions.is_empty());
                }
            } else {
                assert_eq!(actions[0].operation, ticket.id);
                if finish_stage {
                    assert!(matches!(
                        actions.last().unwrap().effect,
                        Effect::RemoveReply(_)
                    ));
                    assert_eq!(actions.last().unwrap().operation, queued.id);
                } else {
                    assert_eq!(actions.len(), 1); // duplicate did not issue another Close
                }
            }
            assert!(!executor.snapshot([1; 16]).fenced);
            assert_eq!(
                old.phase,
                if finish_stage {
                    Phase::Closing
                } else {
                    Phase::Ready
                }
            );
            finish(&store, &executor).await;
        }
    }
}

#[tokio::test]
async fn cancellation_does_not_retract_removal_or_release_its_effect_reservation() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    make_ready(&executor, &request).await;
    let (entered, release) = hooks.pause(false);
    let ticket = executor.submit(remove(request, false)).unwrap();
    let id = ticket.id;
    entered.await.unwrap();
    let observer = tokio::spawn(async move { ticket.wait(WAIT).await });
    observer.abort();
    assert_eq!(state(&executor).phase, Phase::Ready);
    assert!(effects(&executor).is_empty());
    release.send(()).unwrap();
    executor.close(WAIT).await.unwrap(); // drains real accepted work
    assert_eq!(state(&executor).phase, Phase::Closing);
    let actions = executor.take_effects();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].operation, id);
    store.close().await.unwrap();
}

#[derive(Debug, serde::Deserialize)]
struct StoredAggregate {
    format: u32,
    body: AddInstance,
    phase: i64,
    open_token: u64,
    deleted: bool,
    cleanup_pending: bool,
    close_mode: i64,
    close_token: u64,
    fm_stage: i64,
}
#[tokio::test]
async fn missing_remove_stores_native_constructed_tombstone_not_request_or_physical_delete() {
    // RA.cpp:4279–4293; FT.cpp:48–74,366–391; FTDescription.cpp:19–30.
    let (store, executor, hooks) = setup(WAIT).await;
    let mut request = body(1);
    request.unit.previous = Epoch {
        configuration: 999,
        data_loss: 9,
    };
    request.unit.target = 9;
    request.unit.minimum = 3;
    request.replica.service_endpoint = "must-not-be-retained".into();
    run(&executor, remove(request.clone(), false)).await;
    effects(&executor);
    let committed = state(&executor);
    {
        let journal = hooks.requests.lock().unwrap();
        let [Request::Insert(record)] = journal.as_slice() else {
            panic!("one insert")
        };
        let stored: StoredAggregate =
            fube_transport::serialization::serde::from_slice(&record.value).unwrap();
        assert_eq!(record.key.record_type, "ra/entity/v1");
        assert_eq!(stored.format, 2);
        assert_eq!(stored.body, *committed.body);
        assert_eq!(stored.body.unit.previous, Epoch::default());
        assert_eq!((stored.body.unit.target, stored.body.unit.minimum), (0, 0));
        assert_eq!(stored.body.unit.current, request.unit.current);
        assert_eq!(stored.body.service, request.service);
        assert_eq!(stored.body.replica.state, 3);
        assert!(!stored.body.replica.up && stored.body.replica.service_endpoint.is_empty());
        assert_eq!(stored.body.replica.version, VersionInstance::invalid());
        assert_eq!(
            (
                stored.phase,
                stored.open_token,
                stored.close_token,
                stored.close_mode,
                stored.fm_stage
            ),
            (3, 0, 0, 0, 0)
        );
        assert!(stored.deleted && stored.cleanup_pending);
    }
    finish(&store, &executor).await;
}

#[tokio::test]
async fn close_completion_uses_native_identity_instance_epoch_not_extra_packet_predicates() {
    // RA.cpp:3903–3945; EntityTraits.cpp:288–410, no token/runtime/node guard.
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    make_ready(&executor, &request).await;
    run(&executor, remove(request.clone(), false)).await;
    effects(&executor);
    let completion = close_completion(&executor, 0);
    for mode in 0..3 {
        let mut bad = completion.clone();
        match mode {
            0 => bad.replica.id += 1,
            1 => bad.replica.instance -= 1,
            _ => bad.unit.current.configuration -= 1,
        }
        assert_eq!(
            run(&executor, Command::CompleteClose(Box::new(bad))).await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        assert_eq!(state(&executor).phase, Phase::Closing);
        assert_eq!(operations(&hooks).len(), 3);
    }
    let mut higher = completion;
    higher.unit.current.configuration += 1;
    higher.replica.node.instance += 1; // not a native payload predicate
    run(&executor, Command::CompleteClose(Box::new(higher))).await;
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(matches!(
        effects(&executor).last(),
        Some(Effect::RemoveReply(_))
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn force_escalation_during_delete_and_open_preserves_native_deleted_reporting() {
    // FT.cpp:4821–4848; DeleteReplica.cpp:388–415 (shared helpers, mislabeled force inputs);
    // direct force-during-open is helper-derived, not original native E2E test execution.
    for deleting_first in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        let first = executor.submit(add(request.clone())).unwrap();
        first.wait(WAIT).await.unwrap();
        effects(&executor);
        registered(&executor, &request, first.id).await;
        if deleting_first {
            run(&executor, remove(request.clone(), false)).await;
            effects(&executor);
        }
        run(&executor, remove(request.clone(), true)).await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::TerminateHost { .. }]
        ));
        assert_eq!(state(&executor).close_mode, Some(CloseMode::Obliterate));
        assert_eq!(state(&executor).local_deleted, deleting_first);
        assert_eq!(
            run(&executor, remove(request.clone(), true)).await,
            Disposition::IgnoredPending
        );
        assert_eq!(
            run(
                &executor,
                Command::CompleteOpen(Box::new(complete(&request, first.id)))
            )
            .await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: request.unit.id,
                host: registration().host,
            })),
        )
        .await;
        let actions = effects(&executor);
        assert!(!actions.iter().any(|e| matches!(e, Effect::RemoveReply(_))));
        assert_eq!(
            actions
                .iter()
                .any(|e| matches!(e, Effect::RequestReportRetry)),
            !deleting_first
        );
        assert_eq!(state(&executor).local_deleted, deleting_first);
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn registration_retry_thresholds_warning_precedence_and_reacquire_are_native_shaped() {
    // RegistrationWrapper.cpp:245–270,273–332; RetryableErrorState.cpp:21–51.
    for reacquire in [false, true] {
        let store = Store::memory(StoreSettings::default()).await.unwrap();
        let executor = Executor::new(
            Arc::new(store.clone()),
            admission(),
            Settings {
                registration_warning: 1,
                registration_drop: 2,
                ..Settings::default()
            },
        )
        .unwrap();
        let request = body(1);
        make_ready(&executor, &request).await;
        run(&executor, remove(request.clone(), false)).await;
        effects(&executor);
        close_done(&executor, APPLICATION_HOST_CRASH).await;
        effects(&executor);
        run(&executor, Command::RetryClose(request.unit.id.clone())).await;
        effects(&executor);
        let lookup = *state(&executor)
            .registration_lookups
            .keys()
            .next_back()
            .unwrap();
        let event = RegistrationCompletion {
            unit: request.unit.id.clone(),
            token: state(&executor).close_token,
            lookup,
            registration: RuntimeRegistration::default(),
            error: -99,
            retryable: true,
            ..RegistrationCompletion::default()
        };
        run(&executor, Command::Registered(Box::new(event.clone()))).await;
        assert_eq!(
            effects(&executor),
            [Effect::Health(HealthEvent::ServiceTypeRegistrationWarning)]
        );
        assert_eq!(state(&executor).phase, Phase::Closing);
        let mut event = event;
        run(&executor, Command::RetryClose(request.unit.id.clone())).await;
        effects(&executor);
        event.lookup = *state(&executor)
            .registration_lookups
            .keys()
            .next_back()
            .unwrap();
        if reacquire {
            event.error = 0;
            event.registration = registration();
            event.registration.acquisition = event.lookup;
            run(&executor, Command::Registered(Box::new(event))).await;
            let actions = effects(&executor);
            // An earlier enqueued node pass coalesces this registration request.
            assert_eq!(
                actions[0],
                Effect::Health(HealthEvent::ClearServiceTypeRegistrationWarning)
            );
            assert_close(&actions[1..], 8, &state(&executor));
            close_done(&executor, 0).await;
        } else {
            run(&executor, Command::Registered(Box::new(event))).await;
        }
        assert_eq!(state(&executor).phase, Phase::Closed);
        assert!(matches!(
            effects(&executor).last(),
            Some(Effect::RemoveReply(_))
        ));
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn lifecycle_effect_budget_is_reserved_through_store_and_retained_effects() {
    let hooks = Arc::new(Hooks::default());
    let store = Store::new(
        Controlled {
            inner: MemoryBackend::new(),
            hooks: hooks.clone(),
        },
        StoreSettings::default(),
    )
    .await
    .unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_effects: 8,
            ..Settings::default()
        },
    )
    .unwrap();
    let request = body(1);
    make_ready(&executor, &request).await;
    let (entered, release) = hooks.pause(false);
    let ticket = executor.submit(remove(request.clone(), false)).unwrap();
    entered.await.unwrap();
    assert!(matches!(
        executor.submit(Command::RetryClose(request.unit.id.clone())),
        Err(Failure::Capacity)
    ));
    release.send(()).unwrap();
    ticket.wait(WAIT).await.unwrap();
    assert_eq!(executor.statistics().effects, 1);
    assert!(matches!(
        executor.submit(Command::RetryClose(request.unit.id.clone())),
        Err(Failure::Capacity)
    ));
    effects(&executor);
    run(&executor, Command::RetryClose(request.unit.id)).await;
    assert_close(&effects(&executor), 8, &state(&executor));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_create_normalization_is_shared_by_initial_and_closed_recreation() {
    // Phase2 B2: FT.cpp:229–235,5926–5929,5962–5973; Epoch.h:13–16/epoch.cpp:11.
    for recreate in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let mut request = body(1);
        request.service.target = 3;
        request.service.minimum = 1;
        if recreate {
            run(&executor, remove(request.clone(), false)).await;
            effects(&executor);
            request.replica.instance += 1;
        }
        request.unit.previous = Epoch {
            configuration: 4,
            data_loss: 1,
        };
        request.unit.target = 9;
        request.unit.minimum = 2;
        run(&executor, add(request.clone())).await;
        let [Effect::Open { token, body, .. }]: [Effect; 1] =
            effects(&executor).try_into().unwrap()
        else {
            panic!("normalized Open")
        };
        assert_eq!(body.unit.previous, Epoch::default());
        assert_eq!((body.unit.target, body.unit.minimum), (3, 1));
        assert_eq!(state(&executor).body, body);
        registered(&executor, &body, token).await;
        run(
            &executor,
            Command::CompleteOpen(Box::new(complete(&body, token))),
        )
        .await;
        effects(&executor);
        run(&executor, add(request)).await;
        assert!(matches!(effects(&executor).as_slice(), [Effect::Reply(r)] if r.unit==body.unit));
        run(&executor, remove((*body).clone(), true)).await;
        effects(&executor);
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: body.unit.id.clone(),
                host: registration().host,
            })),
        )
        .await;
        effects(&executor);
        run(&executor, add((*body).clone())).await;
        let [Effect::ReplicaUp(report)]: [Effect; 1] = effects(&executor).try_into().unwrap()
        else {
            panic!("closed Add report")
        };
        assert_eq!(report.dropped.0[0].intermediate, Epoch::default()); // B1 at both reporting entry points
        finish(&store, &executor).await;
    }
}

fn acquired(
    request: &AddInstance,
    token: u64,
    lookup: u64,
    other_runtime: bool,
) -> RegistrationCompletion {
    let mut runtime = registration();
    runtime.acquisition = lookup;
    runtime.service_type = request.service.service_type.clone();
    if other_runtime {
        runtime.runtime.push_str("-other");
    }
    RegistrationCompletion {
        unit: request.unit.id.clone(),
        token,
        lookup,
        registration: runtime,
        error: 0,
        retryable: false,
        usage_acquired: true,
        version: request.service.version.clone(),
        ..RegistrationCompletion::default()
    }
}

#[tokio::test]
async fn late_acquired_lookup_after_recreation_releases_its_reference_not_active_same_runtime() {
    // Rust adapter ownership regression F1; native STR usage ownership is
    // ServiceTypeRegistrationWrapper.cpp:78–125,135–157. Not live Hosting.
    for other_runtime in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        let old = executor.submit(add(request.clone())).unwrap();
        old.wait(WAIT).await.unwrap();
        effects(&executor);
        run(&executor, remove(request.clone(), false)).await;
        effects(&executor);
        let mut next = request.clone();
        next.replica.instance += 1;
        let new = executor.submit(add(next.clone())).unwrap();
        new.wait(WAIT).await.unwrap();
        effects(&executor);
        registered(&executor, &next, new.id).await;
        let active = state(&executor).runtime.clone();
        assert_eq!(active.as_ref().unwrap().acquisition, new.id);
        let event = acquired(&request, old.id, old.id, other_runtime);
        assert_eq!(
            run(&executor, Command::Registered(Box::new(event.clone()))).await,
            Disposition::IgnoredStale
        );
        assert_eq!(
            effects(&executor),
            [
                Effect::ReleaseRegistration(event.registration.clone()),
                Effect::ReleaseUsage {
                    acquisition: event.lookup
                }
            ]
        );
        assert_eq!(state(&executor).runtime, active);
        assert_eq!(state(&executor).body.replica.instance, 51);
        assert_eq!(
            run(&executor, Command::Registered(Box::new(event))).await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty()); // replay is not a new acquisition/release
        run(
            &executor,
            Command::CompleteOpen(Box::new(complete(&next, new.id))),
        )
        .await;
        assert!(
            matches!(effects(&executor).as_slice(), [Effect::Reply(r)] if r.replica.instance==51)
        );
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn overlapping_lookup_results_dispose_unused_acquisition_and_callback_replay_is_idempotent() {
    for other_runtime in [false, true] {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        let first = executor.submit(add(request.clone())).unwrap();
        first.wait(WAIT).await.unwrap();
        effects(&executor);
        let retry = executor
            .submit(Command::RetryOpen(request.unit.id.clone()))
            .unwrap();
        retry.wait(WAIT).await.unwrap();
        assert!(
            matches!(effects(&executor).as_slice(), [Effect::Open { token, lookup, .. }] if *token==first.id && *lookup==retry.id)
        );
        let one = acquired(&request, first.id, first.id, false);
        let two = acquired(&request, first.id, retry.id, other_runtime);
        run(&executor, Command::Registered(Box::new(one.clone()))).await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [
                Effect::RequestServiceTypeWork { .. },
                Effect::RuntimeOpen { .. }
            ]
        ));
        run(&executor, Command::Registered(Box::new(two.clone()))).await;
        assert_eq!(
            effects(&executor),
            [
                Effect::ReleaseRegistration(two.registration.clone()),
                Effect::ReleaseUsage {
                    acquisition: two.lookup
                }
            ]
        );
        assert_eq!(state(&executor).runtime, Some(one.registration.clone()));
        for event in [one, two] {
            run(&executor, Command::Registered(Box::new(event))).await;
            assert!(effects(&executor).is_empty());
        }
        assert!(state(&executor).registration_lookups.is_empty());
        assert_eq!(operations(&hooks), ["Insert"]); // lookup bookkeeping is memory-only
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn node_notification_aggregates_partitions_and_consumes_only_instance_matched_partial_ack() {
    // Native ServiceTypeNotification.cpp:158-287 uses SP1; map helpers are
    // state-independent. This is a stateless source-derived translation.
    let hooks = Arc::new(Hooks::default());
    let store = Store::new(
        Controlled {
            inner: MemoryBackend::new(),
            hooks: hooks.clone(),
        },
        StoreSettings::default(),
    )
    .await
    .unwrap();
    let clock = TestClock::new();
    let executor = Executor::new_with_clock(
        Arc::new(store.clone()),
        admission(),
        Settings::default(),
        clock.clone(),
    )
    .unwrap();
    let mut first_work = None;
    for id in 1..=3 {
        let mut request = body(id);
        if id == 3 {
            request.service.service_type.name = "Other".into();
        }
        let token = executor.submit(add(request.clone())).unwrap();
        token.wait(WAIT).await.unwrap();
        effects(&executor);
        let actions = registration_actions(&executor, &request, token.id).await;
        if let Effect::RequestServiceTypeWork { work, after_ticks } = actions[0] {
            assert_eq!(after_ticks, 0); // native idle Request is immediately eligible
            assert!(first_work.replace(work).is_none());
        }
        run(
            &executor,
            Command::CompleteOpen(Box::new(complete(&request, token.id))),
        )
        .await;
        effects(&executor);
    }
    let entries = executor.service_types();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries.iter().map(|e| e.references).sum::<usize>(), 3);
    let writes = hooks.calls.load(Ordering::SeqCst);
    let work = first_work.unwrap();
    run(&executor, Command::CaptureServiceTypeWork { work }).await;
    let actions = effects(&executor);
    let [Effect::ServiceTypeWorkCaptured {
        work: captured,
        body: Some(sent),
    }] = actions.as_slice()
    else {
        panic!("{actions:?}");
    };
    assert_eq!(*captured, work);
    assert_eq!(sent.infos.0.len(), 2);
    assert!(executor.service_types().iter().all(|e| e.pending));
    run(&executor, Command::CompleteServiceTypeWork { work }).await;
    let actions = effects(&executor);
    let [Effect::RequestServiceTypeWork {
        work: queued,
        after_ticks: 50_000_000,
    }, Effect::ArmServiceTypeRetry {
        timer,
        after_ticks: 300_000_000,
    }] = actions.as_slice()
    else {
        panic!("{actions:?}");
    };
    let queued = *queued;
    let timer = *timer;
    let mut ack = sent.infos.0[0].clone();
    ack.code_package = "FM can return a different name".into();
    ack.versioned.version.version.value.minor += 1; // instance alone matches
    run(
        &executor,
        Command::ServiceTypeReply {
            body: Box::new(ServiceTypeNotification {
                infos: fube_transport::serialization::serde::ObjectArray(vec![ack]),
            }),
            source: admission().fm_source,
            generation: GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            }, // generic lower-bound, then instance-only ack
        },
    )
    .await;
    assert!(effects(&executor).is_empty());
    assert_eq!(
        executor
            .service_types()
            .iter()
            .filter(|e| e.pending)
            .count(),
        1
    );
    clock.set(CLOCK_ORIGIN + 50_000_000);
    run(&executor, Command::CaptureServiceTypeWork { work: queued }).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ServiceTypeWorkCaptured { body: None, .. }]
    ));
    run(&executor, Command::CompleteServiceTypeWork { work: queued }).await;
    assert!(effects(&executor).is_empty()); // Set does not restart an armed timer
    let mut timer = timer;
    for (offset, expected) in [(300_000_000, 0), (600_000_000, 1)] {
        clock.set(CLOCK_ORIGIN + offset);
        run(&executor, Command::ServiceTypeRetryTimer { timer }).await;
        let actions = effects(&executor);
        let [Effect::RequestServiceTypeWork {
            work,
            after_ticks: 0,
        }] = actions.as_slice()
        else {
            panic!("{actions:?}");
        };
        let work = *work;
        run(&executor, Command::CaptureServiceTypeWork { work }).await;
        let actions = effects(&executor);
        let [Effect::ServiceTypeWorkCaptured { body, .. }] = actions.as_slice() else {
            panic!("{actions:?}");
        };
        assert_eq!(body.as_ref().map_or(0, |b| b.infos.0.len()), expected);
        run(&executor, Command::CompleteServiceTypeWork { work }).await;
        let actions = effects(&executor);
        let [Effect::ArmServiceTypeRetry {
            timer: next,
            after_ticks: 300_000_000,
        }] = actions.as_slice()
        else {
            panic!("{actions:?}");
        };
        timer = *next;
    }
    assert_eq!(hooks.calls.load(Ordering::SeqCst), writes); // node work never inserts FT
    finish(&store, &executor).await;
}

#[tokio::test]
async fn node_attachment_capacity_veto_preserves_acquired_callback_obligation() {
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_service_types: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    make_ready(&executor, &body(1)).await;
    let mut request = body(2);
    request.service.service_type.name = "Other".into();
    let add_ticket = executor.submit(add(request.clone())).unwrap();
    add_ticket.wait(WAIT).await.unwrap();
    effects(&executor);
    let result = acquired(&request, add_ticket.id, add_ticket.id, false);
    let before = executor.snapshot([2; 16]).committed.unwrap();
    assert_eq!(
        executor
            .submit(Command::Registered(Box::new(result.clone())))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Capacity)
    );
    assert!(Arc::ptr_eq(
        &before,
        &executor.snapshot([2; 16]).committed.unwrap()
    ));
    assert!(effects(&executor).is_empty());
    assert!(before.registration_lookups.contains_key(&add_ticket.id));
    run(&executor, remove(body(1), false)).await;
    effects(&executor);
    close_done(&executor, 0).await;
    effects(&executor);
    assert!(executor.service_types().is_empty());
    run(&executor, Command::Registered(Box::new(result.clone()))).await;
    assert_eq!(executor.service_types()[0].references, 1);
    effects(&executor);
    run(&executor, Command::Registered(Box::new(result))).await;
    assert!(effects(&executor).is_empty()); // replay cannot add a second map ref
    assert_eq!(executor.service_types()[0].references, 1);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn node_projected_payload_veto_does_not_consume_lookup_or_change_prior_entry() {
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            notification_batch_bytes: 590,
            ..Settings::default()
        },
    )
    .unwrap();
    let mut first = body(1);
    first.service.service_type.package.name = "P".repeat(200);
    make_ready(&executor, &first).await;
    let before = executor.service_types();
    let mut second = body(2);
    second.service.version.instance = 2;
    second.replica.version = second.service.version.clone();
    let ticket = executor.submit(add(second.clone())).unwrap();
    ticket.wait(WAIT).await.unwrap();
    effects(&executor);
    let mut candidate = acquired(&second, ticket.id, ticket.id, false);
    candidate.registration.code_package = "C".repeat(200);
    assert_eq!(
        executor
            .submit(Command::Registered(Box::new(candidate)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Capacity)
    );
    assert_eq!(executor.service_types(), before);
    assert!(executor
        .snapshot([2; 16])
        .committed
        .unwrap()
        .registration_lookups
        .contains_key(&ticket.id));
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn vanilla_actual_registration_spelling_ignored_payload_and_captured_replacement() {
    // Hosting.ServiceTypeMapEntity.cpp:16-47,77-80; RetryState.h:76-94.
    let hooks = Arc::new(Hooks::default());
    let store = Store::new(
        Controlled {
            inner: MemoryBackend::new(),
            hooks: hooks.clone(),
        },
        StoreSettings::default(),
    )
    .await
    .unwrap();
    let clock = TestClock::new();
    let executor = Executor::new_with_clock(
        Arc::new(store.clone()),
        admission(),
        Settings {
            notification_batch_bytes: 590,
            ..Settings::default()
        },
        clock.clone(),
    )
    .unwrap();
    executor
        .config()
        .set_notification(NotificationProperty::NodeIntervalTicks, 13);
    let mut request = body(1);
    request.service.service_type = ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: "App".into(),
                number: 7,
            },
            name: "Package".into(),
        },
        name: "ÉType\0requested".into(),
    };
    let mut old_body = None;
    let mut first_work = 0;
    let mut actual = request.service.service_type.clone();
    actual.package.application.type_name = "aPP".into();
    actual.package.name = "pACKAGE".into();
    actual.name = "ÉtYPE\0actual".into();
    let mut requests = Vec::new();
    for id in 1..=3 {
        request.unit.id.guid.0 = [id; 16];
        request.unit.consistency.id = request.unit.id.clone();
        if id == 3 {
            request.service.version.instance = 2;
            request.replica.version = request.service.version.clone();
        }
        let add = executor.submit(add(request.clone())).unwrap();
        add.wait(WAIT).await.unwrap();
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::Open { .. }]
        ));
        let mut event = acquired(&request, add.id, add.id, false);
        event.registration.service_type = actual.clone();
        if id == 2 {
            // Both ignored fields exceed the report budget, not the inbound
            // ownership budget. The effective native report remains unchanged.
            event.registration.service_type.name = format!("ÉTYPE\0{}", "ignored".repeat(2000));
            event.registration.code_package = "C".repeat(20_000);
        }
        run(&executor, Command::Registered(Box::new(event.clone()))).await;
        assert_eq!(
            executor.snapshot([id; 16]).committed.unwrap().runtime,
            Some(event.registration),
        );
        let actions = effects(&executor);
        if id == 1 {
            let [Effect::RequestServiceTypeWork {
                work,
                after_ticks: 0,
            }, Effect::RuntimeOpen { .. }] = actions.as_slice()
            else {
                panic!("{actions:?}");
            };
            first_work = *work;
        } else {
            assert!(matches!(actions.as_slice(), [Effect::RuntimeOpen { .. }]));
        }
        let entry = &executor.service_types()[0];
        assert_eq!(entry.references, usize::from(id));
        assert_eq!(entry.info.versioned.id, actual);
        assert_eq!(
            entry.info.versioned.version.instance,
            if id == 3 { 2 } else { 0 }
        );
        assert_eq!(entry.info.code_package, "Code");
        if id == 2 {
            run(
                &executor,
                Command::CaptureServiceTypeWork { work: first_work },
            )
            .await;
            let actions = effects(&executor);
            let [Effect::ServiceTypeWorkCaptured {
                work,
                body: Some(body),
            }] = actions.as_slice()
            else {
                panic!("{actions:?}");
            };
            assert_eq!(*work, first_work);
            assert_eq!(body.infos.0[0].versioned.id, actual);
            assert_eq!(body.infos.0[0].versioned.version.instance, 0);
            old_body = Some(body.clone());
        }
        requests.push(request.clone());
    }
    clock.set(CLOCK_ORIGIN + 7);
    run(
        &executor,
        Command::CompleteServiceTypeWork { work: first_work },
    )
    .await;
    assert_eq!(executor.service_types()[0].last_sent_ticks, 0);
    let actions = effects(&executor);
    assert!(matches!(
        actions.as_slice(),
        [
            Effect::RequestServiceTypeWork { after_ticks: 6, .. },
            Effect::ArmServiceTypeRetry { .. }
        ]
    ));
    let old_body = old_body.unwrap(); // still legal to send this after version replacement
    assert_eq!(old_body.infos.0[0].versioned.version.instance, 0);
    assert_eq!(operations(&hooks), ["Insert", "Insert", "Insert"]);
    for request in requests {
        run(&executor, remove(request.clone(), false)).await;
        effects(&executor);
        let current = executor.snapshot(request.unit.id.guid.0).committed.unwrap();
        run(
            &executor,
            Command::CompleteClose(Box::new(CloseCompletion {
                unit: current.body.unit.clone(),
                replica: current.body.replica.clone(),
                error: 0,
            })),
        )
        .await;
        effects(&executor);
    }
    assert!(executor.service_types().is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn vanilla_public_work_obligations_are_bounded_and_stale_callbacks_are_idempotent() {
    let store = Store::memory(StoreSettings::default()).await.unwrap();
    let clock = TestClock::new();
    let executor = Executor::new_with_clock(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_effects: 8,
            ..Settings::default()
        },
        clock.clone(),
    )
    .unwrap();
    let config = executor.config();
    config.set_notification(NotificationProperty::BatchCount, 0);
    config.set_notification(NotificationProperty::RetryIntervalTicks, 7);
    let request = body(1);
    let token = executor.submit(add(request.clone())).unwrap();
    token.wait(WAIT).await.unwrap();
    effects(&executor);
    let actions = registration_actions(&executor, &request, token.id).await;
    let [Effect::RequestServiceTypeWork {
        work,
        after_ticks: 0,
    }, Effect::RuntimeOpen { .. }] = actions.as_slice()
    else {
        panic!("{actions:?}");
    };
    let work = *work;
    run(
        &executor,
        Command::CompleteOpen(Box::new(complete(&request, token.id))),
    )
    .await;
    assert!(matches!(effects(&executor).as_slice(), [Effect::Reply(_)]));
    run(&executor, Command::CaptureServiceTypeWork { work }).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ServiceTypeWorkCaptured { body: None, .. }]
    ));
    assert_eq!(
        run(&executor, Command::CaptureServiceTypeWork { work }).await,
        Disposition::IgnoredStale
    );
    assert!(effects(&executor).is_empty());
    // Requests received while executing retain ONE followup, not unbounded jobs.
    for _ in 0..10 {
        run(&executor, Command::RequestServiceTypeWork).await;
        assert!(effects(&executor).is_empty());
    }
    run(&executor, Command::CompleteServiceTypeWork { work }).await;
    let actions = effects(&executor);
    let [Effect::RequestServiceTypeWork {
        work: second,
        after_ticks: 50_000_000,
    }, Effect::ArmServiceTypeRetry {
        timer,
        after_ticks: 7,
    }] = actions.as_slice()
    else {
        panic!("{actions:?}");
    };
    let second = *second;
    let timer = *timer;
    assert_eq!(
        run(&executor, Command::CompleteServiceTypeWork { work }).await,
        Disposition::IgnoredStale
    );
    assert!(effects(&executor).is_empty());
    assert_eq!(executor.service_types()[0].last_sent_ticks, 0);
    // The timer clears isSet but coalesces into the waiting BGM obligation.
    clock.set(CLOCK_ORIGIN + 7);
    run(&executor, Command::ServiceTypeRetryTimer { timer }).await;
    assert!(effects(&executor).is_empty());
    assert_eq!(
        run(&executor, Command::ServiceTypeRetryTimer { timer }).await,
        Disposition::IgnoredStale
    );
    config.set_notification(NotificationProperty::BatchCount, -1);
    config.set_notification(NotificationProperty::RetryIntervalTicks, 3);
    clock.set(CLOCK_ORIGIN + 50_000_000);
    run(&executor, Command::CaptureServiceTypeWork { work: second }).await;
    let actions = effects(&executor);
    assert!(
        matches!(actions.as_slice(), [Effect::ServiceTypeWorkCaptured { body: Some(body), .. }] if body.infos.0.len()==1)
    );
    run(&executor, Command::CompleteServiceTypeWork { work: second }).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ArmServiceTypeRetry { after_ticks: 3, .. }]
    ));
    assert_eq!(
        executor.service_types()[0].last_sent_ticks,
        CLOCK_ORIGIN + 50_000_000
    );
    for _ in 0..7 {
        run(&executor, add(request.clone())).await;
    }
    let before = executor.statistics();
    let types = executor.service_types();
    assert!(matches!(
        executor.submit(Command::RequestServiceTypeWork),
        Err(Failure::Capacity)
    ));
    assert_eq!(executor.statistics(), before);
    assert_eq!(executor.service_types(), types);
    let retained = effects(&executor);
    assert_eq!(retained.len(), 7);
    assert!(retained
        .iter()
        .all(|effect| matches!(effect, Effect::Reply(_))));
    run(&executor, Command::RequestServiceTypeWork).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RequestServiceTypeWork { .. }]
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn node_detachment_is_atomic_with_definite_unknown_and_cancelled_entity_completion() {
    // Rust P1/ownership adaptation, not original native fault-test execution.
    for uncertain in [false, true] {
        for fail in [false, true] {
            let (store, executor, hooks) = setup(if uncertain {
                Duration::from_millis(20)
            } else {
                WAIT
            })
            .await;
            let request = body(1);
            let opening = executor.submit(add(request.clone())).unwrap();
            opening.wait(WAIT).await.unwrap();
            effects(&executor);
            let actions = registration_actions(&executor, &request, opening.id).await;
            let [Effect::RequestServiceTypeWork {
                work,
                after_ticks: 0,
            }, Effect::RuntimeOpen { .. }] = actions.as_slice()
            else {
                panic!("{actions:?}");
            };
            let work = *work;
            run(
                &executor,
                Command::CompleteOpen(Box::new(complete(&request, opening.id))),
            )
            .await;
            effects(&executor);
            run(&executor, remove(body(1), false)).await;
            effects(&executor);
            let previous = state(&executor);
            let node_before = executor.service_types();
            let event = close_completion(&executor, 0);
            let (entered, release) = hooks.pause(fail);
            let ticket = executor
                .submit(Command::CompleteClose(Box::new(event)))
                .unwrap();
            entered.await.unwrap();
            if uncertain {
                assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
            }
            assert!(Arc::ptr_eq(&previous, &state(&executor)));
            assert_eq!(executor.service_types(), node_before);
            assert!(effects(&executor).is_empty());
            // Drop this observer; a queued node retry cannot lose the attachment.
            drop(ticket);
            run(&executor, Command::CaptureServiceTypeWork { work }).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::ServiceTypeWorkCaptured { body: Some(_), .. }]
            ));
            run(&executor, Command::CompleteServiceTypeWork { work }).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::ArmServiceTypeRetry { .. }]
            ));
            release.send(()).unwrap();
            // Same-entity sentinel proves publication/reservation release completed.
            run(&executor, remove(body(1), false)).await;
            effects(&executor);
            if fail {
                assert_eq!(state(&executor).phase, Phase::Closing);
                assert_eq!(executor.service_types()[0].references, 1);
            } else {
                assert_eq!(state(&executor).phase, Phase::Closed);
                assert!(executor.service_types().is_empty());
            }
            assert_eq!(
                operations(&hooks)
                    .iter()
                    .filter(|&&s| s == "Update")
                    .count(),
                3
            );
            finish(&store, &executor).await;
        }
    }
}
#[tokio::test]
async fn vanilla_generic_generation_guard_precedes_node_ack() {
    // MessageHandler.cpp:237-244,815-822; GenerationStateManager.cpp:29-40.
    for (number, owner, accepted) in [
        (0, [8; 16], false),
        (6, [8; 16], false),
        (7, [7; 16], false),
        (7, [8; 16], true),
        (7, [9; 16], true),
        (8, [0; 16], true),
    ] {
        let (store, executor, _) = setup(WAIT).await;
        make_ready(&executor, &body(1)).await;
        let info = executor.service_types()[0].info.clone();
        let generation = GenerationNumber {
            generation: number,
            owner,
        };
        let ticket = executor
            .submit(Command::ServiceTypeReply {
                body: Box::new(ServiceTypeNotification {
                    infos: fube_transport::serialization::serde::ObjectArray(vec![info]),
                }),
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: generation,
                    for_fm_replica: false,
                },
            })
            .unwrap();
        assert_eq!(
            ticket.wait(WAIT).await,
            Ok(if accepted {
                Disposition::Applied
            } else {
                Disposition::IgnoredStale
            })
        );
        assert_eq!(executor.service_types()[0].pending, !accepted);
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn vanilla_raw_inbuild_open_reply_establishes_ready_without_copying_other_fields() {
    // RAP ActionListExecutor.cpp:346-357; FailoverUnit.cpp:5302-5368.
    let (store, executor, _) = setup(WAIT).await;
    let token = begin_open(&executor).await;
    registered(&executor, &body(1), token).await;
    let before = state(&executor);
    let mut reply = runtime_result(&executor, 0, 0);
    reply.replica = before.body.replica.clone(); // actual raw request-shaped IB
    reply.replica.service_endpoint = "native endpoint".into();
    reply.replica.replication_endpoint = "native replicator endpoint".into();
    reply.replica.id += 99;
    reply.replica.node.instance += 1;
    reply.replica.version.instance += 1;
    reply.replica.first_lsn = 123;
    reply.replica.current_role = 0;
    reply.replica.up = false;
    reply.unit.current.configuration += 1; // generic rejects LOWER epoch only
    run(&executor, Command::CompleteOpen(Box::new(reply))).await;
    let mut expected = before.body.replica.clone();
    expected.state = 2;
    expected.service_endpoint = "native endpoint".into();
    expected.replication_endpoint = "native replicator endpoint".into();
    assert_eq!(state(&executor).body.replica, expected);
    assert_eq!(state(&executor).body.unit, before.body.unit);
    assert!(
        matches!(effects(&executor).as_slice(),[Effect::Reply(r)] if r.replica==expected && r.error==0)
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn vanilla_dynamic_open_policy_accepts_old_abort_echo_and_current_success_clear() {
    // ReconfigurationAgent.cpp:3819-3828: asymmetric stale RETURN vs TESTASSERT.
    // RetryableErrorState.cpp:27-77: separate CURRENT getter calls.
    let (store, executor, hooks) = setup_open(
        Settings {
            open_retry: RetryPolicy {
                warning: 10,
                drop: 2,
                ..RetryPolicy::open()
            },
            ..Settings::default()
        },
        WAIT,
    )
    .await;
    let token = begin_open(&executor).await;
    registered(&executor, &body(1), token).await;
    runtime_reply(&executor, -9, 0).await;
    assert!(effects(&executor).is_empty());
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RuntimeOpen { flags: ABORT, .. }]
    ));
    let cfg = executor.config();
    cfg.set(RetryState::Opening, Threshold::Drop, 4);
    runtime_reply(&executor, -9, ABORT).await; // default-disabled assertion is not reject
    assert_eq!(state(&executor).open_failures, 2);
    assert!(effects(&executor).is_empty());
    cfg.set(RetryState::Opening, Threshold::Drop, 1); // lowered below current
    runtime_reply(&executor, -9, 0).await;
    assert_eq!(state(&executor).open_failures, 3);
    cfg.set(RetryState::Opening, Threshold::Warning, -1);
    cfg.set(RetryState::Opening, Threshold::Error, 0);
    runtime_reply(&executor, 0, ABORT).await;
    assert!(
        matches!(effects(&executor).as_slice(),[Effect::OpeningHealth(h),Effect::Reply(r)] if h.event==HealthEvent::ClearOpenWarning && r.error==0)
    );
    assert_eq!(state(&executor).open_failures, 0);
    assert_eq!(
        operations(&hooks),
        ["Insert", "Update", "Update", "Update", "Update"]
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn vanilla_dynamic_registration_signed_thresholds_and_warning_tie() {
    let (store, executor, hooks) = setup_open(
        Settings {
            open_registration: RetryPolicy {
                warning: 0,
                drop: -1,
                restart: -2,
                error: 0,
            },
            ..Settings::default()
        },
        WAIT,
    )
    .await;
    begin_open(&executor).await;
    let cfg = executor.config();
    for count in 1..=3 {
        if count == 2 {
            cfg.set(RetryState::Registration, Threshold::Drop, 1);
        }
        if count == 3 {
            cfg.set(RetryState::Registration, Threshold::Warning, 3);
            cfg.set(RetryState::Registration, Threshold::Drop, 3);
        }
        let e = lookup_result(&executor, HOSTING_NOT_FOUND);
        run(&executor, Command::Registered(Box::new(e))).await;
        assert_eq!(state(&executor).registration_failures, count);
        assert_eq!(effects(&executor).len(), usize::from(count == 3));
        assert_eq!(state(&executor).phase, Phase::Opening);
        run(&executor, Command::RetryOpen(body(1).unit.id)).await;
        effects(&executor);
    }
    cfg.set(RetryState::Registration, Threshold::Warning, 100);
    cfg.set(RetryState::Registration, Threshold::Error, 100);
    let e = lookup_result(&executor, 0);
    run(&executor, Command::Registered(Box::new(e))).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [
            Effect::RequestServiceTypeWork { .. },
            Effect::RuntimeOpen { .. }
        ]
    ));
    assert_eq!(operations(&hooks), ["Insert"]); // no remembered-warning clear
    finish(&store, &executor).await;
}

#[tokio::test]
async fn vanilla_dynamic_delete_and_registration_drop_clear_uses_current_thresholds() {
    for earlier_warning in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        make_ready(&executor, &body(1)).await;
        let cfg = executor.config();
        cfg.set(
            RetryState::Deleting,
            Threshold::Warning,
            if earlier_warning { 1 } else { 10 },
        );
        cfg.set(RetryState::Closing, Threshold::Warning, 0); // independent family
        run(&executor, remove(body(1), false)).await;
        effects(&executor);
        close_done(&executor, -9).await;
        assert_eq!(effects(&executor).len(), usize::from(earlier_warning));
        cfg.set(RetryState::Deleting, Threshold::Warning, 10);
        cfg.set(
            RetryState::Deleting,
            Threshold::Error,
            if earlier_warning { 10 } else { -1 },
        );
        close_done(&executor, 0).await;
        let actions = effects(&executor);
        assert_eq!(
            matches!(
                actions.first(),
                Some(Effect::Health(HealthEvent::ClearCloseWarning))
            ),
            !earlier_warning
        );
        assert_eq!(state(&executor).phase, Phase::Closed);
        finish(&store, &executor).await;

        let (store, executor, _) = setup(WAIT).await;
        make_ready(&executor, &body(1)).await;
        let cfg = executor.config();
        cfg.set(
            RetryState::RegistrationAtDrop,
            Threshold::Warning,
            if earlier_warning { 1 } else { 10 },
        );
        run(&executor, remove(body(1), false)).await;
        effects(&executor);
        close_done(&executor, APPLICATION_HOST_CRASH).await;
        effects(&executor);
        run(&executor, Command::RetryClose(body(1).unit.id)).await;
        effects(&executor);
        let e = lookup_result(&executor, -99); // existing registration retries unknown
        run(&executor, Command::Registered(Box::new(e))).await;
        assert_eq!(effects(&executor).len(), usize::from(earlier_warning));
        cfg.set(RetryState::RegistrationAtDrop, Threshold::Warning, 10);
        cfg.set(
            RetryState::Registration,
            Threshold::Error,
            if earlier_warning { 10 } else { 0 },
        );
        run(&executor, Command::RetryClose(body(1).unit.id)).await;
        effects(&executor);
        let e = lookup_result(&executor, 0);
        run(&executor, Command::Registered(Box::new(e))).await;
        let actions = effects(&executor);
        assert_eq!(
            actions.iter().any(|e| matches!(
                e,
                Effect::Health(HealthEvent::ClearServiceTypeRegistrationWarning)
            )),
            !earlier_warning
        );
        assert_eq!(state(&executor).phase, Phase::Closing);
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn vanilla_retuning_during_unknown_commit_preserves_proposal_then_uses_new_policy() {
    let (store, executor, hooks) = setup_open(
        Settings {
            open_retry: RetryPolicy {
                warning: 1,
                drop: 40,
                ..RetryPolicy::open()
            },
            ..Settings::default()
        },
        Duration::from_millis(20),
    )
    .await;
    let token = begin_open(&executor).await;
    registered(&executor, &body(1), token).await;
    let before = state(&executor);
    let (entered, release) = hooks.pause(false);
    let ticket = executor
        .submit(Command::CompleteOpen(Box::new(runtime_result(
            &executor, -9, 0,
        ))))
        .unwrap();
    entered.await.unwrap();
    assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
    let cfg = executor.config();
    cfg.set(RetryState::Opening, Threshold::Warning, 100);
    cfg.set(RetryState::Opening, Threshold::Error, 100);
    assert!(Arc::ptr_eq(&before, &state(&executor)));
    assert!(effects(&executor).is_empty());
    release.send(()).unwrap();
    ticket.resolve(WAIT).await.unwrap();
    assert_eq!(state(&executor).open_failures, 1);
    assert!(
        matches!(effects(&executor).as_slice(),[Effect::OpeningHealth(h)] if h.event==HealthEvent::OpenWarning)
    );
    // Persistent store stays terminally fenced after its own deadline. A memory
    // retry still reads new thresholds without replacing/reopening the store.
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RuntimeOpen { flags: 0, .. }]
    ));
    assert_eq!(operations(&hooks), ["Insert", "Update"]);
    finish(&store, &executor).await;
}

async fn setup_open(settings: Settings, timeout: Duration) -> (Store, Executor, Arc<Hooks>) {
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
    let executor = Executor::new(Arc::new(store.clone()), admission(), settings).unwrap();
    (store, executor, hooks)
}
async fn begin_open(executor: &Executor) -> u64 {
    let ticket = executor.submit(add(body(1))).unwrap();
    ticket.wait(WAIT).await.unwrap();
    assert!(matches!(
        effects(executor).as_slice(),
        [Effect::Open { .. }]
    ));
    ticket.id
}
fn lookup_result(executor: &Executor, error: i64) -> RegistrationCompletion {
    let s = state(executor);
    let (&lookup, &(token, _)) = s.registration_lookups.last_key_value().unwrap();
    let mut event = acquired(&s.body, token, lookup, false);
    event.error = error;
    event.usage_acquired = error == 0 && s.usage.is_none();
    event
}
fn runtime_result(executor: &Executor, error: i64, flags: i64) -> OpenCompletion {
    let s = state(executor);
    let mut event = complete(&s.body, s.open_token);
    event.error = error;
    event.flags = flags;
    if error != 0 && error != PROXY_STATE_CHANGED_ON_DATA_LOSS {
        event.replica = s.body.replica.clone();
    }
    event
}
async fn runtime_reply(executor: &Executor, error: i64, flags: i64) {
    let event = runtime_result(executor, error, flags);
    run(executor, Command::CompleteOpen(Box::new(event))).await;
}

#[tokio::test]
async fn native_new_registration_allowlist_fatal_and_unknown_errors_preserve_original_reply() {
    // Direct SL: Test.StateMachine.cpp:2602-2617,2633-2650,3102-3120;
    // actual classification: Hosting.FindServiceTypeRegistrationErrorList.cpp:35-65.
    for error in [
        HOSTING_DEPLOYMENT_IN_PROGRESS,
        HOSTING_ACTIVATION_IN_PROGRESS,
        HOSTING_SERVICE_TYPE_NOT_REGISTERED,
        HOSTING_NOT_FOUND,
        HOSTING_APPLICATION_VERSION_MISMATCH,
        HOSTING_PACKAGE_VERSION_MISMATCH,
        HOSTING_REGISTRATION_VERSION_MISMATCH,
        HOSTING_RUNTIME_NOT_REGISTERED,
        HOSTING_SERVICE_TYPE_DISABLED,
        APPLICATION_INSTANCE_DELETED,
        -1234567,
    ] {
        let (store, executor, hooks) = setup(WAIT).await;
        begin_open(&executor).await;
        let captured = state(&executor).body.clone();
        let mut event = lookup_result(&executor, error);
        event.registration = RuntimeRegistration::default();
        event.retryable = !registration_retryable(error); // caller bool cannot override open policy
        run(&executor, Command::Registered(Box::new(event.clone()))).await;
        assert!(state(&executor).registration_lookups.is_empty());
        if registration_retryable(error) {
            assert_eq!(state(&executor).phase, Phase::Opening);
            assert_eq!(state(&executor).registration_failures, 1);
            assert!(effects(&executor).is_empty());
        } else {
            assert_eq!(state(&executor).phase, Phase::Closed);
            assert_eq!(state(&executor).registration_failures, 0);
            assert_eq!(
                effects(&executor),
                vec![
                    Effect::Health(HealthEvent::Close),
                    Effect::Reply(Box::new(AddInstanceReply {
                        unit: captured.unit.clone(),
                        replica: captured.replica.clone(),
                        error,
                    }))
                ]
            );
        }
        assert_eq!(operations(&hooks), ["Insert"]); // native FSR retry is memory-only
        run(&executor, Command::Registered(Box::new(event))).await;
        assert!(effects(&executor).is_empty());
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_registration_threshold_warning_clear_true_exhaustion_and_ties() {
    // Direct SL :2669-2718,3127-3140; true retryable drop is wrapper unit
    // Test.Unit.ServiceTypeRegistrationWrapper.cpp:613-621, not the misleading
    // ExceedThresholdAtFSR test which actually injects a fatal error.
    for complete_success in [false, true] {
        let settings = Settings {
            open_registration: RetryPolicy {
                warning: 2,
                drop: 3,
                ..RetryPolicy::registration()
            },
            ..Settings::default()
        };
        let (store, executor, hooks) = setup_open(settings, WAIT).await;
        begin_open(&executor).await;
        for n in 1..=2 {
            let mut event = lookup_result(&executor, HOSTING_DEPLOYMENT_IN_PROGRESS);
            event.error_message = "deployment pending".into();
            run(&executor, Command::Registered(Box::new(event))).await;
            assert_eq!(state(&executor).registration_failures, n);
            let expected = if n == 2 {
                vec![Effect::OpeningHealth(Box::new(OpeningHealthReport {
                    event: HealthEvent::ServiceTypeRegistrationWarning,
                    error: HOSTING_DEPLOYMENT_IN_PROGRESS,
                    detail: ProxyErrorDetail {
                        message: "deployment pending".into(),
                        ..ProxyErrorDetail::default()
                    },
                }))]
            } else {
                vec![]
            };
            assert_eq!(effects(&executor), expected);
            run(&executor, Command::RetryOpen(body(1).unit.id)).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::Open { .. }]
            ));
        }
        let event = lookup_result(
            &executor,
            if complete_success {
                0
            } else {
                HOSTING_DEPLOYMENT_IN_PROGRESS
            },
        );
        run(&executor, Command::Registered(Box::new(event))).await;
        if complete_success {
            assert!(
                matches!(effects(&executor).as_slice(), [Effect::RequestServiceTypeWork { .. }, Effect::OpeningHealth(h), Effect::RuntimeOpen { flags:0,.. }] if h.event==HealthEvent::ClearServiceTypeRegistrationWarning)
            );
            assert_eq!(state(&executor).phase, Phase::Opening);
            assert_eq!(state(&executor).registration_failures, 0);
        } else {
            assert_eq!(state(&executor).phase, Phase::Closed);
            assert!(
                matches!(effects(&executor).as_slice(), [Effect::Health(HealthEvent::Close), Effect::Reply(r)] if r.error==HOSTING_DEPLOYMENT_IN_PROGRESS && r.replica.state==1)
            );
        }
        assert_eq!(operations(&hooks), ["Insert"]);
        finish(&store, &executor).await;
    }
    let settings = Settings {
        open_registration: RetryPolicy {
            warning: 1,
            drop: 1,
            restart: 2,
            error: 3,
        },
        ..Settings::default()
    };
    let (store, executor, _) = setup_open(settings, WAIT).await;
    begin_open(&executor).await;
    for n in 1..=4 {
        let event = lookup_result(&executor, HOSTING_NOT_FOUND);
        run(&executor, Command::Registered(Box::new(event))).await;
        assert_eq!(state(&executor).registration_failures, n);
        let actions = effects(&executor);
        assert_eq!(actions.len(), usize::from(n == 1)); // warning wins tie; restart/error ignored
        assert_eq!(state(&executor).phase, Phase::Opening);
        run(&executor, Command::RetryOpen(body(1).unit.id)).await;
        effects(&executor);
    }
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_runtime_warning_abort_guard_terminal_cleanup_and_captured_reply() {
    // Direct SL :2763-2788,2809-2852,3155-3194; final guard :3400-3437,4569-4576.
    let settings = Settings {
        open_retry: RetryPolicy {
            warning: 1,
            drop: 3,
            ..RetryPolicy::open()
        },
        ..Settings::default()
    };
    let (store, executor, hooks) = setup_open(settings, WAIT).await;
    let token = begin_open(&executor).await;
    registered(&executor, &body(1), token).await;
    let captured = state(&executor).body.clone();
    let mut event = runtime_result(&executor, -123, 0);
    event.detail = ProxyErrorDetail {
        message: "user Open failed".into(),
        api: ApiNameDescription {
            api: 3,
            interface: 2,
            metadata: "OpenInstance".into(),
        },
    };
    run(&executor, Command::CompleteOpen(Box::new(event.clone()))).await;
    assert_eq!(state(&executor).open_failures, 1);
    assert_eq!(state(&executor).registration_failures, 0);
    assert_eq!(
        effects(&executor),
        [Effect::OpeningHealth(Box::new(OpeningHealthReport {
            event: HealthEvent::OpenWarning,
            error: -123,
            detail: event.detail.clone()
        }))]
    );
    runtime_reply(&executor, HOSTING_SERVICE_TYPE_DISABLED, 0).await; // not fatal at RAP layer
    assert!(effects(&executor).is_empty());
    assert_eq!(state(&executor).open_failures, 2);
    let before = state(&executor);
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RuntimeOpen { flags: ABORT, .. }]
    ));
    assert_eq!(state(&executor).open_failures, 2); // sending is not failing
    for error in [0, -99] {
        assert_eq!(
            run(
                &executor,
                Command::CompleteOpen(Box::new(runtime_result(&executor, error, 0)))
            )
            .await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        assert_eq!(state(&executor).open_failures, before.open_failures);
    }
    let terminal = runtime_result(&executor, -777, ABORT);
    let registration = state(&executor).runtime.clone().unwrap();
    run(&executor, Command::CompleteOpen(Box::new(terminal.clone()))).await;
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(state(&executor).usage.is_none() && state(&executor).runtime.is_none());
    assert_eq!(
        effects(&executor),
        [
            Effect::ReleaseRegistration(registration),
            Effect::ReleaseUsage { acquisition: token },
            Effect::Health(HealthEvent::Close),
            Effect::Reply(Box::new(AddInstanceReply {
                unit: captured.unit.clone(),
                replica: captured.replica.clone(),
                error: -777
            })),
        ]
    );
    assert!(executor.service_types().is_empty());
    assert_eq!(operations(&hooks), ["Insert", "Update", "Update", "Update"]);
    assert_eq!(
        run(&executor, Command::CompleteOpen(Box::new(terminal))).await,
        Disposition::IgnoredStale
    );
    assert_eq!(
        run(&executor, Command::RetryOpen(body(1).unit.id)).await,
        Disposition::IgnoredPending
    );
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn returned_version_keeps_original_add_retry_and_closed_recreation_supported() {
    // ReconfigurationAgent.cpp:1013-1033,1660-1683; FailoverUnit.cpp:5862-5883.
    // Version selection is shared source evidence, not a direct SL native test.
    for remove_after_ready in [false, true] {
        let settings = Settings {
            open_retry: RetryPolicy {
                warning: 10,
                drop: 1,
                ..RetryPolicy::open()
            },
            ..Settings::default()
        };
        let (store, executor, hooks) = setup_open(settings, WAIT).await;
        begin_open(&executor).await;
        let mut candidate = lookup_result(&executor, 0);
        candidate.error = HOSTING_REGISTRATION_VERSION_MISMATCH;
        candidate.version.instance = 9;
        run(&executor, Command::Registered(Box::new(candidate))).await;
        effects(&executor);
        assert_eq!(
            run(&executor, add(body(1))).await,
            Disposition::IgnoredPending
        );
        if remove_after_ready {
            runtime_reply(&executor, 0, ABORT).await;
            effects(&executor);
            let writes = operations(&hooks);
            run(&executor, add(body(1))).await;
            assert!(
                matches!(effects(&executor).as_slice(),[Effect::Reply(r)] if r.error==0 && r.replica.version.instance==9 && r.replica.state==2)
            );
            assert_eq!(operations(&hooks), writes);
            run(&executor, remove(body(1), false)).await;
            effects(&executor);
            close_done(&executor, 0).await;
            effects(&executor);
        } else {
            let failed = runtime_result(&executor, -45, ABORT);
            run(&executor, Command::CompleteOpen(Box::new(failed))).await;
            effects(&executor);
        }
        assert!(state(&executor).runtime.is_none() && state(&executor).usage.is_none());
        assert!(executor.service_types().is_empty());
        let mut next = body(1);
        next.replica.instance += 1;
        run(&executor, add(next)).await;
        assert!(
            matches!(effects(&executor).as_slice(),[Effect::Open {body,usage:None,..}] if body.service.version.instance==0 && body.replica.instance==51)
        );
        assert_eq!(state(&executor).phase, Phase::Opening);
        assert_eq!(state(&executor).requested_version.instance, 0);
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn closing_reacquisition_normalizes_version_and_disposes_rejected_mismatch_once() {
    // Shared helper: ReconfigurationAgent.cpp:987-1033;
    // FailoverUnit.cpp:5072-5130,5919-5924. Unique acquisitions are a Rust seam.
    for (error, required) in [
        (0, None),
        (HOSTING_REGISTRATION_VERSION_MISMATCH, None),
        (HOSTING_REGISTRATION_VERSION_MISMATCH, Some(8)),
        (HOSTING_REGISTRATION_VERSION_MISMATCH, Some(9)),
        (HOSTING_REGISTRATION_VERSION_MISMATCH, Some(10)),
    ] {
        let (store, executor, hooks) = setup(WAIT).await;
        let token = make_ready(&executor, &body(1)).await;
        run(&executor, remove(body(1), false)).await;
        effects(&executor);
        close_done(&executor, APPLICATION_HOST_CRASH).await;
        effects(&executor);
        run(&executor, Command::RetryClose(body(1).unit.id)).await;
        effects(&executor);
        let mut event = lookup_result(&executor, 0);
        event.error = error;
        event.version.instance = 9;
        event.registration.code_package = "ActualCode".into();
        event.upgrade_present = required.is_some();
        event.required_upgrade = required.unwrap_or_default();
        event.retryable = true;
        assert!(!event.usage_acquired);
        let writes = operations(&hooks);
        run(&executor, Command::Registered(Box::new(event.clone()))).await;
        assert_eq!(state(&executor).usage, Some(token));
        assert!(state(&executor).registration_lookups.is_empty());
        if required == Some(10) {
            assert_eq!(
                effects(&executor),
                [Effect::ReleaseRegistration(event.registration.clone())]
            );
            assert!(state(&executor).runtime.is_none());
            assert!(executor.service_types().is_empty());
            assert_eq!(state(&executor).registration_failures, 1);
        } else {
            assert_eq!(state(&executor).runtime, Some(event.registration.clone()));
            let actions = effects(&executor);
            assert!(
                matches!(actions.as_slice(),[Effect::Close {body,flags:8,registration,..}] if body.service.version.instance==9 && body.replica.version==VersionInstance::invalid() && registration==&event.registration)
            );
            // SendReplicaCloseMessage.cpp authority is FailoverUnit.cpp:4760-4778:
            // wire closing replica is canonical, not the local selected version.
            assert_close(&actions, 8, &state(&executor));
            assert_eq!(state(&executor).body.replica.version.instance, 9);
            let info = executor.service_types()[0].info.clone();
            assert_eq!(info.versioned.version.instance, 9);
            assert_eq!(info.code_package, "ActualCode");
            let mut old = info.clone();
            old.versioned.version.instance = 0;
            for (ack, pending) in [(old, true), (info, false)] {
                run(
                    &executor,
                    Command::ServiceTypeReply {
                        body: Box::new(ServiceTypeNotification {
                            infos: fube_transport::serialization::serde::ObjectArray(vec![ack]),
                        }),
                        source: admission().fm_source,
                        generation: GenerationHeader {
                            number: admission().generation,
                            for_fm_replica: false,
                        },
                    },
                )
                .await;
                assert_eq!(executor.service_types()[0].pending, pending);
            }
        }
        assert_eq!(operations(&hooks), writes);
        assert_eq!(
            run(&executor, Command::Registered(Box::new(event))).await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        if required != Some(10) {
            close_done(&executor, 0).await;
            effects(&executor);
            assert!(state(&executor).usage.is_none());
            assert!(executor.service_types().is_empty());
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_usage_increment_failure_and_returned_version_selection_are_distinct() {
    // Direct SL usage failure :2619-2631,3112-3115; version mismatch selection
    // ReconfigurationAgent.cpp:986-1033 is shared/helper-derived, not SL test.
    let settings = Settings {
        open_registration: RetryPolicy {
            warning: 1,
            ..RetryPolicy::registration()
        },
        ..Settings::default()
    };
    let (store, executor, hooks) = setup_open(settings, WAIT).await;
    begin_open(&executor).await;
    let failure = lookup_result(&executor, HOSTING_NOT_FOUND);
    run(&executor, Command::Registered(Box::new(failure))).await;
    effects(&executor);
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    effects(&executor);
    let mut usage_fail = lookup_result(&executor, 0);
    usage_fail.usage_acquired = false;
    usage_fail.usage_error = -88;
    run(&executor, Command::Registered(Box::new(usage_fail.clone()))).await;
    assert_eq!(
        effects(&executor),
        [Effect::ReleaseRegistration(usage_fail.registration)]
    );
    assert_eq!(state(&executor).registration_failures, 1);
    assert!(state(&executor).usage.is_none());
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    effects(&executor);
    let mut event = lookup_result(&executor, HOSTING_REGISTRATION_VERSION_MISMATCH);
    event.version.instance = 9;
    event.usage_acquired = true; // adapter transfers successful increment after version selection
    event.upgrade_present = true;
    event.required_upgrade = 8;
    run(&executor, Command::Registered(Box::new(event.clone()))).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::RequestServiceTypeWork {..}, Effect::OpeningHealth(h), Effect::RuntimeOpen { body,.. }] if h.event==HealthEvent::ClearServiceTypeRegistrationWarning && body.replica.version.instance==9 && body.service.version.instance==9)
    );
    assert_eq!(state(&executor).usage, Some(event.lookup));
    assert_eq!(
        executor.service_types()[0].info.versioned.version.instance,
        9
    );
    assert_eq!(
        run(&executor, add(body(1))).await,
        Disposition::IgnoredPending
    );
    runtime_reply(&executor, PROXY_STATE_CHANGED_ON_DATA_LOSS, 0).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::Reply(r)] if r.error==0 && r.replica.state==2 && !r.replica.service_endpoint.is_empty())
    );
    assert_eq!(operations(&hooks), ["Insert", "Update"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_opening_host_down_retains_usage_accepts_replacement_and_exhausts_without_abort_reply(
) {
    // Handler-derived stateless tuples: ReconfigurationAgent.cpp:1138-1183;
    // FailoverUnit.cpp:5258-5279. Native late-open tests in Close.cpp:872-889
    // are SP1 and not an independent direct-SL oracle.
    let settings = Settings {
        open_retry: RetryPolicy {
            warning: 1,
            drop: 2,
            ..RetryPolicy::open()
        },
        ..Settings::default()
    };
    let (store, executor, hooks) = setup_open(settings, WAIT).await;
    let token = begin_open(&executor).await;
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    effects(&executor);
    let mut replacement = lookup_result(&executor, 0);
    replacement.registration.host = "replacement-host".into();
    // Adopt the initial lookup, leaving the pre-crash second lookup outstanding.
    run(
        &executor,
        Command::Registered(Box::new(acquired(&body(1), token, token, false))),
    )
    .await;
    effects(&executor);
    let old_open = runtime_result(&executor, 0, 0);
    run(
        &executor,
        Command::HostDown(Box::new(HostDown {
            unit: body(1).unit.id,
            host: registration().host,
        })),
    )
    .await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::OpeningHealth(h), Effect::ReleaseRegistration(_)] if h.event==HealthEvent::OpenWarning && h.error==APPLICATION_HOST_CRASH)
    );
    assert!(state(&executor).runtime.is_none());
    assert_eq!(state(&executor).usage, Some(token));
    assert_eq!(state(&executor).open_failures, 1);
    assert!(executor.service_types().is_empty());
    run(
        &executor,
        Command::Registered(Box::new(replacement.clone())),
    )
    .await;
    assert!(matches!(effects(&executor).as_slice(),[
        Effect::ReleaseUsage {acquisition},Effect::RuntimeOpen {flags:ABORT,..}
    ] if *acquisition==replacement.lookup));
    assert_eq!(
        run(&executor, Command::CompleteOpen(Box::new(old_open.clone()))).await,
        Disposition::IgnoredStale
    );
    assert_eq!(state(&executor).usage, Some(token));
    assert_eq!(
        run(&executor, Command::CompleteOpen(Box::new(old_open))).await,
        Disposition::IgnoredStale
    );
    assert!(effects(&executor).is_empty());
    let captured = state(&executor).body.clone();
    run(
        &executor,
        Command::HostDown(Box::new(HostDown {
            unit: body(1).unit.id,
            host: replacement.registration.host.clone(),
        })),
    )
    .await;
    assert_eq!(
        effects(&executor),
        [
            Effect::ReleaseRegistration(replacement.registration),
            Effect::ReleaseUsage { acquisition: token },
            Effect::Health(HealthEvent::Close),
            Effect::Reply(Box::new(AddInstanceReply {
                unit: captured.unit.clone(),
                replica: captured.replica.clone(),
                error: APPLICATION_HOST_CRASH
            })),
        ]
    );
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(executor.service_types().is_empty());
    assert_eq!(operations(&hooks), ["Insert", "Update", "Update"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn opening_intermediate_terminal_and_host_down_updates_keep_state_effects_and_node_ledger_private(
) {
    // Rust storage translation of native persistent Open helper selection:
    // ReconfigurationAgent.cpp:3837-3847, not native power-loss qualification.
    for kind in 0..3 {
        for uncertain in [false, true] {
            for fail in [false, true] {
                let settings = Settings {
                    open_retry: RetryPolicy {
                        warning: 1,
                        drop: if kind == 2 { 1 } else { 40 },
                        ..RetryPolicy::open()
                    },
                    ..Settings::default()
                };
                // Terminal case uses non-tied warning/drop.
                let settings = if kind == 2 {
                    Settings {
                        open_retry: RetryPolicy {
                            warning: 10,
                            drop: 1,
                            ..RetryPolicy::open()
                        },
                        ..settings
                    }
                } else {
                    settings
                };
                let (store, executor, hooks) = setup_open(
                    settings,
                    if uncertain {
                        Duration::from_millis(20)
                    } else {
                        WAIT
                    },
                )
                .await;
                let token = begin_open(&executor).await;
                registered(&executor, &body(1), token).await;
                let before = state(&executor);
                let node = executor.service_types();
                let command = if kind == 1 {
                    Command::HostDown(Box::new(HostDown {
                        unit: body(1).unit.id,
                        host: registration().host,
                    }))
                } else {
                    let e = runtime_result(&executor, -77, if kind == 2 { ABORT } else { 0 });
                    Command::CompleteOpen(Box::new(e))
                };
                let (entered, release) = hooks.pause(fail);
                let ticket = executor.submit(command).unwrap();
                entered.await.unwrap();
                if uncertain {
                    assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
                }
                assert!(Arc::ptr_eq(&before, &state(&executor)));
                assert_eq!(executor.service_types(), node);
                assert!(effects(&executor).is_empty());
                let sentinel = executor.submit(add(body(1))).unwrap();
                assert_eq!(sentinel.status(), Status::Pending);
                drop(ticket); // accepted operation—not its observer—owns commit and resources
                release.send(()).unwrap();
                sentinel.wait(WAIT).await.unwrap();
                let actions = effects(&executor);
                if fail {
                    assert_eq!(*state(&executor), *before);
                    assert_eq!(executor.service_types(), node);
                    assert!(actions.is_empty());
                } else {
                    assert_eq!(
                        state(&executor).open_failures,
                        if kind == 2 { 0 } else { 1 }
                    );
                    assert_eq!(
                        state(&executor).phase,
                        if kind == 2 {
                            Phase::Closed
                        } else {
                            Phase::Opening
                        }
                    );
                    assert_eq!(executor.service_types().len(), usize::from(kind == 0));
                    assert_eq!(
                        actions.len(),
                        if kind == 0 {
                            1
                        } else if kind == 1 {
                            2
                        } else {
                            5
                        }
                    ); // closed Add may also report dropped
                }
                assert_eq!(operations(&hooks), ["Insert", "Update"]);
                finish(&store, &executor).await;
            }
        }
    }
}

#[tokio::test]
async fn native_open_success_clears_warning_or_error_threshold_without_inventing_error_health() {
    // Direct SL Test.StateMachine.cpp:2821-2828,3181-3184.
    // Generic ignored restart/error branches: RetryableErrorState.cpp:21-68;
    // Open consumer FailoverUnit.cpp:5258-5279,5363-5367.
    for policy in [
        RetryPolicy {
            warning: 1,
            ..RetryPolicy::open()
        },
        RetryPolicy {
            warning: 10,
            restart: 1,
            error: 2,
            ..RetryPolicy::open()
        },
    ] {
        let (store, executor, hooks) = setup_open(
            Settings {
                open_retry: policy,
                ..Settings::default()
            },
            WAIT,
        )
        .await;
        let token = begin_open(&executor).await;
        registered(&executor, &body(1), token).await;
        let failures = if policy.warning == 1 { 1 } else { 2 };
        for n in 1..=failures {
            runtime_reply(&executor, -45, 0).await;
            let actions = effects(&executor);
            assert_eq!(actions.len(), usize::from(policy.warning == n));
            assert_eq!(state(&executor).open_failures, n as u64);
        }
        let runtime = state(&executor).runtime.clone();
        let usage = state(&executor).usage;
        runtime_reply(&executor, 0, 0).await;
        assert!(
            matches!(effects(&executor).as_slice(),[Effect::OpeningHealth(h),Effect::Reply(r)] if h.event==HealthEvent::ClearOpenWarning && r.error==0 && r.replica.state==2 && !r.replica.service_endpoint.is_empty())
        );
        assert_eq!(state(&executor).open_failures, 0);
        assert_eq!(state(&executor).runtime, runtime);
        assert_eq!(state(&executor).usage, usage);
        assert_eq!(operations(&hooks).len(), failures as usize + 2);
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn remove_during_opening_reacquisition_releases_retained_usage_and_late_candidate_only() {
    // Handler-derived required removal race, not mislabeled DeleteReplica.cpp
    // :388-400 inputs as actual force RemoveInstance tests.
    let (store, executor, hooks) = setup(WAIT).await;
    let token = begin_open(&executor).await;
    registered(&executor, &body(1), token).await;
    run(
        &executor,
        Command::HostDown(Box::new(HostDown {
            unit: body(1).unit.id,
            host: registration().host,
        })),
    )
    .await;
    effects(&executor);
    run(&executor, Command::RetryOpen(body(1).unit.id)).await;
    effects(&executor);
    let late = lookup_result(&executor, 0);
    run(&executor, remove(body(1), false)).await;
    assert!(
        matches!(effects(&executor).as_slice(),[Effect::ReleaseUsage {acquisition},Effect::Health(HealthEvent::Close),Effect::RemoveReply(_)] if *acquisition==token)
    );
    assert!(state(&executor).usage.is_none());
    run(&executor, Command::Registered(Box::new(late.clone()))).await;
    assert_eq!(
        effects(&executor),
        [Effect::ReleaseRegistration(late.registration)]
    );
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert_eq!(operations(&hooks), ["Insert", "Update", "Update"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn outstanding_lookup_obligations_have_finite_capacity_and_release_only_on_real_result() {
    let store = Store::memory(StoreSettings::default()).await.unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_registration_lookups: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    let request = body(1);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    effects(&executor);
    let before = state(&executor);
    let retry = executor
        .submit(Command::RetryOpen(request.unit.id.clone()))
        .unwrap();
    assert_eq!(retry.wait(WAIT).await, Err(Failure::Capacity));
    assert!(Arc::ptr_eq(&before, &state(&executor)));
    assert!(effects(&executor).is_empty());
    // A real failed lookup consumes the obligation without acquiring a ref.
    let mut failed = acquired(&request, first.id, first.id, false);
    failed.error = HOSTING_SERVICE_TYPE_NOT_REGISTERED;
    failed.usage_acquired = false;
    failed.retryable = true;
    run(&executor, Command::Registered(Box::new(failed))).await;
    assert!(state(&executor).registration_lookups.is_empty());
    run(&executor, Command::RetryOpen(request.unit.id)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Open { .. }]
    ));
    assert_eq!(state(&executor).registration_lookups.len(), 1);
    finish(&store, &executor).await;
}
