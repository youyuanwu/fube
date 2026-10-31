//! Real Residency + real RA Executor + actual MemoryBackend; controlled gates
//! delay/veto real Backend::execute, never manufacture a store operation handle.
//! H1 equal-version registration lookup is real. No native execution, live node,
//! deployment, Hosting event router, RAP/physical teardown or native wire receipt
//! serialization is qualified. Old ra_bridge.rs remains simulated usage evidence.
mod common;
#[path = "support/residency_bridge.rs"]
mod residency_bridge;

use common::*;
use fube_hosting as hosting;
use fube_ra::{entity as ra, protocol as wire};
use fube_ra_store::{
    Backend, BackendContext, Error as StoreError, ErrorKind, MemoryBackend, Output, Request,
    Settings as StoreSettings, Store, StoreFuture,
};
use residency_bridge::{Bridge, Diagnostic, Error, Issued, Limits, Ownership};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(3);
const UNKNOWN: Duration = Duration::from_millis(50);

struct Gate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    fail: bool,
}

#[derive(Default)]
struct Hooks {
    gate: Mutex<Option<Gate>>,
    requests: Mutex<Vec<Request>>,
    written: AtomicUsize,
    failures: AtomicUsize,
}

impl Hooks {
    fn pause(&self, fail: bool) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, reached) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut slot = self.gate.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(Gate {
            entered,
            release: released,
            fail,
        });
        (reached, release)
    }

    fn operations(&self) -> Vec<&'static str> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| match request {
                Request::Insert(_) => "Insert",
                Request::Update(_) => "Update",
                _ => panic!("unexpected storage operation"),
            })
            .collect()
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
            {
                let mut trace = self.hooks.requests.lock().unwrap();
                assert!(trace.len() < 64); // finite controlled fixture diagnostics
                trace.push(match &request {
                    Request::Insert(record) => Request::Insert(record.clone()),
                    Request::Update(record) => Request::Update(record.clone()),
                    _ => panic!("this RA profile performs actual Insert/Update only"),
                });
            }
            let gate = self.hooks.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.send(()).unwrap();
                gate.release.await.expect("resolve actual backend work");
                if gate.fail {
                    self.hooks.failures.fetch_add(1, Ordering::SeqCst);
                    return Err(StoreError::new(ErrorKind::Io, "definite precommit veto"));
                }
            }
            let output = self.inner.execute(request).await?;
            assert_eq!(output, Output::Written);
            self.hooks.written.fetch_add(1, Ordering::SeqCst);
            Ok(output)
        })
    }

    fn close(&mut self) -> StoreFuture<'_, ()> {
        self.inner.close()
    }
}

fn admission() -> ra::Admission {
    ra::Admission {
        local_node: wire::NodeInstance {
            id: [4; 16],
            instance: 20,
        },
        fm_source: wire::NodeInstance {
            id: [8; 16],
            instance: 30,
        },
        generation: wire::GenerationNumber {
            generation: 7,
            owner: [8; 16],
        },
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}

struct Profile {
    core: hosting::Hosting,
    context: hosting::RuntimeContext,
    widget: hosting::TypeInstance,
    other: hosting::TypeInstance,
    bridge: Bridge,
}

fn profile(limits: Limits, zero_grace: bool) -> Profile {
    let mut core = hosting();
    completed_host(&mut core, "H");
    let context = valid_runtime(&mut core, "R", "H");
    let widget = registered(&mut core, "Widget", &context);
    let other = registered(&mut core, "Other", &context);
    let mut residency = hosting::Residency::new(
        hosting::ResidencyLimits::default(),
        hosting::ResidencyConfig {
            shared_grace: hosting::GracePeriod::Finite(if zero_grace { 0 } else { 10 }),
            exclusive_grace: hosting::GracePeriod::Finite(0),
            unused_scan_interval: hosting::GracePeriod::Infinite,
        },
        &hosting::ManualClock(0),
    )
    .unwrap();
    residency
        .admit_package(hosting::ResidencyAdmission {
            package: context.package.clone(),
            version: context.version.clone(),
            package_instance_id: context.package_instance_sequence,
            service_name: "fabric:/app/service".into(),
            application_name: "fabric:/app".into(),
        })
        .unwrap();
    Profile {
        core,
        context,
        widget,
        other,
        bridge: Bridge::new(residency, limits),
    }
}

fn body(
    id: u8,
    target: &hosting::TypeInstance,
    version: &hosting::VersionInstance,
) -> wire::AddInstance {
    let mut body = wire::AddInstance::default();
    body.unit.id.guid.0 = [id; 16];
    body.unit.consistency.id = body.unit.id.clone();
    body.unit.current = wire::Epoch {
        configuration: 5,
        data_loss: 1,
    };
    body.replica.node = admission().local_node;
    body.replica.id = 40;
    body.replica.instance = 50;
    body.replica.state = 1;
    body.replica.version = residency_bridge::version(version);
    body.service.version = residency_bridge::version(version);
    body.service.name = "fabric:/app/service".into();
    body.service.application = "fabric:/app".into();
    body.service.service_type = residency_bridge::service_type(&target.service_type);
    body
}

fn add(body: wire::AddInstance) -> ra::Command {
    ra::Command::Add {
        body: Box::new(body),
        source: admission().fm_source,
        generation: wire::GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

fn remove(body: wire::AddInstance) -> ra::Command {
    ra::Command::Remove {
        body: Box::new(wire::RemoveInstance::new(body, false)),
        source: admission().fm_source,
        generation: wire::GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

async fn setup(timeout: Duration, settings: ra::Settings) -> (Store, ra::Executor, Arc<Hooks>) {
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
    let executor = ra::Executor::new(Arc::new(store.clone()), admission(), settings).unwrap();
    (store, executor, hooks)
}

async fn run(executor: &ra::Executor, command: ra::Command) -> ra::Disposition {
    executor.submit(command).unwrap().wait(WAIT).await.unwrap()
}

fn effects(executor: &ra::Executor) -> Vec<ra::Effect> {
    executor
        .take_effects()
        .into_iter()
        .map(|dispatched| dispatched.effect)
        .collect()
}

async fn issue(executor: &ra::Executor, body: wire::AddInstance) -> Issued {
    run(executor, add(body)).await;
    let effects = effects(executor);
    assert_eq!(effects.len(), 1);
    Issued::from_effect(&effects[0])
}

async fn prepare_and_register(executor: &ra::Executor, p: &mut Profile, issued: &Issued) {
    let result = residency_bridge::lookup(&mut p.core, issued);
    assert_eq!(result.result, Ok(()));
    p.bridge.prepare(issued, &result).unwrap();
    p.bridge.submit(executor, issued.lookup).unwrap();
    assert_eq!(
        p.bridge
            .offer(issued.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(ra::Disposition::Applied)
    );
    p.bridge.collect(executor).unwrap();
}

async fn complete_open(executor: &ra::Executor, p: &mut Profile, issued: &Issued) {
    let before = executor
        .snapshot(issued.body.unit.id.guid.0)
        .committed
        .unwrap();
    let mut replica = before.body.replica.clone();
    replica.up = true;
    replica.service_endpoint = "tcp://controlled-test-open-result".into();
    // Trusted RAP-shaped input only; this test does not implement or execute RAP.
    run(
        executor,
        ra::Command::CompleteOpen(Box::new(ra::OpenCompletion {
            unit: before.body.unit.clone(),
            replica,
            error: 0,
            flags: 0,
            detail: ra::ProxyErrorDetail::default(),
        })),
    )
    .await;
    p.bridge.collect(executor).unwrap();
}

async fn ready(executor: &ra::Executor, p: &mut Profile, id: u8) -> Issued {
    let issued = issue(executor, body(id, &p.widget, &p.context.version)).await;
    prepare_and_register(executor, p, &issued).await;
    complete_open(executor, p, &issued).await;
    issued
}

async fn finish(store: &Store, executor: &ra::Executor) {
    let stats = executor.close(WAIT).await.unwrap();
    assert_eq!(stats.outstanding, 0);
    assert_eq!(stats.accepted, stats.succeeded + stats.failed);
    store.close().await.unwrap();
}

fn assert_count(p: &Profile, package: u64, application: u64, acquisitions: usize) {
    assert_eq!(
        p.bridge
            .residency
            .package_accounting(&p.context.package)
            .unwrap()
            .usage,
        package
    );
    assert_eq!(
        p.bridge
            .residency
            .application_accounting(&p.context.package.package.application)
            .unwrap()
            .usage,
        application
    );
    assert_eq!(p.bridge.residency.resources().acquisitions, acquisitions);
}

fn acquisition_id(p: &Profile, lookup: u64) -> u64 {
    p.bridge.offer(lookup).acquisition.as_ref().unwrap().id()
}

async fn begin_close(executor: &ra::Executor, p: &mut Profile, issued: &Issued) -> usize {
    run(executor, remove((*issued.body).clone())).await;
    p.bridge.collect(executor).unwrap();
    assert_eq!(
        executor
            .snapshot(issued.body.unit.id.guid.0)
            .committed
            .unwrap()
            .phase,
        ra::Phase::Closing
    );
    p.bridge
        .prepare_close(executor, issued.body.unit.id.guid.0)
        .unwrap()
}

#[tokio::test]
async fn native_test_adaptation_add_down_with_processor_derived_reopen_and_local_replay() {
    // Native-test adaptation: Test.Unit.ServiceTypeRegistrationWrapper.cpp:324-411,
    // 413-540: initial/duplicate add and down without decrement. Reopen is the
    // processor-derived combination from wrapper:78-125; exact replay is local.
    let (store, executor, hooks) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let issued = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    assert_eq!(issued.usage, None);
    prepare_and_register(&executor, &mut p, &issued).await;
    let original = acquisition_id(&p, issued.lookup);
    let offer = p.bridge.offer(issued.lookup);
    assert_eq!(offer.ownership, Ownership::Adopted);
    assert_eq!(offer.snapshot.candidate.as_ref().unwrap().runtime_id, "R");
    assert_eq!(offer.target.version, p.context.version);
    assert_eq!(offer.target.package_instance_id, 123);
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        Some(issued.lookup)
    );
    assert_eq!(hooks.operations(), ["Insert"]); // FSR publication is MEMORY-only.
    assert_count(&p, 1, 1, 1);
    p.bridge.submit(&executor, issued.lookup).unwrap();
    assert_eq!(
        p.bridge
            .offer(issued.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(ra::Disposition::IgnoredStale)
    );
    assert!(effects(&executor).is_empty());
    assert_eq!(acquisition_id(&p, issued.lookup), original);

    let current = executor.snapshot([1; 16]).committed.unwrap();
    let mut replica = current.body.replica.clone();
    replica.up = false;
    run(
        &executor,
        ra::Command::CompleteOpen(Box::new(ra::OpenCompletion {
            unit: current.body.unit.clone(),
            replica,
            error: ra::APPLICATION_HOST_CRASH,
            flags: 0,
            detail: ra::ProxyErrorDetail::default(),
        })),
    )
    .await;
    p.bridge.collect(&executor).unwrap();
    assert!(executor
        .snapshot([1; 16])
        .committed
        .unwrap()
        .runtime
        .is_none());
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        Some(issued.lookup)
    );
    assert_count(&p, 1, 1, 1);
    run(
        &executor,
        ra::Command::RetryOpen(issued.body.unit.id.clone()),
    )
    .await;
    let actions = effects(&executor);
    let reopen = Issued::from_effect(&actions[0]);
    assert_ne!(reopen.lookup, issued.lookup);
    assert_eq!(reopen.usage, Some(issued.lookup));
    prepare_and_register(&executor, &mut p, &reopen).await;
    assert_eq!(
        p.bridge.offer(reopen.lookup).ownership,
        Ownership::Reuses(issued.lookup)
    );
    assert!(!p.bridge.offer(reopen.lookup).completion.usage_acquired);
    assert!(p.bridge.offer(reopen.lookup).acquisition.is_none());
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_count(&p, 1, 1, 1);
    complete_open(&executor, &mut p, &reopen).await;
    assert_eq!(hooks.operations(), ["Insert", "Update", "Update"]);
    p.bridge.shutdown();
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_committed_usage_prevents_abandonment_even_before_ticket_classification() {
    let (store, executor, _) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let issued = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    let result = residency_bridge::lookup(&mut p.core, &issued);
    p.bridge.prepare(&issued, &result).unwrap();
    let original = acquisition_id(&p, issued.lookup);
    p.bridge.submit(&executor, issued.lookup).unwrap();
    p.bridge
        .offer(issued.lookup)
        .ticket()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert_eq!(p.bridge.offer(issued.lookup).ownership, Ownership::Offered);
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        Some(issued.lookup)
    );
    assert_eq!(
        p.bridge.abandon(&executor, issued.lookup),
        Err(Error::Outstanding)
    );
    assert_eq!(p.bridge.offer(issued.lookup).ownership, Ownership::Adopted);
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_count(&p, 1, 1, 1);
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 0);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_unissued_success_is_caller_owned_until_explicit_abandonment() {
    let (store, executor, _) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(
        Limits {
            records: 1,
            ..Limits::default()
        },
        false,
    );
    let real = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    // Malformed/unissued caller lookup ID; NOT a fabricated store operation.
    let unissued = Issued {
        lookup: u64::MAX,
        ..real.clone()
    };
    let result = residency_bridge::lookup(&mut p.core, &unissued);
    p.bridge.prepare(&unissued, &result).unwrap();
    let original = acquisition_id(&p, unissued.lookup);
    p.bridge.submit(&executor, unissued.lookup).unwrap();
    assert_eq!(
        p.bridge
            .offer(unissued.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(ra::Disposition::IgnoredStale)
    );
    p.bridge.collect(&executor).unwrap();
    p.bridge.classify(&executor, unissued.lookup);
    assert_eq!(
        p.bridge.offer(unissued.lookup).ownership,
        Ownership::Offered
    );
    assert_eq!(executor.snapshot([1; 16]).committed.unwrap().usage, None);
    assert_eq!(acquisition_id(&p, unissued.lookup), original);
    assert_count(&p, 1, 1, 1);
    p.bridge.abandon(&executor, unissued.lookup).unwrap();
    assert_count(&p, 0, 0, 0);
    assert_eq!(
        p.bridge.offer(unissued.lookup).ownership,
        Ownership::Abandoned
    );
    assert_eq!(p.bridge.offer(unissued.lookup).releases, 1);
    assert_eq!(
        p.bridge.abandon(&executor, unissued.lookup),
        Err(Error::Terminal)
    );
    assert_eq!(
        p.bridge.submit(&executor, unissued.lookup),
        Err(Error::Terminal)
    );
    assert_eq!(p.bridge.records(), 1); // terminal-ID reservation is not reclaimed.
    let real_result = residency_bridge::lookup(&mut p.core, &real);
    let before = p.bridge.residency.resources();
    assert_eq!(p.bridge.prepare(&real, &real_result), Err(Error::Capacity));
    assert_eq!(p.bridge.residency.resources(), before);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn processor_derived_obsolete_disposal_then_replay_releases_only_its_exact_owner_once() {
    // RA opening::dispose/adopt_usage and lifecycle::Registered, not a native test.
    let (store, executor, hooks) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let first = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    run(
        &executor,
        ra::Command::RetryOpen(first.body.unit.id.clone()),
    )
    .await;
    let second = Issued::from_effect(&effects(&executor)[0]);
    let first_result = residency_bridge::lookup(&mut p.core, &first);
    let second_result = residency_bridge::lookup(&mut p.core, &second);
    p.bridge.prepare(&first, &first_result).unwrap();
    p.bridge.prepare(&second, &second_result).unwrap();
    let first_owner = acquisition_id(&p, first.lookup);
    let second_owner = acquisition_id(&p, second.lookup);
    assert_ne!(first_owner, second_owner);
    assert_count(&p, 2, 2, 2);
    for issued in [&first, &second] {
        p.bridge.submit(&executor, issued.lookup).unwrap();
        p.bridge
            .offer(issued.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
        if issued.lookup == second.lookup {
            // Local safety regression P2R-001: consumed issued disposal is
            // authoritative even before its batch is drained/classified.
            let queued = executor.statistics().effects;
            assert!(queued >= 2);
            assert_eq!(
                p.bridge.abandon(&executor, second.lookup),
                Err(Error::Outstanding)
            );
            assert_eq!(
                p.bridge.offer(second.lookup).ownership,
                Ownership::DisposalPending
            );
            assert_eq!(p.bridge.offer(second.lookup).release_attempts, 0);
            assert_eq!(executor.statistics().effects, queued);
        }
        p.bridge.collect(&executor).unwrap();
    }
    assert_eq!(p.bridge.offer(first.lookup).ownership, Ownership::Adopted);
    assert_eq!(p.bridge.offer(second.lookup).ownership, Ownership::Released);
    assert_eq!(p.bridge.offer(second.lookup).release_attempts, 1);
    assert_eq!(p.bridge.offer(second.lookup).releases, 1);
    assert!(p.bridge.offer(second.lookup).work.iter().any(|work|
        matches!(work.effect, ra::Effect::ReleaseUsage { acquisition } if acquisition == second.lookup)));
    assert_eq!(acquisition_id(&p, first.lookup), first_owner);
    assert_count(&p, 1, 1, 1);
    p.bridge.submit(&executor, second.lookup).unwrap();
    assert_eq!(
        p.bridge
            .offer(second.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(ra::Disposition::IgnoredStale)
    );
    assert!(effects(&executor).is_empty());
    assert_eq!(p.bridge.offer(second.lookup).release_attempts, 1);
    assert_eq!(hooks.operations(), ["Insert"]);
    finish(&store, &executor).await;
}

async fn publication_veto_fixture() -> (Store, ra::Executor, Arc<Hooks>, Profile, Issued, Issued) {
    let (store, executor, hooks) = setup(
        WAIT,
        ra::Settings {
            max_service_types: 1,
            ..ra::Settings::default()
        },
    )
    .await;
    let mut p = profile(Limits::default(), false);
    let first = ready(&executor, &mut p, 1).await;
    let second = issue(&executor, body(2, &p.other, &p.context.version)).await;
    let result = residency_bridge::lookup(&mut p.core, &second);
    p.bridge.prepare(&second, &result).unwrap();
    let before = executor.snapshot([2; 16]).committed.unwrap();
    let writes = hooks.operations().len();
    p.bridge.submit(&executor, second.lookup).unwrap();
    assert_eq!(
        p.bridge
            .offer(second.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await,
        Err(ra::Failure::Capacity)
    );
    p.bridge.classify(&executor, second.lookup);
    assert!(Arc::ptr_eq(
        &before,
        &executor.snapshot([2; 16]).committed.unwrap()
    ));
    assert!(effects(&executor).is_empty());
    assert_eq!(hooks.operations().len(), writes);
    assert_eq!(
        p.bridge.offer(second.lookup).diagnostic,
        Some(Diagnostic::PublicationFailed)
    );
    assert_eq!(p.bridge.offer(second.lookup).ownership, Ownership::Offered);
    assert_count(&p, 2, 2, 2);
    (store, executor, hooks, p, first, second)
}

#[tokio::test]
async fn local_safety_replay_before_collection_cannot_erase_consumed_issuance() {
    // P2R-002 local ownership regression using actual issued work/accounting.
    let (store, executor, _) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let first = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    run(
        &executor,
        ra::Command::RetryOpen(first.body.unit.id.clone()),
    )
    .await;
    let second = Issued::from_effect(&effects(&executor)[0]);
    for issued in [&first, &second] {
        let result = residency_bridge::lookup(&mut p.core, issued);
        p.bridge.prepare(issued, &result).unwrap();
        p.bridge.submit(&executor, issued.lookup).unwrap();
        p.bridge
            .offer(issued.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
        if issued.lookup == first.lookup {
            p.bridge.collect(&executor).unwrap();
        }
    }
    assert_eq!(p.bridge.offer(second.lookup).ownership, Ownership::Offered);
    let queued = executor.statistics().effects;
    assert!(queued >= 2);
    // No explicit classify/collect/abandon before this replay.
    p.bridge.submit(&executor, second.lookup).unwrap();
    assert_eq!(
        p.bridge
            .offer(second.lookup)
            .ticket()
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(ra::Disposition::IgnoredStale)
    );
    assert_eq!(
        p.bridge.abandon(&executor, second.lookup),
        Err(Error::Outstanding)
    );
    assert_eq!(executor.statistics().effects, queued);
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(second.lookup).ownership, Ownership::Released);
    assert_eq!(p.bridge.offer(second.lookup).release_attempts, 1);
    assert_eq!(p.bridge.offer(second.lookup).releases, 1);
    assert_count(&p, 1, 1, 1);
    assert!(p.bridge.offer(second.lookup).work.iter().any(|work|
        matches!(work.effect, ra::Effect::ReleaseUsage { acquisition } if acquisition == second.lookup)));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_definite_memory_publication_veto_then_correction_reuses_original_offer() {
    let (store, executor, hooks, mut p, first, second) = publication_veto_fixture().await;
    let original = acquisition_id(&p, second.lookup);
    let serialized = p.bridge.offer(second.lookup).serialized.clone();
    let first_ticket = p
        .bridge
        .offer(second.lookup)
        .original_ticket
        .as_ref()
        .unwrap()
        .id;
    let close = begin_close(&executor, &mut p, &first).await;
    p.bridge.submit_close(&executor, close).unwrap();
    p.bridge
        .close(close)
        .ticket
        .as_ref()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    p.bridge.collect(&executor).unwrap();
    assert_eq!(executor.service_types().len(), 0);
    assert_count(&p, 1, 1, 1);
    p.bridge.submit(&executor, second.lookup).unwrap();
    p.bridge
        .offer(second.lookup)
        .ticket()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(second.lookup).ownership, Ownership::Adopted);
    assert_eq!(p.bridge.offer(second.lookup).serialized, serialized);
    assert_eq!(
        p.bridge
            .offer(second.lookup)
            .original_ticket
            .as_ref()
            .unwrap()
            .id,
        first_ticket
    );
    assert_ne!(
        p.bridge.offer(second.lookup).ticket().unwrap().id,
        first_ticket
    );
    assert_eq!(acquisition_id(&p, second.lookup), original);
    assert_eq!(
        executor.snapshot([2; 16]).committed.unwrap().usage,
        Some(second.lookup)
    );
    assert_count(&p, 1, 1, 1);
    assert_eq!(
        hooks.operations(),
        ["Insert", "Update", "Insert", "Update", "Update"]
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_explicit_abandonment_after_memory_veto_withdraws_then_compensates_once() {
    let (store, executor, _, mut p, _, second) = publication_veto_fixture().await;
    let original = acquisition_id(&p, second.lookup);
    let original_target = p.bridge.offer(second.lookup).target.clone();
    let sequence = p
        .bridge
        .residency
        .package_accounting(&p.context.package)
        .unwrap()
        .sequence;
    assert!(p.bridge.residency.acquisition(original).is_some());
    // A newer caller description/admitted accounting premise is NOT the old
    // acquisition target. Compensation must never resolve through this package.
    let mut replacement = p.context.clone();
    replacement.package.package.name = "ReplacementPkg".into();
    replacement.version.instance += 1;
    replacement.package_instance_sequence += 1;
    p.bridge
        .residency
        .admit_package(hosting::ResidencyAdmission {
            package: replacement.package.clone(),
            version: replacement.version.clone(),
            package_instance_id: replacement.package_instance_sequence,
            service_name: "fabric:/app/replacement".into(),
            application_name: "fabric:/app".into(),
        })
        .unwrap();
    p.bridge.abandon(&executor, second.lookup).unwrap();
    assert!(p.bridge.offer(second.lookup).terminal);
    assert_eq!(
        p.bridge.offer(second.lookup).ownership,
        Ownership::Abandoned
    );
    assert_eq!(p.bridge.offer(second.lookup).release_attempts, 1);
    assert_eq!(p.bridge.offer(second.lookup).releases, 1);
    assert!(p.bridge.residency.acquisition(original).is_none());
    assert_eq!(p.bridge.offer(second.lookup).target, original_target);
    let replacement_accounting = p
        .bridge
        .residency
        .package_accounting(&replacement.package)
        .unwrap();
    assert_eq!(
        (
            replacement_accounting.usage,
            replacement_accounting.sequence
        ),
        (0, 0)
    );
    assert_eq!(
        p.bridge
            .residency
            .package_accounting(&p.context.package)
            .unwrap()
            .sequence,
        sequence + 1
    );
    assert_eq!(
        p.bridge.submit(&executor, second.lookup),
        Err(Error::Terminal)
    );
    assert_eq!(
        p.bridge.abandon(&executor, second.lookup),
        Err(Error::Terminal)
    );
    assert!(executor
        .snapshot([2; 16])
        .committed
        .unwrap()
        .registration_lookups
        .contains_key(&second.lookup));
    assert_count(&p, 1, 1, 1);
    // Explicit local withdrawal is NOT cancellation of RA's retained lookup.
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_nonaccepted_submit_retains_original_for_later_correction() {
    let (store, executor, hooks) = setup(
        WAIT,
        ra::Settings {
            max_outstanding: 1,
            ..ra::Settings::default()
        },
    )
    .await;
    let mut p = profile(Limits::default(), false);
    let issued = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    let result = residency_bridge::lookup(&mut p.core, &issued);
    p.bridge.prepare(&issued, &result).unwrap();
    let original = acquisition_id(&p, issued.lookup);
    let (entered, release) = hooks.pause(false);
    let unrelated = executor
        .submit(add(body(2, &p.widget, &p.context.version)))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        p.bridge.submit(&executor, issued.lookup),
        Err(Error::Capacity)
    );
    assert!(p.bridge.offer(issued.lookup).original_ticket.is_none());
    assert_eq!(
        p.bridge.offer(issued.lookup).diagnostic,
        Some(Diagnostic::SubmitRejected)
    );
    assert_count(&p, 1, 1, 1);
    release.send(()).unwrap();
    unrelated.wait(WAIT).await.unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [ra::Effect::Open { .. }]
    ));
    p.bridge.submit(&executor, issued.lookup).unwrap();
    p.bridge
        .offer(issued.lookup)
        .ticket()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(issued.lookup).ownership, Ownership::Adopted);
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_count(&p, 1, 1, 1);
    finish(&store, &executor).await;
}

fn close_application_between_begin_and_finish(p: &mut Profile) -> hosting::DeactivationCallback {
    p.bridge
        .residency
        .schedule_application_timer(&p.context.package.package.application, 0)
        .unwrap();
    p.bridge.residency.dequeue_due().unwrap().unwrap()
}

#[tokio::test]
async fn processor_derived_partial_increment_is_separate_residual_not_full_success_and_compensates()
{
    // Deactivator.cpp:337-365 + 504-527; package-only compensation is LOCAL safety.
    let (store, executor, hooks) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(
        Limits {
            records: 1,
            ..Limits::default()
        },
        true,
    );
    let issued = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    let result = residency_bridge::lookup(&mut p.core, &issued);
    let callback = close_application_between_begin_and_finish(&mut p);
    p.bridge
        .prepare_split(&issued, &result, |ledger| {
            assert!(matches!(
                ledger.execute_callback(callback),
                hosting::CallbackExecution::Deferred { .. }
            ));
        })
        .unwrap();
    let original = acquisition_id(&p, issued.lookup);
    assert_count(&p, 1, 0, 1);
    assert_eq!(
        p.bridge.offer(issued.lookup).ownership,
        Ownership::PackageOnly
    );
    assert_eq!(
        p.bridge.residency.acquisition(original).unwrap().phase,
        hosting::AcquisitionPhase::PackageOnly
    );
    assert!(!p.bridge.offer(issued.lookup).completion.usage_acquired);
    assert_ne!(p.bridge.offer(issued.lookup).completion.usage_error, 0);
    p.bridge.submit(&executor, issued.lookup).unwrap();
    p.bridge
        .offer(issued.lookup)
        .ticket()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    p.bridge.collect(&executor).unwrap();
    let current = executor.snapshot([1; 16]).committed.unwrap();
    assert!(current.runtime.is_none() && current.usage.is_none());
    assert_eq!(current.registration_failures, 0);
    assert!(!p
        .bridge
        .offer(issued.lookup)
        .work
        .iter()
        .any(|work| matches!(work.effect, ra::Effect::RuntimeOpen { .. })));
    assert_count(&p, 1, 0, 1);
    p.bridge.abandon(&executor, issued.lookup).unwrap();
    assert_count(&p, 0, 0, 0);
    assert_eq!(p.bridge.offer(issued.lookup).releases, 1);
    assert_eq!(
        p.bridge.abandon(&executor, issued.lookup),
        Err(Error::Terminal)
    );
    assert_eq!(p.bridge.residency.resources().final_obligations, 1); // no physical End
    assert_eq!(hooks.operations(), ["Insert"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_count_and_exact_byte_bounds_reject_before_any_increment() {
    let (store, executor, _) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let issued = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    let result = residency_bridge::lookup(&mut p.core, &issued);
    let exact = p.bridge.required_bytes(&issued, &result).unwrap();
    let mut too_small = profile(
        Limits {
            records: 1,
            bytes: exact - 1,
        },
        false,
    );
    let before = too_small.bridge.residency.resources();
    assert_eq!(
        too_small.bridge.prepare(&issued, &result),
        Err(Error::Capacity)
    );
    assert_eq!(too_small.bridge.residency.resources(), before);
    assert_eq!(too_small.bridge.records(), 0);
    assert_eq!(too_small.bridge.bytes(), 0);
    let mut exact_fit = profile(
        Limits {
            records: 1,
            bytes: exact,
        },
        false,
    );
    exact_fit.bridge.prepare(&issued, &result).unwrap();
    assert_eq!(exact_fit.bridge.bytes(), exact);
    assert_count(&exact_fit, 1, 1, 1);
    assert_eq!(
        exact_fit.bridge.prepare(&issued, &result),
        Err(Error::Duplicate)
    );
    run(
        &executor,
        ra::Command::RetryOpen(issued.body.unit.id.clone()),
    )
    .await;
    let second = Issued::from_effect(&effects(&executor)[0]);
    let second_result = residency_bridge::lookup(&mut p.core, &second);
    let before = exact_fit.bridge.residency.resources();
    assert_eq!(
        exact_fit.bridge.prepare(&second, &second_result),
        Err(Error::Capacity)
    );
    assert_eq!(exact_fit.bridge.residency.resources(), before);
    // Submission and its output transfer require no fresh bridge slot/bytes.
    exact_fit.bridge.submit(&executor, issued.lookup).unwrap();
    exact_fit
        .bridge
        .offer(issued.lookup)
        .ticket()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    exact_fit.bridge.collect(&executor).unwrap();
    assert_eq!(
        exact_fit.bridge.offer(issued.lookup).ownership,
        Ownership::Adopted
    );
    assert_eq!(exact_fit.bridge.bytes(), exact);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_unresolved_partial_compensation_at_capacity_survives_shutdown() {
    let (store, executor, _) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(
        Limits {
            records: 1,
            ..Limits::default()
        },
        true,
    );
    let issued = issue(&executor, body(1, &p.widget, &p.context.version)).await;
    let result = residency_bridge::lookup(&mut p.core, &issued);
    let callback = close_application_between_begin_and_finish(&mut p);
    p.bridge
        .prepare_split(&issued, &result, |ledger| {
            assert!(matches!(
                ledger.execute_callback(callback),
                hosting::CallbackExecution::Deferred { .. }
            ));
        })
        .unwrap();
    let original = acquisition_id(&p, issued.lookup);
    let bytes = p.bridge.bytes();
    p.bridge.shutdown();
    p.bridge.abandon(&executor, issued.lookup).unwrap();
    assert_eq!(
        p.bridge.offer(issued.lookup).ownership,
        Ownership::Unresolved
    );
    assert_eq!(
        p.bridge.offer(issued.lookup).diagnostic,
        Some(Diagnostic::ReleaseUnresolved)
    );
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 1);
    assert_eq!(p.bridge.offer(issued.lookup).releases, 0);
    assert_eq!(
        p.bridge.abandon(&executor, issued.lookup),
        Err(Error::Terminal)
    );
    assert_eq!(
        p.bridge.submit(&executor, issued.lookup),
        Err(Error::Terminal)
    );
    assert_eq!(p.bridge.bytes(), bytes);
    assert_count(&p, 1, 0, 1);
    assert_eq!(p.bridge.residency.resources().final_obligations, 1);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn processor_derived_durable_complete_close_releases_only_after_actual_written_commit() {
    // Native action ordering adapted to real RA durable close (FSR stays memory-only).
    let (store, executor, hooks) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let issued = ready(&executor, &mut p, 1).await;
    let original = acquisition_id(&p, issued.lookup);
    let close = begin_close(&executor, &mut p, &issued).await;
    let writes = hooks.written.load(Ordering::SeqCst);
    let (entered, release) = hooks.pause(false);
    p.bridge.submit_close(&executor, close).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        p.bridge.close(close).ticket.as_ref().unwrap().status(),
        ra::Status::Pending
    );
    assert!(Arc::ptr_eq(
        &p.bridge.close(close).before,
        &executor.snapshot([1; 16]).committed.unwrap()
    ));
    assert!(effects(&executor).is_empty());
    assert_eq!(hooks.written.load(Ordering::SeqCst), writes);
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_count(&p, 1, 1, 1);
    release.send(()).unwrap();
    p.bridge
        .close(close)
        .ticket
        .as_ref()
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert_eq!(hooks.written.load(Ordering::SeqCst), writes + 1);
    assert_eq!(executor.snapshot([1; 16]).committed.unwrap().usage, None);
    assert_count(&p, 1, 1, 1); // commit does not itself consume composition token.
    p.bridge.collect(&executor).unwrap();
    assert_count(&p, 0, 0, 0);
    assert_eq!(p.bridge.offer(issued.lookup).ownership, Ownership::Released);
    assert_eq!(p.bridge.offer(issued.lookup).releases, 1);
    assert!(p.bridge.close(close).work.iter().any(|work| matches!(
        work.effect, ra::Effect::ReleaseUsage { acquisition } if acquisition == issued.lookup
    )));
    p.bridge
        .close(close)
        .ticket
        .as_ref()
        .unwrap()
        .resolve(WAIT)
        .await
        .unwrap();
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 1);
    assert_eq!(hooks.operations(), ["Insert", "Update", "Update", "Update"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_definite_backend_close_failure_retains_adopted_exact_usage() {
    let (store, executor, hooks) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let issued = ready(&executor, &mut p, 1).await;
    let original = acquisition_id(&p, issued.lookup);
    let close = begin_close(&executor, &mut p, &issued).await;
    let (entered, release) = hooks.pause(true);
    p.bridge.submit_close(&executor, close).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    release.send(()).unwrap();
    assert!(matches!(
        p.bridge
            .close(close)
            .ticket
            .as_ref()
            .unwrap()
            .wait(WAIT)
            .await,
        Err(ra::Failure::Store(_))
    ));
    assert!(Arc::ptr_eq(
        &p.bridge.close(close).before,
        &executor.snapshot([1; 16]).committed.unwrap()
    ));
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(issued.lookup).ownership, Ownership::Adopted);
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 0);
    assert_count(&p, 1, 1, 1);
    assert_eq!(hooks.failures.load(Ordering::SeqCst), 1);
    finish(&store, &executor).await;
}

async fn cancel_observer(ticket: &ra::Ticket) {
    let observer = ticket.clone();
    let mut waiting = Box::pin(observer.resolve(WAIT));
    tokio::select! {
        result = &mut waiting => panic!("gated backend unexpectedly resolved: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    drop(waiting);
    drop(observer);
}

async fn unknown_close(fail: bool) {
    let (store, executor, hooks) = setup(UNKNOWN, ra::Settings::default()).await;
    let mut p = profile(
        Limits {
            records: 2,
            ..Limits::default()
        },
        false,
    );
    let issued = ready(&executor, &mut p, 1).await;
    let original = acquisition_id(&p, issued.lookup);
    let close = begin_close(&executor, &mut p, &issued).await;
    let serialized = p.bridge.close(close).serialized.clone();
    let records = p.bridge.records();
    let bytes = p.bridge.bytes();
    let writes = hooks.written.load(Ordering::SeqCst);
    let (entered, release) = hooks.pause(fail);
    let ticket_id = p.bridge.submit_close(&executor, close).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        p.bridge
            .close(close)
            .ticket
            .as_ref()
            .unwrap()
            .wait(WAIT)
            .await,
        Err(ra::Failure::Unknown)
    );
    tokio::time::timeout(WAIT, async {
        while store.stats().lifecycle != fube_ra_store::Lifecycle::Fenced {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(executor.snapshot([1; 16]).fenced);
    assert!(Arc::ptr_eq(
        &p.bridge.close(close).before,
        &executor.snapshot([1; 16]).committed.unwrap()
    ));
    assert_eq!(p.bridge.close(close).usage, issued.lookup);
    assert_eq!(p.bridge.close(close).serialized, serialized);
    assert_eq!(p.bridge.close(close).ticket.as_ref().unwrap().id, ticket_id);
    cancel_observer(p.bridge.close(close).ticket.as_ref().unwrap()).await;
    assert!(effects(&executor).is_empty());
    assert_eq!(
        p.bridge.abandon(&executor, issued.lookup),
        Err(Error::Outstanding)
    );
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_eq!(hooks.written.load(Ordering::SeqCst), writes);
    assert_count(&p, 1, 1, 1);
    release.send(()).unwrap();
    let resolved = p
        .bridge
        .close(close)
        .ticket
        .as_ref()
        .unwrap()
        .resolve(WAIT)
        .await;
    if fail {
        assert!(matches!(resolved, Err(ra::Failure::Store(_))));
        assert!(Arc::ptr_eq(
            &p.bridge.close(close).before,
            &executor.snapshot([1; 16]).committed.unwrap()
        ));
        assert_eq!(
            executor.snapshot([1; 16]).committed.unwrap().usage,
            Some(issued.lookup)
        );
        assert_eq!(hooks.written.load(Ordering::SeqCst), writes);
        p.bridge.collect(&executor).unwrap();
        assert_eq!(p.bridge.offer(issued.lookup).ownership, Ownership::Adopted);
        assert_eq!(acquisition_id(&p, issued.lookup), original);
        assert_count(&p, 1, 1, 1);
    } else {
        assert_eq!(resolved, Ok(ra::Disposition::Applied));
        assert_eq!(hooks.written.load(Ordering::SeqCst), writes + 1);
        assert_eq!(
            executor.snapshot([1; 16]).committed.unwrap().phase,
            ra::Phase::Closed
        );
        assert_count(&p, 1, 1, 1);
        p.bridge.collect(&executor).unwrap();
        assert_count(&p, 0, 0, 0);
        assert_eq!(p.bridge.offer(issued.lookup).releases, 1);
    }
    assert!(!executor.snapshot([1; 16]).fenced);
    assert_eq!(p.bridge.records(), records);
    assert_eq!(p.bridge.bytes(), bytes);
    assert_eq!(p.bridge.close(close).serialized, serialized);
    assert_eq!(p.bridge.close(close).ticket.as_ref().unwrap().id, ticket_id);
    assert_eq!(hooks.operations(), ["Insert", "Update", "Update", "Update"]);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_unknown_actual_complete_close_then_late_written_releases_postcommit() {
    unknown_close(false).await;
}

#[tokio::test]
async fn local_safety_unknown_actual_complete_close_then_definite_backend_failure_retains_usage() {
    unknown_close(true).await;
}

#[tokio::test]
async fn local_safety_observer_cancellation_and_drop_never_release_pending_close_usage() {
    let (store, executor, hooks) = setup(WAIT, ra::Settings::default()).await;
    let mut p = profile(Limits::default(), false);
    let issued = ready(&executor, &mut p, 1).await;
    let original = acquisition_id(&p, issued.lookup);
    let close = begin_close(&executor, &mut p, &issued).await;
    let (entered, release) = hooks.pause(true);
    let ticket_id = p.bridge.submit_close(&executor, close).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    cancel_observer(p.bridge.close(close).ticket.as_ref().unwrap()).await;
    assert_eq!(p.bridge.close(close).ticket.as_ref().unwrap().id, ticket_id);
    assert_eq!(
        p.bridge.close(close).ticket.as_ref().unwrap().status(),
        ra::Status::Pending
    );
    assert!(effects(&executor).is_empty());
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 0);
    assert_count(&p, 1, 1, 1);
    release.send(()).unwrap();
    assert!(matches!(
        p.bridge
            .close(close)
            .ticket
            .as_ref()
            .unwrap()
            .resolve(WAIT)
            .await,
        Err(ra::Failure::Store(_))
    ));
    p.bridge.collect(&executor).unwrap();
    assert_count(&p, 1, 1, 1);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_safety_pending_unknown_and_postcommit_unresolved_release_retain_slots_at_shutdown() {
    let (store, executor, hooks) = setup(UNKNOWN, ra::Settings::default()).await;
    let mut p = profile(
        Limits {
            records: 2,
            ..Limits::default()
        },
        false,
    );
    let issued = ready(&executor, &mut p, 1).await;
    let original = acquisition_id(&p, issued.lookup);
    let close = begin_close(&executor, &mut p, &issued).await;
    let bytes = p.bridge.bytes();
    let (entered, release) = hooks.pause(false);
    p.bridge.submit_close(&executor, close).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        p.bridge.close(close).ticket.as_ref().unwrap().status(),
        ra::Status::Pending
    );
    let unissued = Issued {
        lookup: u64::MAX,
        usage: None,
        ..issued.clone()
    };
    let result = residency_bridge::lookup(&mut p.core, &unissued);
    let before = p.bridge.residency.resources();
    assert_eq!(p.bridge.prepare(&unissued, &result), Err(Error::Capacity));
    assert_eq!(p.bridge.residency.resources(), before);
    assert_eq!(
        p.bridge
            .close(close)
            .ticket
            .as_ref()
            .unwrap()
            .wait(WAIT)
            .await,
        Err(ra::Failure::Unknown)
    );
    p.bridge.shutdown();
    assert_eq!(p.bridge.prepare(&unissued, &result), Err(Error::Closed));
    assert!(executor.snapshot([1; 16]).fenced);
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    release.send(()).unwrap();
    p.bridge
        .close(close)
        .ticket
        .as_ref()
        .unwrap()
        .resolve(WAIT)
        .await
        .unwrap();
    p.bridge.collect(&executor).unwrap();
    assert_eq!(executor.snapshot([1; 16]).committed.unwrap().usage, None);
    // RA disposal is authoritative, but closed Residency cannot verify decrement.
    // The same original token is now an exact reconciliation record, not erased.
    assert_eq!(
        p.bridge.offer(issued.lookup).ownership,
        Ownership::Unresolved
    );
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 1);
    assert_eq!(p.bridge.offer(issued.lookup).releases, 0);
    assert_eq!(acquisition_id(&p, issued.lookup), original);
    assert_count(&p, 1, 1, 1);
    assert_eq!(p.bridge.records(), 2);
    assert_eq!(p.bridge.bytes(), bytes);
    assert_eq!(
        p.bridge.abandon(&executor, issued.lookup),
        Err(Error::Terminal)
    );
    p.bridge.collect(&executor).unwrap();
    assert_eq!(p.bridge.offer(issued.lookup).release_attempts, 1);
    finish(&store, &executor).await;
}
